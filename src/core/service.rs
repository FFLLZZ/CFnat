use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::{Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::core::{IpPool, LoadBalancer, HttpingConfig, build_hyper_client, run_continuous_httping, run_forward, CancellationToken, ConnectionRegistry};
use crate::core::types::{StatusInfo, ConfigOverrides};
use crate::log::{push_log, reset_start_time, get_log_buffer};

/// 单次测速 / 健康检查请求超时（毫秒）
const REQUEST_TIMEOUT_MS: u64 = 1800;

/// stop 阶段单个后台任务的等待上限
const HANDLE_STOP_TIMEOUT: Duration = Duration::from_secs(3);

/// 服务相位：实例所有权随相位移动，任意时刻至多一个有效实例
enum Phase {
    Stopped,
    Starting,
    Running(Arc<ServiceInstance>),
    /// 实例由 stop 的局部变量持有，此相位仅表示停止进行中
    Stopping,
}

/// 一次有效运行的全部资源：句柄与取消令牌随实例生灭，不再有游离字段
struct ServiceInstance {
    cancel_token: CancellationToken,
    lb: Arc<LoadBalancer>,
    registry: Arc<ConnectionRegistry>,
    start_time: Instant,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

pub struct ServiceState {
    phase: Mutex<Phase>,
    config: RwLock<ServiceConfig>,
}

/// 启动过程的所有权守卫：构建期间相位保持 Starting，
/// panic 或提前返回时回滚为 Stopped；启动成功后由 finish_start 消费（forget）
struct StartGuard<'a>(&'a Mutex<Phase>);

impl Drop for StartGuard<'_> {
    fn drop(&mut self) {
        *self.0.lock() = Phase::Stopped;
    }
}

/// 逐句柄等待退出；超时的句柄显式 abort 并等其落地，不静默丢弃。
/// 返回被强制终止的任务数。
async fn await_handles(handles: &mut [JoinHandle<()>], limit: Duration) -> usize {
    let mut stuck = Vec::new();
    for handle in handles.iter_mut() {
        if tokio::time::timeout(limit, &mut *handle).await.is_err() {
            stuck.push(handle);
        }
    }
    let count = stuck.len();
    for handle in &stuck {
        handle.abort();
    }
    for handle in stuck {
        // abort 后任务在下一个 await 点落地，等待其完全退出再宣告停止
        let _ = handle.await;
    }
    count
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceConfig {
    pub ip_file: String,
    pub http: String,
    pub delay_limit: u64,
    pub tlr: f64,
    pub ips: usize,
    pub threads: usize,
    pub tls_port: u16,
    pub http_port: u16,
    pub colo: Option<Vec<String>>,
    pub addr: SocketAddr,
    pub max_sticky_slots: usize,
    #[cfg(feature = "web")]
    pub api_addr: Option<SocketAddr>,
}

impl ServiceConfig {
    // 各字段合法界与 args.rs 的 CLI 解析保持同一套（dl 1..=2000、tlr 0..=1、ips 1..=128、
    // threads 1..=1024、端口 ≥1、sticky 槽 1..=32），两个入口不出现第二套规则
    pub fn apply_overrides(&mut self, overrides: &ConfigOverrides) {
        if let Some(v) = &overrides.ip_file { self.ip_file = v.clone(); }
        if let Some(v) = &overrides.http { self.http = v.clone(); }
        if let Some(v) = overrides.delay_limit { self.delay_limit = v.clamp(1, 2000); }
        if let Some(v) = overrides.tlr { self.tlr = v.clamp(0.0, 1.0); }
        if let Some(v) = overrides.ips { self.ips = v.max(1).min(128); }
        if let Some(v) = overrides.threads { self.threads = v.max(1).min(1024); }
        if let Some(v) = overrides.tls_port { self.tls_port = v.max(1); }
        if let Some(v) = overrides.http_port { self.http_port = v.max(1); }
        if let Some(v) = &overrides.colo { self.colo = Some(v.clone()); }
        if let Some(v) = overrides.addr { self.addr = v; }
        if let Some(v) = overrides.max_sticky_slots { self.max_sticky_slots = v.max(1).min(32); }
        #[cfg(feature = "web")]
        if let Some(v) = overrides.api_addr { self.api_addr = Some(v); }
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            ip_file: "ip.txt".to_string(),
            http: "http://cp.cloudflare.com/cdn-cgi/trace".to_string(),
            delay_limit: 500,
            tlr: 0.1,
            ips: 10,
            threads: 16,
            tls_port: 443,
            http_port: 80,
            colo: None,
            addr: "127.6.6.6:1234".parse().unwrap(),
            max_sticky_slots: 5,
            #[cfg(feature = "web")]
            api_addr: None,
        }
    }
}

impl Default for ServiceState {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceState {
    pub fn new() -> Self {
        Self {
            phase: Mutex::new(Phase::Stopped),
            config: RwLock::new(ServiceConfig::default()),
        }
    }

    pub fn is_running(&self) -> bool {
        matches!(&*self.phase.lock(), Phase::Running(_))
    }

    pub fn get_config(&self) -> ServiceConfig {
        self.config.read().clone()
    }

    pub fn update_config(&self, new_config: ServiceConfig) {
        *self.config.write() = new_config;
    }

    pub fn get_uptime_secs(&self) -> u64 {
        match &*self.phase.lock() {
            Phase::Running(inst) => inst.start_time.elapsed().as_secs(),
            _ => 0,
        }
    }

    pub fn build_full_status(&self) -> StatusInfo {
        match &*self.phase.lock() {
            Phase::Running(inst) => {
                let mut info = StatusInfo::from_loadbalancer(&inst.lb);
                info.running = true;
                info.uptime_secs = inst.start_time.elapsed().as_secs();
                info
            }
            _ => StatusInfo::empty(),
        }
    }

    /// 占住 Starting 相位，构建期间任何并发 start/stop 都得到明确错误。
    /// 返回的守卫在 panic 或提前返回时把相位回滚为 Stopped。
    fn begin_start(&self) -> Result<StartGuard<'_>, String> {
        let mut phase = self.phase.lock();
        match *phase {
            Phase::Stopped => {
                *phase = Phase::Starting;
                Ok(StartGuard(&self.phase))
            }
            Phase::Starting => Err("服务正在启动".to_string()),
            Phase::Running(_) => Err("服务已在运行".to_string()),
            Phase::Stopping => Err("服务正在停止".to_string()),
        }
    }

    pub fn start(&self) -> Result<(), String> {
        let guard = self.begin_start()?;

        let config = self.get_config();
        let ip_pool = Arc::new(IpPool::from_file(&config.ip_file));
        if ip_pool.total_count() == 0 {
            return Err("未找到有效的 IP".to_string());
        }

        self.finish_start(ip_pool, guard)
    }

    pub fn start_with_ips(&self, ip_file: Option<&str>, ip_content: Option<&[String]>) -> Result<(), String> {
        let guard = self.begin_start()?;

        let mut all_ips = Vec::new();

        if let Some(file) = ip_file
            && !file.is_empty()
        {
            match std::fs::File::open(file) {
                Ok(f) => {
                    use std::io::{BufRead, BufReader};
                    for line in BufReader::new(f).lines().map_while(Result::ok) {
                        let line = line.trim();
                        if !line.is_empty() {
                            all_ips.push(line.to_string());
                        }
                    }
                }
                Err(e) => push_log("WARN", &format!("IP 文件打开失败: {} ({})", file, e)),
            }
        }

        if let Some(content) = ip_content {
            all_ips.extend(content.iter().cloned());
        }

        if all_ips.is_empty() {
            let config = self.get_config();
            // 显式传入的文件未能提供任何 IP 时回退到配置文件，必须让调用方从日志识别实际来源
            if let Some(file) = ip_file.filter(|f| !f.is_empty()) {
                push_log("WARN", &format!("IP 文件 {} 未提供有效 IP，回退使用 {}", file, config.ip_file));
            }
            let ip_pool = Arc::new(IpPool::from_file(&config.ip_file));
            if ip_pool.total_count() == 0 {
                return Err("未找到有效的 IP".to_string());
            }
            return self.finish_start(ip_pool, guard);
        }

        let ip_pool = Arc::new(IpPool::new(&all_ips));
        self.finish_start(ip_pool, guard)
    }

    /// 构建 instance 并登记为 Running。构建全部在临界区外完成；
    /// 守卫在此被消费，之后相位由 Phase::Running 承接。
    fn finish_start(&self, ip_pool: Arc<IpPool>, guard: StartGuard<'_>) -> Result<(), String> {
        // 启动前清空旧日志并重置时间基准
        reset_start_time();
        get_log_buffer().clear();

        let config = self.get_config();

        let client = Arc::new(build_hyper_client(config.delay_limit));
        let (notify_tx, notify_rx) = tokio::sync::watch::channel(false);
        let colo_filter = config.colo.clone();

        let cancel_token = CancellationToken::new();

        let lb = Arc::new(
            LoadBalancer::new(config.ips)
                .with_delay_threshold(config.delay_limit as f32)
                .with_loss_threshold(config.tlr as f32)
                .with_health_check_url(config.http.clone())
                .with_ports(config.tls_port, config.http_port)
                .with_timeout(REQUEST_TIMEOUT_MS)
                .with_notify(notify_tx)
                .with_client(client.clone())
                .with_colo_filter(colo_filter.clone())
                .with_max_sticky_slots(config.max_sticky_slots),
        );

        crate::core::init_global_limiter(config.threads);

        let registry = Arc::new(ConnectionRegistry::new());
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        let lb_clone = lb.clone();
        let http = config.http.clone();
        let tls_port = config.tls_port;
        let http_port = config.http_port;
        let delay_limit = config.delay_limit;
        let addr = config.addr;
        let cancel_token_for_httping = cancel_token.clone();

        let httping_handle: JoinHandle<()> = tokio::spawn(async move {
            run_continuous_httping(
                ip_pool,
                lb_clone,
                &http,
                HttpingConfig {
                    tls_port,
                    http_port,
                    timeout_ms: REQUEST_TIMEOUT_MS,
                    delay_limit,
                    colo_filter: colo_filter.map(Arc::new),
                    client,
                },
                notify_rx,
                cancel_token_for_httping,
            ).await;
        });
        handles.push(httping_handle);

        handles.extend(lb.clone().start_health_check());

        let lb_forward = lb.clone();
        let forward_cancel_token = cancel_token.clone();
        let registry_forward = registry.clone();

        let forward_handle: JoinHandle<()> = tokio::spawn(async move {
            if let Err(e) = run_forward(addr, lb_forward, tls_port, http_port, forward_cancel_token, registry_forward).await {
                push_log("ERROR", &format!("转发服务错误：{}", e));
            }
        });
        handles.push(forward_handle);

        std::mem::forget(guard);
        *self.phase.lock() = Phase::Running(Arc::new(ServiceInstance {
            cancel_token,
            lb,
            registry,
            start_time: Instant::now(),
            handles: Mutex::new(handles),
        }));

        Ok(())
    }

    pub async fn stop(&self) -> Result<(), String> {
        let instance = {
            let mut phase = self.phase.lock();
            match &*phase {
                Phase::Running(_) => {}
                Phase::Stopping => return Err("服务正在停止".to_string()),
                Phase::Starting => return Err("服务正在启动".to_string()),
                Phase::Stopped => return Err("服务未运行".to_string()),
            }
            let inst = match std::mem::replace(&mut *phase, Phase::Stopped) {
                Phase::Running(inst) => inst,
                _ => unreachable!(),
            };
            *phase = Phase::Stopping;
            inst
        };

        instance.cancel_token.cancel();
        instance.lb.stop();
        // 先关断全部存量连接，唤醒阻塞在读上的转发任务（含 Linux splice 线程）
        instance.registry.shutdown_all();

        // 句柄取出后立刻归还锁，await 不持有 MutexGuard（保证 stop future 为 Send）
        let stuck = {
            let mut handles = {
                let mut lock = instance.handles.lock();
                std::mem::take(&mut *lock)
            };
            await_handles(&mut handles, HANDLE_STOP_TIMEOUT).await
        };
        if stuck > 0 {
            push_log("WARN", &format!("停止超时，{} 个任务未自行退出，已强制终止", stuck));
        }

        *self.phase.lock() = Phase::Stopped;

        push_log("INFO", "服务已停止");

        Ok(())
    }

    pub fn get_loadbalancer(&self) -> Option<Arc<LoadBalancer>> {
        match &*self.phase.lock() {
            Phase::Running(inst) => Some(inst.lb.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 覆盖层与 CLI（args.rs）必须落在同一套界内，防止入口规则分裂
    #[test]
    fn overrides_clamp_to_cli_bounds() {
        let mut config = ServiceConfig::default();
        config.apply_overrides(&ConfigOverrides {
            delay_limit: Some(0),
            tlr: Some(5.0),
            ips: Some(0),
            threads: Some(100000),
            tls_port: Some(0),
            http_port: Some(0),
            max_sticky_slots: Some(0),
            ..Default::default()
        });
        assert_eq!(config.delay_limit, 1);
        assert_eq!(config.tlr, 1.0);
        assert_eq!(config.ips, 1);
        assert_eq!(config.threads, 1024);
        assert_eq!(config.tls_port, 1);
        assert_eq!(config.http_port, 1);
        assert_eq!(config.max_sticky_slots, 1);

        config.apply_overrides(&ConfigOverrides {
            delay_limit: Some(999999),
            ips: Some(999999),
            max_sticky_slots: Some(999999),
            ..Default::default()
        });
        assert_eq!(config.delay_limit, 2000);
        assert_eq!(config.ips, 128);
        assert_eq!(config.max_sticky_slots, 32);
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
    }

    /// 写入临时 IP 文件并返回配置好该文件的服务；测试结束后删除文件
    fn service_with_ip_file(tag: &str) -> (Arc<ServiceState>, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("cfnat_test_ips_{}_{}.txt", tag, std::process::id()));
        std::fs::write(&path, "1.2.3.4\n5.6.7.8\n").unwrap();

        let service = Arc::new(ServiceState::new());
        let mut config = service.get_config();
        config.ip_file = path.to_string_lossy().into_owned();
        config.http = "http://127.0.0.1:9/cdn-cgi/trace".to_string();
        config.addr = "127.0.0.1:0".parse().unwrap();
        service.update_config(config);
        (service, path)
    }

    #[test]
    fn concurrent_start_admits_single_instance() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("conc_start");
        let handle = rt.handle().clone();

        let results: Vec<_> = (0..8).map(|_| {
            let service = service.clone();
            let handle = handle.clone();
            std::thread::spawn(move || {
                let _guard = handle.enter();
                service.start()
            })
        }).collect::<Vec<_>>().into_iter().map(|j| j.join().unwrap()).collect();

        let ok = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(ok, 1, "8 个并发 start 应恰有 1 个成功，实际: {:?}", results);
        assert!(service.is_running());

        rt.block_on(service.stop()).unwrap();
        assert!(!service.is_running());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn stop_then_start_recovers() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("restart");

        // Stopped 时 stop 得到明确错误
        assert_eq!(rt.block_on(service.stop()).unwrap_err(), "服务未运行");

        let _guard = rt.handle().enter();
        service.start().unwrap();
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        assert!(!service.is_running());

        // stop 完成后可以再次启动，实例无残留
        service.start().unwrap();
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn second_start_while_running_is_rejected() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("double_start");

        let _guard = rt.handle().enter();
        service.start().unwrap();
        assert_eq!(service.start().unwrap_err(), "服务已在运行");
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn starting_phase_rejects_start_and_stop_and_rolls_back() {
        let rt = test_runtime();
        let (service, path) = service_with_ip_file("starting_phase");

        let guard = service.begin_start().unwrap();
        assert_eq!(service.start().unwrap_err(), "服务正在启动");
        let _guard = rt.handle().enter();
        assert_eq!(rt.block_on(service.stop()).unwrap_err(), "服务正在启动");

        // 守卫 drop（模拟启动 panic / 提前返回）后相位回滚，可正常再次启动
        drop(guard);
        assert!(!service.is_running());
        let _guard2 = rt.handle().enter();
        service.start().unwrap();
        assert!(service.is_running());
        rt.block_on(service.stop()).unwrap();
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn await_handles_aborts_stuck_tasks() {
        let rt = test_runtime();
        let _guard = rt.handle().enter();

        let stuck = tokio::spawn(async { tokio::time::sleep(Duration::from_secs(60)).await });
        let quick = tokio::spawn(async {});
        let mut handles = vec![stuck, quick];

        let aborted = rt.block_on(await_handles(&mut handles, Duration::from_millis(50)));
        assert_eq!(aborted, 1, "只有超时的任务被强制终止");
    }
}