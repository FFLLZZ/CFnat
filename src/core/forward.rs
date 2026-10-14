use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::io::{self};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::core::accept_error::{self, AcceptAction};
use crate::core::backend::Backend;
use crate::core::cancel::CancellationToken;
use crate::core::loadbalancer::LoadBalancer;
use crate::log::push_log;

const READABLE_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const PEEK_TIMEOUT: Duration = Duration::from_secs(5);
const FAST_FAIL_THRESHOLD: Duration = Duration::from_millis(3000);
const FAST_FAIL_LIMIT: u32 = 2;

/// 复制一个独立描述符供登记表持有。tokio 没有 TcpStream::try_clone（dup 套接字
/// 的反应器注册有歧义），改用 std 的 fd/socket 复制：得到的流只用于 shutdown，
/// 不做任何读写，不经过 tokio 反应器。
fn duplicate(stream: &TcpStream) -> io::Result<std::net::TcpStream> {
    #[cfg(unix)]
    {
        use std::os::fd::{AsRawFd, BorrowedFd};
        // SAFETY: stream 在调用期间存活，其原始描述符有效
        let borrowed = unsafe { BorrowedFd::borrow_raw(stream.as_raw_fd()) };
        Ok(std::net::TcpStream::from(borrowed.try_clone_to_owned()?))
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::{AsRawSocket, BorrowedSocket};
        // SAFETY: 同上，原始 socket 句柄有效
        let borrowed = unsafe { BorrowedSocket::borrow_raw(stream.as_raw_socket()) };
        Ok(std::net::TcpStream::from(borrowed.try_clone_to_owned()?))
    }
}

struct ConnectionEntry {
    client: Option<std::net::TcpStream>,
    server: Option<std::net::TcpStream>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// 存量连接登记表：保存客户端/服务端套接字的克隆与任务句柄。
/// stop 时统一 shutdown，以"关套接字"作为 async 任务与 spawn_blocking 线程
/// 共用的取消机制——spawn_blocking 无法 abort，只能靠唤醒其阻塞的系统调用。
pub struct ConnectionRegistry {
    entries: Mutex<HashMap<u64, ConnectionEntry>>,
    next_id: AtomicU64,
    active: AtomicUsize,
    /// 与 entries 锁配对使用：登记成功后若已进入关断，句柄立刻 abort
    shutting_down: AtomicBool,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            active: AtomicUsize::new(0),
            shutting_down: AtomicBool::new(false),
        }
    }

    /// 登记新连接并返回 id；描述符复制失败（fd 耗尽）返回 None，调用方应直接断开
    fn register(&self, client: &TcpStream) -> Option<u64> {
        let client = duplicate(client).ok()?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.entries.lock().insert(
            id,
            ConnectionEntry {
                client: Some(client),
                server: None,
                handle: Mutex::new(None),
            },
        );
        self.active.fetch_add(1, Ordering::Relaxed);
        Some(id)
    }

    fn attach_server(&self, id: u64, server: &TcpStream) {
        let Ok(server) = duplicate(server) else { return };
        if let Some(entry) = self.entries.lock().get_mut(&id) {
            entry.server = Some(server);
        }
    }

    /// 任务可能已自行退出摘除条目，此时句柄随条目一并丢弃；
    /// 与 shutdown_all 同持 entries 锁，避免"登记完成但句柄未落表"的窗口漏掉 abort
    fn set_handle(&self, id: u64, handle: JoinHandle<()>) {
        let entries = self.entries.lock();
        let Some(entry) = entries.get(&id) else {
            handle.abort();
            return;
        };
        if self.shutting_down.load(Ordering::Acquire) {
            drop(entries);
            handle.abort();
            return;
        }
        *entry.handle.lock() = Some(handle);
    }

    fn unregister(&self, id: u64) {
        if self.entries.lock().remove(&id).is_some() {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn active_count(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    /// 停止全部存量连接。shutdown 作用于底层连接本身：Linux 上 dup 共享文件
    /// 描述与 epoll 注册，能唤醒 poll 与 splice 的阻塞系统调用；Windows 上
    /// WSADuplicateSocket 的事件关联按句柄分离，反应器感知不到复制句柄的
    /// shutdown，因此 async 任务需额外 abort（spawn_blocking 仅存在于 Linux）。
    pub fn shutdown_all(&self) {
        // 先置位再取锁：此后任何 set_handle 都自行 abort，未及落表的句柄不会漏掉
        self.shutting_down.store(true, Ordering::Release);
        // 锁内只做所有权转移（O(1)、无系统调用），shutdown 与 abort 都放到锁外，
        // 临界区长度不随连接数增长
        let (sockets, handles): (Vec<_>, Vec<_>) = {
            let mut entries = self.entries.lock();
            let mut sockets = Vec::with_capacity(entries.len());
            let mut handles = Vec::with_capacity(entries.len());
            for entry in entries.values_mut() {
                sockets.push((entry.client.take(), entry.server.take()));
                if let Some(handle) = entry.handle.lock().take() {
                    handles.push(handle);
                }
            }
            (sockets, handles)
        };
        for (client, server) in sockets {
            if let Some(client) = client {
                let _ = client.shutdown(std::net::Shutdown::Both);
            }
            if let Some(server) = server {
                let _ = server.shutdown(std::net::Shutdown::Both);
            }
        }
        for handle in handles {
            handle.abort();
        }
    }
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// 任务退出（含 panic）时自动把连接从登记表摘除
struct RegistryGuard {
    registry: Arc<ConnectionRegistry>,
    id: u64,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        self.registry.unregister(self.id);
    }
}

#[inline]
fn is_tls(first_byte: u8) -> bool {
    first_byte == 0x16
}

async fn transfer_direction(
    reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    metrics: Option<(Arc<LoadBalancer>, Arc<Backend>, Instant)>,
) -> io::Result<()> {
    tokio::time::timeout(READABLE_TIMEOUT, reader.readable())
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;

    if let Some((lb, backend, start)) = metrics {
        lb.record(&backend, Some(start.elapsed().as_secs_f32() * 1000.0), 0.0);
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let rfd = reader.as_ref().as_raw_fd();
        let wfd = writer.as_ref().as_raw_fd();
        tokio::task::spawn_blocking(move || {
            let _reader = reader;
            let _writer = writer;
            crate::core::splice::transfer(rfd, wfd)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))??
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut reader = reader;
        let mut writer = writer;
        io::copy(&mut reader, &mut writer).await?;
    }

    Ok(())
}

async fn race_connect(
    backends: &[Arc<Backend>],
    port: u16,
    lb: &Arc<LoadBalancer>,
) -> Option<(Arc<Backend>, TcpStream)> {
    let mut set: tokio::task::JoinSet<std::io::Result<(Arc<Backend>, Option<TcpStream>)>> =
        tokio::task::JoinSet::new();
    for b in backends {
        let b = b.clone();
        let addr = SocketAddr::new(b.addr.ip(), port);
        set.spawn(async move {
            match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    #[allow(deprecated)]
                    let _ = stream.set_linger(Some(std::time::Duration::ZERO));
                    Ok((b, Some(stream)))
                },
                _ => Ok((b, None)),
            }
        });
    }

    let mut winner = None;
    let mut failed: Vec<Arc<Backend>> = Vec::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok((b, Some(stream)))) => {
                winner = Some((b, stream));
                break;
            }
            Ok(Ok((b, None))) => failed.push(b),
            _ => {}
        }
    }
    set.shutdown().await;

    if winner.is_none() {
        for b in &failed {
            lb.record(b, None, 1.0);
        }
    }
    winner
}

/// 胜出后端的配额守卫：析构时归还连接计数，
/// nodelay 等设置失败的提前返回路径同样成对释放
struct BackendGuard {
    lb: Arc<LoadBalancer>,
    backend: Arc<Backend>,
}

impl Drop for BackendGuard {
    fn drop(&mut self) {
        self.lb.check_and_evict(&self.backend);
        self.lb.release(&self.backend);
    }
}

async fn handle_client(
    client: TcpStream,
    lb: Arc<LoadBalancer>,
    tls_port: u16,
    http_port: u16,
    warned: Arc<AtomicBool>,
    conn_id: u64,
    registry: Arc<ConnectionRegistry>,
) -> io::Result<()> {
    let connect_start = Instant::now();

    let target_port = if tls_port == http_port {
        tls_port
    } else {
        let mut buf = [0u8; 1];
        let tls = match tokio::time::timeout(PEEK_TIMEOUT, client.peek(&mut buf)).await {
            Ok(Ok(n)) => {
                if n == 0 {
                    return Ok(());
                }
                is_tls(buf[0])
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(io::Error::from(io::ErrorKind::TimedOut)),
        };
        if tls { tls_port } else { http_port }
    };

    let mut backends: Vec<Arc<Backend>> = Vec::with_capacity(2);
    while backends.len() < 2 {
        let Some(b) = lb.select() else { break };
        if backends.iter().any(|x| Arc::ptr_eq(x, &b)) {
            lb.release(&b);
            break;
        }
        backends.push(b);
    }

    if backends.is_empty() {
        if !warned.swap(true, Ordering::Relaxed) {
            push_log("WARN", "有客户端连接但无可用后端");
        }
        return Err(io::ErrorKind::NotConnected.into());
    }
    warned.store(false, Ordering::Relaxed);

    let winner = race_connect(&backends, target_port, &lb).await;

    let (backend, server) = match winner {
        Some(w) => w,
        None => {
            for b in &backends {
                lb.check_and_evict(b);
                lb.release(b);
            }
            return Err(io::Error::from(io::ErrorKind::NotConnected));
        }
    };

    for b in &backends {
        if !Arc::ptr_eq(b, &backend) {
            lb.release(b);
        }
    }

    let backend_guard = BackendGuard { lb: lb.clone(), backend: backend.clone() };

    server.set_nodelay(true)?;
    client.set_nodelay(true)?;
    registry.attach_server(conn_id, &server);
    #[allow(deprecated)]
    let _ = server.set_linger(Some(std::time::Duration::ZERO));
    #[allow(deprecated)]
    let _ = client.set_linger(Some(std::time::Duration::ZERO));

    let start = Instant::now();
    let (cr, cw) = client.into_split();
    let (sr, sw) = server.into_split();

    // 内联 join! 而非再 spawn：任务树保持单层，连接任务被 abort 时两个传输
    // 方向随之丢弃，不产生脱离控制的残留任务
    let (r1, r2) = tokio::join!(
        transfer_direction(cr, sw, None),
        transfer_direction(sr, cw, Some((lb.clone(), backend.clone(), start))),
    );

    let result = match (r1, r2) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(e), _) | (_, Err(e)) => Err(e),
    };

    // 先归还配额再记快败统计
    drop(backend_guard);

    let lifetime = connect_start.elapsed();
    if result.is_err() && lifetime < FAST_FAIL_THRESHOLD {
        backend.record_fast_fail();
        if backend.get_fast_fail_count() >= FAST_FAIL_LIMIT
            && lb.try_deactivate()
        {
            push_log("WARN", &format!("[-] {} 连续快速断开，移除", backend.addr));
            backend.mark_removed();
        }
    } else if result.is_ok() {
        backend.reset_fast_fail();
    }

    result
}

pub async fn run_forward(
    listener: TcpListener,
    lb: Arc<LoadBalancer>,
    tls_port: u16,
    http_port: u16,
    cancel_token: CancellationToken,
    registry: Arc<ConnectionRegistry>,
) -> io::Result<()> {
    let local_addr = listener.local_addr()?;
    let warned = Arc::new(AtomicBool::new(false));
    push_log("INFO", &format!("转发服务 {} (TLS:{}, HTTP:{})", local_addr, tls_port, http_port));

    // 连续 accept 失败：退避档位与故障升级共用同一个恢复窗口
    let mut failure_streak = accept_error::FailureStreak::default();

    loop {
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((client, _)) => {
                        failure_streak.clear();
                        // 克隆失败（fd 耗尽）的连接无法纳入 stop 回收，直接断开
                        let Some(conn_id) = registry.register(&client) else {
                            drop(client);
                            continue;
                        };
                        let lb = lb.clone();
                        let warned = warned.clone();
                        let registry = registry.clone();
                        let task_registry = registry.clone();
                        // 守卫随 future 一并移动：任务若在首次轮询前就被 abort，
                        // future 被丢弃时守卫随之析构，登记不会残留
                        let guard = RegistryGuard { registry: registry.clone(), id: conn_id };
                        let handle = tokio::spawn(async move {
                            let _guard = guard;
                            let _ = handle_client(client, lb, tls_port, http_port, warned, conn_id, task_registry).await;
                        });
                        registry.set_handle(conn_id, handle);
                    }
                    Err(e) => {
                        match accept_error::classify(&e) {
                            // 交回原始错误：describe 只在日志与故障出口调用一次，
                            // 重建 io::Error 会丢掉 raw_os_error
                            AcceptAction::Fatal => return Err(e),
                            action => {
                                let sample = failure_streak.record(Instant::now());
                                if sample.elapsed >= accept_error::ESCALATE_AFTER {
                                    push_log("ERROR", &format!(
                                        "accept 连续失败超过 {:?}，放弃转发",
                                        accept_error::ESCALATE_AFTER
                                    ));
                                    return Err(e);
                                }
                                let delay = accept_error::backoff_delay(action, sample.prior);
                                // 退避期间仍响应停止信号
                                if tokio::time::timeout(delay, cancel_token.cancelled()).await.is_ok() {
                                    push_log("INFO", "[转发服务] 收到停止信号，退出");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                push_log("INFO", "[转发服务] 收到停止信号，退出");
                break;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    async fn wait_until(f: impl Fn() -> bool, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if f() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        f()
    }

    /// 接受连接后一直持有不关的 mock 后端
    async fn holding_backend() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                if let Ok((stream, _)) = listener.accept().await {
                    held.push(stream);
                }
            }
        });
        addr
    }

    async fn spawn_forward(backend: SocketAddr) -> (SocketAddr, Arc<ConnectionRegistry>, CancellationToken) {
        let lb = Arc::new(LoadBalancer::new(1));
        lb.add_to_primary(backend, 0.0, 0.0, None);
        let registry = Arc::new(ConnectionRegistry::new());
        let token = CancellationToken::new();

        // 绑定由调用方完成：监听地址可直接取自 listener，无需先探测再释放
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = backend.port();
        tokio::spawn({
            let registry = registry.clone();
            let token = token.clone();
            async move {
                run_forward(listener, lb, port, port, token, registry)
                    .await
                    .unwrap();
            }
        });
        (addr, registry, token)
    }

    /// 连接是否已被对端关闭（EOF 或错误）
    async fn read_closed(client: &mut TcpStream) -> bool {
        let mut buf = [0u8; 1];
        matches!(
            tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf)).await,
            Ok(Ok(0)) | Ok(Err(_))
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stop_shuts_down_live_connections() {
        let backend = holding_backend().await;
        let (addr, registry, token) = spawn_forward(backend).await;

        let mut clients = Vec::new();
        for _ in 0..5 {
            clients.push(TcpStream::connect(addr).await.unwrap());
        }
        assert!(wait_until(|| registry.active_count() == 5, Duration::from_secs(2)).await);

        registry.shutdown_all();
        assert!(wait_until(|| registry.active_count() == 0, Duration::from_secs(3)).await);

        for client in clients.iter_mut() {
            assert!(read_closed(client).await, "客户端应感知连接被关断");
        }
        token.cancel();
    }

    // 对端断开后连接最迟在 READABLE_TIMEOUT(10s) 内被回收
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn peer_disconnect_reclaims_entry() {
        let backend = holding_backend().await;
        let (addr, registry, token) = spawn_forward(backend).await;

        let client = TcpStream::connect(addr).await.unwrap();
        assert!(wait_until(|| registry.active_count() == 1, Duration::from_secs(2)).await);
        drop(client);
        assert!(wait_until(|| registry.active_count() == 0, Duration::from_secs(12)).await);
        token.cancel();
    }

    /// 配额守卫析构时归还连接计数，设置失败的提前返回不会泄漏配额
    #[test]
    fn backend_guard_releases_quota_on_drop() {
        let lb = Arc::new(LoadBalancer::new(1));
        let addr: SocketAddr = "127.0.0.1:80".parse().unwrap();
        lb.add_to_primary(addr, 0.0, 0.0, None);

        let backend = lb.select().expect("池非空应可选");
        assert_eq!(backend.connections(), 1);

        {
            let _guard = BackendGuard { lb: lb.clone(), backend: backend.clone() };
            assert_eq!(backend.connections(), 1, "守卫存续期间配额保持占用");
        }
        assert_eq!(backend.connections(), 0, "守卫析构后配额归还");
    }
}