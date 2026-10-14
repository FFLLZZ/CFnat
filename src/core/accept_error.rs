use std::io;
use std::time::{Duration, Instant};

/// accept 失败之后采取的动作
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcceptAction {
    /// 只影响这一个已排队连接，或已排队的网络错误：退避后重试
    Retry,
    /// 资源暂时耗尽：更长退避后重试
    Backoff,
    /// 监听 socket 或进程状态已损坏，重试无意义：结束转发任务
    Fatal,
}

/// Retry 的退避起点
const RETRY_BASE: Duration = Duration::from_millis(20);
/// Backoff 的退避起点
const BACKOFF_BASE: Duration = Duration::from_millis(200);
/// 退避上限，避免资源持续耗尽时高速循环
const BACKOFF_MAX: Duration = Duration::from_millis(1000);
/// 退避翻倍次数上限
const BACKOFF_MAX_SHIFT: u32 = 5;

/// 连续失败持续到该时长即升级为故障；与退避档位使用的失败次数是两个独立度量
pub(crate) const ESCALATE_AFTER: Duration = Duration::from_secs(30);

/// 连续失败时段内允许的相邻失败最大间隔：超过它即视为上一段失败已结束。
/// 取最大退避（BACKOFF_MAX）的 5 倍，任何超过它的空档都说明失败流已中断
pub(crate) const FAILURE_IDLE_GAP: Duration = Duration::from_millis(5000);

/// accept 连续失败的计时：只有相邻失败间隔不超过 FAILURE_IDLE_GAP 才属同一段，
/// 一段失败持续到 ESCALATE_AFTER 即应升级为故障
#[derive(Default)]
pub(crate) struct FailureStreak {
    start: Option<Instant>,
    last: Option<Instant>,
}

impl FailureStreak {
    /// 记录一次失败，返回当前这一段失败已持续的时长。
    /// 与上次失败的间隔超过 FAILURE_IDLE_GAP 时重新起算
    pub(crate) fn record(&mut self, now: Instant) -> Duration {
        let continued = self
            .last
            .is_some_and(|prev| now.duration_since(prev) <= FAILURE_IDLE_GAP);
        let start = if continued { self.start.unwrap_or(now) } else { now };
        self.start = Some(start);
        self.last = Some(now);
        now.duration_since(start)
    }

    /// 记录一次成功接受：当前失败段结束，下次失败重新起算
    pub(crate) fn clear(&mut self) {
        self.start = None;
        self.last = None;
    }
}

/// 判定 accept 错误类别。输入取 `io::Error::raw_os_error()`：
/// Linux 为 errno，Windows 为 Winsock WSA 码。两者取值不重叠
/// （errno < 200，WSA 码 >= 10000），因此同一张表同时容纳两边，
/// 任一平台上都能覆盖另一平台的映射。
pub(crate) fn classify(err: &io::Error) -> AcceptAction {
    match err.raw_os_error() {
        Some(code) => classify_code(code),
        // 无 OS 错误码（自定义 io::Error 等）无法判断，按故障处理
        None => AcceptAction::Fatal,
    }
}

fn classify_code(code: i32) -> AcceptAction {
    match code {
        // Linux errno：accept(2) 要求"像 EAGAIN 一样重试"的已排队网络错误
        103     // ECONNABORTED
        | 71    // EPROTO
        | 100   // ENETDOWN
        | 92    // ENOPROTOOPT
        | 112   // EHOSTDOWN
        | 64    // ENONET
        | 113   // EHOSTUNREACH
        | 95    // EOPNOTSUPP
        | 101   // ENETUNREACH
        | 4     // EINTR：std 的 cvt_r 已重试，保留以防上层改动
        | 11    // EAGAIN/EWOULDBLOCK：tokio 的 async_io 已吞，保留同上
        => AcceptAction::Retry,
        24      // EMFILE
        | 23    // ENFILE
        | 105   // ENOBUFS
        | 12    // ENOMEM
        => AcceptAction::Backoff,
        9       // EBADF
        | 88    // ENOTSOCK
        | 22    // EINVAL
        | 1     // EPERM：防火墙拒绝，重试无意义
        => AcceptAction::Fatal,

        // Windows WSA：数值取自 std 自带的 Windows 绑定
        10053   // WSAECONNABORTED
        | 10054 // WSAECONNRESET
        | 10004 // WSAEINTR
        | 10035 // WSAEWOULDBLOCK
        | 10050 // WSAENETDOWN
        | 10051 // WSAENETUNREACH
        | 10052 // WSAENETRESET
        | 10065 // WSAEHOSTUNREACH
        | 10060 // WSAETIMEDOUT
        | 10045 // WSAEOPNOTSUPP
        | 10036 // WSAEINPROGRESS
        => AcceptAction::Retry,
        10024   // WSAEMFILE
        | 10055 // WSAENOBUFS
        => AcceptAction::Backoff,
        10038   // WSAENOTSOCK
        | 10022 // WSAEINVAL
        | 10009 // WSAEBADF
        | 10013 // WSAEACCES
        => AcceptAction::Fatal,

        // 未知错误码：由调用方带原始码记录后按故障处理
        _ => AcceptAction::Fatal,
    }
}

/// 第 `consecutive_failures` 次连续失败（从 0 起）之后的退避时长
pub(crate) fn backoff_delay(action: AcceptAction, consecutive_failures: u32) -> Duration {
    let base = match action {
        AcceptAction::Backoff => BACKOFF_BASE,
        _ => RETRY_BASE,
    };
    let shift = consecutive_failures.min(BACKOFF_MAX_SHIFT);
    base.saturating_mul(1 << shift).min(BACKOFF_MAX)
}

/// 判据与日志用的错误描述：带原始错误码与目标平台
pub(crate) fn describe(err: &io::Error) -> String {
    format!(
        "{}（raw_os_error={:?}，os={}）",
        err,
        err.raw_os_error(),
        std::env::consts::OS
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_transient_errors_are_retried() {
        for code in [103, 71, 100, 92, 112, 64, 113, 95, 101, 4, 11] {
            assert_eq!(classify_code(code), AcceptAction::Retry, "errno {code}");
        }
    }

    #[test]
    fn linux_resource_exhaustion_uses_backoff() {
        for code in [24, 23, 105, 12] {
            assert_eq!(classify_code(code), AcceptAction::Backoff, "errno {code}");
        }
    }

    #[test]
    fn linux_broken_listener_is_fatal() {
        for code in [9, 88, 22, 1] {
            assert_eq!(classify_code(code), AcceptAction::Fatal, "errno {code}");
        }
    }

    #[test]
    fn windows_transient_errors_are_retried() {
        for code in [10053, 10054, 10004, 10035, 10050, 10051, 10052, 10065, 10060, 10045, 10036] {
            assert_eq!(classify_code(code), AcceptAction::Retry, "WSA {code}");
        }
    }

    #[test]
    fn windows_resource_exhaustion_uses_backoff() {
        for code in [10024, 10055] {
            assert_eq!(classify_code(code), AcceptAction::Backoff, "WSA {code}");
        }
    }

    #[test]
    fn windows_broken_listener_is_fatal() {
        for code in [10038, 10022, 10009, 10013] {
            assert_eq!(classify_code(code), AcceptAction::Fatal, "WSA {code}");
        }
    }

    #[test]
    fn unknown_codes_are_fatal() {
        assert_eq!(classify_code(0), AcceptAction::Fatal);
        assert_eq!(classify_code(12345), AcceptAction::Fatal);
        assert_eq!(classify_code(-1), AcceptAction::Fatal);
    }

    #[test]
    fn retry_backoff_grows_then_stops_at_six_hundred_forty() {
        let seq: Vec<u64> = (0..7)
            .map(|n| backoff_delay(AcceptAction::Retry, n).as_millis() as u64)
            .collect();
        assert_eq!(seq, vec![20, 40, 80, 160, 320, 640, 640]);
    }

    #[test]
    fn resource_backoff_grows_then_caps_at_one_second() {
        let seq: Vec<u64> = (0..7)
            .map(|n| backoff_delay(AcceptAction::Backoff, n).as_millis() as u64)
            .collect();
        assert_eq!(seq, vec![200, 400, 800, 1000, 1000, 1000, 1000]);
    }

    #[test]
    fn classify_reads_raw_os_error() {
        assert_eq!(classify(&io::Error::from_raw_os_error(24)), AcceptAction::Backoff);
        assert_eq!(classify(&io::Error::from_raw_os_error(10024)), AcceptAction::Backoff);
        assert_eq!(classify(&io::Error::from_raw_os_error(103)), AcceptAction::Retry);
        assert_eq!(classify(&io::Error::from_raw_os_error(10054)), AcceptAction::Retry);
        assert_eq!(classify(&io::Error::other("x")), AcceptAction::Fatal);
    }

    #[test]
    fn describe_carries_raw_code_and_platform() {
        let text = describe(&io::Error::from_raw_os_error(12345));
        assert!(text.contains("12345"), "{text}");
        assert!(text.contains(std::env::consts::OS), "{text}");
    }

    #[test]
    fn failure_streak_escalates_within_idle_gap() {
        let t0 = Instant::now();
        let mut streak = FailureStreak::default();
        assert!(streak.record(t0) < ESCALATE_AFTER);

        let mut t = t0;
        let mut escalated_at = None;
        for second in 1..=40 {
            t += Duration::from_secs(1);
            if streak.record(t) >= ESCALATE_AFTER {
                escalated_at = Some(second);
                break;
            }
        }
        assert_eq!(escalated_at, Some(30), "1s 间隔的连续失败应在第 30 秒升级");
    }

    #[test]
    fn failure_streak_restarts_after_idle_gap() {
        let t0 = Instant::now();
        let mut streak = FailureStreak::default();
        streak.record(t0);

        // 空档超过阈值：视为新的一段，第一次孤立失败不升级
        let t1 = t0 + FAILURE_IDLE_GAP + Duration::from_secs(1);
        assert!(streak.record(t1) < ESCALATE_AFTER);
        // 再隔 31s 的孤立失败仍不升级
        let t2 = t1 + Duration::from_secs(31);
        assert!(streak.record(t2) < ESCALATE_AFTER);
    }

    #[test]
    fn failure_streak_clears_on_success() {
        let t0 = Instant::now();
        let mut streak = FailureStreak::default();
        streak.record(t0);
        let mut t = t0;
        for _ in 0..30 {
            t += Duration::from_secs(1);
            streak.record(t);
        }
        assert!(streak.record(t + Duration::from_secs(1)) >= ESCALATE_AFTER);

        streak.clear();
        assert!(streak.record(t + Duration::from_secs(2)) < ESCALATE_AFTER, "成功后应重新起算");
    }
}