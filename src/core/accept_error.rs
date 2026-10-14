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

/// 连续失败持续到该时长即升级为故障
pub(crate) const ESCALATE_AFTER: Duration = Duration::from_secs(30);

/// 连续失败的恢复窗口：相邻两次失败的间隔超过它即视为上一段失败已结束，
/// 段内计数与持续时长同时归零。上限独立于 BACKOFF_MAX（退避上限），
/// 本身不再随时间增长
pub(crate) const FAILURE_RECOVERY_WINDOW: Duration = Duration::from_millis(5000);

/// 一次失败在"连续失败段"内所处的位置。段由 FAILURE_RECOVERY_WINDOW 界定
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FailureSample {
    /// 本段从首次失败到本次的持续时长
    pub elapsed: Duration,
    /// 本次之前本段已发生的失败次数，退避档位用它（从 0 起）
    pub prior: u32,
}

/// accept 连续失败的计时：相邻失败间隔不超过 FAILURE_RECOVERY_WINDOW 才属同一段
#[derive(Default)]
pub(crate) struct FailureStreak {
    start: Option<Instant>,
    last: Option<Instant>,
    count: u32,
}

impl FailureStreak {
    /// 记录一次失败。与上次失败的间隔超过 FAILURE_RECOVERY_WINDOW 时开启新的一段，
    /// 计数与起算点一并重置，退避档位回到起点
    pub(crate) fn record(&mut self, now: Instant) -> FailureSample {
        let continued = self
            .last
            .is_some_and(|prev| now.duration_since(prev) <= FAILURE_RECOVERY_WINDOW);
        if continued {
            let elapsed = now.duration_since(self.start.unwrap_or(now));
            let prior = self.count;
            self.last = Some(now);
            self.count = self.count.saturating_add(1);
            FailureSample { elapsed, prior }
        } else {
            self.start = Some(now);
            self.last = Some(now);
            self.count = 1;
            FailureSample { elapsed: Duration::ZERO, prior: 0 }
        }
    }

    /// 记录一次成功接受：当前失败段结束，下次失败重新起算
    pub(crate) fn clear(&mut self) {
        self.start = None;
        self.last = None;
        self.count = 0;
    }
}

/// 判定 accept 错误类别。语义分类走 io::ErrorKind：std 按目标平台把系统错误码
/// 映射到 ErrorKind，各平台取值差异（如 ECONNABORTED 在 macOS 为 53、Linux 为 103）
/// 由 std 处理，本函数不持有任何手写数值表。std 未归类到具体 ErrorKind 的错误
/// （主要是 fd / 内核缓冲耗尽）由平台专属原始码补充判定
pub(crate) fn classify(err: &io::Error) -> AcceptAction {
    match err.kind() {
        // 只影响等待中的那个连接，或已排队的网络错误：退避后重试
        io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::Interrupted
        | io::ErrorKind::WouldBlock
        | io::ErrorKind::TimedOut
        | io::ErrorKind::NotConnected
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::HostUnreachable
        | io::ErrorKind::NetworkUnreachable
        | io::ErrorKind::NetworkDown => AcceptAction::Retry,
        // 内存不足：需要更长退避
        io::ErrorKind::OutOfMemory => AcceptAction::Backoff,
        // 监听 socket 或进程状态已损坏，重试无意义
        io::ErrorKind::PermissionDenied
        | io::ErrorKind::InvalidInput
        | io::ErrorKind::AddrInUse
        | io::ErrorKind::AddrNotAvailable => AcceptAction::Fatal,
        // ErrorKind 未给出可区分语义的错误（std 归入 Uncategorized，或
        // Unsupported 同时覆盖 EOPNOTSUPP 与 ENOSYS 两种相反语义）：
        // 按平台原始码补充；无原始码或无法识别的一律按故障处理
        _ => match err.raw_os_error() {
            Some(code) => classify_raw(code),
            None => AcceptAction::Fatal,
        },
    }
}

/// ErrorKind 无法区分的错误按平台原始码判定
#[cfg(target_os = "linux")]
fn classify_raw(code: i32) -> AcceptAction {
    match code {
        // accept(2)：内核把已排队连接上的网络错误直接透传，要求按 EAGAIN 重试
        71 /* EPROTO */ | 92 /* ENOPROTOOPT */ | 112 /* EHOSTDOWN */
        | 64 /* ENONET */ | 95 /* EOPNOTSUPP */ => AcceptAction::Retry,
        // fd / 内核缓冲耗尽
        24 /* EMFILE */ | 23 /* ENFILE */ | 105 /* ENOBUFS */ => AcceptAction::Backoff,
        _ => AcceptAction::Fatal,
    }
}

/// ErrorKind 无法区分的错误按平台原始码判定。
/// BSD 系不透传已排队连接的网络错误，因此这里只需补 fd / 缓冲耗尽
#[cfg(target_vendor = "apple")]
fn classify_raw(code: i32) -> AcceptAction {
    match code {
        24 /* EMFILE */ | 23 /* ENFILE */ | 55 /* ENOBUFS */ => AcceptAction::Backoff,
        _ => AcceptAction::Fatal,
    }
}

/// ErrorKind 无法区分的错误按平台原始码判定
#[cfg(target_os = "windows")]
fn classify_raw(code: i32) -> AcceptAction {
    match code {
        10004 /* WSAEINTR */ | 10036 /* WSAEINPROGRESS */
        | 10045 /* WSAEOPNOTSUPP */ | 10052 /* WSAENETRESET */ => AcceptAction::Retry,
        10024 /* WSAEMFILE */ | 10055 /* WSAENOBUFS */ => AcceptAction::Backoff,
        _ => AcceptAction::Fatal,
    }
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple", target_os = "windows")))]
fn classify_raw(_code: i32) -> AcceptAction {
    AcceptAction::Fatal
}

/// 本段内第 `prior_failures`（从 0 起）次失败之后的退避时长
pub(crate) fn backoff_delay(action: AcceptAction, prior_failures: u32) -> Duration {
    let base = match action {
        AcceptAction::Backoff => BACKOFF_BASE,
        _ => RETRY_BASE,
    };
    let shift = prior_failures.min(BACKOFF_MAX_SHIFT);
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
    fn transient_kinds_are_retried() {
        for kind in [
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::Interrupted,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::TimedOut,
            io::ErrorKind::NotConnected,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::HostUnreachable,
            io::ErrorKind::NetworkUnreachable,
            io::ErrorKind::NetworkDown,
        ] {
            assert_eq!(classify(&io::Error::new(kind, "x")), AcceptAction::Retry, "{kind:?}");
        }
    }

    #[test]
    fn broken_listener_kinds_are_fatal() {
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::AddrInUse,
            io::ErrorKind::AddrNotAvailable,
        ] {
            assert_eq!(classify(&io::Error::new(kind, "x")), AcceptAction::Fatal, "{kind:?}");
        }
    }

    #[test]
    fn out_of_memory_uses_backoff() {
        assert_eq!(
            classify(&io::Error::new(io::ErrorKind::OutOfMemory, "x")),
            AcceptAction::Backoff
        );
    }

    #[test]
    fn unresolvable_errors_are_fatal() {
        // 0 与 12345 在 std 的映射里都落到 Uncategorized，且不属于任何补充表
        assert_eq!(classify(&io::Error::from_raw_os_error(0)), AcceptAction::Fatal);
        assert_eq!(classify(&io::Error::from_raw_os_error(12345)), AcceptAction::Fatal);
        // 无原始码的自定义错误无法判断，同样按故障处理
        assert_eq!(classify(&io::Error::other("x")), AcceptAction::Fatal);
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

    /// 平台补充表只在本平台可验证：数值本身就带平台语义
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_raw_supplements_follow_accept_man_page() {
        // 内核透传的已排队网络错误，按 EAGAIN 处理
        for code in [71, 92, 112, 64, 95] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Retry, "errno {code}");
        }
        for code in [24, 23, 105] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Backoff, "errno {code}");
        }
        // EBADF / ENOTSOCK 不被 std 归类，ENOSYS 与 EOPNOTSUPP 共享 Unsupported
        for code in [9, 88, 38] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Fatal, "errno {code}");
        }
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn apple_raw_supplements_are_exhaustion_only() {
        for code in [24, 23, 55] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Backoff, "errno {code}");
        }
        // EBADF / ENOTSOCK / EOPNOTSUPP：BSD 不透传，按故障处理
        for code in [9, 38, 102] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Fatal, "errno {code}");
        }
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_raw_supplements_cover_unmapped_wsa() {
        for code in [10004, 10036, 10045, 10052] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Retry, "WSA {code}");
        }
        for code in [10024, 10055] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Backoff, "WSA {code}");
        }
        for code in [10009, 10038] {
            assert_eq!(classify(&io::Error::from_raw_os_error(code)), AcceptAction::Fatal, "WSA {code}");
        }
    }

    #[test]
    fn describe_carries_raw_code_and_platform() {
        let text = describe(&io::Error::from_raw_os_error(12345));
        assert!(text.contains("12345"), "{text}");
        assert!(text.contains(std::env::consts::OS), "{text}");
    }

    #[test]
    fn failure_streak_escalates_within_recovery_window() {
        let t0 = Instant::now();
        let mut streak = FailureStreak::default();
        assert!(streak.record(t0).elapsed < ESCALATE_AFTER);

        let mut t = t0;
        let mut escalated_at = None;
        for second in 1..=40 {
            t += Duration::from_secs(1);
            if streak.record(t).elapsed >= ESCALATE_AFTER {
                escalated_at = Some(second);
                break;
            }
        }
        assert_eq!(escalated_at, Some(30), "1s 间隔的连续失败应在第 30 秒升级");
    }

    #[test]
    fn failure_streak_restarts_after_recovery_window() {
        let t0 = Instant::now();
        let mut streak = FailureStreak::default();
        streak.record(t0);

        // 空档超过窗口：视为新的一段，第一次孤立失败不升级
        let t1 = t0 + FAILURE_RECOVERY_WINDOW + Duration::from_secs(1);
        assert!(streak.record(t1).elapsed < ESCALATE_AFTER);
        // 再隔 31s 的孤立失败仍不升级
        let t2 = t1 + Duration::from_secs(31);
        assert!(streak.record(t2).elapsed < ESCALATE_AFTER);
    }

    #[test]
    fn failure_count_resets_outside_recovery_window() {
        let t0 = Instant::now();
        let mut streak = FailureStreak::default();
        assert_eq!(streak.record(t0).prior, 0);
        assert_eq!(streak.record(t0 + Duration::from_secs(1)).prior, 1);
        assert_eq!(streak.record(t0 + Duration::from_secs(2)).prior, 2);

        // 空档超过窗口：计数与持续时长同时回到起点，退避档位不再继承
        let after_gap =
            streak.record(t0 + Duration::from_secs(2) + FAILURE_RECOVERY_WINDOW + Duration::from_millis(1));
        assert_eq!(after_gap.prior, 0);
        assert_eq!(after_gap.elapsed, Duration::ZERO);
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
        assert!(streak.record(t + Duration::from_secs(1)).elapsed >= ESCALATE_AFTER);

        streak.clear();
        let after_success = streak.record(t + Duration::from_secs(2));
        assert!(after_success.elapsed < ESCALATE_AFTER, "成功后应重新起算");
        assert_eq!(after_success.prior, 0, "成功后退避档位应从起点开始");
    }
}