//! 逻辑时钟抽象：回测用事件时间、实盘用真实时间，策略代码不感知差异。

use crate::types::Timestamp;

/// 时钟 trait：策略/信号只通过它取「当前时间」。
///
/// - 回测：[`EventClock`]，时间 = 当前回放事件的时间戳（确定性，杜绝 wall-clock 影响）。
/// - 实盘：[`SystemClock`]，时间 = 系统 UTC 时间。
pub trait Clock: Send {
    fn now(&self) -> Timestamp;
}

/// 回测时钟：由回放引擎按事件推进
#[derive(Debug, Default)]
pub struct EventClock {
    now_ms: i64,
}

impl EventClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// 回放引擎处理每个事件前调用
    pub fn advance_to(&mut self, ts: Timestamp) {
        self.now_ms = ts.as_millis();
    }
}

impl Clock for EventClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.now_ms)
    }
}

// 实盘时钟
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_clock_advances() {
        let mut c = EventClock::new();
        assert_eq!(c.now().as_millis(), 0);
        c.advance_to(Timestamp::from_millis(123_456));
        assert_eq!(c.now().as_millis(), 123_456);
        c.advance_to(Timestamp::from_millis(200_000));
        assert_eq!(c.now().as_millis(), 200_000);
    }

    #[test]
    fn system_clock_is_reasonable() {
        let c = SystemClock;
        let now = c.now().as_millis();
        // 大于 2020-01-01 且小于一个遥远的未来， sanity check
        assert!(now > 1_577_836_800_000);
        assert!(now < 4_000_000_000_000);
    }
}
