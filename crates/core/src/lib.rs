//! core: 全系统共享领域模型：事件流 + 插件

pub mod clock;
pub mod event;
pub mod plugin;
pub mod types;

pub use clock::Clock;
pub use event::Event;
pub use types::{Exchange, Price, Qty, Side, Symbol, Timestamp};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_smoke() {
        let s = Symbol::new("BTCUSDT");
        assert_eq!(s.as_str(), "BTCUSDT");
        let t = Timestamp::from_millis(1_700_000_000_000);
        assert!(t.as_millis() > 0);
    }
}
