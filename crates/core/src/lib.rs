//! core: 全系统共享领域模型：事件流 + 插件
//!
//! - [`types`]：定点价格/数量（i64）、Symbol、Timestamp、Side、Exchange。
//! - [`event`]：Trade / BookSnapshot / OiTick / Event，回测与实盘共用。
//! - [`clock`]：逻辑时钟抽象（EventClock / SystemClock）。
//! - [`plugin`]：四大插件 trait

pub mod clock;
pub mod event;
pub mod plugin;
pub mod types;

pub use clock::Clock;
pub use event::Event;
pub use types::{Exchange, Price, Qty, Side, Symbol, Timestamp, PRICE_SCALE, QTY_SCALE};

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
