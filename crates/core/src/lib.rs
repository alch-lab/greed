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

pub use clock::{Clock, EventClock, SystemClock};
pub use event::{BookSnapshot, Event, OiTick, Trade};
pub use plugin::{
    Ctx, ExitAction, ExitPlugin, FilterPlugin, OrderIntent, Position, Signal, SignalKind,
    SignalPlugin, TriggerPlugin, Verdict,
};
pub use types::{
    notional_usd, Exchange, Price, Qty, Side, Symbol, Timestamp, PRICE_SCALE, QTY_SCALE,
};

#[cfg(test)]
mod tests {
    use super::*;

    /// 冒烟测试：核心类型可用、crate 可链接。
    #[test]
    fn core_smoke() {
        let s = Symbol::new("BTCUSDT");
        assert_eq!(s.as_str(), "BTCUSDT");
        let p = Price::from_f64(67000.0);
        let q = Qty::from_f64(0.5);
        assert!(notional_usd(p, q) > 0.0);
        let t = Timestamp::from_millis(1_700_000_000_000);
        assert!(t.as_millis() > 0);
    }
}
