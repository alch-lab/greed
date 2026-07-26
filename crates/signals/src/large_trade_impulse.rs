//! 大单冲击信号：单笔大单即时检测。
//!
//! 规则：当单笔成交名义额 ≥ threshold_usd 时，立即发出 Impulse 信号，
//! payload 中包含价格、数量、方向（taker side）。
//! 带冷却：同一方向连续大单在 cooldown_ms 内只发一次信号。

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

/// 大单冲击信号插件。
pub struct LargeTradeImpulse {
    /// 大单阈值（USD）
    pub threshold_usd: f64,
    /// 冷却时间（毫秒）：同方向大单在此时间内不重复发信号
    pub cooldown_ms: i64,
    /// 上次发信号时间（按方向）
    last_buy_ts: Option<i64>,
    last_sell_ts: Option<i64>,
}

impl LargeTradeImpulse {
    pub fn new(threshold_usd: f64, cooldown_ms: i64) -> Self {
        Self {
            threshold_usd,
            cooldown_ms,
            last_buy_ts: None,
            last_sell_ts: None,
        }
    }

    fn on_trade(&mut self, t: &tcore::Trade) -> Option<Signal> {
        let ts = t.ts.as_millis();
        let notional = t.notional();
        if notional < self.threshold_usd {
            return None;
        }

        // taker side: is_buyer_maker=true → seller is taker (sell)
        //             is_buyer_maker=false → buyer is taker (buy)
        let is_buy = !t.is_buyer_maker;

        // 冷却检查
        if is_buy {
            if self.last_buy_ts.is_some_and(|last| ts - last < self.cooldown_ms) {
                return None;
            }
            self.last_buy_ts = Some(ts);
        } else {
            if self.last_sell_ts.is_some_and(|last| ts - last < self.cooldown_ms) {
                return None;
            }
            self.last_sell_ts = Some(ts);
        }

        let side_str = if is_buy { "buy" } else { "sell" };

        Some(Signal::new(
            SignalKind::FlowSurge,
            t.ts,
            self.name(),
            serde_json::json!({
                "price": t.price.to_f64(),
                "qty": t.qty.to_f64(),
                "notional": notional,
                "taker_side": side_str,
                "is_buyer_maker": t.is_buyer_maker,
            }),
        ))
    }
}

impl SignalPlugin for LargeTradeImpulse {
    fn name(&self) -> &'static str {
        "LargeTradeImpulse"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self.on_trade(t).into_iter().collect(),
            _ => Vec::new(),
        }
    }
}

impl LargeTradeImpulse {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self::new(
            g("threshold_usd", 100_000.0),
            g("cooldown_ms", 1_000.0) as i64,
        )
    }
}
