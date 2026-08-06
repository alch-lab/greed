//! 唯一生产入场扳机：全市场数据完整的订单流力竭确认。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::{Ctx, OrderIntent, Price, Qty, Side, Signal, Symbol, TriggerPlugin};

pub struct OrderFlowEntry {
    risk_scale: f64,
    tp1_r: f64,
    min_stop_pct: f64,
    max_stop_pct: f64,
    market_entry: bool,
}

impl OrderFlowEntry {
    fn intent(&self, signal: &Signal, symbol: &Symbol) -> Option<OrderIntent> {
        if signal.source != "OrderFlowExhaustion"
            || signal.payload.get("stage")?.as_str()? != "confirmed"
        {
            return None;
        }
        let p = &signal.payload;
        if p.get("trade_eligible").and_then(Json::as_bool) != Some(true) {
            return None;
        }

        let side = match p.get("side")?.as_str()? {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => return None,
        };
        let signal_price = p.get("price")?.as_f64()?;
        let zone = p.get("zone").and_then(Json::as_f64).unwrap_or(signal_price);
        let entry = if self.market_entry {
            signal_price
        } else {
            zone
        };
        let structural_stop = p.get("stop_anchor")?.as_f64()?;
        let structural_risk_pct = (entry - structural_stop).abs() / entry;
        if structural_risk_pct > self.max_stop_pct {
            return None;
        }
        let stop = match side {
            Side::Buy => structural_stop.min(entry * (1.0 - self.min_stop_pct)),
            Side::Sell => structural_stop.max(entry * (1.0 + self.min_stop_pct)),
        };
        let risk = (entry - stop).abs();
        let tp1 = match side {
            Side::Buy => entry + risk * self.tp1_r,
            Side::Sell => entry - risk * self.tp1_r,
        };
        Some(OrderIntent {
            symbol: symbol.clone(),
            side,
            qty: Qty::ZERO,
            risk_scale: self.risk_scale,
            limit_price: (!self.market_entry).then_some(Price::from_f64(zone)),
            stop_price: Price::from_f64(stop),
            tp1_price: Some(Price::from_f64(tp1)),
            reason: "orderflow_verified_context".to_string(),
            ts: signal.ts,
        })
    }
}

impl TriggerPlugin for OrderFlowEntry {
    fn name(&self) -> &'static str {
        "OrderFlowEntry"
    }

    fn should_fire(&self, _signals: &[Signal], _ctx: &Ctx) -> Option<OrderIntent> {
        None
    }

    fn on_signals(
        &mut self,
        signals: &[Signal],
        _ctx: &Ctx,
        symbol: &Symbol,
    ) -> Option<OrderIntent> {
        signals
            .iter()
            .find_map(|signal| self.intent(signal, symbol))
    }
}

pub fn build_orderflow(p: &Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
    let f = |key: &str, default: f64| p.get(key).and_then(Json::as_f64).unwrap_or(default);
    let b = |key: &str, default: bool| p.get(key).and_then(Json::as_bool).unwrap_or(default);
    Ok(Box::new(OrderFlowEntry {
        risk_scale: f("risk_scale", 0.15).clamp(0.0, 1.0),
        tp1_r: f("tp1_r", 1.5).max(0.1),
        min_stop_pct: f("min_stop_pct", 0.005).clamp(0.0005, 0.02),
        max_stop_pct: f("max_stop_pct", 0.010).clamp(0.001, 0.05),
        market_entry: b("market_entry", true),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tcore::{SignalKind, Timestamp};

    fn signal(eligible: bool) -> Signal {
        Signal::new(
            SignalKind::Other,
            Timestamp::from_millis(1),
            "OrderFlowExhaustion",
            json!({
                "stage":"confirmed", "side":"sell", "price":100.0,
                "zone":99.9, "stop_anchor":100.5, "trade_eligible":eligible
            }),
        )
    }

    #[test]
    fn requires_all_verified_context_gates() {
        let mut trigger = build_orderflow(&json!({"max_stop_pct":0.02})).unwrap();
        let symbol = Symbol::new("BTCUSDT");
        assert!(trigger
            .on_signals(&[signal(false)], &Ctx::default(), &symbol)
            .is_none());
        let intent = trigger
            .on_signals(&[signal(true)], &Ctx::default(), &symbol)
            .unwrap();
        assert_eq!(intent.side, Side::Sell);
        assert!(intent.limit_price.is_none());
        assert!((intent.risk_scale - 0.15).abs() < 1e-9);
        assert_eq!(intent.reason, "orderflow_verified_context");
    }
}
