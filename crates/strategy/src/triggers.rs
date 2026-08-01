//! 分层订单流确认入场。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::{Ctx, OrderIntent, Price, Qty, Side, Signal, Symbol, TriggerPlugin};

pub struct OrderFlowEntry {
    trade_classic: bool,
    trade_strength: bool,
    trade_high_frequency: bool,
    trade_balanced: bool,
    trade_quality: bool,
    classic_risk_scale: f64,
    strength_risk_scale: f64,
    high_frequency_risk_scale: f64,
    balanced_risk_scale: f64,
    quality_risk_scale: f64,
    tp1_r: f64,
    min_stop_pct: f64,
    max_stop_pct: f64,
    passive_entry: bool,
}

impl OrderFlowEntry {
    fn profile_config(&self, profile: &str) -> Option<(bool, f64)> {
        Some(match profile {
            "classic" => (self.trade_classic, self.classic_risk_scale),
            "strength" => (self.trade_strength, self.strength_risk_scale),
            "high_frequency" => (self.trade_high_frequency, self.high_frequency_risk_scale),
            "balanced" => (self.trade_balanced, self.balanced_risk_scale),
            "quality" => (self.trade_quality, self.quality_risk_scale),
            _ => return None,
        })
    }

    fn intent(&self, signal: &Signal, symbol: &Symbol) -> Option<OrderIntent> {
        if signal.source != "OrderFlowExhaustion"
            || signal.payload.get("stage")?.as_str()? != "confirmed"
        {
            return None;
        }
        let p = &signal.payload;
        let profile = p.get("profile")?.as_str()?;
        let (enabled, risk_scale) = self.profile_config(profile)?;
        if !enabled || risk_scale <= 0.0 {
            return None;
        }
        let side = match p.get("side")?.as_str()? {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => return None,
        };
        let signal_price = p.get("price")?.as_f64()?;
        let zone = p.get("zone").and_then(Json::as_f64).unwrap_or(signal_price);
        let entry = if self.passive_entry {
            zone
        } else {
            signal_price
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
            risk_scale,
            limit_price: self.passive_entry.then_some(Price::from_f64(zone)),
            stop_price: Price::from_f64(stop),
            tp1_price: Some(Price::from_f64(tp1)),
            reason: format!("orderflow_{profile}"),
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
        // 信号层已经按 event_id 去重；此处只消费一次确认后的最高通过层级。
        signals.iter().find_map(|s| self.intent(s, symbol))
    }
}

pub fn build_orderflow(p: &Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
    let f = |k: &str, d: f64| p.get(k).and_then(Json::as_f64).unwrap_or(d);
    let b = |k: &str, d: bool| p.get(k).and_then(Json::as_bool).unwrap_or(d);
    Ok(Box::new(OrderFlowEntry {
        trade_classic: b("trade_classic", true),
        trade_strength: b("trade_strength", true),
        trade_high_frequency: b("trade_high_frequency", true),
        trade_balanced: b("trade_balanced", true),
        trade_quality: b("trade_quality", true),
        classic_risk_scale: f("classic_risk_scale", 0.20).clamp(0.0, 1.0),
        strength_risk_scale: f("strength_risk_scale", 0.35).clamp(0.0, 1.0),
        high_frequency_risk_scale: f("high_frequency_risk_scale", 0.15).clamp(0.0, 1.0),
        balanced_risk_scale: f("balanced_risk_scale", 0.60).clamp(0.0, 1.0),
        quality_risk_scale: f("quality_risk_scale", 1.00).clamp(0.0, 1.0),
        tp1_r: f("tp1_r", 2.0).max(0.1),
        min_stop_pct: f("min_stop_pct", 0.003).clamp(0.0005, 0.02),
        max_stop_pct: f("max_stop_pct", 0.008).clamp(0.001, 0.05),
        passive_entry: b("passive_entry", false),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tcore::{SignalKind, Timestamp};

    fn signal(profile: &str) -> Signal {
        Signal::new(
            SignalKind::Other,
            Timestamp::from_millis(1),
            "OrderFlowExhaustion",
            json!({
                "stage":"confirmed", "profile":profile, "side":"sell",
                "price":100.0, "zone":100.0, "stop_anchor":100.5
            }),
        )
    }

    #[test]
    fn profile_controls_risk_scale() {
        let mut t = build_orderflow(&json!({"max_stop_pct":0.02})).unwrap();
        let i = t
            .on_signals(
                &[signal("balanced")],
                &Ctx::default(),
                &Symbol::new("BTCUSDT"),
            )
            .unwrap();
        assert_eq!(i.side, Side::Sell);
        assert!((i.risk_scale - 0.60).abs() < 1e-9);
        assert_eq!(i.reason, "orderflow_balanced");
    }

    #[test]
    fn disabled_profile_does_not_trade() {
        let mut t = build_orderflow(&json!({"trade_high_frequency":false})).unwrap();
        assert!(t
            .on_signals(
                &[signal("high_frequency")],
                &Ctx::default(),
                &Symbol::new("BTCUSDT")
            )
            .is_none());
    }
}
