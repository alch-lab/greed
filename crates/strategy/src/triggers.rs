//! 订单流模型入场。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::{Ctx, OrderIntent, Price, Qty, Side, Signal, Symbol, TriggerPlugin};

pub struct OrderFlowEntry {
    trade_extreme: bool,
    trade_confirmed: bool,
    trade_second: bool,
    extreme_risk_scale: f64,
    confirmed_risk_scale: f64,
    second_risk_scale: f64,
    tp1_r: f64,
    min_stop_pct: f64,
    max_stop_pct: f64,
    passive_entry: bool,
}

impl OrderFlowEntry {
    fn intent(&self, signal: &Signal, symbol: &Symbol) -> Option<OrderIntent> {
        if signal.source != "OrderFlowExhaustion" {
            return None;
        }
        let p = &signal.payload;
        let stage = p.get("stage")?.as_str()?;
        let enabled = match stage {
            "extreme" => self.trade_extreme,
            "confirmed" => self.trade_confirmed,
            "second" => self.trade_second,
            _ => false,
        };
        if !enabled {
            return None;
        }
        let side = match p.get("side")?.as_str()? {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => return None,
        };
        let signal_price = p.get("price")?.as_f64()?;
        let zone = p.get("zone").and_then(Json::as_f64).unwrap_or(signal_price);
        let entry = if self.passive_entry && stage != "extreme" {
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
        let risk_scale = match stage {
            "extreme" => self.extreme_risk_scale,
            "confirmed" => self.confirmed_risk_scale,
            "second" => self.second_risk_scale,
            _ => return None,
        };
        let tp1 = match side {
            Side::Buy => entry + risk * self.tp1_r,
            Side::Sell => entry - risk * self.tp1_r,
        };
        Some(OrderIntent {
            symbol: symbol.clone(),
            side,
            qty: Qty::ZERO,
            risk_scale,
            limit_price: (self.passive_entry && stage != "extreme")
                .then_some(Price::from_f64(zone)),
            stop_price: Price::from_f64(stop),
            tp1_price: Some(Price::from_f64(tp1)),
            reason: format!("orderflow_{stage}"),
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
        // 同一个桶若有多个阶段，以更保守的最后一条状态为准。
        signals.iter().rev().find_map(|s| self.intent(s, symbol))
    }
}

pub fn build_orderflow(p: &Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
    let f = |k: &str, d: f64| p.get(k).and_then(Json::as_f64).unwrap_or(d);
    Ok(Box::new(OrderFlowEntry {
        trade_extreme: p
            .get("trade_extreme")
            .and_then(Json::as_bool)
            .unwrap_or(false),
        trade_confirmed: p
            .get("trade_confirmed")
            .and_then(Json::as_bool)
            .unwrap_or(true),
        trade_second: p
            .get("trade_second")
            .and_then(Json::as_bool)
            .unwrap_or(true),
        extreme_risk_scale: f("extreme_risk_scale", 0.35).clamp(0.0, 1.0),
        confirmed_risk_scale: f("confirmed_risk_scale", 1.0).clamp(0.0, 1.0),
        second_risk_scale: f("second_risk_scale", 0.50).clamp(0.0, 1.0),
        tp1_r: f("tp1_r", 2.0).max(0.1),
        min_stop_pct: f("min_stop_pct", 0.003).clamp(0.0005, 0.02),
        max_stop_pct: f("max_stop_pct", 0.006).clamp(0.001, 0.05),
        passive_entry: p
            .get("passive_entry")
            .and_then(Json::as_bool)
            .unwrap_or(true),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tcore::{SignalKind, Timestamp};

    #[test]
    fn extreme_is_small_structural_entry() {
        let mut t = build_orderflow(&json!({"trade_extreme":true,"max_stop_pct":0.02})).unwrap();
        let s = Signal::new(
            SignalKind::Other,
            Timestamp::from_millis(1),
            "OrderFlowExhaustion",
            json!({
                "stage":"extreme","side":"sell","price":100.0,"stop_anchor":101.0,
                "volume_ratio":2.0,"delta_share":0.3,"efficiency":0.1
            }),
        );
        let i = t
            .on_signals(&[s], &Ctx::default(), &Symbol::new("BTCUSDT"))
            .unwrap();
        assert_eq!(i.side, Side::Sell);
        assert!((i.risk_scale - 0.35).abs() < 1e-9);
        assert_eq!(i.tp1_price.unwrap(), Price::from_f64(98.0));
    }
}
