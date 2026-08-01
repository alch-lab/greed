//! 订单流交易管理：TP1 → 保本 → TP2 → runner，外加时间止损。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::{Ctx, ExitAction, ExitPlugin, Position, Price, Side};

pub struct OrderFlowTradeManagement {
    tp1_close: f64,
    tp2_original_close: f64,
    tp2_r: f64,
    runner_trail_pct: f64,
    max_hold_ms: i64,
}

impl ExitPlugin for OrderFlowTradeManagement {
    fn name(&self) -> &'static str {
        "OrderFlowTradeManagement"
    }

    fn manage(&self, pos: &Position, ctx: &Ctx) -> Vec<ExitAction> {
        let Some(now) = ctx.now else { return vec![] };
        if now.as_millis() - pos.entry_ts.as_millis() >= self.max_hold_ms {
            return vec![ExitAction::CloseAll];
        }
        let Some(last) = ctx.flag("last_price").and_then(|v| v.parse::<f64>().ok()) else {
            return vec![];
        };
        let favorable = |target: f64| match pos.side {
            Side::Buy => last >= target,
            Side::Sell => last <= target,
        };
        let entry = pos.entry_price.to_f64();
        let initial_stop = pos.initial_stop_price.to_f64();
        let risk = (entry - initial_stop).abs();
        if risk <= 1e-9 {
            return vec![];
        }

        if pos.closed_frac < 0.49 {
            if let Some(tp1) = pos.tp1_price.map(Price::to_f64) {
                if favorable(tp1) {
                    return vec![
                        ExitAction::ClosePartial(self.tp1_close),
                        ExitAction::MoveStop(pos.entry_price),
                    ];
                }
            }
            return vec![];
        }

        let tp2 = match pos.side {
            Side::Buy => entry + risk * self.tp2_r,
            Side::Sell => entry - risk * self.tp2_r,
        };
        let tp2_done = self.tp1_close + self.tp2_original_close;
        if pos.closed_frac + 1e-6 < tp2_done && favorable(tp2) {
            let frac_remaining = self.tp2_original_close / (1.0 - pos.closed_frac).max(1e-9);
            return vec![ExitAction::ClosePartial(frac_remaining.clamp(0.0, 1.0))];
        }

        if pos.closed_frac + 1e-6 >= tp2_done {
            let candidate = match pos.side {
                Side::Buy => last * (1.0 - self.runner_trail_pct),
                Side::Sell => last * (1.0 + self.runner_trail_pct),
            };
            let improves = match pos.side {
                Side::Buy => candidate > pos.stop_price.to_f64(),
                Side::Sell => candidate < pos.stop_price.to_f64(),
            };
            if improves {
                return vec![ExitAction::MoveStop(Price::from_f64(candidate))];
            }
        }
        vec![]
    }
}

pub fn build_orderflow_management(p: &Json) -> Result<Box<dyn ExitPlugin>, PluginBuildError> {
    let f = |k: &str, d: f64| p.get(k).and_then(Json::as_f64).unwrap_or(d);
    Ok(Box::new(OrderFlowTradeManagement {
        tp1_close: f("tp1_close", 0.50).clamp(0.05, 0.90),
        tp2_original_close: f("tp2_original_close", 0.25).clamp(0.05, 0.45),
        tp2_r: f("tp2_r", 4.0).max(0.2),
        runner_trail_pct: f("runner_trail_pct", 0.006).clamp(0.001, 0.05),
        max_hold_ms: (f("max_hold_hours", 4.0).max(0.1) * 3_600_000.0) as i64,
    }))
}
