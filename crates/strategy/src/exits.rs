//! 订单流交易管理：TP1 → 保本 → TP2 → runner，外加时间止损。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::{Ctx, ExitAction, ExitPlugin, Position, Price, Side};

pub struct OrderFlowTradeManagement {
    tp1_close: f64,
    tp2_original_close: f64,
    tp2_r: f64,
    runner_trail_pct: f64,
    mr_breakeven_buffer_pct: f64,
    max_hold_ms: i64,
    trend_trail_pct: f64,
    trend_trail_activation_pct: f64,
    trend_partial_activation_pct: f64,
    trend_partial_close: f64,
    trend_lock_profit_pct: f64,
    trend_max_hold_ms: i64,
}

impl ExitPlugin for OrderFlowTradeManagement {
    fn name(&self) -> &'static str {
        "OrderFlowTradeManagement"
    }

    fn manage(&self, pos: &Position, ctx: &Ctx) -> Vec<ExitAction> {
        let Some(now) = ctx.now else { return vec![] };
        let is_trend = ctx.flag("position_strategy") == Some("trend");
        let max_hold_ms = if is_trend {
            self.trend_max_hold_ms
        } else {
            self.max_hold_ms
        };
        if now.as_millis() - pos.entry_ts.as_millis() >= max_hold_ms {
            return vec![ExitAction::CloseAll];
        }
        let Some(last) = ctx.flag("last_price").and_then(|v| v.parse::<f64>().ok()) else {
            return vec![];
        };
        let favorable = |target: f64| match pos.side {
            Side::Buy => last >= target,
            Side::Sell => last <= target,
        };
        if is_trend {
            let favorable_pct = match pos.side {
                Side::Buy => last / pos.entry_price.to_f64() - 1.0,
                Side::Sell => 1.0 - last / pos.entry_price.to_f64(),
            };
            if pos.closed_frac + 1e-6 < self.trend_partial_close
                && favorable_pct >= self.trend_partial_activation_pct
            {
                let protected = match pos.side {
                    Side::Buy => entry_with_buffer(pos.entry_price, self.trend_lock_profit_pct),
                    Side::Sell => entry_with_buffer(pos.entry_price, -self.trend_lock_profit_pct),
                };
                return vec![
                    ExitAction::ClosePartial(self.trend_partial_close),
                    ExitAction::MoveStop(protected),
                ];
            }
            if favorable_pct < self.trend_trail_activation_pct {
                return vec![];
            }
            let candidate = match pos.side {
                Side::Buy => last * (1.0 - self.trend_trail_pct),
                Side::Sell => last * (1.0 + self.trend_trail_pct),
            };
            let improves = match pos.side {
                Side::Buy => candidate > pos.stop_price.to_f64(),
                Side::Sell => candidate < pos.stop_price.to_f64(),
            };
            return if improves {
                vec![ExitAction::MoveStop(Price::from_f64(candidate))]
            } else {
                vec![]
            };
        }
        let entry = pos.entry_price.to_f64();
        let initial_stop = pos.initial_stop_price.to_f64();
        let risk = (entry - initial_stop).abs();
        if risk <= 1e-9 {
            return vec![];
        }

        if pos.closed_frac < 0.49 {
            if let Some(tp1) = pos.tp1_price.map(Price::to_f64) {
                if favorable(tp1) {
                    let protected = match pos.side {
                        Side::Buy => {
                            entry_with_buffer(pos.entry_price, self.mr_breakeven_buffer_pct)
                        }
                        Side::Sell => {
                            entry_with_buffer(pos.entry_price, -self.mr_breakeven_buffer_pct)
                        }
                    };
                    return vec![
                        ExitAction::ClosePartial(self.tp1_close),
                        ExitAction::MoveStop(protected),
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
        mr_breakeven_buffer_pct: f("mr_breakeven_buffer_pct", 0.0008).clamp(0.0, 0.005),
        max_hold_ms: (f("max_hold_hours", 4.0).max(0.1) * 3_600_000.0) as i64,
        trend_trail_pct: f("trend_trail_pct", 0.0025).clamp(0.001, 0.05),
        trend_trail_activation_pct: f("trend_trail_activation_pct", 0.0025).clamp(0.001, 0.05),
        trend_partial_activation_pct: f("trend_partial_activation_pct", 0.006).clamp(0.002, 0.05),
        trend_partial_close: f("trend_partial_close", 0.25).clamp(0.05, 0.75),
        trend_lock_profit_pct: f("trend_lock_profit_pct", 0.001).clamp(0.0, 0.02),
        trend_max_hold_ms: (f("trend_max_hold_hours", 2.0).max(0.1) * 3_600_000.0) as i64,
    }))
}

fn entry_with_buffer(entry: Price, signed_pct: f64) -> Price {
    Price::from_f64(entry.to_f64() * (1.0 + signed_pct))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tcore::{Qty, Symbol, Timestamp};

    fn position() -> Position {
        Position {
            symbol: Symbol::new("BTCUSDT"),
            side: Side::Buy,
            entry_price: Price::from_f64(100.0),
            qty: Qty::from_f64(1.0),
            entry_ts: Timestamp::from_millis(0),
            stop_price: Price::from_f64(99.75),
            initial_stop_price: Price::from_f64(99.75),
            tp1_price: Some(Price::from_f64(100.20)),
            breakeven_moved: false,
            closed_frac: 0.0,
        }
    }

    #[test]
    fn trend_trail_waits_for_activation_profit() {
        let exit = build_orderflow_management(&json!({})).unwrap();
        let mut ctx = Ctx::default();
        ctx.now = Some(Timestamp::from_millis(60_000));
        ctx.flags.insert("position_strategy".into(), "trend".into());
        ctx.flags.insert("last_price".into(), "100.10".into());
        assert!(exit.manage(&position(), &ctx).is_empty());
        ctx.flags.insert("last_price".into(), "100.30".into());
        assert!(matches!(
            exit.manage(&position(), &ctx)[0],
            ExitAction::MoveStop(_)
        ));
    }

    #[test]
    fn mr_uses_partial_tp_and_breakeven() {
        let exit = build_orderflow_management(
            &json!({"max_hold_hours":0.5,"mr_breakeven_buffer_pct":0.0008}),
        )
        .unwrap();
        let mut ctx = Ctx::default();
        ctx.now = Some(Timestamp::from_millis(60_000));
        ctx.flags.insert("position_strategy".into(), "mr".into());
        ctx.flags.insert("last_price".into(), "100.20".into());
        let actions = exit.manage(&position(), &ctx);
        assert!(matches!(actions[0], ExitAction::ClosePartial(_)));
        assert!(matches!(actions[1], ExitAction::MoveStop(price) if price.to_f64() > 100.0));
    }

    #[test]
    fn trend_takes_partial_before_wider_trailing_stage() {
        let exit = build_orderflow_management(&json!({
            "trend_partial_activation_pct":0.006,
            "trend_partial_close":0.25,
            "trend_lock_profit_pct":0.001,
            "trend_trail_activation_pct":0.010,
            "trend_trail_pct":0.0025
        }))
        .unwrap();
        let mut ctx = Ctx::default();
        ctx.now = Some(Timestamp::from_millis(60_000));
        ctx.flags.insert("position_strategy".into(), "trend".into());
        ctx.flags.insert("last_price".into(), "100.70".into());
        let actions = exit.manage(&position(), &ctx);
        assert!(matches!(actions[0], ExitAction::ClosePartial(frac) if (frac - 0.25).abs() < 1e-9));
        assert!(matches!(actions[1], ExitAction::MoveStop(price) if price.to_f64() > 100.0));
    }
}
