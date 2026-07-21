//! 出场插件（暂时提供基础实现；后续 补齐 TieredTakeProfit/ReverseOnSignal 等）。
//!
//! 出场插件在持仓期间每个事件后被调用，产出保本/止盈/反手/时间止损动作。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::plugin::{Ctx, ExitAction, ExitPlugin, Position};
use tcore::types::Side;

/// 保本：价格离开入场价 `trigger_usd` 美元后，把止损推到入场价。
///
/// 教程规则（BreakevenAt300）：离开 300 美元推保本，统计约 90% 至少不亏，
/// 代价约 1/3 被二探打掉。`trigger_usd` 默认 300。
pub struct BreakevenAt {
    pub trigger_usd: f64,
}

impl ExitPlugin for BreakevenAt {
    fn name(&self) -> &'static str {
        "BreakevenAt300"
    }
    fn manage(&self, pos: &Position, ctx: &Ctx) -> Vec<ExitAction> {
        if pos.breakeven_moved {
            return vec![];
        }
        let now_px = match current_price(ctx) {
            Some(p) => p,
            None => return vec![],
        };
        let moved = match pos.side {
            Side::Buy => now_px.to_f64() >= pos.entry_price.to_f64() + self.trigger_usd,
            Side::Sell => now_px.to_f64() <= pos.entry_price.to_f64() - self.trigger_usd,
        };
        if moved {
            vec![ExitAction::MoveStop(pos.entry_price)]
        } else {
            vec![]
        }
    }
}

pub fn build_breakeven(p: &Json) -> Result<Box<dyn ExitPlugin>, PluginBuildError> {
    let trigger = p
        .get("trigger_usd")
        .and_then(|v| v.as_f64())
        .unwrap_or(300.0);
    Ok(Box::new(BreakevenAt {
        trigger_usd: trigger,
    }))
}

/// 时间止损：持仓超过 `max_hours` 未达 TP1 且未触损 → 全部平仓。
pub struct TimeStop {
    pub max_hours: f64,
}

impl ExitPlugin for TimeStop {
    fn name(&self) -> &'static str {
        "TimeStop"
    }
    fn manage(&self, pos: &Position, ctx: &Ctx) -> Vec<ExitAction> {
        if let Some(now) = ctx.now {
            let held_h = now.diff_ms(pos.entry_ts) as f64 / 3_600_000.0;
            if held_h >= self.max_hours {
                return vec![ExitAction::CloseAll];
            }
        }
        vec![]
    }
}

pub fn build_time_stop(p: &Json) -> Result<Box<dyn ExitPlugin>, PluginBuildError> {
    let h = p.get("max_hours").and_then(|v| v.as_f64()).unwrap_or(36.0);
    Ok(Box::new(TimeStop { max_hours: h }))
}

/// 分级止盈：到达 TP1（首个反向色带/参考位）平 `tp1_pct`% 仓位。
///
/// 简化实现（PR-4）：当现价触及 `pos.tp1_price` 时平掉剩余仓位的 tp1_pct，
/// 并由状态机配合 BreakevenAt 推保本。完整版（Phase 3）接反向色带与 AccDelta。
pub struct TieredTakeProfit {
    pub tp1_frac: f64, // 0.5 = 平 50%
}

impl ExitPlugin for TieredTakeProfit {
    fn name(&self) -> &'static str {
        "TieredTakeProfit"
    }
    fn manage(&self, pos: &Position, ctx: &Ctx) -> Vec<ExitAction> {
        let tp1 = match pos.tp1_price {
            Some(p) => p,
            None => return vec![],
        };
        if pos.closed_frac >= self.tp1_frac {
            return vec![]; // TP1 已执行过
        }
        let now_px = match current_price(ctx) {
            Some(p) => p,
            None => return vec![],
        };
        let hit = match pos.side {
            Side::Buy => now_px >= tp1,
            Side::Sell => now_px <= tp1,
        };
        if hit {
            vec![ExitAction::ClosePartial(self.tp1_frac)]
        } else {
            vec![]
        }
    }
}

pub fn build_tiered_tp(p: &Json) -> Result<Box<dyn ExitPlugin>, PluginBuildError> {
    let pct = p.get("tp1_pct").and_then(|v| v.as_f64()).unwrap_or(50.0);
    Ok(Box::new(TieredTakeProfit {
        tp1_frac: pct / 100.0,
    }))
}

/// 从 Ctx 取当前价（由状态机写入 flags["last_price"]）。
fn current_price(ctx: &Ctx) -> Option<tcore::types::Price> {
    ctx.flag("last_price")
        .and_then(|s| s.parse::<f64>().ok())
        .map(tcore::types::Price::from_f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Price, Qty, Symbol, Timestamp};

    fn pos(side: Side, entry: f64) -> Position {
        Position {
            symbol: Symbol::new("BTCUSDT"),
            side,
            entry_price: Price::from_f64(entry),
            qty: Qty::from_f64(0.1),
            entry_ts: Timestamp::from_millis(0),
            stop_price: Price::from_f64(entry - 150.0),
            tp1_price: None,
            breakeven_moved: false,
            closed_frac: 0.0,
        }
    }
    fn ctx_at(price: f64, now_ms: i64) -> Ctx {
        let mut c = Ctx {
            now: Some(Timestamp::from_millis(now_ms)),
            ..Default::default()
        };
        c.flags.insert("last_price".into(), price.to_string());
        c
    }

    #[test]
    fn breakeven_triggers_for_long() {
        let e = BreakevenAt { trigger_usd: 300.0 };
        let p = pos(Side::Buy, 67000.0);
        // 价格未到 67300：不动
        assert!(e.manage(&p, &ctx_at(67200.0, 1000)).is_empty());
        // 价格到 67300：推保本到入场价
        let acts = e.manage(&p, &ctx_at(67350.0, 1000));
        assert_eq!(acts.len(), 1);
        assert!(matches!(acts[0], ExitAction::MoveStop(_)));
    }

    #[test]
    fn breakeven_triggers_for_short() {
        let e = BreakevenAt { trigger_usd: 300.0 };
        let p = pos(Side::Sell, 67000.0);
        assert!(e.manage(&p, &ctx_at(66800.0, 1000)).is_empty());
        assert_eq!(e.manage(&p, &ctx_at(66650.0, 1000)).len(), 1);
    }

    #[test]
    fn breakeven_not_repeat() {
        let e = BreakevenAt { trigger_usd: 300.0 };
        let mut p = pos(Side::Buy, 67000.0);
        p.breakeven_moved = true; // 已推过
        assert!(e.manage(&p, &ctx_at(68000.0, 1000)).is_empty());
    }

    #[test]
    fn time_stop_closes_after_max() {
        let t = TimeStop { max_hours: 36.0 };
        let p = pos(Side::Buy, 67000.0);
        let before = ctx_at(67000.0, 35 * 3_600_000);
        let after = ctx_at(67000.0, 37 * 3_600_000);
        assert!(t.manage(&p, &before).is_empty());
        assert!(matches!(t.manage(&p, &after)[0], ExitAction::CloseAll));
    }
}
