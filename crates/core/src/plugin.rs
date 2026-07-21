//! 插件
//! 设计原则：策略 = 插件的组合装配，四大类别：
//! - [`SignalPlugin`]  信号源：从事件流产出观察信号，不直接下单
//! - [`FilterPlugin`]  过滤器：对下单意图一票否决 / 降权
//! - [`TriggerPlugin`] 扳机：在信号与过滤都满足后决定是否扣扳机
//! - [`ExitPlugin`]    出场：持仓期间的保本 / 止盈 / 反手 / 时间止损
//!
//! 插件通过名称在 [`str`](mod@std) 配置中声明、由注册表装配；
//! 同一类别可注册多个实现，便于 A/B 对比（如不同入场模型）。

use crate::event::Event;
use crate::types::{Price, Qty, Side, Symbol, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// 信号
// ============================================================================

/// 信号类别（用于扳机/过滤按需检索，而不必关心具体插件）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SignalKind {
    /// renko 反转砖已收（力竭反转的输入）
    BrickClosed,
    /// 大单流速率激增（AGGR 声音替代）
    FlowSurge,
    /// 色带/挂单墙区域（OBI）
    ObiZone,
    /// 总Delta 档位（WATCH/ENTRY_OK/HIGH_Q/EXTREME）
    DeltaTier,
    /// 趋势状态（RANGE/TREND_UP/TREND_DOWN）
    TrendRegime,
    /// OI 四象限
    OiQuadrant,
    /// 占位/未分类
    Other,
}

/// 一条信号（载荷为 JSON，便于各插件自定义数据结构）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub kind: SignalKind,
    pub ts: Timestamp,
    /// 来源插件名
    pub source: &'static str,
    /// 结构化载荷（砖、档位、象限等具体数据）
    pub payload: serde_json::Value,
}

impl Signal {
    pub fn new(
        kind: SignalKind,
        ts: Timestamp,
        source: &'static str,
        payload: serde_json::Value,
    ) -> Self {
        Signal {
            kind,
            ts,
            source,
            payload,
        }
    }
}

// ============================================================================
// 下单意图与持仓
// ============================================================================

/// 下单意图（扳机产出，经过滤器裁决后由执行层落地）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntent {
    pub symbol: Symbol,
    pub side: Side,
    /// 数量（定点）。执行前会再乘过滤器降权系数。
    pub qty: Qty,
    /// 限价；None 表示市价。
    pub limit_price: Option<Price>,
    /// 止损价（结构锚定：针尖下/墙外/蓝带下）。
    pub stop_price: Price,
    /// 第一止盈参考位（可选）。
    pub tp1_price: Option<Price>,
    /// 触发原因（扳机名 + 关键上下文，便于复盘）。
    pub reason: String,
    pub ts: Timestamp,
}

/// 过滤器加载
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Verdict {
    /// 放行
    Allow,
    /// 放行但仓位乘以系数（如美盘降权，周末降仓）
    /// 多个过滤器降权时取最小系数（最保守）
    Scale(f64),
    /// 否决（附原因，静态字符串，便于日志聚合）
    Veto(&'static str),
}

/// 当前持仓（出场插件的输入）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: Symbol,
    pub side: Side,
    pub entry_price: Price,
    pub qty: Qty,
    pub entry_ts: Timestamp,
    pub stop_price: Price,
    pub tp1_price: Option<Price>,
    /// 是否已推保本
    pub breakeven_moved: bool,
    /// 已平仓比例（0.0–1.0），配合 TP1 平部分仓
    pub closed_frac: f64,
}

/// 出场动作。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExitAction {
    /// 移动止损到指定价（保本/跟进）
    MoveStop(Price),
    /// 平掉部分仓位（比例为当前剩余的比例）
    ClosePartial(f64),
    /// 全部平仓
    CloseAll,
    /// 反手（先平仓再按新意图开仓）
    Reverse(Box<OrderIntent>),
}

// ============================================================================
// 上下文
// ============================================================================

/// 插件共享的只读上下文。
///
/// 包含当前时间、最新信号缓存、环境标志（时段/事件/熔断），
/// 由策略状态机在每个事件处理后更新。
#[derive(Debug, Default)]
pub struct Ctx {
    pub now: Option<Timestamp>,
    /// 最近一条持仓（无持仓为 None）
    pub position: Option<Position>,
    /// 各 SignalKind 最新一条信号（便于扳机/过滤检索）
    pub latest: BTreeMap<String, Signal>,
    /// 环境标志（如 "session"="asia"/"us"/"weekend"，"event"="fomc" 等）
    pub flags: BTreeMap<String, String>,
}

impl Ctx {
    pub fn set_latest(&mut self, sig: Signal) {
        self.latest.insert(format!("{:?}", sig.kind), sig);
    }
    pub fn latest_of(&self, kind: SignalKind) -> Option<&Signal> {
        self.latest.get(&format!("{:?}", kind))
    }
    pub fn flag(&self, key: &str) -> Option<&str> {
        self.flags.get(key).map(|s| s.as_str())
    }
}

// ============================================================================
// 插件 trait
// ============================================================================

/// 信号源插件
pub trait SignalPlugin: Send {
    fn name(&self) -> &'static str;
    fn on_event(&mut self, ev: &Event, ctx: &Ctx) -> Vec<Signal>;
}

/// 过滤器插件
pub trait FilterPlugin: Send {
    fn name(&self) -> &'static str;
    fn check(&self, intent: &OrderIntent, ctx: &Ctx) -> Verdict;
}

/// 入场插件
pub trait TriggerPlugin: Send {
    fn name(&self) -> &'static str;
    /// 返回 `Some(intent)` 表示入场
    fn should_fire(&self, signals: &[Signal], ctx: &Ctx) -> Option<OrderIntent>;
}

/// 出场管理插件
pub trait ExitPlugin: Send {
    fn name(&self) -> &'static str;
    fn manage(&self, pos: &Position, ctx: &Ctx) -> Vec<ExitAction>;
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Exchange, Price, Qty};

    fn sym() -> Symbol {
        Symbol::new("BTCUSDT")
    }

    fn intent() -> OrderIntent {
        OrderIntent {
            symbol: sym(),
            side: Side::Buy,
            qty: Qty::from_f64(0.1),
            limit_price: None,
            stop_price: Price::from_f64(66000.0),
            tp1_price: Some(Price::from_f64(67600.0)),
            reason: "test".into(),
            ts: Timestamp::from_millis(1000),
        }
    }

    #[test]
    fn verdict_semantics() {
        assert_eq!(Verdict::Allow, Verdict::Allow);
        assert_ne!(Verdict::Scale(0.5), Verdict::Allow);
        assert_eq!(Verdict::Veto("trend"), Verdict::Veto("trend"));
    }

    #[test]
    fn ctx_latest_signal_lookup() {
        let mut ctx = Ctx::default();
        let sig = Signal::new(
            SignalKind::DeltaTier,
            Timestamp::from_millis(1000),
            "AggDeltaTier",
            serde_json::json!({"tier": 3}),
        );
        ctx.set_latest(sig);
        let got = ctx.latest_of(SignalKind::DeltaTier).unwrap();
        assert_eq!(got.payload["tier"], 3);
        assert!(ctx.latest_of(SignalKind::BrickClosed).is_none());
    }

    #[test]
    fn ctx_flags() {
        let mut ctx = Ctx::default();
        ctx.flags.insert("session".into(), "us".into());
        assert_eq!(ctx.flag("session"), Some("us"));
        assert_eq!(ctx.flag("event"), None);
    }

    #[test]
    fn order_intent_and_position_serde() {
        let i = intent();
        let s = serde_json::to_string(&i).unwrap();
        let back: OrderIntent = serde_json::from_str(&s).unwrap();
        assert_eq!(back.side, Side::Buy);
        assert!((back.stop_price.to_f64() - 66000.0).abs() < 1e-6);

        let p = Position {
            symbol: sym(),
            side: Side::Buy,
            entry_price: Price::from_f64(67000.0),
            qty: Qty::from_f64(0.1),
            entry_ts: Timestamp::from_millis(1000),
            stop_price: Price::from_f64(66000.0),
            tp1_price: Some(Price::from_f64(67600.0)),
            breakeven_moved: false,
            closed_frac: 0.0,
        };
        let sp = serde_json::to_string(&p).unwrap();
        let backp: Position = serde_json::from_str(&sp).unwrap();
        assert_eq!(backp.qty.to_f64(), 0.1);
        let _ = Exchange::BinanceFutures; // 保持引用
    }

    #[test]
    fn exit_action_variants() {
        let mv = ExitAction::MoveStop(Price::from_f64(67000.0));
        assert!(matches!(mv, ExitAction::MoveStop(_)));
        let cp = ExitAction::ClosePartial(0.5);
        assert!(matches!(cp, ExitAction::ClosePartial(_)));
    }
}
