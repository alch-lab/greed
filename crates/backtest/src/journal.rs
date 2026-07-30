//! 回测/实盘共用的决策流水（journal）：前端监控与复盘的数据源。
//!
//! 三类记录：
//! - [`JournalIntent`]：下单意图（时间、方向、数量、限价、止损、止盈、原因）
//! - 成交：直接用 [`crate::account::Fill`]（含实际成交价、费用、已实现盈亏）
//! - 权益曲线：[`crate::report::EquityPoint`]（按 UTC 日采样）

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct JournalSleeve {
    pub key: String,
    pub label: String,
    pub state: String,
    pub pnl: f64,
    pub fees: f64,
    pub trades: u64,
    pub wins: u64,
    pub target_qty: f64,
    pub actual_source: String,
    pub last_action_ms: Option<i64>,
    pub detail: String,
}

/// 一条下单意图记录（扳机扣扳机时产生）。
#[derive(Debug, Clone, Serialize)]
pub struct JournalIntent {
    pub ts_ms: i64,
    pub side: String,
    pub qty: f64,
    /// 限价（None = 市价）
    pub limit_price: Option<f64>,
    pub stop_price: f64,
    pub tp1_price: Option<f64>,
    /// 下单原因（扳机名 + 关键上下文，如信号价位/EMA/结构锚）
    pub reason: String,
}

/// 一条策略评估记录（信号插件每次评估后产生，可观测性用）。
/// `note` 为插件自定义 JSON（偏离/阈值/趋势/决策/原因等）。
#[derive(Debug, Clone, Serialize)]
pub struct JournalEval {
    pub ts_ms: i64,
    pub source: String,
    pub note: serde_json::Value,
}

/// 完整流水（CLI `--journal` 导出 JSON 的顶层结构）。
#[derive(Debug, Serialize)]
pub struct Journal {
    pub meta: JournalMeta,
    pub intents: Vec<JournalIntent>,
    pub fills: Vec<crate::account::Fill>,
    pub equity_curve: Vec<crate::report::EquityPoint>,
    /// 策略评估流水（为什么下单/不下单）；回测不收集则为空
    pub evals: Vec<JournalEval>,
    pub sleeves: Vec<JournalSleeve>,
}

#[derive(Debug, Serialize)]
pub struct JournalMeta {
    pub symbol: String,
    pub from: String,
    pub to: String,
    pub strategy: String,
    pub initial_cash: f64,
    pub final_equity: f64,
}
