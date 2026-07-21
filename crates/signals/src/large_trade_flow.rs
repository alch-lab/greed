//! 大单流速率信号（后续 实现完整逻辑；此处提供占位骨架供装配）。
//!
//! AGGR 听觉信号的量化替代：滑动窗口内 ≥ size_large 的大单笔数/秒，
//! 速率激增时发 FlowSurge 预警（“听到声音切过去”）。

use tcore::plugin::{Ctx, Signal, SignalPlugin};
use tcore::Event;

/// 占位：大单流速率信号插件（后续 填充速率统计与激增检测）。
pub struct LargeTradeFlow {
    pub size_large_usd: f64,
    pub window_secs: u32,
    pub surge_mult: f64,
}

impl SignalPlugin for LargeTradeFlow {
    fn name(&self) -> &'static str {
        "LargeTradeFlow"
    }
    fn on_event(&mut self, _ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        vec![] // 后续 实现
    }
}

impl LargeTradeFlow {
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self {
            size_large_usd: g("size_large_usd", 200_000.0),
            window_secs: g("window_secs", 10.0) as u32,
            surge_mult: g("surge_mult", 5.0),
        }
    }
}
