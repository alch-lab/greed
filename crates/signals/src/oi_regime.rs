//! OI 四象限信号（后续 实现完整逻辑；此处提供占位骨架供装配）。

use tcore::plugin::{Ctx, Signal, SignalPlugin};
use tcore::Event;

/// 占位：OI 四象限信号插件（后续 填充四象限与重置检测）。
pub struct OiQuadrant {
    pub oi_reset_pct: f64,
}

impl SignalPlugin for OiQuadrant {
    fn name(&self) -> &'static str {
        "OiQuadrant"
    }
    fn on_event(&mut self, _ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        vec![] // 后续 实现
    }
}

impl OiQuadrant {
    pub fn from_params(p: &serde_json::Value) -> Self {
        Self {
            oi_reset_pct: p
                .get("oi_reset_pct")
                .and_then(|v| v.as_f64())
                .unwrap_or(3.0),
        }
    }
}
