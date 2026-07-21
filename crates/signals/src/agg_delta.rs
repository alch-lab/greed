//! 总 Delta 阈值信号（后续实现完整逻辑；此处提供占位骨架供装配）。
//!
//! 完整实现将：滚动累加全网 AccDelta，按 T1–T4 档位发 DeltaTier 信号，
//! 并用多空极值不对称驱动 TrendRegime。

use tcore::plugin::{Ctx, Signal, SignalPlugin};
use tcore::Event;

/// 占位：总Delta 阈值信号插件（后续填充累加与档位逻辑）。
pub struct AggDeltaTier {
    /// 阈值档位（B USD），后续使用。
    pub t1: f64,
    pub t2: f64,
    pub t3: f64,
    pub t4: f64,
    pub window_hours: u32,
    pub r_trend: f64,
}

impl SignalPlugin for AggDeltaTier {
    fn name(&self) -> &'static str {
        "AggDeltaTier"
    }
    fn on_event(&mut self, _ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        vec![] // 后续实现
    }
}

impl AggDeltaTier {
    /// 从参数表构造（默认值锚定教程）。
    pub fn from_params(p: &serde_json::Value) -> Self {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        Self {
            t1: g("t1", 1.0),
            t2: g("t2", 2.0),
            t3: g("t3", 3.0),
            t4: g("t4", 3.5),
            window_hours: g("window_hours", 24.0) as u32,
            r_trend: g("r_trend", 2.0),
        }
    }
}
