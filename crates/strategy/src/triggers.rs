//! 扳机插件（后续实现力竭反转；此处提供占位与注册入口）。
//!
//! 扳机在信号与过滤都满足后决定是否扣扳机。
//! PR-8 将实现 ExhaustionReversal（三条件）、BlueBandMarket、GridCluster。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::plugin::{Ctx, OrderIntent, Signal, TriggerPlugin};

/// 占位扳机：从不触发。用于装配链路打通与测试。
pub struct NoopTrigger;

impl TriggerPlugin for NoopTrigger {
    fn name(&self) -> &'static str {
        "NoopTrigger"
    }
    fn should_fire(&self, _signals: &[Signal], _ctx: &Ctx) -> Option<OrderIntent> {
        None
    }
}

pub fn build_noop(_p: &Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
    Ok(Box::new(NoopTrigger))
}

/// 力竭反转扳机（后续实现三条件逻辑；此处为占位骨架供装配）。
///
/// 完整实现将：在观察区（色带/Delta 阈值区）内，
/// 检测 renko 砖的放量快速 + 力竭不推进 + Delta 反转三条件。
pub struct ExhaustionReversal {
    pub vol_mult_exh: f64,
    pub delta_flip_pct: f64,
    pub dur_max_ms: u64,
    pub prog_ratio: f64,
}

impl TriggerPlugin for ExhaustionReversal {
    fn name(&self) -> &'static str {
        "ExhaustionReversal"
    }
    fn should_fire(&self, _signals: &[Signal], _ctx: &Ctx) -> Option<OrderIntent> {
        None // PR-8 实现
    }
}

pub fn build_exhaustion(p: &Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
    let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
    Ok(Box::new(ExhaustionReversal {
        vol_mult_exh: g("vol_mult_exh", 3.0),
        delta_flip_pct: g("delta_flip_pct", 8.0),
        dur_max_ms: g("dur_max_ms", 30_000.0) as u64,
        prog_ratio: g("prog_ratio", 0.4),
    }))
}
