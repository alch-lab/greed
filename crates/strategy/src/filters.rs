//! 与订单流模型正交的账户级保护。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::{Ctx, FilterPlugin, OrderIntent, Verdict};

pub struct SessionFilter {
    us_scale: f64,
    weekend_scale: f64,
}

impl FilterPlugin for SessionFilter {
    fn name(&self) -> &'static str {
        "SessionFilter"
    }
    fn check(&self, _intent: &OrderIntent, ctx: &Ctx) -> Verdict {
        match ctx.flag("session") {
            Some("us") => Verdict::Scale(self.us_scale),
            Some("weekend") => Verdict::Scale(self.weekend_scale),
            _ => Verdict::Allow,
        }
    }
}

pub fn build_session(p: &Json) -> Result<Box<dyn FilterPlugin>, PluginBuildError> {
    let f = |k: &str, d: f64| p.get(k).and_then(Json::as_f64).unwrap_or(d);
    Ok(Box::new(SessionFilter {
        us_scale: f("us", 1.0).clamp(0.0, 1.0),
        weekend_scale: f("weekend", 0.5).clamp(0.0, 1.0),
    }))
}

pub struct CircuitBreakerFilter;

impl FilterPlugin for CircuitBreakerFilter {
    fn name(&self) -> &'static str {
        "CircuitBreaker"
    }
    fn check(&self, _intent: &OrderIntent, ctx: &Ctx) -> Verdict {
        if ctx.flag("circuit_breaker") == Some("on") {
            Verdict::Veto("circuit_breaker_on")
        } else {
            Verdict::Allow
        }
    }
}

pub fn build_circuit_breaker(_p: &Json) -> Result<Box<dyn FilterPlugin>, PluginBuildError> {
    Ok(Box::new(CircuitBreakerFilter))
}
