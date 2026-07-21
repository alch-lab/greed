//! 过滤插件（暂时提供基础实现；后续补齐 TrendRegime/EventCalendar 等）。
//!
//! 过滤器对下单意图做裁决：放行 / 降权 / 否决。
//! 这里实现与「环境标志」联动的通用过滤器：时段、周末、熔断，
//! 它们读取 `Ctx.flags` 中由状态机维护的环境信息。

use crate::registry::PluginBuildError;
use serde_json::Value as Json;
use tcore::plugin::{Ctx, FilterPlugin, OrderIntent, Verdict};

/// 时段过滤：按 `Ctx.flags["session"]`（asia/europe/us/weekend）应用仓位系数。
///
/// 参数（可选）：
/// ```toml
/// [strategy.plugins.SessionFilter]
/// us = 0.8        # 美盘降权
/// weekend = 0.5   # 周末降仓
/// # asia/europe 默认 1.0
/// ```
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
    let get = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
    Ok(Box::new(SessionFilter {
        us_scale: get("us", 0.8),
        weekend_scale: get("weekend", 0.5),
    }))
}

/// 熔断过滤：`Ctx.flags["circuit_breaker"] == "on"` 时否决所有开仓。
///
/// 状态机在日连损 2 次或日回撤 3% 时把该标志置为 "on"。
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

/// 趋势过滤：`Ctx.flags["trend"]` 为 "up"/"down" 时，否决与趋势反向的反转单。
///
/// 对应体系纪律：单边市不做反转。
pub struct TrendRegimeFilter;

impl FilterPlugin for TrendRegimeFilter {
    fn name(&self) -> &'static str {
        "TrendRegimeFilter"
    }
    fn check(&self, intent: &OrderIntent, ctx: &Ctx) -> Verdict {
        use tcore::types::Side;
        match (ctx.flag("trend"), intent.side) {
            // 下跌趋势中否决做多反转；上涨趋势中否决做空反转
            (Some("down"), Side::Buy) => Verdict::Veto("trend_down_no_long_reversal"),
            (Some("up"), Side::Sell) => Verdict::Veto("trend_up_no_short_reversal"),
            _ => Verdict::Allow,
        }
    }
}

pub fn build_trend_regime(_p: &Json) -> Result<Box<dyn FilterPlugin>, PluginBuildError> {
    Ok(Box::new(TrendRegimeFilter))
}

/// 宏观事件过滤：`Ctx.flags["event"]` 非空（如 "fomc"/"cpi"）时否决开仓。
///
/// 事件窗口由状态机根据事件日历 CSV 维护（事件前后 evt_buf 内置为 "on"）。
pub struct EventCalendarFilter;

impl FilterPlugin for EventCalendarFilter {
    fn name(&self) -> &'static str {
        "EventCalendarFilter"
    }
    fn check(&self, _intent: &OrderIntent, ctx: &Ctx) -> Verdict {
        match ctx.flag("event") {
            Some(e) if !e.is_empty() && e != "none" => Verdict::Veto("macro_event_window"),
            _ => Verdict::Allow,
        }
    }
}

pub fn build_event_calendar(_p: &Json) -> Result<Box<dyn FilterPlugin>, PluginBuildError> {
    Ok(Box::new(EventCalendarFilter))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Price, Qty, Side, Symbol, Timestamp};

    fn intent(side: Side) -> OrderIntent {
        OrderIntent {
            symbol: Symbol::new("BTCUSDT"),
            side,
            qty: Qty::from_f64(0.1),
            limit_price: None,
            stop_price: Price::from_f64(66000.0),
            tp1_price: None,
            reason: "t".into(),
            ts: Timestamp::from_millis(0),
        }
    }
    fn ctx_with(k: &str, v: &str) -> Ctx {
        let mut c = Ctx::default();
        c.flags.insert(k.into(), v.into());
        c
    }

    #[test]
    fn session_scaling() {
        let f = SessionFilter {
            us_scale: 0.8,
            weekend_scale: 0.5,
        };
        assert_eq!(
            f.check(&intent(Side::Buy), &ctx_with("session", "us")),
            Verdict::Scale(0.8)
        );
        assert_eq!(
            f.check(&intent(Side::Buy), &ctx_with("session", "weekend")),
            Verdict::Scale(0.5)
        );
        assert_eq!(
            f.check(&intent(Side::Buy), &ctx_with("session", "asia")),
            Verdict::Allow
        );
    }

    #[test]
    fn circuit_breaker_veto() {
        let f = CircuitBreakerFilter;
        assert_eq!(
            f.check(&intent(Side::Buy), &ctx_with("circuit_breaker", "on")),
            Verdict::Veto("circuit_breaker_on")
        );
        assert_eq!(f.check(&intent(Side::Buy), &Ctx::default()), Verdict::Allow);
    }

    #[test]
    fn trend_blocks_counter_reversal() {
        let f = TrendRegimeFilter;
        // 下跌趋势否决做多
        assert_eq!(
            f.check(&intent(Side::Buy), &ctx_with("trend", "down")),
            Verdict::Veto("trend_down_no_long_reversal")
        );
        // 下跌趋势允许做空
        assert_eq!(
            f.check(&intent(Side::Sell), &ctx_with("trend", "down")),
            Verdict::Allow
        );
        // 上涨趋势否决做空
        assert_eq!(
            f.check(&intent(Side::Sell), &ctx_with("trend", "up")),
            Verdict::Veto("trend_up_no_short_reversal")
        );
        // RANGE 放行
        assert_eq!(
            f.check(&intent(Side::Buy), &ctx_with("trend", "range")),
            Verdict::Allow
        );
    }
}
