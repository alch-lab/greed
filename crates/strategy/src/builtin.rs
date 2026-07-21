//! 内置插件注册入口
//!
//! 把 strategy crate 自带的插件注册进 [`PluginRegistry`]。
//! signals 插件（AggDeltaTier/OiQuadrant/LargeTradeFlow）在 signals crate
//! 实现后，也在此追加注册；实验插件可用 feature flag 隔离。

use crate::registry::PluginRegistry;
use crate::{exits, filters, triggers};

/// 构造注册了全部内置插件的注册表。
pub fn builtin_registry() -> PluginRegistry {
    let mut r = PluginRegistry::new();
    register_builtin(&mut r);
    r
}

/// 把内置插件注册到给定注册表（便于与实验插件混排）。
pub fn register_builtin(r: &mut PluginRegistry) {
    // ---- 过滤器 ----
    r.register_filter("SessionFilter", filters::build_session);
    r.register_filter("CircuitBreaker", filters::build_circuit_breaker);
    r.register_filter("TrendRegimeFilter", filters::build_trend_regime);
    r.register_filter("EventCalendarFilter", filters::build_event_calendar);

    // ---- 出场 ----
    r.register_exit("BreakevenAt300", exits::build_breakeven);
    r.register_exit("TimeStop", exits::build_time_stop);
    r.register_exit("TieredTakeProfit", exits::build_tiered_tp);

    // ---- 扳机 ----
    r.register_trigger("NoopTrigger", triggers::build_noop);
    r.register_trigger("ExhaustionReversal", triggers::build_exhaustion);

    // ---- 信号（PR-8/9 填充完整逻辑；此处注册占位骨架供装配）----
    r.register_signal("AggDeltaTier", |p| {
        Ok(Box::new(signals::agg_delta::AggDeltaTier::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("OiQuadrant", |p| {
        Ok(Box::new(signals::oi_regime::OiQuadrant::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("LargeTradeFlow", |p| {
        Ok(
            Box::new(signals::large_trade_flow::LargeTradeFlow::from_params(p))
                as Box<dyn tcore::SignalPlugin>,
        )
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_registry_has_core_plugins() {
        let r = builtin_registry();
        assert!(r.filter_names().contains(&"SessionFilter"));
        assert!(r.filter_names().contains(&"CircuitBreaker"));
        assert!(r.filter_names().contains(&"TrendRegimeFilter"));
        assert!(r.exit_names().contains(&"BreakevenAt300"));
        assert!(r.exit_names().contains(&"TimeStop"));
        assert!(r.trigger_names().contains(&"NoopTrigger"));
    }
}
