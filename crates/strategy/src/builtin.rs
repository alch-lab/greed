//! 内置插件注册入口
use crate::registry::PluginRegistry;
use crate::{exits, filters, triggers};

pub fn builtin_registry() -> PluginRegistry {
    let mut r = PluginRegistry::new();
    register_builtin(&mut r);
    r
}

pub fn register_builtin(r: &mut PluginRegistry) {
    r.register_filter("SessionFilter", filters::build_session);
    r.register_filter("CircuitBreaker", filters::build_circuit_breaker);
    r.register_filter("TrendRegimeFilter", filters::build_trend_regime);
    r.register_filter("EventCalendarFilter", filters::build_event_calendar);
    r.register_filter("FundingFilter", filters::build_funding);
    r.register_filter("OiConfirmFilter", filters::build_oi_confirm);

    r.register_exit("BreakevenAt300", exits::build_breakeven);
    r.register_exit("TimeStop", exits::build_time_stop);
    r.register_exit("TieredTakeProfit", exits::build_tiered_tp);
    r.register_exit("PercentTrail", exits::build_percent_trail);

    r.register_trigger("NoopTrigger", triggers::build_noop);
    r.register_trigger("ExhaustionReversal", triggers::build_exhaustion);
    r.register_trigger("SimpleRenkoReversal", triggers::build_simple_renko);
    r.register_trigger("ImpulseFollow", triggers::build_impulse_follow);
    r.register_trigger("EmaFollow", triggers::build_ema_follow);
    r.register_trigger("MeanReversionFollow", triggers::build_mean_reversion_follow);
    r.register_trigger("BollingerReversionFollow", triggers::build_bollinger_reversion);
    r.register_trigger("MrAtrFollow", triggers::build_mr_atr_follow);
    r.register_trigger("PyramidMrFollow", triggers::build_pyramid_mr_follow);
    r.register_trigger("RegimeSwitchFollow", triggers::build_regime_switch_follow);
    r.register_trigger("SqueezeFollow", triggers::build_squeeze_follow);

    r.register_signal("EmaCross", |p| {
        Ok(Box::new(signals::ema_cross::EmaCross::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("EmaMeanReversion", |p| {
        Ok(Box::new(signals::ema_mean_reversion::EmaMeanReversion::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("EmaMeanReversionV2", |p| {
        Ok(Box::new(signals::ema_mean_reversion_v2::EmaMeanReversionV2::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("BollingerMeanReversion", |p| {
        Ok(Box::new(signals::bollinger_mr::BollingerMeanReversion::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("DualTfMeanReversion", |p| {
        Ok(Box::new(signals::dual_tf_mr::DualTfMeanReversion::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("VolAdaptiveMr", |p| {
        Ok(Box::new(signals::vol_adaptive_mr::VolAdaptiveMr::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("RenkoBricks", |p| {
        let g = |k: &str, d: f64| p.get(k).and_then(|v| v.as_f64()).unwrap_or(d);
        let trend = g("t_ren_ticks", 200.0) * g("tick_usd", 0.5);
        let reversal = g("r_run_ticks", 124.0) * g("tick_usd", 0.5);
        Ok(Box::new(signals::renko::RenkoBricks::new(
            signals::renko::RenkoConfig::trend_reversal_usd(trend, reversal),
        )) as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("AggDeltaTier", |p| {
        Ok(Box::new(signals::agg_delta::AggDeltaTier::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("OiQuadrant", |p| {
        Ok(Box::new(signals::oi_regime::OiQuadrant::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("LargeTradeFlow", |p| {
        Ok(Box::new(signals::large_trade_flow::LargeTradeFlow::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("LargeTradeImpulse", |p| {
        Ok(Box::new(signals::large_trade_impulse::LargeTradeImpulse::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("RegimeSwitchMr", |p| {
        Ok(Box::new(signals::regime_switch_mr::RegimeSwitchMr::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("OiTracker", |p| {
        Ok(Box::new(signals::oi_tracker::OiTracker::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
    });
    r.register_signal("SqueezeCompletion", |p| {
        Ok(Box::new(signals::squeeze_completion::SqueezeCompletion::from_params(p))
            as Box<dyn tcore::SignalPlugin>)
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
        assert!(r.trigger_names().contains(&"NoopTrigger"));
    }
}
