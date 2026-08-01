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
    r.register_exit(
        "OrderFlowTradeManagement",
        exits::build_orderflow_management,
    );
    r.register_trigger("OrderFlowEntry", triggers::build_orderflow);
    r.register_signal("OrderFlowExhaustion", |p| {
        Ok(
            Box::new(signals::orderflow_exhaustion::OrderFlowExhaustion::from_params(p))
                as Box<dyn tcore::SignalPlugin>,
        )
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn builtin_registry_has_layered_orderflow_plugins() {
        let r = builtin_registry();
        assert!(r.filter_names().contains(&"SessionFilter"));
        assert!(r.filter_names().contains(&"CircuitBreaker"));
        assert_eq!(r.signal_names(), vec!["OrderFlowExhaustion"]);
        assert_eq!(r.trigger_names(), vec!["OrderFlowEntry"]);
    }
}
