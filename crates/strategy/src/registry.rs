//! 插件注册表。
//!
//! 每类插件一个名称 → 工厂函数的映射。内置插件在此注册；
//! 实验/第三方插件（如 Discord/kiyotaka 规则）通过 [`PluginRegistry::register_*`]
//! 或 feature flag 追加注册，便于受控 A/B 对比。
//!
//! 工厂函数接收插件的 TOML 参数表，返回 boxed 插件实例。

use serde_json::Value as Json;
use std::{collections::HashMap, format};
use tcore::plugin::{ExitPlugin, FilterPlugin, SignalPlugin, TriggerPlugin};

/// 插件构造错误
#[derive(Debug, thiserror::Error)]
pub enum PluginBuildError {
    #[error("参数解析失败: {0}")]
    BadParams(String),
}

/// 各类插件的工厂函数签名
pub type SignalFactory = fn(&Json) -> Result<Box<dyn SignalPlugin>, PluginBuildError>;
pub type FilterFactory = fn(&Json) -> Result<Box<dyn FilterPlugin>, PluginBuildError>;
pub type TriggerFactory = fn(&Json) -> Result<Box<dyn TriggerPlugin>, PluginBuildError>;
pub type ExitFactory = fn(&Json) -> Result<Box<dyn ExitPlugin>, PluginBuildError>;

/// 插件注册表
#[derive(Default)]
pub struct PluginRegistry {
    signals: HashMap<&'static str, SignalFactory>,
    filters: HashMap<&'static str, FilterFactory>,
    triggers: HashMap<&'static str, TriggerFactory>,
    exits: HashMap<&'static str, ExitFactory>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_signal(&mut self, name: &'static str, f: SignalFactory) -> &mut Self {
        self.signals.insert(name, f);
        self
    }

    pub fn register_filter(&mut self, name: &'static str, f: FilterFactory) -> &mut Self {
        self.filters.insert(name, f);
        self
    }

    pub fn register_trigger(&mut self, name: &'static str, f: TriggerFactory) -> &mut Self {
        self.triggers.insert(name, f);
        self
    }

    pub fn register_exit(&mut self, name: &'static str, f: ExitFactory) -> &mut Self {
        self.exits.insert(name, f);
        self
    }

    pub fn build_signal(
        &self,
        name: &str,
        params: &Json,
    ) -> Result<Box<dyn SignalPlugin>, PluginBuildError> {
        self.signals
            .get(name)
            .ok_or_else(|| PluginBuildError::BadParams(format!("Unknow singal plugin: {}", name)))
            .and_then(|f| f(params))
    }

    pub fn build_filter(
        &self,
        name: &str,
        params: &Json,
    ) -> Result<Box<dyn FilterPlugin>, PluginBuildError> {
        self.filters
            .get(name)
            .ok_or_else(|| PluginBuildError::BadParams(format!("Unknow filter plugin: {}", name)))
            .and_then(|f| f(params))
    }

    pub fn build_trigger(
        &self,
        name: &str,
        params: &Json,
    ) -> Result<Box<dyn TriggerPlugin>, PluginBuildError> {
        self.triggers
            .get(name)
            .ok_or_else(|| PluginBuildError::BadParams(format!("Unknow trigger plugin: {}", name)))
            .and_then(|f| f(params))
    }

    pub fn build_exit(
        &self,
        name: &str,
        params: &Json,
    ) -> Result<Box<dyn ExitPlugin>, PluginBuildError> {
        self.exits
            .get(name)
            .ok_or_else(|| PluginBuildError::BadParams(format!("Unknow exit plugin: {}", name)))
            .and_then(|f| f(params))
    }

    pub fn signal_names(&self) -> Vec<&'static str> {
        self.signals.keys().copied().collect()
    }

    pub fn filter_names(&self) -> Vec<&'static str> {
        self.filters.keys().copied().collect()
    }

    pub fn trigger_names(&self) -> Vec<&'static str> {
        self.triggers.keys().copied().collect()
    }

    pub fn exit_names(&self) -> Vec<&'static str> {
        self.exits.keys().copied().collect()
    }
}

/// 空参数表（插件无参数时使用）
pub fn no_params() -> Json {
    Json::Object(serde_json::Map::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::plugin::{Ctx, OrderIntent, Signal, Verdict};
    use tcore::Event;

    struct DummySignal;
    impl SignalPlugin for DummySignal {
        fn name(&self) -> &'static str {
            "DummySignal"
        }
        fn on_event(&mut self, _ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
            vec![]
        }
    }
    struct DummyFilter;
    impl FilterPlugin for DummyFilter {
        fn name(&self) -> &'static str {
            "DummyFilter"
        }
        fn check(&self, _i: &OrderIntent, _ctx: &Ctx) -> Verdict {
            Verdict::Allow
        }
    }

    fn make_signal(_p: &Json) -> Result<Box<dyn SignalPlugin>, PluginBuildError> {
        Ok(Box::new(DummySignal))
    }
    fn make_filter(_p: &Json) -> Result<Box<dyn FilterPlugin>, PluginBuildError> {
        Ok(Box::new(DummyFilter))
    }

    #[test]
    fn register_and_build() {
        let mut r = PluginRegistry::new();
        r.register_signal("DummySignal", make_signal);
        r.register_filter("DummyFilter", make_filter);

        let s = r.build_signal("DummySignal", &no_params()).unwrap();
        assert_eq!(s.name(), "DummySignal");
        let f = r.build_filter("DummyFilter", &no_params()).unwrap();
        assert_eq!(f.name(), "DummyFilter");

        assert!(r.build_signal("Nope", &no_params()).is_err());
        assert!(r.signal_names().contains(&"DummySignal"));
    }
}
