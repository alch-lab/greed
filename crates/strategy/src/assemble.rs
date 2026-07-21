//! 策略装配：从 TOML 配置把插件组装成一套 [`Strategy`]。
//!
//! 配置形态（`config/strategy.toml`）：
//! ```toml
//! [strategy]
//! signals = ["AggDeltaTier", "OiQuadrant"]
//! filters = ["TrendRegimeFilter", "SessionFilter"]
//! trigger = "ExhaustionReversal"
//! exits   = ["BreakevenAt300", "TimeStop"]
//!
//! [strategy.plugins.AggDeltaTier]   # 各插件的参数表（可选）
//! t1 = 1.0
//! ```
//!
//! 装配规则：
//! - 插件按名称从注册表构造，参数表从 `[strategy.plugins.<Name>]` 读取（缺省为空表）。
//! - 未知名称给出**列出可用插件**的清晰报错。

use crate::registry::{no_params, PluginRegistry};
use serde::Deserialize;
use serde_json::Value as Json;
use tcore::plugin::{ExitPlugin, FilterPlugin, SignalPlugin, TriggerPlugin};

/// 一套装配好的策略（可执行单元）
pub struct Strategy {
    pub name: String,
    pub signals: Vec<Box<dyn SignalPlugin>>,
    pub filters: Vec<Box<dyn FilterPlugin>>,
    pub trigger: Box<dyn TriggerPlugin>,
    pub exits: Vec<Box<dyn ExitPlugin>>,
}

impl std::fmt::Debug for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.describe())
    }
}

impl Strategy {
    /// 便于日志/报告标识
    pub fn describe(&self) -> String {
        let s: Vec<_> = self.signals.iter().map(|p| p.name()).collect();
        let f: Vec<_> = self.filters.iter().map(|p| p.name()).collect();
        let e: Vec<_> = self.exits.iter().map(|p| p.name()).collect();

        format!(
            "Strategy[{}] signals={:?} filters={:?} trigger={} exits={:?}",
            self.name,
            s,
            f,
            self.trigger.name(),
            e
        )
    }
}

/// TOML `[strategy]` 段的反序列化结构
#[derive(Debug, Deserialize)]
pub struct StrategySepc {
    /// 策略名(可选，默认 "default")
    #[serde(default = "default_name")]
    pub name: String,
    #[serde(default)]
    pub signals: Vec<String>,
    #[serde(default)]
    pub filters: Vec<String>,
    pub trigger: String,
    #[serde(default)]
    pub exits: Vec<String>,
    /// 各插件参数表: name -> table
    #[serde(default)]
    pub plugins: std::collections::HashMap<String, Json>,
}

fn default_name() -> String {
    "default".to_string()
}

/// 顶层配置
#[derive(Debug, Deserialize)]
pub struct StrategyConfig {
    pub strategy: StrategySepc,
}

/// 装配错误
#[derive(Debug, thiserror::Error)]
pub enum AssembleError {
    #[error("config parse error: {0}")]
    Parse(String),
    #[error("plugin build error: {0}")]
    Plugin(String),
}

/// 从TOML 字符串匹配装配策略
pub fn assemble_from_toml(
    toml_str: &str,
    registry: &PluginRegistry,
) -> Result<Strategy, AssembleError> {
    let cfg: StrategyConfig =
        toml::from_str(toml_str).map_err(|e| AssembleError::Parse(e.to_string()))?;
    assemble(&cfg.strategy, registry)
}

/// 从已解析的 spec 装配策略
pub fn assemble(spec: &StrategySepc, registry: &PluginRegistry) -> Result<Strategy, AssembleError> {
    let params = |name: &str| -> Json { spec.plugins.get(name).cloned().unwrap_or_else(no_params) };

    let signals = spec
        .signals
        .iter()
        .map(|n| {
            registry
                .build_signal(n, &params(n))
                .map_err(|e| AssembleError::Plugin(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let filters = spec
        .filters
        .iter()
        .map(|n| {
            registry
                .build_filter(n, &params(n))
                .map_err(|e| AssembleError::Plugin(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let trigger = registry
        .build_trigger(&spec.trigger, &params(&spec.trigger))
        .map_err(|e| {
            AssembleError::Plugin(format!("{}（可用扳机: {:?}）", e, registry.trigger_names()))
        })?;

    let exits = spec
        .exits
        .iter()
        .map(|n| {
            registry
                .build_exit(n, &params(n))
                .map_err(|e| AssembleError::Plugin(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Strategy {
        name: spec.name.clone(),
        signals,
        filters,
        trigger,
        exits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::PluginBuildError;
    use tcore::plugin::{Ctx, ExitAction, OrderIntent, Position, Signal, Verdict};
    use tcore::Event;

    // ---- 一批测试用插件 ----
    struct SigA;
    impl SignalPlugin for SigA {
        fn name(&self) -> &'static str {
            "SigA"
        }
        fn on_event(&mut self, _e: &Event, _c: &Ctx) -> Vec<Signal> {
            vec![]
        }
    }
    struct FiltAllow;
    impl FilterPlugin for FiltAllow {
        fn name(&self) -> &'static str {
            "FiltAllow"
        }
        fn check(&self, _i: &OrderIntent, _c: &Ctx) -> Verdict {
            Verdict::Allow
        }
    }
    struct TrigFire;
    impl TriggerPlugin for TrigFire {
        fn name(&self) -> &'static str {
            "TrigFire"
        }
        fn should_fire(&self, _s: &[Signal], _c: &Ctx) -> Option<OrderIntent> {
            None
        }
    }
    struct ExitNoop;
    impl ExitPlugin for ExitNoop {
        fn name(&self) -> &'static str {
            "ExitNoop"
        }
        fn manage(&self, _p: &Position, _c: &Ctx) -> Vec<ExitAction> {
            vec![]
        }
    }

    fn reg() -> PluginRegistry {
        let mut r = PluginRegistry::new();
        r.register_signal("SigA", |_| Ok(Box::new(SigA) as Box<dyn SignalPlugin>));
        r.register_filter("FiltAllow", |_| {
            Ok(Box::new(FiltAllow) as Box<dyn FilterPlugin>)
        });
        r.register_trigger("TrigFire", |_| {
            Ok(Box::new(TrigFire) as Box<dyn TriggerPlugin>)
        });
        r.register_exit(
            "ExitNoop",
            |_| Ok(Box::new(ExitNoop) as Box<dyn ExitPlugin>),
        );
        r
    }

    #[test]
    fn assembles_full_strategy() {
        let toml = r#"
[strategy]
name = "demo"
signals = ["SigA"]
filters = ["FiltAllow"]
trigger = "TrigFire"
exits = ["ExitNoop"]
"#;
        let s = assemble_from_toml(toml, &reg()).unwrap();
        assert_eq!(s.name, "demo");
        assert_eq!(s.signals.len(), 1);
        assert_eq!(s.filters.len(), 1);
        assert_eq!(s.trigger.name(), "TrigFire");
        assert_eq!(s.exits.len(), 1);
        assert!(s.describe().contains("SigA"));
    }

    #[test]
    fn unknown_trigger_lists_available() {
        let toml = r#"
[strategy]
trigger = "Nope"
"#;
        let err = assemble_from_toml(toml, &reg()).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("TrigFire"), "报错应列出可用扳机: {}", msg);
    }

    #[test]
    fn params_passed_to_plugin() {
        // 验证参数表能传到工厂
        fn echo_factory(p: &Json) -> Result<Box<dyn SignalPlugin>, PluginBuildError> {
            let v = p.get("x").and_then(|x| x.as_i64()).unwrap_or(0);
            assert_eq!(v, 42);
            Ok(Box::new(SigA))
        }
        let mut r = reg();
        r.register_signal("ParamSig", echo_factory);
        let toml = r#"
[strategy]
signals = ["ParamSig"]
trigger = "TrigFire"

[strategy.plugins.ParamSig]
x = 42
"#;
        let s = assemble_from_toml(toml, &r).unwrap();
        assert_eq!(s.signals.len(), 1);
    }
}
