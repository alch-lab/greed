//! strategy：策略编排。
//!
//! 落地插件框架：
//! - [`registry`]：插件注册表（名称 → 工厂）。
//! - [`assemble`]：从 TOML 把插件装配成 [`assemble::Strategy`]。
//! - [`builtin`]：内置插件注册入口（filters/exits/triggers）。
//! - [`filters`] / [`exits`] / [`triggers`]：基础插件实现。
//!
//!  后续在 signals crate 实现信号插件后于此注册；Phase 3 实现 [`fsm`] 状态机。

pub mod assemble;
pub mod builtin;
pub mod exits;
pub mod filters;
pub mod fsm;
pub mod registry;
pub mod triggers;

pub use assemble::{assemble, assemble_from_toml, Strategy};
pub use builtin::builtin_registry;
pub use registry::{PluginBuildError, PluginRegistry};

#[cfg(test)]
mod tests {
    #[test]
    fn strategy_smoke() {
        assert_eq!(1 + 1, 2);
    }
}
