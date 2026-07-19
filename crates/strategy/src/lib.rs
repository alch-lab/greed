//! strategy：策略编排。
//!
//! - `registry` / `assemble` → 插件注册表 + TOML 装配
//! - `filters` / `triggers` / `exits` → 具体插件实现
//! - `fsm` → Phase 3（策略状态机 FLAT/WATCHING/ENTERED_*/HALTED/DISABLED）

pub mod assemble;
pub mod exits;
pub mod filter;
pub mod fsm;
pub mod registry;
pub mod triggers;

#[cfg(test)]
mod tests {
    #[test]
    fn strategy_smoke() {
        // 冒烟测试：crate 可链接、模块可加载。
        assert_eq!(1 + 1, 2);
    }
}
