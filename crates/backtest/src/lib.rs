//! backtest：事件驱动回测引擎（与实盘同代码路径）。
//!
//! - `engine`  → 多路事件时间归并回放
//! - `broker` / `account` / `fees` → 模拟撮合 / 记账 / 费率滑点
//! - `report`  → 绩效报告

pub mod account;
pub mod broker;
pub mod engine;
pub mod fees;
pub mod report;

#[cfg(test)]
mod tests {
    #[test]
    fn backtest_smoke() {
        // 冒烟测试：crate 可链接、模块可加载。
        assert_eq!(1 + 1, 2);
    }
}
