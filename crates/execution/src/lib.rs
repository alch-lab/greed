//! execution：实盘执行
//!
//! WS 下单、仓位对账、熔断。风险红线（risk_pct / 日熔断 / 总杠杆上限）
//! 硬编码于 strategy 层，不可被配置覆盖（防误配）。

#[cfg(test)]
mod tests {
    #[test]
    fn execution_smoke() {
        // 冒烟测试：crate 可链接、模块可加载。
        assert_eq!(1 + 1, 2);
    }
}
