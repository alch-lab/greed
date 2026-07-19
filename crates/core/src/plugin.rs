//! 插件
//! 设计原则：策略 = 插件的组合装配，四大类别：
//! - [`SignalPlugin`]  信号源：从事件流产出观察信号，不直接下单
//! - [`FilterPlugin`]  过滤器：对下单意图一票否决 / 降权
//! - [`TriggerPlugin`] 扳机：在信号与过滤都满足后决定是否扣扳机
//! - [`ExitPlugin`]    出场：持仓期间的保本 / 止盈 / 反手 / 时间止损

use crate::event::Event;
use serde::{Deserialize, Serialize};

/// 信号输出
pub enum Signal {
    Placeholder,
}

/// 下单意图
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderIntent {
    pub placeholder: (),
}

/// 过滤器加载
#[derive(Debug, PartialEq)]
pub enum Verdict {
    /// 放行
    Allow,
    /// 放行但仓位乘以系数（如美盘降权，周末降仓）
    Scale(f64),
    /// 否决（附原因，静态字符串，便于日志聚合）
    Veto(&'static str),
}

//// 插件共享只读上下文
#[derive(Debug, Default)]
pub struct Ctx {
    pub placeholder: (),
}

/// 信号源插件
pub trait SignalPlugin: Send {
    fn name(&self) -> &'static str;
    fn on_event(&mut self, ev: &Event, ctx: &Ctx) -> Vec<Signal>;
}

/// 过滤器插件
pub trait FilterPlugin: Send {
    fn name(&self) -> &'static str;
    fn check(&self, intent: &OrderIntent, ctx: &Ctx) -> Verdict;
}

/// 入场插件
pub trait TriggerPlugin: Send {
    fn name(&self) -> &'static str;
    /// 返回 `Some(intent)` 表示入场
    fn should_fire(&self, signals: &[Signal], ctx: &Ctx) -> Option<OrderIntent>;
}

/// 出场管理插件
pub trait ExitPlugin: Send {
    fn name(&self) -> &'static str;
    fn manage(&self, ctx: &Ctx) -> Vec<&'static str>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verdict_semantics() {
        assert_eq!(Verdict::Allow, Verdict::Allow);
        assert_eq!(Verdict::Scale(0.5), Verdict::Scale(0.5));
        assert_eq!(Verdict::Veto("trend"), Verdict::Veto("trend"));
    }
}
