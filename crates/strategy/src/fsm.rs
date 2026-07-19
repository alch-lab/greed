//! 策略状态机。
//!
//! FLAT / WATCHING / ENTERED_A / ENTERED_GRID / HALTED(日) / DISABLED(单边)，
//! 负责把信号 → 过滤 → 扳机 → 仓位 → 出场编排成完整决策循环。
