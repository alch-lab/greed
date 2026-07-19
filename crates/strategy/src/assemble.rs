//! 策略装配：从 TOML 配置组装一套 Strategy。
//!
//! 策略 = signals[] + filters[] + trigger + exits[] 的声明式组合；
//! 未知插件名给出清晰报错，便于配置排错。
