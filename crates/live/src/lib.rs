//! live：实盘/模拟盘执行引擎。
//!
//! 与回测共用策略决策代码路径（signals → trigger → filters → exits），
//! 仅替换撮合层：
//! - `feed`   → aggTrade WebSocket 实时行情
//! - `rest`   → 币安 USDⓈ-M 合约签名 REST（HMAC-SHA256）
//! - `broker` → 统一经纪层：Dry（模拟撮合）/ Testnet（真实下单 + userTrades 对账）
//! - `engine` → 事件循环 + Journal 原子落盘（前端监控数据源）
//!
//! CLI：`greed trade --config config/base.toml --strategy config/strategy-final.toml
//!        --journal data/journal/live.json [--dry-run]`

pub mod broker;
pub mod engine;
pub mod feed;
pub mod rest;
pub mod warmup;

pub use broker::AnyBroker;
pub use engine::{EngineSnapshot, LiveConfig, LiveEngine, PositionSnap};
pub use rest::{RestClient, SymbolFilters};
pub use warmup::{warmup_engine, MAINNET_FAPI};
