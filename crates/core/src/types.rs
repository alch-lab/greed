//! 基础值类型
//!
//! 约定：
//! - 价格/数量在之后改为定点 `i64`，杜绝浮点误差；此处先占位接口。
//! - 时间戳统一用 UTC 毫秒。

use serde::{Deserialize, Serialize};
use std::fmt;

/// 交易对，如 BTCUSDT。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// UTC 毫秒时间戳
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn from_millis(ms: i64) -> Self {
        Self(ms)
    }
    pub fn as_millis(self) -> i64 {
        self.0
    }
}

/// 交易所来源
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Exchange {
    Binance,
    ByBit,
    Okx,
}

/// 买卖方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// 价格占位
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Price(f64);

impl Price {
    pub fn new(v: f64) -> Self {
        Self(v)
    }
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

/// 数量占位
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Qty(f64);

impl Qty {
    pub fn new(v: f64) -> Self {
        Self(v)
    }
    pub fn as_f64(self) -> f64 {
        self.0
    }
}
