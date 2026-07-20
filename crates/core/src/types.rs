//! 基础值类型
//!
//! 核心约定：
//! - **价格与数量用定点 `i64`**，杜绝浮点误差（回测与实盘数值一致性的根基）。
//! - 时间戳统一 UTC 毫秒（`i64`）。
//! - 所有类型实现 `serde`，可落盘/跨进程传输。

use serde::{Deserialize, Serialize};
use std::fmt;

// ============================================================================
// 定点数（Fixed-point）
// ============================================================================

/// 价格定点：1 单位 = 1e-8 美元（8 位小数，覆盖加密 tick 精度）。
///
/// 内部存 `i64`，表示 `value × 1e-8` 美元。
/// 例：`Price::from_f64(67000.5)` 内部为 `6_700_050_000_000`。
/// 交易对，如 BTCUSDT。
pub const PRICE_SCALE: i64 = 100_000_000; // 1e8

/// 数量定点：1 单位 = 1e-8（币 / 合约张数），与价格同精度便于相乘。
pub const QTY_SCALE: i64 = 100_000_000; // 1e8

/// 定点价格
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Price(i64);

impl Price {
    pub const ZERO: Price = Price(0);

    /// 从原始定点整数构造(value 已是 x1e8 后的小数)
    pub const fn from_raw(raw: i64) -> Self {
        Price(raw)
    }

    /// 从浮点数构造(仅用于配置/测试边界；生产路径尽量使用 from_raw / from_ticks)
    pub fn from_f64(v: f64) -> Self {
        Price((v * PRICE_SCALE as f64).round() as i64)
    }

    /// 按 tick 构造：`ticks × tick_size`。tick_size 以美元计（如 0.5）。
    pub fn from_ticks(ticks: i64, tick_size: f64) -> Self {
        Price::from_f64(ticks as f64 * tick_size)
    }

    /// 原始定点值(x1e8)
    pub const fn raw(self) -> i64 {
        self.0
    }

    /// 转浮点（仅用于展示/报告，不用于计算比较）。
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / PRICE_SCALE as f64
    }

    /// 价格差（绝对值），仍是 Price 量级。
    pub fn abs_diff(self, other: Price) -> Price {
        Price((self.0 - other.0).abs())
    }

    /// 相对另一价格的百分比差（基点 bp，1bp=0.01%）。
    /// 返回 i64 表示 bp，避免浮点。other 为 0 时返回 i64::MAX。
    pub fn diff_bps(self, other: Price) -> i64 {
        if other.0 == 0 {
            return i64::MAX;
        }
        // (self - other) / other * 10000
        ((self.0 - other.0) as i128 * 10_000 / other.0 as i128) as i64
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.2}", self.to_f64())
    }
}

/// 定点数量(币数或张数)
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Qty(i64);

impl Qty {
    pub const ZERO: Qty = Qty(0);

    pub const fn from_raw(raw: i64) -> Self {
        Qty(raw)
    }

    pub const fn from_f64(v: f64) -> Self {
        Qty((v * QTY_SCALE as f64).round() as i64)
    }

    pub const fn raw(self) -> i64 {
        self.0
    }

    pub fn to_f64(self) -> f64 {
        self.0 as f64 / QTY_SCALE as f64
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.4}", self.to_f64())
    }
}

/// 名义价值（USD）= Price × Qty。
/// 结果用 f64 表示美元（名义值只做量级判断，不参与累加守恒，故允许浮点）
pub fn notional_usd(price: Price, qty: Qty) -> f64 {
    price.to_f64() * qty.to_f64()
}

// ============================================================================
// 交易标识与时间
// ============================================================================

/// 交易对，如 BTCUSDT
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
    pub const ZERO: Timestamp = Timestamp(0);

    pub fn from_millis(ms: i64) -> Self {
        Self(ms)
    }
    pub fn as_millis(self) -> i64 {
        self.0
    }
    /// 距另一时间戳的毫秒差(self - other,可为负)
    pub fn diff_ms(self, other: Timestamp) -> i64 {
        self.0 - other.0
    }
    /// 增加毫秒
    pub fn add_ms(self, ms: i64) -> Timestamp {
        Timestamp(self.0 + ms)
    }
}
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 简单可读格式：秒级时间戳（报告层再做时区格式化）
        write!(f, "{}ms", self.0)
    }
}

/// 交易所来源
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Exchange {
    Binance,
    ByBit,
    Okx,
}

impl Exchange {
    pub fn as_str(self) -> &'static str {
        match self {
            Exchange::Binance => "binance",
            Exchange::ByBit => "bybit",
            Exchange::Okx => "okx",
        }
    }
}
/// 买卖方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// 反方向
    pub fn opposite(self) -> Side {
        match self {
            Side::Buy => Self::Sell,
            Side::Sell => Self::Buy,
        }
    }
    /// 用于带符号计算：Buy=+1，Sell=-1
    pub fn sign(self) -> i64 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_roundtrip_raw_f64() {
        let p = Price::from_f64(67000.5);
        assert!((p.to_f64() - 67000.5).abs() < 1e-6);
        assert_eq!(p, Price::from_raw(6_700_050_000_000));
    }

    #[test]
    fn price_from_ticks() {
        // 200 tick × 0.5 = 100 美元
        let p = Price::from_ticks(200, 0.5);
        assert!((p.to_f64() - 100.0).abs() < 1e-6);
    }

    #[test]
    fn price_ordering_and_diff() {
        let a = Price::from_f64(100.0);
        let b = Price::from_f64(101.0);
        assert!(a < b);
        assert!((b.abs_diff(a).to_f64() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn price_diff_bps() {
        // 100 -> 101 = +1% = +100bp
        let a = Price::from_f64(100.0);
        let b = Price::from_f64(101.0);
        assert_eq!(b.diff_bps(a), 100);
        // 反向 -100bp 近似（向下取整允许 ±1）
        assert!((a.diff_bps(b) - -99).abs() <= 1);
    }

    #[test]
    fn qty_roundtrip() {
        let q = Qty::from_f64(1.23456789);
        assert!((q.to_f64() - 1.23456789).abs() < 1e-6);
    }

    #[test]
    fn notional_calc() {
        let n = notional_usd(Price::from_f64(67000.0), Qty::from_f64(0.5));
        assert!((n - 33500.0).abs() < 1e-6);
    }

    #[test]
    fn timestamp_ops() {
        let t0 = Timestamp::from_millis(1_700_000_000_000);
        let t1 = t0.add_ms(1500);
        assert_eq!(t1.diff_ms(t0), 1500);
        assert!(t1 > t0);
    }

    #[test]
    fn side_sign_and_opposite() {
        assert_eq!(Side::Buy.sign(), 1);
        assert_eq!(Side::Sell.sign(), -1);
        assert_eq!(Side::Buy.opposite(), Side::Sell);
    }

    #[test]
    fn symbol_display() {
        assert_eq!(Symbol::new("BTCUSDT").to_string(), "BTCUSDT");
    }

    #[test]
    fn serde_roundtrip() {
        let p = Price::from_f64(123.456);
        let s = serde_json::to_string(&p).unwrap();
        let back: Price = serde_json::from_str(&s).unwrap();
        assert_eq!(p, back);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// 定点价格在合理范围内 from_f64 -> to_f64 往返误差 < 1e-6
        #[test]
        fn price_f64_roundtrip(v in 0.0001f64..1_000_000.0) {
            let p = Price::from_f64(v);
            prop_assert!((p.to_f64() - v).abs() < 1e-6);
        }

        /// diff_bps 与浮点演算一致（±1bp 容差，定点向下取整）
        #[test]
        fn diff_bps_matches_float(a in 1.0f64..100_000.0, b in 1.0f64..100_000.0) {
            let pa = Price::from_f64(a);
            let pb = Price::from_f64(b);
            let expected = (b - a) / a * 10_000.0;
            prop_assert!((pb.diff_bps(pa) as f64 - expected).abs() <= 1.0);
        }

        /// 数量往返
        #[test]
        fn qty_f64_roundtrip(v in 0.0001f64..10_000.0) {
            let q = Qty::from_f64(v);
            prop_assert!((q.to_f64() - v).abs() < 1e-6);
        }
    }
}
