//! 统一事件 采集/回测/实盘 共用同一事件模型
//! //!
//! 采集（WS）、回测（历史 Parquet）、实盘共用同一事件模型。
//! 信号引擎与策略只消费 [`Event`]，不关心其来源

use crate::types::{notional_usd, Exchange, Price, Qty, Side, Symbol, Timestamp};
use serde::{Deserialize, Serialize};

/// 一笔逐笔交易
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub ts: Timestamp,
    pub exchange: Exchange,
    pub symbol: Symbol,
    pub price: Price,
    pub qty: Qty,
    /// 买方是否为 marker
    /// true -> 卖方主动 (taker sell)
    /// false -> 买方主动 (taker buy)
    pub is_buyer_maker: bool,
}

impl Trade {
    /// 成交的主动方向(taker 方向)
    pub fn taker_side(&self) -> Side {
        if self.is_buyer_maker {
            Side::Sell
        } else {
            Side::Buy
        }
    }
    /// 带符号的主动量：买方主动为正，卖方主动为负(用于 Delta 累加)
    pub fn signed_qty(&self) -> f64 {
        self.qty.to_f64() * self.taker_side().sign() as f64
    }
    /// 带符号的主动名义额(USD)
    pub fn signed_notional(&self) -> f64 {
        notional_usd(self.price, self.qty) * self.taker_side().sign() as f64
    }
    /// 名义额（USD，无符号)
    pub fn notional(&self) -> f64 {
        notional_usd(self.price, self.qty)
    }
}

/// 订单薄快照（某时刻的深度）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub ts: Timestamp,
    pub exchange: Exchange,
    pub symbol: Symbol,
    /// bid 档位 (price, qty)，按价格**降序**（最优买价在前）。
    pub bids: Vec<(Price, Qty)>,
    /// ask 档位 (price, qty)，按价格**升序**（最优卖价在前）。
    pub asks: Vec<(Price, Qty)>,
}

impl BookSnapshot {
    /// 最优买价
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first().map(|(p, _)| *p)
    }
    /// 最优卖价
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first().map(|(p, _)| *p)
    }
    /// 中间价（best_bid 与 best_ask 均值），任一侧缺失则返回 None
    pub fn mid_price(&self) -> Option<Price> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some(Price::from_raw((b.raw() + a.raw()) / 2)),
            _ => None,
        }
    }
    /// 价差（ask - bid，可为0）
    pub fn spread(&self) -> Option<Price> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some(a.abs_diff(b)),
            _ => None,
        }
    }
    /// 某百分比区间内的买方挂单名义量（USD）。
    /// `band` 为小数（如 0.01 = 1%），统计价格 ∈ [mid·(1−band), mid) 的 bid。
    pub fn bid_qty_within(&self, band: f64) -> f64 {
        match self.mid_price() {
            None => 0.0,
            Some(mid) => {
                let lo = mid.to_f64() * (1.0 - band);
                let hi = mid.to_f64();
                self.bids
                    .iter()
                    .filter(|(p, _)| {
                        let pf = p.to_f64();
                        pf >= lo && pf < hi
                    })
                    .map(|(p, q)| notional_usd(*p, *q))
                    .sum()
            }
        }
    }
    /// 某百分比区间内的卖方挂单名义量（USD），统计价格 ∈ (mid, mid·(1+band)]。
    pub fn ask_qty_within(&self, band: f64) -> f64 {
        match self.mid_price() {
            None => 0.0,
            Some(mid) => {
                let lo = mid.to_f64();
                let hi = mid.to_f64() * (1.0 + band);
                self.asks
                    .iter()
                    .filter(|(p, _)| {
                        let pf = p.to_f64();
                        pf > lo && pf <= hi
                    })
                    .map(|(p, q)| notional_usd(*p, *q))
                    .sum()
            }
        }
    }
}
/// 持仓量刻度
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OiTick {
    pub ts: Timestamp,
    pub exchange: Exchange,
    pub symbol: Symbol,
    /// 以 USD 计的持仓量（聚合时统一折算）
    pub oi_usd: f64,
}

/// 系统统一事件
///
/// 信号引擎与策略只消费 `Event`，不关心其来着历史 Parquet（回测）
/// 还是实时 WebSocket （实盘）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    Trade(Trade),
    Book(BookSnapshot),
    Oi(OiTick),
    /// 逻辑时钟心跳
    Timer(Timestamp),
}

impl Event {
    /// 事件时间，用于回测的事件时间归并排序
    pub fn ts(&self) -> Timestamp {
        match self {
            Event::Trade(t) => t.ts,
            Event::Book(b) => b.ts,
            Event::Oi(o) => o.ts,
            Event::Timer(ts) => *ts,
        }
    }
    /// 事件来源交易所：Timer 无来源返回 None
    pub fn exchange(&self) -> Option<Exchange> {
        match self {
            Event::Trade(t) => Some(t.exchange),
            Event::Book(b) => Some(b.exchange),
            Event::Oi(o) => Some(o.exchange),
            Event::Timer(_) => None,
        }
    }
    pub fn symbol(&self) -> Option<&Symbol> {
        match self {
            Event::Trade(t) => Some(&t.symbol),
            Event::Book(b) => Some(&b.symbol),
            Event::Oi(o) => Some(&o.symbol),
            Event::Timer(_) => None,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sym() -> Symbol {
        Symbol::new("BTCUSDT")
    }
    fn ts(ms: i64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    fn trade(price: f64, qty: f64, is_buyer_maker: bool) -> Trade {
        Trade {
            ts: ts(1000),
            exchange: Exchange::BinanceFutures,
            symbol: sym(),
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
            is_buyer_maker,
        }
    }

    #[test]
    fn taker_side_mapping() {
        // is_buyer_maker=true → 卖方主动
        assert_eq!(trade(100.0, 1.0, true).taker_side(), Side::Sell);
        assert_eq!(trade(100.0, 1.0, false).taker_side(), Side::Buy);
    }

    #[test]
    fn signed_qty_and_notional() {
        let buy = trade(100.0, 2.0, false); // 买方主动
        assert!((buy.signed_qty() - 2.0).abs() < 1e-9);
        assert!((buy.signed_notional() - 200.0).abs() < 1e-6);
        let sell = trade(100.0, 2.0, true); // 卖方主动
        assert!((sell.signed_qty() - -2.0).abs() < 1e-9);
        assert!((sell.signed_notional() - -200.0).abs() < 1e-6);
    }

    fn book() -> BookSnapshot {
        // 中间价 100；bid 99.9/99.5/99.0，ask 100.1/100.5/101.0
        BookSnapshot {
            ts: ts(1000),
            exchange: Exchange::BinanceFutures,
            symbol: sym(),
            bids: vec![
                (Price::from_f64(99.9), Qty::from_f64(1.0)),
                (Price::from_f64(99.5), Qty::from_f64(2.0)),
                (Price::from_f64(99.0), Qty::from_f64(5.0)),
            ],
            asks: vec![
                (Price::from_f64(100.1), Qty::from_f64(1.0)),
                (Price::from_f64(100.5), Qty::from_f64(2.0)),
                (Price::from_f64(101.0), Qty::from_f64(5.0)),
            ],
        }
    }

    #[test]
    fn book_mid_and_spread() {
        let b = book();
        let mid = b.mid_price().unwrap();
        assert!((mid.to_f64() - 100.0).abs() < 0.01);
        let sp = b.spread().unwrap();
        assert!((sp.to_f64() - 0.2).abs() < 0.01);
    }

    #[test]
    fn book_band_qty() {
        let b = book();
        // 中间价 100。0.1% 区间只含最优档。
        let bid_narrow = b.bid_qty_within(0.001);
        assert!((bid_narrow - 99.9).abs() < 0.5, "bid_narrow={}", bid_narrow);
        let ask_narrow = b.ask_qty_within(0.001);
        assert!(
            (ask_narrow - 100.1).abs() < 0.5,
            "ask_narrow={}",
            ask_narrow
        );

        // 0.6% 区间 bid 侧 [99.4, 100) → 99.9×1 + 99.5×2 = 298.9（不含 99.0）
        let bid06 = b.bid_qty_within(0.006);
        assert!((bid06 - (99.9 + 199.0)).abs() < 0.5, "bid06={}", bid06);
        // ask 侧 (100, 100.6] → 100.1×1 + 100.5×2 = 301.1（不含 101.0）
        let ask06 = b.ask_qty_within(0.006);
        assert!((ask06 - (100.1 + 201.0)).abs() < 0.5, "ask06={}", ask06);

        // 1% 边界含 99.0/101.0（浮点边界 inclusive，容忍）
        let bid1 = b.bid_qty_within(0.01);
        assert!(bid1 >= 298.9, "bid1 应至少含近端档位: {}", bid1);
    }

    #[test]
    fn event_ts_extraction() {
        let e = Event::Trade(trade(100.0, 1.0, false));
        assert_eq!(e.ts(), ts(1000));
        let e2 = Event::Timer(ts(5000));
        assert_eq!(e2.ts(), ts(5000));
        assert_eq!(e2.exchange(), None);
    }

    #[test]
    fn event_serde_roundtrip() {
        let e = Event::Book(book());
        let s = serde_json::to_string(&e).unwrap();
        let back: Event = serde_json::from_str(&s).unwrap();
        assert_eq!(back.ts(), ts(1000));
    }
}
