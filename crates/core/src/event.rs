//! 统一事件 采集/回测/实盘 共用同一事件模型
use crate::types::{Exchange, Price, Qty, Side, Symbol, Timestamp};
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

/// 订单薄快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub ts: Timestamp,
    pub exchange: Exchange,
    pub symbol: Symbol,
    /// (price, qty), bid 按价格降序，ask 按价格升序
    pub bids: Vec<(Price, Qty)>,
    pub asks: Vec<(Price, Qty)>,
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
}

impl Trade {
    /// 成交的主动方(taker 方向)
    pub fn taker_side(&self) -> Side {
        if self.is_buyer_maker {
            Side::Sell
        } else {
            Side::Buy
        }
    }
}
