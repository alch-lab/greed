//! 模拟撮合：市价单吃 spread、限价单按价位穿越成交。
//!
//! 只有逐笔成交（无订单簿历史），用 trade price 作为价格参考：
//! - **市价单**：立即以「最新成交价 + 滑点」成交（taker）。
//! - **限价单**：挂单等待；买单在 trade price ≤ limit 时成交，卖单在 ≥ limit 时成交（maker）。
//! - **止损单（stop-market）**：价格穿越触发价时以市价成交（taker + 滑点）。
//!
//! 撮合规则确定性：同一事件序列 → 同一成交序列。

use crate::fees::FeeModel;
use tcore::types::{Price, Qty, Side, Timestamp};

/// 下单请求（由策略执行器发出）。
#[derive(Debug, Clone)]
pub struct Order {
    pub side: Side,
    pub qty: Qty,
    pub kind: OrderKind,
    pub reason: String,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderKind {
    Market,
    Limit(Price),
    StopMarket(Price), // 触发价
}

/// 一笔成交。
#[derive(Debug, Clone)]
pub struct Execution {
    pub ts: Timestamp,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub fee: f64,
    pub is_maker: bool,
    pub reason: String,
}

/// 挂单。
#[derive(Debug, Clone)]
struct PendingOrder {
    side: Side,
    qty: Qty,
    kind: PendingKind,
    reason: String,
}

#[derive(Debug, Clone, Copy)]
enum PendingKind {
    Limit(Price),
    StopMarket(Price),
}

/// 模拟撮合器。
#[derive(Debug)]
pub struct Broker {
    fee_model: FeeModel,
    pending: Vec<PendingOrder>,
}

impl Broker {
    pub fn new(fee_model: FeeModel) -> Self {
        Broker {
            fee_model,
            pending: Vec::new(),
        }
    }

    pub fn fee_model(&self) -> &FeeModel {
        &self.fee_model
    }

    /// 提交订单。市价单立即成交并返回；限价/止损单进入挂单队列。
    pub fn submit(&mut self, ts: Timestamp, ref_price: Price, order: Order) -> Option<Execution> {
        match order.kind {
            OrderKind::Market => {
                let px = self.fee_model.market_fill_price(ref_price, order.side);
                let notional = px.to_f64() * order.qty.to_f64();
                Some(Execution {
                    ts,
                    side: order.side,
                    price: px,
                    qty: order.qty,
                    fee: self.fee_model.fee(notional, false),
                    is_maker: false,
                    reason: order.reason,
                })
            }
            OrderKind::Limit(limit) => {
                // 立即可成交的限价（穿越当前价）按限价成交
                let immediate = match order.side {
                    Side::Buy => ref_price <= limit,
                    Side::Sell => ref_price >= limit,
                };
                if immediate {
                    let notional = limit.to_f64() * order.qty.to_f64();
                    Some(Execution {
                        ts,
                        side: order.side,
                        price: limit,
                        qty: order.qty,
                        fee: self.fee_model.fee(notional, true),
                        is_maker: true,
                        reason: order.reason,
                    })
                } else {
                    self.pending.push(PendingOrder {
                        side: order.side,
                        qty: order.qty,
                        kind: PendingKind::Limit(limit),
                        reason: order.reason,
                    });
                    None
                }
            }
            OrderKind::StopMarket(trigger) => {
                self.pending.push(PendingOrder {
                    side: order.side,
                    qty: order.qty,
                    kind: PendingKind::StopMarket(trigger),
                    reason: order.reason,
                });
                None
            }
        }
    }

    /// 新成交价到达，检查挂单触发，返回成交列表（按挂单顺序）。
    pub fn on_trade_price(&mut self, ts: Timestamp, price: Price) -> Vec<Execution> {
        let mut executed = Vec::new();
        let mut still_pending = Vec::new();
        for po in std::mem::take(&mut self.pending) {
            let fill = match po.kind {
                PendingKind::Limit(limit) => {
                    let hit = match po.side {
                        Side::Buy => price <= limit,
                        Side::Sell => price >= limit,
                    };
                    hit.then(|| {
                        let notional = limit.to_f64() * po.qty.to_f64();
                        Execution {
                            ts,
                            side: po.side,
                            price: limit,
                            qty: po.qty,
                            fee: self.fee_model.fee(notional, true),
                            is_maker: true,
                            reason: po.reason.clone(),
                        }
                    })
                }
                PendingKind::StopMarket(trigger) => {
                    let hit = match po.side {
                        // 止损卖单：价格跌破触发价
                        Side::Sell => price <= trigger,
                        // 止损买单：价格涨破触发价
                        Side::Buy => price >= trigger,
                    };
                    hit.then(|| {
                        let px = self.fee_model.market_fill_price(price, po.side);
                        let notional = px.to_f64() * po.qty.to_f64();
                        Execution {
                            ts,
                            side: po.side,
                            price: px,
                            qty: po.qty,
                            fee: self.fee_model.fee(notional, false),
                            is_maker: false,
                            reason: po.reason.clone(),
                        }
                    })
                }
            };
            match fill {
                Some(ex) => executed.push(ex),
                None => still_pending.push(po),
            }
        }
        self.pending = still_pending;
        executed
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// 撤销所有挂单（如反手/熔断时清理）。
    pub fn cancel_all(&mut self) {
        self.pending.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: i64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    #[test]
    fn market_order_fills_with_slippage() {
        let mut b = Broker::new(FeeModel::default());
        let ex = b
            .submit(
                ts(1),
                Price::from_f64(100.0),
                Order {
                    side: Side::Buy,
                    qty: Qty::from_f64(1.0),
                    kind: OrderKind::Market,
                    reason: "t".into(),
                },
            )
            .expect("市价单应立即成交");
        assert!(ex.price.to_f64() > 100.0); // 买价上滑
        assert!(!ex.is_maker);
        assert!(ex.fee > 0.0);
    }

    #[test]
    fn limit_order_fills_on_cross() {
        let mut b = Broker::new(FeeModel::default());
        // 挂限价买单 99.0，当前价 100 → 挂单
        let r = b.submit(
            ts(1),
            Price::from_f64(100.0),
            Order {
                side: Side::Buy,
                qty: Qty::from_f64(1.0),
                kind: OrderKind::Limit(Price::from_f64(99.0)),
                reason: "limit".into(),
            },
        );
        assert!(r.is_none());
        assert_eq!(b.pending_count(), 1);
        // 价格到 99.2 不成交
        assert!(b.on_trade_price(ts(2), Price::from_f64(99.2)).is_empty());
        // 价格到 99.0 成交
        let fills = b.on_trade_price(ts(3), Price::from_f64(99.0));
        assert_eq!(fills.len(), 1);
        assert!(fills[0].is_maker);
        assert!((fills[0].price.to_f64() - 99.0).abs() < 1e-9);
    }

    #[test]
    fn stop_market_triggers_on_cross() {
        let mut b = Broker::new(FeeModel::default());
        // 止损卖单：触发价 99.0
        b.submit(
            ts(1),
            Price::from_f64(100.0),
            Order {
                side: Side::Sell,
                qty: Qty::from_f64(1.0),
                kind: OrderKind::StopMarket(Price::from_f64(99.0)),
                reason: "stop".into(),
            },
        );
        assert!(b.on_trade_price(ts(2), Price::from_f64(99.5)).is_empty());
        let fills = b.on_trade_price(ts(3), Price::from_f64(98.9));
        assert_eq!(fills.len(), 1);
        assert!(!fills[0].is_maker); // 止损市价单为 taker
        assert!(fills[0].price.to_f64() < 98.9); // 卖价下滑
    }

    #[test]
    fn limit_immediate_when_crossed() {
        let mut b = Broker::new(FeeModel::default());
        // 限价买单 100.5，当前价 100 → 立即按限价成交
        let ex = b
            .submit(
                ts(1),
                Price::from_f64(100.0),
                Order {
                    side: Side::Buy,
                    qty: Qty::from_f64(1.0),
                    kind: OrderKind::Limit(Price::from_f64(100.5)),
                    reason: "t".into(),
                },
            )
            .expect("穿越限价应立即成交");
        assert!((ex.price.to_f64() - 100.5).abs() < 1e-9);
        assert!(ex.is_maker);
    }
}
