//! 经纪层：统一下单/撤单/成交回报接口，两种实现：
//!
//! - [`DryBroker`]：包装回测模拟撮合器（`--dry-run`），不碰网络签名接口；
//!   成交价格/费用与回测完全一致（同一 FeeModel）。
//! - [`TestnetBroker`]：币安 testnet 实盘下单；成交检测轮询
//!   `GET /fapi/v1/userTrades`（游标去重），拿回真实成交价/手续费/maker 标记，
//!   经本地登记表映射回下单原因。
//!
//! 两种实现产出同一 [`Execution`] 序列 → 同一个 [`Account`] 记账 →
//! 同一份 Journal 契约，前端无感知。

use std::collections::HashMap;

use backtest::{Broker, Execution, FeeModel, Order, OrderKind};
use tcore::types::{Price, Qty, Side, Timestamp};
use tracing::{info, warn};

use crate::rest::{RestClient, RestError, SymbolFilters};

/// 登记表：orderId → 下单上下文（成交回报映射原因用）。
#[derive(Debug, Clone)]
struct OrderMeta {
    qty: f64,
    reason: String,
    /// 已成交量（部分成交累计）
    filled_qty: f64,
}

pub struct DryBroker {
    inner: Broker,
}

pub struct TestnetBroker {
    rest: RestClient,
    symbol: String,
    filters: SymbolFilters,
    open: HashMap<i64, OrderMeta>,
    /// userTrades 游标（最后处理过的 trade id）
    last_trade_id: i64,
    last_submitted_order_id: Option<i64>,
}

/// 经纪层统一入口（枚举分发，避免 async trait 对象安全问题）。
pub enum AnyBroker {
    Dry(DryBroker),
    Testnet(TestnetBroker),
}

impl AnyBroker {
    pub fn dry(fee_model: FeeModel) -> Self {
        AnyBroker::Dry(DryBroker {
            inner: Broker::new(fee_model),
        })
    }

    pub fn testnet(rest: RestClient, symbol: &str, filters: SymbolFilters) -> Self {
        AnyBroker::Testnet(TestnetBroker {
            rest,
            symbol: symbol.to_string(),
            filters,
            open: HashMap::new(),
            last_trade_id: 0,
            last_submitted_order_id: None,
        })
    }

    /// 提交订单。Dry：市价单立即成交（与回测一致）；
    /// Testnet：全部走挂单登记，成交经 `poll_fills` 异步回报。
    pub async fn submit(
        &mut self,
        ts: Timestamp,
        ref_price: Price,
        order: Order,
    ) -> Result<Option<Execution>, RestError> {
        match self {
            AnyBroker::Dry(b) => Ok(b.inner.submit(ts, ref_price, order)),
            AnyBroker::Testnet(b) => {
                let (kind, price, stop_price, reduce_only) = match order.kind {
                    OrderKind::Market => ("MARKET", None, None, false),
                    OrderKind::Limit(lp) => ("LIMIT", Some(lp.to_f64()), None, false),
                    // 止损/平仓单 reduceOnly，防反向开仓
                    OrderKind::StopMarket(sp) => ("STOP_MARKET", None, Some(sp.to_f64()), true),
                };
                // 平仓市价单（止损之外的 market_close）也加 reduceOnly 由 reason 判断
                let reduce_only = reduce_only
                    || matches!(
                        order.reason.as_str(),
                        "close_all" | "tp_partial" | "reverse_out"
                    );
                let order_id = b
                    .rest
                    .place_order(
                        &b.symbol,
                        match order.side {
                            Side::Buy => "BUY",
                            Side::Sell => "SELL",
                        },
                        kind,
                        order.qty.to_f64(),
                        price,
                        stop_price,
                        reduce_only,
                        &b.filters,
                    )
                    .await?;
                b.last_submitted_order_id = Some(order_id);
                info!(
                    order_id,
                    side = ?order.side,
                    kind,
                    qty = order.qty.to_f64(),
                    reason = %order.reason,
                    "testnet 订单已提交"
                );
                b.open.insert(
                    order_id,
                    OrderMeta {
                        qty: order.qty.to_f64(),
                        reason: order.reason,
                        filled_qty: 0.0,
                    },
                );
                Ok(None)
            }
        }
    }

    /// 新成交价到达（Dry：驱动模拟撮合；Testnet：忽略，成交靠 poll_fills）。
    pub async fn on_trade_price(&mut self, ts: Timestamp, price: Price) -> Vec<Execution> {
        match self {
            AnyBroker::Dry(b) => b.inner.on_trade_price(ts, price),
            AnyBroker::Testnet(_) => Vec::new(),
        }
    }

    /// 轮询成交回报（Testnet：userTrades 游标；Dry：空）。
    /// 返回按成交时间升序的 Execution（真实价/费/maker）。
    pub async fn poll_fills(&mut self) -> Vec<Execution> {
        let AnyBroker::Testnet(b) = self else {
            return Vec::new();
        };
        let trades = match b.rest.user_trades(&b.symbol, b.last_trade_id + 1).await {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "userTrades 轮询失败（下周期重试）");
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for ut in trades {
            b.last_trade_id = b.last_trade_id.max(ut.trade_id);
            let side = match ut.side.as_str() {
                "BUY" => Side::Buy,
                _ => Side::Sell,
            };
            let (price, qty, fee) = match (
                ut.price.parse::<f64>(),
                ut.qty.parse::<f64>(),
                ut.commission.parse::<f64>(),
            ) {
                (Ok(p), Ok(q), Ok(f)) => (p, q, f),
                _ => {
                    warn!(trade_id = ut.trade_id, "userTrade 数值解析失败，跳过");
                    continue;
                }
            };
            let reason = match b.open.get_mut(&ut.order_id) {
                Some(meta) => {
                    meta.filled_qty += qty;
                    let r = meta.reason.clone();
                    // 全部成交后从登记表移除
                    if meta.filled_qty >= meta.qty * 0.999 {
                        b.open.remove(&ut.order_id);
                    }
                    r
                }
                None => "external".to_string(), // 非本引擎下的单（手动/遗留）
            };
            info!(
                trade_id = ut.trade_id,
                order_id = ut.order_id,
                side = %ut.side,
                price,
                qty,
                fee,
                maker = ut.maker,
                reason = %reason,
                "testnet 成交"
            );
            out.push(Execution {
                order_id: Some(ut.order_id),
                trade_id: Some(ut.trade_id),
                ts: Timestamp::from_millis(ut.time),
                side,
                price: Price::from_f64(price),
                qty: Qty::from_f64(qty),
                fee,
                is_maker: ut.maker,
                reason,
            });
        }
        out.sort_by_key(|e| e.ts);
        out
    }

    pub fn last_submitted_order_id(&self) -> Option<i64> {
        match self {
            AnyBroker::Dry(_) => None,
            AnyBroker::Testnet(b) => b.last_submitted_order_id,
        }
    }

    /// 撤销全部挂单。
    pub async fn cancel_all(&mut self) {
        match self {
            AnyBroker::Dry(b) => b.inner.cancel_all(),
            AnyBroker::Testnet(b) => {
                if let Err(e) = b.rest.cancel_all_open_orders(&b.symbol).await {
                    warn!(error = %e, "testnet 全部撤单失败");
                }
                b.open.clear();
            }
        }
    }

    /// Testnet REST 只读访问（对账/初始化）。
    pub fn rest(&self) -> Option<&RestClient> {
        match self {
            AnyBroker::Dry(_) => None,
            AnyBroker::Testnet(b) => Some(&b.rest),
        }
    }

    /// 定期重对时（防长跑时钟漂移触发 -1021）。Dry 无操作。
    pub async fn resync_time(&mut self) {
        if let AnyBroker::Testnet(b) = self {
            if let Err(e) = b.rest.sync_time().await {
                warn!(error = %e, "定期对时失败（下周期重试）");
            }
        }
    }
}
