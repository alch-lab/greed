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

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use backtest::{Broker, Execution, FeeModel, Order, OrderKind};
use tcore::types::{Price, Qty, Side, Timestamp};
use tracing::{info, warn};

use crate::rest::{PositionRisk, RestClient, RestError, SymbolFilters};

/// 登记表：orderId → 下单上下文（成交回报映射原因用）。
#[derive(Debug, Clone)]
struct OrderMeta {
    qty: f64,
    reason: String,
    side: Side,
    /// Algo 条件单触发后的真实 orderId 与 algoId 不同。
    is_algo: bool,
    /// 已成交量（部分成交累计）
    filled_qty: f64,
}

fn register_fill_reason(
    open: &mut HashMap<i64, OrderMeta>,
    order_id: i64,
    fill_qty: f64,
) -> Option<String> {
    let meta = open.get_mut(&order_id)?;
    meta.filled_qty += fill_qty;
    let reason = meta.reason.clone();
    if meta.filled_qty >= meta.qty * 0.999 {
        open.remove(&order_id);
    }
    Some(reason)
}

/// Algo STOP_MARKET 的成交使用新的 orderId。系统每个 symbol 只管理一个仓位和一个
/// 保护止损；仅在方向一致且候选唯一时回退映射，避免误收普通 external 成交。
fn register_unique_algo_fill_reason(
    open: &mut HashMap<i64, OrderMeta>,
    side: Side,
    fill_qty: f64,
) -> Option<(i64, String)> {
    let candidates = open
        .iter()
        .filter(|(_, meta)| {
            meta.is_algo && meta.side == side && meta.filled_qty + fill_qty <= meta.qty * 1.001
        })
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return None;
    }
    let algo_id = candidates[0];
    register_fill_reason(open, algo_id, fill_qty).map(|reason| (algo_id, reason))
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
    /// 最近一次 userTrades 轮询是否成功；失败期间禁止新增风险。
    execution_healthy: bool,
    consecutive_poll_failures: u32,
    last_poll_success: Instant,
}

const EXECUTION_STALE_AFTER: Duration = Duration::from_secs(30);

fn execution_channel_healthy(since_success: Duration) -> bool {
    since_success < EXECUTION_STALE_AFTER
}

/// 经纪层统一入口（枚举分发，避免 async trait 对象安全问题）。
pub enum AnyBroker {
    Dry(DryBroker),
    // REST 客户端及交易所状态明显大于本地撮合器，使用间接存储避免每个
    // AnyBroker 都按最大 variant 分配栈空间。
    Testnet(Box<TestnetBroker>),
}

impl AnyBroker {
    pub fn dry(fee_model: FeeModel) -> Self {
        AnyBroker::Dry(DryBroker {
            inner: Broker::new(fee_model),
        })
    }

    pub fn testnet(rest: RestClient, symbol: &str, filters: SymbolFilters) -> Self {
        AnyBroker::Testnet(Box::new(TestnetBroker {
            rest,
            symbol: symbol.to_string(),
            filters,
            open: HashMap::new(),
            last_trade_id: 0,
            last_submitted_order_id: None,
            execution_healthy: true,
            consecutive_poll_failures: 0,
            last_poll_success: Instant::now(),
        }))
    }

    /// 把成交游标定位到启动时账户的最新成交。
    ///
    /// Binance `userTrades` 未给 `fromId` 时返回最近成交；若从 0 开始轮询，旧成交会在
    /// 每次进程启动时被重复记账。引擎尚未下单时做一次基线定位，可保证后续只消费本次
    /// 进程启动以后产生的成交。
    pub async fn prime_fill_cursor(&mut self) -> Result<(), RestError> {
        let AnyBroker::Testnet(b) = self else {
            return Ok(());
        };
        let trades = b.rest.user_trades(&b.symbol, 0).await?;
        b.last_trade_id = trades.iter().map(|trade| trade.trade_id).max().unwrap_or(0);
        b.last_poll_success = Instant::now();
        b.consecutive_poll_failures = 0;
        b.execution_healthy = true;
        info!(
            last_trade_id = b.last_trade_id,
            ignored_history = trades.len(),
            "成交游标已定位，历史成交不会重复导入"
        );
        Ok(())
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
                        "close_all" | "tp_partial" | "trend_tight_tranche" | "reverse_out"
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
                        side: order.side,
                        is_algo: kind == "STOP_MARKET",
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
            Ok(t) => {
                if !b.execution_healthy {
                    info!(
                        failures = b.consecutive_poll_failures,
                        "userTrades 成交回报通道已恢复"
                    );
                }
                b.last_poll_success = Instant::now();
                b.consecutive_poll_failures = 0;
                b.execution_healthy = true;
                t
            }
            Err(e) => {
                b.consecutive_poll_failures = b.consecutive_poll_failures.saturating_add(1);
                let stale_for = b.last_poll_success.elapsed();
                let was_healthy = b.execution_healthy;
                b.execution_healthy = execution_channel_healthy(stale_for);
                // 瞬时 502 不应封死策略，也不应每 2 秒刷屏；超过 30 秒才禁止新增风险。
                if b.consecutive_poll_failures == 1
                    || (was_healthy && !b.execution_healthy)
                    || b.consecutive_poll_failures % 30 == 0
                {
                    warn!(
                        error = %e,
                        failures = b.consecutive_poll_failures,
                        stale_seconds = stale_for.as_secs(),
                        execution_healthy = b.execution_healthy,
                        "userTrades 轮询失败（短暂故障容忍 30 秒，持续故障禁止新开仓）"
                    );
                }
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
            let reason = register_fill_reason(&mut b.open, ut.order_id, qty).or_else(|| {
                register_unique_algo_fill_reason(&mut b.open, side, qty).map(|(algo_id, reason)| {
                    warn!(
                        algo_id,
                        actual_order_id = ut.order_id,
                        trade_id = ut.trade_id,
                        "Algo 止损成交通过唯一方向/数量候选完成映射"
                    );
                    reason
                })
            });
            let Some(reason) = reason else {
                warn!(
                    trade_id = ut.trade_id,
                    order_id = ut.order_id,
                    side = %ut.side,
                    price = %ut.price,
                    qty = %ut.qty,
                    "忽略非本次引擎订单的 external 成交（不进入本地账户）"
                );
                continue;
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

    /// 成交回报通道失效时禁止新增风险；Dry 模式始终健康。
    pub fn execution_healthy(&self) -> bool {
        match self {
            AnyBroker::Dry(_) => true,
            AnyBroker::Testnet(b) => b.execution_healthy,
        }
    }

    /// 交易所实时仓位。Dry 模式没有外部仓位，返回 None。
    pub async fn exchange_position_risk(&self) -> Result<Option<PositionRisk>, RestError> {
        match self {
            AnyBroker::Dry(_) => Ok(None),
            AnyBroker::Testnet(b) => b.rest.position_risk(&b.symbol).await.map(Some),
        }
    }

    /// 当 positionRisk 已经为空、但本地仍有仓位时，从最近真实 userTrades 中恢复
    /// 最后发生的反向成交。Algo 止损触发后的 child orderId 可能无法与本地 algoId
    /// 映射，这条恢复路径保证真实成交不会被永久丢弃。
    pub async fn recover_recent_close_fills(
        &self,
        position_side: Side,
        remaining_qty: f64,
        since_ms: i64,
    ) -> Result<Vec<Execution>, RestError> {
        let AnyBroker::Testnet(b) = self else {
            return Ok(Vec::new());
        };
        let expected_side = position_side.opposite();
        let mut trades = b.rest.user_trades(&b.symbol, 0).await?;
        trades.retain(|trade| {
            trade.time >= since_ms
                && matches!(
                    (expected_side, trade.side.as_str()),
                    (Side::Buy, "BUY") | (Side::Sell, "SELL")
                )
        });
        // 从最新成交向前覆盖当前本地剩余数量，可避开已经记账的早期部分止盈。
        trades.sort_by_key(|trade| std::cmp::Reverse(trade.time));
        let mut needed = remaining_qty.max(0.0);
        let mut recovered = Vec::new();
        for trade in trades {
            if needed <= 1e-9 {
                break;
            }
            let (price, raw_qty, raw_fee) = match (
                trade.price.parse::<f64>(),
                trade.qty.parse::<f64>(),
                trade.commission.parse::<f64>(),
            ) {
                (Ok(price), Ok(qty), Ok(fee)) if price > 0.0 && qty > 0.0 => (price, qty, fee),
                _ => continue,
            };
            let qty = raw_qty.min(needed);
            let fee = raw_fee * (qty / raw_qty);
            needed -= qty;
            recovered.push(Execution {
                order_id: Some(trade.order_id),
                trade_id: Some(trade.trade_id),
                ts: Timestamp::from_millis(trade.time),
                side: expected_side,
                price: Price::from_f64(price),
                qty: Qty::from_f64(qty),
                fee,
                is_maker: trade.maker,
                reason: "exchange_flat_reconcile".into(),
            });
        }
        recovered.sort_by_key(|execution| execution.ts);
        Ok(recovered)
    }

    /// 撤销全部挂单。
    pub async fn cancel_all(&mut self) {
        match self {
            AnyBroker::Dry(b) => b.inner.cancel_all(),
            AnyBroker::Testnet(b) => match b.rest.cancel_all_open_orders(&b.symbol).await {
                Ok(()) => b.open.clear(),
                Err(e) => warn!(error = %e, "testnet 普通单/Algo 条件单全部撤单失败"),
            },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_poll_failure_does_not_immediately_disable_entries() {
        assert!(execution_channel_healthy(Duration::from_secs(5)));
        assert!(!execution_channel_healthy(Duration::from_secs(30)));
    }

    #[test]
    fn external_fill_is_not_registered_or_accounted() {
        let mut open = HashMap::new();
        assert_eq!(register_fill_reason(&mut open, 99, 0.1), None);
        assert!(open.is_empty());
    }

    #[test]
    fn registered_partial_fills_keep_reason_until_complete() {
        let mut open = HashMap::from([(
            42,
            OrderMeta {
                qty: 0.1,
                reason: "orderflow_balanced".into(),
                side: Side::Buy,
                is_algo: false,
                filled_qty: 0.0,
            },
        )]);
        assert_eq!(
            register_fill_reason(&mut open, 42, 0.04).as_deref(),
            Some("orderflow_balanced")
        );
        assert!(open.contains_key(&42));
        assert_eq!(
            register_fill_reason(&mut open, 42, 0.06).as_deref(),
            Some("orderflow_balanced")
        );
        assert!(!open.contains_key(&42));
    }

    #[test]
    fn unique_algo_fill_maps_triggered_child_order() {
        let mut open = HashMap::from([(
            4001,
            OrderMeta {
                qty: 0.1,
                reason: "stop".into(),
                side: Side::Sell,
                is_algo: true,
                filled_qty: 0.0,
            },
        )]);
        let mapped = register_unique_algo_fill_reason(&mut open, Side::Sell, 0.1);
        assert_eq!(mapped, Some((4001, "stop".into())));
        assert!(open.is_empty());
    }

    #[test]
    fn ambiguous_algo_fill_is_not_mapped() {
        let meta = OrderMeta {
            qty: 0.1,
            reason: "stop".into(),
            side: Side::Sell,
            is_algo: true,
            filled_qty: 0.0,
        };
        let mut open = HashMap::from([(4001, meta.clone()), (4002, meta)]);
        assert_eq!(
            register_unique_algo_fill_reason(&mut open, Side::Sell, 0.1),
            None
        );
    }
}
