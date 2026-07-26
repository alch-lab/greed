//! 实盘执行引擎：与回测同一决策代码路径（信号 → 扳机 → 过滤 → 仓位 → 出场），
//! 仅把撮合层换成 [`AnyBroker`]（模拟 / testnet REST）。
//!
//! 事件流：
//! - `on_trade`：行情 WS 逐笔 → 与回测 `BacktestEngine::on_trade` 同序处理
//!   （时钟/价格 → 撮合回报 → 信号 → 持仓管理/开仓评估 → 熔断）。
//! - `on_timer`：1s 节拍。testnet 成交轮询（2s）、过期限价撤单、
//!   资金费率标志刷新（5min）、权益采样（60s）、钱包对账（1h）。
//!
//! Journal：每次意图/成交/权益采样后原子重写（tmp + rename），
//! 前端随时读到最新状态；进程崩溃最多丢最后一笔未落盘事件。
//!
//! 启动安全：CLI 在构建引擎前已确认「无持仓 + 无遗留挂单 + 杠杆/逐仓就绪」；
//! 运行中收到 SIGINT 只落盘退出，**不撤交易所挂单**（止损单是持仓的保护）。

use std::path::PathBuf;

use backtest::{Account, EquityPoint, FillRequest, Journal, JournalIntent, JournalMeta};use strategy::Strategy;
use tcore::plugin::{Ctx, ExitAction, OrderIntent, Signal, Verdict};
use tcore::types::{Price, Qty, Symbol, Timestamp};
use tcore::{Event, EventClock, Trade};
use tracing::{error, info, warn};

use crate::broker::AnyBroker;
use crate::rest::floor_to_step;

#[derive(Debug, Clone)]
pub struct LiveConfig {
    pub symbol: String,
    /// 固定风险百分比（同回测）
    pub risk_pct: f64,
    pub max_risk_pct: f64,
    pub entry_ttl_ms: i64,
    pub cb_max_daily_losses: u32,
    pub cb_daily_dd_pct: f64,
    /// testnet 数量步长（dry 模式无约束，用 1e-8）
    pub qty_step: f64,
    /// 最小名义价值（USDT；dry 模式 0）
    pub min_notional: f64,
    /// journal 输出路径（原子重写）
    pub journal_path: PathBuf,
    /// 策略描述（写进 journal meta，前端展示）
    pub strategy_name: String,
}

/// 挂起中的入场（testnet：已下单待成交；dry：限价单待触发）。
#[derive(Debug, Clone, Copy)]
struct PendingEntry {
    stop_price: Price,
    tp1_price: Option<Price>,
    expire_ts: Timestamp,
}

/// 引擎状态快照（控制面 `/api/trade/status` 的数据源）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct EngineSnapshot {
    pub last_price: Option<f64>,
    pub equity: f64,
    pub cash: f64,
    pub position: Option<PositionSnap>,
    pub n_intents: usize,
    pub n_fills: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PositionSnap {
    pub side: String,
    pub qty: f64,
    pub entry_price: f64,
    pub stop_price: Option<f64>,
    pub unrealized_pnl: f64,
}

pub struct LiveEngine {
    strategy: Strategy,
    broker: AnyBroker,
    account: Account,
    ctx: Ctx,
    clock: EventClock,
    symbol: Symbol,
    latest_price: Option<Price>,
    config: LiveConfig,
    // ---- journal 状态 ----
    started_at: String,
    intents: Vec<JournalIntent>,
    equity_curve: Vec<EquityPoint>,
    last_equity_sample_ms: i64,
    // ---- 入场挂起 ----
    pending_entry: Option<PendingEntry>,
    // ---- 熔断状态（同回测）----
    cb_day: i64,
    cb_day_start_equity: f64,
    cb_consec_losses: u32,
    cb_on: bool,
    cb_noted_fills: usize,
    // ---- 定时任务节拍 ----
    last_fill_poll_ms: i64,
    last_funding_poll_ms: i64,
    last_reconcile_ms: i64,
}

impl LiveEngine {
    pub fn new(
        strategy: Strategy,
        broker: AnyBroker,
        config: LiveConfig,
        initial_cash: f64,
        started_at: String,
    ) -> Self {
        let symbol = Symbol::new(&config.symbol);
        LiveEngine {
            strategy,
            broker,
            account: Account::new(initial_cash),
            ctx: Ctx::default(),
            clock: EventClock::new(),
            symbol,
            latest_price: None,
            config,
            started_at,
            intents: Vec::new(),
            equity_curve: Vec::new(),
            last_equity_sample_ms: 0,
            pending_entry: None,
            cb_day: i64::MIN,
            cb_day_start_equity: 0.0,
            cb_consec_losses: 0,
            cb_on: false,
            cb_noted_fills: 0,
            last_fill_poll_ms: 0,
            last_funding_poll_ms: 0,
            last_reconcile_ms: 0,
        }
    }

    pub fn account(&self) -> &Account {
        &self.account
    }

    /// 预热专用：只推进时钟/价格并喂信号插件，**不做撮合/持仓管理/开仓评估**。
    /// 用于启动时用历史 K 线合成逐笔重建信号状态（EMA/ATR/RSI），
    /// 避免基于陈旧价格触发交易。
    pub fn warmup_trade(&mut self, trade: &Trade) {
        self.clock.advance_to(trade.ts);
        self.latest_price = Some(trade.price);
        self.update_env_flags(trade.ts, trade.price);
        let ev = Event::Trade(trade.clone());
        self.ctx.now = Some(trade.ts);
        for sp in self.strategy.signals.iter_mut() {
            for sig in sp.on_event(&ev, &self.ctx) {
                self.ctx.set_latest(sig);
            }
        }
    }

    /// 状态快照（控制面轮询用）。
    pub fn snapshot(&self) -> EngineSnapshot {        let px = self.latest_price;
        EngineSnapshot {
            last_price: px.map(|p| p.to_f64()),
            equity: self.account.equity(px.unwrap_or(Price::ZERO)),
            cash: self.account.cash(),
            position: self.account.position().map(|p| PositionSnap {
                side: format!("{:?}", p.side),
                qty: p.qty.to_f64(),
                entry_price: p.entry_price.to_f64(),
                stop_price: p.stop_price.map(|s| s.to_f64()),
                unrealized_pnl: px.map(|x| p.unrealized(x)).unwrap_or(0.0),
            }),
            n_intents: self.intents.len(),
            n_fills: self.account.fills().len(),
        }
    }

    // ==================================================================
    // 事件入口
    // ==================================================================

    /// 行情逐笔：与回测同序处理。
    pub async fn on_trade(&mut self, trade: &Trade) {
        self.clock.advance_to(trade.ts);
        self.latest_price = Some(trade.price);
        self.update_env_flags(trade.ts, trade.price);
        self.cb_update(trade.ts, trade.price);

        // 1) 撮合/成交回报（dry：本地模拟撮合；testnet：本地挂单不消费价格）
        let execs = self.broker.on_trade_price(trade.ts, trade.price).await;
        for ex in execs {
            self.account.apply_fill(FillRequest {
                ts: ex.ts,
                side: ex.side,
                price: ex.price,
                qty: ex.qty,
                fee: ex.fee,
                is_maker: ex.is_maker,
                reason: ex.reason,
            });
        }
        self.settle_pending_entry(trade.ts).await;

        // 2) 信号插件
        let ev = Event::Trade(trade.clone());
        self.ctx.now = Some(trade.ts);
        self.sync_position_flag();
        let mut new_signals: Vec<Signal> = Vec::new();
        for sp in self.strategy.signals.iter_mut() {
            for sig in sp.on_event(&ev, &self.ctx) {
                self.ctx.set_latest(sig.clone());
                new_signals.push(sig);
            }
        }

        // 3) 持仓管理 / 开仓评估
        if self.account.position().is_some() {
            self.manage_position(trade).await;
            self.try_add(trade, &new_signals).await;
        } else {
            self.try_enter(trade, &new_signals).await;
        }

        // 4) 熔断统计
        self.cb_note_fills();
        self.cb_update(trade.ts, trade.price);
        self.persist_journal();
    }

    /// 1s 节拍：成交轮询 / 限价过期 / 资金费率 / 权益采样 / 对账。
    pub async fn on_timer(&mut self, now_ms: i64) {
        let now = Timestamp::from_millis(now_ms);

        // 成交轮询（2s）：testnet 的 fill 全部从这里来
        if now_ms - self.last_fill_poll_ms >= 2_000 {
            self.last_fill_poll_ms = now_ms;
            let execs = self.broker.poll_fills().await;
            let mut got_fill = false;
            for ex in execs {
                got_fill = true;
                self.account.apply_fill(FillRequest {
                    ts: ex.ts,
                    side: ex.side,
                    price: ex.price,
                    qty: ex.qty,
                    fee: ex.fee,
                    is_maker: ex.is_maker,
                    reason: ex.reason,
                });
            }
            if got_fill {
                self.settle_pending_entry(now).await;
                self.cb_note_fills();
                self.persist_journal();
            }
        }

        // 过期限价入场：撤单 + 清挂起
        if let Some(pe) = self.pending_entry {
            if now_ms > pe.expire_ts.as_millis() && self.account.position().is_none() {
                info!("限价入场单过期，撤单");
                self.broker.cancel_all().await;
                self.pending_entry = None;
            }
        }

        // 资金费率标志（5min）：与回测 Funding 事件对齐
        if now_ms - self.last_funding_poll_ms >= 300_000 {
            self.last_funding_poll_ms = now_ms;
            if let Some(rest) = self.broker.rest() {
                match rest.premium_index(&self.config.symbol).await {
                    Ok((_mark, rate)) => {
                        self.ctx
                            .flags
                            .insert("funding_rate".into(), format!("{}", rate));
                    }
                    Err(e) => warn!(error = %e, "premiumIndex 查询失败"),
                }
            }
        }

        // 权益采样（60s，前端曲线粒度；首个节拍立即采样）
        if let Some(px) = self.latest_price {
            if self.equity_curve.is_empty() || now_ms - self.last_equity_sample_ms >= 60_000 {
                self.last_equity_sample_ms = now_ms;
                self.equity_curve.push(EquityPoint {
                    ts_ms: now_ms,
                    equity: self.account.equity(px),
                });
                self.persist_journal();
            }
        }

        // 钱包对账（1h）：本地记账 vs testnet 钱包，漂移告警
        if now_ms - self.last_reconcile_ms >= 3_600_000 {
            self.last_reconcile_ms = now_ms;
            if let Some(rest) = self.broker.rest() {
                match rest.wallet_balance_usdt().await {
                    Ok(wallet) => {
                        let drift = (wallet - self.account.cash()).abs();
                        if drift > 1.0 {
                            warn!(
                                wallet,
                                local = self.account.cash(),
                                drift,
                                "钱包与本地记账漂移（资金费率/外部操作？）"
                            );
                        } else {
                            info!(wallet, local = self.account.cash(), "钱包对账一致");
                        }
                    }
                    Err(e) => warn!(error = %e, "钱包对账查询失败"),
                }
            }
        }
    }

    /// 优雅退出：落盘（不撤交易所挂单——止损单是保护）。
    pub async fn shutdown(&mut self) {
        self.persist_journal();
        info!(
            fills = self.account.fills().len(),
            intents = self.intents.len(),
            "引擎退出，journal 已落盘；交易所挂单保留（止损保护）"
        );
    }

    // ==================================================================
    // 决策（与 BacktestEngine 同构）
    // ==================================================================

    async fn manage_position(&mut self, trade: &Trade) {
        let pos_view = match self.account.position_view(&self.symbol) {
            Some(p) => p,
            None => return,
        };
        let mut actions = Vec::new();
        for ep in self.strategy.exits.iter() {
            actions.extend(ep.manage(&pos_view, &self.ctx));
        }
        for act in actions {
            match act {
                ExitAction::MoveStop(px) => {
                    if let Some(p) = self.account.position_mut() {
                        p.stop_price = Some(px);
                        if (px.to_f64() - p.entry_price.to_f64()).abs() < 1e-9 {
                            p.breakeven_moved = true;
                        }
                        let side = p.side;
                        let qty = p.qty;
                        self.broker.cancel_all().await;
                        if let Err(e) = self
                            .broker
                            .submit(
                                trade.ts,
                                trade.price,
                                backtest::Order {
                                    side: side.opposite(),
                                    qty,
                                    kind: backtest::OrderKind::StopMarket(px),
                                    reason: "stop".into(),
                                    expire_ts: None,
                                },
                            )
                            .await
                        {
                            error!(error = %e, "移动止损挂单失败");
                        }
                    }
                }
                ExitAction::ClosePartial(frac) => {
                    if let Some(p) = self.account.position().copied() {
                        let close_qty = Qty::from_f64(p.qty.to_f64() * frac);
                        self.market_close(trade, close_qty, "tp_partial").await;
                        if let Some(pp) = self.account.position_mut() {
                            pp.closed_frac = (pp.closed_frac + frac).min(1.0);
                        }
                    }
                }
                ExitAction::CloseAll => {
                    if let Some(p) = self.account.position().copied() {
                        self.market_close(trade, p.qty, "close_all").await;
                    }
                }
                ExitAction::Reverse(intent) => {
                    if let Some(p) = self.account.position().copied() {
                        self.market_close(trade, p.qty, "reverse_out").await;
                    }
                    self.enter(trade, *intent).await;
                }
            }
        }
    }

    async fn try_enter(&mut self, trade: &Trade, signals: &[Signal]) {
        if self.pending_entry.is_some() {
            return;
        }
        let intent = self
            .strategy
            .trigger
            .should_fire(signals, &self.ctx)
            .or_else(|| {
                self.strategy
                    .trigger
                    .on_signals(signals, &self.ctx, &self.symbol)
            });
        let Some(intent) = intent else { return };
        let mut scale = 1.0f64;
        for fp in self.strategy.filters.iter() {
            match fp.check(&intent, &self.ctx) {
                Verdict::Allow => {}
                Verdict::Scale(s) => scale = scale.min(s),
                Verdict::Veto(_) => return,
            }
        }
        let mut intent = intent;
        intent.qty = Qty::from_f64(intent.qty.to_f64() * scale);
        self.enter(trade, intent).await;
    }

    async fn try_add(&mut self, trade: &Trade, signals: &[Signal]) {
        if self.pending_entry.is_some() {
            return;
        }
        let Some(pos) = self.account.position_view(&self.symbol) else {
            return;
        };
        let Some(intent) = self
            .strategy
            .trigger
            .on_add(&pos, signals, &self.ctx, &self.symbol)
        else {
            return;
        };
        let mut scale = 1.0f64;
        for fp in self.strategy.filters.iter() {
            match fp.check(&intent, &self.ctx) {
                Verdict::Allow => {}
                Verdict::Scale(s) => scale = scale.min(s),
                Verdict::Veto(_) => return,
            }
        }
        let mut intent = intent;
        intent.qty = Qty::from_f64(intent.qty.to_f64() * scale);
        self.enter(trade, intent).await;
    }

    /// 开仓：仓位计算（与回测同式）→ 精度约束 → 记录意图 → 下单。
    async fn enter(&mut self, trade: &Trade, intent: OrderIntent) {
        let stop_dist = (intent.stop_price.to_f64() - trade.price.to_f64()).abs();
        let qty = if stop_dist > 1e-9 {
            let risk_frac = self.config.risk_pct.min(self.config.max_risk_pct);
            let risk_usd = self.account.equity(trade.price) * risk_frac;
            let q = risk_usd / stop_dist;
            if intent.qty.to_f64() > 1e-9 {
                q.min(intent.qty.to_f64())
            } else {
                q
            }
        } else {
            intent.qty.to_f64()
        };
        // 精度约束：数量向下取整到步长；名义价值过低放弃
        let qty = floor_to_step(qty, self.config.qty_step);
        if qty <= 1e-9 {
            return;
        }
        if self.config.min_notional > 0.0 && qty * trade.price.to_f64() < self.config.min_notional
        {
            warn!(
                qty,
                notional = qty * trade.price.to_f64(),
                min = self.config.min_notional,
                "名义价值低于最小约束，放弃下单"
            );
            return;
        }
        let qty = Qty::from_f64(qty);

        self.intents.push(JournalIntent {
            ts_ms: trade.ts.as_millis(),
            side: format!("{:?}", intent.side),
            qty: qty.to_f64(),
            limit_price: intent.limit_price.map(|p| p.to_f64()),
            stop_price: intent.stop_price.to_f64(),
            tp1_price: intent.tp1_price.map(|p| p.to_f64()),
            reason: intent.reason.clone(),
        });
        self.persist_journal();

        let is_limit = intent.limit_price.is_some();
        // 市价单成交回报也应很快到达：testnet 给 120s 兜底窗口；限价用 entry_ttl
        let ttl = if is_limit { self.config.entry_ttl_ms } else { 120_000 };
        let expire_ts = Timestamp::from_millis(trade.ts.as_millis() + ttl);

        let order = backtest::Order {
            side: intent.side,
            qty,
            kind: match intent.limit_price {
                Some(lp) => backtest::OrderKind::Limit(lp),
                None => backtest::OrderKind::Market,
            },
            reason: intent.reason.clone(),
            expire_ts: if is_limit { Some(expire_ts) } else { None },
        };
        match self.broker.submit(trade.ts, trade.price, order).await {
            Ok(Some(ex)) => {
                // dry 市价单立即成交
                self.account.apply_fill(FillRequest {
                    ts: ex.ts,
                    side: ex.side,
                    price: ex.price,
                    qty: ex.qty,
                    fee: ex.fee,
                    is_maker: ex.is_maker,
                    reason: ex.reason,
                });
                self.place_protective_stop(trade.ts, trade.price, intent.stop_price)
                    .await;
            }
            Ok(None) => {
                self.pending_entry = Some(PendingEntry {
                    stop_price: intent.stop_price,
                    tp1_price: intent.tp1_price,
                    expire_ts,
                });
            }
            Err(e) => {
                error!(error = %e, reason = %intent.reason, "下单失败");
            }
        }
    }

    /// 持仓建立后挂保护性止损（先清旧单，按总仓重挂）。
    async fn place_protective_stop(&mut self, ts: Timestamp, ref_price: Price, stop: Price) {
        if let Some(p) = self.account.position_mut() {
            p.stop_price = Some(stop);
            let side = p.side;
            let q = p.qty;
            self.broker.cancel_all().await;
            if let Err(e) = self
                .broker
                .submit(
                    ts,
                    ref_price,
                    backtest::Order {
                        side: side.opposite(),
                        qty: q,
                        kind: backtest::OrderKind::StopMarket(stop),
                        reason: "stop".into(),
                        expire_ts: None,
                    },
                )
                .await
            {
                error!(error = %e, "保护性止损挂单失败");
            }
        }
    }

    /// 挂起入场的善后：成交 → 补挂止损；过期由 on_timer 处理。
    async fn settle_pending_entry(&mut self, now: Timestamp) {
        let Some(pe) = self.pending_entry else { return };
        if self.account.position().is_some() {
            if let Some(p) = self.account.position_mut() {
                p.tp1_price = pe.tp1_price;
            }
            let ref_price = self.latest_price.unwrap_or(pe.stop_price);
            self.place_protective_stop(now, ref_price, pe.stop_price).await;
            self.pending_entry = None;
        }
    }

    async fn market_close(&mut self, trade: &Trade, qty: Qty, reason: &str) {
        let Some(p) = self.account.position() else { return };
        let side = p.side.opposite();
        self.broker.cancel_all().await; // 清掉止损单
        let order = backtest::Order {
            side,
            qty,
            kind: backtest::OrderKind::Market,
            reason: reason.to_string(),
            expire_ts: None,
        };
        match self.broker.submit(trade.ts, trade.price, order).await {
            Ok(Some(ex)) => {
                self.account.apply_fill(FillRequest {
                    ts: ex.ts,
                    side: ex.side,
                    price: ex.price,
                    qty: ex.qty,
                    fee: ex.fee,
                    is_maker: ex.is_maker,
                    reason: ex.reason,
                });
            }
            Ok(None) => {} // testnet：成交经 poll_fills 回报
            Err(e) => error!(error = %e, reason, "平仓下单失败"),
        }
    }

    // ==================================================================
    // 环境/熔断/流水（与回测同逻辑）
    // ==================================================================

    fn sync_position_flag(&mut self) {
        self.ctx.position = self.account.position_view(&self.symbol);
    }

    fn update_env_flags(&mut self, ts: Timestamp, price: Price) {
        use chrono::{Datelike, Timelike};
        let secs = ts.as_millis() / 1000;
        let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
        let hour = dt.hour();
        let weekday = dt.weekday();
        let session = if matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun) {
            "weekend"
        } else if hour < 7 {
            "asia"
        } else if hour < 13 {
            "europe"
        } else {
            "us"
        };
        self.ctx.flags.insert("session".into(), session.into());
        self.ctx
            .flags
            .insert("last_price".into(), format!("{}", price.to_f64()));
    }

    fn cb_update(&mut self, ts: Timestamp, price: Price) {
        let day = ts.as_millis() / 86_400_000;
        if day != self.cb_day {
            self.cb_day = day;
            self.cb_day_start_equity = self.account.equity(price);
            self.cb_consec_losses = 0;
            self.cb_on = false;
        }
        let eq = self.account.equity(price);
        if self.config.cb_daily_dd_pct > 0.0
            && self.cb_day_start_equity > 0.0
            && eq < self.cb_day_start_equity * (1.0 - self.config.cb_daily_dd_pct)
        {
            self.cb_on = true;
        }
        if self.config.cb_max_daily_losses > 0
            && self.cb_consec_losses >= self.config.cb_max_daily_losses
        {
            self.cb_on = true;
        }
        let v = if self.cb_on { "on" } else { "off" };
        self.ctx.flags.insert("circuit_breaker".into(), v.into());
    }

    fn cb_note_fills(&mut self) {
        let fills = self.account.fills();
        while self.cb_noted_fills < fills.len() {
            let f = &fills[self.cb_noted_fills];
            self.cb_noted_fills += 1;
            if f.realized_pnl.abs() > 1e-9 {
                if f.realized_pnl - f.fee < 0.0 {
                    self.cb_consec_losses += 1;
                } else {
                    self.cb_consec_losses = 0;
                }
            }
        }
    }

    /// 原子重写 journal（tmp + rename）。
    pub fn persist_journal(&self) {
        let px = self.latest_price.unwrap_or(Price::ZERO);
        let journal = Journal {
            meta: JournalMeta {
                symbol: self.config.symbol.clone(),
                from: self.started_at.clone(),
                to: "live".into(),
                strategy: self.config.strategy_name.clone(),
                initial_cash: self.account.initial_cash(),
                final_equity: self.account.equity(px),
            },
            intents: self.intents.clone(),
            fills: self.account.fills().to_vec(),
            equity_curve: self.equity_curve.clone(),
        };
        let path = &self.config.journal_path;
        let tmp = path.with_extension("tmp");
        match serde_json::to_string_pretty(&journal) {
            Ok(text) => {
                if let Err(e) = std::fs::write(&tmp, text) {
                    warn!(error = %e, "journal tmp 写入失败");
                    return;
                }
                if let Err(e) = std::fs::rename(&tmp, path) {
                    warn!(error = %e, "journal rename 失败");
                }
            }
            Err(e) => warn!(error = %e, "journal 序列化失败"),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use backtest::FeeModel;
    use strategy::{assemble_from_toml, builtin_registry};
    use tcore::types::{Exchange, Side};

    fn trade(ts_ms: i64, price: f64) -> Trade {
        Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(0.01),
            is_buyer_maker: false,
        }
    }

    fn noop_strategy() -> Strategy {
        let toml = r#"
[strategy]
trigger = "NoopTrigger"
"#;
        assemble_from_toml(toml, &builtin_registry()).unwrap()
    }

    fn test_config(tag: &str) -> LiveConfig {
        LiveConfig {
            symbol: "BTCUSDT".into(),
            risk_pct: 0.0075,
            max_risk_pct: 0.015,
            entry_ttl_ms: 4 * 3_600_000,
            cb_max_daily_losses: 0,
            cb_daily_dd_pct: 0.0,
            qty_step: 1e-8,
            min_notional: 0.0,
            journal_path: std::env::temp_dir().join(format!("greed-live-test-{}.json", tag)),
            strategy_name: "test".into(),
        }
    }

    /// 干跑冒烟：noop 策略吃 1000 笔逐笔，不交易、权益守恒、journal 可解析。
    #[tokio::test]
    async fn dry_run_smoke_conserves() {
        let cfg = test_config("smoke");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        for i in 0..1000i64 {
            eng.on_trade(&trade(i * 100, 67000.0 + (i % 50) as f64))
                .await;
        }
        assert!(eng.account().fills().is_empty());
        assert!(
            eng.account()
                .conservation_error(Price::from_f64(67200.0))
                < 1e-9
        );
        // journal 已落盘且契约完整（前端三要素：intents/fills/equity_curve）
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(j["meta"]["symbol"], "BTCUSDT");
        assert_eq!(j["meta"]["to"], "live");
        assert!(j["intents"].is_array());
        assert!(j["fills"].is_array());
        assert!(j["equity_curve"].is_array());
        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    /// 干跑全链路：手动开仓（市价）→ 止损单自动挂出 → 价格击穿 → 止损成交。
    #[tokio::test]
    async fn dry_run_entry_then_stop() {
        let cfg = test_config("stop");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        let t0 = trade(0, 67000.0);
        eng.on_trade(&t0).await;
        let intent = OrderIntent {
            symbol: Symbol::new("BTCUSDT"),
            side: Side::Buy,
            qty: Qty::from_f64(0.1),
            limit_price: None,
            stop_price: Price::from_f64(66800.0),
            tp1_price: None,
            reason: "manual".into(),
            ts: t0.ts,
        };
        eng.enter(&t0, intent).await;
        assert!(eng.account().position().is_some());
        // 意图已入流水（含止损价与原因）
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(j["intents"][0]["reason"], "manual");
        assert!((j["intents"][0]["stop_price"].as_f64().unwrap() - 66800.0).abs() < 1e-9);

        // 价格跌破止损 → 平仓
        eng.on_trade(&trade(1000, 66900.0)).await;
        eng.on_trade(&trade(2000, 66750.0)).await;
        assert!(eng.account().position().is_none());
        assert_eq!(eng.account().fills().len(), 2);
        assert_eq!(eng.account().fills()[1].reason, "stop");
        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    /// 定时器路径：权益采样（60s 节律）。
    #[tokio::test]
    async fn timer_samples_equity() {
        let cfg = test_config("timer");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        eng.on_trade(&trade(0, 67000.0)).await;
        eng.on_timer(1_000).await; // 首个采样点
        eng.on_timer(30_000).await; // 未到 60s，不采样
        eng.on_timer(61_000).await; // 第二个采样点
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(j["equity_curve"].as_array().unwrap().len(), 2);
        let _ = std::fs::remove_file(&cfg.journal_path);
    }
}
