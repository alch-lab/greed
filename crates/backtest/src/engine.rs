//! 回放引擎：按时间戳归并多路事件流，驱动策略。
//!
//! 每个逐笔成交事件的处理顺序（确定性）：
//! 1. 推进逻辑时钟，更新最新价。
//! 2. 撮合器检查挂单触发（止损/限价）→ 记账。
//! 3. 信号插件处理事件 → 信号写入 [`Ctx`]。
//! 4. 有持仓：出场插件管理（保本/止盈/时间止损/反手）。
//! 5. 无持仓：扳机评估 → 过滤器裁决 → 仓位计算 → 下单（含止损单）。
//!
//! 时段环境（asia/europe/us/weekend）在此按 UTC 维护进 `Ctx.flags`；
//! trend/event/circuit_breaker 标志由后续的信号与风控模块维护。

use crate::account::{Account, Fill, FillRequest};
use crate::journal::JournalIntent;
use crate::broker::{Broker, Order, OrderKind};
use crate::fees::FeeModel;
use crate::report::EquityPoint;
use strategy::Strategy;
use tcore::plugin::{Ctx, ExitAction, OrderIntent, Signal, Verdict};
use tcore::types::{Price, Qty, Symbol, Timestamp};
use tcore::{Event, EventClock, Trade};

/// 回测配置。
#[derive(Debug, Clone, Copy)]
pub struct BacktestConfig {
    pub initial_cash: f64,
    pub fee_model: FeeModel,
    /// 固定风险百分比：基础仓位 = equity × risk_pct / 止损距离（USD）
    pub risk_pct: f64,
    /// 单笔最大风险（占 equity 比例上限）
    pub max_risk_pct: f64,
    /// 限价入场单的有效期（毫秒），超时未成交自动撤单
    pub entry_ttl_ms: i64,
    /// 熔断：日连亏笔数上限（0 = 关闭）。触发后当日不再开仓
    pub cb_max_daily_losses: u32,
    /// 熔断：日内回撤上限（0 = 关闭）。equity 跌破日初 ×(1-此值) 当日不再开仓
    pub cb_daily_dd_pct: f64,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            initial_cash: 100_000.0,
            fee_model: FeeModel::default(),
            risk_pct: 0.0075,
            max_risk_pct: 0.015,
            entry_ttl_ms: 4 * 3_600_000, // 默认 4 小时
            cb_max_daily_losses: 0,
            cb_daily_dd_pct: 0.0,
        }
    }
}

/// 回测结果Ò
#[derive(Debug)]
pub struct BacktestResult {
    pub fills: Vec<Fill>,
    /// 全部下单意图（含被仓位计算调整后的数量）
    pub intents: Vec<JournalIntent>,
    pub final_equity: f64,
    pub initial_cash: f64,
    /// 权益曲线（按 UTC 日采集）
    pub equity_curve: Vec<EquityPoint>,
}

/// 回测引擎。
pub struct BacktestEngine {
    strategy: Strategy,
    broker: Broker,
    account: Account,
    ctx: Ctx,
    clock: EventClock,
    symbol: Symbol,
    latest_price: Option<Price>,
    config: BacktestConfig,
    equity_curve: Vec<EquityPoint>,
    last_equity_day: i64,
    /// 未成交的限价入场意图（延迟成交后补挂止损/止盈）
    pending_entry: Option<PendingEntry>,
    // ---- 熔断状态（日连亏/日回撤）----
    cb_day: i64,
    cb_day_start_equity: f64,
    cb_consec_losses: u32,
    cb_on: bool,
    /// 已统计过的成交条数（fills 下标游标）
    cb_noted_fills: usize,
    /// 决策流水：下单意图
    intents: Vec<JournalIntent>,
}

/// 挂起中的限价入场：成交后需要补挂的止损/止盈参数。
#[derive(Debug, Clone, Copy)]
struct PendingEntry {
    stop_price: Price,
    tp1_price: Option<Price>,
    expire_ts: Timestamp,
}

impl BacktestEngine {
    pub fn new(strategy: Strategy, symbol: Symbol, config: BacktestConfig) -> Self {
        let broker = Broker::new(config.fee_model);
        let account = Account::new(config.initial_cash);
        BacktestEngine {
            strategy,
            broker,
            account,
            ctx: Ctx::default(),
            clock: EventClock::new(),
            symbol,
            latest_price: None,
            config,
            equity_curve: Vec::new(),
            last_equity_day: i64::MIN,
            pending_entry: None,
            cb_day: i64::MIN,
            cb_day_start_equity: 0.0,
            cb_consec_losses: 0,
            cb_on: false,
            cb_noted_fills: 0,
            intents: Vec::new(),
        }
    }

    pub fn account(&self) -> &Account {
        &self.account
    }

    /// 运行整个事件序列（trades 与 OI/资金费率按时间戳归并后传入），返回结果。
    pub fn run(&mut self, events: &[Event]) -> BacktestResult {
        for ev in events {
            match ev {
                Event::Trade(t) => self.on_trade(t),
                Event::Oi(o) => self.on_oi(o, ev),
                Event::Funding(f) => self.on_funding(f, ev),
                _ => {}
            }
        }
        let final_px = self.latest_price.unwrap_or(Price::ZERO);
        BacktestResult {
            fills: self.account.fills().to_vec(),
            intents: std::mem::take(&mut self.intents),
            final_equity: self.account.equity(final_px),
            initial_cash: self.config.initial_cash,
            equity_curve: std::mem::take(&mut self.equity_curve),
        }
    }

    /// OI 刻度：推进时钟并喂给信号插件（不触发撮合/出场/开仓）。
    fn on_oi(&mut self, o: &tcore::OiTick, ev: &Event) {
        self.clock.advance_to(o.ts);
        self.ctx.now = Some(o.ts);
        for sp in self.strategy.signals.iter_mut() {
            for sig in sp.on_event(ev, &self.ctx) {
                self.ctx.set_latest(sig);
            }
        }
    }

    /// 资金费率刻度：写入 ctx.flags["funding_rate"] 供过滤器裁决，并喂给信号插件。
    fn on_funding(&mut self, f: &tcore::FundingTick, ev: &Event) {
        self.clock.advance_to(f.ts);
        self.ctx.now = Some(f.ts);
        self.ctx
            .flags
            .insert("funding_rate".into(), format!("{}", f.rate));
        for sp in self.strategy.signals.iter_mut() {
            for sig in sp.on_event(ev, &self.ctx) {
                self.ctx.set_latest(sig);
            }
        }
    }

    /// 按 UTC 日采样权益
    fn sample_equity(&mut self, ts: Timestamp) {
        let day = ts.as_millis() / 86_400_000;
        if day != self.last_equity_day {
            self.last_equity_day = day;
            let px = self.latest_price.unwrap_or(Price::ZERO);
            self.equity_curve.push(EquityPoint {
                ts_ms: ts.as_millis(),
                equity: self.account.equity(px),
            });
        }
    }

    /// 熔断：日 rollover + 阈值评估，把结果写进 ctx.flags["circuit_breaker"]。
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

    /// 统计新成交的已实现盈亏，维护日连亏计数（开仓 realized=0 忽略）。
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

    /// 处理单个逐笔成交。
    fn on_trade(&mut self, trade: &Trade) {
        // 1) 时钟与最新价
        self.clock.advance_to(trade.ts);
        self.latest_price = Some(trade.price);
        self.update_env_flags(trade.ts, trade.price);
        self.sample_equity(trade.ts);
        self.cb_update(trade.ts, trade.price);

        // 2) 撮合器挂单触发（止损/限价）
        let execs = self.broker.on_trade_price(trade.ts, trade.price);
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
        self.settle_pending_entry(trade);

        // 3) 信号插件
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

        // 4) 持仓管理（出场插件 + 加仓钩子）
        if self.account.position().is_some() {
            self.manage_position(trade);
            self.try_add(trade, &new_signals);
        } else {
            // 5) 无持仓：扳机评估
            self.try_enter(trade, &new_signals);
        }

        // 6) 熔断：统计本笔新成交的盈亏并重新评估阈值
        self.cb_note_fills();
        self.cb_update(trade.ts, trade.price);
    }

    /// 出场插件管理持仓。
    fn manage_position(&mut self, trade: &Trade) {
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
                        // 重挂止损单（先清旧单）
                        self.broker.cancel_all();
                        self.broker.submit(
                            trade.ts,
                            trade.price,
                            Order {
                                side: side.opposite(),
                                qty,
                                kind: OrderKind::StopMarket(px),
                                reason: "stop".into(),
                                expire_ts: None,
                            },
                        );
                    }
                }
                ExitAction::ClosePartial(frac) => {
                    if let Some(p) = self.account.position().copied() {
                        let close_qty = Qty::from_f64(p.qty.to_f64() * frac);
                        self.market_close(trade, close_qty, "tp_partial");
                        if let Some(pp) = self.account.position_mut() {
                            pp.closed_frac = (pp.closed_frac + frac).min(1.0);
                        }
                    }
                }
                ExitAction::CloseAll => {
                    if let Some(p) = self.account.position().copied() {
                        self.market_close(trade, p.qty, "close_all");
                    }
                }
                ExitAction::Reverse(intent) => {
                    if let Some(p) = self.account.position().copied() {
                        self.market_close(trade, p.qty, "reverse_out");
                    }
                    self.enter(trade, *intent);
                }
            }
        }
    }

    /// 无持仓时尝试开仓。
    fn try_enter(&mut self, trade: &Trade, signals: &[Signal]) {
        // 有未成交的限价入场单在挂：不再开新仓
        if self.pending_entry.is_some() {
            return;
        }
        // 通用无状态扳机优先;力竭反转等有状态扳机走 on_signals 路径
        let intent = self
            .strategy
            .trigger
            .should_fire(signals, &self.ctx)
            .or_else(|| {
                self.strategy
                    .trigger
                    .on_signals(signals, &self.ctx, &self.symbol)
            });
        let Some(intent) = intent else {
            return;
        };

        // 过滤器裁决：任一 Veto 则放弃；Scale 取最小系数
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
        self.enter(trade, intent);
    }

    /// 持仓中尝试加仓（金字塔）。
    fn try_add(&mut self, trade: &Trade, signals: &[Signal]) {
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
        // 过滤器裁决：任一 Veto 则放弃；Scale 取最小系数
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
        self.enter(trade, intent);
    }

    /// 执行开仓（含仓位计算与止损挂单）。
    fn enter(&mut self, trade: &Trade, intent: OrderIntent) {
        // 仓位计算：equity × risk_pct / 止损距离，风险比例封顶 max_risk_pct；
        // 若扳机已指定数量（网格/蓝带等），取较小者。
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
        if qty <= 1e-9 {
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
        let is_limit = intent.limit_price.is_some();
        let expire_ts = if is_limit {
            Some(Timestamp::from_millis(
                trade.ts.as_millis() + self.config.entry_ttl_ms,
            ))
        } else {
            None
        };

        let order = Order {
            side: intent.side,
            qty,
            kind: match intent.limit_price {
                Some(lp) => OrderKind::Limit(lp),
                None => OrderKind::Market,
            },
            reason: intent.reason.clone(),
            expire_ts,
        };
        if let Some(ex) = self.broker.submit(trade.ts, trade.price, order) {
            self.account.apply_fill(FillRequest {
                ts: ex.ts,
                side: ex.side,
                price: ex.price,
                qty: ex.qty,
                fee: ex.fee,
                is_maker: ex.is_maker,
                reason: ex.reason,
            });
        } else if is_limit {
            // 限价单未立即成交：记录挂起意图，成交后由 settle_pending_entry 补挂止损
            if let Some(exp) = expire_ts {
                self.pending_entry = Some(PendingEntry {
                    stop_price: intent.stop_price,
                    tp1_price: intent.tp1_price,
                    expire_ts: exp,
                });
            }
        }
        // 设置持仓止损/止盈并挂止损单（加仓时先撤旧止损单，按新总仓重挂）
        if let Some(p) = self.account.position_mut() {
            p.stop_price = Some(intent.stop_price);
            p.tp1_price = intent.tp1_price;
            let side = p.side;
            let q = p.qty;
            self.broker.cancel_all();
            self.broker.submit(
                trade.ts,
                trade.price,
                Order {
                    side: side.opposite(),
                    qty: q,
                    kind: OrderKind::StopMarket(intent.stop_price),
                    reason: "stop".into(),
                    expire_ts: None,
                },
            );
        }
    }

    /// 挂起限价入场的善后：成交→补挂止损/止盈；过期→清除意图。
    fn settle_pending_entry(&mut self, trade: &Trade) {
        let Some(pe) = self.pending_entry else {
            return;
        };
        if self.account.position().is_some() {
            // 延迟成交：补挂止损单与 TP 参考
            if let Some(p) = self.account.position_mut() {
                p.stop_price = Some(pe.stop_price);
                p.tp1_price = pe.tp1_price;
                let side = p.side;
                let q = p.qty;
                self.broker.submit(
                    trade.ts,
                    trade.price,
                    Order {
                        side: side.opposite(),
                        qty: q,
                        kind: OrderKind::StopMarket(pe.stop_price),
                        reason: "stop".into(),
                        expire_ts: None,
                    },
                );
            }
            self.pending_entry = None;
        } else if trade.ts.as_millis() > pe.expire_ts.as_millis() {
            // 已过期（broker 侧已撤单）
            self.pending_entry = None;
        }
    }

    /// 市价平仓。
    fn market_close(&mut self, trade: &Trade, qty: Qty, reason: &str) {
        let Some(p) = self.account.position() else {
            return;
        };
        let side = p.side.opposite();
        let order = Order {
            side,
            qty,
            kind: OrderKind::Market,
            reason: reason.to_string(),
            expire_ts: None,
        };
        self.broker.cancel_all(); // 清掉止损单
        if let Some(ex) = self.broker.submit(trade.ts, trade.price, order) {
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
    }

    /// 把账户持仓同步到 ctx.position（供出场/扳机插件读取）。
    fn sync_position_flag(&mut self) {
        self.ctx.position = self.account.position_view(&self.symbol);
    }

    /// 维护时段等环境标志（UTC）。
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use strategy::{assemble_from_toml, builtin_registry};
    use tcore::types::{Exchange, Side};

    fn trade(ts_ms: i64, price: f64, qty: f64) -> Trade {
        Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
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

    #[test]
    fn empty_strategy_runs_and_conserves() {
        let mut eng = BacktestEngine::new(
            noop_strategy(),
            Symbol::new("BTCUSDT"),
            BacktestConfig::default(),
        );
        let trades: Vec<Trade> = (0..1000)
            .map(|i| trade(i * 100, 67000.0 + (i % 50) as f64, 0.01))
            .collect();
        let events: Vec<Event> = trades.iter().cloned().map(Event::Trade).collect();
        let res = eng.run(&events);
        assert!(res.fills.is_empty());
        assert_eq!(res.final_equity, res.initial_cash);
        assert_eq!(
            eng.account().conservation_error(Price::from_f64(67200.0)),
            0.0
        );
    }

    /// 手动下单策略（不经扳机）：验证 engine 的开仓→止损链路。
    #[test]
    fn stop_loss_via_broker() {
        let mut eng = BacktestEngine::new(
            noop_strategy(),
            Symbol::new("BTCUSDT"),
            BacktestConfig::default(),
        );
        // 直接通过内部 enter 开多（模拟扳机触发）
        let t0 = trade(0, 67000.0, 0.01);
        eng.clock.advance_to(t0.ts);
        eng.latest_price = Some(t0.price);
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
        eng.enter(&t0, intent);
        assert!(eng.account().position().is_some());
        // 价格跌到 66750 触发止损
        let trades = vec![trade(1000, 66900.0, 0.01), trade(2000, 66750.0, 0.01)];
        let events: Vec<Event> = trades.iter().cloned().map(Event::Trade).collect();
        let res = eng.run(&events);
        // 应有开仓 + 止损平仓两笔成交
        assert_eq!(res.fills.len(), 2);
        assert_eq!(res.fills[1].reason, "stop");
        assert!(eng.account().position().is_none());
        // 亏损约 200*0.1=20 美元 + 手续费
        assert!(res.final_equity < res.initial_cash);
    }

    #[test]
    fn session_flag_set() {
        let mut eng = BacktestEngine::new(
            noop_strategy(),
            Symbol::new("BTCUSDT"),
            BacktestConfig::default(),
        );
        // 2024-01-01 是周一；03:00 UTC → asia
        let t = trade(1_704_067_200_000, 67000.0, 0.01); // 2024-01-01 00:00 UTC
        eng.on_trade(&t);
        assert_eq!(eng.ctx.flag("session"), Some("asia"));
        // 14:00 UTC → us
        let t2 = trade(1_704_117_600_000, 67000.0, 0.01);
        eng.on_trade(&t2);
        assert_eq!(eng.ctx.flag("session"), Some("us"));
    }
}
