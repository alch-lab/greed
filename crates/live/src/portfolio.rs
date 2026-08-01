//! 单账户组合执行器：各策略生成目标敞口，执行层只向币安提交合并后的净仓位。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use backtest::JournalSleeve;
use serde::Deserialize;
use tracing::{info, warn};

use crate::rest::{floor_to_step, RestClient, RestError, SymbolFilters};
use crate::spot::SpotRestClient;
use crate::warmup::fetch_klines;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PortfolioConfig {
    pub enabled: bool,
    pub ema_enabled: bool,
    pub ema_fast: usize,
    pub ema_slow: usize,
    pub ema_notional_x: f64,
    pub carry_enabled: bool,
    pub carry_notional_x: f64,
    pub probe_enabled: bool,
    pub probe_notional_usdt: f64,
    pub reconcile_secs: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ema_detects_uptrend() {
        let values: Vec<f64> = (1..=150).map(|x| x as f64).collect();
        assert!(
            PortfolioExecutor::ema(&values, 10).unwrap()
                > PortfolioExecutor::ema(&values, 100).unwrap()
        );
    }

    #[test]
    fn carry_does_not_book_directional_btc_move() {
        let mut ledger = Ledger::new("funding_carry", "carry", "test");
        ledger.target(-0.25, 100_000.0, 1);
        ledger.mark(110_000.0);
        assert_eq!(ledger.pnl, 0.0);
    }

    #[test]
    fn probe_round_trip_counts_once() {
        let mut ledger = Ledger::new("execution_probe", "probe", "test");
        ledger.target(0.001, 100_000.0, 1);
        ledger.target(0.0, 100_100.0, 2);
        assert_eq!(ledger.trades, 1);
        assert_eq!(ledger.wins, 1);
    }

    #[test]
    fn persisted_state_keeps_statistics() {
        let state = PersistedState {
            spot_baseline_qty: 0.5,
            funding_cursor_ms: 123,
            ema: LedgerState {
                pnl: 12.5,
                trades: 4,
                ..Default::default()
            },
            ..Default::default()
        };
        let text = serde_json::to_string(&state).unwrap();
        let restored: PersistedState = serde_json::from_str(&text).unwrap();
        assert_eq!(restored.ema.pnl, 12.5);
        assert_eq!(restored.ema.trades, 4);
        assert_eq!(restored.spot_baseline_qty, 0.5);
    }
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ema_enabled: true,
            ema_fast: 10,
            ema_slow: 100,
            ema_notional_x: 0.10,
            carry_enabled: true,
            carry_notional_x: 0.25,
            probe_enabled: true,
            probe_notional_usdt: 10.0,
            reconcile_secs: 5,
        }
    }
}

#[derive(Debug, Clone)]
struct Ledger {
    key: &'static str,
    label: &'static str,
    target_qty: f64,
    last_price: Option<f64>,
    pnl: f64,
    fees: f64,
    trades: u64,
    wins: u64,
    trip_pnl: f64,
    last_action_ms: Option<i64>,
    detail: String,
}

impl Ledger {
    fn new(key: &'static str, label: &'static str, detail: impl Into<String>) -> Self {
        Self {
            key,
            label,
            target_qty: 0.0,
            last_price: None,
            pnl: 0.0,
            fees: 0.0,
            trades: 0,
            wins: 0,
            trip_pnl: 0.0,
            last_action_ms: None,
            detail: detail.into(),
        }
    }

    fn mark(&mut self, price: f64) {
        if let Some(prev) = self.last_price {
            // carry 的现货多与永续空共同抵消方向价格风险；其收益以真实资金费
            // 入账为准。这里不把永续腿单独的价格变化误记为策略收益。
            let change = if self.key == "funding_carry" {
                0.0
            } else {
                self.target_qty * (price - prev)
            };
            self.pnl += change;
            self.trip_pnl += change;
        }
        self.last_price = Some(price);
    }

    fn target(&mut self, qty: f64, price: f64, now_ms: i64) -> bool {
        self.mark(price);
        if (qty - self.target_qty).abs() < 1e-10 {
            return false;
        }
        if self.target_qty.abs() > 1e-10
            && (qty.abs() < 1e-10 || qty.signum() != self.target_qty.signum())
        {
            self.trades += 1;
            if self.trip_pnl > 0.0 {
                self.wins += 1;
            }
            self.trip_pnl = 0.0;
        }
        self.target_qty = qty;
        self.last_action_ms = Some(now_ms);
        true
    }

    fn fee(&mut self, fee: f64) {
        self.fees += fee;
        self.pnl -= fee;
        self.trip_pnl -= fee;
    }

    fn snapshot(&self, source: &str) -> JournalSleeve {
        JournalSleeve {
            key: self.key.into(),
            label: self.label.into(),
            state: "executing".into(),
            pnl: self.pnl,
            fees: self.fees,
            trades: self.trades,
            wins: self.wins,
            target_qty: self.target_qty,
            actual_source: source.into(),
            last_action_ms: self.last_action_ms,
            detail: self.detail.clone(),
        }
    }

    fn restore(&mut self, state: &LedgerState) {
        self.target_qty = state.target_qty;
        self.last_price = state.last_price;
        self.pnl = state.pnl;
        self.fees = state.fees;
        self.trades = state.trades;
        self.wins = state.wins;
        self.trip_pnl = state.trip_pnl;
        self.last_action_ms = state.last_action_ms;
    }

    fn state(&self) -> LedgerState {
        LedgerState {
            target_qty: self.target_qty,
            last_price: self.last_price,
            pnl: self.pnl,
            fees: self.fees,
            trades: self.trades,
            wins: self.wins,
            trip_pnl: self.trip_pnl,
            last_action_ms: self.last_action_ms,
        }
    }
}

pub struct PortfolioExecutor {
    cfg: PortfolioConfig,
    symbol: String,
    /// 仓位规模基准权益（启动=钱包余额，运行中每 5min 跟随真实钱包刷新）
    sizing_equity: f64,
    futures: RestClient,
    futures_filters: SymbolFilters,
    spot: Option<SpotRestClient>,
    spot_step: f64,
    http: reqwest::Client,
    market_base: String,
    ema_side: i8,
    applied_ema_side: i8,
    ema: Ledger,
    carry: Ledger,
    probe: Ledger,
    /// MR 腿镜像到真实净仓的成交费用承接账本（fee_weights 无法分摊时的残余）
    mr_mirror: Ledger,
    probe_day: Option<i64>,
    probe_open: bool,
    last_reconcile_ms: i64,
    last_daily_refresh_ms: i64,
    last_equity_refresh_ms: i64,
    last_time_sync_ms: i64,
    last_trade_id: i64,
    funding_cursor_ms: i64,
    funding_ids: HashSet<i64>,
    spot_baseline_qty: f64,
    state_path: PathBuf,
    /// 每个真实永续订单对应的 sleeve 目标变化，用 orderId 精确归因成交费用。
    fee_weights_by_order: HashMap<i64, [f64; 3]>,
    last_actual_qty: f64,
    /// 真实账户权益（合约钱包+未实现盈亏；现货 USDT+BTC 市值），reconcile 周期刷新
    real_futures_equity: f64,
    real_spot_equity: f64,
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
struct LedgerState {
    target_qty: f64,
    last_price: Option<f64>,
    pnl: f64,
    fees: f64,
    trades: u64,
    wins: u64,
    trip_pnl: f64,
    last_action_ms: Option<i64>,
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
struct PersistedState {
    spot_baseline_qty: f64,
    funding_cursor_ms: i64,
    ema_side: i8,
    applied_ema_side: i8,
    probe_day: Option<i64>,
    probe_open: bool,
    ema: LedgerState,
    carry: LedgerState,
    probe: LedgerState,
    mr_mirror: LedgerState,
}

impl PortfolioExecutor {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        cfg: PortfolioConfig,
        symbol: String,
        sizing_equity: f64,
        mut futures: RestClient,
        futures_filters: SymbolFilters,
        mut spot: Option<SpotRestClient>,
        http: reqwest::Client,
        market_base: String,
        state_path: PathBuf,
    ) -> Result<Self, RestError> {
        futures.sync_time().await?;
        let last_actual_qty = futures.position_amt(&symbol).await?;
        let last_trade_id = futures
            .user_trades(&symbol, 0)
            .await?
            .last()
            .map(|t| t.trade_id)
            .unwrap_or(0);
        let spot_step = if let Some(client) = &mut spot {
            client.sync_time().await?;
            client.lot_step(&symbol).await?
        } else {
            0.0
        };
        let now = chrono::Utc::now().timestamp_millis();
        let saved = std::fs::read_to_string(&state_path)
            .ok()
            .and_then(|text| serde_json::from_str::<PersistedState>(&text).ok());
        let base_asset = symbol.strip_suffix("USDT").unwrap_or("BTC");
        let spot_baseline_qty = match saved.as_ref() {
            Some(state) => state.spot_baseline_qty,
            None => match &spot {
                Some(client) => client.free_balance(base_asset).await?,
                None => 0.0,
            },
        };
        let funding_cursor_ms = saved
            .as_ref()
            .map(|s| s.funding_cursor_ms)
            .unwrap_or(now - 24 * 3_600_000);
        let mut out = Self {
            cfg,
            symbol,
            sizing_equity,
            futures,
            futures_filters,
            spot,
            spot_step,
            http,
            market_base,
            ema_side: 0,
            applied_ema_side: 0,
            ema: Ledger::new("ema_trend", "EMA 日线趋势", "币安 1d Kline · EMA10/100"),
            carry: Ledger::new(
                "funding_carry",
                "资金费 Carry",
                "真实现货多 + 永续空 · FUNDING_FEE 入账",
            ),
            probe: Ledger::new(
                "execution_probe",
                "执行探针（健康检查）",
                "非盈利策略 · 每日 UTC 00:00 一次真实往返",
            ),
            mr_mirror: Ledger::new(
                "mr_mirror",
                "MR 镜像净仓",
                "MR 腿镜像成交的真实费用（引擎内为模拟撮合）",
            ),
            probe_day: None,
            probe_open: false,
            last_reconcile_ms: 0,
            last_daily_refresh_ms: 0,
            last_equity_refresh_ms: 0,
            last_time_sync_ms: now,
            last_trade_id,
            funding_cursor_ms,
            funding_ids: HashSet::new(),
            spot_baseline_qty,
            state_path,
            fee_weights_by_order: HashMap::new(),
            last_actual_qty,
            real_futures_equity: sizing_equity,
            real_spot_equity: 0.0,
        };
        if let Some(state) = saved {
            out.ema_side = state.ema_side;
            out.applied_ema_side = state.applied_ema_side;
            out.probe_day = state.probe_day;
            out.probe_open = state.probe_open;
            out.ema.restore(&state.ema);
            out.carry.restore(&state.carry);
            out.probe.restore(&state.probe);
            out.mr_mirror.restore(&state.mr_mirror);
        }
        out.persist_state();
        out.refresh_ema(now).await?;
        Ok(out)
    }

    fn persist_state(&self) {
        let state = PersistedState {
            spot_baseline_qty: self.spot_baseline_qty,
            funding_cursor_ms: self.funding_cursor_ms,
            ema_side: self.ema_side,
            applied_ema_side: self.applied_ema_side,
            probe_day: self.probe_day,
            probe_open: self.probe_open,
            ema: self.ema.state(),
            carry: self.carry.state(),
            probe: self.probe.state(),
            mr_mirror: self.mr_mirror.state(),
        };
        if let Some(parent) = self.state_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(text) = serde_json::to_string_pretty(&state) {
            let _ = std::fs::write(&self.state_path, text);
        }
    }

    fn ema(values: &[f64], period: usize) -> Option<f64> {
        if values.len() < period || period == 0 {
            return None;
        }
        let k = 2.0 / (period as f64 + 1.0);
        Some(
            values[1..]
                .iter()
                .fold(values[0], |acc, p| p * k + acc * (1.0 - k)),
        )
    }

    async fn refresh_ema(&mut self, now_ms: i64) -> Result<(), RestError> {
        if !self.cfg.ema_enabled {
            return Ok(());
        }
        let bars = fetch_klines(&self.http, &self.market_base, &self.symbol, "1d", 150).await?;
        let closes: Vec<f64> = bars
            .iter()
            .take(bars.len().saturating_sub(1))
            .map(|b| b.close)
            .collect();
        let (Some(fast), Some(slow)) = (
            Self::ema(&closes, self.cfg.ema_fast),
            Self::ema(&closes, self.cfg.ema_slow),
        ) else {
            return Ok(());
        };
        self.ema_side = if fast >= slow { 1 } else { -1 };
        self.ema.detail = format!(
            "币安 1d Kline · EMA{} {:.2} / EMA{} {:.2}",
            self.cfg.ema_fast, fast, self.cfg.ema_slow, slow
        );
        self.last_daily_refresh_ms = now_ms;
        Ok(())
    }

    async fn actual_fees_and_funding(&mut self) {
        match self
            .futures
            .user_trades(&self.symbol, self.last_trade_id + 1)
            .await
        {
            Ok(rows) => {
                for row in rows {
                    self.last_trade_id = self.last_trade_id.max(row.trade_id);
                    let fee = row.commission.parse::<f64>().unwrap_or(0.0);
                    let weights = self
                        .fee_weights_by_order
                        .get(&row.order_id)
                        .copied()
                        .unwrap_or([0.0; 3]);
                    let total: f64 = weights.iter().sum();
                    if total > 0.0 {
                        self.ema.fee(fee * weights[0] / total);
                        self.carry.fee(fee * weights[1] / total);
                        self.probe.fee(fee * weights[2] / total);
                    } else {
                        // 三条腿目标都未变化时，这笔净仓成交是 MR 腿镜像驱动的
                        // （含重启强平等场景）——费用归入 MR 镜像账本，不再静默丢弃。
                        self.mr_mirror.fee(fee);
                    }
                }
            }
            Err(e) => warn!(error = %e, "组合成交费用查询失败"),
        }
        match self
            .futures
            .funding_income_since(&self.symbol, self.funding_cursor_ms)
            .await
        {
            Ok(rows) => {
                for row in rows {
                    self.funding_cursor_ms = self.funding_cursor_ms.max(row.time + 1);
                    if self.funding_ids.insert(row.tran_id) {
                        let income = row.income.parse::<f64>().unwrap_or(0.0);
                        self.carry.pnl += income;
                        self.carry.trip_pnl += income;
                        self.carry.trades += 1;
                        if income > 0.0 {
                            self.carry.wins += 1;
                        }
                        self.carry.last_action_ms = Some(row.time);
                    }
                }
            }
            Err(e) => warn!(error = %e, "FUNDING_FEE 入账查询失败"),
        }
        self.persist_state();
    }

    pub async fn reconcile(
        &mut self,
        now_ms: i64,
        price: f64,
        mr_qty: f64,
    ) -> Result<Vec<JournalSleeve>, RestError> {
        if now_ms - self.last_reconcile_ms < self.cfg.reconcile_secs * 1000 {
            return Ok(self.snapshots());
        }
        self.last_reconcile_ms = now_ms;
        // 仓位规模跟随真实权益（5min 刷新）：复利/亏损都反映到 EMA/carry 名义敞口
        if now_ms - self.last_equity_refresh_ms >= 300_000 {
            self.last_equity_refresh_ms = now_ms;
            match self.futures.wallet_balance_usdt().await {
                Ok(w) if w > 0.0 => self.sizing_equity = w,
                Ok(_) => {}
                Err(e) => warn!(error = %e, "权益刷新失败（沿用上值）"),
            }
        }
        // 长跑时钟漂移防护（6h 重对时，防 -1021）
        if now_ms - self.last_time_sync_ms >= 6 * 3_600_000 {
            self.last_time_sync_ms = now_ms;
            if let Err(e) = self.futures.sync_time().await {
                warn!(error = %e, "组合定期对时失败（下周期重试）");
            }
        }
        if now_ms - self.last_daily_refresh_ms >= 3_600_000 {
            self.refresh_ema(now_ms).await?;
        }
        let old_targets = [
            self.ema.target_qty,
            self.carry.target_qty,
            self.probe.target_qty,
        ];
        let ema_qty = if self.ema.target_qty.abs() < 1e-10 || self.applied_ema_side != self.ema_side
        {
            self.applied_ema_side = self.ema_side;
            self.ema_side as f64 * self.sizing_equity * self.cfg.ema_notional_x / price
        } else {
            self.ema.target_qty
        };
        self.ema.target(
            if self.cfg.ema_enabled { ema_qty } else { 0.0 },
            price,
            now_ms,
        );
        // 双腿安全顺序：先把现货腿对齐，再以现货实际余额生成永续空目标。
        // 现货失败时直接返回，不会先留下裸空仓。
        let mut spot_change = 0.0f64;
        let carry_qty = if self.cfg.carry_enabled {
            let spot = self
                .spot
                .as_ref()
                .ok_or_else(|| RestError::Data("carry 已启用但缺少现货 Demo 凭证".into()))?;
            let base_asset = self.symbol.strip_suffix("USDT").unwrap_or("BTC");
            let actual_spot = spot.free_balance(base_asset).await?;
            let target_spot =
                self.spot_baseline_qty + self.sizing_equity * self.cfg.carry_notional_x / price;
            let spot_delta = floor_to_step((target_spot - actual_spot).abs(), self.spot_step);
            let mut managed_spot = actual_spot - self.spot_baseline_qty;
            if spot_delta * price >= 10.0 && spot_delta > 0.0 {
                let side = if target_spot > actual_spot {
                    "BUY"
                } else {
                    "SELL"
                };
                let fill = spot
                    .market_order(&self.symbol, side, spot_delta, self.spot_step)
                    .await?;
                self.carry.fee(fill.fee_usdt);
                spot_change = if side == "BUY" { fill.qty } else { -fill.qty };
                managed_spot += spot_change;
                info!(
                    side,
                    qty = fill.qty,
                    quote = fill.quote_qty,
                    "carry 现货腿已对齐"
                );
            }
            -managed_spot.max(0.0)
        } else {
            0.0
        };
        self.carry.target(carry_qty, price, now_ms);

        let day = now_ms.div_euclid(86_400_000);
        if self.cfg.probe_enabled && self.probe_day != Some(day) {
            self.probe_day = Some(day);
            self.probe_open = true;
            let notional = self
                .cfg
                .probe_notional_usdt
                .max(self.futures_filters.min_notional * 1.05);
            self.probe.target(notional / price, price, now_ms);
        } else if self.probe_open {
            self.probe_open = false;
            self.probe.target(0.0, price, now_ms);
        } else {
            self.probe.mark(price);
        }

        let target = mr_qty + self.ema.target_qty + self.carry.target_qty + self.probe.target_qty;
        let actual = self.futures.position_amt(&self.symbol).await?;
        let delta = floor_to_step((target - actual).abs(), self.futures_filters.step_size);
        if delta * price >= self.futures_filters.min_notional && delta > 0.0 {
            let fee_weights = [
                (self.ema.target_qty - old_targets[0]).abs(),
                (self.carry.target_qty - old_targets[1]).abs(),
                (self.probe.target_qty - old_targets[2]).abs(),
            ];
            let side = if target > actual { "BUY" } else { "SELL" };
            match self
                .futures
                .place_order(
                    &self.symbol,
                    side,
                    "MARKET",
                    delta,
                    None,
                    None,
                    false,
                    &self.futures_filters,
                )
                .await
            {
                Ok(order_id) => {
                    self.fee_weights_by_order.insert(order_id, fee_weights);
                }
                Err(error) => {
                    // 超时不代表订单一定失败：先查询实际净仓。若确实未对齐，撤回本轮
                    // 新增的现货数量，避免 carry 单腿暴露。
                    let after = self
                        .futures
                        .position_amt(&self.symbol)
                        .await
                        .unwrap_or(actual);
                    if (target - after).abs() * price >= self.futures_filters.min_notional {
                        if spot_change.abs() > 0.0 {
                            if let Some(spot) = &self.spot {
                                let rollback_side = if spot_change > 0.0 { "SELL" } else { "BUY" };
                                if let Ok(fill) = spot
                                    .market_order(
                                        &self.symbol,
                                        rollback_side,
                                        spot_change.abs(),
                                        self.spot_step,
                                    )
                                    .await
                                {
                                    self.carry.fee(fill.fee_usdt);
                                    warn!(
                                        rollback_side,
                                        qty = fill.qty,
                                        "永续腿失败，已回滚本轮现货变化"
                                    );
                                }
                            }
                        }
                        return Err(error);
                    }
                    warn!(error = %error, "永续下单响应异常，但持仓查询确认已成交");
                }
            }
            info!(target, actual, delta, side, "组合永续净仓位已对齐");
        }

        // 真实账户权益（总资产口径）：合约钱包+未实现盈亏，现货 USDT+BTC 市值。
        // 查询失败沿用旧值，不阻塞交易。
        match self.futures.account_equity().await {
            Ok((wallet, unrealized)) => self.real_futures_equity = wallet + unrealized,
            Err(e) => warn!(error = %e, "合约真实权益查询失败（沿用上值）"),
        }
        if let Some(spot) = &self.spot {
            let base_asset = self.symbol.strip_suffix("USDT").unwrap_or("BTC");
            match spot.equity_balances(base_asset).await {
                Ok((usdt, base_qty)) => {
                    self.real_spot_equity = usdt + base_qty * price;
                }
                Err(e) => warn!(error = %e, "现货真实权益查询失败（沿用上值）"),
            }
        }

        self.actual_fees_and_funding().await;
        self.last_actual_qty = self
            .futures
            .position_amt(&self.symbol)
            .await
            .unwrap_or(target);
        Ok(self.snapshots())
    }

    /// 真实账户权益分解：(合约, 现货, 总计)。
    pub fn real_equity(&self) -> (f64, f64, f64) {
        (
            self.real_futures_equity,
            self.real_spot_equity,
            self.real_futures_equity + self.real_spot_equity,
        )
    }

    pub fn actual_qty(&self) -> f64 {
        self.last_actual_qty
    }

    /// 停止组合：撤回全部受管永续净仓，并把 carry 现货腿恢复到启动基线。
    pub async fn shutdown(
        &mut self,
        now_ms: i64,
        price: f64,
    ) -> Result<Vec<JournalSleeve>, RestError> {
        let actual = self.futures.position_amt(&self.symbol).await?;
        let qty = floor_to_step(actual.abs(), self.futures_filters.step_size);
        if qty * price >= self.futures_filters.min_notional && qty > 0.0 {
            let side = if actual > 0.0 { "SELL" } else { "BUY" };
            self.futures
                .place_order(
                    &self.symbol,
                    side,
                    "MARKET",
                    qty,
                    None,
                    None,
                    true,
                    &self.futures_filters,
                )
                .await?;
            info!(side, qty, "组合停止：永续净仓已撤回");
        }
        if let Some(spot) = &self.spot {
            let base_asset = self.symbol.strip_suffix("USDT").unwrap_or("BTC");
            let actual_spot = spot.free_balance(base_asset).await?;
            let delta = floor_to_step(
                (actual_spot - self.spot_baseline_qty).max(0.0),
                self.spot_step,
            );
            if delta * price >= 10.0 && delta > 0.0 {
                let fill = spot
                    .market_order(&self.symbol, "SELL", delta, self.spot_step)
                    .await?;
                self.carry.fee(fill.fee_usdt);
                info!(qty = fill.qty, "组合停止：carry 现货腿已恢复基线");
            }
        }
        self.ema.target(0.0, price, now_ms);
        self.carry.target(0.0, price, now_ms);
        self.probe.target(0.0, price, now_ms);
        self.probe_open = false;
        self.actual_fees_and_funding().await;
        self.last_actual_qty = 0.0;
        self.persist_state();
        Ok(self.snapshots())
    }

    pub fn snapshots(&self) -> Vec<JournalSleeve> {
        let mut rows = Vec::new();
        if self.cfg.ema_enabled {
            rows.push(self.ema.snapshot("币安实际净仓成交 + sleeve 目标仓归因"));
        }
        if self.cfg.carry_enabled {
            rows.push(
                self.carry
                    .snapshot("Binance Spot + Futures + Income History"),
            );
        }
        if self.cfg.probe_enabled {
            rows.push(self.probe.snapshot("币安模拟盘（Demo）实际往返成交"));
        }
        rows.push(self.mr_mirror.snapshot("组合净仓真实成交（MR 镜像归因）"));
        rows
    }
}
