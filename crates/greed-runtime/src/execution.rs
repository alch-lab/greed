use crate::{
    config::{ExecutionConfig, PortfolioConfig},
    market_stream::MarketStreamHub,
};
use anyhow::{anyhow, Context, Result};
use greed_kernel::{
    AccountFrame, Artifact, ArtifactMeta, ArtifactRecord, Candle, DataQuality, GraphEvaluation,
    MarketFrame, Side, StateArtifact, TradeCandidate, Verdict,
};
use greed_strategy::{LaneConfig, RiskConfig};
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::Path,
    time::Duration,
};

type HmacSha256 = Hmac<Sha256>;

const MAKER_REPRICE_INTERVAL_MS: i64 = 15_000;
const MAX_MAKER_REPRICES: u8 = 3;
const ENTRY_GUARD_STALE_GRACE_MS: i64 = 5_000;
const ENTRY_GUARD_REVERSAL_CONFIRM_MS: i64 = 15_000;
const MIN_PERFORMANCE_FILL_RATIO: f64 = 0.20;
const PERFORMANCE_BASIS_VERSION: u32 = 3;
const FAST_TREND_ACTIVATION_RECIPE: &str = "fast_trend_activation";
const TREND_REENTRY_RECIPE: &str = "trend_continuation_reentry";
const TREND_PROFIT_REVERSAL_RECIPE: &str = "trend_profit_reversal";
const MAX_PROFIT_REVERSALS_PER_CHAIN: u8 = 2;
const LIQUIDATION_REVERSAL_RECIPE: &str = "liquidation_exhaustion_reversal";
const LIQUIDATION_EXECUTION_BASIS_VERSION: u32 = 1;

fn recipe_slot_full(
    recipe: &str,
    fast_active: usize,
    standard_active: usize,
    max_standard: usize,
) -> bool {
    if recipe == FAST_TREND_ACTIVATION_RECIPE {
        fast_active >= 1
    } else {
        standard_active >= max_standard
    }
}

pub struct ExchangeEvent {
    pub kind: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecipeGateStatus {
    pub allowed: bool,
    pub completed_trades: usize,
    pub excluded_partial_trades: usize,
    pub rolling_profit_factor: Option<f64>,
    pub rolling_net_pnl_usd: f64,
    pub next_probe_ms: Option<i64>,
    pub entry_attempts: u64,
    pub filled_entry_attempts: u64,
    pub formal_positions: u64,
    pub closed_trades: u64,
}

#[derive(Debug, Clone)]
struct SymbolRules {
    quantity_step: f64,
    min_quantity: f64,
    max_limit_quantity: f64,
    max_market_quantity: f64,
    price_tick: f64,
    min_notional: f64,
}

#[derive(Debug, Clone, Copy)]
enum ProtectiveOrderId {
    Standard(i64),
    Algo(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionMeta {
    candidate_id: String,
    recipe: String,
    side: Side,
    /// Legacy persisted holding clock. New entries set this to first_fill_ms;
    /// old state files retain their original value without pretending that it
    /// was an exchange-confirmed fill timestamp.
    entry_ms: i64,
    #[serde(default)]
    signal_ms: i64,
    #[serde(default)]
    order_submitted_ms: i64,
    #[serde(default)]
    first_fill_ms: Option<i64>,
    #[serde(default)]
    entry_completed_ms: Option<i64>,
    #[serde(default)]
    entry_time_source: Option<String>,
    #[serde(default)]
    entry_price: f64,
    #[serde(default)]
    initial_quantity: f64,
    #[serde(default)]
    last_observed_quantity: f64,
    #[serde(default)]
    cumulative_reported_fee_usd: f64,
    #[serde(default)]
    cumulative_reported_pnl_usd: f64,
    /// Planned/actual exposure is persisted so a tiny maker partial does not
    /// count as a full statistical sample in the rolling performance gate.
    #[serde(default)]
    planned_notional_usd: Option<f64>,
    #[serde(default)]
    fill_ratio: Option<f64>,
    #[serde(default)]
    initial_risk_usd: Option<f64>,
    stop_price: f64,
    #[serde(default)]
    take_profit_price: f64,
    #[serde(default)]
    take_profit_prices: Vec<(f64, f64)>,
    #[serde(default)]
    unprotected_runner_fraction: Option<f64>,
    #[serde(default)]
    runner_active: bool,
    #[serde(default)]
    executable_profit: Option<crate::profit_guard::ProfitGuard>,
    #[serde(default)]
    break_even_after_fraction: Option<f64>,
    #[serde(default)]
    break_even_buffer_pct: f64,
    #[serde(default)]
    profit_shield_activation_pct: Option<f64>,
    #[serde(default)]
    break_even_armed: bool,
    #[serde(default)]
    extreme_price: f64,
    #[serde(default)]
    adverse_price: f64,
    #[serde(default)]
    trailing_activation_pct: Option<f64>,
    #[serde(default)]
    trailing_distance_pct: Option<f64>,
    #[serde(default)]
    early_failure_after_ms: i64,
    #[serde(default)]
    early_failure_adverse_pct: f64,
    #[serde(default)]
    early_failure_max_favorable_pct: f64,
    max_hold_ms: i64,
    #[serde(default)]
    fixed_time_exit: bool,
    /// Number of causal deadline reviews already granted. Older state files
    /// deserialize to zero, so upgrading cannot silently invent a grace
    /// period that was already consumed.
    #[serde(default)]
    max_hold_reviews: u8,
    #[serde(default)]
    exit_requested: bool,
    #[serde(default)]
    pending_exit_reason: Option<String>,
    #[serde(default)]
    pending_exit_actor: Option<String>,
    #[serde(default)]
    pending_exit_note: Option<String>,
    #[serde(default)]
    stop_algo_id: Option<i64>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    take_profit_order_ids: Vec<i64>,
    /// Number of immediate direction changes already executed in the current
    /// position chain. A fresh strategy entry starts at zero; the counter is
    /// persisted so a restart cannot bypass the two-reversal ceiling.
    #[serde(default)]
    profit_reversal_count: u8,
}

impl ExecutionMeta {
    fn holding_started_ms(&self) -> i64 {
        self.first_fill_ms.unwrap_or(self.entry_ms)
    }

    fn holding_time_source(&self) -> &str {
        self.entry_time_source
            .as_deref()
            .unwrap_or("legacy_entry_ms_unknown_semantics")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrendReentrySignal {
    signal_ms: i64,
    reference_price: f64,
    stop_pct: f64,
    body_pct: f64,
    directional_flow: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrendReentryCampaign {
    source_candidate_id: String,
    symbol: String,
    side: Side,
    armed_ms: i64,
    expires_ms: i64,
    favorable_extreme: f64,
    first_exit_price: f64,
    #[serde(default)]
    reset_ms: Option<i64>,
    #[serde(default)]
    last_evaluated_bar_ms: i64,
    #[serde(default)]
    signal: Option<TrendReentrySignal>,
    /// Immediate reversals are armed by a confirmed executable-profit exit and
    /// do not wait for the ordinary second-leg reset/confirmation sequence.
    #[serde(default)]
    immediate_profit_reversal: bool,
    #[serde(default)]
    profit_reversal_count: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingEntryState {
    plan: greed_kernel::PositionPlan,
    recipe: String,
    signal_ms: i64,
    #[serde(default)]
    discovery_latency_ms: Option<i64>,
    size_multiplier: f64,
    requested_quantity: f64,
    active_client_id: String,
    passive_price: f64,
    order_submitted_ms: i64,
    deadline_ms: i64,
    next_reprice_ms: i64,
    #[serde(default)]
    reprice_attempt: u8,
    #[serde(default)]
    first_fill_ms: Option<i64>,
    #[serde(default)]
    first_fill_time_source: Option<String>,
    #[serde(default)]
    preliminary_stop_algo_id: Option<i64>,
    #[serde(default)]
    preliminary_stop_price: Option<f64>,
    #[serde(default)]
    guard_unavailable_since_ms: Option<i64>,
    #[serde(default)]
    micro_reversal_since_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct RecipeExecutionCounts {
    entry_attempts: u64,
    filled_entry_attempts: u64,
    formal_positions: u64,
    closed_trades: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingAccountingState {
    candidate_id: String,
    recipe: String,
    symbol: String,
    side: Side,
    start_ms: i64,
    requested_quantity: f64,
    expected_exit_quantity: f64,
    reference_price: f64,
    planned_stop_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct DemoState {
    baseline_wallet_usd: Option<f64>,
    peak_equity_usd: Option<f64>,
    risk_day_start_equity_usd: Option<f64>,
    risk_day: String,
    seen: BTreeSet<String>,
    positions: BTreeMap<String, ExecutionMeta>,
    pending_entries: BTreeMap<String, PendingEntryState>,
    recipe_outcomes: BTreeMap<String, Vec<ExecutionOutcome>>,
    symbol_outcomes: BTreeMap<String, Vec<ExecutionOutcome>>,
    trend_reentries: BTreeMap<String, TrendReentryCampaign>,
    performance_epoch: u32,
    performance_basis_version: u32,
    /// Version the adaptive gate independently when a lane's execution
    /// contract changes. Old outcomes remain available for history while the
    /// corrected lane starts with an uncontaminated performance window.
    liquidation_execution_basis_version: u32,
    execution_halt_reason: Option<String>,
    entry_attempt_ids: BTreeSet<String>,
    filled_entry_attempt_ids: BTreeSet<String>,
    accounted_attempt_ids: BTreeSet<String>,
    formal_position_ids: BTreeSet<String>,
    closed_trade_ids: BTreeSet<String>,
    recipe_execution_counts: BTreeMap<String, RecipeExecutionCounts>,
    pending_accounting: BTreeMap<String, PendingAccountingState>,
}

fn reset_portfolio_risk_baselines(
    state: &mut DemoState,
    equity: f64,
    risk_day: String,
) -> (Option<f64>, Option<f64>) {
    let previous = (state.risk_day_start_equity_usd, state.peak_equity_usd);
    state.risk_day = risk_day;
    state.risk_day_start_equity_usd = Some(equity);
    state.peak_equity_usd = Some(equity);
    previous
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ExecutionOutcome {
    #[serde(default)]
    candidate_id: String,
    exit_ms: i64,
    pnl_usd: f64,
    #[serde(default)]
    pnl_r: Option<f64>,
    #[serde(default)]
    fill_ratio: Option<f64>,
    #[serde(default)]
    initial_risk_usd: Option<f64>,
    #[serde(default)]
    sample_kind: String,
}

impl ExecutionOutcome {
    fn performance_eligible(&self) -> bool {
        self.fill_ratio
            .is_none_or(|value| value >= MIN_PERFORMANCE_FILL_RATIO)
    }

    fn performance_value(&self) -> f64 {
        self.pnl_r.unwrap_or(self.pnl_usd)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DemoPositionSnapshot {
    pub candidate_id: String,
    pub recipe: String,
    pub symbol: String,
    pub side: Side,
    pub entry_ms: i64,
    pub signal_ms: i64,
    pub order_submitted_ms: i64,
    pub first_fill_ms: Option<i64>,
    pub entry_completed_ms: Option<i64>,
    pub entry_time_source: String,
    pub entry_price: f64,
    pub quantity: f64,
    pub initial_quantity: f64,
    pub remaining_quantity: f64,
    pub stop_price: f64,
    pub take_profit_prices: Vec<(f64, f64)>,
    pub unprotected_runner_fraction: Option<f64>,
    pub runner_active: bool,
    pub isolated: bool,
    pub isolated_wallet_usd: f64,
    pub max_hold_ms: i64,
    pub fixed_time_exit: bool,
    pub break_even_armed: bool,
    pub profit_shield_activation_pct: Option<f64>,
    pub executable_profit: Option<crate::profit_guard::ProfitGuard>,
    pub executable_profit_guard_enabled: bool,
    pub profit_reversal_count: u8,
    pub max_profit_reversals: u8,
    pub extreme_price: Option<f64>,
    pub trailing_activation_pct: Option<f64>,
    pub trailing_distance_pct: Option<f64>,
    pub early_failure_after_ms: i64,
    pub early_failure_adverse_pct: f64,
    pub early_failure_max_favorable_pct: f64,
    pub current_price: Option<f64>,
    pub current_notional_usd: Option<f64>,
    pub unrealized_pnl_usd: Option<f64>,
    pub unrealized_pnl_pct: Option<f64>,
}

#[derive(Debug, Clone)]
struct RemotePosition {
    symbol: String,
    side: Side,
    quantity: f64,
    entry_price: f64,
    mark_price: f64,
    unrealized_pnl: f64,
    isolated: bool,
    isolated_wallet_usd: f64,
}

#[derive(Debug, Clone)]
struct RemoteAccount {
    wallet_balance: f64,
    margin_balance: f64,
    available_balance: f64,
    positions: BTreeMap<String, RemotePosition>,
}

#[derive(Debug)]
struct EntryExecution {
    order: Value,
    requested_quantity: f64,
    mode: &'static str,
    maker_attempted: bool,
    maker_wait_ms: i64,
    maker_reprices: u8,
    final_maker_limit: Option<f64>,
    size_multiplier: f64,
}

#[derive(Debug, Clone, Default)]
struct TradeSummary {
    entry_price: f64,
    entry_quantity: f64,
    exit_price: f64,
    exit_quantity: f64,
    last_exit_price: f64,
    last_exit_quantity: f64,
    last_exit_order_id: Option<i64>,
    fees_usd: f64,
    net_pnl_usd: f64,
    maker_fills: usize,
    taker_fills: usize,
    maker_notional_usd: f64,
    taker_notional_usd: f64,
}

#[derive(Debug)]
struct BracketExecution {
    entry_price: f64,
    quantity: f64,
    order_id: i64,
    stop_price: f64,
    take_profit_prices: Vec<(f64, f64)>,
    stop_algo_id: i64,
    take_profit_order_ids: Vec<i64>,
    entry_mode: &'static str,
    entry_price_source: &'static str,
    maker_attempted: bool,
    maker_wait_ms: i64,
    maker_reprices: u8,
    final_maker_limit: Option<f64>,
    size_multiplier: f64,
    order_submitted_ms: i64,
    first_fill_ms: i64,
    entry_completed_ms: i64,
    entry_time_source: &'static str,
}

#[derive(Debug, Clone, PartialEq)]
enum EntryGuardState {
    Healthy,
    Unavailable(String),
    StructuralInvalidation(String),
    MicroReversal(String),
}

pub struct BinanceDemoExecution {
    client: Client,
    config: ExecutionConfig,
    portfolio: PortfolioConfig,
    risk: RiskConfig,
    lanes: LaneConfig,
    api_key: String,
    api_secret: String,
    clock_offset_ms: i64,
    rules: BTreeMap<String, SymbolRules>,
    state_path: String,
    state: DemoState,
    account: Option<RemoteAccount>,
    last_error: Option<String>,
    last_sync_ms: Option<i64>,
    performance_epoch_reset: bool,
    performance_basis_reset: bool,
    /// Mainnet websocket data is the strategy's source of truth. Demo order
    /// books can diverge for thin contracts, so pending entries must continue
    /// to be checked against this shared cache until they fill or are canceled.
    market_stream: Option<MarketStreamHub>,
}

impl BinanceDemoExecution {
    pub async fn connect(
        config: ExecutionConfig,
        portfolio: PortfolioConfig,
        risk: RiskConfig,
        lanes: LaneConfig,
        state_path: String,
        proxy: Option<&str>,
        market_stream: Option<MarketStreamHub>,
    ) -> Result<Self> {
        let api_key = env::var(&config.api_key_env)
            .with_context(|| format!("missing environment variable {}", config.api_key_env))?;
        let api_secret = env::var(&config.api_secret_env)
            .with_context(|| format!("missing environment variable {}", config.api_secret_env))?;
        let mut builder = Client::builder().timeout(Duration::from_secs(10));
        if let Some(proxy) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy)?);
        }
        let client = builder.build()?;
        let state: DemoState = fs::read_to_string(&state_path)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        let performance_epoch_reset = state.performance_epoch != risk.rolling_pf_epoch;
        let performance_basis_reset = state.performance_basis_version != PERFORMANCE_BASIS_VERSION;
        let liquidation_execution_basis_reset =
            state.liquidation_execution_basis_version != LIQUIDATION_EXECUTION_BASIS_VERSION;
        let mut value = Self {
            client,
            config,
            portfolio,
            risk,
            lanes,
            api_key,
            api_secret,
            clock_offset_ms: 0,
            rules: BTreeMap::new(),
            state_path,
            state,
            account: None,
            last_error: None,
            last_sync_ms: None,
            performance_epoch_reset,
            performance_basis_reset,
            market_stream,
        };
        value.initialize().await?;
        if value.performance_epoch_reset {
            let account_is_flat = value
                .account
                .as_ref()
                .is_some_and(|account| account.positions.is_empty());
            if !account_is_flat
                || !value.state.positions.is_empty()
                || !value.state.pending_entries.is_empty()
                || !value.state.pending_accounting.is_empty()
            {
                return Err(anyhow!(
                    "cannot start a new performance epoch while Binance demo positions are open"
                ));
            }
            // Commit the new epoch only after reconciliation has proved the
            // account and every persisted lifecycle are flat. Previously sync
            // could persist the epoch number before this check and leave a
            // half-reset state if startup then failed.
            value.state.recipe_outcomes.clear();
            value.state.symbol_outcomes.clear();
            value.state.trend_reentries.clear();
            value.state.accounted_attempt_ids.clear();
            value.state.entry_attempt_ids.clear();
            value.state.filled_entry_attempt_ids.clear();
            value.state.formal_position_ids.clear();
            value.state.closed_trade_ids.clear();
            value.state.recipe_execution_counts.clear();
            value.state.performance_epoch = value.risk.rolling_pf_epoch;
            value.state.performance_basis_version = PERFORMANCE_BASIS_VERSION;
            value.state.liquidation_execution_basis_version = LIQUIDATION_EXECUTION_BASIS_VERSION;
            value.state.execution_halt_reason = None;
            let wallet = value
                .account
                .as_ref()
                .map(|account| account.wallet_balance)
                .ok_or_else(|| anyhow!("demo account not synchronized"))?;
            value.state.baseline_wallet_usd = Some(wallet);
            value.state.peak_equity_usd = Some(value.portfolio.initial_equity_usd);
            value.state.risk_day = chrono::Utc::now().format("%Y-%m-%d").to_string();
            value.state.risk_day_start_equity_usd = Some(value.portfolio.initial_equity_usd);
            value.state.seen.clear();
            value.save()?;
        } else if value.performance_basis_reset {
            // Dollar PF and risk-normalized R PF cannot share one rolling
            // window. Reset only the internal recipe sample window; account
            // baseline, run history, PnL curve, symbol cooldowns and exits stay.
            value.state.recipe_outcomes.clear();
            value.state.performance_basis_version = PERFORMANCE_BASIS_VERSION;
            value.save()?;
        }
        if !value.performance_epoch_reset && liquidation_execution_basis_reset {
            if value
                .state
                .execution_halt_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("could not establish protection"))
            {
                // The bounded IOC migration fixes the diagnosed venue mismatch.
                // Resume only that halt; unrelated operator/risk stops survive.
                value.state.execution_halt_reason = None;
            }
            value.state.liquidation_execution_basis_version = LIQUIDATION_EXECUTION_BASIS_VERSION;
            value.save()?;
        }
        Ok(value)
    }

    async fn initialize(&mut self) -> Result<()> {
        let server_time = self.public_get("/fapi/v1/time").await?["serverTime"]
            .as_i64()
            .ok_or_else(|| anyhow!("Binance demo time response is missing serverTime"))?;
        self.clock_offset_ms = server_time - chrono::Utc::now().timestamp_millis();
        let mode = self
            .signed(Method::GET, "/fapi/v1/positionSide/dual", vec![])
            .await?;
        if mode["dualSidePosition"].as_bool().unwrap_or(false) {
            return Err(anyhow!(
                "Binance demo account is in Hedge Mode; switch it to One-way Mode before starting greed"
            ));
        }
        let info = self.public_get("/fapi/v1/exchangeInfo").await?;
        self.rules = parse_rules(&info)?;
        self.sync().await?;
        Ok(())
    }

    async fn public_get(&self, path: &str) -> Result<Value> {
        self.public_get_params(path, &[]).await
    }

    async fn public_get_params(&self, path: &str, parameters: &[(&str, &str)]) -> Result<Value> {
        let response = self
            .client
            .get(format!(
                "{}{path}",
                self.config.base_url.trim_end_matches('/')
            ))
            .query(parameters)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow!("Binance demo {path} returned {status}: {body}"));
        }
        serde_json::from_str(&body)
            .with_context(|| format!("invalid Binance demo response: {path}"))
    }

    async fn signed(
        &self,
        method: Method,
        path: &str,
        mut parameters: Vec<(String, String)>,
    ) -> Result<Value> {
        parameters.push(("recvWindow".into(), self.config.recv_window_ms.to_string()));
        parameters.push((
            "timestamp".into(),
            (chrono::Utc::now().timestamp_millis() + self.clock_offset_ms).to_string(),
        ));
        let query: String = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(
                parameters
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            )
            .finish();
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC accepts arbitrary key lengths");
        mac.update(query.as_bytes());
        let signature = hex_bytes(&mac.finalize().into_bytes());
        let url = format!(
            "{}{path}?{query}&signature={signature}",
            self.config.base_url.trim_end_matches('/')
        );
        let response = self
            .client
            .request(method, url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow!("Binance demo {path} returned {status}: {body}"));
        }
        serde_json::from_str(&body)
            .with_context(|| format!("invalid Binance demo response: {path}"))
    }

    /// Retry read-only account queries inside the same reconciliation cycle.
    ///
    /// Binance Demo occasionally drops a request while the public streams are
    /// still healthy.  Waiting for the next five-second sync unnecessarily
    /// makes the local protection view stale.  Only GET requests use this
    /// helper: order mutations retain their deterministic client-id based
    /// reconciliation and are never blindly retried.
    async fn signed_read(&self, path: &str, parameters: Vec<(String, String)>) -> Result<Value> {
        let mut last_error = None;
        for delay_ms in [0, 250, 750] {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            match self.signed(Method::GET, path, parameters.clone()).await {
                Ok(value) => return Ok(value),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("read-only Binance request failed")))
            .with_context(|| format!("Binance demo read failed after three attempts: {path}"))
    }

    pub async fn sync(&mut self) -> Result<Vec<ExchangeEvent>> {
        let value = self.signed_read("/fapi/v2/account", vec![]).await;
        match value {
            Ok(value) => {
                let mut account = parse_account(&value)?;
                let now_ms = chrono::Utc::now().timestamp_millis();
                let (mut events, pending_changed) = match self
                    .progress_pending_entries(now_ms, &account.positions)
                    .await
                {
                    Ok(events) => {
                        let changed = !events.is_empty();
                        (events, changed)
                    }
                    Err(error) => (
                        vec![ExchangeEvent {
                            kind: "exchange_pending_entry_error".into(),
                            payload: serde_json::json!({
                                "ts_ms":now_ms,
                                // Preserve the full anyhow chain. `to_string()` only
                                // exposes the outer context and previously hid the
                                // actual Binance rejection (for example an invalid
                                // clientAlgoId), making a protection failure impossible
                                // to diagnose from a production bundle.
                                "reason":format!("{error:#}"),
                                "pending_entries":self.state.pending_entries.len(),
                                "existing_position_management_continued":true,
                                "venue":"binance_demo",
                                "paper_only":true
                            }),
                        }],
                        false,
                    ),
                };
                if pending_changed {
                    let refreshed = self.signed_read("/fapi/v2/account", vec![]).await?;
                    account = parse_account(&refreshed)?;
                }
                events.extend(self.progress_pending_accounting(now_ms).await);
                let foreign: Vec<_> = account
                    .positions
                    .keys()
                    .filter(|symbol| {
                        !self.state.positions.contains_key(*symbol)
                            && !self.state.pending_entries.contains_key(*symbol)
                    })
                    .cloned()
                    .collect();
                if !foreign.is_empty() {
                    return Err(anyhow!(
                        "Binance demo contains positions not owned by this runtime: {}; flatten them before starting",
                        foreign.join(",")
                    ));
                }
                if self.state.baseline_wallet_usd.is_none() {
                    self.state.baseline_wallet_usd = Some(account.wallet_balance);
                }
                // A Fast deadline may be released only after executable-value
                // protection is genuinely armed. A mark-price excursion alone
                // is not bankable and must never extend an unprotected trade.
                let due_for_review: Vec<_> = self
                    .state
                    .positions
                    .iter()
                    .filter(|(symbol, meta)| {
                        !meta.exit_requested
                            && meta.max_hold_ms > 0
                            && now_ms - meta.holding_started_ms() >= meta.max_hold_ms
                            && account.positions.contains_key(*symbol)
                    })
                    .map(|(symbol, _)| symbol.clone())
                    .collect();
                let mut hold_review_changed = false;
                for symbol in due_for_review {
                    let Some(meta) = self.state.positions.get_mut(&symbol) else {
                        continue;
                    };
                    let mark_price = account
                        .positions
                        .get(&symbol)
                        .map(|position| position.mark_price)
                        .unwrap_or_default();
                    match max_hold_review(meta, now_ms, mark_price) {
                        MaxHoldReview::NotApplicable => {}
                        MaxHoldReview::Exit => {
                            // Persist the decision before touching the exchange.
                            // A failed close must be retried, never reconsidered
                            // later as a newly eligible hold extension.
                            if meta.pending_exit_reason.as_deref() != Some("max_hold") {
                                meta.pending_exit_reason = Some("max_hold".into());
                                hold_review_changed = true;
                            }
                        }
                        MaxHoldReview::ReleaseToProtection => {
                            meta.max_hold_ms = 0;
                            hold_review_changed = true;
                            events.push(ExchangeEvent {
                                kind: "exchange_max_hold_released".into(),
                                payload: serde_json::json!({
                                    "ts_ms":now_ms,
                                    "symbol":symbol,
                                    "recipe":meta.recipe,
                                    "reason":"exchange_profit_protection_armed",
                                    "venue":"binance_demo",
                                    "paper_only":true
                                }),
                            });
                        }
                        MaxHoldReview::Extend {
                            extension_ms,
                            progress_r,
                            current_r,
                            executable_net_pnl_usd,
                            initial_risk_usd,
                            exit_impact_bps,
                            reason,
                        } => {
                            meta.max_hold_ms = meta.max_hold_ms.saturating_add(extension_ms);
                            meta.max_hold_reviews = meta.max_hold_reviews.saturating_add(1);
                            hold_review_changed = true;
                            events.push(ExchangeEvent {
                                kind: "exchange_max_hold_extended".into(),
                                payload: serde_json::json!({
                                    "ts_ms":now_ms,"symbol":symbol,"recipe":meta.recipe,
                                    "extension_ms":extension_ms,
                                    "progress_r":progress_r,
                                    "current_r":current_r,
                                    "executable_net_pnl_usd":executable_net_pnl_usd,
                                    "initial_risk_usd":initial_risk_usd,
                                    "exit_impact_bps":exit_impact_bps,
                                    "review_count":meta.max_hold_reviews,
                                    "reason":reason,
                                    "venue":"binance_demo","paper_only":true
                                }),
                            });
                        }
                    }
                }
                if hold_review_changed {
                    self.save()?;
                }
                let expired: Vec<_> = self
                    .state
                    .positions
                    .iter()
                    .filter(|(symbol, meta)| {
                        !meta.exit_requested
                            && meta.max_hold_ms > 0
                            && now_ms - meta.holding_started_ms() >= meta.max_hold_ms
                            && account.positions.contains_key(*symbol)
                    })
                    .map(|(symbol, _)| symbol.clone())
                    .collect();
                let mut protected: BTreeMap<String, (bool, bool)> = BTreeMap::new();
                let mut stop_orders: BTreeMap<String, ProtectiveOrderId> = BTreeMap::new();
                for symbol in account.positions.keys() {
                    let open_orders = self
                        .signed_read(
                            "/fapi/v1/openOrders",
                            vec![("symbol".into(), symbol.clone())],
                        )
                        .await?;
                    for order in open_orders
                        .as_array()
                        .ok_or_else(|| anyhow!("openOrders response is not an array"))?
                    {
                        let entry = protected.entry(symbol.clone()).or_insert((false, false));
                        match order["type"].as_str().unwrap_or_default() {
                            "STOP_MARKET" => {
                                entry.0 = true;
                                if let Some(order_id) = order["orderId"].as_i64() {
                                    stop_orders.insert(
                                        symbol.clone(),
                                        ProtectiveOrderId::Standard(order_id),
                                    );
                                }
                            }
                            "TAKE_PROFIT_MARKET" => entry.1 = true,
                            "LIMIT" if order["reduceOnly"].as_bool().unwrap_or(false) => {
                                entry.1 = true
                            }
                            _ => {}
                        }
                    }
                    let open_algo_orders = self
                        .signed_read(
                            "/fapi/v1/openAlgoOrders",
                            vec![
                                ("algoType".into(), "CONDITIONAL".into()),
                                ("symbol".into(), symbol.clone()),
                            ],
                        )
                        .await?;
                    for order in open_algo_orders
                        .as_array()
                        .ok_or_else(|| anyhow!("openAlgoOrders response is not an array"))?
                    {
                        let entry = protected.entry(symbol.clone()).or_insert((false, false));
                        match order["orderType"].as_str().unwrap_or_default() {
                            "STOP_MARKET" => {
                                entry.0 = true;
                                if let Some(algo_id) = order["algoId"].as_i64() {
                                    stop_orders
                                        .insert(symbol.clone(), ProtectiveOrderId::Algo(algo_id));
                                }
                            }
                            "TAKE_PROFIT_MARKET" => entry.1 = true,
                            _ => {}
                        }
                    }
                }
                let mut protection_updates = Vec::new();
                let partial_exits: Vec<_> = account
                    .positions
                    .iter()
                    .filter_map(|(symbol, position)| {
                        let meta = self.state.positions.get(symbol)?;
                        let previous = if meta.last_observed_quantity > 0.0 {
                            meta.last_observed_quantity
                        } else {
                            meta.initial_quantity
                        };
                        (previous > position.quantity.abs() + f64::EPSILON).then_some((
                            symbol.clone(),
                            previous - position.quantity.abs(),
                            position.quantity.abs(),
                            meta.holding_started_ms(),
                            meta.candidate_id.clone(),
                            meta.recipe.clone(),
                            meta.side,
                        ))
                    })
                    .collect();
                let mut runner_activations = Vec::new();
                for (
                    symbol,
                    closed_quantity,
                    remaining_quantity,
                    entry_ms,
                    candidate_id,
                    recipe,
                    side,
                ) in partial_exits
                {
                    let summary = self.trade_summary(&symbol, entry_ms, side).await.ok();
                    let (fee_delta, pnl_delta) = if let Some(summary) = summary.as_ref() {
                        let meta = self.state.positions.get(&symbol);
                        (
                            summary.fees_usd
                                - meta
                                    .map(|value| value.cumulative_reported_fee_usd)
                                    .unwrap_or(0.0),
                            summary.net_pnl_usd
                                - meta
                                    .map(|value| value.cumulative_reported_pnl_usd)
                                    .unwrap_or(0.0),
                        )
                    } else {
                        (0.0, 0.0)
                    };
                    events.push(ExchangeEvent {
                        kind: "exchange_partial_exit".into(),
                        payload: serde_json::json!({
                            "ts_ms":now_ms,
                            "candidate_id":candidate_id,
                            "recipe":recipe,
                            "symbol":symbol,
                            "side":side,
                            "reason":"staged_take_profit",
                            "exit_price":summary.as_ref().map(|value| value.last_exit_price),
                            "trade_entry_price":summary.as_ref().map(|value| value.entry_price),
                            "trade_entry_quantity":summary.as_ref().map(|value| value.entry_quantity),
                            "closed_quantity":closed_quantity,
                            "remaining_quantity":remaining_quantity,
                            "fee_usd":fee_delta,
                            "pnl_usd":pnl_delta,
                            "cumulative_fee_usd":summary.as_ref().map(|value| value.fees_usd),
                            "cumulative_net_pnl_usd":summary.as_ref().map(|value| value.net_pnl_usd),
                            "maker_fills":summary.as_ref().map(|value| value.maker_fills),
                            "taker_fills":summary.as_ref().map(|value| value.taker_fills),
                            "maker_notional_usd":summary.as_ref().map(|value| value.maker_notional_usd),
                            "taker_notional_usd":summary.as_ref().map(|value| value.taker_notional_usd),
                            "venue":"binance_demo"
                        }),
                    });
                    if let Some(meta) = self.state.positions.get_mut(&symbol) {
                        meta.last_observed_quantity = remaining_quantity;
                        if let Some(summary) = summary {
                            meta.cumulative_reported_fee_usd = summary.fees_usd;
                            meta.cumulative_reported_pnl_usd = summary.net_pnl_usd;
                        }
                        if !meta.runner_active {
                            if let Some(runner_fraction) = meta.unprotected_runner_fraction {
                                let runner_quantity = meta.initial_quantity * runner_fraction;
                                let remote_position = account.positions.get(&symbol);
                                let isolated_wallet = remote_position
                                    .map(|value| value.isolated_wallet_usd)
                                    .unwrap_or_default();
                                // trade_summary.net_pnl_usd is already net of
                                // commission. Requiring fees a second time made
                                // a genuinely funded tail unnecessarily hard to
                                // activate. Never remove protection unless the
                                // exchange confirms isolated margin and exposes
                                // a positive, fully covered isolated wallet.
                                let profits_cover_runner =
                                    remote_position.is_some_and(|position| {
                                        position.isolated
                                            && isolated_wallet > f64::EPSILON
                                            && meta.cumulative_reported_pnl_usd > isolated_wallet
                                    });
                                if remaining_quantity <= runner_quantity * 1.01
                                    && remaining_quantity > f64::EPSILON
                                    && profits_cover_runner
                                {
                                    runner_activations.push((
                                        symbol.clone(),
                                        remaining_quantity,
                                        isolated_wallet,
                                        meta.cumulative_reported_pnl_usd,
                                    ));
                                }
                            }
                        }
                    }
                }
                for (symbol, remaining_quantity, isolated_wallet, locked_pnl) in runner_activations
                {
                    match self.cancel_all(&symbol).await {
                        Ok(()) => {
                            if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                meta.runner_active = true;
                                meta.stop_price = 0.0;
                                meta.stop_algo_id = None;
                                meta.stop_reason = Some("unprotected_tail_runner".into());
                                meta.take_profit_order_ids.clear();
                                meta.profit_shield_activation_pct = None;
                                meta.trailing_activation_pct = None;
                                meta.trailing_distance_pct = None;
                                meta.early_failure_after_ms = 0;
                                meta.max_hold_ms = 0;
                            }
                            events.push(ExchangeEvent {
                                kind: "exchange_runner_activated".into(),
                                payload: serde_json::json!({
                                    "ts_ms":now_ms,
                                    "symbol":symbol,
                                    "remaining_quantity":remaining_quantity,
                                    "isolated_wallet_usd":isolated_wallet,
                                    "locked_net_pnl_usd":locked_pnl,
                                    "worst_case_campaign_pnl_usd":locked_pnl-isolated_wallet,
                                    "reason":"preceding_realized_profit_covers_isolated_runner",
                                    "venue":"binance_demo",
                                    "paper_only":true
                                }),
                            });
                        }
                        Err(error) => events.push(ExchangeEvent {
                            kind: "exchange_order_rejected".into(),
                            payload: serde_json::json!({
                                "ts_ms":now_ms,
                                "symbol":symbol,
                                "reason":format!("tail runner activation could not cancel protection: {error}"),
                                "venue":"binance_demo"
                            }),
                        }),
                    }
                }
                // Concurrent, bounded READS only. Account mutation/exit submission
                // remains serialized in this owner; one slow symbol cannot add
                // a full REST timeout per position to the management loop.
                let mut quote_tasks = tokio::task::JoinSet::new();
                if self.config.executable_profit_guard {
                    for (symbol, position) in &account.positions {
                        let Some(meta) = self.state.positions.get(symbol) else {
                            continue;
                        };
                        if meta.exit_requested
                            || meta.runner_active
                            || meta.fixed_time_exit
                            || meta.profit_shield_activation_pct.is_none()
                        {
                            continue;
                        }
                        let client = self.client.clone();
                        let url = format!(
                            "{}/fapi/v1/depth",
                            self.config.base_url.trim_end_matches('/')
                        );
                        let symbol = symbol.clone();
                        let side = position.side;
                        let quantity = position.quantity.abs();
                        let offset = self.clock_offset_ms;
                        quote_tasks.spawn(async move {
                            let read = async {
                                let value: Value = client
                                    .get(url)
                                    .query(&[("symbol", symbol.as_str()), ("limit", "100")])
                                    .send()
                                    .await?
                                    .error_for_status()?
                                    .json()
                                    .await?;
                                crate::profit_guard::quote(
                                    &value,
                                    side,
                                    quantity,
                                    chrono::Utc::now().timestamp_millis(),
                                    offset,
                                )
                            };
                            let result = match tokio::time::timeout(
                                Duration::from_millis(750),
                                read,
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(anyhow!("execution depth request exceeded 750 ms")),
                            };
                            (symbol, result)
                        });
                    }
                }
                let mut executable_quotes = BTreeMap::new();
                while let Some(result) = quote_tasks.join_next().await {
                    if let Ok((symbol, result)) = result {
                        match result {
                            Ok(quote) => { executable_quotes.insert(symbol,quote); }
                            Err(error) => events.push(ExchangeEvent {
                                kind:"executable_profit_unscorable".into(),
                                payload:serde_json::json!({"ts_ms":now_ms,"symbol":symbol,"reason":error.to_string(),"hard_stop_retained":true}),
                            }),
                        }
                    }
                }
                let mut executable_exits = Vec::new();
                let mut early_failures = Vec::new();
                for (symbol, position) in &account.positions {
                    let Some(meta) = self.state.positions.get_mut(symbol) else {
                        continue;
                    };
                    if meta.runner_active || meta.exit_requested {
                        continue;
                    }
                    meta.extreme_price = if meta.extreme_price <= 0.0 {
                        position.mark_price
                    } else {
                        match meta.side {
                            Side::Buy => meta.extreme_price.max(position.mark_price),
                            Side::Sell => meta.extreme_price.min(position.mark_price),
                        }
                    };
                    meta.adverse_price = if meta.adverse_price <= 0.0 {
                        position.mark_price
                    } else {
                        match meta.side {
                            Side::Buy => meta.adverse_price.min(position.mark_price),
                            Side::Sell => meta.adverse_price.max(position.mark_price),
                        }
                    };
                    let initial_quantity = meta.initial_quantity.max(position.quantity.abs());
                    let closed_fraction =
                        1.0 - position.quantity.abs() / initial_quantity.max(f64::EPSILON);
                    let partial_shield_hit = meta
                        .break_even_after_fraction
                        .is_some_and(|threshold| closed_fraction + 1e-6 >= threshold);
                    let executable_mode = self.config.executable_profit_guard
                        && !meta.fixed_time_exit
                        && meta.profit_shield_activation_pct.is_some();
                    let fresh_quote = executable_quotes.get(symbol).filter(|q| {
                        chrono::Utc::now().timestamp_millis() + self.clock_offset_ms - q.exchange_ms
                            <= crate::profit_guard::MAX_QUOTE_AGE_MS
                    });
                    if executable_mode {
                        if let Some(quote) = fresh_quote {
                            let trailing_activation = effective_trailing_activation(meta);
                            let exit = crate::profit_guard::observe(
                                &mut meta.executable_profit,
                                *quote,
                                meta.side,
                                meta.entry_price,
                                position.quantity.abs(),
                                crate::profit_guard::Protection {
                                    activation: meta.profit_shield_activation_pct,
                                    floor: meta.break_even_buffer_pct,
                                    trailing_activation,
                                    trailing_distance: meta.trailing_distance_pct,
                                    partial_activated: partial_shield_hit,
                                },
                            );
                            events.push(ExchangeEvent {kind:"executable_profit_observation".into(),payload:serde_json::json!({
                                "ts_ms":now_ms,"symbol":symbol,"candidate_id":meta.candidate_id,
                                "recipe":meta.recipe,"mark_price":position.mark_price,"entry_price":meta.entry_price,
                                "remaining_quantity":position.quantity.abs(),"guard":meta.executable_profit,
                                "cost_reserve_bps":crate::profit_guard::COST_RESERVE*10000.0,
                                "quote_source":"execution_venue_depth","exit_requested":exit,
                                "latency":{
                                    "exchange_quote_ms":quote.exchange_ms,
                                    "quote_observed_ms":quote.observed_ms,
                                    "decision_ms":chrono::Utc::now().timestamp_millis(),
                                    "quote_age_ms":chrono::Utc::now().timestamp_millis()+self.clock_offset_ms-quote.exchange_ms,
                                },
                            })});
                            if exit {
                                executable_exits.push((symbol.clone(), position.clone()));
                                continue;
                            }
                        }
                    }
                    if !meta.exit_requested
                        && early_failure_triggered(
                            meta.side,
                            meta.entry_price,
                            meta.extreme_price,
                            position.mark_price,
                            now_ms.saturating_sub(meta.holding_started_ms()),
                            (
                                meta.early_failure_after_ms,
                                meta.early_failure_adverse_pct,
                                meta.early_failure_max_favorable_pct,
                            ),
                        )
                    {
                        early_failures.push((symbol.clone(), position.clone()));
                        continue;
                    }
                    // Missing/stale execution quotes cannot arm/ratchet profit
                    // protection using another venue or a mark-price peak.
                    if executable_mode && fresh_quote.is_none() {
                        continue;
                    }
                    let protective_extreme = if executable_mode {
                        let net_peak = meta
                            .executable_profit
                            .as_ref()
                            .map(|s| s.peak_net_return)
                            .unwrap_or(0.0);
                        meta.entry_price
                            * (1.0
                                + meta.side.sign() * (net_peak + crate::profit_guard::COST_RESERVE))
                    } else {
                        meta.extreme_price
                    };
                    let favorable = meta.side.sign()
                        * (protective_extreme / meta.entry_price.max(f64::EPSILON) - 1.0);
                    let profit_shield_hit = meta
                        .profit_shield_activation_pct
                        .is_some_and(|activation| favorable >= activation);
                    let shield_hit = partial_shield_hit || profit_shield_hit;
                    let mut desired_stop = None;
                    let mut reason = if profit_shield_hit && !partial_shield_hit {
                        "pre_tp_profit_shield"
                    } else {
                        "risk_shield"
                    };
                    if shield_hit && !meta.break_even_armed && meta.entry_price > 0.0 {
                        desired_stop = Some(
                            meta.entry_price
                                * (1.0 + meta.side.sign() * meta.break_even_buffer_pct),
                        );
                    }
                    if shield_hit
                        && effective_trailing_activation(meta)
                            .is_some_and(|activation| favorable >= activation)
                    {
                        if let Some(distance) = meta.trailing_distance_pct {
                            let trailing = protective_extreme * (1.0 - meta.side.sign() * distance);
                            desired_stop = Some(match (meta.side, desired_stop) {
                                (Side::Buy, Some(current)) => current.max(trailing),
                                (Side::Sell, Some(current)) => current.min(trailing),
                                (_, None) => trailing,
                            });
                            reason = "trailing_protection";
                        }
                    }
                    if let Some(desired) = desired_stop {
                        let tighter = match meta.side {
                            Side::Buy => desired / meta.stop_price.max(f64::EPSILON) - 1.0 >= 0.001,
                            Side::Sell => {
                                meta.stop_price / desired.max(f64::EPSILON) - 1.0 >= 0.001
                            }
                        };
                        if tighter {
                            protection_updates.push((
                                symbol.clone(),
                                desired,
                                reason,
                                position.clone(),
                                stop_orders.get(symbol).copied(),
                            ));
                        }
                    }
                }
                // Persist causal peaks before issuing exits. Hosted stops stay
                // active; close_market re-reads quantity and uses reduce-only.
                if self.config.executable_profit_guard {
                    self.save()?;
                }
                for (symbol, position) in executable_exits {
                    let decision_ms = chrono::Utc::now().timestamp_millis();
                    let trigger_quote = self
                        .state
                        .positions
                        .get(&symbol)
                        .and_then(|meta| meta.executable_profit.as_ref())
                        .map(|guard| (guard.exchange_ms, guard.observed_ms));
                    match self.close_market(&position).await {
                        Ok(()) => {
                            let flat_confirmed_ms = chrono::Utc::now().timestamp_millis();
                            if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                meta.exit_requested = true;
                                meta.pending_exit_reason = Some("executable_profit_protection".into());
                            }
                            self.save()?;
                            events.push(ExchangeEvent {kind:"exchange_exit_requested".into(),payload:serde_json::json!({
                                "ts_ms":flat_confirmed_ms,"symbol":symbol,"reason":"executable_profit_protection",
                                "latency":{
                                    "trigger_exchange_ms":trigger_quote.map(|value|value.0),
                                    "trigger_observed_ms":trigger_quote.map(|value|value.1),
                                    "decision_ms":decision_ms,
                                    "flat_confirmed_ms":flat_confirmed_ms,
                                    "decision_to_flat_ms":flat_confirmed_ms.saturating_sub(decision_ms),
                                },
                            })});
                        }
                        Err(error) => events.push(ExchangeEvent {kind:"exchange_order_rejected".into(),payload:serde_json::json!({
                            "ts_ms":now_ms,"symbol":symbol,"reason":format!("executable profit close failed: {error:#}"),"hard_stop_retained":true,
                        })}),
                    }
                }
                for (symbol, position) in early_failures {
                    match self.close_market(&position).await {
                        Ok(()) => {
                            if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                meta.exit_requested = true;
                                meta.pending_exit_reason = Some("early_failure".into());
                            }
                            events.push(ExchangeEvent {
                                kind: "exchange_exit_requested".into(),
                                payload: serde_json::json!({
                                    "ts_ms":now_ms,
                                    "symbol":symbol,
                                    "reason":"early_failure",
                                    "mark_price":position.mark_price,
                                    "venue":"binance_demo"
                                }),
                            });
                        }
                        Err(error) => events.push(ExchangeEvent {
                            kind: "exchange_order_rejected".into(),
                            payload: serde_json::json!({
                                "ts_ms":now_ms,
                                "symbol":symbol,
                                "reason":format!("early-failure close failed: {error}"),
                                "venue":"binance_demo"
                            }),
                        }),
                    }
                }
                for (symbol, desired, reason, position, stop_order_id) in protection_updates {
                    let decision_ms = chrono::Utc::now().timestamp_millis();
                    let rules = self
                        .rules
                        .get(&symbol)
                        .cloned()
                        .ok_or_else(|| anyhow!("missing exchange rules for {symbol}"))?;
                    let replacement = async {
                        if let Some(order_id) = stop_order_id {
                            self.cancel_protective_order(&symbol, order_id).await?;
                        }
                        self.place_close_all_trigger(
                            &symbol,
                            position.side.opposite(),
                            "STOP_MARKET",
                            desired,
                            &rules,
                            client_order_id(
                                "protect",
                                &format!("{symbol}:{desired:.10}:{}", now_ms / 60_000),
                            ),
                        )
                        .await
                    }
                    .await;
                    match replacement {
                        Ok(stop_algo_id) => {
                            let exchange_ack_ms = chrono::Utc::now().timestamp_millis();
                            if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                meta.stop_price = desired;
                                meta.break_even_armed = true;
                                meta.stop_algo_id = Some(stop_algo_id);
                                meta.stop_reason = Some(reason.into());
                            }
                            events.push(ExchangeEvent {
                                kind: "exchange_protection_updated".into(),
                                payload: serde_json::json!({
                                    "ts_ms":exchange_ack_ms,"symbol":symbol,"stop_price":desired,
                                    "reason":reason,"venue":"binance_demo",
                                    "latency":{
                                        "decision_ms":decision_ms,
                                        "exchange_ack_ms":exchange_ack_ms,
                                        "decision_to_ack_ms":exchange_ack_ms.saturating_sub(decision_ms),
                                    },
                                }),
                            });
                        }
                        Err(error) => {
                            self.close_market(&position).await.with_context(|| {
                                format!(
                                    "protection update failed ({error}); emergency close failed"
                                )
                            })?;
                            if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                meta.exit_requested = true;
                                meta.pending_exit_reason = Some("protection_update_failed".into());
                            }
                            events.push(ExchangeEvent {
                                kind: "exchange_exit_requested".into(),
                                payload: serde_json::json!({"ts_ms":now_ms,"symbol":symbol,"reason":"protection_update_failed","detail":error.to_string(),"venue":"binance_demo"}),
                            });
                        }
                    }
                }
                let unprotected: Vec<_> = account
                    .positions
                    .keys()
                    .filter(|symbol| {
                        self.state.positions.contains_key(*symbol)
                            && !self
                                .state
                                .positions
                                .get(*symbol)
                                .is_some_and(|meta| meta.runner_active)
                            && !protected.get(*symbol).is_some_and(|(stop, take)| {
                                *stop
                                    && (*take
                                        || self.state.positions.get(*symbol).is_some_and(|meta| {
                                            meta.fixed_time_exit
                                                || managed_stop_only_exit(
                                                    meta.max_hold_ms,
                                                    meta.profit_shield_activation_pct,
                                                    meta.break_even_buffer_pct,
                                                    meta.trailing_activation_pct,
                                                    meta.trailing_distance_pct,
                                                )
                                                || meta.break_even_armed
                                                || meta.last_observed_quantity
                                                    < meta.initial_quantity - f64::EPSILON
                                        }))
                            })
                    })
                    .cloned()
                    .collect();
                // The account snapshot was taken before all per-symbol order
                // queries. A protective stop can legitimately trigger during
                // that interval, leaving the old snapshot with a position and
                // the newer order snapshot without its finished stop. Refresh
                // positions before emergency flattening so a normal stop fill
                // is not misreported as an unprotected-position failure.
                let verified_positions = if unprotected.is_empty() {
                    None
                } else {
                    let latest = self.signed_read("/fapi/v2/account", vec![]).await?;
                    Some(parse_account(&latest)?.positions)
                };
                for symbol in unprotected {
                    if let Some(position) = verified_positions
                        .as_ref()
                        .and_then(|positions| positions.get(&symbol))
                    {
                        self.close_market(position).await.with_context(|| {
                            format!(
                                "unprotected Binance demo position {symbol} could not be closed"
                            )
                        })?;
                        if let Some(meta) = self.state.positions.get_mut(&symbol) {
                            meta.exit_requested = true;
                            meta.pending_exit_reason = Some("protection_missing".into());
                        }
                        events.push(ExchangeEvent {
                            kind: "exchange_exit_requested".into(),
                            payload: serde_json::json!({"ts_ms":now_ms,"symbol":symbol,"reason":"protection_missing","venue":"binance_demo"}),
                        });
                    }
                }
                for symbol in expired {
                    if self
                        .state
                        .positions
                        .get(&symbol)
                        .is_some_and(|meta| meta.exit_requested)
                    {
                        continue;
                    }
                    if let Some(position) = account.positions.get(&symbol) {
                        match self.close_market(position).await {
                            Ok(()) => {
                                if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                    meta.exit_requested = true;
                                    meta.pending_exit_reason = Some("max_hold".into());
                                }
                                events.push(ExchangeEvent {
                                    kind: "exchange_exit_requested".into(),
                                    payload: serde_json::json!({"ts_ms":now_ms,"symbol":symbol,"reason":"max_hold","venue":"binance_demo"}),
                                });
                            }
                            Err(error) => events.push(ExchangeEvent {
                                kind: "exchange_order_rejected".into(),
                                payload: serde_json::json!({"ts_ms":now_ms,"symbol":symbol,"reason":format!("max-hold close failed: {error}"),"venue":"binance_demo"}),
                            }),
                        }
                    }
                }
                let disappeared: Vec<_> = self
                    .state
                    .positions
                    .keys()
                    .filter(|symbol| !account.positions.contains_key(*symbol))
                    .cloned()
                    .collect();
                for symbol in disappeared {
                    if let Some(meta) = self.state.positions.remove(&symbol) {
                        let summary = self
                            .trade_summary(&symbol, meta.holding_started_ms(), meta.side)
                            .await
                            .ok();
                        let (exit_reason, exit_order_id, exit_order_status) =
                            self.attribute_exit(&symbol, &meta, summary.as_ref()).await;
                        self.cancel_all(&symbol).await.ok();
                        let pnl_r = summary.as_ref().and_then(|value| {
                            meta.initial_risk_usd
                                .filter(|risk| *risk > f64::EPSILON)
                                .map(|risk| value.net_pnl_usd / risk)
                        });
                        // Positions restored from the previous state schema do
                        // not have an initial-risk basis. Reconcile and report
                        // them normally, but do not mix their dollar PnL into
                        // the new R-multiple performance window.
                        let statistical_fill_ratio = meta
                            .initial_risk_usd
                            .is_some()
                            .then_some(meta.fill_ratio.unwrap_or_default());
                        let operator_intervention = meta.pending_exit_actor.is_some();
                        let performance_sample = !operator_intervention
                            && statistical_fill_ratio
                                .is_some_and(|value| value >= MIN_PERFORMANCE_FILL_RATIO);
                        if let Some(summary) = summary.as_ref().filter(|_| !operator_intervention) {
                            let trade_pnl_usd = summary.net_pnl_usd;
                            let outcome = ExecutionOutcome {
                                candidate_id: meta.candidate_id.clone(),
                                exit_ms: now_ms,
                                pnl_usd: trade_pnl_usd,
                                pnl_r,
                                fill_ratio: statistical_fill_ratio.or(Some(0.0)),
                                initial_risk_usd: meta.initial_risk_usd,
                                sample_kind: "formal_position".into(),
                            };
                            self.record_outcome_once(
                                &meta.candidate_id,
                                &meta.recipe,
                                &symbol,
                                meta.side,
                                outcome,
                            );
                        }
                        self.record_closed_trade(&meta.candidate_id, &meta.recipe);
                        let mfe_pct = meta.side.sign()
                            * (meta.extreme_price / meta.entry_price.max(f64::EPSILON) - 1.0);
                        let mae_pct = (-meta.side.sign()
                            * (meta.adverse_price / meta.entry_price.max(f64::EPSILON) - 1.0))
                            .max(0.0);
                        let stop_slippage_bps = summary.as_ref().and_then(|value| {
                            (meta.stop_price > f64::EPSILON && value.last_exit_price > f64::EPSILON)
                                .then(|| match meta.side {
                                    Side::Buy => {
                                        (meta.stop_price / value.last_exit_price - 1.0) * 10_000.0
                                    }
                                    Side::Sell => {
                                        (value.last_exit_price / meta.stop_price - 1.0) * 10_000.0
                                    }
                                })
                        });
                        let executable_reversal = exit_reason == "executable_profit_protection"
                            && matches!(
                                meta.recipe.as_str(),
                                "trend_continuation"
                                    | TREND_REENTRY_RECIPE
                                    | TREND_PROFIT_REVERSAL_RECIPE
                            )
                            && next_profit_reversal_count(meta.profit_reversal_count, &exit_reason)
                                .is_some();
                        let ordinary_second_leg = self.lanes.trend_reentry_enabled
                            && meta.recipe == "trend_continuation"
                            && exit_reason == "profit_shield_stop"
                            && summary
                                .as_ref()
                                .is_some_and(|value| value.net_pnl_usd > 0.0)
                            && meta.extreme_price > f64::EPSILON;
                        if executable_reversal || ordinary_second_leg {
                            let first_exit_price = summary
                                .as_ref()
                                .map(|value| value.last_exit_price)
                                .unwrap_or(meta.stop_price);
                            let reversal_count = if executable_reversal {
                                next_profit_reversal_count(meta.profit_reversal_count, &exit_reason)
                                    .expect("executable reversal was checked above")
                            } else {
                                0
                            };
                            let stop_pct = (meta.initial_risk_usd.unwrap_or_default()
                                / (meta.entry_price * meta.initial_quantity).max(f64::EPSILON))
                            .clamp(
                                self.lanes.trend_reentry_min_stop_pct,
                                self.lanes.trend_reentry_max_stop_pct,
                            );
                            let campaign = TrendReentryCampaign {
                                source_candidate_id: meta.candidate_id.clone(),
                                symbol: symbol.clone(),
                                side: if executable_reversal {
                                    meta.side.opposite()
                                } else {
                                    meta.side
                                },
                                armed_ms: now_ms,
                                expires_ms: if executable_reversal {
                                    now_ms + 60_000
                                } else {
                                    now_ms
                                        + i64::from(self.lanes.trend_reentry_window_minutes)
                                            * 60_000
                                },
                                favorable_extreme: meta.extreme_price,
                                first_exit_price,
                                reset_ms: executable_reversal.then_some(now_ms),
                                last_evaluated_bar_ms: now_ms,
                                signal: executable_reversal.then_some(TrendReentrySignal {
                                    signal_ms: now_ms,
                                    reference_price: first_exit_price,
                                    stop_pct,
                                    body_pct: 0.0,
                                    directional_flow: 0.0,
                                }),
                                immediate_profit_reversal: executable_reversal,
                                profit_reversal_count: reversal_count,
                            };
                            self.state
                                .trend_reentries
                                .insert(symbol.clone(), campaign.clone());
                            events.push(ExchangeEvent {
                                kind: "trend_reentry_state".into(),
                                payload: serde_json::json!({
                                    "ts_ms":now_ms,
                                    "recipe":if executable_reversal { TREND_PROFIT_REVERSAL_RECIPE } else { TREND_REENTRY_RECIPE },
                                    "source_candidate_id":campaign.source_candidate_id,
                                    "symbol":campaign.symbol,
                                    "side":campaign.side,
                                    "stage":if executable_reversal { "ready_to_reverse" } else { "waiting_for_reset" },
                                    "immediate_profit_reversal":executable_reversal,
                                    "profit_reversal_count":reversal_count,
                                    "max_profit_reversals":MAX_PROFIT_REVERSALS_PER_CHAIN,
                                    "favorable_extreme":campaign.favorable_extreme,
                                    "first_exit_price":campaign.first_exit_price,
                                    "reset_required_pct":self.lanes.trend_reentry_reset_pct,
                                    "expires_ms":campaign.expires_ms,
                                    "venue":"binance_demo",
                                    "paper_only":true,
                                }),
                            });
                        } else if exit_reason == "executable_profit_protection"
                            && meta.profit_reversal_count >= MAX_PROFIT_REVERSALS_PER_CHAIN
                        {
                            events.push(ExchangeEvent {
                                kind: "trend_reentry_state".into(),
                                payload: serde_json::json!({
                                    "ts_ms":now_ms,
                                    "recipe":TREND_PROFIT_REVERSAL_RECIPE,
                                    "source_candidate_id":meta.candidate_id,
                                    "symbol":symbol,
                                    "side":meta.side.opposite(),
                                    "stage":"reversal_chain_complete",
                                    "profit_reversal_count":meta.profit_reversal_count,
                                    "max_profit_reversals":MAX_PROFIT_REVERSALS_PER_CHAIN,
                                    "venue":"binance_demo",
                                    "paper_only":true,
                                }),
                            });
                        }
                        events.push(ExchangeEvent {
                            kind: "exchange_exit".into(),
                            payload: serde_json::json!({
                                "ts_ms": chrono::Utc::now().timestamp_millis(),
                                "candidate_id": meta.candidate_id,
                                "recipe": meta.recipe,
                                "symbol": symbol,
                                "side": meta.side,
                                "exit_price": summary.as_ref().map(|value| value.last_exit_price),
                                "trade_entry_price": summary.as_ref().map(|value| value.entry_price),
                                "trade_entry_quantity": summary.as_ref().map(|value| value.entry_quantity),
                                "exit_quantity": summary.as_ref().map(|value| value.last_exit_quantity),
                                "average_exit_price": summary.as_ref().map(|value| value.exit_price),
                                "trade_exit_quantity": summary.as_ref().map(|value| value.exit_quantity),
                                "fee_usd": summary.as_ref().map(|value| value.fees_usd - meta.cumulative_reported_fee_usd),
                                "pnl_usd": summary.as_ref().map(|value| value.net_pnl_usd - meta.cumulative_reported_pnl_usd),
                                "trade_net_pnl_usd": summary.as_ref().map(|value| value.net_pnl_usd),
                                "pnl_r": pnl_r,
                                "fill_ratio": meta.fill_ratio,
                                "performance_sample": performance_sample,
                                "maker_fills":summary.as_ref().map(|value| value.maker_fills),
                                "taker_fills":summary.as_ref().map(|value| value.taker_fills),
                                "maker_notional_usd":summary.as_ref().map(|value| value.maker_notional_usd),
                                "taker_notional_usd":summary.as_ref().map(|value| value.taker_notional_usd),
                                "mfe_pct": mfe_pct,
                                "mae_pct": mae_pct,
                                "hold_ms": now_ms - meta.holding_started_ms(),
                                "holding_time_source":meta.holding_time_source(),
                                "reason": exit_reason,
                                "exit_actor": meta.pending_exit_actor,
                                "operator_note": meta.pending_exit_note,
                                "planned_stop_price": meta.stop_price,
                                "stop_slippage_bps": stop_slippage_bps,
                                "exit_order_id": exit_order_id,
                                "exit_order_status": exit_order_status,
                                "venue": "binance_demo"
                            }),
                        });
                    }
                }
                self.account = Some(account);
                self.last_error = None;
                self.last_sync_ms = Some(chrono::Utc::now().timestamp_millis());
                self.save()?;
                Ok(events)
            }
            Err(error) => {
                self.last_error = Some(error.to_string());
                Err(error)
            }
        }
    }

    pub fn account_frame(&mut self) -> Result<AccountFrame> {
        let account = self
            .account
            .as_ref()
            .ok_or_else(|| anyhow!("demo account not synchronized"))?;
        let baseline = self
            .state
            .baseline_wallet_usd
            .unwrap_or(account.wallet_balance);
        let realized = account.wallet_balance - baseline;
        let equity = self.portfolio.initial_equity_usd + account.margin_balance - baseline;
        let day = chrono::Utc::now().format("%Y-%m-%d").to_string();
        if self.state.risk_day != day {
            self.state.risk_day = day;
            self.state.risk_day_start_equity_usd = Some(equity);
        }
        let peak = self.state.peak_equity_usd.unwrap_or(equity).max(equity);
        self.state.peak_equity_usd = Some(peak);
        let gross = account
            .positions
            .values()
            .map(|position| position.quantity.abs() * position.mark_price)
            .sum();
        let mut active_slots_by_recipe = BTreeMap::new();
        for meta in self.state.positions.values() {
            *active_slots_by_recipe
                .entry(meta.recipe.clone())
                .or_insert(0) += 1;
        }
        for pending in self.state.pending_entries.values() {
            *active_slots_by_recipe
                .entry(pending.recipe.clone())
                .or_insert(0) += 1;
        }
        Ok(AccountFrame {
            equity_usd: equity,
            cash_usd: self.portfolio.initial_equity_usd + account.available_balance - baseline,
            realized_pnl_usd: realized,
            peak_equity_usd: peak,
            risk_day_start_equity_usd: self.state.risk_day_start_equity_usd.unwrap_or(equity),
            gross_exposure_usd: gross,
            open_positions: account.positions.len() + self.state.pending_entries.len(),
            active_slots_by_recipe,
        })
    }

    /// Forgive the current paper-session drawdown without touching positions,
    /// orders, PnL history, or execution-safety halts. This is deliberately an
    /// operator-only simulation control; the monitor serializes it through the
    /// same execution lock as reconciliation and order management.
    pub fn reset_risk_guard(&mut self, actor: &str, requested_ms: i64) -> Result<ExchangeEvent> {
        let account = self
            .account
            .as_ref()
            .ok_or_else(|| anyhow!("demo account not synchronized"))?;
        if self.state.execution_halt_reason.is_some() {
            return Err(anyhow!(
                "execution safety halt cannot be cleared by resetting the portfolio risk guard"
            ));
        }
        let baseline = self
            .state
            .baseline_wallet_usd
            .unwrap_or(account.wallet_balance);
        let equity = self.portfolio.initial_equity_usd + account.margin_balance - baseline;
        let (previous_day_start, previous_peak) = reset_portfolio_risk_baselines(
            &mut self.state,
            equity,
            chrono::Utc::now().format("%Y-%m-%d").to_string(),
        );
        self.save()?;
        Ok(ExchangeEvent {
            kind: "operator_risk_reset".into(),
            payload: serde_json::json!({
                "ts_ms":requested_ms,
                "actor":actor,
                "operator_action":true,
                "paper_only":true,
                "previous_risk_day_start_equity_usd":previous_day_start,
                "previous_peak_equity_usd":previous_peak,
                "risk_day_start_equity_usd":equity,
                "peak_equity_usd":equity,
                "positions_unchanged":true,
                "history_unchanged":true,
            }),
        })
    }

    /// Advance profitable Trend-continuation exits through a single, persisted
    /// second-leg campaign. The execution layer owns this state because it is
    /// created by a confirmed exchange exit, not by a stateless market scan.
    pub fn trend_reentry_artifacts(
        &mut self,
        frame: &MarketFrame,
    ) -> (Vec<ArtifactRecord>, Vec<ExchangeEvent>) {
        let mut events = Vec::new();
        let mut changed = false;
        let symbols: Vec<_> = self.state.trend_reentries.keys().cloned().collect();

        for symbol in symbols {
            let Some(mut campaign) = self.state.trend_reentries.remove(&symbol) else {
                continue;
            };
            if frame.as_of_ms > campaign.expires_ms {
                changed = true;
                events.push(reentry_state_event(
                    frame.as_of_ms,
                    &campaign,
                    "expired",
                    serde_json::json!({"reason":"confirmation_window_expired"}),
                ));
                continue;
            }
            let Some(instrument) = frame.instrument(&symbol) else {
                self.state.trend_reentries.insert(symbol, campaign);
                continue;
            };

            if campaign.reset_ms.is_none() {
                let adverse_reset = match campaign.side {
                    Side::Buy => {
                        (campaign.favorable_extreme - instrument.price)
                            / campaign.favorable_extreme.max(f64::EPSILON)
                    }
                    Side::Sell => {
                        (instrument.price - campaign.favorable_extreme)
                            / campaign.favorable_extreme.max(f64::EPSILON)
                    }
                };
                if adverse_reset >= self.lanes.trend_reentry_reset_pct {
                    campaign.reset_ms = Some(frame.as_of_ms);
                    campaign.last_evaluated_bar_ms = frame.as_of_ms;
                    changed = true;
                    events.push(reentry_state_event(
                        frame.as_of_ms,
                        &campaign,
                        "waiting_for_resume",
                        serde_json::json!({
                            "observed_reset_pct":adverse_reset,
                            "reset_required_pct":self.lanes.trend_reentry_reset_pct,
                        }),
                    ));
                }
            }

            if campaign.signal.is_none() {
                if let (Some(reset_ms), Some(series)) =
                    (campaign.reset_ms, instrument.fast_perpetual.as_ref())
                {
                    let closed: Vec<_> = series.values.iter().filter(|bar| bar.closed).collect();
                    let lookback = self.lanes.trend_reentry_lookback_bars;
                    for index in lookback..closed.len() {
                        let bar = closed[index];
                        if bar.close_ms <= reset_ms
                            || bar.close_ms <= campaign.last_evaluated_bar_ms
                        {
                            continue;
                        }
                        campaign.last_evaluated_bar_ms = bar.close_ms;
                        changed = true;
                        let prior = &closed[index - lookback..index];
                        let Some(signal) = second_leg_confirmation(
                            campaign.side,
                            instrument.price,
                            bar,
                            prior,
                            self.lanes.trend_reentry_min_body_pct,
                            self.lanes.trend_reentry_min_flow,
                            (
                                self.lanes.trend_reentry_min_stop_pct,
                                self.lanes.trend_reentry_max_stop_pct,
                            ),
                        ) else {
                            continue;
                        };
                        campaign.signal = Some(signal.clone());
                        events.push(reentry_state_event(
                            frame.as_of_ms,
                            &campaign,
                            "confirmed",
                            serde_json::json!({
                                "signal_ms":signal.signal_ms,
                                "reference_price":signal.reference_price,
                                "stop_pct":signal.stop_pct,
                                "body_pct":signal.body_pct,
                                "directional_flow":signal.directional_flow,
                            }),
                        ));
                        break;
                    }
                }
            }

            if campaign.signal.as_ref().is_some_and(|signal| {
                frame.as_of_ms > (signal.signal_ms + 300_000).min(campaign.expires_ms)
            }) {
                campaign.signal = None;
                changed = true;
                events.push(reentry_state_event(
                    frame.as_of_ms,
                    &campaign,
                    "waiting_for_resume",
                    serde_json::json!({"reason":"confirmed_entry_window_expired"}),
                ));
            }
            self.state.trend_reentries.insert(symbol, campaign);
        }

        let mut artifacts = Vec::new();
        let mut reset_count = 0u64;
        let mut confirmed_count = 0u64;
        let mut reasons = Vec::new();
        for campaign in self.state.trend_reentries.values() {
            if campaign.reset_ms.is_some() {
                reset_count += 1;
            }
            if campaign.signal.is_some() {
                confirmed_count += 1;
            }
            let stage = if campaign.signal.is_some() {
                if campaign.immediate_profit_reversal {
                    "ready_to_reverse"
                } else {
                    "confirmed"
                }
            } else if campaign.reset_ms.is_some() {
                "waiting_for_resume"
            } else {
                "waiting_for_reset"
            };
            reasons.push(format!("{}: {stage}", campaign.symbol));
            artifacts.push(ArtifactRecord {
                key: format!("campaign.trend_reentry.{}", campaign.symbol),
                producer: "lane.trend_continuation_reentry".into(),
                artifact: Artifact::State(StateArtifact {
                    state: stage.into(),
                    score: campaign
                        .signal
                        .as_ref()
                        .map(|signal| signal.body_pct * signal.directional_flow.max(0.0))
                        .unwrap_or_default(),
                    side: Some(campaign.side),
                    verdict: if campaign.signal.is_some() {
                        Verdict::Pass
                    } else {
                        Verdict::Block
                    },
                    reasons: vec![match stage {
                        "waiting_for_reset" => format!(
                            "waiting for a {:.1}% reset from the first-leg extreme",
                            self.lanes.trend_reentry_reset_pct * 100.0
                        ),
                        "waiting_for_resume" => format!(
                            "waiting for a completed 5m close through the prior {} bars",
                            self.lanes.trend_reentry_lookback_bars
                        ),
                        "ready_to_reverse" => format!(
                            "executable-profit exit confirmed; reverse leg {}/{} is ready",
                            campaign.profit_reversal_count, MAX_PROFIT_REVERSALS_PER_CHAIN
                        ),
                        _ => "second-leg entry is confirmed".into(),
                    }],
                    metrics: BTreeMap::from([
                        ("armed_ms".into(), campaign.armed_ms as f64),
                        ("expires_ms".into(), campaign.expires_ms as f64),
                        ("favorable_extreme".into(), campaign.favorable_extreme),
                        ("first_exit_price".into(), campaign.first_exit_price),
                        (
                            "reset_required_pct".into(),
                            self.lanes.trend_reentry_reset_pct,
                        ),
                    ]),
                    meta: ArtifactMeta {
                        as_of_ms: frame.as_of_ms,
                        expires_ms: campaign.expires_ms,
                        quality: DataQuality::Complete,
                        confidence: 1.0,
                        lineage: vec![campaign.source_candidate_id.clone()],
                    },
                }),
            });
            if let Some(signal) = campaign.signal.as_ref() {
                if let Some(book) = frame
                    .instrument(&campaign.symbol)
                    .and_then(|instrument| instrument.book.as_ref())
                    .filter(|book| book.meta.usable_at(frame.as_of_ms))
                {
                    let recipe = if campaign.immediate_profit_reversal {
                        TREND_PROFIT_REVERSAL_RECIPE
                    } else {
                        TREND_REENTRY_RECIPE
                    };
                    let mut tags = BTreeMap::from([
                        ("lane".into(), recipe.into()),
                        (
                            "priority".into(),
                            if campaign.immediate_profit_reversal {
                                "1.4"
                            } else {
                                "1.2"
                            }
                            .into(),
                        ),
                        (
                            "parent_candidate_id".into(),
                            campaign.source_candidate_id.clone(),
                        ),
                        ("second_leg".into(), "true".into()),
                        (
                            "profit_reversal_count".into(),
                            campaign.profit_reversal_count.to_string(),
                        ),
                        (
                            "immediate_profit_reversal".into(),
                            campaign.immediate_profit_reversal.to_string(),
                        ),
                        (
                            "risk_per_trade_pct".into(),
                            self.lanes.trend_risk_per_trade_pct.to_string(),
                        ),
                        ("stop_pct".into(), signal.stop_pct.to_string()),
                        ("target_r".into(), "10".into()),
                        ("take_profit_fraction".into(), "1".into()),
                        (
                            "profit_shield_activation_r".into(),
                            (self.lanes.trend_reentry_profit_shield_pct / signal.stop_pct)
                                .to_string(),
                        ),
                        (
                            "break_even_buffer_pct".into(),
                            self.lanes.trend_profit_shield_buffer_pct.to_string(),
                        ),
                        ("cost_aware_profit_shield".into(), "true".into()),
                        (
                            "pre_tp_trailing_activation_r".into(),
                            (self.lanes.trend_reentry_trailing_activation_pct / signal.stop_pct)
                                .to_string(),
                        ),
                        (
                            "trailing_distance_pct".into(),
                            self.lanes.trend_reentry_trailing_distance_pct.to_string(),
                        ),
                        (
                            "entry_timeout_ms".into(),
                            (i64::from(self.lanes.trend_reentry_entry_timeout_seconds) * 1_000)
                                .to_string(),
                        ),
                        ("taker_fallback".into(), "true".into()),
                        (
                            "taker_fallback_max_adverse_bps".into(),
                            if campaign.immediate_profit_reversal {
                                "20"
                            } else {
                                "8"
                            }
                            .into(),
                        ),
                        ("taker_fallback_size_multiplier".into(), "1".into()),
                        (
                            "max_entry_adverse_bps".into(),
                            if campaign.immediate_profit_reversal {
                                "20"
                            } else {
                                "8"
                            }
                            .into(),
                        ),
                        ("min_fill_ratio".into(), "0.8".into()),
                        (
                            "min_managed_fill_ratio".into(),
                            self.lanes.trend_min_managed_fill_ratio.to_string(),
                        ),
                        ("entry_invalidation_bps".into(), "20".into()),
                        (
                            "entry_guard_max_opposing_flow".into(),
                            self.lanes.trend_max_opposing_micro_flow.to_string(),
                        ),
                        (
                            "entry_guard_max_opposing_return_bps".into(),
                            self.lanes.trend_max_opposing_micro_return_bps.to_string(),
                        ),
                        ("early_failure_after_ms".into(), "0".into()),
                        ("early_failure_adverse_r".into(), "0".into()),
                        ("early_failure_max_mfe_r".into(), "0".into()),
                        ("body_pct".into(), signal.body_pct.to_string()),
                        (
                            "directional_flow".into(),
                            signal.directional_flow.to_string(),
                        ),
                    ]);
                    if campaign.immediate_profit_reversal {
                        tags.insert("bounded_taker_ioc".into(), "true".into());
                    } else {
                        tags.insert(
                            "entry_limit".into(),
                            if campaign.side == Side::Buy {
                                book.bid
                            } else {
                                book.ask
                            }
                            .to_string(),
                        );
                    }
                    let candidate = TradeCandidate {
                        id: format!(
                            "{recipe}:{}:{}:{}",
                            campaign.symbol, signal.signal_ms, campaign.profit_reversal_count
                        ),
                        recipe: recipe.into(),
                        symbol: campaign.symbol.clone(),
                        side: campaign.side,
                        signal_ms: signal.signal_ms,
                        expires_ms: (signal.signal_ms + 300_000).min(campaign.expires_ms),
                        reference_price: signal.reference_price,
                        score: if campaign.immediate_profit_reversal {
                            1.0
                        } else {
                            signal.body_pct * signal.directional_flow.max(0.0)
                        },
                        confidence: if campaign.immediate_profit_reversal {
                            0.85
                        } else {
                            (0.70
                                + signal.body_pct.min(0.01) * 10.0
                                + signal.directional_flow.min(0.30) * 0.30)
                                .min(0.95)
                        },
                        verdict: Verdict::Pass,
                        blockers: vec![],
                        evidence: vec![
                            if campaign.immediate_profit_reversal {
                                format!("{}.confirmed_executable_profit_exit", campaign.symbol)
                            } else {
                                format!("{}.binance_5m_second_leg", campaign.symbol)
                            },
                            format!("{}.binance_ws_microstructure", campaign.symbol),
                            format!("{}.book", campaign.symbol),
                        ],
                        tags,
                    };
                    artifacts.push(ArtifactRecord {
                        key: format!("candidate.{}", candidate.id),
                        producer: "lane.trend_continuation_reentry".into(),
                        artifact: Artifact::Candidate(candidate),
                    });
                }
            }
        }
        artifacts.push(ArtifactRecord {
            key: "lane.trend_continuation_reentry.status".into(),
            producer: "lane.trend_continuation_reentry".into(),
            artifact: Artifact::State(StateArtifact {
                state: if confirmed_count > 0 {
                    "ready_to_execute"
                } else if reset_count > 0 {
                    "waiting_for_resume"
                } else if self.state.trend_reentries.is_empty() {
                    "idle"
                } else {
                    "waiting_for_reset"
                }
                .into(),
                score: confirmed_count as f64,
                side: None,
                verdict: if confirmed_count > 0 {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if reasons.is_empty() {
                    vec!["waiting for a profitable Trend continuation shield exit".into()]
                } else {
                    reasons
                },
                metrics: BTreeMap::from([
                    (
                        "armed_campaigns".into(),
                        self.state.trend_reentries.len() as f64,
                    ),
                    ("reset_campaigns".into(), reset_count as f64),
                    ("confirmed_campaigns".into(), confirmed_count as f64),
                ]),
                meta: ArtifactMeta {
                    as_of_ms: frame.as_of_ms,
                    expires_ms: frame.as_of_ms + 30_000,
                    quality: DataQuality::Complete,
                    confidence: 1.0,
                    lineage: vec!["binance_confirmed_exit".into(), "binance_ws_5m".into()],
                },
            }),
        });
        if changed {
            self.save().ok();
        }
        (artifacts, events)
    }

    pub fn pinned_symbols(&self) -> BTreeSet<String> {
        self.position_symbols()
            .cloned()
            .chain(self.state.pending_entries.keys().cloned())
            .chain(self.state.trend_reentries.keys().cloned())
            .collect()
    }

    pub fn trend_reentry_status(&self) -> Value {
        serde_json::to_value(&self.state.trend_reentries).unwrap_or_else(|_| serde_json::json!({}))
    }

    pub fn has_seen(&self, candidate_id: &str) -> bool {
        self.state.seen.contains(candidate_id)
    }

    pub fn recipe_gate_status(&self, recipe: &str, now_ms: i64) -> RecipeGateStatus {
        let outcomes = self
            .state
            .recipe_outcomes
            .get(recipe)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let eligible: Vec<_> = outcomes
            .iter()
            .filter(|outcome| outcome.performance_eligible())
            .collect();
        let start = eligible.len().saturating_sub(self.risk.rolling_pf_window);
        let window = &eligible[start..];
        let profit: f64 = window
            .iter()
            .map(|outcome| outcome.performance_value())
            .filter(|value| *value > 0.0)
            .sum();
        let loss: f64 = -window
            .iter()
            .map(|outcome| outcome.performance_value())
            .filter(|value| *value < 0.0)
            .sum::<f64>();
        let rolling_profit_factor = (loss > f64::EPSILON).then_some(profit / loss);
        let failed = window.len() >= self.risk.rolling_pf_min_trades
            && loss > f64::EPSILON
            && rolling_profit_factor.unwrap_or_default() < self.risk.rolling_pf_floor;
        let next_probe_ms = failed.then(|| {
            window
                .last()
                .map(|outcome| outcome.exit_ms)
                .unwrap_or_default()
                + i64::from(self.risk.rolling_pf_cooldown_minutes) * 60_000
        });
        let count_key = recipe
            .strip_suffix(":buy")
            .or_else(|| recipe.strip_suffix(":sell"))
            .unwrap_or(recipe)
            .split('@')
            .next()
            .unwrap_or(recipe);
        let counts = self
            .state
            .recipe_execution_counts
            .get(count_key)
            .cloned()
            .unwrap_or_default();
        RecipeGateStatus {
            allowed: !failed || next_probe_ms.is_some_and(|value| now_ms >= value),
            completed_trades: window.len(),
            excluded_partial_trades: outcomes.len().saturating_sub(eligible.len()),
            rolling_profit_factor,
            rolling_net_pnl_usd: window.iter().map(|outcome| outcome.pnl_usd).sum(),
            next_probe_ms,
            entry_attempts: counts.entry_attempts,
            filled_entry_attempts: counts.filled_entry_attempts,
            formal_positions: counts.formal_positions,
            closed_trades: counts.closed_trades,
        }
    }

    pub fn candidate_gate_status(&self, recipe: &str, side: Side, now_ms: i64) -> RecipeGateStatus {
        self.recipe_gate_status(&gate_key(recipe, side), now_ms)
    }

    fn symbol_loss_cooldown_until(&self, symbol: &str) -> Option<i64> {
        loss_cooldown_until(
            self.state.symbol_outcomes.get(symbol).map(Vec::as_slice),
            self.risk.loss_cooldown_minutes,
        )
    }

    fn latest_symbol_exit_ms(&self, symbol: &str) -> Option<i64> {
        self.state
            .symbol_outcomes
            .get(symbol)
            .and_then(|values| values.last())
            .map(|outcome| outcome.exit_ms)
    }

    pub fn recipe_gate_snapshots(&self, now_ms: i64) -> BTreeMap<String, RecipeGateStatus> {
        [
            "btc_key_zone",
            "sfp_reversal",
            "trend_continuation",
            TREND_REENTRY_RECIPE,
            TREND_PROFIT_REVERSAL_RECIPE,
            "fast_trend_activation",
            LIQUIDATION_REVERSAL_RECIPE,
            "intraday_sweep_reversal",
            "early_ignition",
        ]
        .into_iter()
        .flat_map(|recipe| {
            [Side::Buy, Side::Sell].into_iter().map(move |side| {
                let key = gate_key(recipe, side);
                let status = self.recipe_gate_status(&key, now_ms);
                (key, status)
            })
        })
        .collect()
    }

    pub fn position_symbols(&self) -> impl Iterator<Item = &String> {
        self.account
            .iter()
            .flat_map(|account| account.positions.keys())
    }

    pub fn supports_symbol(&self, symbol: &str) -> bool {
        self.rules.contains_key(symbol)
    }

    pub fn position_snapshots(&self) -> BTreeMap<String, DemoPositionSnapshot> {
        let Some(account) = self.account.as_ref() else {
            return BTreeMap::new();
        };
        account
            .positions
            .iter()
            .map(|(symbol, position)| {
                let meta = self.state.positions.get(symbol);
                let pending = self.state.pending_entries.get(symbol);
                let notional = position.quantity.abs() * position.mark_price;
                let entry_notional = position.quantity.abs() * position.entry_price;
                (
                    symbol.clone(),
                    DemoPositionSnapshot {
                        candidate_id: meta
                            .map(|value| value.candidate_id.clone())
                            .or_else(|| pending.map(|value| value.plan.candidate_id.clone()))
                            .unwrap_or_default(),
                        recipe: meta
                            .map(|value| value.recipe.clone())
                            .or_else(|| pending.map(|value| value.recipe.clone()))
                            .unwrap_or_else(|| "external".into()),
                        symbol: symbol.clone(),
                        side: position.side,
                        entry_ms: meta
                            .map(ExecutionMeta::holding_started_ms)
                            .or_else(|| {
                                pending.map(|value| {
                                    value.first_fill_ms.unwrap_or(value.order_submitted_ms)
                                })
                            })
                            .unwrap_or_default(),
                        signal_ms: meta
                            .map(|value| value.signal_ms)
                            .or_else(|| pending.map(|value| value.signal_ms))
                            .unwrap_or_default(),
                        order_submitted_ms: meta
                            .map(|value| value.order_submitted_ms)
                            .or_else(|| pending.map(|value| value.order_submitted_ms))
                            .unwrap_or_default(),
                        first_fill_ms: meta
                            .and_then(|value| value.first_fill_ms)
                            .or_else(|| pending.and_then(|value| value.first_fill_ms)),
                        entry_completed_ms: meta.and_then(|value| value.entry_completed_ms),
                        entry_time_source: meta
                            .map(|value| value.holding_time_source().to_string())
                            .or_else(|| {
                                pending.and_then(|value| value.first_fill_time_source.clone())
                            })
                            .unwrap_or_else(|| "unavailable".into()),
                        entry_price: position.entry_price,
                        quantity: position.quantity.abs(),
                        initial_quantity: meta
                            .map(|value| value.initial_quantity)
                            .unwrap_or(position.quantity.abs()),
                        remaining_quantity: position.quantity.abs(),
                        stop_price: meta.map(|value| value.stop_price).unwrap_or_default(),
                        take_profit_prices: meta
                            .map(|value| {
                                if value.take_profit_prices.is_empty()
                                    && value.take_profit_price.is_finite()
                                    && value.take_profit_price > 0.0
                                {
                                    vec![(value.take_profit_price, 1.0)]
                                } else {
                                    value.take_profit_prices.clone()
                                }
                            })
                            .unwrap_or_default(),
                        unprotected_runner_fraction: meta
                            .and_then(|value| value.unprotected_runner_fraction),
                        runner_active: meta.map(|value| value.runner_active).unwrap_or(false),
                        isolated: position.isolated,
                        isolated_wallet_usd: position.isolated_wallet_usd,
                        max_hold_ms: meta.map(|value| value.max_hold_ms).unwrap_or_default(),
                        fixed_time_exit: meta.map(|value| value.fixed_time_exit).unwrap_or(false),
                        break_even_armed: meta.map(|value| value.break_even_armed).unwrap_or(false),
                        profit_shield_activation_pct: meta
                            .and_then(|value| value.profit_shield_activation_pct),
                        executable_profit: meta.and_then(|value| value.executable_profit.clone()),
                        executable_profit_guard_enabled: self.config.executable_profit_guard
                            && meta.is_some_and(|value| {
                                !value.fixed_time_exit
                                    && !value.runner_active
                                    && value.profit_shield_activation_pct.is_some()
                            }),
                        profit_reversal_count: meta
                            .map(|value| value.profit_reversal_count)
                            .unwrap_or_default(),
                        max_profit_reversals: MAX_PROFIT_REVERSALS_PER_CHAIN,
                        extreme_price: meta.map(|value| value.extreme_price),
                        trailing_activation_pct: meta.and_then(effective_trailing_activation),
                        trailing_distance_pct: meta.and_then(|value| value.trailing_distance_pct),
                        early_failure_after_ms: meta
                            .map(|value| value.early_failure_after_ms)
                            .unwrap_or_default(),
                        early_failure_adverse_pct: meta
                            .map(|value| value.early_failure_adverse_pct)
                            .unwrap_or_default(),
                        early_failure_max_favorable_pct: meta
                            .map(|value| value.early_failure_max_favorable_pct)
                            .unwrap_or_default(),
                        current_price: Some(position.mark_price),
                        current_notional_usd: Some(notional),
                        unrealized_pnl_usd: Some(position.unrealized_pnl),
                        unrealized_pnl_pct: (entry_notional > f64::EPSILON)
                            .then_some(position.unrealized_pnl / entry_notional),
                    },
                )
            })
            .collect()
    }

    pub async fn apply_plans(
        &mut self,
        frame: &MarketFrame,
        evaluation: &GraphEvaluation,
        allow_entries: bool,
    ) -> Vec<ExchangeEvent> {
        let mut events = self.apply_exit_intents(frame, evaluation).await;
        if self.state.execution_halt_reason.is_some() || !allow_entries {
            return events;
        }
        for record in evaluation.artifacts.values() {
            let Artifact::PositionPlan(plan) = &record.artifact else {
                continue;
            };
            if self.has_seen(&plan.candidate_id) {
                continue;
            }
            let candidate =
                evaluation
                    .artifacts
                    .values()
                    .find_map(|record| match &record.artifact {
                        Artifact::Candidate(value) if value.id == plan.candidate_id => Some(value),
                        _ => None,
                    });
            if candidate.is_none_or(|value| frame.as_of_ms > value.expires_ms) {
                continue;
            }
            let recipe = candidate
                .map(|value| value.recipe.clone())
                .unwrap_or_else(|| "unknown".into());
            if recipe == LIQUIDATION_REVERSAL_RECIPE {
                if liquidation_cooldown_active(
                    &self.state.seen,
                    &plan.symbol,
                    signal_ms_from_candidate(candidate, frame.as_of_ms),
                    i64::from(self.lanes.liquidation_cooldown_seconds) * 1_000,
                ) {
                    self.state.seen.insert(plan.candidate_id.clone());
                    events.push(ExchangeEvent {
                        kind: "exchange_plan_rejected".into(),
                        payload: serde_json::json!({
                            "ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,
                            "recipe":recipe,"symbol":plan.symbol,"side":plan.side,
                            "reason":"liquidation_symbol_cooldown","venue":"binance_demo",
                            "paper_only":true
                        }),
                    });
                    continue;
                }
                if self
                    .state
                    .pending_entries
                    .get(&plan.symbol)
                    .is_some_and(|pending| pending.recipe != LIQUIDATION_REVERSAL_RECIPE)
                {
                    let pending = self
                        .state
                        .pending_entries
                        .get(&plan.symbol)
                        .cloned()
                        .expect("pending entry was checked");
                    let Some(rules) = self.rules.get(&plan.symbol).cloned() else {
                        events.push(ExchangeEvent {
                            kind: "exchange_order_rejected".into(),
                            payload: serde_json::json!({
                                "ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,
                                "recipe":recipe,"symbol":plan.symbol,
                                "reason":"cannot safely supersede pending order without exchange rules",
                                "stage":"pending_priority","venue":"binance_demo","paper_only":true
                            }),
                        });
                        continue;
                    };
                    match self
                        .cancel_or_reconcile_entry(&plan.symbol, &pending.active_client_id)
                        .await
                    {
                        Ok(order) => {
                            let executed = parse_f64(&order, "executedQty").unwrap_or_default();
                            match self
                                .finish_failed_pending_entry(
                                    pending,
                                    executed,
                                    &rules,
                                    frame.as_of_ms,
                                    "superseded_by_liquidation_exhaustion_reversal".into(),
                                )
                                .await
                            {
                                Ok(event) => events.push(event),
                                Err(error) => {
                                    events.push(ExchangeEvent {
                                        kind: "exchange_order_rejected".into(),
                                        payload: serde_json::json!({
                                            "ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,
                                            "recipe":recipe,"symbol":plan.symbol,
                                            "reason":format!("pending strategy order could not be safely superseded: {error:#}"),
                                            "stage":"pending_priority","venue":"binance_demo","paper_only":true
                                        }),
                                    });
                                    continue;
                                }
                            }
                        }
                        Err(error) => {
                            events.push(ExchangeEvent {
                                kind: "exchange_order_rejected".into(),
                                payload: serde_json::json!({
                                    "ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,
                                    "recipe":recipe,"symbol":plan.symbol,
                                    "reason":format!("pending strategy order cancellation was not reconciled: {error:#}"),
                                    "stage":"pending_priority","venue":"binance_demo","paper_only":true
                                }),
                            });
                            continue;
                        }
                    }
                }
            }
            let rotates_existing_tail = recipe == "btc_key_zone"
                && self.state.positions.get(&plan.symbol).is_some_and(|meta| {
                    let remaining_fraction =
                        meta.last_observed_quantity / meta.initial_quantity.max(f64::EPSILON);
                    meta.runner_active
                        || (meta.recipe == "btc_key_zone"
                            && meta.unprotected_runner_fraction.is_some_and(|runner| {
                                remaining_fraction > 0.0 && remaining_fraction <= runner * 1.01
                            }))
                });
            if rotates_existing_tail {
                if let Some(position) = self
                    .account
                    .as_ref()
                    .and_then(|account| account.positions.get(&plan.symbol))
                    .cloned()
                {
                    match self.close_market(&position).await {
                        Ok(()) => {
                            if let Some(meta) = self.state.positions.get_mut(&plan.symbol) {
                                meta.exit_requested = true;
                                meta.pending_exit_reason = Some("campaign_tail_rotation".into());
                            }
                            self.save().ok();
                            events.push(ExchangeEvent {
                                kind: "exchange_exit_requested".into(),
                                payload: serde_json::json!({
                                    "ts_ms":frame.as_of_ms,
                                    "candidate_id":plan.candidate_id,
                                    "recipe":recipe,
                                    "symbol":plan.symbol,
                                    "side":position.side,
                                    "reason":"campaign_tail_rotation",
                                    "next_side":plan.side,
                                    "venue":"binance_demo"
                                }),
                            });
                        }
                        Err(error) => events.push(ExchangeEvent {
                            kind: "exchange_order_rejected".into(),
                            payload: serde_json::json!({
                                "ts_ms":frame.as_of_ms,
                                "candidate_id":plan.candidate_id,
                                "recipe":recipe,
                                "symbol":plan.symbol,
                                "reason":format!("campaign tail rotation failed: {error}"),
                                "venue":"binance_demo"
                            }),
                        }),
                    }
                }
                // Reconciliation removes the old one-way position on the next
                // loop. The same unconsumed zone can then create the new leg.
                continue;
            }
            let fast_active = self
                .state
                .positions
                .values()
                .filter(|meta| meta.recipe == FAST_TREND_ACTIVATION_RECIPE)
                .count()
                + self
                    .state
                    .pending_entries
                    .values()
                    .filter(|pending| pending.recipe == FAST_TREND_ACTIVATION_RECIPE)
                    .count();
            let locally_active = self.state.positions.len() + self.state.pending_entries.len();
            let unattributed_exchange_positions = self
                .account
                .as_ref()
                .map(|account| {
                    account
                        .positions
                        .len()
                        .saturating_sub(self.state.positions.len())
                })
                .unwrap_or_default();
            let standard_active =
                locally_active.saturating_sub(fast_active) + unattributed_exchange_positions;
            let slot_full = recipe_slot_full(
                &recipe,
                fast_active,
                standard_active,
                self.portfolio.max_positions,
            );
            if self.account.as_ref().is_some_and(|account| {
                account.positions.contains_key(&plan.symbol)
                    || self.state.positions.contains_key(&plan.symbol)
                    || self.state.pending_entries.contains_key(&plan.symbol)
                    || slot_full
            }) {
                continue;
            }
            // A fresh liquidation shock is the higher-priority, short-lived
            // signal. Once the account is confirmed flat for this symbol it
            // may retire an older trend re-entry campaign; otherwise that
            // stale campaign would silently consume the 12-second event.
            if recipe == LIQUIDATION_REVERSAL_RECIPE {
                if let Some(campaign) = self.state.trend_reentries.remove(&plan.symbol) {
                    events.push(reentry_state_event(
                        frame.as_of_ms,
                        &campaign,
                        "superseded_by_liquidation_exhaustion_reversal",
                        serde_json::json!({"candidate_id":plan.candidate_id}),
                    ));
                }
            }
            // A profitable first leg hands this symbol to the persisted re-entry
            // campaign until it confirms, expires, or makes its single attempt.
            // Without ownership here, a fresh 15m candidate could bypass the
            // required reset and compete with the dedicated 5m second leg.
            if !matches!(
                recipe.as_str(),
                TREND_REENTRY_RECIPE | TREND_PROFIT_REVERSAL_RECIPE
            ) && self.state.trend_reentries.contains_key(&plan.symbol)
            {
                self.state.seen.insert(plan.candidate_id.clone());
                events.push(ExchangeEvent {
                    kind: "exchange_plan_rejected".into(),
                    payload: serde_json::json!({
                        "ts_ms":frame.as_of_ms,
                        "candidate_id":plan.candidate_id,
                        "recipe":recipe,
                        "symbol":plan.symbol,
                        "side":plan.side,
                        "reason":"trend_reentry_campaign_owns_symbol",
                        "venue":"binance_demo",
                        "paper_only":true
                    }),
                });
                continue;
            }
            if !self.supports_symbol(&plan.symbol) {
                self.state.seen.insert(plan.candidate_id.clone());
                events.push(ExchangeEvent {
                    kind: "exchange_plan_rejected".into(),
                    payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"symbol":plan.symbol,"side":plan.side,"reason":"symbol_not_tradable_on_binance_demo","venue":"binance_demo","paper_only":true}),
                });
                continue;
            }
            let performance_key = gate_key(&recipe, plan.side);
            // A candle that predates this symbol's latest exit belongs to the
            // same episode we just traded. Reusing it caused an immediate PROM
            // re-entry after a profitable trailing exit.
            if recipe != TREND_PROFIT_REVERSAL_RECIPE
                && candidate.is_some_and(|value| {
                    self.latest_symbol_exit_ms(&plan.symbol)
                        .is_some_and(|exit_ms| value.signal_ms <= exit_ms)
                })
            {
                self.state.seen.insert(plan.candidate_id.clone());
                events.push(ExchangeEvent {
                    kind: "exchange_plan_rejected".into(),
                    payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"symbol":plan.symbol,"side":plan.side,"reason":"signal_precedes_latest_symbol_exit","signal_ms":candidate.map(|value| value.signal_ms),"latest_symbol_exit_ms":self.latest_symbol_exit_ms(&plan.symbol),"venue":"binance_demo","paper_only":true}),
                });
                continue;
            }
            if !matches!(
                recipe.as_str(),
                LIQUIDATION_REVERSAL_RECIPE | TREND_PROFIT_REVERSAL_RECIPE
            ) && self
                .symbol_loss_cooldown_until(&plan.symbol)
                .is_some_and(|until_ms| frame.as_of_ms < until_ms)
            {
                self.state.seen.insert(plan.candidate_id.clone());
                events.push(ExchangeEvent {
                    kind: "exchange_plan_rejected".into(),
                    payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"symbol":plan.symbol,"side":plan.side,"reason":"symbol_loss_cooldown","cooldown_until_ms":self.symbol_loss_cooldown_until(&plan.symbol),"venue":"binance_demo","paper_only":true}),
                });
                continue;
            }
            let performance_gate = self.recipe_gate_status(&performance_key, frame.as_of_ms);
            if !performance_gate.allowed {
                self.state.seen.insert(plan.candidate_id.clone());
                events.push(ExchangeEvent {
                    kind: "exchange_plan_rejected".into(),
                    payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"performance_key":performance_key,"symbol":plan.symbol,"side":plan.side,"reason":"rolling_profit_factor_gate","venue":"binance_demo","paper_only":true}),
                });
                continue;
            }
            let probe = performance_gate.completed_trades < self.risk.rolling_pf_min_trades
                || performance_gate.next_probe_ms.is_some();
            let size_multiplier = if probe {
                self.risk.rolling_pf_probe_size_multiplier
            } else {
                1.0
            };
            let cost = plan_cost_diagnostics(frame, plan);
            if matches!(
                recipe.as_str(),
                TREND_REENTRY_RECIPE | TREND_PROFIT_REVERSAL_RECIPE
            ) {
                if let Some(campaign) = self.state.trend_reentries.remove(&plan.symbol) {
                    events.push(reentry_state_event(
                        frame.as_of_ms,
                        &campaign,
                        "entry_attempted",
                        serde_json::json!({"candidate_id":plan.candidate_id}),
                    ));
                }
            }
            // Claim the candidate before touching the exchange. At-most-once is the
            // safe failure mode: an entry can fill even when a later protection or
            // reconciliation request fails. Retrying the same signal would open and
            // flatten it repeatedly, paying spread and fees on every frame.
            self.state.seen.insert(plan.candidate_id.clone());
            self.save().ok();
            let signal_ms = candidate
                .map(|value| value.signal_ms)
                .unwrap_or(frame.as_of_ms);
            let discovery_latency_ms = candidate
                .and_then(|value| value.tags.get("discovery_latency_ms"))
                .and_then(|value| value.parse::<i64>().ok());
            if plan.entry_limit.is_some() {
                match self
                    .start_pending_entry(
                        plan,
                        recipe.clone(),
                        size_multiplier,
                        signal_ms,
                        discovery_latency_ms,
                    )
                    .await
                {
                    Ok(event) => events.push(event),
                    Err(error) => events.push(ExchangeEvent {
                        kind: "exchange_order_rejected".into(),
                        payload: serde_json::json!({
                            "ts_ms":chrono::Utc::now().timestamp_millis(),
                            "candidate_id":plan.candidate_id,
                            "recipe":recipe,
                            "symbol":plan.symbol,
                            "side":plan.side,
                            "reason":error.to_string(),
                            "stage":"entry_submission",
                            "venue":"binance_demo",
                            "paper_only":true,
                        }),
                    }),
                }
                continue;
            }
            let attempt_started_ms = chrono::Utc::now().timestamp_millis();
            self.record_entry_attempt(&plan.candidate_id, &recipe);
            self.save().ok();
            match self.place_bracket(plan, size_multiplier).await {
                Ok(fill) => {
                    let planned_notional_usd =
                        plan.notional_usd * size_multiplier * fill.size_multiplier;
                    let actual_notional_usd = fill.entry_price * fill.quantity;
                    let fill_ratio = actual_notional_usd / planned_notional_usd.max(f64::EPSILON);
                    let actual_initial_risk_usd =
                        fill.quantity * (fill.entry_price - fill.stop_price).abs();
                    let actual_protection =
                        protection_from_fill(plan, actual_notional_usd, actual_initial_risk_usd);
                    self.state.positions.insert(
                        plan.symbol.clone(),
                        ExecutionMeta {
                            candidate_id: plan.candidate_id.clone(),
                            recipe: recipe.clone(),
                            side: plan.side,
                            entry_ms: fill.first_fill_ms,
                            signal_ms: candidate.map(|value| value.signal_ms).unwrap_or_default(),
                            order_submitted_ms: fill.order_submitted_ms,
                            first_fill_ms: Some(fill.first_fill_ms),
                            entry_completed_ms: Some(fill.entry_completed_ms),
                            entry_time_source: Some(fill.entry_time_source.into()),
                            entry_price: fill.entry_price,
                            initial_quantity: fill.quantity,
                            last_observed_quantity: fill.quantity,
                            cumulative_reported_fee_usd: 0.0,
                            cumulative_reported_pnl_usd: 0.0,
                            planned_notional_usd: Some(planned_notional_usd),
                            fill_ratio: Some(fill_ratio),
                            initial_risk_usd: Some(actual_initial_risk_usd),
                            stop_price: fill.stop_price,
                            take_profit_price: fill
                                .take_profit_prices
                                .last()
                                .map(|value| value.0)
                                .unwrap_or_default(),
                            take_profit_prices: fill.take_profit_prices.clone(),
                            unprotected_runner_fraction: plan.unprotected_runner_fraction,
                            runner_active: false,
                            executable_profit: None,
                            break_even_after_fraction: plan.break_even_after_fraction.map(
                                |fraction| {
                                    self.rules
                                        .get(&plan.symbol)
                                        .map(|rules| {
                                            floor_step(
                                                fill.quantity * fraction,
                                                rules.quantity_step,
                                            ) / fill.quantity.max(f64::EPSILON)
                                        })
                                        .unwrap_or(fraction)
                                },
                            ),
                            break_even_buffer_pct: plan.break_even_buffer_pct,
                            profit_shield_activation_pct: actual_protection.activation_pct,
                            break_even_armed: false,
                            extreme_price: fill.entry_price,
                            adverse_price: fill.entry_price,
                            trailing_activation_pct: actual_protection.activation_pct,
                            trailing_distance_pct: actual_protection.trailing_distance_pct,
                            early_failure_after_ms: plan.early_failure_after_ms,
                            early_failure_adverse_pct: plan.early_failure_adverse_pct,
                            early_failure_max_favorable_pct: plan.early_failure_max_favorable_pct,
                            max_hold_ms: plan.max_hold_ms,
                            fixed_time_exit: plan.fixed_time_exit,
                            max_hold_reviews: 0,
                            exit_requested: false,
                            pending_exit_reason: None,
                            pending_exit_actor: None,
                            pending_exit_note: None,
                            stop_algo_id: Some(fill.stop_algo_id),
                            stop_reason: Some("initial_stop".into()),
                            take_profit_order_ids: fill.take_profit_order_ids.clone(),
                            profit_reversal_count: plan
                                .signal_context
                                .get("profit_reversal_count")
                                .and_then(|value| value.parse().ok())
                                .unwrap_or_default(),
                        },
                    );
                    self.record_filled_attempt(&plan.candidate_id, &recipe);
                    self.record_formal_position(&plan.candidate_id, &recipe);
                    self.save().ok();
                    let signal_to_order_ms = candidate
                        .map(|value| frame.as_of_ms.saturating_sub(value.signal_ms))
                        .unwrap_or_default();
                    events.push(ExchangeEvent {
                        kind: "exchange_entry".into(),
                        payload: serde_json::json!({"ts_ms":fill.first_fill_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"lane":recipe,"symbol":plan.symbol,"side":plan.side,"signal_context":plan.signal_context,"entry_mode":fill.entry_mode,"entry_price_source":fill.entry_price_source,"maker_attempted":fill.maker_attempted,"maker_wait_ms":fill.maker_wait_ms,"maker_reprices":fill.maker_reprices,"requested_limit":plan.entry_limit,"final_maker_limit":fill.final_maker_limit,"entry_price":fill.entry_price,"quantity":fill.quantity,"notional_usd":actual_notional_usd,"actual_notional_usd":actual_notional_usd,"planned_notional_usd":planned_notional_usd,"fill_ratio":fill_ratio,"partial_fill":fill_ratio < 0.999,"actual_protection":{"initial_risk_usd":actual_initial_risk_usd,"activation_pct":actual_protection.activation_pct,"trailing_distance_pct":actual_protection.trailing_distance_pct},"probe_size_multiplier":size_multiplier,"entry_size_multiplier":fill.size_multiplier,"margin_type":"isolated","unprotected_runner_fraction":plan.unprotected_runner_fraction,"entry_timing":{"signal_ms":candidate.map(|value|value.signal_ms),"order_submitted_ms":fill.order_submitted_ms,"first_fill_ms":fill.first_fill_ms,"entry_completed_ms":fill.entry_completed_ms,"source":fill.entry_time_source},"entry_guard":{"invalidation_bps":plan.entry_invalidation_bps,"max_opposing_flow":plan.entry_guard_max_opposing_flow,"max_opposing_return_bps":plan.entry_guard_max_opposing_return_bps},"early_failure":{"after_ms":plan.early_failure_after_ms,"adverse_pct":plan.early_failure_adverse_pct,"max_favorable_pct":plan.early_failure_max_favorable_pct},"spread_bps":cost.spread_bps,"expected_exit_slippage_bps":cost.expected_exit_slippage_bps,"estimated_round_trip_cost_bps":cost.estimated_round_trip_cost_bps,"gross_target_bps":cost.gross_target_bps,"target_to_cost_ratio":cost.target_to_cost_ratio,"order_id":fill.order_id,"stop_algo_id":fill.stop_algo_id,"take_profit_order_ids":fill.take_profit_order_ids,"signal_to_order_ms":signal_to_order_ms,"discovery_latency_ms":discovery_latency_ms,"venue":"binance_demo","paper_only":true}),
                    });
                }
                Err(error) => {
                    if is_invalid_symbol_error(&error) {
                        // Demo exchangeInfo can briefly retain contracts that
                        // its order gateway no longer accepts. Quarantine the
                        // symbol so the next universe refresh removes it.
                        self.rules.remove(&plan.symbol);
                    }
                    // A rejected bracket may still contain a filled entry followed
                    // by an emergency close. Preserve that round trip in the
                    // diagnostic ledger instead of reporting it as a zero-cost
                    // rejection.
                    let attempt = self
                        .trade_summary(
                            &plan.symbol,
                            attempt_started_ms.saturating_sub(1_000),
                            plan.side,
                        )
                        .await
                        .ok();
                    if let Some(summary) = attempt.as_ref().filter(|value| {
                        value.entry_quantity > f64::EPSILON && value.exit_quantity > f64::EPSILON
                    }) {
                        self.record_filled_attempt(&plan.candidate_id, &recipe);
                        let actual_risk = summary.entry_quantity
                            * (summary.entry_price
                                - plan.stop_price * summary.entry_price
                                    / plan.reference_price.max(f64::EPSILON))
                            .abs();
                        self.record_outcome_once(
                            &plan.candidate_id,
                            &recipe,
                            &plan.symbol,
                            plan.side,
                            ExecutionOutcome {
                                candidate_id: plan.candidate_id.clone(),
                                exit_ms: chrono::Utc::now().timestamp_millis(),
                                pnl_usd: summary.net_pnl_usd,
                                pnl_r: (actual_risk > f64::EPSILON)
                                    .then_some(summary.net_pnl_usd / actual_risk),
                                fill_ratio: Some(
                                    summary.entry_quantity
                                        / (plan.notional_usd * size_multiplier
                                            / plan.reference_price.max(f64::EPSILON))
                                        .max(f64::EPSILON),
                                ),
                                initial_risk_usd: (actual_risk > f64::EPSILON)
                                    .then_some(actual_risk),
                                sample_kind: "filled_entry_attempt".into(),
                            },
                        );
                        self.save().ok();
                    }
                    let failure = error.to_string();
                    let filled_round_trip = attempt.as_ref().is_some_and(|value| {
                        value.entry_quantity > f64::EPSILON && value.exit_quantity > f64::EPSILON
                    });
                    let accounting_may_be_late = !filled_round_trip
                        && (attempt
                            .as_ref()
                            .is_some_and(|value| value.entry_quantity > f64::EPSILON)
                            || failure.contains("protective order failed")
                            || failure.contains("incidental fill was flattened"));
                    if accounting_may_be_late {
                        self.state.pending_accounting.insert(
                            plan.candidate_id.clone(),
                            PendingAccountingState {
                                candidate_id: plan.candidate_id.clone(),
                                recipe: recipe.clone(),
                                symbol: plan.symbol.clone(),
                                side: plan.side,
                                start_ms: attempt_started_ms.saturating_sub(1_000),
                                requested_quantity: plan.notional_usd * size_multiplier
                                    / plan.reference_price.max(f64::EPSILON),
                                expected_exit_quantity: attempt
                                    .as_ref()
                                    .map(|value| value.entry_quantity)
                                    .unwrap_or_default(),
                                reference_price: plan.reference_price,
                                planned_stop_price: plan.stop_price,
                            },
                        );
                        self.save().ok();
                    }
                    events.push(ExchangeEvent {
                        kind: if filled_round_trip {
                            "exchange_entry_attempt_closed"
                        } else if accounting_may_be_late {
                            "exchange_entry_attempt_pending_accounting"
                        } else {
                            entry_failure_event_kind(&failure)
                        }
                        .into(),
                        payload: serde_json::json!({
                            "ts_ms":frame.as_of_ms,
                            "candidate_id":plan.candidate_id,
                            "recipe":recipe,
                            "lane":recipe,
                            "symbol":plan.symbol,
                            "side":plan.side,
                            "reference_price":plan.reference_price,
                            "requested_limit":plan.entry_limit,
                            "max_entry_adverse_bps":plan.max_entry_adverse_bps,
                            "taker_fallback":plan.taker_fallback,
                            "taker_fallback_max_adverse_bps":plan.taker_fallback_max_adverse_bps,
                            "taker_fallback_size_multiplier":plan.taker_fallback_size_multiplier,
                            "entry_invalidation_bps":plan.entry_invalidation_bps,
                            "entry_guard_max_opposing_flow":plan.entry_guard_max_opposing_flow,
                            "entry_guard_max_opposing_return_bps":plan.entry_guard_max_opposing_return_bps,
                            "spread_bps":cost.spread_bps,
                            "expected_exit_slippage_bps":cost.expected_exit_slippage_bps,
                            "estimated_round_trip_cost_bps":cost.estimated_round_trip_cost_bps,
                            "gross_target_bps":cost.gross_target_bps,
                            "target_to_cost_ratio":cost.target_to_cost_ratio,
                            "reason":failure,
                            "attempt_exit_price":attempt.as_ref().map(|value| value.exit_price),
                            "attempt_exit_quantity":attempt.as_ref().map(|value| value.exit_quantity),
                            "attempt_entry_price":attempt.as_ref().map(|value| value.entry_price),
                            "attempt_entry_quantity":attempt.as_ref().map(|value| value.entry_quantity),
                            "attempt_entry_notional_usd":attempt.as_ref().map(|value| value.entry_price * value.entry_quantity),
                            "attempt_fill_ratio":attempt.as_ref().map(|value| value.entry_quantity / (plan.notional_usd * size_multiplier / plan.reference_price.max(f64::EPSILON)).max(f64::EPSILON)),
                            "attempt_initial_risk_usd":attempt.as_ref().map(|value| value.entry_quantity * (value.entry_price - plan.stop_price * value.entry_price / plan.reference_price.max(f64::EPSILON)).abs()),
                            "attempt_fee_usd":attempt.as_ref().map(|value| value.fees_usd),
                            "attempt_net_pnl_usd":attempt.as_ref().map(|value| value.net_pnl_usd),
                            "attempt_maker_fills":attempt.as_ref().map(|value| value.maker_fills),
                            "attempt_taker_fills":attempt.as_ref().map(|value| value.taker_fills),
                            "venue":"binance_demo",
                            "paper_only":true
                        }),
                    });
                    if error.to_string().contains("protective order failed") {
                        self.state.execution_halt_reason = Some(format!(
                            "execution halted after a filled entry could not establish protection: {error}"
                        ));
                        self.save().ok();
                        break;
                    }
                }
            }
        }
        events
    }

    fn record_entry_attempt(&mut self, candidate_id: &str, recipe: &str) {
        if self
            .state
            .entry_attempt_ids
            .insert(candidate_id.to_string())
        {
            self.state
                .recipe_execution_counts
                .entry(recipe.to_string())
                .or_default()
                .entry_attempts += 1;
        }
    }

    fn record_filled_attempt(&mut self, candidate_id: &str, recipe: &str) {
        if self
            .state
            .filled_entry_attempt_ids
            .insert(candidate_id.to_string())
        {
            self.state
                .recipe_execution_counts
                .entry(recipe.to_string())
                .or_default()
                .filled_entry_attempts += 1;
        }
    }

    fn record_formal_position(&mut self, candidate_id: &str, recipe: &str) {
        if self
            .state
            .formal_position_ids
            .insert(candidate_id.to_string())
        {
            self.state
                .recipe_execution_counts
                .entry(recipe.to_string())
                .or_default()
                .formal_positions += 1;
        }
    }

    fn record_closed_trade(&mut self, candidate_id: &str, recipe: &str) {
        if self.state.closed_trade_ids.insert(candidate_id.to_string()) {
            self.state
                .recipe_execution_counts
                .entry(recipe.to_string())
                .or_default()
                .closed_trades += 1;
        }
    }

    fn record_outcome_once(
        &mut self,
        candidate_id: &str,
        recipe: &str,
        symbol: &str,
        side: Side,
        outcome: ExecutionOutcome,
    ) -> bool {
        if !self
            .state
            .accounted_attempt_ids
            .insert(candidate_id.to_string())
        {
            return false;
        }
        self.state
            .recipe_outcomes
            .entry(gate_key(recipe, side))
            .or_default()
            .push(outcome.clone());
        self.state
            .symbol_outcomes
            .entry(symbol.to_string())
            .or_default()
            .push(outcome);
        self.record_closed_trade(candidate_id, recipe);
        true
    }

    async fn progress_pending_accounting(&mut self, now_ms: i64) -> Vec<ExchangeEvent> {
        let ids: Vec<_> = self.state.pending_accounting.keys().cloned().collect();
        let mut events = Vec::new();
        for candidate_id in ids {
            let Some(pending) = self.state.pending_accounting.get(&candidate_id).cloned() else {
                continue;
            };
            let Ok(summary) = self
                .trade_summary(&pending.symbol, pending.start_ms, pending.side)
                .await
            else {
                continue;
            };
            let expected_exit = if pending.expected_exit_quantity > f64::EPSILON {
                pending.expected_exit_quantity
            } else {
                summary.entry_quantity
            };
            if summary.entry_quantity <= f64::EPSILON
                || summary.exit_quantity + 1e-9 < expected_exit
            {
                continue;
            }
            let risk = summary.entry_quantity
                * (summary.entry_price
                    - pending.planned_stop_price * summary.entry_price
                        / pending.reference_price.max(f64::EPSILON))
                .abs();
            self.record_filled_attempt(&candidate_id, &pending.recipe);
            self.record_outcome_once(
                &candidate_id,
                &pending.recipe,
                &pending.symbol,
                pending.side,
                ExecutionOutcome {
                    candidate_id: candidate_id.clone(),
                    exit_ms: now_ms,
                    pnl_usd: summary.net_pnl_usd,
                    pnl_r: (risk > f64::EPSILON).then_some(summary.net_pnl_usd / risk),
                    fill_ratio: Some(
                        summary.entry_quantity / pending.requested_quantity.max(f64::EPSILON),
                    ),
                    initial_risk_usd: (risk > f64::EPSILON).then_some(risk),
                    sample_kind: "filled_entry_attempt".into(),
                },
            );
            self.state.pending_accounting.remove(&candidate_id);
            self.save().ok();
            events.push(ExchangeEvent {
                kind: "exchange_entry_attempt_closed".into(),
                payload: serde_json::json!({
                    "ts_ms":now_ms,"candidate_id":candidate_id,"recipe":pending.recipe,
                    "symbol":pending.symbol,"side":pending.side,
                    "reason":"deferred_fill_accounting_reconciled",
                    "planned_quantity":pending.requested_quantity,
                    "filled_quantity":summary.entry_quantity,
                    "fill_ratio":summary.entry_quantity / pending.requested_quantity.max(f64::EPSILON),
                    "attempt_entry_price":summary.entry_price,
                    "attempt_entry_quantity":summary.entry_quantity,
                    "attempt_exit_price":summary.exit_price,
                    "attempt_exit_quantity":summary.exit_quantity,
                    "attempt_initial_risk_usd":risk,
                    "attempt_fee_usd":summary.fees_usd,
                    "attempt_net_pnl_usd":summary.net_pnl_usd,
                    "venue":"binance_demo","paper_only":true
                }),
            });
        }
        events
    }

    async fn start_pending_entry(
        &mut self,
        plan: &greed_kernel::PositionPlan,
        recipe: String,
        size_multiplier: f64,
        signal_ms: i64,
        discovery_latency_ms: Option<i64>,
    ) -> Result<ExchangeEvent> {
        let rules = self
            .rules
            .get(&plan.symbol)
            .cloned()
            .ok_or_else(|| anyhow!("missing exchange rules for {}", plan.symbol))?;
        let desired_quantity = floor_step(
            plan.notional_usd * size_multiplier / plan.reference_price,
            rules.quantity_step,
        );
        let quantity = cap_entry_quantity(
            desired_quantity,
            rules.max_limit_quantity,
            rules.quantity_step,
        )?;
        validate_entry_quantity_and_exits(plan, quantity, plan.reference_price, &rules)?;
        match self.pending_entry_guard(plan, chrono::Utc::now().timestamp_millis()) {
            EntryGuardState::Healthy | EntryGuardState::MicroReversal(_) => {}
            EntryGuardState::StructuralInvalidation(reason) => {
                return Err(anyhow!(
                    "entry signal invalidated before submission: {reason}"
                ));
            }
            EntryGuardState::Unavailable(reason) => {
                return Err(anyhow!(
                    "entry guard unavailable before submission: {reason}"
                ));
            }
        }
        self.prepare_symbol_for_entry(&plan.symbol).await?;
        self.ensure_isolated_margin(&plan.symbol).await?;
        self.signed(
            Method::POST,
            "/fapi/v1/leverage",
            vec![
                ("symbol".into(), plan.symbol.clone()),
                ("leverage".into(), self.config.leverage.to_string()),
            ],
        )
        .await
        .with_context(|| format!("set {} leverage to {}x", plan.symbol, self.config.leverage))?;

        let base_client_id = client_order_id("entry", &plan.candidate_id);
        let mut active_client_id = base_client_id.clone();
        let mut passive_price = if plan.side == Side::Buy {
            floor_step(
                plan.entry_limit.unwrap_or(plan.reference_price),
                rules.price_tick,
            )
        } else {
            ceil_step(
                plan.entry_limit.unwrap_or(plan.reference_price),
                rules.price_tick,
            )
        };
        let mut reprice_attempt = 0u8;
        let order_submitted_ms = chrono::Utc::now().timestamp_millis();
        let mut pending = PendingEntryState {
            plan: plan.clone(),
            recipe: recipe.clone(),
            signal_ms,
            discovery_latency_ms,
            size_multiplier,
            requested_quantity: quantity,
            active_client_id: active_client_id.clone(),
            passive_price,
            order_submitted_ms,
            deadline_ms: order_submitted_ms + plan.entry_timeout_ms.clamp(5_000, 120_000),
            next_reprice_ms: order_submitted_ms + MAKER_REPRICE_INTERVAL_MS,
            reprice_attempt,
            first_fill_ms: None,
            first_fill_time_source: None,
            preliminary_stop_algo_id: None,
            preliminary_stop_price: None,
            guard_unavailable_since_ms: None,
            micro_reversal_since_ms: None,
        };
        self.record_entry_attempt(&plan.candidate_id, &recipe);
        // Write the deterministic client id before touching Binance. If the
        // process dies after submission, restart reconciliation can find the
        // same order and establish protection instead of losing ownership.
        self.state
            .pending_entries
            .insert(plan.symbol.clone(), pending.clone());
        self.save()?;
        let order = loop {
            pending.active_client_id.clone_from(&active_client_id);
            pending.passive_price = passive_price;
            pending.reprice_attempt = reprice_attempt;
            self.state
                .pending_entries
                .insert(plan.symbol.clone(), pending.clone());
            self.save()?;
            match self
                .submit_or_lookup(
                    &plan.symbol,
                    &active_client_id,
                    vec![
                        ("symbol".into(), plan.symbol.clone()),
                        ("side".into(), side_name(plan.side).into()),
                        ("type".into(), "LIMIT".into()),
                        ("timeInForce".into(), "GTX".into()),
                        ("quantity".into(), decimal(quantity, rules.quantity_step)),
                        ("price".into(), decimal(passive_price, rules.price_tick)),
                        ("newClientOrderId".into(), active_client_id.clone()),
                        ("newOrderRespType".into(), "ACK".into()),
                    ],
                )
                .await
            {
                Ok(order) => break order,
                Err(error) if is_post_only_rejection(&error) => {
                    reprice_attempt += 1;
                    if reprice_attempt > 3 {
                        self.state.pending_entries.remove(&plan.symbol);
                        self.save().ok();
                        return Err(error).context("post-only entry crossed after three reprices");
                    }
                    let ticker = self
                        .public_get_params(
                            "/fapi/v1/ticker/bookTicker",
                            &[("symbol", plan.symbol.as_str())],
                        )
                        .await?;
                    passive_price = match plan.side {
                        Side::Buy => floor_step(
                            parse_f64(&ticker, "bidPrice")
                                .ok_or_else(|| anyhow!("book ticker bid is missing"))?,
                            rules.price_tick,
                        ),
                        Side::Sell => ceil_step(
                            parse_f64(&ticker, "askPrice")
                                .ok_or_else(|| anyhow!("book ticker ask is missing"))?,
                            rules.price_tick,
                        ),
                    };
                    let adverse_bps =
                        entry_adverse_bps(plan.side, passive_price, plan.reference_price);
                    if adverse_bps > plan.max_entry_adverse_bps {
                        self.state.pending_entries.remove(&plan.symbol);
                        self.save().ok();
                        return Err(anyhow!(
                            "post-only reprice drifted {adverse_bps:.1} bps against the signal (max {:.1})",
                            plan.max_entry_adverse_bps
                        ));
                    }
                    active_client_id = reprice_client_id(&base_client_id, reprice_attempt);
                }
                Err(error) => {
                    self.state.pending_entries.remove(&plan.symbol);
                    self.save().ok();
                    return Err(error);
                }
            }
        };
        let accepted_ms = chrono::Utc::now().timestamp_millis();
        let reconciled = self
            .signed_read(
                "/fapi/v1/order",
                vec![
                    ("symbol".into(), plan.symbol.clone()),
                    ("origClientOrderId".into(), active_client_id.clone()),
                ],
            )
            .await
            .unwrap_or(order.clone());
        let initially_executed = parse_f64(&reconciled, "executedQty").unwrap_or_default();
        let preliminary_stop_algo_id = if initially_executed > f64::EPSILON {
            let stop_distance = plan.side.sign() * (plan.reference_price - plan.stop_price)
                / plan.reference_price.max(f64::EPSILON);
            let provisional_stop_price = passive_price * (1.0 - plan.side.sign() * stop_distance);
            match self
                .place_close_all_trigger(
                    &plan.symbol,
                    plan.side.opposite(),
                    "STOP_MARKET",
                    provisional_stop_price,
                    &rules,
                    client_order_id("pendingstop", &plan.candidate_id),
                )
                .await
            {
                Ok(order_id) => Some(order_id),
                Err(protection_error) => {
                    let final_order = self
                        .cancel_or_reconcile_entry(&plan.symbol, &active_client_id)
                        .await
                        .with_context(|| {
                            format!(
                                "pending stop failed ({protection_error}) and entry cancellation could not be reconciled"
                            )
                        })?;
                    let executed = parse_f64(&final_order, "executedQty").unwrap_or_default();
                    if executed > f64::EPSILON {
                        let order_id = final_order["orderId"].as_i64().unwrap_or_default();
                        let (fill_ms, source) = self
                            .resolve_first_fill_time(
                                &plan.symbol,
                                order_id,
                                &final_order,
                                plan.side,
                                accepted_ms,
                            )
                            .await;
                        pending.first_fill_ms = Some(fill_ms);
                        pending.first_fill_time_source = Some(source.into());
                        self.state
                            .pending_entries
                            .insert(plan.symbol.clone(), pending.clone());
                        self.save().ok();
                        let reason = format!("provisional_protection_failed: {protection_error}");
                        return match self
                            .finish_failed_pending_entry(
                                pending,
                                executed,
                                &rules,
                                accepted_ms,
                                reason,
                            )
                            .await
                        {
                            Ok(event) => Ok(event),
                            Err(close_error) => {
                                self.state.execution_halt_reason = Some(format!(
                                    "unprotected maker fill after stop and emergency close failure: {close_error}"
                                ));
                                self.save().ok();
                                Err(close_error)
                            }
                        };
                    }
                    self.state.pending_entries.remove(&plan.symbol);
                    self.save().ok();
                    return Err(anyhow!(
                        "pending entry was canceled because its immediate close-all stop failed: {protection_error}"
                    ));
                }
            }
        } else {
            None
        };
        pending.active_client_id = active_client_id.clone();
        pending.passive_price = passive_price;
        pending.deadline_ms = accepted_ms + plan.entry_timeout_ms.clamp(5_000, 120_000);
        pending.next_reprice_ms = accepted_ms + MAKER_REPRICE_INTERVAL_MS;
        pending.reprice_attempt = reprice_attempt;
        pending.preliminary_stop_algo_id = preliminary_stop_algo_id;
        pending.preliminary_stop_price = preliminary_stop_algo_id.map(|_| {
            let stop_distance = plan.side.sign() * (plan.reference_price - plan.stop_price)
                / plan.reference_price.max(f64::EPSILON);
            passive_price * (1.0 - plan.side.sign() * stop_distance)
        });
        self.state
            .pending_entries
            .insert(plan.symbol.clone(), pending);
        if let Err(error) = self.save() {
            let final_order = self
                .cancel_or_reconcile_entry(&plan.symbol, &active_client_id)
                .await
                .with_context(|| {
                    format!(
                        "persist pending entry failed ({error}); cancellation reconciliation failed"
                    )
                })?;
            let executed = parse_f64(&final_order, "executedQty").unwrap_or_default();
            let mut cleanup_stop = preliminary_stop_algo_id;
            if executed > f64::EPSILON {
                if cleanup_stop.is_none() {
                    let stop_distance = plan.side.sign() * (plan.reference_price - plan.stop_price)
                        / plan.reference_price.max(f64::EPSILON);
                    cleanup_stop = Some(
                        self.place_close_all_trigger(
                            &plan.symbol,
                            plan.side.opposite(),
                            "STOP_MARKET",
                            passive_price * (1.0 - plan.side.sign() * stop_distance),
                            &rules,
                            client_order_id("pendingstop", &plan.candidate_id),
                        )
                        .await
                        .context("protect raced fill after pending-state persistence failed")?,
                    );
                }
                self.flatten_entry_quantity(
                    &plan.symbol,
                    plan.side,
                    executed,
                    &rules,
                    &client_order_id("persistfail", &plan.candidate_id),
                )
                .await
                .with_context(|| {
                    format!(
                        "persist pending entry failed ({error}); raced fill remains protected but emergency close failed"
                    )
                })?;
            }
            if let Some(order_id) = cleanup_stop {
                self.cancel_protective_order(&plan.symbol, ProtectiveOrderId::Algo(order_id))
                    .await
                    .ok();
            }
            self.state.pending_entries.remove(&plan.symbol);
            return Err(error).context("persist pending entry before returning to the main loop");
        }
        Ok(ExchangeEvent {
            kind: "exchange_entry_submitted".into(),
            payload: serde_json::json!({
                "ts_ms":order_submitted_ms,
                "candidate_id":plan.candidate_id,
                "recipe":recipe,
                "symbol":plan.symbol,
                "side":plan.side,
                "requested_quantity":quantity,
                "limit_price":passive_price,
                "order_id":order["orderId"],
                "client_order_id":active_client_id,
                "deadline_ms":accepted_ms + plan.entry_timeout_ms.clamp(5_000, 120_000),
                "venue":"binance_demo",
                "paper_only":true,
            }),
        })
    }

    /// Advance every resting maker entry by one non-blocking reconciliation
    /// step. The state is persisted after every exchange mutation, so a
    /// restart resumes the same client order rather than submitting a second
    /// entry. Existing positions are managed by the remainder of `sync`
    /// regardless of how long these orders wait.
    async fn progress_pending_entries(
        &mut self,
        now_ms: i64,
        account_positions: &BTreeMap<String, RemotePosition>,
    ) -> Result<Vec<ExchangeEvent>> {
        let symbols: Vec<_> = self.state.pending_entries.keys().cloned().collect();
        let mut events = Vec::new();
        for symbol in symbols {
            let Some(mut pending) = self.state.pending_entries.get(&symbol).cloned() else {
                continue;
            };
            let rules = self
                .rules
                .get(&symbol)
                .cloned()
                .ok_or_else(|| anyhow!("missing exchange rules for pending entry {symbol}"))?;
            let mut order = match self
                .signed_read(
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), symbol.clone()),
                        ("origClientOrderId".into(), pending.active_client_id.clone()),
                    ],
                )
                .await
            {
                Ok(value) => value,
                Err(_) => {
                    // The persisted write-ahead state can legitimately exist
                    // before Binance accepted the request. Re-submit with the
                    // same deterministic id; submit_or_lookup makes this safe
                    // when the original acknowledgement was merely lost.
                    self.submit_or_lookup(
                        &symbol,
                        &pending.active_client_id,
                        vec![
                            ("symbol".into(), symbol.clone()),
                            ("side".into(), side_name(pending.plan.side).into()),
                            ("type".into(), "LIMIT".into()),
                            ("timeInForce".into(), "GTX".into()),
                            (
                                "quantity".into(),
                                decimal(pending.requested_quantity, rules.quantity_step),
                            ),
                            (
                                "price".into(),
                                decimal(pending.passive_price, rules.price_tick),
                            ),
                            ("newClientOrderId".into(), pending.active_client_id.clone()),
                            ("newOrderRespType".into(), "ACK".into()),
                        ],
                    )
                    .await?
                }
            };
            let mut executed = parse_f64(&order, "executedQty").unwrap_or_default();
            if executed > f64::EPSILON && pending.first_fill_ms.is_none() {
                let order_id = order["orderId"].as_i64().unwrap_or_default();
                let (fill_ms, source) = self
                    .resolve_first_fill_time(&symbol, order_id, &order, pending.plan.side, now_ms)
                    .await;
                pending.first_fill_ms = Some(fill_ms);
                pending.first_fill_time_source = Some(source.into());
                self.record_filled_attempt(&pending.plan.candidate_id, &pending.recipe);
                // The fill timestamp and ownership must survive even when the
                // following protection request fails or the process restarts.
                self.state
                    .pending_entries
                    .insert(symbol.clone(), pending.clone());
                self.save()?;
            }

            let mut invalidation = None;
            match self.pending_entry_guard(&pending.plan, now_ms) {
                EntryGuardState::Healthy => {
                    pending.guard_unavailable_since_ms = None;
                    pending.micro_reversal_since_ms = None;
                }
                EntryGuardState::StructuralInvalidation(reason) => invalidation = Some(reason),
                EntryGuardState::MicroReversal(reason) => {
                    pending.guard_unavailable_since_ms = None;
                    invalidation = persistent_guard_reason(
                        &mut pending.micro_reversal_since_ms,
                        now_ms,
                        ENTRY_GUARD_REVERSAL_CONFIRM_MS,
                        &reason,
                    );
                }
                EntryGuardState::Unavailable(reason) => {
                    pending.micro_reversal_since_ms = None;
                    let missing_since = *pending.guard_unavailable_since_ms.get_or_insert(now_ms);
                    invalidation =
                        (now_ms - missing_since >= ENTRY_GUARD_STALE_GRACE_MS).then(|| {
                            format!(
                                "{reason} for {}ms (max {}ms)",
                                now_ms - missing_since,
                                ENTRY_GUARD_STALE_GRACE_MS
                            )
                        });
                }
            }

            let status = order["status"].as_str().unwrap_or("NEW").to_string();
            let terminal = order_status_is_terminal(&status);
            // A user or exchange-side protection may close a recovered fill
            // before the runtime has promoted it into `positions`. Only treat
            // it as flat after userTrades proves a complete round trip; a
            // freshly filled order can briefly precede the account snapshot.
            if executed > f64::EPSILON && !account_positions.contains_key(&symbol) {
                let started_ms = pending
                    .first_fill_ms
                    .unwrap_or(pending.order_submitted_ms)
                    .saturating_sub(1_000);
                if let Ok(mut summary) = self
                    .trade_summary_after_close(&symbol, started_ms, pending.plan.side, executed)
                    .await
                {
                    // An operator can flatten a partially filled position while
                    // the remainder of its maker order is still resting. Never
                    // leave that order able to reopen the symbol. Freeze it,
                    // account for any cancel-race fill, and flatten that race
                    // quantity before declaring the pending lifecycle closed.
                    if !terminal {
                        order = self
                            .cancel_or_reconcile_entry(&symbol, &pending.active_client_id)
                            .await?;
                        executed = reconciled_executed_quantity(executed, &order);
                        if executed > summary.exit_quantity + 1e-9 {
                            let close_id = client_order_id("raceclose", &pending.plan.candidate_id);
                            self.flatten_entry_quantity(
                                &symbol,
                                pending.plan.side,
                                executed - summary.exit_quantity,
                                &rules,
                                &close_id,
                            )
                            .await
                            .context(
                                "maker cancel raced with an operator close and the incremental fill could not be flattened",
                            )?;
                        }
                        summary = self
                            .trade_summary_after_close(
                                &symbol,
                                started_ms,
                                pending.plan.side,
                                executed,
                            )
                            .await?;
                    }
                    self.cancel_pending_stop(&symbol, &pending).await;
                    // Also remove any old regular/algo order whose identifier
                    // predates the current state schema. This is best-effort;
                    // `prepare_symbol_for_entry` performs a second hard check
                    // before this symbol can ever be traded again.
                    self.cancel_all(&symbol).await.ok();
                    self.state.pending_entries.remove(&symbol);
                    if !self.performance_epoch_reset {
                        let risk = summary.entry_quantity
                            * (summary.entry_price
                                - pending.plan.stop_price * summary.entry_price
                                    / pending.plan.reference_price.max(f64::EPSILON))
                            .abs();
                        self.record_filled_attempt(&pending.plan.candidate_id, &pending.recipe);
                        self.record_outcome_once(
                            &pending.plan.candidate_id,
                            &pending.recipe,
                            &pending.plan.symbol,
                            pending.plan.side,
                            ExecutionOutcome {
                                candidate_id: pending.plan.candidate_id.clone(),
                                exit_ms: now_ms,
                                pnl_usd: summary.net_pnl_usd,
                                pnl_r: (risk > f64::EPSILON).then_some(summary.net_pnl_usd / risk),
                                fill_ratio: Some(
                                    summary.entry_quantity
                                        / pending.requested_quantity.max(f64::EPSILON),
                                ),
                                initial_risk_usd: (risk > f64::EPSILON).then_some(risk),
                                sample_kind: "filled_entry_attempt".into(),
                            },
                        );
                    }
                    self.save()?;
                    events.push(ExchangeEvent {
                        kind: "exchange_pending_entry_reconciled_flat".into(),
                        payload: serde_json::json!({
                            "ts_ms":now_ms,"candidate_id":pending.plan.candidate_id,
                            "recipe":pending.recipe,"symbol":symbol,"side":pending.plan.side,
                            "filled_quantity":summary.entry_quantity,
                            "exit_quantity":summary.exit_quantity,
                            "fee_usd":summary.fees_usd,"net_pnl_usd":summary.net_pnl_usd,
                            "performance_epoch_reset":self.performance_epoch_reset,
                            "reason":"exchange_position_already_flat_with_complete_round_trip",
                            "venue":"binance_demo","paper_only":true
                        }),
                    });
                    continue;
                }
            }
            let wait_satisfied = managed_fill_threshold_reached(
                executed,
                pending.requested_quantity,
                rules.quantity_step,
                pending.plan.min_fill_ratio,
            );
            let deadline = now_ms >= pending.deadline_ms;
            if invalidation.is_some() || terminal || wait_satisfied || deadline {
                if status != "FILLED"
                    && !matches!(status.as_str(), "CANCELED" | "EXPIRED" | "REJECTED")
                {
                    order = self
                        .cancel_or_reconcile_entry(&symbol, &pending.active_client_id)
                        .await?;
                    executed = reconciled_executed_quantity(executed, &order);
                    if executed > f64::EPSILON && pending.first_fill_ms.is_none() {
                        let order_id = order["orderId"].as_i64().unwrap_or_default();
                        let (fill_ms, source) = self
                            .resolve_first_fill_time(
                                &symbol,
                                order_id,
                                &order,
                                pending.plan.side,
                                now_ms,
                            )
                            .await;
                        pending.first_fill_ms = Some(fill_ms);
                        pending.first_fill_time_source = Some(source.into());
                        self.record_filled_attempt(&pending.plan.candidate_id, &pending.recipe);
                    }
                }

                if invalidation.is_none()
                    && executed <= f64::EPSILON
                    && deadline
                    && pending.plan.taker_fallback
                {
                    let ticker = self
                        .public_get_params(
                            "/fapi/v1/ticker/bookTicker",
                            &[("symbol", symbol.as_str())],
                        )
                        .await?;
                    let executable = if pending.plan.side == Side::Buy {
                        parse_f64(&ticker, "askPrice")
                    } else {
                        parse_f64(&ticker, "bidPrice")
                    }
                    .ok_or_else(|| anyhow!("book ticker is missing an executable price"))?;
                    let adverse = entry_adverse_bps(
                        pending.plan.side,
                        executable,
                        pending.plan.reference_price,
                    );
                    if adverse <= pending.plan.taker_fallback_max_adverse_bps {
                        let fallback_limit = bounded_taker_ioc_price(
                            pending.plan.side,
                            pending.plan.reference_price,
                            executable,
                            pending.plan.taker_fallback_max_adverse_bps,
                            rules.price_tick,
                        )?;
                        let fallback_size =
                            pending.plan.taker_fallback_size_multiplier.clamp(0.01, 1.0);
                        let fallback_quantity = bounded_fallback_quantity(
                            pending.requested_quantity,
                            fallback_size,
                            rules.quantity_step,
                        );
                        validate_entry_quantity_and_exits(
                            &pending.plan,
                            fallback_quantity,
                            executable,
                            &rules,
                        )?;
                        let fallback_id = client_order_id("fallback", &pending.plan.candidate_id);
                        order = self
                            .submit_or_lookup(
                                &symbol,
                                &fallback_id,
                                vec![
                                    ("symbol".into(), symbol.clone()),
                                    ("side".into(), side_name(pending.plan.side).into()),
                                    ("type".into(), "LIMIT".into()),
                                    ("timeInForce".into(), "IOC".into()),
                                    (
                                        "quantity".into(),
                                        decimal(fallback_quantity, rules.quantity_step),
                                    ),
                                    ("price".into(), decimal(fallback_limit, rules.price_tick)),
                                    ("newClientOrderId".into(), fallback_id.clone()),
                                    ("newOrderRespType".into(), "RESULT".into()),
                                ],
                            )
                            .await?;
                        pending.requested_quantity = fallback_quantity;
                        pending.size_multiplier *= fallback_size;
                        executed = parse_f64(&order, "executedQty").unwrap_or_default();
                        let order_id = order["orderId"].as_i64().unwrap_or_default();
                        let (fill_ms, source) = self
                            .resolve_first_fill_time(
                                &symbol,
                                order_id,
                                &order,
                                pending.plan.side,
                                now_ms,
                            )
                            .await;
                        pending.first_fill_ms = Some(fill_ms);
                        pending.first_fill_time_source = Some(source.into());
                        self.record_filled_attempt(&pending.plan.candidate_id, &pending.recipe);
                    }
                }

                if let Some(reason) = invalidation.as_ref() {
                    events.push(
                        self.finish_failed_pending_entry(
                            pending,
                            executed,
                            &rules,
                            now_ms,
                            format!("signal_invalidated: {reason}"),
                        )
                        .await?,
                    );
                    continue;
                }
                if executed <= f64::EPSILON {
                    self.cancel_pending_stop(&symbol, &pending).await;
                    self.state.pending_entries.remove(&symbol);
                    self.save()?;
                    events.push(ExchangeEvent {
                        kind: "exchange_entry_expired".into(),
                        payload: serde_json::json!({
                            "ts_ms":now_ms,"candidate_id":pending.plan.candidate_id,
                            "recipe":pending.recipe,"symbol":symbol,"side":pending.plan.side,
                            "reason":if terminal {format!("order_{status}")} else {"maker_timeout".into()},
                            "venue":"binance_demo","paper_only":true
                        }),
                    });
                    continue;
                }
                let manageable = partial_fill_is_manageable(
                    executed,
                    pending.requested_quantity,
                    entry_price_hint(&order, pending.passive_price),
                    pending.plan.min_managed_fill_ratio,
                    &rules,
                    &pending.plan.take_profit_prices,
                );
                if manageable {
                    events.push(
                        self.finalize_pending_entry(pending, order, executed, &rules, now_ms)
                            .await?,
                    );
                } else {
                    events.push(
                        self.finish_failed_pending_entry(
                            pending,
                            executed,
                            &rules,
                            now_ms,
                            "fill_below_manageable_minimum".into(),
                        )
                        .await?,
                    );
                }
                continue;
            }

            // Only a genuinely resting, sub-threshold partial fill needs a
            // provisional close-all stop. A filled/terminal order must proceed
            // directly to finalization above; the old ordering tried to create
            // a provisional stop first and a single rejection permanently
            // prevented the formal stop and take-profit orders from being
            // installed.
            if pending_fill_needs_provisional_stop(
                executed,
                terminal,
                wait_satisfied,
                deadline,
                invalidation.is_some(),
            ) && pending.preliminary_stop_algo_id.is_none()
            {
                let stop_distance = pending.plan.side.sign()
                    * (pending.plan.reference_price - pending.plan.stop_price)
                    / pending.plan.reference_price.max(f64::EPSILON);
                let trigger =
                    pending.passive_price * (1.0 - pending.plan.side.sign() * stop_distance);
                match self
                    .place_close_all_trigger(
                        &symbol,
                        pending.plan.side.opposite(),
                        "STOP_MARKET",
                        trigger,
                        &rules,
                        client_order_id("pendingstop", &pending.plan.candidate_id),
                    )
                    .await
                {
                    Ok(algo_id) => {
                        pending.preliminary_stop_algo_id = Some(algo_id);
                        pending.preliminary_stop_price = Some(trigger);
                        self.state
                            .pending_entries
                            .insert(symbol.clone(), pending.clone());
                        self.save()?;
                    }
                    Err(protection_error) => {
                        // Do not leave an already-filled entry in an endless
                        // retry loop without formal management. Freeze the
                        // remaining entry quantity, reconcile the cancel race,
                        // then either promote the actual fill to a protected
                        // position or flatten it as an accounted failed attempt.
                        let canceled = match self
                            .cancel_or_reconcile_entry(&symbol, &pending.active_client_id)
                            .await
                        {
                            Ok(value) => value,
                            Err(cancel_error) => {
                                let reason = format!(
                                    "provisional stop failed ({protection_error:#}); entry cancellation could not be reconciled ({cancel_error:#})"
                                );
                                self.state.execution_halt_reason = Some(reason.clone());
                                self.state
                                    .pending_entries
                                    .insert(symbol.clone(), pending.clone());
                                self.save().ok();
                                return Err(anyhow!(reason));
                            }
                        };
                        executed = reconciled_executed_quantity(executed, &canceled);
                        if executed > f64::EPSILON && pending.first_fill_ms.is_none() {
                            let order_id = canceled["orderId"].as_i64().unwrap_or_default();
                            let (fill_ms, source) = self
                                .resolve_first_fill_time(
                                    &symbol,
                                    order_id,
                                    &canceled,
                                    pending.plan.side,
                                    now_ms,
                                )
                                .await;
                            pending.first_fill_ms = Some(fill_ms);
                            pending.first_fill_time_source = Some(source.into());
                            self.record_filled_attempt(&pending.plan.candidate_id, &pending.recipe);
                        }
                        self.state
                            .pending_entries
                            .insert(symbol.clone(), pending.clone());
                        self.save()?;
                        let manageable = partial_fill_is_manageable(
                            executed,
                            pending.requested_quantity,
                            entry_price_hint(&canceled, pending.passive_price),
                            pending.plan.min_managed_fill_ratio,
                            &rules,
                            &pending.plan.take_profit_prices,
                        );
                        let event = if manageable {
                            self.finalize_pending_entry(pending, canceled, executed, &rules, now_ms)
                                .await?
                        } else {
                            self.finish_failed_pending_entry(
                                pending,
                                executed,
                                &rules,
                                now_ms,
                                format!("provisional_protection_failed: {protection_error:#}"),
                            )
                            .await?
                        };
                        events.push(event);
                        continue;
                    }
                }
            }

            if executed <= f64::EPSILON
                && now_ms >= pending.next_reprice_ms
                && pending.reprice_attempt < MAX_MAKER_REPRICES
            {
                let ticker = self
                    .public_get_params("/fapi/v1/ticker/bookTicker", &[("symbol", symbol.as_str())])
                    .await?;
                let best_passive = match pending.plan.side {
                    Side::Buy => parse_f64(&ticker, "bidPrice"),
                    Side::Sell => parse_f64(&ticker, "askPrice"),
                }
                .ok_or_else(|| anyhow!("book ticker passive price is missing"))?;
                let next_price = capped_passive_price(
                    pending.plan.side,
                    best_passive,
                    pending.plan.reference_price,
                    pending.plan.max_entry_adverse_bps,
                    rules.price_tick,
                );
                pending.next_reprice_ms += MAKER_REPRICE_INTERVAL_MS;
                if passive_price_improves(
                    pending.plan.side,
                    pending.passive_price,
                    next_price,
                    rules.price_tick,
                ) {
                    let canceled = self
                        .cancel_or_reconcile_entry(&symbol, &pending.active_client_id)
                        .await?;
                    let raced = reconciled_executed_quantity(executed, &canceled);
                    if raced > f64::EPSILON {
                        let order_id = canceled["orderId"].as_i64().unwrap_or_default();
                        let (fill_ms, source) = self
                            .resolve_first_fill_time(
                                &symbol,
                                order_id,
                                &canceled,
                                pending.plan.side,
                                now_ms,
                            )
                            .await;
                        pending.first_fill_ms = Some(fill_ms);
                        pending.first_fill_time_source = Some(source.into());
                        self.record_filled_attempt(&pending.plan.candidate_id, &pending.recipe);
                        if partial_fill_is_manageable(
                            raced,
                            pending.requested_quantity,
                            entry_price_hint(&canceled, pending.passive_price),
                            pending.plan.min_managed_fill_ratio,
                            &rules,
                            &pending.plan.take_profit_prices,
                        ) {
                            events.push(
                                self.finalize_pending_entry(
                                    pending, canceled, raced, &rules, now_ms,
                                )
                                .await?,
                            );
                        } else {
                            events.push(
                                self.finish_failed_pending_entry(
                                    pending,
                                    raced,
                                    &rules,
                                    now_ms,
                                    "reprice_cancel_race_below_manageable_minimum".into(),
                                )
                                .await?,
                            );
                        }
                        continue;
                    }
                    pending.reprice_attempt += 1;
                    pending.passive_price = next_price;
                    pending.active_client_id = reprice_client_id(
                        &client_order_id("entry", &pending.plan.candidate_id),
                        pending.reprice_attempt,
                    );
                    self.submit_or_lookup(
                        &symbol,
                        &pending.active_client_id,
                        vec![
                            ("symbol".into(), symbol.clone()),
                            ("side".into(), side_name(pending.plan.side).into()),
                            ("type".into(), "LIMIT".into()),
                            ("timeInForce".into(), "GTX".into()),
                            (
                                "quantity".into(),
                                decimal(pending.requested_quantity, rules.quantity_step),
                            ),
                            ("price".into(), decimal(next_price, rules.price_tick)),
                            ("newClientOrderId".into(), pending.active_client_id.clone()),
                            ("newOrderRespType".into(), "ACK".into()),
                        ],
                    )
                    .await?;
                }
            }
            self.state.pending_entries.insert(symbol, pending);
            self.save()?;
        }
        Ok(events)
    }

    async fn finalize_pending_entry(
        &mut self,
        pending: PendingEntryState,
        order: Value,
        executed: f64,
        rules: &SymbolRules,
        completed_ms: i64,
    ) -> Result<ExchangeEvent> {
        let plan = &pending.plan;
        let order_id = order["orderId"].as_i64().unwrap_or_default();
        let (entry_price, entry_price_source) = self
            .resolve_entry_price(&plan.symbol, order_id, &order, plan.side)
            .await
            .unwrap_or((pending.passive_price, "strategy_reference_fallback"));
        let (first_fill_ms, time_source) = match (
            pending.first_fill_ms,
            pending.first_fill_time_source.as_deref(),
        ) {
            (Some(value), Some(source)) => (value, source.to_string()),
            _ => {
                let (value, source) = self
                    .resolve_first_fill_time(
                        &plan.symbol,
                        order_id,
                        &order,
                        plan.side,
                        completed_ms,
                    )
                    .await;
                (value, source.into())
            }
        };
        let sign = plan.side.sign();
        let stop_distance = sign * (plan.reference_price - plan.stop_price)
            / plan.reference_price.max(f64::EPSILON);
        let planned_stop_price = entry_price * (1.0 - sign * stop_distance);
        let take_profit_prices = take_profit_prices_from_fill(plan, entry_price, executed);

        // A sub-threshold partial fill is already guarded by a close-all stop.
        // Reuse it during promotion instead of briefly duplicating (and often
        // conflicting with) another Binance closePosition order. The stored
        // trigger is the authoritative risk price for this position.
        let (stop_algo_id, stop_price) = match pending.preliminary_stop_algo_id {
            Some(value) => (
                value,
                pending.preliminary_stop_price.unwrap_or(planned_stop_price),
            ),
            None => match self
                .place_close_all_trigger(
                    &plan.symbol,
                    plan.side.opposite(),
                    "STOP_MARKET",
                    planned_stop_price,
                    rules,
                    client_order_id("stop", &plan.candidate_id),
                )
                .await
            {
                Ok(value) => (value, planned_stop_price),
                Err(error) => {
                    return self
                        .finish_failed_pending_entry(
                            pending,
                            executed,
                            rules,
                            completed_ms,
                            format!("exact_protection_failed: {error:#}"),
                        )
                        .await;
                }
            },
        };
        let mut take_profit_order_ids = Vec::new();
        for (index, (target, fraction)) in take_profit_prices.iter().enumerate() {
            match self
                .place_reduce_only_take_profit(
                    &plan.symbol,
                    plan.side.opposite(),
                    *target,
                    floor_step(executed * fraction, rules.quantity_step),
                    rules,
                    client_order_id(&format!("take{index}"), &plan.candidate_id),
                )
                .await
            {
                Ok(value) => take_profit_order_ids.push(value),
                Err(error) => {
                    return self
                        .finish_failed_pending_entry(
                            pending,
                            executed,
                            rules,
                            completed_ms,
                            format!("take_profit_protection_failed: {error}"),
                        )
                        .await;
                }
            }
        }
        let planned_notional_usd = plan.notional_usd * pending.size_multiplier;
        let actual_notional_usd = entry_price * executed;
        let fill_ratio = executed / pending.requested_quantity.max(f64::EPSILON);
        let actual_initial_risk_usd = executed * (entry_price - stop_price).abs();
        let actual_protection =
            protection_from_fill(plan, actual_notional_usd, actual_initial_risk_usd);
        self.state.positions.insert(
            plan.symbol.clone(),
            ExecutionMeta {
                candidate_id: plan.candidate_id.clone(),
                recipe: pending.recipe.clone(),
                side: plan.side,
                entry_ms: first_fill_ms,
                signal_ms: pending.signal_ms,
                order_submitted_ms: pending.order_submitted_ms,
                first_fill_ms: Some(first_fill_ms),
                entry_completed_ms: Some(completed_ms),
                entry_time_source: Some(time_source.clone()),
                entry_price,
                initial_quantity: executed,
                last_observed_quantity: executed,
                cumulative_reported_fee_usd: 0.0,
                cumulative_reported_pnl_usd: 0.0,
                planned_notional_usd: Some(planned_notional_usd),
                fill_ratio: Some(fill_ratio),
                initial_risk_usd: Some(actual_initial_risk_usd),
                stop_price,
                take_profit_price: take_profit_prices
                    .last()
                    .map(|value| value.0)
                    .unwrap_or_default(),
                take_profit_prices: take_profit_prices.clone(),
                unprotected_runner_fraction: plan.unprotected_runner_fraction,
                runner_active: false,
                executable_profit: None,
                break_even_after_fraction: plan.break_even_after_fraction.map(|fraction| {
                    floor_step(executed * fraction, rules.quantity_step)
                        / executed.max(f64::EPSILON)
                }),
                break_even_buffer_pct: plan.break_even_buffer_pct,
                profit_shield_activation_pct: actual_protection.activation_pct,
                break_even_armed: false,
                extreme_price: entry_price,
                adverse_price: entry_price,
                trailing_activation_pct: actual_protection.activation_pct,
                trailing_distance_pct: actual_protection.trailing_distance_pct,
                early_failure_after_ms: plan.early_failure_after_ms,
                early_failure_adverse_pct: plan.early_failure_adverse_pct,
                early_failure_max_favorable_pct: plan.early_failure_max_favorable_pct,
                max_hold_ms: plan.max_hold_ms,
                fixed_time_exit: plan.fixed_time_exit,
                max_hold_reviews: 0,
                exit_requested: false,
                pending_exit_reason: None,
                pending_exit_actor: None,
                pending_exit_note: None,
                stop_algo_id: Some(stop_algo_id),
                stop_reason: Some("initial_stop".into()),
                take_profit_order_ids: take_profit_order_ids.clone(),
                profit_reversal_count: plan
                    .signal_context
                    .get("profit_reversal_count")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_default(),
            },
        );
        self.state.pending_entries.remove(&plan.symbol);
        self.record_filled_attempt(&plan.candidate_id, &pending.recipe);
        self.record_formal_position(&plan.candidate_id, &pending.recipe);
        self.save()?;
        Ok(ExchangeEvent {
            kind: "exchange_entry".into(),
            payload: serde_json::json!({
                "ts_ms":first_fill_ms,"candidate_id":plan.candidate_id,"recipe":pending.recipe,
                "lane":pending.recipe,"symbol":plan.symbol,"side":plan.side,
                "signal_context":plan.signal_context,"entry_mode":"persistent_maker_limit",
                "entry_price_source":entry_price_source,"maker_attempted":true,
                "maker_wait_ms":completed_ms.saturating_sub(pending.order_submitted_ms),
                "maker_reprices":pending.reprice_attempt,"requested_limit":plan.entry_limit,
                "final_maker_limit":pending.passive_price,"entry_price":entry_price,
                "quantity":executed,"notional_usd":actual_notional_usd,
                "actual_notional_usd":actual_notional_usd,"planned_notional_usd":planned_notional_usd,
                "fill_ratio":fill_ratio,"partial_fill":fill_ratio < 0.999,
                "actual_protection":{"initial_risk_usd":actual_initial_risk_usd,
                    "activation_pct":actual_protection.activation_pct,
                    "trailing_distance_pct":actual_protection.trailing_distance_pct},
                "probe_size_multiplier":pending.size_multiplier,"margin_type":"isolated",
                "entry_timing":{"signal_ms":pending.signal_ms,"order_submitted_ms":pending.order_submitted_ms,
                    "first_fill_ms":first_fill_ms,"entry_completed_ms":completed_ms,"source":time_source},
                "order_id":order_id,"stop_algo_id":stop_algo_id,
                "take_profit_order_ids":take_profit_order_ids,
                "discovery_latency_ms":pending.discovery_latency_ms,
                "venue":"binance_demo","paper_only":true
            }),
        })
    }

    async fn finish_failed_pending_entry(
        &mut self,
        pending: PendingEntryState,
        executed: f64,
        rules: &SymbolRules,
        now_ms: i64,
        reason: String,
    ) -> Result<ExchangeEvent> {
        if executed > f64::EPSILON {
            let close_id = client_order_id("attemptclose", &pending.plan.candidate_id);
            // Remove staged regular targets but keep the close-all stop alive
            // until the emergency reduce-only close is confirmed.
            self.cancel_regular_orders(&pending.plan.symbol).await?;
            self.flatten_entry_or_confirm_flat(
                &pending.plan.symbol,
                pending.plan.side,
                executed,
                rules,
                &close_id,
            )
            .await
            .with_context(|| format!("{reason}; emergency close failed"))?;
            self.record_filled_attempt(&pending.plan.candidate_id, &pending.recipe);
            self.cancel_all(&pending.plan.symbol).await.ok();
        } else {
            self.cancel_pending_stop(&pending.plan.symbol, &pending)
                .await;
        }
        // Keep the provisional close-all stop live until Binance has accepted
        // the reduce-only flatten. A failed close therefore leaves a protected,
        // recoverable pending state instead of an unprotected foreign position.
        self.state.pending_entries.remove(&pending.plan.symbol);
        let started_ms = pending
            .first_fill_ms
            .unwrap_or(pending.order_submitted_ms)
            .saturating_sub(1_000);
        let summary = if executed > f64::EPSILON {
            self.trade_summary_after_close(
                &pending.plan.symbol,
                started_ms,
                pending.plan.side,
                executed,
            )
            .await
            .ok()
        } else {
            None
        };
        if let Some(summary) = summary.as_ref() {
            let risk = summary.entry_quantity
                * (summary.entry_price
                    - pending.plan.stop_price * summary.entry_price
                        / pending.plan.reference_price.max(f64::EPSILON))
                .abs();
            let outcome = ExecutionOutcome {
                candidate_id: pending.plan.candidate_id.clone(),
                exit_ms: now_ms,
                pnl_usd: summary.net_pnl_usd,
                pnl_r: (risk > f64::EPSILON).then_some(summary.net_pnl_usd / risk),
                fill_ratio: Some(
                    summary.entry_quantity / pending.requested_quantity.max(f64::EPSILON),
                ),
                initial_risk_usd: (risk > f64::EPSILON).then_some(risk),
                sample_kind: "filled_entry_attempt".into(),
            };
            self.record_outcome_once(
                &pending.plan.candidate_id,
                &pending.recipe,
                &pending.plan.symbol,
                pending.plan.side,
                outcome,
            );
        }
        if executed > f64::EPSILON && summary.is_none() {
            self.state.pending_accounting.insert(
                pending.plan.candidate_id.clone(),
                PendingAccountingState {
                    candidate_id: pending.plan.candidate_id.clone(),
                    recipe: pending.recipe.clone(),
                    symbol: pending.plan.symbol.clone(),
                    side: pending.plan.side,
                    start_ms: started_ms,
                    requested_quantity: pending.requested_quantity,
                    expected_exit_quantity: executed,
                    reference_price: pending.plan.reference_price,
                    planned_stop_price: pending.plan.stop_price,
                },
            );
        }
        self.save()?;
        Ok(ExchangeEvent {
            kind: if executed > f64::EPSILON && summary.is_some() {
                "exchange_entry_attempt_closed"
            } else if executed > f64::EPSILON {
                "exchange_entry_attempt_pending_accounting"
            } else {
                "exchange_entry_expired"
            }
            .into(),
            payload: serde_json::json!({
                "ts_ms":now_ms,"candidate_id":pending.plan.candidate_id,
                "recipe":pending.recipe,"symbol":pending.plan.symbol,"side":pending.plan.side,
                "reason":reason,"planned_quantity":pending.requested_quantity,
                "filled_quantity":executed,
                "fill_ratio":executed / pending.requested_quantity.max(f64::EPSILON),
                "attempt_entry_price":summary.as_ref().map(|value| value.entry_price),
                "attempt_entry_quantity":summary.as_ref().map(|value| value.entry_quantity),
                "attempt_exit_price":summary.as_ref().map(|value| value.exit_price),
                "attempt_exit_quantity":summary.as_ref().map(|value| value.exit_quantity),
                "attempt_initial_risk_usd":summary.as_ref().map(|value| value.entry_quantity * (value.entry_price - pending.plan.stop_price * value.entry_price / pending.plan.reference_price.max(f64::EPSILON)).abs()),
                "attempt_fee_usd":summary.as_ref().map(|value| value.fees_usd),
                "attempt_net_pnl_usd":summary.as_ref().map(|value| value.net_pnl_usd),
                "venue":"binance_demo","paper_only":true
            }),
        })
    }

    async fn cancel_pending_stop(&self, symbol: &str, pending: &PendingEntryState) {
        if let Some(order_id) = pending.preliminary_stop_algo_id {
            self.cancel_protective_order(symbol, ProtectiveOrderId::Algo(order_id))
                .await
                .ok();
        }
    }

    /// Submit a reduce-only market close requested by an authenticated operator.
    ///
    /// The exchange remains the source of truth. This method records the intent
    /// in persisted execution state; the next `sync` attributes the confirmed
    /// fill and emits the final `exchange_exit` ledger event.
    pub async fn request_manual_close(
        &mut self,
        symbol: &str,
        actor: &str,
        note: Option<&str>,
        requested_ms: i64,
    ) -> Result<ExchangeEvent> {
        let position = self
            .account
            .as_ref()
            .and_then(|account| account.positions.get(symbol))
            .cloned()
            .ok_or_else(|| anyhow!("{symbol} is not an open position"))?;
        let meta = self
            .state
            .positions
            .get(symbol)
            .cloned()
            .ok_or_else(|| anyhow!("{symbol} is not owned by this runtime"))?;
        if meta.exit_requested {
            return Err(anyhow!("{symbol} already has a close in progress"));
        }
        let current_notional_usd = position.quantity.abs() * position.mark_price;
        let entry_notional_usd = position.quantity.abs() * position.entry_price;
        let unrealized_pnl_pct = (entry_notional_usd > f64::EPSILON)
            .then_some(position.unrealized_pnl / entry_notional_usd);
        let mfe_pct =
            meta.side.sign() * (meta.extreme_price / meta.entry_price.max(f64::EPSILON) - 1.0);
        let mae_pct = (-meta.side.sign()
            * (meta.adverse_price / meta.entry_price.max(f64::EPSILON) - 1.0))
            .max(0.0);
        self.close_market(&position).await?;
        if let Some(state) = self.state.positions.get_mut(symbol) {
            state.exit_requested = true;
            state.pending_exit_reason = Some("operator_manual_close".into());
            state.pending_exit_actor = Some(actor.to_string());
            state.pending_exit_note = note.map(str::to_string);
        }
        self.save()?;
        Ok(ExchangeEvent {
            kind: "exchange_exit_requested".into(),
            payload: serde_json::json!({
                "ts_ms":requested_ms,
                "candidate_id":meta.candidate_id,
                "recipe":meta.recipe,
                "symbol":symbol,
                "side":position.side,
                "reason":"operator_manual_close",
                "operator_action":true,
                "exit_actor":actor,
                "operator_note":note,
                "entry_ms":meta.holding_started_ms(),
                "signal_ms":meta.signal_ms,
                "order_submitted_ms":meta.order_submitted_ms,
                "first_fill_ms":meta.first_fill_ms,
                "entry_completed_ms":meta.entry_completed_ms,
                "holding_time_source":meta.holding_time_source(),
                "entry_price":position.entry_price,
                "mark_price":position.mark_price,
                "quantity":position.quantity.abs(),
                "current_notional_usd":current_notional_usd,
                "unrealized_pnl_usd":position.unrealized_pnl,
                "unrealized_pnl_pct":unrealized_pnl_pct,
                "mfe_pct":mfe_pct,
                "mae_pct":mae_pct,
                "realized_before_close_usd":meta.cumulative_reported_pnl_usd,
                "hold_ms":requested_ms.saturating_sub(meta.holding_started_ms()),
                "protection":{
                    "stop_price":meta.stop_price,
                    "stop_reason":meta.stop_reason,
                    "take_profit_prices":meta.take_profit_prices,
                    "break_even_armed":meta.break_even_armed,
                    "runner_active":meta.runner_active,
                    "extreme_price":meta.extreme_price,
                },
                "venue":"binance_demo",
                "paper_only":true,
            }),
        })
    }

    async fn apply_exit_intents(
        &mut self,
        frame: &MarketFrame,
        evaluation: &GraphEvaluation,
    ) -> Vec<ExchangeEvent> {
        let intents: Vec<_> = evaluation
            .artifacts
            .values()
            .filter_map(|record| match &record.artifact {
                Artifact::PositionExitIntent(value) => Some(value.clone()),
                _ => None,
            })
            .collect();
        if intents.is_empty() {
            return Vec::new();
        }
        let positions: Vec<_> = self
            .account
            .as_ref()
            .into_iter()
            .flat_map(|account| account.positions.values())
            .filter(|position| {
                self.state
                    .positions
                    .get(&position.symbol)
                    .is_some_and(|meta| {
                        !meta.exit_requested
                            && intents.iter().any(|intent| {
                                meta.recipe == intent.recipe && meta.side == intent.side
                            })
                    })
            })
            .cloned()
            .collect();
        let mut events = Vec::new();
        let mut state_changed = false;
        for position in positions {
            let Some(intent) = intents.iter().find(|intent| {
                self.state
                    .positions
                    .get(&position.symbol)
                    .is_some_and(|meta| meta.recipe == intent.recipe && meta.side == intent.side)
            }) else {
                continue;
            };
            match self.close_market(&position).await {
                Ok(()) => {
                    if let Some(meta) = self.state.positions.get_mut(&position.symbol) {
                        meta.exit_requested = true;
                        meta.pending_exit_reason = Some(intent.reason.clone());
                        state_changed = true;
                    }
                    events.push(ExchangeEvent {
                        kind: "exchange_exit_requested".into(),
                        payload: serde_json::json!({"ts_ms":frame.as_of_ms,"symbol":position.symbol,"recipe":intent.recipe,"side":intent.side,"reason":intent.reason,"venue":"binance_demo"}),
                    });
                }
                Err(error) => events.push(ExchangeEvent {
                    kind: "exchange_order_rejected".into(),
                    payload: serde_json::json!({"ts_ms":frame.as_of_ms,"symbol":position.symbol,"recipe":intent.recipe,"side":intent.side,"reason":format!("breadth exit failed: {error}"),"venue":"binance_demo"}),
                }),
            }
        }
        if state_changed {
            self.save().ok();
        }
        events
    }

    async fn place_bracket(
        &self,
        plan: &greed_kernel::PositionPlan,
        size_multiplier: f64,
    ) -> Result<BracketExecution> {
        let rules = self
            .rules
            .get(&plan.symbol)
            .ok_or_else(|| anyhow!("missing exchange rules for {}", plan.symbol))?;
        let bounded_taker_ioc = plan
            .signal_context
            .get("bounded_taker_ioc")
            .is_some_and(|value| value == "true");
        let desired_quantity = floor_step(
            plan.notional_usd * size_multiplier / plan.reference_price,
            rules.quantity_step,
        );
        let quantity = cap_entry_quantity(
            desired_quantity,
            if bounded_taker_ioc {
                rules.max_limit_quantity
            } else {
                rules.max_market_quantity
            },
            rules.quantity_step,
        )?;
        if quantity < rules.min_quantity || quantity * plan.reference_price < rules.min_notional {
            return Err(anyhow!(
                "order is below Binance quantity or notional minimum"
            ));
        }
        if plan.take_profit_prices.is_empty()
            && !plan.fixed_time_exit
            && !plan_has_managed_exit(plan)
        {
            return Err(anyhow!("position plan requires at least one take profit"));
        }
        let take_profit_fraction = plan
            .take_profit_prices
            .iter()
            .map(|(_, fraction)| *fraction)
            .sum::<f64>();
        if take_profit_fraction > 1.0 + 1e-6 {
            return Err(anyhow!("staged take-profit fractions exceed the position"));
        }
        if let Some(runner_fraction) = plan.unprotected_runner_fraction {
            if (take_profit_fraction + runner_fraction - 1.0).abs() > 1e-6 {
                return Err(anyhow!(
                    "take-profit fractions and tail runner must sum to one"
                ));
            }
        }
        for (_, fraction) in &plan.take_profit_prices {
            if *fraction >= 1.0 - f64::EPSILON {
                continue;
            }
            let partial = floor_step(quantity * fraction, rules.quantity_step);
            if partial < rules.min_quantity || partial * plan.reference_price < rules.min_notional {
                return Err(anyhow!(
                    "staged take profit is below Binance quantity or notional minimum"
                ));
            }
        }
        // Re-check live microstructure immediately before any exchange-side
        // mutation. Candle evaluation and order submission are asynchronous;
        // without this final gate a valid close can turn into a stale market
        // entry while the request is being prepared.
        match self.pending_entry_guard(plan, chrono::Utc::now().timestamp_millis()) {
            // A single 10-second flow sample is too noisy to veto a completed
            // 15-minute setup. The pending-order loop below requires the
            // reversal to persist before it cancels the passive order.
            EntryGuardState::Healthy | EntryGuardState::MicroReversal(_) => {}
            EntryGuardState::StructuralInvalidation(reason) => {
                return Err(anyhow!(
                    "entry signal invalidated before submission: {reason}"
                ));
            }
            EntryGuardState::Unavailable(reason) => {
                return Err(anyhow!(
                    "entry guard unavailable before submission: {reason}"
                ));
            }
        }
        self.prepare_symbol_for_entry(&plan.symbol).await?;
        self.ensure_isolated_margin(&plan.symbol).await?;
        self.signed(
            Method::POST,
            "/fapi/v1/leverage",
            vec![
                ("symbol".into(), plan.symbol.clone()),
                ("leverage".into(), self.config.leverage.to_string()),
            ],
        )
        .await
        .with_context(|| format!("set {} leverage to {}x", plan.symbol, self.config.leverage))?;
        if plan.entry_limit.is_some() {
            return Err(anyhow!(
                "maker plans must be advanced by the persistent pending-entry state machine"
            ));
        }
        let execution_limit = if bounded_taker_ioc {
            let quote = self
                .public_get_params(
                    "/fapi/v1/ticker/bookTicker",
                    &[("symbol", plan.symbol.as_str())],
                )
                .await
                .with_context(|| format!("read {} execution-venue quote", plan.symbol))?;
            let executable_quote = match plan.side {
                Side::Buy => parse_f64(&quote, "askPrice"),
                Side::Sell => parse_f64(&quote, "bidPrice"),
            }
            .filter(|value| value.is_finite() && *value > f64::EPSILON)
            .ok_or_else(|| {
                anyhow!(
                    "{} execution-venue quote is missing or invalid",
                    plan.symbol
                )
            })?;
            let limit = bounded_taker_ioc_price(
                plan.side,
                plan.reference_price,
                executable_quote,
                plan.max_entry_adverse_bps,
                rules.price_tick,
            )?;
            Some(limit)
        } else {
            None
        };
        let client_id = client_order_id("entry", &plan.candidate_id);
        let order_submitted_ms = chrono::Utc::now().timestamp_millis();
        let mut entry_parameters = vec![
            ("symbol".into(), plan.symbol.clone()),
            ("side".into(), side_name(plan.side).into()),
            ("quantity".into(), decimal(quantity, rules.quantity_step)),
            ("newClientOrderId".into(), client_id.clone()),
            ("newOrderRespType".into(), "RESULT".into()),
        ];
        if let Some(limit) = execution_limit {
            entry_parameters.extend([
                ("type".into(), "LIMIT".into()),
                ("timeInForce".into(), "IOC".into()),
                ("price".into(), decimal(limit, rules.price_tick)),
            ]);
        } else {
            entry_parameters.push(("type".into(), "MARKET".into()));
        }
        let entry = EntryExecution {
            order: self
                .submit_or_lookup(&plan.symbol, &client_id, entry_parameters)
                .await?,
            requested_quantity: quantity,
            mode: if bounded_taker_ioc {
                "bounded_taker_ioc"
            } else {
                "taker_market"
            },
            maker_attempted: false,
            maker_wait_ms: 0,
            maker_reprices: 0,
            final_maker_limit: execution_limit,
            size_multiplier: 1.0,
        };
        let mut executed =
            parse_f64(&entry.order, "executedQty").unwrap_or(entry.requested_quantity);
        if executed <= f64::EPSILON {
            return Err(anyhow!("entry order completed without a fill"));
        }
        let fill_ratio = executed / entry.requested_quantity.max(f64::EPSILON);
        let manageable_partial = partial_fill_is_manageable(
            executed,
            entry.requested_quantity,
            entry_price_hint(&entry.order, plan.reference_price),
            plan.min_managed_fill_ratio,
            rules,
            &plan.take_profit_prices,
        );
        if plan.min_fill_ratio > 0.0
            && fill_ratio + f64::EPSILON < plan.min_fill_ratio
            && !manageable_partial
        {
            let close_id = client_order_id("underfill", &plan.candidate_id);
            let close = self
                .submit_or_lookup(
                    &plan.symbol,
                    &close_id,
                    vec![
                        ("symbol".into(), plan.symbol.clone()),
                        ("side".into(), side_name(plan.side.opposite()).into()),
                        ("type".into(), "MARKET".into()),
                        ("quantity".into(), decimal(executed, rules.quantity_step)),
                        ("reduceOnly".into(), "true".into()),
                        ("newClientOrderId".into(), close_id.clone()),
                        ("newOrderRespType".into(), "RESULT".into()),
                    ],
                )
                .await;
            match close {
                Ok(close) => {
                    let closed = parse_f64(&close, "executedQty").unwrap_or_default();
                    if closed + rules.quantity_step * 0.5 >= executed {
                        return Err(anyhow!(
                            "maker fill ratio {:.1}% was below the manageable minimum {:.1}% or failed exchange-size validation; incidental fill was flattened",
                            fill_ratio * 100.0,
                            plan.min_managed_fill_ratio * 100.0
                        ));
                    }
                    executed = (executed - closed).max(0.0);
                    tracing::error!(
                        symbol = %plan.symbol,
                        filled_quantity = executed + closed,
                        closed_quantity = closed,
                        residual_quantity = executed,
                        "undersized maker fill was only partially flattened; protecting the residual"
                    );
                }
                Err(error) => tracing::error!(
                    symbol = %plan.symbol,
                    filled_quantity = executed,
                    error = %error,
                    "undersized maker fill could not be flattened; protecting it instead"
                ),
            }
        }
        let order_id = entry.order["orderId"].as_i64().unwrap_or_default();
        let entry_completed_ms = chrono::Utc::now().timestamp_millis();
        let (entry_price, entry_price_source) = self
            .resolve_entry_price(&plan.symbol, order_id, &entry.order, plan.side)
            .await
            .unwrap_or((plan.reference_price, "strategy_reference_fallback"));
        if bounded_taker_ioc {
            let adverse_bps = entry_adverse_bps(plan.side, entry_price, plan.reference_price);
            let tick_tolerance_bps =
                rules.price_tick / plan.reference_price.max(f64::EPSILON) * 10_000.0 + 0.1;
            if adverse_bps > plan.max_entry_adverse_bps + tick_tolerance_bps {
                let close_id = client_order_id("priceguard", &plan.candidate_id);
                self.flatten_entry_or_confirm_flat(
                    &plan.symbol,
                    plan.side,
                    executed,
                    rules,
                    &close_id,
                )
                .await
                .with_context(|| {
                    format!(
                        "bounded IOC filled {adverse_bps:.1} bps beyond the strategy reference and emergency close failed"
                    )
                })?;
                return Err(anyhow!(
                    "bounded IOC execution invariant failed: fill was {adverse_bps:.1} bps adverse (max {:.1}); entry was immediately closed",
                    plan.max_entry_adverse_bps
                ));
            }
        }
        let (first_fill_ms, entry_time_source) = self
            .resolve_first_fill_time(
                &plan.symbol,
                order_id,
                &entry.order,
                plan.side,
                entry_completed_ms,
            )
            .await;
        // Preserve the planned risk/reward distances from the actual exchange
        // fill. A fast market can move between signal construction and fill;
        // anchoring protection to the stale reference would silently change
        // both the dollar risk and the take-profit geometry.
        let sign = plan.side.sign();
        let stop_distance = sign * (plan.reference_price - plan.stop_price)
            / plan.reference_price.max(f64::EPSILON);
        let stop_price = entry_price * (1.0 - sign * stop_distance);
        let take_profit_prices = take_profit_prices_from_fill(plan, entry_price, executed);
        let protective = async {
            let stop_algo_id = self
                .place_close_all_trigger(
                    &plan.symbol,
                    plan.side.opposite(),
                    "STOP_MARKET",
                    stop_price,
                    rules,
                    client_order_id("stop", &plan.candidate_id),
                )
                .await?;
            let mut take_profit_order_ids = Vec::new();
            for (index, (take_profit, fraction)) in take_profit_prices.iter().enumerate() {
                take_profit_order_ids.push(
                    self.place_reduce_only_take_profit(
                        &plan.symbol,
                        plan.side.opposite(),
                        *take_profit,
                        floor_step(executed * fraction, rules.quantity_step),
                        rules,
                        client_order_id(&format!("take{index}"), &plan.candidate_id),
                    )
                    .await?,
                );
            }
            Result::<(i64, Vec<i64>)>::Ok((stop_algo_id, take_profit_order_ids))
        }
        .await;
        let (stop_algo_id, take_profit_order_ids) = match protective {
            Ok(value) => value,
            Err(error) => {
                // Preserve the conditional stop while removing any staged
                // regular targets and flattening the entry. This guarantees a
                // protection API failure cannot turn into a naked position if
                // the emergency market request itself times out.
                self.cancel_regular_orders(&plan.symbol).await.ok();
                let close_id = client_order_id("protectfail", &plan.candidate_id);
                self.flatten_entry_or_confirm_flat(
                    &plan.symbol,
                    plan.side,
                    executed,
                    rules,
                    &close_id,
                )
                .await
                .with_context(|| {
                    format!("protective order failed ({error}); emergency close also failed")
                })?;
                self.cancel_all(&plan.symbol).await.ok();
                return Err(anyhow!(
                    "protective order failed; entry was immediately closed: {error}"
                ));
            }
        };
        Ok(BracketExecution {
            entry_price,
            quantity: executed,
            order_id,
            stop_price,
            take_profit_prices,
            stop_algo_id,
            take_profit_order_ids,
            entry_mode: entry.mode,
            entry_price_source,
            maker_attempted: entry.maker_attempted,
            maker_wait_ms: entry.maker_wait_ms,
            maker_reprices: entry.maker_reprices,
            final_maker_limit: entry.final_maker_limit,
            size_multiplier: entry.size_multiplier,
            order_submitted_ms,
            first_fill_ms,
            entry_completed_ms,
            entry_time_source,
        })
    }

    async fn prepare_symbol_for_entry(&self, symbol: &str) -> Result<()> {
        let regular = self
            .signed(
                Method::GET,
                "/fapi/v1/openOrders",
                vec![("symbol".into(), symbol.into())],
            )
            .await?;
        for order in regular
            .as_array()
            .ok_or_else(|| anyhow!("openOrders response is not an array"))?
        {
            let client_id = order["clientOrderId"].as_str().unwrap_or_default();
            if !client_id.starts_with("greed-") {
                return Err(anyhow!(
                    "{symbol} has a foreign open order; refusing to open a strategy position"
                ));
            }
            self.signed(
                Method::DELETE,
                "/fapi/v1/order",
                vec![
                    ("symbol".into(), symbol.into()),
                    ("orderId".into(), order["orderId"].to_string()),
                ],
            )
            .await
            .with_context(|| format!("cancel orphaned greed order on {symbol}"))?;
        }

        let algo = self
            .signed(
                Method::GET,
                "/fapi/v1/openAlgoOrders",
                vec![
                    ("algoType".into(), "CONDITIONAL".into()),
                    ("symbol".into(), symbol.into()),
                ],
            )
            .await?;
        for order in algo
            .as_array()
            .ok_or_else(|| anyhow!("openAlgoOrders response is not an array"))?
        {
            let client_id = order["clientAlgoId"].as_str().unwrap_or_default();
            if !client_id.starts_with("greed-") {
                return Err(anyhow!(
                    "{symbol} has a foreign conditional order; refusing to open a strategy position"
                ));
            }
            self.signed(
                Method::DELETE,
                "/fapi/v1/algoOrder",
                vec![("algoId".into(), order["algoId"].to_string())],
            )
            .await
            .with_context(|| format!("cancel orphaned greed conditional order on {symbol}"))?;
        }
        Ok(())
    }

    async fn ensure_isolated_margin(&self, symbol: &str) -> Result<()> {
        match self
            .signed(
                Method::POST,
                "/fapi/v1/marginType",
                vec![
                    ("symbol".into(), symbol.into()),
                    ("marginType".into(), "ISOLATED".into()),
                ],
            )
            .await
        {
            Ok(_) => Ok(()),
            // Binance reports "No need to change margin type" when the
            // symbol is already isolated. Treat that idempotent response as
            // success; every other response remains a hard entry failure.
            Err(error) if error.to_string().contains("-4046") => Ok(()),
            Err(error) => Err(error)
                .with_context(|| format!("could not guarantee isolated margin for {symbol}")),
        }
    }

    async fn resolve_entry_price(
        &self,
        symbol: &str,
        order_id: i64,
        order: &Value,
        side: Side,
    ) -> Result<(f64, &'static str)> {
        if let Some(price) = order_average_price(order) {
            return Ok((price, "order_response"));
        }
        if order_id > 0 {
            if let Ok(reconciled) = self
                .signed(
                    Method::GET,
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), symbol.into()),
                        ("orderId".into(), order_id.to_string()),
                    ],
                )
                .await
            {
                if let Some(price) = order_average_price(&reconciled) {
                    return Ok((price, "order_reconciliation"));
                }
            }
            for _ in 0..3 {
                let trades = self
                    .signed(
                        Method::GET,
                        "/fapi/v1/userTrades",
                        vec![
                            ("symbol".into(), symbol.into()),
                            ("orderId".into(), order_id.to_string()),
                            ("limit".into(), "1000".into()),
                        ],
                    )
                    .await?;
                if let Some(rows) = trades.as_array() {
                    if let Some(price) = average_fills(rows, side_name(side)) {
                        return Ok((price, "user_trades"));
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Err(anyhow!(
            "Binance did not expose a verifiable entry fill price"
        ))
    }

    async fn resolve_first_fill_time(
        &self,
        symbol: &str,
        order_id: i64,
        order: &Value,
        side: Side,
        locally_observed_ms: i64,
    ) -> (i64, &'static str) {
        if order_id > 0 {
            for _ in 0..3 {
                if let Ok(trades) = self
                    .signed(
                        Method::GET,
                        "/fapi/v1/userTrades",
                        vec![
                            ("symbol".into(), symbol.into()),
                            ("orderId".into(), order_id.to_string()),
                            ("limit".into(), "1000".into()),
                        ],
                    )
                    .await
                {
                    let first = trades.as_array().and_then(|rows| {
                        rows.iter()
                            .filter(|row| {
                                row["side"].as_str().is_some_and(|value| {
                                    value.eq_ignore_ascii_case(side_name(side))
                                })
                            })
                            .filter_map(|row| row["time"].as_i64())
                            .min()
                    });
                    if let Some(first_fill_ms) = first {
                        return (first_fill_ms, "exchange_user_trade_time");
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        // `updateTime` is an order lifecycle timestamp and may represent the
        // final fill, so it is deliberately not relabelled as the first fill.
        // The local observation is honest and safe for the holding clock.
        let _ = order;
        (locally_observed_ms, "local_first_fill_observation")
    }

    async fn attribute_exit(
        &self,
        symbol: &str,
        meta: &ExecutionMeta,
        summary: Option<&TradeSummary>,
    ) -> (String, Option<i64>, Option<String>) {
        if let Some(reason) = meta.pending_exit_reason.clone() {
            return (reason, None, Some("requested".into()));
        }
        if let Some(algo_id) = meta.stop_algo_id {
            if let Ok(order) = self
                .signed(
                    Method::GET,
                    "/fapi/v1/algoOrder",
                    vec![("algoId".into(), algo_id.to_string())],
                )
                .await
            {
                let status = order["algoStatus"]
                    .as_str()
                    .or_else(|| order["status"].as_str())
                    .unwrap_or_default()
                    .to_string();
                if matches!(status.as_str(), "FINISHED" | "TRIGGERED" | "FILLED") {
                    let reason = protective_exit_reason(meta);
                    return (reason.into(), Some(algo_id), Some(status));
                }
            }
        }
        let full_take_profit = meta
            .take_profit_prices
            .iter()
            .map(|(_, fraction)| *fraction)
            .sum::<f64>()
            >= 1.0 - 1e-6;
        if full_take_profit {
            for order_id in &meta.take_profit_order_ids {
                if let Ok(order) = self
                    .signed(
                        Method::GET,
                        "/fapi/v1/order",
                        vec![
                            ("symbol".into(), symbol.into()),
                            ("orderId".into(), order_id.to_string()),
                        ],
                    )
                    .await
                {
                    let status = order["status"].as_str().unwrap_or_default().to_string();
                    if status == "FILLED" {
                        return ("take_profit".into(), Some(*order_id), Some(status));
                    }
                }
            }
        }
        // Binance Demo can remove a finished algo order before the next
        // reconciliation query exposes it. The fills remain authoritative.
        // If the last exit fill is at/through the active stop (with a small
        // allowance for stop-market slippage), preserve the strategy reason
        // instead of incorrectly labelling it as a manual close.
        if let Some(summary) = summary
            .filter(|value| exit_matches_stop(meta.side, meta.stop_price, value.last_exit_price))
        {
            return (
                protective_exit_reason(meta).into(),
                summary.last_exit_order_id,
                Some("INFERRED_FROM_EXIT_FILL".into()),
            );
        }
        ("external_or_manual_close".into(), None, None)
    }

    fn pending_entry_guard(
        &self,
        plan: &greed_kernel::PositionPlan,
        now_ms: i64,
    ) -> EntryGuardState {
        if plan.entry_invalidation_bps <= 0.0
            && plan.entry_guard_max_opposing_flow <= 0.0
            && plan.entry_guard_max_opposing_return_bps <= 0.0
        {
            return EntryGuardState::Healthy;
        }
        let Some(stream) = &self.market_stream else {
            return EntryGuardState::Unavailable("mainnet websocket guard is unavailable".into());
        };
        let Some(book) = stream.book(&plan.symbol, now_ms) else {
            return EntryGuardState::Unavailable(format!(
                "mainnet order book is missing for {}",
                plan.symbol
            ));
        };
        if now_ms > book.meta.expires_ms || book.bid <= 0.0 || book.ask <= 0.0 {
            return EntryGuardState::Unavailable(format!(
                "mainnet order book is stale for {}",
                plan.symbol
            ));
        }
        let signal_mid = (book.bid + book.ask) * 0.5;
        // A quiet symbol may legitimately have no aggregate trade for longer
        // than STREAM_TTL_MS even while its book websocket is healthy. Missing
        // microstructure therefore disables only the optional flow veto; it
        // must not cancel an already-resting maker order.
        let micro = stream
            .microstructure(&plan.symbol, now_ms)
            .filter(|value| now_ms <= value.meta.expires_ms);
        pending_entry_guard_state(
            plan,
            signal_mid,
            micro.as_ref().and_then(|value| value.trade_imbalance()),
            micro.as_ref().and_then(|value| value.mid_return_bps_10s),
        )
    }

    async fn cancel_or_reconcile_entry(&self, symbol: &str, client_id: &str) -> Result<Value> {
        let mut last_error = None;
        for attempt in 0..3 {
            let cancel_detail = match self
                .signed(
                    Method::DELETE,
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), symbol.into()),
                        ("origClientOrderId".into(), client_id.into()),
                    ],
                )
                .await
            {
                Ok(value) if order_value_is_terminal(&value) => return Ok(value),
                Ok(value) => format!(
                    "cancel response status {}",
                    value["status"].as_str().unwrap_or("UNKNOWN")
                ),
                Err(error) => format!("cancel request failed: {error:#}"),
            };

            match self
                .signed_read(
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), symbol.into()),
                        ("origClientOrderId".into(), client_id.into()),
                    ],
                )
                .await
            {
                Ok(value) if order_value_is_terminal(&value) => return Ok(value),
                Ok(value) => {
                    last_error = Some(anyhow!(
                        "{cancel_detail}; order remains active after cancel attempt {} with status {}",
                        attempt + 1,
                        value["status"].as_str().unwrap_or("UNKNOWN")
                    ));
                }
                Err(error) => {
                    last_error = Some(anyhow!(
                        "{cancel_detail}; cancellation reconciliation failed: {error:#}"
                    ));
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(last_error.unwrap_or_else(|| anyhow!("entry cancellation could not be verified")))
            .with_context(|| format!("could not freeze pending entry {client_id} on {symbol}"))
    }

    async fn place_close_all_trigger(
        &self,
        symbol: &str,
        side: Side,
        kind: &str,
        trigger: f64,
        rules: &SymbolRules,
        client_id: String,
    ) -> Result<i64> {
        let order = self
            .submit_algo_or_lookup(
                &client_id,
                vec![
                    ("algoType".into(), "CONDITIONAL".into()),
                    ("symbol".into(), symbol.into()),
                    ("side".into(), side_name(side).into()),
                    ("type".into(), kind.into()),
                    (
                        "triggerPrice".into(),
                        decimal(round_step(trigger, rules.price_tick), rules.price_tick),
                    ),
                    ("closePosition".into(), "true".into()),
                    ("workingType".into(), "MARK_PRICE".into()),
                    // MARK_PRICE remains the trigger source. Disabling the
                    // additional divergence filter prevents a hard stop from
                    // waiting through an altcoin gap.
                    ("priceProtect".into(), "false".into()),
                    ("clientAlgoId".into(), client_id.clone()),
                ],
            )
            .await?;
        order["algoId"]
            .as_i64()
            .ok_or_else(|| anyhow!("conditional protection response is missing algoId"))
    }

    async fn place_reduce_only_take_profit(
        &self,
        symbol: &str,
        side: Side,
        target: f64,
        quantity: f64,
        rules: &SymbolRules,
        client_id: String,
    ) -> Result<i64> {
        let price = if side == Side::Buy {
            floor_step(target, rules.price_tick)
        } else {
            ceil_step(target, rules.price_tick)
        };
        let order = self
            .submit_or_lookup(
                symbol,
                &client_id,
                vec![
                    ("symbol".into(), symbol.into()),
                    ("side".into(), side_name(side).into()),
                    ("type".into(), "LIMIT".into()),
                    // A target submitted before price reaches it rests and earns maker
                    // liquidity. GTC intentionally permits an immediate profitable
                    // fill if price crosses the target while the stop is being placed;
                    // strict GTX would reject that race and turn a winning move into a
                    // protection failure plus emergency close.
                    ("timeInForce".into(), "GTC".into()),
                    ("price".into(), decimal(price, rules.price_tick)),
                    ("quantity".into(), decimal(quantity, rules.quantity_step)),
                    ("reduceOnly".into(), "true".into()),
                    ("newClientOrderId".into(), client_id.clone()),
                    ("newOrderRespType".into(), "ACK".into()),
                ],
            )
            .await?;
        order["orderId"]
            .as_i64()
            .ok_or_else(|| anyhow!("take-profit response is missing orderId"))
    }

    async fn submit_or_lookup(
        &self,
        symbol: &str,
        client_id: &str,
        parameters: Vec<(String, String)>,
    ) -> Result<Value> {
        match self
            .signed(Method::POST, "/fapi/v1/order", parameters)
            .await
        {
            Ok(value) => Ok(value),
            Err(submit_error) => {
                if is_post_only_rejection(&submit_error) {
                    return Err(submit_error);
                }
                let mut lookup_error = None;
                for _ in 0..3 {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    match self
                        .signed(
                            Method::GET,
                            "/fapi/v1/order",
                            vec![
                                ("symbol".into(), symbol.into()),
                                ("origClientOrderId".into(), client_id.into()),
                            ],
                        )
                        .await
                    {
                        Ok(value) => return Ok(value),
                        Err(error) => lookup_error = Some(error),
                    }
                }
                Err(lookup_error.unwrap_or_else(|| anyhow!("order lookup failed"))).with_context(
                    || {
                        format!(
                            "order submission failed and deterministic reconciliation found no order: {submit_error}"
                        )
                    },
                )
            }
        }
    }

    async fn submit_algo_or_lookup(
        &self,
        client_id: &str,
        parameters: Vec<(String, String)>,
    ) -> Result<Value> {
        match self
            .signed(Method::POST, "/fapi/v1/algoOrder", parameters)
            .await
        {
            Ok(value) => Ok(value),
            Err(submit_error) => {
                let mut lookup_error = None;
                for _ in 0..3 {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    match self
                        .signed(
                            Method::GET,
                            "/fapi/v1/algoOrder",
                            vec![("clientAlgoId".into(), client_id.into())],
                        )
                        .await
                    {
                        Ok(value) => return Ok(value),
                        Err(error) => lookup_error = Some(error),
                    }
                }
                Err(lookup_error.unwrap_or_else(|| anyhow!("algo order lookup failed")))
                    .with_context(|| {
                        format!(
                            "algo order submission failed and deterministic reconciliation found no order: {submit_error}"
                        )
                    })
            }
        }
    }

    async fn cancel_all(&self, symbol: &str) -> Result<()> {
        let regular = self.cancel_regular_orders(symbol).await;
        let algo = self
            .signed(
                Method::DELETE,
                "/fapi/v1/algoOpenOrders",
                vec![("symbol".into(), symbol.into())],
            )
            .await;
        regular.context("cancel regular open orders")?;
        algo.context("cancel conditional algo orders")?;
        Ok(())
    }

    async fn cancel_regular_orders(&self, symbol: &str) -> Result<()> {
        self.signed(
            Method::DELETE,
            "/fapi/v1/allOpenOrders",
            vec![("symbol".into(), symbol.into())],
        )
        .await?;
        Ok(())
    }

    async fn cancel_protective_order(
        &self,
        symbol: &str,
        order_id: ProtectiveOrderId,
    ) -> Result<()> {
        match order_id {
            ProtectiveOrderId::Standard(order_id) => {
                self.signed(
                    Method::DELETE,
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), symbol.into()),
                        ("orderId".into(), order_id.to_string()),
                    ],
                )
                .await?;
            }
            ProtectiveOrderId::Algo(algo_id) => {
                self.signed(
                    Method::DELETE,
                    "/fapi/v1/algoOrder",
                    vec![("algoId".into(), algo_id.to_string())],
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn trade_summary(
        &self,
        symbol: &str,
        start_ms: i64,
        position_side: Side,
    ) -> Result<TradeSummary> {
        let value = self
            .signed_read(
                "/fapi/v1/userTrades",
                vec![
                    ("symbol".into(), symbol.into()),
                    ("startTime".into(), start_ms.to_string()),
                    ("limit".into(), "1000".into()),
                ],
            )
            .await?;
        let rows = value
            .as_array()
            .ok_or_else(|| anyhow!("userTrades response is not an array"))?;
        Ok(summarize_trades(rows, position_side))
    }

    async fn trade_summary_after_close(
        &self,
        symbol: &str,
        start_ms: i64,
        position_side: Side,
        expected_exit_quantity: f64,
    ) -> Result<TradeSummary> {
        let mut last = None;
        for delay_ms in [0, 100, 250, 500] {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            let summary = self.trade_summary(symbol, start_ms, position_side).await?;
            if summary.entry_quantity > f64::EPSILON
                && summary.exit_quantity + 1e-9 >= expected_exit_quantity
            {
                return Ok(summary);
            }
            last = Some(summary);
        }
        Err(anyhow!(
            "userTrades did not expose the complete filled-attempt round trip; latest entry={} exit={} expected_exit={expected_exit_quantity}",
            last.as_ref().map(|value| value.entry_quantity).unwrap_or_default(),
            last.as_ref().map(|value| value.exit_quantity).unwrap_or_default(),
        ))
    }

    async fn close_market(&self, position: &RemotePosition) -> Result<()> {
        let rules = self
            .rules
            .get(&position.symbol)
            .ok_or_else(|| anyhow!("missing exchange rules for {}", position.symbol))?;
        // Cancel regular take-profits first so they cannot race the requested
        // full close, but retain the conditional stop until Binance has
        // definitely accepted the reduce-only market order.
        self.cancel_regular_orders(&position.symbol).await?;
        let close_cycle_ms = chrono::Utc::now().timestamp_millis();
        for sequence in 0..128_u16 {
            let latest = self.signed_read("/fapi/v2/account", vec![]).await?;
            let latest_positions = parse_account(&latest)?.positions;
            let Some(latest_position) = latest_positions.get(&position.symbol) else {
                self.cancel_all(&position.symbol).await.ok();
                return Ok(());
            };
            if latest_position.side != position.side {
                return Err(anyhow!(
                    "{} changed from {:?} to {:?} while its close was prepared",
                    position.symbol,
                    position.side,
                    latest_position.side
                ));
            }
            let before_quantity = latest_position.quantity.abs();
            let close_quantity = market_close_chunk(before_quantity, rules)?;
            // Include the observed remaining quantity in the deterministic id.
            // An ambiguous response can therefore be looked up safely, while a
            // genuinely reduced remainder receives a distinct child order.
            let close_id = client_order_id(
                "close",
                &format!(
                    "{}:{:?}:{close_cycle_ms}:{sequence}:{before_quantity:.12}",
                    position.symbol, latest_position.side
                ),
            );
            let submission = self
                .submit_or_lookup(
                    &position.symbol,
                    &close_id,
                    vec![
                        ("symbol".into(), position.symbol.clone()),
                        ("side".into(), side_name(position.side.opposite()).into()),
                        ("type".into(), "MARKET".into()),
                        (
                            "quantity".into(),
                            decimal(close_quantity, rules.quantity_step),
                        ),
                        ("reduceOnly".into(), "true".into()),
                        ("newClientOrderId".into(), close_id.clone()),
                        ("newOrderRespType".into(), "RESULT".into()),
                    ],
                )
                .await;
            if let Err(error) = submission {
                // The hosted stop may have filled while this child close was
                // submitted. Reconcile once before declaring the close failed.
                let refreshed = self.signed_read("/fapi/v2/account", vec![]).await?;
                let refreshed_positions = parse_account(&refreshed)?.positions;
                match refreshed_positions.get(&position.symbol) {
                    None => {
                        self.cancel_all(&position.symbol).await.ok();
                        return Ok(());
                    }
                    Some(current)
                        if current.side == position.side
                            && current.quantity.abs()
                                < before_quantity - rules.quantity_step * 0.5 =>
                    {
                        continue;
                    }
                    _ => return Err(error),
                }
            }

            let mut reduced = false;
            for delay_ms in [0, 50, 150, 300, 600] {
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                let confirmed = self.signed_read("/fapi/v2/account", vec![]).await?;
                let confirmed_positions = parse_account(&confirmed)?.positions;
                match confirmed_positions.get(&position.symbol) {
                    None => {
                        self.cancel_all(&position.symbol).await.ok();
                        return Ok(());
                    }
                    Some(current)
                        if current.side == position.side
                            && current.quantity.abs()
                                < before_quantity - rules.quantity_step * 0.5 =>
                    {
                        reduced = true;
                        break;
                    }
                    Some(current) if current.side != position.side => {
                        return Err(anyhow!(
                            "{} reversed while a reduce-only close was reconciled",
                            position.symbol
                        ));
                    }
                    _ => {}
                }
            }
            if !reduced {
                return Err(anyhow!(
                    "{} close child accepted but quantity reduction was not observed; hosted stop retained",
                    position.symbol
                ));
            }
        }
        Err(anyhow!(
            "{} required more than 128 market-close chunks; hosted stop retained",
            position.symbol
        ))
    }

    async fn flatten_entry_or_confirm_flat(
        &self,
        symbol: &str,
        side: Side,
        fallback_quantity: f64,
        rules: &SymbolRules,
        client_id: &str,
    ) -> Result<()> {
        let latest = self.signed_read("/fapi/v2/account", vec![]).await?;
        let positions = parse_account(&latest)?.positions;
        let quantity = match positions.get(symbol) {
            Some(position) if position.side == side => {
                return self.close_market(position).await;
            }
            Some(position) => {
                return Err(anyhow!(
                    "{symbol} changed from {side:?} to {:?}; refusing to flatten an operator-owned reverse position",
                    position.side
                ));
            }
            // Account snapshots can briefly lag a just-returned fill. Submit
            // the known executed amount; an exchange-confirmed flat state is
            // accepted below if Binance rejects the redundant reduce-only order.
            None => fallback_quantity,
        };
        match self
            .flatten_entry_quantity(symbol, side, quantity, rules, client_id)
            .await
        {
            Ok(_) => Ok(()),
            Err(close_error) => {
                let refreshed = self.signed_read("/fapi/v2/account", vec![]).await?;
                let refreshed_positions = parse_account(&refreshed)?.positions;
                if refreshed_positions.contains_key(symbol) {
                    Err(close_error)
                        .context("reduce-only close failed and the position remains open")
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn flatten_entry_quantity(
        &self,
        symbol: &str,
        side: Side,
        quantity: f64,
        rules: &SymbolRules,
        client_id: &str,
    ) -> Result<Value> {
        let mut remaining = quantity;
        let mut last = Value::Null;
        for sequence in 0..128_u16 {
            if remaining <= rules.quantity_step * 0.5 {
                return Ok(last);
            }
            let chunk = market_close_chunk(remaining, rules)?;
            let child_id = client_order_id("attemptclose", &format!("{client_id}:{sequence}"));
            last = self
                .submit_or_lookup(
                    symbol,
                    &child_id,
                    vec![
                        ("symbol".into(), symbol.into()),
                        ("side".into(), side_name(side.opposite()).into()),
                        ("type".into(), "MARKET".into()),
                        ("quantity".into(), decimal(chunk, rules.quantity_step)),
                        ("reduceOnly".into(), "true".into()),
                        ("newClientOrderId".into(), child_id.clone()),
                        ("newOrderRespType".into(), "RESULT".into()),
                    ],
                )
                .await?;
            remaining = (remaining - chunk).max(0.0);
        }
        Err(anyhow!(
            "{symbol} emergency close required more than 128 market chunks"
        ))
    }

    fn save(&self) -> Result<()> {
        let path = Path::new(&self.state_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("tmp");
        fs::write(&temporary, serde_json::to_vec_pretty(&self.state)?)?;
        fs::rename(temporary, path)?;
        Ok(())
    }

    pub fn health(&self) -> Value {
        serde_json::json!({
            "mode": "binance_demo",
            "venue": "binance_demo",
            "authenticated": self.account.is_some(),
            "last_sync_ms": self.last_sync_ms,
            "last_error": self.state.execution_halt_reason.as_ref().or(self.last_error.as_ref()),
            "execution_halted": self.state.execution_halt_reason.is_some(),
            "remote_matching": true,
            "leverage": self.config.leverage,
            "initial_equity_usd":self.portfolio.initial_equity_usd,
            "performance_epoch": self.state.performance_epoch,
            "performance_epoch_reset": self.performance_epoch_reset,
            "performance_basis":"risk_r_v1",
            "performance_basis_reset":self.performance_basis_reset,
            "liquidation_execution_basis_version":self.state.liquidation_execution_basis_version,
            "pending_entries":self.state.pending_entries.len(),
            "pending_accounting":self.state.pending_accounting.len(),
            "recipe_execution_counts":self.state.recipe_execution_counts,
        })
    }
}

fn summarize_trades(rows: &[Value], position_side: Side) -> TradeSummary {
    let mut entry_notional = 0.0;
    let mut entry_quantity = 0.0;
    let mut exit_notional = 0.0;
    let mut exit_quantity = 0.0;
    let mut fees = 0.0;
    let mut realized = 0.0;
    let mut maker_fills = 0usize;
    let mut taker_fills = 0usize;
    let mut maker_notional = 0.0;
    let mut taker_notional = 0.0;
    let mut exit_orders: BTreeMap<i64, (i64, f64, f64)> = BTreeMap::new();
    let exit_side = match position_side {
        Side::Buy => "SELL",
        Side::Sell => "BUY",
    };
    let entry_side = side_name(position_side);
    for row in rows {
        let pnl = parse_f64(row, "realizedPnl").unwrap_or(0.0);
        let quantity = parse_f64(row, "qty").unwrap_or(0.0);
        let price = parse_f64(row, "price").unwrap_or(0.0);
        if row["maker"].as_bool().unwrap_or(false) {
            maker_fills += 1;
            maker_notional += price * quantity;
        } else {
            taker_fills += 1;
            taker_notional += price * quantity;
        }
        fees += parse_f64(row, "commission").unwrap_or(0.0);
        realized += pnl;
        if row["side"]
            .as_str()
            .is_some_and(|value| value.eq_ignore_ascii_case(entry_side))
        {
            entry_quantity += quantity;
            entry_notional += price * quantity;
        }
        if row["side"]
            .as_str()
            .is_some_and(|value| value.eq_ignore_ascii_case(exit_side))
        {
            exit_quantity += quantity;
            exit_notional += price * quantity;
            let order_id = row["orderId"].as_i64().unwrap_or_default();
            let entry = exit_orders.entry(order_id).or_default();
            entry.0 = entry.0.max(row["time"].as_i64().unwrap_or_default());
            entry.1 += price * quantity;
            entry.2 += quantity;
        }
    }
    let exit_price = if exit_quantity > f64::EPSILON {
        exit_notional / exit_quantity
    } else {
        0.0
    };
    let (last_exit_order_id, (_, last_exit_notional, last_exit_quantity)) = exit_orders
        .into_iter()
        .max_by_key(|(_, value)| value.0)
        .map(|(order_id, value)| (Some(order_id), value))
        .unwrap_or((None, (0, 0.0, 0.0)));
    let last_exit_price = if last_exit_quantity > f64::EPSILON {
        last_exit_notional / last_exit_quantity
    } else {
        0.0
    };
    TradeSummary {
        entry_price: if entry_quantity > f64::EPSILON {
            entry_notional / entry_quantity
        } else {
            0.0
        },
        entry_quantity,
        exit_price,
        exit_quantity,
        last_exit_price,
        last_exit_quantity,
        last_exit_order_id,
        fees_usd: fees,
        net_pnl_usd: realized - fees,
        maker_fills,
        taker_fills,
        maker_notional_usd: maker_notional,
        taker_notional_usd: taker_notional,
    }
}

fn protective_exit_reason(meta: &ExecutionMeta) -> &str {
    match meta.stop_reason.as_deref() {
        Some("pre_tp_profit_shield") => "profit_shield_stop",
        Some("trailing_protection") => "trailing_protection",
        Some("risk_shield") => "risk_shield_stop",
        Some("initial_stop") => "initial_stop",
        Some(reason) => reason,
        None if meta.break_even_armed => "trailing_or_protected_stop",
        None => "initial_stop",
    }
}

/// Existing positions retain their entry-time parameters in the restart
/// journal. Apply the new continuous-profit threshold at runtime as well, so
/// deploying the fix protects an already-open second/reversal leg instead of
/// waiting for the next position chain.
fn effective_trailing_activation(meta: &ExecutionMeta) -> Option<f64> {
    meta.trailing_activation_pct.map(|activation| {
        if matches!(
            meta.recipe.as_str(),
            TREND_REENTRY_RECIPE | TREND_PROFIT_REVERSAL_RECIPE
        ) {
            activation.min(0.010)
        } else {
            activation
        }
    })
}

fn next_profit_reversal_count(current: u8, exit_reason: &str) -> Option<u8> {
    (exit_reason == "executable_profit_protection" && current < MAX_PROFIT_REVERSALS_PER_CHAIN)
        .then(|| current + 1)
}

fn reentry_state_event(
    ts_ms: i64,
    campaign: &TrendReentryCampaign,
    stage: &str,
    detail: Value,
) -> ExchangeEvent {
    let recipe = if campaign.immediate_profit_reversal {
        TREND_PROFIT_REVERSAL_RECIPE
    } else {
        TREND_REENTRY_RECIPE
    };
    ExchangeEvent {
        kind: "trend_reentry_state".into(),
        payload: serde_json::json!({
            "ts_ms":ts_ms,
            "recipe":recipe,
            "source_candidate_id":campaign.source_candidate_id,
            "symbol":campaign.symbol,
            "side":campaign.side,
            "stage":stage,
            "immediate_profit_reversal":campaign.immediate_profit_reversal,
            "profit_reversal_count":campaign.profit_reversal_count,
            "max_profit_reversals":MAX_PROFIT_REVERSALS_PER_CHAIN,
            "armed_ms":campaign.armed_ms,
            "expires_ms":campaign.expires_ms,
            "reset_ms":campaign.reset_ms,
            "detail":detail,
            "venue":"binance_demo",
            "paper_only":true,
        }),
    }
}

fn second_leg_confirmation(
    side: Side,
    reference_price: f64,
    bar: &Candle,
    prior: &[&Candle],
    min_body_pct: f64,
    min_directional_flow: f64,
    stop_bounds: (f64, f64),
) -> Option<TrendReentrySignal> {
    if prior.is_empty() || reference_price <= f64::EPSILON || bar.quote_volume <= f64::EPSILON {
        return None;
    }
    let sign = side.sign();
    let body_pct = sign * (bar.close / bar.open.max(f64::EPSILON) - 1.0);
    let directional_flow =
        sign * (2.0 * bar.taker_buy_quote? / bar.quote_volume.max(f64::EPSILON) - 1.0);
    let resume_level = match side {
        Side::Buy => prior
            .iter()
            .map(|value| value.high)
            .fold(f64::NEG_INFINITY, f64::max),
        Side::Sell => prior
            .iter()
            .map(|value| value.low)
            .fold(f64::INFINITY, f64::min),
    };
    let resumed = match side {
        Side::Buy => bar.close > resume_level,
        Side::Sell => bar.close < resume_level,
    };
    if !resumed || body_pct < min_body_pct || directional_flow < min_directional_flow {
        return None;
    }
    let structural_stop = match side {
        Side::Buy => prior
            .iter()
            .map(|value| value.low)
            .chain(std::iter::once(bar.low))
            .fold(f64::INFINITY, f64::min),
        Side::Sell => prior
            .iter()
            .map(|value| value.high)
            .chain(std::iter::once(bar.high))
            .fold(f64::NEG_INFINITY, f64::max),
    };
    let raw_stop_pct = sign * (reference_price - structural_stop) / reference_price;
    if raw_stop_pct <= 0.0 {
        return None;
    }
    Some(TrendReentrySignal {
        signal_ms: bar.close_ms,
        reference_price,
        stop_pct: raw_stop_pct.clamp(stop_bounds.0, stop_bounds.1),
        body_pct,
        directional_flow,
    })
}

fn early_failure_triggered(
    side: Side,
    entry_price: f64,
    extreme_price: f64,
    mark_price: f64,
    elapsed_ms: i64,
    thresholds: (i64, f64, f64),
) -> bool {
    let (after_ms, adverse_pct, max_favorable_pct) = thresholds;
    if after_ms <= 0 || adverse_pct <= 0.0 || entry_price <= f64::EPSILON || elapsed_ms < after_ms {
        return false;
    }
    let favorable = side.sign() * (extreme_price / entry_price - 1.0);
    let current = side.sign() * (mark_price / entry_price - 1.0);
    favorable < max_favorable_pct && current <= -adverse_pct
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum MaxHoldReview {
    NotApplicable,
    ReleaseToProtection,
    Extend {
        extension_ms: i64,
        progress_r: f64,
        current_r: f64,
        executable_net_pnl_usd: f64,
        initial_risk_usd: f64,
        exit_impact_bps: f64,
        reason: &'static str,
    },
    Exit,
}

const FAST_TREND_EXECUTABLE_GRACE_MS: i64 = 90_000;
const FAST_TREND_PROGRESS_REVIEW_MS: i64 = 10 * 60_000;
const FAST_TREND_MAX_PROGRESS_REVIEWS: u8 = 2;
const FAST_TREND_MIN_PROGRESS_R: f64 = 0.25;
const FAST_TREND_MAX_CURRENT_ADVERSE_R: f64 = 0.25;
const MAX_STORED_EXECUTABLE_QUOTE_AGE_MS: i64 = 5_000;
const FAST_TREND_HIGH_EXIT_IMPACT_BPS: f64 = 10.0;

fn max_hold_review(meta: &ExecutionMeta, now_ms: i64, mark_price: f64) -> MaxHoldReview {
    if meta.exit_requested
        || meta.max_hold_ms <= 0
        || now_ms - meta.holding_started_ms() < meta.max_hold_ms
    {
        return MaxHoldReview::NotApplicable;
    }
    if meta.fixed_time_exit {
        return MaxHoldReview::Exit;
    }
    if meta.recipe != "fast_trend_activation" || meta.entry_price <= f64::EPSILON {
        return MaxHoldReview::Exit;
    }
    if meta.break_even_armed
        || meta
            .executable_profit
            .as_ref()
            .is_some_and(|guard| guard.floor_net_return.is_some())
    {
        return MaxHoldReview::ReleaseToProtection;
    }
    if meta.pending_exit_reason.as_deref() == Some("max_hold")
        || meta.max_hold_reviews >= FAST_TREND_MAX_PROGRESS_REVIEWS
    {
        return MaxHoldReview::Exit;
    }
    let Some(initial_risk_usd) = meta.initial_risk_usd.filter(|value| *value > f64::EPSILON) else {
        return MaxHoldReview::Exit;
    };
    let stop_risk_pct = meta.side.sign() * (meta.entry_price - meta.stop_price)
        / meta.entry_price.max(f64::EPSILON);
    if stop_risk_pct <= f64::EPSILON {
        return MaxHoldReview::Exit;
    }
    let favorable_pct = meta.side.sign() * (meta.extreme_price / meta.entry_price - 1.0);
    let current_pct = meta.side.sign() * (mark_price / meta.entry_price - 1.0);
    let progress_r = favorable_pct.max(0.0) / stop_risk_pct;
    let current_r = current_pct / stop_risk_pct;
    let guard = meta.executable_profit.as_ref();

    // The ten-minute Fast deadline is a review, not an unconditional market
    // exit. A trade that has already demonstrated meaningful favorable
    // movement and has not moved materially through its initial risk still
    // has live continuation optionality. Give it a bounded ten-minute review
    // window while the exchange-side catastrophe stop remains active. This
    // also prevents one noisy Demo quote at the deadline from converting a
    // valid mainnet continuation into an avoidable taker loss.
    if progress_r >= FAST_TREND_MIN_PROGRESS_R && current_r >= -FAST_TREND_MAX_CURRENT_ADVERSE_R {
        let executable_net_pnl_usd = guard
            .map(|value| {
                value.current_net_return * meta.entry_price * meta.last_observed_quantity.max(0.0)
            })
            .unwrap_or_default();
        let exit_impact_bps = guard
            .map(|value| match meta.side {
                Side::Buy => (mark_price - value.exit_vwap) / mark_price,
                Side::Sell => (value.exit_vwap - mark_price) / mark_price,
            })
            .unwrap_or_default()
            .max(0.0)
            * 10_000.0;
        return MaxHoldReview::Extend {
            extension_ms: FAST_TREND_PROGRESS_REVIEW_MS,
            progress_r,
            current_r,
            executable_net_pnl_usd,
            initial_risk_usd,
            exit_impact_bps,
            reason: "meaningful_progress_with_bounded_adverse_move",
        };
    }

    let Some(guard) = guard else {
        return MaxHoldReview::Exit;
    };
    if now_ms.saturating_sub(guard.observed_ms) > MAX_STORED_EXECUTABLE_QUOTE_AGE_MS {
        return MaxHoldReview::Exit;
    }
    let executable_net_pnl_usd =
        guard.current_net_return * meta.entry_price * meta.last_observed_quantity.max(0.0);
    // A losing or flat sprint has already failed by its deadline. Never defer
    // that loss merely because an executable quote is fresh.
    if executable_net_pnl_usd <= 0.0 || !mark_price.is_finite() || mark_price <= f64::EPSILON {
        return MaxHoldReview::Exit;
    }
    let exit_impact_pct = match meta.side {
        Side::Buy => (mark_price - guard.exit_vwap) / mark_price,
        Side::Sell => (guard.exit_vwap - mark_price) / mark_price,
    }
    .max(0.0);
    let exit_impact_bps = exit_impact_pct * 10_000.0;
    if exit_impact_bps < FAST_TREND_HIGH_EXIT_IMPACT_BPS {
        return MaxHoldReview::Exit;
    }
    // The short liquidity grace is deliberately one-shot. Subsequent reviews
    // must either qualify through real price progress above or exit.
    if meta.max_hold_reviews > 0 {
        return MaxHoldReview::Exit;
    }
    // Grant one short grace only when the position is already bankably
    // profitable and the current executable VWAP shows unusually high exit
    // impact. The second review always exits if protection has not armed.
    MaxHoldReview::Extend {
        extension_ms: FAST_TREND_EXECUTABLE_GRACE_MS,
        progress_r,
        current_r,
        executable_net_pnl_usd,
        initial_risk_usd,
        exit_impact_bps,
        reason: "profitable_position_with_temporarily_high_exit_impact",
    }
}

fn exit_matches_stop(side: Side, stop_price: f64, exit_price: f64) -> bool {
    if stop_price <= 0.0 || exit_price <= 0.0 {
        return false;
    }
    const STOP_MARKET_TOLERANCE_PCT: f64 = 0.0025;
    match side {
        Side::Buy => exit_price <= stop_price * (1.0 + STOP_MARKET_TOLERANCE_PCT),
        Side::Sell => exit_price >= stop_price * (1.0 - STOP_MARKET_TOLERANCE_PCT),
    }
}

fn entry_failure_event_kind(reason: &str) -> &'static str {
    let reason = reason.to_ascii_lowercase();
    if reason.contains("entry signal invalidated")
        || reason.contains("pending entry signal invalidated")
        || reason.contains("entry guard unavailable")
        || reason.contains("expired without fill")
        || reason.contains("maker entry expired")
    {
        "exchange_entry_canceled"
    } else {
        "exchange_order_rejected"
    }
}

fn parse_rules(value: &Value) -> Result<BTreeMap<String, SymbolRules>> {
    let symbols = value["symbols"]
        .as_array()
        .ok_or_else(|| anyhow!("exchangeInfo symbols is missing"))?;
    let mut out = BTreeMap::new();
    for symbol in symbols {
        let Some(name) = symbol["symbol"].as_str() else {
            continue;
        };
        if symbol["status"].as_str() != Some("TRADING")
            || symbol["contractType"].as_str() != Some("PERPETUAL")
            || symbol["quoteAsset"].as_str() != Some("USDT")
        {
            continue;
        }
        let filters = symbol["filters"].as_array().cloned().unwrap_or_default();
        let filter = |kind: &str, field: &str| {
            filters
                .iter()
                .find(|value| value["filterType"] == kind)
                .and_then(|value| value[field].as_str())
                .and_then(|value| value.parse().ok())
        };
        out.insert(
            name.into(),
            SymbolRules {
                quantity_step: filter("LOT_SIZE", "stepSize").unwrap_or(1.0),
                min_quantity: filter("LOT_SIZE", "minQty").unwrap_or(0.0),
                max_limit_quantity: filter("LOT_SIZE", "maxQty")
                    .filter(|value| *value > 0.0)
                    .unwrap_or(f64::INFINITY),
                max_market_quantity: filter("MARKET_LOT_SIZE", "maxQty")
                    .filter(|value| *value > 0.0)
                    .or_else(|| filter("LOT_SIZE", "maxQty").filter(|value| *value > 0.0))
                    .unwrap_or(f64::INFINITY),
                price_tick: filter("PRICE_FILTER", "tickSize").unwrap_or(0.01),
                min_notional: filter("MIN_NOTIONAL", "notional").unwrap_or(5.0),
            },
        );
    }
    Ok(out)
}

fn parse_account(value: &Value) -> Result<RemoteAccount> {
    let number =
        |key| parse_f64(value, key).ok_or_else(|| anyhow!("account field {key} is missing"));
    let mut positions = BTreeMap::new();
    for row in value["positions"]
        .as_array()
        .ok_or_else(|| anyhow!("account positions are missing"))?
    {
        let quantity = parse_f64(row, "positionAmt").unwrap_or(0.0);
        if quantity.abs() <= f64::EPSILON {
            continue;
        }
        let symbol = row["symbol"].as_str().unwrap_or_default().to_owned();
        positions.insert(
            symbol.clone(),
            RemotePosition {
                symbol,
                side: if quantity > 0.0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                quantity,
                entry_price: parse_f64(row, "entryPrice").unwrap_or(0.0),
                mark_price: parse_f64(row, "markPrice")
                    .or_else(|| {
                        parse_f64(row, "notional")
                            .map(|notional| notional.abs() / quantity.abs().max(f64::EPSILON))
                    })
                    .unwrap_or_else(|| parse_f64(row, "entryPrice").unwrap_or(0.0)),
                unrealized_pnl: parse_f64(row, "unrealizedProfit").unwrap_or(0.0),
                isolated: row["isolated"].as_bool().unwrap_or(false)
                    || row["marginType"].as_str() == Some("isolated"),
                isolated_wallet_usd: parse_f64(row, "isolatedWallet").unwrap_or_default(),
            },
        );
    }
    Ok(RemoteAccount {
        wallet_balance: number("totalWalletBalance")?,
        margin_balance: number("totalMarginBalance")?,
        available_balance: number("availableBalance")?,
        positions,
    })
}

fn parse_f64(value: &Value, key: &str) -> Option<f64> {
    value[key]
        .as_str()
        .and_then(|value| value.parse().ok())
        .or_else(|| value[key].as_f64())
}

fn reconciled_executed_quantity(previously_observed: f64, final_order: &Value) -> f64 {
    // executedQty is cumulative and must never move backwards. Keeping the
    // larger observation also protects against an incomplete cancel response.
    parse_f64(final_order, "executedQty")
        .unwrap_or_default()
        .max(previously_observed)
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}
fn gate_key(recipe: &str, side: Side) -> String {
    let recipe = if recipe == LIQUIDATION_REVERSAL_RECIPE {
        format!("{recipe}@{LIQUIDATION_EXECUTION_BASIS_VERSION}")
    } else {
        recipe.to_owned()
    };
    format!(
        "{recipe}:{}",
        match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    )
}
fn loss_cooldown_until(
    outcomes: Option<&[ExecutionOutcome]>,
    cooldown_minutes: u32,
) -> Option<i64> {
    outcomes
        .and_then(|values| values.last())
        .filter(|outcome| outcome.pnl_usd < 0.0)
        .map(|outcome| outcome.exit_ms + i64::from(cooldown_minutes) * 60_000)
}

#[derive(Debug, Clone, Copy, Default)]
struct PlanCostDiagnostics {
    spread_bps: Option<f64>,
    expected_exit_slippage_bps: Option<f64>,
    estimated_round_trip_cost_bps: Option<f64>,
    gross_target_bps: Option<f64>,
    target_to_cost_ratio: Option<f64>,
}

fn plan_cost_diagnostics(
    frame: &MarketFrame,
    plan: &greed_kernel::PositionPlan,
) -> PlanCostDiagnostics {
    let Some(book) = frame
        .instrument(&plan.symbol)
        .and_then(|instrument| instrument.book.as_ref())
    else {
        return PlanCostDiagnostics::default();
    };
    let mid = (book.bid + book.ask) * 0.5;
    let spread_bps = (mid > f64::EPSILON).then_some((book.ask - book.bid) / mid * 10_000.0);
    let expected_exit_slippage_bps = match plan.side {
        Side::Buy => book.expected_sell_slippage_bps,
        Side::Sell => book.expected_buy_slippage_bps,
    };
    // Binance futures maker entry plus taker/protective exit is 7 bps in the
    // conservative research model. Crossing half the spread and the measured
    // depth slippage are added for a comparable per-candidate cost budget.
    let estimated_round_trip_cost_bps = spread_bps
        .map(|spread| 7.0 + spread * 0.5 + expected_exit_slippage_bps.unwrap_or_default());
    let gross_target_bps = plan.take_profit_prices.first().map(|(target, _)| {
        plan.side.sign() * (target / plan.reference_price.max(f64::EPSILON) - 1.0) * 10_000.0
    });
    let target_to_cost_ratio = gross_target_bps
        .zip(estimated_round_trip_cost_bps)
        .and_then(|(target, cost)| (cost > f64::EPSILON).then_some(target / cost));
    PlanCostDiagnostics {
        spread_bps,
        expected_exit_slippage_bps,
        estimated_round_trip_cost_bps,
        gross_target_bps,
        target_to_cost_ratio,
    }
}

fn take_profit_prices_from_fill(
    plan: &greed_kernel::PositionPlan,
    entry_price: f64,
    executed_quantity: f64,
) -> Vec<(f64, f64)> {
    let sign = plan.side.sign();
    let cost_aware = plan
        .signal_context
        .get("cost_aware_full_take_profit")
        .is_some_and(|value| value == "true");
    let desired_net_usd = plan
        .signal_context
        .get("target_account_profit_usd")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0);
    let estimated_cost_bps = plan
        .signal_context
        .get("estimated_target_cost_bps")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0);

    if cost_aware && plan.take_profit_prices.len() == 1 {
        if let (Some(desired_net_usd), Some(estimated_cost_bps)) =
            (desired_net_usd, estimated_cost_bps)
        {
            let actual_notional_usd = entry_price * executed_quantity;
            if actual_notional_usd.is_finite() && actual_notional_usd > f64::EPSILON {
                let gross_target_pct =
                    desired_net_usd / actual_notional_usd + estimated_cost_bps / 10_000.0;
                return vec![(
                    entry_price * (1.0 + sign * gross_target_pct),
                    plan.take_profit_prices[0].1,
                )];
            }
        }
    }

    plan.take_profit_prices
        .iter()
        .map(|(target, fraction)| {
            let distance = sign * (*target / plan.reference_price.max(f64::EPSILON) - 1.0);
            (entry_price * (1.0 + sign * distance), *fraction)
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct FillProtection {
    activation_pct: Option<f64>,
    trailing_distance_pct: Option<f64>,
}

/// Rebase Fast protection on what actually filled. The full target stays at
/// the requested account-level dollar objective. The lock arms at 55% of that
/// objective and trails by 25%; both are recomputed from actual notional so a
/// liquidity-reduced fill cannot inherit thresholds from the planned size.
fn protection_from_fill(
    plan: &greed_kernel::PositionPlan,
    actual_notional_usd: f64,
    actual_initial_risk_usd: f64,
) -> FillProtection {
    let cost_aware = plan
        .signal_context
        .get("cost_aware_profit_shield")
        .is_some_and(|value| value == "true");
    let desired_net_usd = plan
        .signal_context
        .get("target_account_profit_usd")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0);
    if cost_aware
        && actual_notional_usd.is_finite()
        && actual_notional_usd > f64::EPSILON
        && actual_initial_risk_usd.is_finite()
        && actual_initial_risk_usd > f64::EPSILON
    {
        if let Some(desired_net_usd) = desired_net_usd {
            let activation_usd = desired_net_usd * 0.55;
            let retracement_usd = desired_net_usd * 0.25;
            return FillProtection {
                activation_pct: Some(
                    crate::profit_guard::COST_RESERVE + activation_usd / actual_notional_usd,
                ),
                trailing_distance_pct: Some(retracement_usd / actual_notional_usd),
            };
        }
    }
    FillProtection {
        activation_pct: plan.profit_shield_activation_pct,
        trailing_distance_pct: plan.trailing_distance_pct,
    }
}

fn floor_step(value: f64, step: f64) -> f64 {
    (value / step).floor() * step
}

fn ceil_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        value
    } else {
        (value / step).ceil() * step
    }
}
fn round_step(value: f64, step: f64) -> f64 {
    (value / step).round() * step
}
fn decimal(value: f64, step: f64) -> String {
    let precision = (-step.log10().floor()).max(0.0) as usize;
    format!("{value:.precision$}")
}
fn order_average_price(order: &Value) -> Option<f64> {
    parse_f64(order, "avgPrice")
        .filter(|value| *value > 0.0)
        .or_else(|| {
            let quote = parse_f64(order, "cumQuote")?;
            let quantity = parse_f64(order, "executedQty")?;
            (quantity > f64::EPSILON).then_some(quote / quantity)
        })
}
fn entry_price_hint(order: &Value, fallback: f64) -> f64 {
    order_average_price(order).unwrap_or(fallback)
}
fn average_fills(rows: &[Value], side: &str) -> Option<f64> {
    let (notional, quantity) = rows
        .iter()
        .filter(|row| {
            row["side"]
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case(side))
        })
        .fold((0.0, 0.0), |(notional, quantity), row| {
            let fill_quantity = parse_f64(row, "qty").unwrap_or_default();
            let fill_price = parse_f64(row, "price").unwrap_or_default();
            (
                notional + fill_price * fill_quantity,
                quantity + fill_quantity,
            )
        });
    (quantity > f64::EPSILON).then_some(notional / quantity)
}
fn is_post_only_rejection(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("-5022")
        || message.contains("could not be executed as maker")
        // Binance Demo can lag the mainnet market data used to build a plan.
        // Its percent-price filter then rejects an otherwise passive limit.
        // Treat these deterministic price-band errors like a stale passive
        // quote so place_post_only_entry refreshes from the execution venue's
        // own book instead of dropping the signal outright.
        || message.contains("\"code\":-4016")
        || message.contains("\"code\":-4024")
}
fn is_invalid_symbol_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("\"code\":-1121") || message.contains("Invalid symbol")
}
fn capped_passive_price(
    side: Side,
    best_passive: f64,
    reference_price: f64,
    max_adverse_bps: f64,
    tick: f64,
) -> f64 {
    let adverse = max_adverse_bps.max(0.0) / 10_000.0;
    match side {
        Side::Buy => floor_step(best_passive.min(reference_price * (1.0 + adverse)), tick),
        Side::Sell => ceil_step(best_passive.max(reference_price * (1.0 - adverse)), tick),
    }
}
fn passive_price_improves(side: Side, current: f64, proposed: f64, tick: f64) -> bool {
    match side {
        Side::Buy => proposed >= current + tick * 0.5,
        Side::Sell => proposed <= current - tick * 0.5,
    }
}
fn entry_adverse_bps(side: Side, executable: f64, reference: f64) -> f64 {
    side.sign() * (executable / reference.max(f64::EPSILON) - 1.0) * 10_000.0
}
fn absolute_price_divergence_bps(executable: f64, reference: f64) -> f64 {
    (executable / reference.max(f64::EPSILON) - 1.0).abs() * 10_000.0
}
fn bounded_taker_ioc_price(
    side: Side,
    reference: f64,
    executable_quote: f64,
    max_divergence_bps: f64,
    tick: f64,
) -> Result<f64> {
    if !reference.is_finite()
        || reference <= f64::EPSILON
        || !executable_quote.is_finite()
        || executable_quote <= f64::EPSILON
        || !tick.is_finite()
        || tick <= f64::EPSILON
    {
        return Err(anyhow!("bounded IOC received an invalid price or tick"));
    }
    if max_divergence_bps <= 0.0 {
        return Err(anyhow!("bounded IOC requires a positive divergence limit"));
    }
    let divergence_bps = absolute_price_divergence_bps(executable_quote, reference);
    if divergence_bps > max_divergence_bps {
        return Err(anyhow!(
            "execution venue quote {executable_quote} diverged {divergence_bps:.1} bps from strategy reference {reference} (max {max_divergence_bps:.1}); entry skipped"
        ));
    }
    let distance = max_divergence_bps / 10_000.0;
    let limit = match side {
        // The limit is marketable at the quote already checked above, while
        // floor/ceil preserve the adverse-price boundary after tick rounding.
        Side::Buy => floor_step(reference * (1.0 + distance), tick),
        Side::Sell => ceil_step(reference * (1.0 - distance), tick),
    };
    let marketable = match side {
        Side::Buy => limit + tick * 0.5 >= executable_quote,
        Side::Sell => limit <= executable_quote + tick * 0.5,
    };
    if !marketable {
        return Err(anyhow!(
            "execution venue moved outside the bounded IOC price before submission"
        ));
    }
    Ok(limit)
}
fn pending_entry_guard_state(
    plan: &greed_kernel::PositionPlan,
    signal_mid: f64,
    trade_imbalance: Option<f64>,
    mid_return_bps_10s: Option<f64>,
) -> EntryGuardState {
    if let Some(limit) = plan.entry_limit.filter(|value| *value > f64::EPSILON) {
        let overshoot_bps = -plan.side.sign() * (signal_mid / limit - 1.0) * 10_000.0;
        if plan.entry_invalidation_bps > 0.0 && overshoot_bps > plan.entry_invalidation_bps {
            return EntryGuardState::StructuralInvalidation(format!(
                "strategy midpoint {signal_mid} overshot entry limit {limit} by {overshoot_bps:.1} bps against the setup (max {:.1})",
                plan.entry_invalidation_bps
            ));
        }
    }
    if let (Some(flow), Some(response)) = (trade_imbalance, mid_return_bps_10s) {
        let directional_flow = plan.side.sign() * flow;
        let directional_response = plan.side.sign() * response;
        if plan.entry_guard_max_opposing_flow > 0.0
            && plan.entry_guard_max_opposing_return_bps > 0.0
            && directional_flow < -plan.entry_guard_max_opposing_flow
            && directional_response < -plan.entry_guard_max_opposing_return_bps
        {
            return EntryGuardState::MicroReversal(format!(
                "live flow {:.0}% and 10s strategy-market response {response:.1} bps reversed against the pending entry",
                flow * 100.0
            ));
        }
    }
    EntryGuardState::Healthy
}
fn persistent_guard_reason(
    since_ms: &mut Option<i64>,
    now_ms: i64,
    confirmation_ms: i64,
    reason: &str,
) -> Option<String> {
    let started_ms = *since_ms.get_or_insert(now_ms);
    let elapsed_ms = now_ms.saturating_sub(started_ms);
    (elapsed_ms >= confirmation_ms)
        .then(|| format!("{reason} continuously for {elapsed_ms}ms (need {confirmation_ms}ms)"))
}
fn bounded_fallback_quantity(quantity: f64, multiplier: f64, step: f64) -> f64 {
    floor_step(quantity * multiplier.clamp(0.01, 1.0), step)
}
fn managed_fill_threshold_reached(
    executed: f64,
    requested: f64,
    quantity_step: f64,
    min_fill_ratio: f64,
) -> bool {
    min_fill_ratio > 0.0
        && executed > f64::EPSILON
        && executed + quantity_step * 0.5 >= requested * min_fill_ratio
}

fn partial_fill_is_manageable(
    executed: f64,
    requested: f64,
    price: f64,
    min_managed_fill_ratio: f64,
    rules: &SymbolRules,
    take_profit_prices: &[(f64, f64)],
) -> bool {
    let threshold = min_managed_fill_ratio.clamp(0.0, 1.0);
    let ratio_ok =
        requested > f64::EPSILON && executed + rules.quantity_step * 0.5 >= requested * threshold;
    let position_ok = executed >= rules.min_quantity && executed * price >= rules.min_notional;
    let exits_ok = take_profit_prices.iter().all(|(_, fraction)| {
        if *fraction >= 1.0 - f64::EPSILON {
            return position_ok;
        }
        let quantity = floor_step(executed * fraction, rules.quantity_step);
        quantity >= rules.min_quantity && quantity * price >= rules.min_notional
    });
    ratio_ok && position_ok && exits_ok
}

fn cap_entry_quantity(desired: f64, maximum: f64, quantity_step: f64) -> Result<f64> {
    let capped = floor_step(desired.min(maximum), quantity_step);
    if desired > f64::EPSILON && capped + quantity_step * 0.5 < desired {
        let supported_ratio = capped / desired;
        if supported_ratio < 0.50 {
            return Err(anyhow!(
                "Binance maximum quantity supports only {:.1}% of the planned position; entry skipped",
                supported_ratio * 100.0
            ));
        }
    }
    Ok(capped)
}

fn market_close_chunk(remaining: f64, rules: &SymbolRules) -> Result<f64> {
    if !remaining.is_finite() || remaining <= f64::EPSILON {
        return Err(anyhow!("market close has no positive remaining quantity"));
    }
    let maximum = if rules.max_market_quantity.is_finite() {
        rules.max_market_quantity
    } else {
        remaining
    };
    let chunk = floor_step(remaining.min(maximum), rules.quantity_step);
    if chunk < rules.min_quantity || chunk <= f64::EPSILON {
        return Err(anyhow!(
            "remaining market-close quantity {remaining} cannot satisfy Binance minimum {} at step {}",
            rules.min_quantity,
            rules.quantity_step
        ));
    }
    Ok(chunk)
}

// Infer from persisted numerical settings, not an ephemeral candidate tag.
// A real exchange stop is still mandatory in the account protection check.
fn managed_stop_only_exit(
    max_hold_ms: i64,
    shield: Option<f64>,
    floor: f64,
    activation: Option<f64>,
    distance: Option<f64>,
) -> bool {
    match (shield, activation, distance) {
        (Some(shield), Some(activation), Some(distance)) => {
            max_hold_ms > 0
                && [shield, floor, activation, distance]
                    .iter()
                    .all(|v| v.is_finite())
                && floor >= 0.0
                && shield > floor
                && activation >= shield
                && distance > 0.0
                && distance < activation
        }
        _ => false,
    }
}

fn plan_has_managed_exit(plan: &greed_kernel::PositionPlan) -> bool {
    managed_stop_only_exit(
        plan.max_hold_ms,
        plan.profit_shield_activation_pct,
        plan.break_even_buffer_pct,
        plan.trailing_activation_pct,
        plan.trailing_distance_pct,
    )
}

fn validate_entry_quantity_and_exits(
    plan: &greed_kernel::PositionPlan,
    quantity: f64,
    price: f64,
    rules: &SymbolRules,
) -> Result<()> {
    if quantity < rules.min_quantity || quantity * price < rules.min_notional {
        return Err(anyhow!(
            "order is below Binance quantity or notional minimum"
        ));
    }
    if quantity > rules.max_limit_quantity + rules.quantity_step * 0.5 {
        return Err(anyhow!("order exceeds Binance maximum quantity"));
    }
    if plan.take_profit_prices.is_empty() && !plan.fixed_time_exit && !plan_has_managed_exit(plan) {
        return Err(anyhow!("position plan requires at least one take profit"));
    }
    let take_profit_fraction = plan
        .take_profit_prices
        .iter()
        .map(|(_, fraction)| *fraction)
        .sum::<f64>();
    if take_profit_fraction > 1.0 + 1e-6 {
        return Err(anyhow!("staged take-profit fractions exceed the position"));
    }
    if let Some(runner_fraction) = plan.unprotected_runner_fraction {
        if (take_profit_fraction + runner_fraction - 1.0).abs() > 1e-6 {
            return Err(anyhow!(
                "take-profit fractions and tail runner must sum to one"
            ));
        }
    }
    for (_, fraction) in &plan.take_profit_prices {
        if *fraction >= 1.0 - f64::EPSILON {
            continue;
        }
        let partial = floor_step(quantity * fraction, rules.quantity_step);
        if partial < rules.min_quantity || partial * price < rules.min_notional {
            return Err(anyhow!(
                "staged take profit is below Binance quantity or notional minimum"
            ));
        }
    }
    Ok(())
}
fn reprice_client_id(base: &str, attempt: u8) -> String {
    format!("{}-r{attempt}", &base[..base.len().min(32)])
}

fn signal_ms_from_candidate(candidate: Option<&TradeCandidate>, fallback_ms: i64) -> i64 {
    candidate.map_or(fallback_ms, |value| value.signal_ms)
}

fn liquidation_cooldown_active(
    seen: &BTreeSet<String>,
    symbol: &str,
    signal_ms: i64,
    cooldown_ms: i64,
) -> bool {
    let prefix = format!("{LIQUIDATION_REVERSAL_RECIPE}:{symbol}:");
    seen.iter()
        .filter_map(|candidate_id| candidate_id.strip_prefix(&prefix))
        .filter_map(|value| value.parse::<i64>().ok())
        .any(|previous_ms| {
            previous_ms < signal_ms && signal_ms.saturating_sub(previous_ms) < cooldown_ms
        })
}

fn client_order_id(prefix: &str, candidate_id: &str) -> String {
    let digest = hex_bytes(&Sha256::digest(candidate_id.as_bytes()));
    const MAX_CLIENT_ORDER_ID_LEN: usize = 36;
    let stem = format!("greed-{prefix}-");
    // Keep the historical 20-hex suffix whenever it already fits so a
    // restart can still reconcile orders created by older binaries. Only the
    // long internal prefixes (pendingstop/attemptclose/persistfail) need a
    // shorter suffix to satisfy Binance's 36-character maximum.
    let digest_len = 20usize
        .min(MAX_CLIENT_ORDER_ID_LEN.saturating_sub(stem.len()))
        .min(digest.len());
    format!("{stem}{}", &digest[..digest_len])
}

fn pending_fill_needs_provisional_stop(
    executed: f64,
    terminal: bool,
    wait_satisfied: bool,
    deadline: bool,
    invalidated: bool,
) -> bool {
    executed > f64::EPSILON && !terminal && !wait_satisfied && !deadline && !invalidated
}

fn order_status_is_terminal(status: &str) -> bool {
    matches!(status, "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED")
}

fn order_value_is_terminal(order: &Value) -> bool {
    order["status"]
        .as_str()
        .is_some_and(order_status_is_terminal)
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_risk_reset_changes_only_the_loss_baselines() {
        let mut state = DemoState {
            risk_day: "2026-09-08".into(),
            risk_day_start_equity_usd: Some(10_000.0),
            peak_equity_usd: Some(10_250.0),
            seen: BTreeSet::from(["candidate-1".into()]),
            execution_halt_reason: None,
            ..DemoState::default()
        };
        let previous = reset_portfolio_risk_baselines(&mut state, 9_700.0, "2026-09-09".into());
        assert_eq!(previous, (Some(10_000.0), Some(10_250.0)));
        assert_eq!(state.risk_day, "2026-09-09");
        assert_eq!(state.risk_day_start_equity_usd, Some(9_700.0));
        assert_eq!(state.peak_equity_usd, Some(9_700.0));
        assert!(state.seen.contains("candidate-1"));
        assert!(state.execution_halt_reason.is_none());
    }

    fn five_minute_bar(
        open_ms: i64,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        buy_share: f64,
    ) -> Candle {
        Candle {
            open_ms,
            close_ms: open_ms + 299_999,
            open,
            high,
            low,
            close,
            quote_volume: 1_000.0,
            taker_buy_quote: Some(1_000.0 * buy_share),
            closed: true,
        }
    }

    fn guarded_plan(side: Side) -> greed_kernel::PositionPlan {
        greed_kernel::PositionPlan {
            candidate_id: "trend_continuation:TESTUSDT:1".into(),
            signal_context: BTreeMap::new(),
            symbol: "TESTUSDT".into(),
            side,
            reference_price: 100.0,
            notional_usd: 1_000.0,
            entry_limit: Some(99.0),
            entry_timeout_ms: 60_000,
            taker_fallback: true,
            max_entry_adverse_bps: 8.0,
            taker_fallback_max_adverse_bps: 20.0,
            taker_fallback_size_multiplier: 0.05,
            min_fill_ratio: 0.80,
            min_managed_fill_ratio: 0.20,
            entry_invalidation_bps: 30.0,
            entry_guard_max_opposing_flow: 0.15,
            entry_guard_max_opposing_return_bps: 3.0,
            stop_price: 98.75,
            take_profit_prices: vec![(102.5, 0.4)],
            unprotected_runner_fraction: None,
            break_even_after_fraction: Some(0.4),
            break_even_buffer_pct: 0.0018,
            profit_shield_activation_pct: Some(0.00625),
            trailing_activation_pct: Some(0.01),
            trailing_distance_pct: Some(0.005),
            early_failure_after_ms: 180_000,
            early_failure_adverse_pct: 0.00625,
            early_failure_max_favorable_pct: 0.0025,
            max_hold_ms: 0,
            fixed_time_exit: false,
        }
    }

    #[test]
    fn exchange_quantization_never_rounds_quantity_up() {
        assert_eq!(floor_step(1.239, 0.01), 1.23);
        assert_eq!(decimal(1.23, 0.01), "1.23");
        assert_eq!(ceil_step(1.231, 0.01), 1.24);
    }

    #[test]
    fn fast_take_profit_retargets_the_actual_fill_to_thirty_dollars_net() {
        let mut plan = guarded_plan(Side::Buy);
        plan.take_profit_prices = vec![(100.39, 1.0)];
        plan.signal_context
            .insert("cost_aware_full_take_profit".into(), "true".into());
        plan.signal_context
            .insert("target_account_profit_usd".into(), "30".into());
        plan.signal_context
            .insert("estimated_target_cost_bps".into(), "9".into());

        let full = take_profit_prices_from_fill(&plan, 101.0, 10_000.0 / 101.0);
        assert!((full[0].0 - 101.3939).abs() < 1e-9);
        assert_eq!(full[0].1, 1.0);

        let half = take_profit_prices_from_fill(&plan, 101.0, 5_000.0 / 101.0);
        assert!((half[0].0 - 101.6969).abs() < 1e-9);
        assert_eq!(half[0].1, 1.0);
    }

    #[test]
    fn fast_profit_protection_rebases_to_actual_fill_and_initial_risk() {
        let mut plan = guarded_plan(Side::Buy);
        plan.signal_context
            .insert("cost_aware_profit_shield".into(), "true".into());
        plan.signal_context
            .insert("target_account_profit_usd".into(), "30".into());
        let protection = protection_from_fill(&plan, 2_626.0, 18.90);
        let expected_activation = crate::profit_guard::COST_RESERVE + 30.0 * 0.55 / 2_626.0;
        let expected_distance = 30.0 * 0.25 / 2_626.0;
        assert!((protection.activation_pct.unwrap() - expected_activation).abs() < 1e-12);
        assert!((protection.trailing_distance_pct.unwrap() - expected_distance).abs() < 1e-12);

        let small_fill = protection_from_fill(&plan, 247.0, 1.67);
        assert!(small_fill.activation_pct.unwrap() > 0.06);
        assert!(small_fill.trailing_distance_pct.unwrap() > 0.03);
    }

    #[test]
    fn market_close_is_split_at_the_exchange_market_quantity_limit() {
        let rules = SymbolRules {
            quantity_step: 1.0,
            min_quantity: 1.0,
            max_limit_quantity: 100_000.0,
            max_market_quantity: 20_000.0,
            price_tick: 0.00001,
            min_notional: 5.0,
        };
        assert_eq!(market_close_chunk(73_882.0, &rules).unwrap(), 20_000.0);
        assert_eq!(market_close_chunk(13_882.0, &rules).unwrap(), 13_882.0);
    }

    #[test]
    fn bounded_taker_rejects_a_demo_quote_far_from_the_strategy_market() {
        let error = bounded_taker_ioc_price(Side::Sell, 0.009641, 0.009167, 15.0, 0.000001)
            .expect_err("a 4.9% venue mismatch must never reach the exchange");
        assert!(error.to_string().contains("diverged"));
    }

    #[test]
    fn bounded_taker_limit_is_marketable_without_crossing_the_adverse_cap() {
        let buy = bounded_taker_ioc_price(Side::Buy, 100.0, 100.08, 15.0, 0.01).unwrap();
        let sell = bounded_taker_ioc_price(Side::Sell, 100.0, 99.92, 15.0, 0.01).unwrap();
        assert!((buy - 100.15).abs() < 1e-9);
        assert!((sell - 99.85).abs() < 1e-9);
        assert!(buy >= 100.08);
        assert!(sell <= 99.92);
    }

    #[test]
    fn client_ids_are_stable_and_within_binance_limit() {
        let value = client_order_id("entry", "alt.recipe:test:cycle-123");
        assert_eq!(value, client_order_id("entry", "alt.recipe:test:cycle-123"));
        assert!(value.len() <= 36);
    }

    #[test]
    fn liquidation_gate_uses_a_versioned_execution_basis() {
        assert_eq!(
            gate_key(LIQUIDATION_REVERSAL_RECIPE, Side::Buy),
            "liquidation_exhaustion_reversal@1:buy"
        );
        assert_eq!(
            gate_key("trend_continuation", Side::Sell),
            "trend_continuation:sell"
        );
    }

    #[test]
    fn second_leg_requires_structure_body_and_directional_flow() {
        let first = five_minute_bar(0, 100.0, 100.4, 99.5, 100.0, 0.50);
        let second = five_minute_bar(300_000, 100.0, 100.6, 99.7, 100.2, 0.50);
        let confirmed = five_minute_bar(600_000, 100.2, 101.2, 100.0, 100.9, 0.58);
        let prior = [&first, &second];
        let signal = second_leg_confirmation(
            Side::Buy,
            100.9,
            &confirmed,
            &prior,
            0.002,
            0.05,
            (0.003, 0.0125),
        )
        .expect("completed structural resume should confirm");
        assert_eq!(signal.signal_ms, confirmed.close_ms);
        assert!((signal.directional_flow - 0.16).abs() < 1e-9);
        assert_eq!(signal.stop_pct, 0.0125);

        let weak_flow = five_minute_bar(600_000, 100.2, 101.2, 100.0, 100.9, 0.51);
        assert!(second_leg_confirmation(
            Side::Buy,
            100.9,
            &weak_flow,
            &prior,
            0.002,
            0.05,
            (0.003, 0.0125),
        )
        .is_none());
    }

    #[test]
    fn second_leg_confirmation_is_directionally_symmetric() {
        let first = five_minute_bar(0, 100.0, 100.5, 99.6, 100.0, 0.50);
        let second = five_minute_bar(300_000, 100.0, 100.3, 99.4, 99.8, 0.50);
        let confirmed = five_minute_bar(600_000, 99.8, 100.0, 98.8, 99.1, 0.42);
        let prior = [&first, &second];
        let signal = second_leg_confirmation(
            Side::Sell,
            99.1,
            &confirmed,
            &prior,
            0.002,
            0.05,
            (0.003, 0.0125),
        )
        .expect("short resume should use the mirrored rules");
        assert!((signal.directional_flow - 0.16).abs() < 1e-9);
        assert_eq!(signal.stop_pct, 0.0125);
    }

    #[test]
    fn trade_summary_separates_latest_exit_from_trade_average() {
        let rows = vec![
            serde_json::json!({"side":"BUY","price":"100","qty":"10","realizedPnl":"0","commission":"0.4","maker":true,"orderId":1,"time":1}),
            serde_json::json!({"side":"SELL","price":"110","qty":"4","realizedPnl":"40","commission":"0.1","maker":true,"orderId":2,"time":2}),
            serde_json::json!({"side":"SELL","price":"105","qty":"3","realizedPnl":"15","commission":"0.2","maker":false,"orderId":3,"time":3}),
            // A break-even fill still belongs to the exit order even though
            // realizedPnl is zero.
            serde_json::json!({"side":"SELL","price":"104","qty":"3","realizedPnl":"0","commission":"0.2","maker":false,"orderId":3,"time":3}),
        ];
        let summary = summarize_trades(&rows, Side::Buy);
        assert_eq!(summary.entry_price, 100.0);
        assert_eq!(summary.entry_quantity, 10.0);
        assert!((summary.exit_price - 106.7).abs() < 1e-9);
        assert_eq!(summary.exit_quantity, 10.0);
        assert!((summary.last_exit_price - 104.5).abs() < 1e-9);
        assert_eq!(summary.last_exit_quantity, 6.0);
        assert_eq!(summary.last_exit_order_id, Some(3));
        assert!((summary.net_pnl_usd - 54.1).abs() < 1e-9);
    }

    #[test]
    fn stop_fill_matching_allows_slippage_but_not_favorable_distance() {
        assert!(exit_matches_stop(Side::Buy, 100.0, 99.8));
        assert!(exit_matches_stop(Side::Buy, 100.0, 100.2));
        assert!(!exit_matches_stop(Side::Buy, 100.0, 100.3));
        assert!(exit_matches_stop(Side::Sell, 100.0, 100.2));
        assert!(exit_matches_stop(Side::Sell, 100.0, 99.8));
        assert!(!exit_matches_stop(Side::Sell, 100.0, 99.7));
    }

    #[test]
    fn safety_cancels_are_not_reported_as_exchange_rejections() {
        assert_eq!(
            entry_failure_event_kind("entry signal invalidated before submission: flow reversed"),
            "exchange_entry_canceled"
        );
        assert_eq!(
            entry_failure_event_kind("adaptive maker entry expired without fill"),
            "exchange_entry_canceled"
        );
        assert_eq!(
            entry_failure_event_kind("Binance demo /fapi/v1/order returned 400"),
            "exchange_order_rejected"
        );
    }

    #[test]
    fn early_failure_requires_time_adversity_and_no_prior_follow_through() {
        assert!(early_failure_triggered(
            Side::Buy,
            100.0,
            100.10,
            99.30,
            180_000,
            (180_000, 0.00625, 0.0025),
        ));
        assert!(!early_failure_triggered(
            Side::Buy,
            100.0,
            100.40,
            99.30,
            180_000,
            (180_000, 0.00625, 0.0025),
        ));
        assert!(!early_failure_triggered(
            Side::Sell,
            100.0,
            99.90,
            100.70,
            120_000,
            (180_000, 0.00625, 0.0025),
        ));
        assert!(early_failure_triggered(
            Side::Sell,
            100.0,
            99.90,
            100.70,
            180_000,
            (180_000, 0.00625, 0.0025),
        ));
    }

    #[test]
    fn fill_average_uses_cumulative_quote_when_avg_price_is_missing() {
        let order = serde_json::json!({"avgPrice":"0","cumQuote":"25.96","executedQty":"10000"});
        assert!((order_average_price(&order).unwrap() - 0.002596).abs() < 1e-12);
        let fills = vec![
            serde_json::json!({"side":"BUY","price":"0.002595","qty":"4000"}),
            serde_json::json!({"side":"BUY","price":"0.002597","qty":"6000"}),
            serde_json::json!({"side":"SELL","price":"0.002600","qty":"1000"}),
        ];
        assert!((average_fills(&fills, "BUY").unwrap() - 0.0025962).abs() < 1e-12);
    }

    #[test]
    fn recognizes_definitive_post_only_rejection() {
        let error = anyhow!(
            "{}",
            "Binance returned {\"code\":-5022,\"msg\":\"Due to the order could not be executed as maker\"}"
        );
        assert!(is_post_only_rejection(&error));
    }

    #[test]
    fn reprices_demo_price_band_rejections_from_the_execution_book() {
        let too_high = anyhow!(
            "{}",
            "Binance demo /fapi/v1/order returned 400 Bad Request: {\"code\":-4016,\"msg\":\"Limit price can't be higher than 2.324700.\"}"
        );
        let too_low = anyhow!(
            "{}",
            "Binance demo /fapi/v1/order returned 400 Bad Request: {\"code\":-4024,\"msg\":\"Limit price can't be lower than 2.100000.\"}"
        );
        assert!(is_post_only_rejection(&too_high));
        assert!(is_post_only_rejection(&too_low));
    }

    #[test]
    fn recognizes_invalid_demo_symbol_rejection() {
        let error = anyhow!(
            "{}",
            "Binance demo /fapi/v1/order returned 400 Bad Request: {\"code\":-1121,\"msg\":\"Invalid symbol.\"}"
        );
        assert!(is_invalid_symbol_error(&error));
    }

    #[test]
    fn adaptive_maker_reprices_toward_book_with_a_hard_chase_cap() {
        // These are the observed TUTU and ENA cases from 2026-08-27. The old
        // fixed orders stayed 78.2 and 33.8 bps below the reference. A live
        // passive bid can now move closer without crossing the configured cap.
        let tutu = capped_passive_price(Side::Buy, 0.05609, 0.05621, 8.0, 0.00001);
        assert!((tutu - 0.05609).abs() < 1e-12);
        let ena = capped_passive_price(Side::Buy, 0.15259, 0.15262, 8.0, 0.00001);
        assert!((ena - 0.15259).abs() < 1e-12);

        let capped_buy = capped_passive_price(Side::Buy, 101.0, 100.0, 8.0, 0.01);
        // Floating-point quantization may conservatively leave one extra tick;
        // it must never exceed the adverse-price ceiling.
        assert!((100.07..=100.08).contains(&capped_buy));
        let capped_sell = capped_passive_price(Side::Sell, 99.0, 100.0, 8.0, 0.01);
        assert!((capped_sell - 99.92).abs() < 1e-12);
        assert!(passive_price_improves(Side::Buy, 99.0, 99.5, 0.01));
        assert!(!passive_price_improves(Side::Buy, 99.5, 99.0, 0.01));
    }

    #[test]
    fn bounded_fallback_uses_only_the_configured_probe_and_directional_cap() {
        assert_eq!(bounded_fallback_quantity(1_000.0, 0.05, 0.1), 50.0);
        assert!((entry_adverse_bps(Side::Buy, 100.20, 100.0) - 20.0).abs() < 1e-9);
        assert!((entry_adverse_bps(Side::Sell, 99.80, 100.0) - 20.0).abs() < 1e-9);
        assert!(entry_adverse_bps(Side::Buy, 99.0, 100.0) < 0.0);
    }

    #[test]
    fn meaningful_partial_fill_becomes_a_managed_position() {
        assert!(managed_fill_threshold_reached(800.0, 1_000.0, 1.0, 0.80));
        assert!(managed_fill_threshold_reached(799.6, 1_000.0, 1.0, 0.80));
        assert!(!managed_fill_threshold_reached(799.4, 1_000.0, 1.0, 0.80));
        assert!(!managed_fill_threshold_reached(1_000.0, 1_000.0, 1.0, 0.0));
    }

    #[test]
    fn cancel_race_keeps_the_additional_fill() {
        let observed_before_cancel = 0.0;
        let cancel_response = serde_json::json!({"status":"CANCELED","executedQty":"55"});
        assert_eq!(
            reconciled_executed_quantity(observed_before_cancel, &cancel_response),
            55.0
        );
        let incomplete_cancel_response = serde_json::json!({"status":"CANCELED"});
        assert_eq!(
            reconciled_executed_quantity(12.0, &incomplete_cancel_response),
            12.0
        );
    }

    #[test]
    fn maker_wait_is_not_counted_as_holding_time() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"test:1","recipe":"test","side":"buy",
            "entry_ms":190_000,"signal_ms":10_000,"order_submitted_ms":20_000,
            "first_fill_ms":190_000,"entry_completed_ms":200_000,
            "entry_time_source":"exchange_user_trade_time",
            "stop_price":99.0,"max_hold_ms":60_000
        }))
        .unwrap();
        assert_eq!(meta.holding_started_ms(), 190_000);
        assert_eq!(200_000 - meta.holding_started_ms(), 10_000);

        let legacy: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"legacy:1","recipe":"test","side":"buy",
            "entry_ms":42_000,"stop_price":99.0,"max_hold_ms":60_000
        }))
        .unwrap();
        assert_eq!(legacy.holding_started_ms(), 42_000);
        assert_eq!(
            legacy.holding_time_source(),
            "legacy_entry_ms_unknown_semantics"
        );
    }

    #[test]
    fn fast_trend_deadline_does_not_extend_an_unprotected_mark_peak() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:1","recipe":"fast_trend_activation","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":0.01916,
            "extreme_price":0.0192736,"stop_price":0.01892865,
            "profit_shield_activation_pct":0.006037,"max_hold_ms":1_800_000
        }))
        .unwrap();
        assert_eq!(
            max_hold_review(&meta, 1_801_000, 0.01920),
            MaxHoldReview::Exit
        );
    }

    #[test]
    fn fast_trend_deadline_yields_to_an_armed_profit_stop() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:2","recipe":"fast_trend_activation","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "extreme_price":101.0,"stop_price":100.18,"break_even_armed":true,
            "profit_shield_activation_pct":0.005,"max_hold_ms":1_800_000
        }))
        .unwrap();
        assert_eq!(
            max_hold_review(&meta, 1_801_000, 101.0),
            MaxHoldReview::ReleaseToProtection
        );
    }

    #[test]
    fn fast_trend_deadline_yields_to_an_executable_floor_before_stop_update() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:floor","recipe":"fast_trend_activation","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "stop_price":98.0,"profit_shield_activation_pct":0.005,
            "max_hold_ms":600_000,
            "executable_profit":{
                "quantity":10.0,"peak_net_return":0.006,"current_net_return":0.005,
                "floor_net_return":0.002,"observed_ms":600_000,"exchange_ms":600_000,
                "exit_vwap":100.6
            }
        }))
        .unwrap();
        assert_eq!(
            max_hold_review(&meta, 601_000, 100.8),
            MaxHoldReview::ReleaseToProtection
        );
    }

    #[test]
    fn fast_trend_deadline_grants_one_short_grace_for_profitable_high_exit_impact() {
        let mut meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:progress","recipe":"fast_trend_activation","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "initial_quantity":10.0,"last_observed_quantity":10.0,
            "initial_risk_usd":20.0,"stop_price":98.0,
            "profit_shield_activation_pct":0.03,"max_hold_ms":600_000,
            "executable_profit":{
                "quantity":10.0,"peak_net_return":0.006,"current_net_return":0.005,
                "floor_net_return":null,"observed_ms":600_000,"exchange_ms":600_000,
                "exit_vwap":100.6
            }
        }))
        .unwrap();
        assert!(matches!(
            max_hold_review(&meta, 601_000, 100.8),
            MaxHoldReview::Extend { .. }
        ));
        meta.max_hold_reviews = 1;
        assert_eq!(max_hold_review(&meta, 601_000, 100.8), MaxHoldReview::Exit);
        meta.max_hold_reviews = 0;
        meta.pending_exit_reason = Some("max_hold".into());
        assert_eq!(max_hold_review(&meta, 601_000, 100.8), MaxHoldReview::Exit);
    }

    #[test]
    fn fast_trend_deadline_extends_meaningful_progress_through_a_noisy_quote() {
        let mut meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:tut","recipe":"fast_trend_activation","side":"sell",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":0.02053,
            "initial_quantity":213_938.0,"last_observed_quantity":213_938.0,
            "initial_risk_usd":37.42,"stop_price":0.0207049148,
            "extreme_price":0.02046,"max_hold_ms":600_000,
            "executable_profit":{
                "quantity":213_938.0,"peak_net_return":0.0025,
                "current_net_return":-0.003,"floor_net_return":null,
                "observed_ms":600_000,"exchange_ms":600_000,"exit_vwap":0.02057
            }
        }))
        .unwrap();
        let review = max_hold_review(&meta, 601_000, 0.02057);
        assert!(matches!(
            review,
            MaxHoldReview::Extend {
                extension_ms: FAST_TREND_PROGRESS_REVIEW_MS,
                reason: "meaningful_progress_with_bounded_adverse_move",
                ..
            }
        ));

        meta.max_hold_reviews = FAST_TREND_MAX_PROGRESS_REVIEWS;
        assert_eq!(
            max_hold_review(&meta, 601_000, 0.02057),
            MaxHoldReview::Exit
        );
    }

    #[test]
    fn fast_trend_deadline_exits_when_progress_has_fully_reversed() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:reversed","recipe":"fast_trend_activation","side":"sell",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "initial_quantity":10.0,"last_observed_quantity":10.0,
            "initial_risk_usd":20.0,"stop_price":102.0,
            "extreme_price":99.4,"max_hold_ms":600_000,
            "executable_profit":{
                "quantity":10.0,"peak_net_return":0.005,
                "current_net_return":-0.007,"floor_net_return":null,
                "observed_ms":600_000,"exchange_ms":600_000,"exit_vwap":100.7
            }
        }))
        .unwrap();
        assert_eq!(max_hold_review(&meta, 601_000, 100.7), MaxHoldReview::Exit);
    }

    #[test]
    fn fast_trend_deadline_never_extends_an_executable_loss() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:fresh-loss","recipe":"fast_trend_activation","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "initial_quantity":10.0,"last_observed_quantity":10.0,
            "initial_risk_usd":20.0,"stop_price":98.0,
            "profit_shield_activation_pct":0.03,"max_hold_ms":600_000,
            "executable_profit":{
                "quantity":10.0,"peak_net_return":0.001,"current_net_return":-0.003,
                "floor_net_return":null,"observed_ms":600_000,"exchange_ms":600_000,
                "exit_vwap":99.8
            }
        }))
        .unwrap();
        assert_eq!(max_hold_review(&meta, 601_000, 99.9), MaxHoldReview::Exit);
    }

    #[test]
    fn fast_trend_deadline_exits_profitable_positions_when_impact_is_normal() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:cheap-exit","recipe":"fast_trend_activation","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "initial_quantity":10.0,"last_observed_quantity":10.0,
            "initial_risk_usd":20.0,"stop_price":98.0,
            "profit_shield_activation_pct":0.03,"max_hold_ms":600_000,
            "executable_profit":{
                "quantity":10.0,"peak_net_return":0.006,"current_net_return":0.005,
                "floor_net_return":null,"observed_ms":600_000,"exchange_ms":600_000,
                "exit_vwap":100.6
            }
        }))
        .unwrap();
        assert_eq!(max_hold_review(&meta, 601_000, 100.65), MaxHoldReview::Exit);
    }

    #[test]
    fn fast_trend_deadline_still_exits_a_stagnant_position() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"fast:3","recipe":"fast_trend_activation","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "extreme_price":100.1,"stop_price":98.8,
            "profit_shield_activation_pct":0.006,"max_hold_ms":1_800_000
        }))
        .unwrap();
        assert_eq!(
            max_hold_review(&meta, 1_801_000, 100.0),
            MaxHoldReview::Exit
        );
    }

    #[test]
    fn fixed_horizon_position_never_receives_a_hold_extension() {
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"liquidation_exhaustion_reversal:ALTUSDT:1000",
            "recipe":"liquidation_exhaustion_reversal","side":"buy",
            "entry_ms":1_000,"first_fill_ms":1_000,"entry_price":100.0,
            "extreme_price":110.0,"stop_price":98.0,"break_even_armed":true,
            "profit_shield_activation_pct":0.001,"max_hold_ms":900_000,
            "fixed_time_exit":true
        }))
        .unwrap();
        assert_eq!(max_hold_review(&meta, 901_000, 110.0), MaxHoldReview::Exit);
    }

    #[test]
    fn managed_stop_only_plan_requires_complete_protection_and_survives_restart() {
        let mut plan = guarded_plan(Side::Buy);
        plan.take_profit_prices.clear();
        plan.fixed_time_exit = false;
        plan.max_hold_ms = 900_000;
        plan.profit_shield_activation_pct = Some(0.0025);
        plan.break_even_buffer_pct = 0.001;
        plan.trailing_activation_pct = Some(0.005);
        plan.trailing_distance_pct = Some(0.003);
        let rules = SymbolRules {
            quantity_step: 1.0,
            min_quantity: 1.0,
            max_limit_quantity: 1_000.0,
            max_market_quantity: 1_000.0,
            price_tick: 0.01,
            min_notional: 5.0,
        };
        assert!(validate_entry_quantity_and_exits(&plan, 10.0, 100.0, &rules).is_ok());
        plan.trailing_distance_pct = None;
        assert!(validate_entry_quantity_and_exits(&plan, 10.0, 100.0, &rules).is_err());
        assert!(!managed_stop_only_exit(
            900_000,
            Some(f64::NAN),
            0.001,
            Some(0.005),
            Some(0.003)
        ));
        assert!(!managed_stop_only_exit(
            900_000,
            Some(0.0025),
            0.003,
            Some(0.005),
            Some(0.003)
        ));
        let meta: ExecutionMeta = serde_json::from_value(serde_json::json!({
            "candidate_id":"liq:test", "recipe":"liquidation_exhaustion_reversal",
            "side":"buy", "entry_ms":1000, "first_fill_ms":1000,
            "stop_price":98.0, "max_hold_ms":900000,
            "profit_shield_activation_pct":0.0025, "break_even_buffer_pct":0.001,
            "trailing_activation_pct":0.005, "trailing_distance_pct":0.003
        }))
        .unwrap();
        let restored: ExecutionMeta =
            serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
        assert!(managed_stop_only_exit(
            restored.max_hold_ms,
            restored.profit_shield_activation_pct,
            restored.break_even_buffer_pct,
            restored.trailing_activation_pct,
            restored.trailing_distance_pct
        ));
        assert_ne!(
            max_hold_review(&restored, 61_000, 100.0),
            MaxHoldReview::Exit
        );
        assert_eq!(
            max_hold_review(&restored, 901_000, 100.0),
            MaxHoldReview::Exit
        );
    }

    #[test]
    fn liquidation_cooldown_is_symbol_scoped_and_restart_safe() {
        let seen = BTreeSet::from([
            "liquidation_exhaustion_reversal:ALTUSDT:1000".to_string(),
            "liquidation_exhaustion_reversal:OTHERUSDT:29000".to_string(),
        ]);
        assert!(liquidation_cooldown_active(
            &seen, "ALTUSDT", 20_000, 30_000
        ));
        assert!(!liquidation_cooldown_active(
            &seen, "ALTUSDT", 31_000, 30_000
        ));
        assert!(!liquidation_cooldown_active(
            &seen, "NEWUSDT", 20_000, 30_000
        ));
    }

    #[test]
    fn fifty_five_percent_fill_is_managed_only_when_every_exit_is_executable() {
        let rules = SymbolRules {
            quantity_step: 1.0,
            min_quantity: 1.0,
            max_limit_quantity: 1_000.0,
            max_market_quantity: 1_000.0,
            price_tick: 0.01,
            min_notional: 5.0,
        };
        assert!(partial_fill_is_manageable(
            55.0,
            100.0,
            1.0,
            0.20,
            &rules,
            &[(1.02, 0.40), (1.04, 0.60)]
        ));
        assert!(!partial_fill_is_manageable(
            4.0,
            100.0,
            1.0,
            0.20,
            &rules,
            &[(1.02, 0.40), (1.04, 0.60)]
        ));
        let coarse = SymbolRules {
            quantity_step: 10.0,
            min_quantity: 10.0,
            max_limit_quantity: 1_000.0,
            max_market_quantity: 1_000.0,
            price_tick: 0.01,
            min_notional: 5.0,
        };
        assert!(!partial_fill_is_manageable(
            20.0,
            100.0,
            1.0,
            0.20,
            &coarse,
            &[(1.02, 0.33), (1.04, 0.67)]
        ));
    }

    #[test]
    fn exchange_max_quantity_is_capped_only_when_the_remaining_trade_is_meaningful() {
        let rules = SymbolRules {
            quantity_step: 1.0,
            min_quantity: 1.0,
            max_limit_quantity: 600.0,
            max_market_quantity: 600.0,
            price_tick: 0.01,
            min_notional: 5.0,
        };
        assert_eq!(
            cap_entry_quantity(1_000.0, rules.max_limit_quantity, rules.quantity_step).unwrap(),
            600.0
        );
        let error = cap_entry_quantity(2_000.0, rules.max_market_quantity, rules.quantity_step)
            .expect_err("a tiny exchange-capped position should be skipped");
        assert!(error.to_string().contains("30.0%"));
    }

    #[test]
    fn pending_entry_state_survives_restart_serialization() {
        let plan = guarded_plan(Side::Buy);
        let mut state = DemoState::default();
        state.pending_entries.insert(
            plan.symbol.clone(),
            PendingEntryState {
                plan,
                recipe: "trend_continuation".into(),
                signal_ms: 10,
                discovery_latency_ms: Some(7),
                size_multiplier: 1.0,
                requested_quantity: 10.0,
                active_client_id: "greed-entry-1".into(),
                passive_price: 99.0,
                order_submitted_ms: 20,
                deadline_ms: 120_000,
                next_reprice_ms: 35_000,
                reprice_attempt: 1,
                first_fill_ms: Some(30),
                first_fill_time_source: Some("exchange_user_trade_time".into()),
                preliminary_stop_algo_id: Some(123),
                preliminary_stop_price: Some(98.75),
                guard_unavailable_since_ms: None,
                micro_reversal_since_ms: None,
            },
        );
        let restored: DemoState = serde_json::from_slice(&serde_json::to_vec(&state).unwrap())
            .expect("pending order state must be restart-safe");
        let pending = restored.pending_entries.get("TESTUSDT").unwrap();
        assert_eq!(pending.active_client_id, "greed-entry-1");
        assert_eq!(pending.first_fill_ms, Some(30));
        assert_eq!(pending.preliminary_stop_algo_id, Some(123));
    }

    #[test]
    fn pending_buy_is_canceled_after_it_falls_through_the_pullback_limit() {
        let plan = guarded_plan(Side::Buy);
        assert_eq!(
            pending_entry_guard_state(&plan, 99.0, Some(0.0), Some(0.0)),
            EntryGuardState::Healthy
        );
        let guard = pending_entry_guard_state(&plan, 98.60, Some(0.0), Some(0.0));
        assert!(
            matches!(guard, EntryGuardState::StructuralInvalidation(reason) if reason.contains("40.4 bps"))
        );
    }

    #[test]
    fn prom_regression_cancels_the_stale_rebound_entry() {
        let mut plan = guarded_plan(Side::Buy);
        plan.symbol = "PROMUSDT".into();
        plan.reference_price = 5.381;
        plan.entry_limit = Some(5.34747);
        let guard = pending_entry_guard_state(&plan, 5.219, Some(-0.19), Some(-14.4));
        assert!(
            matches!(guard, EntryGuardState::StructuralInvalidation(reason) if reason.contains("overshot entry limit"))
        );
    }

    #[test]
    fn pending_sell_uses_the_same_directional_overshoot_guard() {
        let mut plan = guarded_plan(Side::Sell);
        plan.entry_limit = Some(101.0);
        assert_eq!(
            pending_entry_guard_state(&plan, 101.0, Some(0.0), Some(0.0)),
            EntryGuardState::Healthy
        );
        assert!(matches!(
            pending_entry_guard_state(&plan, 101.40, Some(0.0), Some(0.0)),
            EntryGuardState::StructuralInvalidation(_)
        ));
    }

    #[test]
    fn pending_entry_requires_flow_and_price_to_reverse_together() {
        let plan = guarded_plan(Side::Buy);
        assert_eq!(
            pending_entry_guard_state(&plan, 99.0, Some(-0.20), Some(2.0)),
            EntryGuardState::Healthy
        );
        assert!(matches!(
            pending_entry_guard_state(&plan, 99.0, Some(-0.20), Some(-4.0)),
            EntryGuardState::MicroReversal(reason) if reason.contains("live flow")
        ));
        assert_eq!(
            pending_entry_guard_state(&plan, 99.0, None, None),
            EntryGuardState::Healthy
        );
    }

    #[test]
    fn micro_reversal_must_persist_before_canceling_a_maker_order() {
        let mut since = None;
        assert_eq!(
            persistent_guard_reason(&mut since, 10_000, 15_000, "flow reversed"),
            None
        );
        assert_eq!(
            persistent_guard_reason(&mut since, 24_999, 15_000, "flow reversed"),
            None
        );
        assert_eq!(
            persistent_guard_reason(&mut since, 25_000, 15_000, "flow reversed"),
            Some("flow reversed continuously for 15000ms (need 15000ms)".into())
        );
    }

    #[test]
    fn adaptive_maker_client_ids_stay_within_binance_limit() {
        let base = client_order_id("entry", "alt.recipe:test:cycle-123");
        let repriced = reprice_client_id(&base, MAX_MAKER_REPRICES);
        assert!(repriced.len() <= 36);
    }

    #[test]
    fn every_execution_client_id_stays_within_binance_limit() {
        for prefix in [
            "entry",
            "fallback",
            "pendingstop",
            "stop",
            "attemptclose",
            "raceclose",
            "protectfail",
            "close",
            "persistfail",
            "underfill",
        ] {
            let value = client_order_id(prefix, "trend_continuation:1000BONKUSDT:1788703199999");
            assert!(
                value.len() <= 36,
                "{prefix} generated an invalid {}-character id: {value}",
                value.len()
            );
            assert!(value.starts_with(&format!("greed-{prefix}-")));
        }
    }

    #[test]
    fn completed_or_actionable_fills_skip_provisional_protection() {
        assert!(pending_fill_needs_provisional_stop(
            10.0, false, false, false, false
        ));
        assert!(!pending_fill_needs_provisional_stop(
            10.0, true, false, false, false
        ));
        assert!(!pending_fill_needs_provisional_stop(
            80.0, false, true, false, false
        ));
        assert!(!pending_fill_needs_provisional_stop(
            10.0, false, false, true, false
        ));
        assert!(!pending_fill_needs_provisional_stop(
            10.0, false, false, false, true
        ));
        assert!(!pending_fill_needs_provisional_stop(
            0.0, false, false, false, false
        ));
    }

    #[test]
    fn cancel_reconciliation_accepts_only_terminal_order_states() {
        for status in ["FILLED", "CANCELED", "EXPIRED", "REJECTED"] {
            assert!(order_value_is_terminal(
                &serde_json::json!({"status":status})
            ));
        }
        for status in ["NEW", "PARTIALLY_FILLED", "PENDING_CANCEL", "UNKNOWN"] {
            assert!(!order_value_is_terminal(
                &serde_json::json!({"status":status})
            ));
        }
        assert!(!order_value_is_terminal(&serde_json::json!({})));
    }

    #[test]
    fn legacy_pending_state_without_stop_price_remains_compatible() {
        let plan = guarded_plan(Side::Buy);
        let pending: PendingEntryState = serde_json::from_value(serde_json::json!({
            "plan":plan,
            "recipe":"trend_continuation",
            "signal_ms":10,
            "size_multiplier":1.0,
            "requested_quantity":10.0,
            "active_client_id":"greed-entry-1",
            "passive_price":99.0,
            "order_submitted_ms":20,
            "deadline_ms":120000,
            "next_reprice_ms":35000,
            "preliminary_stop_algo_id":123
        }))
        .expect("old pending states must deserialize after the stop-price field is added");
        assert_eq!(pending.preliminary_stop_algo_id, Some(123));
        assert_eq!(pending.preliminary_stop_price, None);
    }

    #[test]
    fn exchange_rules_only_admit_tradable_usdt_perpetuals() {
        let info = serde_json::json!({"symbols":[
            {"symbol":"BTCUSDT","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDT","filters":[]},
            {"symbol":"OLDUSDT","status":"SETTLING","contractType":"PERPETUAL","quoteAsset":"USDT","filters":[]},
            {"symbol":"BTCUSDC","status":"TRADING","contractType":"PERPETUAL","quoteAsset":"USDC","filters":[]},
            {"symbol":"BTCUSDT_260925","status":"TRADING","contractType":"CURRENT_QUARTER","quoteAsset":"USDT","filters":[]}
        ]});
        let rules = parse_rules(&info).unwrap();
        assert_eq!(rules.len(), 1);
        assert!(rules.contains_key("BTCUSDT"));
    }

    #[test]
    fn only_a_latest_loss_arms_the_symbol_cooldown() {
        let loss = ExecutionOutcome {
            exit_ms: 1_000,
            pnl_usd: -2.0,
            pnl_r: Some(-1.0),
            fill_ratio: Some(1.0),
            ..ExecutionOutcome::default()
        };
        assert_eq!(loss_cooldown_until(Some(&[loss]), 180), Some(10_801_000));
        let recovered = [
            ExecutionOutcome {
                exit_ms: 1_000,
                pnl_usd: -2.0,
                pnl_r: Some(-1.0),
                fill_ratio: Some(1.0),
                ..ExecutionOutcome::default()
            },
            ExecutionOutcome {
                exit_ms: 2_000,
                pnl_usd: 1.0,
                pnl_r: Some(0.5),
                fill_ratio: Some(1.0),
                ..ExecutionOutcome::default()
            },
        ];
        assert_eq!(loss_cooldown_until(Some(&recovered), 180), None);
    }

    #[test]
    fn executable_profit_reversal_chain_stops_after_two_direction_changes() {
        assert_eq!(
            next_profit_reversal_count(0, "executable_profit_protection"),
            Some(1)
        );
        assert_eq!(
            next_profit_reversal_count(1, "executable_profit_protection"),
            Some(2)
        );
        assert_eq!(
            next_profit_reversal_count(2, "executable_profit_protection"),
            None
        );
        assert_eq!(next_profit_reversal_count(0, "initial_stop"), None);
    }

    #[test]
    fn legacy_reentry_campaign_starts_outside_a_profit_reversal_chain() {
        let campaign: TrendReentryCampaign = serde_json::from_value(serde_json::json!({
            "source_candidate_id":"trend_continuation:HYPEUSDT:1",
            "symbol":"HYPEUSDT",
            "side":"sell",
            "armed_ms":1,
            "expires_ms":2,
            "favorable_extreme":80.0,
            "first_exit_price":82.0
        }))
        .expect("old campaign state should remain readable");
        assert!(!campaign.immediate_profit_reversal);
        assert_eq!(campaign.profit_reversal_count, 0);
    }

    #[test]
    fn performance_gate_uses_r_and_excludes_tiny_partial_fills() {
        let full = ExecutionOutcome {
            exit_ms: 1_000,
            pnl_usd: 20.0,
            pnl_r: Some(1.0),
            fill_ratio: Some(0.95),
            ..ExecutionOutcome::default()
        };
        let tiny = ExecutionOutcome {
            exit_ms: 2_000,
            pnl_usd: -0.50,
            pnl_r: Some(-1.0),
            fill_ratio: Some(0.05),
            ..ExecutionOutcome::default()
        };
        let legacy = ExecutionOutcome {
            exit_ms: 3_000,
            pnl_usd: -4.0,
            pnl_r: None,
            fill_ratio: None,
            ..ExecutionOutcome::default()
        };
        assert!(full.performance_eligible());
        assert!(!tiny.performance_eligible());
        assert!(legacy.performance_eligible());
        assert_eq!(full.performance_value(), 1.0);
        assert_eq!(legacy.performance_value(), -4.0);
    }

    #[test]
    fn legacy_outcome_schema_remains_readable() {
        let outcome: ExecutionOutcome = serde_json::from_value(serde_json::json!({
            "exit_ms": 1_000,
            "pnl_usd": 2.5
        }))
        .expect("legacy outcome should deserialize");
        assert_eq!(outcome.pnl_r, None);
        assert_eq!(outcome.fill_ratio, None);
        assert!(outcome.performance_eligible());
        assert_eq!(outcome.performance_value(), 2.5);
    }

    #[test]
    fn fast_activation_has_one_slot_outside_the_standard_pool() {
        assert!(!recipe_slot_full(FAST_TREND_ACTIVATION_RECIPE, 0, 3, 3));
        assert!(recipe_slot_full(FAST_TREND_ACTIVATION_RECIPE, 1, 0, 3));
        assert!(!recipe_slot_full("trend_continuation", 1, 2, 3));
        assert!(recipe_slot_full("trend_continuation", 0, 3, 3));
    }
}
