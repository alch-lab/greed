use crate::config::{ExecutionConfig, PortfolioConfig};
use anyhow::{anyhow, Context, Result};
use greed_kernel::{AccountFrame, Artifact, GraphEvaluation, MarketFrame, Side};
use greed_strategy::RiskConfig;
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
const MIN_PERFORMANCE_FILL_RATIO: f64 = 0.20;
const PERFORMANCE_BASIS_VERSION: u32 = 1;

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
}

#[derive(Debug, Clone)]
struct SymbolRules {
    quantity_step: f64,
    min_quantity: f64,
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
    entry_ms: i64,
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
    max_hold_ms: i64,
    #[serde(default)]
    exit_requested: bool,
    #[serde(default)]
    pending_exit_reason: Option<String>,
    #[serde(default)]
    stop_algo_id: Option<i64>,
    #[serde(default)]
    stop_reason: Option<String>,
    #[serde(default)]
    take_profit_order_ids: Vec<i64>,
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
    recipe_outcomes: BTreeMap<String, Vec<ExecutionOutcome>>,
    symbol_outcomes: BTreeMap<String, Vec<ExecutionOutcome>>,
    performance_epoch: u32,
    performance_basis_version: u32,
    execution_halt_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionOutcome {
    exit_ms: i64,
    pnl_usd: f64,
    #[serde(default)]
    pnl_r: Option<f64>,
    #[serde(default)]
    fill_ratio: Option<f64>,
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
    pub entry_price: f64,
    pub quantity: f64,
    pub initial_quantity: f64,
    pub remaining_quantity: f64,
    pub stop_price: f64,
    pub take_profit_prices: Vec<(f64, f64)>,
    pub max_hold_ms: i64,
    pub break_even_armed: bool,
    pub profit_shield_activation_pct: Option<f64>,
    pub extreme_price: Option<f64>,
    pub trailing_activation_pct: Option<f64>,
    pub trailing_distance_pct: Option<f64>,
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
    mode: &'static str,
    maker_attempted: bool,
    maker_wait_ms: i64,
    maker_reprices: u8,
    final_maker_limit: Option<f64>,
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
}

#[derive(Debug)]
enum PostOnlyEntry {
    Filled {
        order: Value,
        waited_ms: i64,
        reprices: u8,
        final_limit: f64,
    },
    Unfilled {
        waited_ms: i64,
        reprices: u8,
        final_limit: f64,
    },
}

pub struct BinanceDemoExecution {
    client: Client,
    config: ExecutionConfig,
    portfolio: PortfolioConfig,
    risk: RiskConfig,
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
}

impl BinanceDemoExecution {
    pub async fn connect(
        config: ExecutionConfig,
        portfolio: PortfolioConfig,
        risk: RiskConfig,
        state_path: String,
        proxy: Option<&str>,
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
        let mut state: DemoState = fs::read_to_string(&state_path)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        let performance_epoch_reset = state.performance_epoch != risk.rolling_pf_epoch;
        if performance_epoch_reset {
            state.recipe_outcomes.clear();
            state.symbol_outcomes.clear();
            state.performance_epoch = risk.rolling_pf_epoch;
            state.execution_halt_reason = None;
        }
        let performance_basis_reset = state.performance_basis_version != PERFORMANCE_BASIS_VERSION;
        if performance_basis_reset {
            // Dollar PF and risk-normalized R PF cannot share one rolling
            // window. Reset only the internal recipe sample window; account
            // baseline, run history, PnL curve, symbol cooldowns and exits stay.
            state.recipe_outcomes.clear();
            state.performance_basis_version = PERFORMANCE_BASIS_VERSION;
        }
        let mut value = Self {
            client,
            config,
            portfolio,
            risk,
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
        };
        value.initialize().await?;
        if value.performance_epoch_reset {
            let account_is_flat = value
                .account
                .as_ref()
                .is_some_and(|account| account.positions.is_empty());
            if !account_is_flat || !value.state.positions.is_empty() {
                return Err(anyhow!(
                    "cannot start a new performance epoch while Binance demo positions are open"
                ));
            }
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

    pub async fn sync(&mut self) -> Result<Vec<ExchangeEvent>> {
        let value = self.signed(Method::GET, "/fapi/v2/account", vec![]).await;
        match value {
            Ok(value) => {
                let account = parse_account(&value)?;
                let foreign: Vec<_> = account
                    .positions
                    .keys()
                    .filter(|symbol| !self.state.positions.contains_key(*symbol))
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
                let now_ms = chrono::Utc::now().timestamp_millis();
                let expired: Vec<_> = self
                    .state
                    .positions
                    .iter()
                    .filter(|(symbol, meta)| {
                        !meta.exit_requested
                            && meta.max_hold_ms > 0
                            && now_ms - meta.entry_ms >= meta.max_hold_ms
                            && account.positions.contains_key(*symbol)
                    })
                    .map(|(symbol, _)| symbol.clone())
                    .collect();
                let mut events = Vec::new();
                let mut protected: BTreeMap<String, (bool, bool)> = BTreeMap::new();
                let mut stop_orders: BTreeMap<String, ProtectiveOrderId> = BTreeMap::new();
                for symbol in account.positions.keys() {
                    let open_orders = self
                        .signed(
                            Method::GET,
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
                        .signed(
                            Method::GET,
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
                            meta.entry_ms,
                            meta.candidate_id.clone(),
                            meta.recipe.clone(),
                            meta.side,
                        ))
                    })
                    .collect();
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
                    }
                }
                for (symbol, position) in &account.positions {
                    let Some(meta) = self.state.positions.get_mut(symbol) else {
                        continue;
                    };
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
                    let favorable = meta.side.sign()
                        * (meta.extreme_price / meta.entry_price.max(f64::EPSILON) - 1.0);
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
                        && meta
                            .trailing_activation_pct
                            .is_some_and(|activation| favorable >= activation)
                    {
                        if let Some(distance) = meta.trailing_distance_pct {
                            let trailing = meta.extreme_price * (1.0 - meta.side.sign() * distance);
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
                for (symbol, desired, reason, position, stop_order_id) in protection_updates {
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
                            if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                meta.stop_price = desired;
                                meta.break_even_armed = true;
                                meta.stop_algo_id = Some(stop_algo_id);
                                meta.stop_reason = Some(reason.into());
                            }
                            events.push(ExchangeEvent {
                                kind: "exchange_protection_updated".into(),
                                payload: serde_json::json!({"ts_ms":now_ms,"symbol":symbol,"stop_price":desired,"reason":reason,"venue":"binance_demo"}),
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
                            && !protected.get(*symbol).is_some_and(|(stop, take)| {
                                *stop
                                    && (*take
                                        || self.state.positions.get(*symbol).is_some_and(|meta| {
                                            meta.break_even_armed
                                                || meta.last_observed_quantity
                                                    < meta.initial_quantity - f64::EPSILON
                                        }))
                            })
                    })
                    .cloned()
                    .collect();
                for symbol in unprotected {
                    if let Some(position) = account.positions.get(&symbol) {
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
                            .trade_summary(&symbol, meta.entry_ms, meta.side)
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
                        let performance_sample = statistical_fill_ratio
                            .is_some_and(|value| value >= MIN_PERFORMANCE_FILL_RATIO);
                        if let Some(summary) = summary.as_ref() {
                            let trade_pnl_usd = summary.net_pnl_usd;
                            let outcome = ExecutionOutcome {
                                exit_ms: now_ms,
                                pnl_usd: trade_pnl_usd,
                                pnl_r,
                                fill_ratio: statistical_fill_ratio.or(Some(0.0)),
                            };
                            self.state
                                .recipe_outcomes
                                .entry(gate_key(&meta.recipe, meta.side))
                                .or_default()
                                .push(outcome.clone());
                            self.state
                                .symbol_outcomes
                                .entry(symbol.clone())
                                .or_default()
                                .push(outcome);
                        }
                        let mfe_pct = meta.side.sign()
                            * (meta.extreme_price / meta.entry_price.max(f64::EPSILON) - 1.0);
                        let mae_pct = (-meta.side.sign()
                            * (meta.adverse_price / meta.entry_price.max(f64::EPSILON) - 1.0))
                            .max(0.0);
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
                                "hold_ms": now_ms - meta.entry_ms,
                                "reason": exit_reason,
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
        Ok(AccountFrame {
            equity_usd: equity,
            cash_usd: self.portfolio.initial_equity_usd + account.available_balance - baseline,
            realized_pnl_usd: realized,
            peak_equity_usd: peak,
            risk_day_start_equity_usd: self.state.risk_day_start_equity_usd.unwrap_or(equity),
            gross_exposure_usd: gross,
            open_positions: account.positions.len(),
        })
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
        RecipeGateStatus {
            allowed: !failed || next_probe_ms.is_some_and(|value| now_ms >= value),
            completed_trades: window.len(),
            excluded_partial_trades: outcomes.len().saturating_sub(eligible.len()),
            rolling_profit_factor,
            rolling_net_pnl_usd: window.iter().map(|outcome| outcome.pnl_usd).sum(),
            next_probe_ms,
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
        ["sfp_reversal", "trend_continuation", "ignition_sprint"]
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
                let notional = position.quantity.abs() * position.mark_price;
                let entry_notional = position.quantity.abs() * position.entry_price;
                (
                    symbol.clone(),
                    DemoPositionSnapshot {
                        candidate_id: meta
                            .map(|value| value.candidate_id.clone())
                            .unwrap_or_default(),
                        recipe: meta
                            .map(|value| value.recipe.clone())
                            .unwrap_or_else(|| "external".into()),
                        symbol: symbol.clone(),
                        side: position.side,
                        entry_ms: meta.map(|value| value.entry_ms).unwrap_or_default(),
                        entry_price: position.entry_price,
                        quantity: position.quantity.abs(),
                        initial_quantity: meta
                            .map(|value| value.initial_quantity)
                            .unwrap_or(position.quantity.abs()),
                        remaining_quantity: position.quantity.abs(),
                        stop_price: meta.map(|value| value.stop_price).unwrap_or_default(),
                        take_profit_prices: meta
                            .map(|value| {
                                if value.take_profit_prices.is_empty() {
                                    vec![(value.take_profit_price, 1.0)]
                                } else {
                                    value.take_profit_prices.clone()
                                }
                            })
                            .unwrap_or_default(),
                        max_hold_ms: meta.map(|value| value.max_hold_ms).unwrap_or_default(),
                        break_even_armed: meta.map(|value| value.break_even_armed).unwrap_or(false),
                        profit_shield_activation_pct: meta
                            .and_then(|value| value.profit_shield_activation_pct),
                        extreme_price: meta.map(|value| value.extreme_price),
                        trailing_activation_pct: meta
                            .and_then(|value| value.trailing_activation_pct),
                        trailing_distance_pct: meta.and_then(|value| value.trailing_distance_pct),
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
    ) -> Vec<ExchangeEvent> {
        let mut events = Vec::new();
        if self.state.execution_halt_reason.is_some() {
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
            if self.account.as_ref().is_some_and(|account| {
                account.positions.contains_key(&plan.symbol)
                    || self.state.positions.contains_key(&plan.symbol)
                    || self.state.positions.len() >= self.portfolio.max_positions
            }) {
                continue;
            }
            let recipe = candidate
                .map(|value| value.recipe.clone())
                .unwrap_or_else(|| "unknown".into());
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
            if candidate.is_some_and(|value| {
                self.latest_symbol_exit_ms(&plan.symbol)
                    .is_some_and(|exit_ms| value.signal_ms <= exit_ms)
            }) {
                self.state.seen.insert(plan.candidate_id.clone());
                events.push(ExchangeEvent {
                    kind: "exchange_plan_rejected".into(),
                    payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"symbol":plan.symbol,"side":plan.side,"reason":"signal_precedes_latest_symbol_exit","signal_ms":candidate.map(|value| value.signal_ms),"latest_symbol_exit_ms":self.latest_symbol_exit_ms(&plan.symbol),"venue":"binance_demo","paper_only":true}),
                });
                continue;
            }
            if self
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
            // Claim the candidate before touching the exchange. At-most-once is the
            // safe failure mode: an entry can fill even when a later protection or
            // reconciliation request fails. Retrying the same signal would open and
            // flatten it repeatedly, paying spread and fees on every frame.
            self.state.seen.insert(plan.candidate_id.clone());
            self.save().ok();
            let attempt_started_ms = chrono::Utc::now().timestamp_millis();
            match self.place_bracket(plan, size_multiplier).await {
                Ok(fill) => {
                    let planned_notional_usd = plan.notional_usd * size_multiplier;
                    let actual_notional_usd = fill.entry_price * fill.quantity;
                    let fill_ratio = actual_notional_usd / planned_notional_usd.max(f64::EPSILON);
                    self.state.positions.insert(
                        plan.symbol.clone(),
                        ExecutionMeta {
                            candidate_id: plan.candidate_id.clone(),
                            recipe: recipe.clone(),
                            side: plan.side,
                            entry_ms: frame.as_of_ms,
                            entry_price: fill.entry_price,
                            initial_quantity: fill.quantity,
                            last_observed_quantity: fill.quantity,
                            cumulative_reported_fee_usd: 0.0,
                            cumulative_reported_pnl_usd: 0.0,
                            planned_notional_usd: Some(planned_notional_usd),
                            fill_ratio: Some(fill_ratio),
                            initial_risk_usd: Some(
                                fill.quantity * (fill.entry_price - fill.stop_price).abs(),
                            ),
                            stop_price: fill.stop_price,
                            take_profit_price: fill
                                .take_profit_prices
                                .last()
                                .map(|value| value.0)
                                .unwrap_or_default(),
                            take_profit_prices: fill.take_profit_prices.clone(),
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
                            profit_shield_activation_pct: plan.profit_shield_activation_pct,
                            break_even_armed: false,
                            extreme_price: fill.entry_price,
                            adverse_price: fill.entry_price,
                            trailing_activation_pct: plan.trailing_activation_pct,
                            trailing_distance_pct: plan.trailing_distance_pct,
                            max_hold_ms: plan.max_hold_ms,
                            exit_requested: false,
                            pending_exit_reason: None,
                            stop_algo_id: Some(fill.stop_algo_id),
                            stop_reason: Some("initial_stop".into()),
                            take_profit_order_ids: fill.take_profit_order_ids.clone(),
                        },
                    );
                    self.save().ok();
                    let signal_to_order_ms = candidate
                        .map(|value| frame.as_of_ms.saturating_sub(value.signal_ms))
                        .unwrap_or_default();
                    let discovery_latency_ms = candidate
                        .and_then(|value| value.tags.get("discovery_latency_ms"))
                        .and_then(|value| value.parse::<i64>().ok());
                    events.push(ExchangeEvent {
                        kind: "exchange_entry".into(),
                        payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"lane":recipe,"symbol":plan.symbol,"side":plan.side,"entry_mode":fill.entry_mode,"entry_price_source":fill.entry_price_source,"maker_attempted":fill.maker_attempted,"maker_wait_ms":fill.maker_wait_ms,"maker_reprices":fill.maker_reprices,"requested_limit":plan.entry_limit,"final_maker_limit":fill.final_maker_limit,"entry_price":fill.entry_price,"quantity":fill.quantity,"notional_usd":actual_notional_usd,"actual_notional_usd":actual_notional_usd,"planned_notional_usd":planned_notional_usd,"fill_ratio":fill_ratio,"partial_fill":fill_ratio < 0.999,"probe_size_multiplier":size_multiplier,"spread_bps":cost.spread_bps,"expected_exit_slippage_bps":cost.expected_exit_slippage_bps,"estimated_round_trip_cost_bps":cost.estimated_round_trip_cost_bps,"gross_target_bps":cost.gross_target_bps,"target_to_cost_ratio":cost.target_to_cost_ratio,"order_id":fill.order_id,"stop_algo_id":fill.stop_algo_id,"take_profit_order_ids":fill.take_profit_order_ids,"signal_to_order_ms":signal_to_order_ms,"discovery_latency_ms":discovery_latency_ms,"venue":"binance_demo","paper_only":true}),
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
                    events.push(ExchangeEvent {
                        kind: "exchange_order_rejected".into(),
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
                            "spread_bps":cost.spread_bps,
                            "expected_exit_slippage_bps":cost.expected_exit_slippage_bps,
                            "estimated_round_trip_cost_bps":cost.estimated_round_trip_cost_bps,
                            "gross_target_bps":cost.gross_target_bps,
                            "target_to_cost_ratio":cost.target_to_cost_ratio,
                            "reason":error.to_string(),
                            "attempt_exit_price":attempt.as_ref().map(|value| value.exit_price),
                            "attempt_exit_quantity":attempt.as_ref().map(|value| value.exit_quantity),
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

    async fn place_bracket(
        &self,
        plan: &greed_kernel::PositionPlan,
        size_multiplier: f64,
    ) -> Result<BracketExecution> {
        let rules = self
            .rules
            .get(&plan.symbol)
            .ok_or_else(|| anyhow!("missing exchange rules for {}", plan.symbol))?;
        let quantity = floor_step(
            plan.notional_usd * size_multiplier / plan.reference_price,
            rules.quantity_step,
        );
        if quantity < rules.min_quantity || quantity * plan.reference_price < rules.min_notional {
            return Err(anyhow!(
                "order is below Binance quantity or notional minimum"
            ));
        }
        if plan.take_profit_prices.is_empty() {
            return Err(anyhow!("position plan requires at least one take profit"));
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
        self.prepare_symbol_for_entry(&plan.symbol).await?;
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
        let client_id = client_order_id("entry", &plan.candidate_id);
        let entry = if let Some(limit) = plan.entry_limit {
            let price = if plan.side == Side::Buy {
                floor_step(limit, rules.price_tick)
            } else {
                ceil_step(limit, rules.price_tick)
            };
            match self
                .place_post_only_entry(plan, quantity, price, rules, &client_id)
                .await?
            {
                PostOnlyEntry::Filled {
                    order,
                    waited_ms,
                    reprices,
                    final_limit,
                } => EntryExecution {
                    order,
                    mode: if reprices > 0 {
                        "adaptive_maker_limit"
                    } else {
                        "maker_limit"
                    },
                    maker_attempted: true,
                    maker_wait_ms: waited_ms,
                    maker_reprices: reprices,
                    final_maker_limit: Some(final_limit),
                },
                PostOnlyEntry::Unfilled {
                    waited_ms,
                    reprices,
                    final_limit,
                } if plan.taker_fallback => {
                    let ticker = self
                        .public_get_params(
                            "/fapi/v1/ticker/bookTicker",
                            &[("symbol", plan.symbol.as_str())],
                        )
                        .await?;
                    let executable = if plan.side == Side::Buy {
                        parse_f64(&ticker, "askPrice")
                    } else {
                        parse_f64(&ticker, "bidPrice")
                    }
                    .ok_or_else(|| anyhow!("book ticker is missing an executable price"))?;
                    let adverse_bps = plan.side.sign()
                        * (executable / plan.reference_price.max(f64::EPSILON) - 1.0)
                        * 10_000.0;
                    if adverse_bps > plan.max_entry_adverse_bps {
                        return Err(anyhow!(
                            "maker entry expired and price drifted {adverse_bps:.1} bps against the signal (max {:.1})",
                            plan.max_entry_adverse_bps
                        ));
                    }
                    let fallback_id = client_order_id("fallback", &plan.candidate_id);
                    let order = self
                        .submit_or_lookup(
                            &plan.symbol,
                            &fallback_id,
                            vec![
                                ("symbol".into(), plan.symbol.clone()),
                                ("side".into(), side_name(plan.side).into()),
                                ("type".into(), "MARKET".into()),
                                ("quantity".into(), decimal(quantity, rules.quantity_step)),
                                ("newClientOrderId".into(), fallback_id.clone()),
                                ("newOrderRespType".into(), "RESULT".into()),
                            ],
                        )
                        .await?;
                    EntryExecution {
                        order,
                        mode: "maker_timeout_taker_fallback",
                        maker_attempted: true,
                        maker_wait_ms: waited_ms,
                        maker_reprices: reprices,
                        final_maker_limit: Some(final_limit),
                    }
                }
                PostOnlyEntry::Unfilled {
                    waited_ms,
                    reprices,
                    final_limit,
                } => {
                    return Err(anyhow!(
                        "adaptive maker entry expired without fill after {waited_ms}ms ({reprices} reprices, final limit {final_limit}); chasing is disabled"
                    ));
                }
            }
        } else {
            EntryExecution {
                order: self
                    .submit_or_lookup(
                        &plan.symbol,
                        &client_id,
                        vec![
                            ("symbol".into(), plan.symbol.clone()),
                            ("side".into(), side_name(plan.side).into()),
                            ("type".into(), "MARKET".into()),
                            ("quantity".into(), decimal(quantity, rules.quantity_step)),
                            ("newClientOrderId".into(), client_id.clone()),
                            ("newOrderRespType".into(), "RESULT".into()),
                        ],
                    )
                    .await?,
                mode: "taker_market",
                maker_attempted: false,
                maker_wait_ms: 0,
                maker_reprices: 0,
                final_maker_limit: None,
            }
        };
        let executed = parse_f64(&entry.order, "executedQty").unwrap_or(quantity);
        if executed <= f64::EPSILON {
            return Err(anyhow!("entry order completed without a fill"));
        }
        let order_id = entry.order["orderId"].as_i64().unwrap_or_default();
        let (entry_price, entry_price_source) = self
            .resolve_entry_price(&plan.symbol, order_id, &entry.order, plan.side)
            .await
            .unwrap_or((plan.reference_price, "strategy_reference_fallback"));
        // Preserve the planned risk/reward distances from the actual exchange
        // fill. A fast market can move between signal construction and fill;
        // anchoring protection to the stale reference would silently change
        // both the dollar risk and the take-profit geometry.
        let sign = plan.side.sign();
        let stop_distance = sign * (plan.reference_price - plan.stop_price)
            / plan.reference_price.max(f64::EPSILON);
        let stop_price = entry_price * (1.0 - sign * stop_distance);
        let take_profit_prices: Vec<_> = plan
            .take_profit_prices
            .iter()
            .map(|(target, fraction)| {
                let distance = sign * (*target / plan.reference_price.max(f64::EPSILON) - 1.0);
                (entry_price * (1.0 + sign * distance), *fraction)
            })
            .collect();
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
                self.cancel_all(&plan.symbol).await.ok();
                self.signed(
                    Method::POST,
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), plan.symbol.clone()),
                        ("side".into(), side_name(plan.side.opposite()).into()),
                        ("type".into(), "MARKET".into()),
                        ("quantity".into(), decimal(executed, rules.quantity_step)),
                        ("reduceOnly".into(), "true".into()),
                    ],
                )
                .await
                .with_context(|| {
                    format!("protective order failed ({error}); emergency close also failed")
                })?;
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

    async fn place_post_only_entry(
        &self,
        plan: &greed_kernel::PositionPlan,
        quantity: f64,
        price: f64,
        rules: &SymbolRules,
        client_id: &str,
    ) -> Result<PostOnlyEntry> {
        let mut active_client_id = client_id.to_string();
        let mut passive_price = price;
        let mut reprice_attempt = 0u8;
        let mut order = loop {
            let submitted = self
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
                .await;
            match submitted {
                Ok(order) => break order,
                Err(error) if is_post_only_rejection(&error) => {
                    reprice_attempt += 1;
                    if reprice_attempt > 3 {
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
                    let adverse_bps = plan.side.sign()
                        * (passive_price / plan.reference_price.max(f64::EPSILON) - 1.0)
                        * 10_000.0;
                    if adverse_bps > plan.max_entry_adverse_bps {
                        return Err(anyhow!(
                            "post-only reprice drifted {adverse_bps:.1} bps against the signal (max {:.1})",
                            plan.max_entry_adverse_bps
                        ));
                    }
                    active_client_id = reprice_client_id(client_id, reprice_attempt);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        };
        let started_ms = chrono::Utc::now().timestamp_millis();
        let deadline = started_ms + plan.entry_timeout_ms.clamp(5_000, 120_000);
        let mut next_reprice_ms = started_ms + MAKER_REPRICE_INTERVAL_MS;
        loop {
            let status = order.get("status").and_then(Value::as_str).unwrap_or("NEW");
            if status == "FILLED" {
                return Ok(PostOnlyEntry::Filled {
                    order,
                    waited_ms: chrono::Utc::now()
                        .timestamp_millis()
                        .saturating_sub(started_ms),
                    reprices: reprice_attempt,
                    final_limit: passive_price,
                });
            }
            if matches!(status, "CANCELED" | "EXPIRED" | "REJECTED") {
                let executed = parse_f64(&order, "executedQty").unwrap_or_default();
                if executed > f64::EPSILON {
                    return Ok(PostOnlyEntry::Filled {
                        order,
                        waited_ms: chrono::Utc::now()
                            .timestamp_millis()
                            .saturating_sub(started_ms),
                        reprices: reprice_attempt,
                        final_limit: passive_price,
                    });
                }
                return Err(anyhow!("post-only entry ended with status {status}"));
            }
            let now_ms = chrono::Utc::now().timestamp_millis();
            if now_ms >= deadline {
                let canceled = self
                    .cancel_or_reconcile_entry(&plan.symbol, &active_client_id)
                    .await?;
                let executed = parse_f64(&canceled, "executedQty")
                    .or_else(|| parse_f64(&order, "executedQty"))
                    .unwrap_or_default();
                if executed > f64::EPSILON {
                    return Ok(PostOnlyEntry::Filled {
                        order: canceled,
                        waited_ms: chrono::Utc::now()
                            .timestamp_millis()
                            .saturating_sub(started_ms),
                        reprices: reprice_attempt,
                        final_limit: passive_price,
                    });
                }
                return Ok(PostOnlyEntry::Unfilled {
                    waited_ms: plan.entry_timeout_ms.clamp(5_000, 120_000),
                    reprices: reprice_attempt,
                    final_limit: passive_price,
                });
            }
            if now_ms >= next_reprice_ms && reprice_attempt < MAX_MAKER_REPRICES {
                // Once a maker order has started filling, keep the remainder at
                // the same queue position until the deadline. Canceling on the
                // first partial fill turned 3-5% probes into complete trades and
                // threw away the remaining minute of fill opportunity.
                if parse_f64(&order, "executedQty").unwrap_or_default() > f64::EPSILON {
                    next_reprice_ms = deadline.saturating_add(1);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if let Ok(value) = self
                        .signed(
                            Method::GET,
                            "/fapi/v1/order",
                            vec![
                                ("symbol".into(), plan.symbol.clone()),
                                ("origClientOrderId".into(), active_client_id.clone()),
                            ],
                        )
                        .await
                    {
                        order = value;
                    }
                    continue;
                }
                let ticker = self
                    .public_get_params(
                        "/fapi/v1/ticker/bookTicker",
                        &[("symbol", plan.symbol.as_str())],
                    )
                    .await?;
                let best_passive = match plan.side {
                    Side::Buy => parse_f64(&ticker, "bidPrice"),
                    Side::Sell => parse_f64(&ticker, "askPrice"),
                }
                .ok_or_else(|| anyhow!("book ticker passive price is missing"))?;
                let next_price = capped_passive_price(
                    plan.side,
                    best_passive,
                    plan.reference_price,
                    plan.max_entry_adverse_bps,
                    rules.price_tick,
                );
                next_reprice_ms += MAKER_REPRICE_INTERVAL_MS;
                if passive_price_improves(plan.side, passive_price, next_price, rules.price_tick) {
                    let canceled = self
                        .cancel_or_reconcile_entry(&plan.symbol, &active_client_id)
                        .await?;
                    if parse_f64(&canceled, "executedQty").unwrap_or_default() > f64::EPSILON {
                        return Ok(PostOnlyEntry::Filled {
                            order: canceled,
                            waited_ms: now_ms.saturating_sub(started_ms),
                            reprices: reprice_attempt,
                            final_limit: passive_price,
                        });
                    }
                    reprice_attempt += 1;
                    passive_price = next_price;
                    active_client_id = reprice_client_id(client_id, reprice_attempt);
                    order = self
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
                        .with_context(|| {
                            format!("adaptive maker reprice {reprice_attempt} failed")
                        })?;
                    continue;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Ok(value) = self
                .signed(
                    Method::GET,
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), plan.symbol.clone()),
                        ("origClientOrderId".into(), active_client_id.clone()),
                    ],
                )
                .await
            {
                order = value;
            }
        }
    }

    async fn cancel_or_reconcile_entry(&self, symbol: &str, client_id: &str) -> Result<Value> {
        match self
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
            Ok(value) => Ok(value),
            Err(cancel_error) => self
                .signed(
                    Method::GET,
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), symbol.into()),
                        ("origClientOrderId".into(), client_id.into()),
                    ],
                )
                .await
                .with_context(|| {
                    format!(
                        "post-only cancel failed ({cancel_error}) and final reconciliation failed"
                    )
                }),
        }
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
                    ("priceProtect".into(), "true".into()),
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
        let regular = self
            .signed(
                Method::DELETE,
                "/fapi/v1/allOpenOrders",
                vec![("symbol".into(), symbol.into())],
            )
            .await;
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
            .signed(
                Method::GET,
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

    async fn close_market(&self, position: &RemotePosition) -> Result<()> {
        let rules = self
            .rules
            .get(&position.symbol)
            .ok_or_else(|| anyhow!("missing exchange rules for {}", position.symbol))?;
        self.cancel_all(&position.symbol).await?;
        self.signed(
            Method::POST,
            "/fapi/v1/order",
            vec![
                ("symbol".into(), position.symbol.clone()),
                ("side".into(), side_name(position.side.opposite()).into()),
                ("type".into(), "MARKET".into()),
                (
                    "quantity".into(),
                    decimal(position.quantity.abs(), rules.quantity_step),
                ),
                ("reduceOnly".into(), "true".into()),
            ],
        )
        .await?;
        Ok(())
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
            "performance_epoch": self.state.performance_epoch,
            "performance_epoch_reset": self.performance_epoch_reset,
            "performance_basis":"risk_r_v1",
            "performance_basis_reset":self.performance_basis_reset,
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

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}
fn gate_key(recipe: &str, side: Side) -> String {
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
    message.contains("-5022") || message.contains("could not be executed as maker")
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
fn reprice_client_id(base: &str, attempt: u8) -> String {
    format!("{}-r{attempt}", &base[..base.len().min(32)])
}
fn client_order_id(prefix: &str, candidate_id: &str) -> String {
    let digest = hex_bytes(&Sha256::digest(candidate_id.as_bytes()));
    format!("greed-{prefix}-{}", &digest[..20])
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
    fn exchange_quantization_never_rounds_quantity_up() {
        assert_eq!(floor_step(1.239, 0.01), 1.23);
        assert_eq!(decimal(1.23, 0.01), "1.23");
        assert_eq!(ceil_step(1.231, 0.01), 1.24);
    }

    #[test]
    fn client_ids_are_stable_and_within_binance_limit() {
        let value = client_order_id("entry", "alt.recipe:test:cycle-123");
        assert_eq!(value, client_order_id("entry", "alt.recipe:test:cycle-123"));
        assert!(value.len() <= 36);
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
    fn adaptive_maker_client_ids_stay_within_binance_limit() {
        let base = client_order_id("entry", "alt.recipe:test:cycle-123");
        let repriced = reprice_client_id(&base, MAX_MAKER_REPRICES);
        assert!(repriced.len() <= 36);
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
        };
        assert_eq!(loss_cooldown_until(Some(&[loss]), 180), Some(10_801_000));
        let recovered = [
            ExecutionOutcome {
                exit_ms: 1_000,
                pnl_usd: -2.0,
                pnl_r: Some(-1.0),
                fill_ratio: Some(1.0),
            },
            ExecutionOutcome {
                exit_ms: 2_000,
                pnl_usd: 1.0,
                pnl_r: Some(0.5),
                fill_ratio: Some(1.0),
            },
        ];
        assert_eq!(loss_cooldown_until(Some(&recovered), 180), None);
    }

    #[test]
    fn performance_gate_uses_r_and_excludes_tiny_partial_fills() {
        let full = ExecutionOutcome {
            exit_ms: 1_000,
            pnl_usd: 20.0,
            pnl_r: Some(1.0),
            fill_ratio: Some(0.95),
        };
        let tiny = ExecutionOutcome {
            exit_ms: 2_000,
            pnl_usd: -0.50,
            pnl_r: Some(-1.0),
            fill_ratio: Some(0.05),
        };
        let legacy = ExecutionOutcome {
            exit_ms: 3_000,
            pnl_usd: -4.0,
            pnl_r: None,
            fill_ratio: None,
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
}
