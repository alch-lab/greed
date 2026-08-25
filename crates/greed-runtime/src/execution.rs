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

pub struct ExchangeEvent {
    pub kind: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecipeGateStatus {
    pub allowed: bool,
    pub completed_trades: usize,
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
    performance_epoch: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionOutcome {
    exit_ms: i64,
    pnl_usd: f64,
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
            state.performance_epoch = risk.rolling_pf_epoch;
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
        };
        value.initialize().await?;
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
        let response = self
            .client
            .get(format!(
                "{}{path}",
                self.config.base_url.trim_end_matches('/')
            ))
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
                let mut stop_orders: BTreeMap<String, i64> = BTreeMap::new();
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
                                    stop_orders.insert(symbol.clone(), order_id);
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
                    let summary = self.trade_summary(&symbol, entry_ms).await.ok();
                    let (fee_delta, pnl_delta) = if let Some((_, _, fee, pnl)) = summary.as_ref() {
                        let meta = self.state.positions.get(&symbol);
                        (
                            fee - meta
                                .map(|value| value.cumulative_reported_fee_usd)
                                .unwrap_or(0.0),
                            pnl - meta
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
                            "closed_quantity":closed_quantity,
                            "remaining_quantity":remaining_quantity,
                            "fee_usd":fee_delta,
                            "pnl_usd":pnl_delta,
                            "cumulative_fee_usd":summary.as_ref().map(|value| value.2),
                            "cumulative_net_pnl_usd":summary.as_ref().map(|value| value.3),
                            "venue":"binance_demo"
                        }),
                    });
                    if let Some(meta) = self.state.positions.get_mut(&symbol) {
                        meta.last_observed_quantity = remaining_quantity;
                        if let Some((_, _, fee, pnl)) = summary {
                            meta.cumulative_reported_fee_usd = fee;
                            meta.cumulative_reported_pnl_usd = pnl;
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
                    let shield_hit = meta
                        .break_even_after_fraction
                        .is_some_and(|threshold| closed_fraction + 1e-6 >= threshold);
                    let mut desired_stop = None;
                    let mut reason = "risk_shield";
                    if shield_hit && !meta.break_even_armed && meta.entry_price > 0.0 {
                        desired_stop = Some(
                            meta.entry_price
                                * (1.0 + meta.side.sign() * meta.break_even_buffer_pct),
                        );
                    }
                    let favorable = meta.side.sign()
                        * (meta.extreme_price / meta.entry_price.max(f64::EPSILON) - 1.0);
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
                            self.cancel_order(&symbol, order_id).await?;
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
                        Ok(()) => {
                            if let Some(meta) = self.state.positions.get_mut(&symbol) {
                                meta.stop_price = desired;
                                meta.break_even_armed = true;
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
                        self.cancel_all(&symbol).await.ok();
                        let summary = self.trade_summary(&symbol, meta.entry_ms).await.ok();
                        if let Some((_, _, _, pnl)) = summary.as_ref() {
                            self.state
                                .recipe_outcomes
                                .entry(gate_key(&meta.recipe, meta.side))
                                .or_default()
                                .push(ExecutionOutcome {
                                    exit_ms: now_ms,
                                    pnl_usd: *pnl,
                                });
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
                                "exit_price": summary.as_ref().map(|value| value.0),
                                "quantity": summary.as_ref().map(|value| value.1),
                                "fee_usd": summary.as_ref().map(|value| value.2 - meta.cumulative_reported_fee_usd),
                                "pnl_usd": summary.as_ref().map(|value| value.3 - meta.cumulative_reported_pnl_usd),
                                "trade_net_pnl_usd": summary.as_ref().map(|value| value.3),
                                "mfe_pct": mfe_pct,
                                "mae_pct": mae_pct,
                                "hold_ms": now_ms - meta.entry_ms,
                                "reason": "exchange_position_reconciled",
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
        let start = outcomes.len().saturating_sub(self.risk.rolling_pf_window);
        let window = &outcomes[start..];
        let profit: f64 = window
            .iter()
            .filter(|outcome| outcome.pnl_usd > 0.0)
            .map(|outcome| outcome.pnl_usd)
            .sum();
        let loss: f64 = -window
            .iter()
            .filter(|outcome| outcome.pnl_usd < 0.0)
            .map(|outcome| outcome.pnl_usd)
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
            rolling_profit_factor,
            rolling_net_pnl_usd: window.iter().map(|outcome| outcome.pnl_usd).sum(),
            next_probe_ms,
        }
    }

    pub fn candidate_gate_status(&self, recipe: &str, side: Side, now_ms: i64) -> RecipeGateStatus {
        self.recipe_gate_status(&gate_key(recipe, side), now_ms)
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
            let performance_key = gate_key(&recipe, plan.side);
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
            match self.place_bracket(plan, size_multiplier).await {
                Ok((entry_price, quantity, order_id, stop_price, take_profit_prices)) => {
                    self.state.seen.insert(plan.candidate_id.clone());
                    self.state.positions.insert(
                        plan.symbol.clone(),
                        ExecutionMeta {
                            candidate_id: plan.candidate_id.clone(),
                            recipe: recipe.clone(),
                            side: plan.side,
                            entry_ms: frame.as_of_ms,
                            entry_price,
                            initial_quantity: quantity,
                            last_observed_quantity: quantity,
                            cumulative_reported_fee_usd: 0.0,
                            cumulative_reported_pnl_usd: 0.0,
                            stop_price,
                            take_profit_price: take_profit_prices
                                .last()
                                .map(|value| value.0)
                                .unwrap_or_default(),
                            take_profit_prices,
                            break_even_after_fraction: plan.break_even_after_fraction.map(|fraction| {
                                self.rules
                                    .get(&plan.symbol)
                                    .map(|rules| {
                                        floor_step(quantity * fraction, rules.quantity_step)
                                            / quantity.max(f64::EPSILON)
                                    })
                                    .unwrap_or(fraction)
                            }),
                            break_even_buffer_pct: plan.break_even_buffer_pct,
                            break_even_armed: false,
                            extreme_price: entry_price,
                            adverse_price: entry_price,
                            trailing_activation_pct: plan.trailing_activation_pct,
                            trailing_distance_pct: plan.trailing_distance_pct,
                            max_hold_ms: plan.max_hold_ms,
                            exit_requested: false,
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
                        payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"lane":recipe,"symbol":plan.symbol,"side":plan.side,"entry_mode":if plan.entry_limit.is_some(){"post_only_limit"}else{"market"},"requested_limit":plan.entry_limit,"entry_price":entry_price,"quantity":quantity,"notional_usd":plan.notional_usd * size_multiplier,"probe_size_multiplier":size_multiplier,"order_id":order_id,"signal_to_order_ms":signal_to_order_ms,"discovery_latency_ms":discovery_latency_ms,"venue":"binance_demo","paper_only":true}),
                    });
                }
                Err(error) => events.push(ExchangeEvent {
                    kind: "exchange_order_rejected".into(),
                    payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"lane":recipe,"symbol":plan.symbol,"side":plan.side,"reason":error.to_string(),"venue":"binance_demo","paper_only":true}),
                }),
            }
        }
        events
    }

    async fn place_bracket(
        &self,
        plan: &greed_kernel::PositionPlan,
        size_multiplier: f64,
    ) -> Result<(f64, f64, i64, f64, Vec<(f64, f64)>)> {
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
            self.place_post_only_entry(plan, quantity, price, rules, &client_id)
                .await?
        } else {
            self.submit_or_lookup(
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
            .await?
        };
        let executed = parse_f64(&entry, "executedQty").unwrap_or(quantity);
        if executed <= f64::EPSILON {
            return Err(anyhow!("entry order completed without a fill"));
        }
        let entry_price = parse_f64(&entry, "avgPrice")
            .filter(|value| *value > 0.0)
            .unwrap_or(plan.reference_price);
        let order_id = entry["orderId"].as_i64().unwrap_or_default();
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
            self.place_close_all_trigger(
                &plan.symbol,
                plan.side.opposite(),
                "STOP_MARKET",
                stop_price,
                rules,
                client_order_id("stop", &plan.candidate_id),
            )
            .await?;
            for (index, (take_profit, fraction)) in take_profit_prices.iter().enumerate() {
                let last = index + 1 == take_profit_prices.len();
                if last && *fraction >= 1.0 - f64::EPSILON {
                    self.place_close_all_trigger(
                        &plan.symbol,
                        plan.side.opposite(),
                        "TAKE_PROFIT_MARKET",
                        *take_profit,
                        rules,
                        client_order_id("runner", &plan.candidate_id),
                    )
                    .await?;
                } else {
                    self.place_partial_trigger(
                        &plan.symbol,
                        plan.side.opposite(),
                        *take_profit,
                        floor_step(executed * fraction, rules.quantity_step),
                        rules,
                        client_order_id(&format!("take{index}"), &plan.candidate_id),
                    )
                    .await?;
                }
            }
            Result::<()>::Ok(())
        }
        .await;
        if let Err(error) = protective {
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
        Ok((
            entry_price,
            executed,
            order_id,
            stop_price,
            take_profit_prices,
        ))
    }

    async fn place_post_only_entry(
        &self,
        plan: &greed_kernel::PositionPlan,
        quantity: f64,
        price: f64,
        rules: &SymbolRules,
        client_id: &str,
    ) -> Result<Value> {
        let mut order = self
            .submit_or_lookup(
                &plan.symbol,
                client_id,
                vec![
                    ("symbol".into(), plan.symbol.clone()),
                    ("side".into(), side_name(plan.side).into()),
                    ("type".into(), "LIMIT".into()),
                    ("timeInForce".into(), "GTX".into()),
                    ("quantity".into(), decimal(quantity, rules.quantity_step)),
                    ("price".into(), decimal(price, rules.price_tick)),
                    ("newClientOrderId".into(), client_id.into()),
                    ("newOrderRespType".into(), "ACK".into()),
                ],
            )
            .await?;
        let deadline =
            chrono::Utc::now().timestamp_millis() + plan.entry_timeout_ms.clamp(5_000, 60_000);
        loop {
            let status = order.get("status").and_then(Value::as_str).unwrap_or("NEW");
            if status == "FILLED" {
                return Ok(order);
            }
            if matches!(status, "CANCELED" | "EXPIRED" | "REJECTED") {
                let executed = parse_f64(&order, "executedQty").unwrap_or_default();
                if executed > f64::EPSILON {
                    return Ok(order);
                }
                return Err(anyhow!("post-only entry ended with status {status}"));
            }
            if chrono::Utc::now().timestamp_millis() >= deadline {
                let canceled = match self
                    .signed(
                        Method::DELETE,
                        "/fapi/v1/order",
                        vec![
                            ("symbol".into(), plan.symbol.clone()),
                            ("origClientOrderId".into(), client_id.into()),
                        ],
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(cancel_error) => self
                        .signed(
                            Method::GET,
                            "/fapi/v1/order",
                            vec![
                                ("symbol".into(), plan.symbol.clone()),
                                ("origClientOrderId".into(), client_id.into()),
                            ],
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "post-only cancel failed ({cancel_error}) and final reconciliation failed"
                            )
                        })?,
                };
                let executed = parse_f64(&canceled, "executedQty")
                    .or_else(|| parse_f64(&order, "executedQty"))
                    .unwrap_or_default();
                if executed > f64::EPSILON {
                    return Ok(canceled);
                }
                return Err(anyhow!(
                    "post-only entry expired without fill after {}ms",
                    plan.entry_timeout_ms
                ));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Ok(value) = self
                .signed(
                    Method::GET,
                    "/fapi/v1/order",
                    vec![
                        ("symbol".into(), plan.symbol.clone()),
                        ("origClientOrderId".into(), client_id.into()),
                    ],
                )
                .await
            {
                order = value;
            }
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
    ) -> Result<()> {
        self.submit_or_lookup(
            symbol,
            &client_id,
            vec![
                ("symbol".into(), symbol.into()),
                ("side".into(), side_name(side).into()),
                ("type".into(), kind.into()),
                (
                    "stopPrice".into(),
                    decimal(round_step(trigger, rules.price_tick), rules.price_tick),
                ),
                ("closePosition".into(), "true".into()),
                ("workingType".into(), "MARK_PRICE".into()),
                ("priceProtect".into(), "true".into()),
                ("newClientOrderId".into(), client_id.clone()),
            ],
        )
        .await?;
        Ok(())
    }

    async fn place_partial_trigger(
        &self,
        symbol: &str,
        side: Side,
        trigger: f64,
        quantity: f64,
        rules: &SymbolRules,
        client_id: String,
    ) -> Result<()> {
        self.submit_or_lookup(
            symbol,
            &client_id,
            vec![
                ("symbol".into(), symbol.into()),
                ("side".into(), side_name(side).into()),
                ("type".into(), "TAKE_PROFIT_MARKET".into()),
                (
                    "stopPrice".into(),
                    decimal(round_step(trigger, rules.price_tick), rules.price_tick),
                ),
                ("quantity".into(), decimal(quantity, rules.quantity_step)),
                ("reduceOnly".into(), "true".into()),
                ("workingType".into(), "MARK_PRICE".into()),
                ("priceProtect".into(), "true".into()),
                ("newClientOrderId".into(), client_id.clone()),
            ],
        )
        .await?;
        Ok(())
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

    async fn cancel_all(&self, symbol: &str) -> Result<()> {
        self.signed(
            Method::DELETE,
            "/fapi/v1/allOpenOrders",
            vec![("symbol".into(), symbol.into())],
        )
        .await?;
        Ok(())
    }

    async fn cancel_order(&self, symbol: &str, order_id: i64) -> Result<()> {
        self.signed(
            Method::DELETE,
            "/fapi/v1/order",
            vec![
                ("symbol".into(), symbol.into()),
                ("orderId".into(), order_id.to_string()),
            ],
        )
        .await?;
        Ok(())
    }

    async fn trade_summary(&self, symbol: &str, start_ms: i64) -> Result<(f64, f64, f64, f64)> {
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
        let mut exit_notional = 0.0;
        let mut exit_quantity = 0.0;
        let mut fees = 0.0;
        let mut realized = 0.0;
        for row in rows {
            let pnl = parse_f64(row, "realizedPnl").unwrap_or(0.0);
            fees += parse_f64(row, "commission").unwrap_or(0.0);
            realized += pnl;
            if pnl.abs() > f64::EPSILON {
                let quantity = parse_f64(row, "qty").unwrap_or(0.0);
                exit_quantity += quantity;
                exit_notional += parse_f64(row, "price").unwrap_or(0.0) * quantity;
            }
        }
        let exit_price = if exit_quantity > f64::EPSILON {
            exit_notional / exit_quantity
        } else {
            0.0
        };
        Ok((exit_price, exit_quantity, fees, realized - fees))
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
            "last_error": self.last_error,
            "remote_matching": true,
            "leverage": self.config.leverage,
            "performance_epoch": self.state.performance_epoch,
            "performance_epoch_reset": self.performance_epoch_reset,
        })
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
}
