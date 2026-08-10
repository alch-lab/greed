//! 多币种山寨币放量突破执行器。
//!
//! 该执行器与 BTC 的 TRDR 引擎互斥运行：控制面同一时间只允许一个交易任务。
//! 行情来自 Binance 主网公共 REST，paper 订单仍发送到 Futures Demo。

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;

use anyhow::{Context, Result};
use data::live::config::AccountConfig;
use data::live::CollectorConfig;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::trade_runner::{TradeArgs, TradeMode};

const FUTURES_BASE: &str = "https://fapi.binance.com";
const SPOT_BASE: &str = "https://api.binance.com";
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AltcoinImpulseConfig {
    pub enabled: bool,
    pub capital_usdt: f64,
    pub exchange_leverage: u32,
    pub risk_per_trade: f64,
    pub max_positions: usize,
    pub max_daily_entries: u32,
    pub daily_loss_limit: f64,
    pub max_gross_multiple: f64,
    pub stop_pct: f64,
    pub trail_activation_pct: f64,
    pub trail_pct: f64,
    pub max_hold_hours: u32,
    pub cooldown_hours: u32,
    #[serde(default = "default_max_entry_slippage_pct")]
    pub max_entry_slippage_pct: f64,
    pub min_24h_volume_usd: f64,
    pub min_return_1h: f64,
    pub max_return_1h: f64,
    pub min_return_4h: f64,
    pub max_return_4h: f64,
    pub min_volume_ratio: f64,
    pub min_efficiency: f64,
    pub min_close_location: f64,
    pub scan_limit: usize,
    pub poll_seconds: u64,
    #[serde(default)]
    pub allow_live: bool,
}

#[derive(Debug, Deserialize)]
struct StrategyFile {
    altcoin_impulse: AltcoinImpulseConfig,
}

#[derive(Debug, Clone)]
struct Bar {
    open_ms: i64,
    close_ms: i64,
    high: f64,
    low: f64,
    close: f64,
    quote_volume: f64,
}

#[derive(Debug, Clone, Serialize)]
struct Candidate {
    symbol: String,
    signal_ms: i64,
    side: i32,
    price: f64,
    return_1h: f64,
    return_4h: f64,
    volume_ratio: f64,
    efficiency: f64,
    close_location: f64,
    volume_24h: f64,
    score: f64,
    blockers: Vec<String>,
    spot_return_1h: Option<f64>,
    oi_change_1h: Option<f64>,
    funding_rate: Option<f64>,
    perp_premium: Option<f64>,
}

impl Candidate {
    fn eligible(&self) -> bool {
        self.blockers.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Position {
    symbol: String,
    side: i32,
    qty: f64,
    entry_ms: i64,
    entry_price: f64,
    entry_fee: f64,
    initial_notional: f64,
    extreme: f64,
    stop_price: f64,
    last_bar_ms: i64,
    #[serde(default)]
    protection_order_id: Option<i64>,
    #[serde(default = "default_protection_reason")]
    protection_reason: String,
}

fn default_max_entry_slippage_pct() -> f64 {
    0.015
}

fn default_protection_reason() -> String {
    "initial_stop".to_owned()
}

fn detected_exit_reason(position: &Position, exit_price: f64, reconciled: bool) -> &'static str {
    if !reconciled || position.stop_price <= 0.0 {
        return "unknown";
    }
    let distance = (exit_price / position.stop_price - 1.0).abs();
    if distance <= 0.02 {
        if position.protection_reason == "trailing_take_profit" {
            "trailing_take_profit"
        } else {
            "initial_stop"
        }
    } else {
        "manual_or_external"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionIssue {
    ts_ms: i64,
    symbol: String,
    stage: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedState {
    cash: f64,
    realized_pnl: f64,
    fees: f64,
    positions: HashMap<String, Position>,
    cooldown_until: HashMap<String, i64>,
    seen_signal: HashMap<String, i64>,
    day: i64,
    day_start_equity: f64,
    daily_entries: u32,
    total_entries: u64,
    total_exits: u64,
    wins: u64,
    #[serde(default)]
    rejected_entries: u64,
    #[serde(default)]
    last_execution_issue: Option<ExecutionIssue>,
    #[serde(default)]
    recent_trades: Vec<Value>,
}

impl PersistedState {
    fn new(cash: f64, now_ms: i64) -> Self {
        Self {
            cash,
            realized_pnl: 0.0,
            fees: 0.0,
            positions: HashMap::new(),
            cooldown_until: HashMap::new(),
            seen_signal: HashMap::new(),
            day: now_ms / DAY_MS,
            day_start_equity: cash,
            daily_entries: 0,
            total_entries: 0,
            total_exits: 0,
            wins: 0,
            rejected_entries: 0,
            last_execution_issue: None,
            recent_trades: Vec::new(),
        }
    }

    fn note_execution_issue(&mut self, ts_ms: i64, symbol: &str, stage: &str, reason: String) {
        self.rejected_entries += 1;
        self.last_execution_issue = Some(ExecutionIssue {
            ts_ms,
            symbol: symbol.to_owned(),
            stage: stage.to_owned(),
            reason,
        });
    }

    fn record_trade(&mut self, event: Value) {
        self.recent_trades.push(event);
        if self.recent_trades.len() > 100 {
            self.recent_trades.drain(..self.recent_trades.len() - 100);
        }
    }
}

fn parse_num(v: &Value, index: usize) -> Result<f64> {
    v.get(index)
        .and_then(Value::as_str)
        .context("K 线数字字段缺失")?
        .parse()
        .context("K 线数字格式错误")
}

fn parse_bars(value: Value, now_ms: i64) -> Result<Vec<Bar>> {
    let rows = value.as_array().context("K 线响应不是数组")?;
    rows.iter()
        .filter(|row| row.get(6).and_then(Value::as_i64).unwrap_or(i64::MAX) < now_ms)
        .map(|row| {
            Ok(Bar {
                open_ms: row
                    .get(0)
                    .and_then(Value::as_i64)
                    .context("K 线缺 openTime")?,
                close_ms: row
                    .get(6)
                    .and_then(Value::as_i64)
                    .context("K 线缺 closeTime")?,
                high: parse_num(row, 2)?,
                low: parse_num(row, 3)?,
                close: parse_num(row, 4)?,
                quote_volume: parse_num(row, 7)?,
            })
        })
        .collect()
}

fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn evaluate(symbol: String, bars: &[Bar], cfg: &AltcoinImpulseConfig) -> Option<Candidate> {
    if bars.len() < 7 * 96 + 17 {
        return None;
    }
    let i = bars.len() - 1;
    let close = bars[i].close;
    let prior_high = bars[i - 96..i]
        .iter()
        .map(|b| b.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let prior_low = bars[i - 96..i]
        .iter()
        .map(|b| b.low)
        .fold(f64::INFINITY, f64::min);
    let side = if close > prior_high {
        1
    } else if close < prior_low {
        -1
    } else {
        0
    };
    let return_1h = close / bars[i - 4].close - 1.0;
    let return_4h = close / bars[i - 16].close - 1.0;
    let hour_volume = |end: usize| {
        bars[end - 3..=end]
            .iter()
            .map(|b| b.quote_volume)
            .sum::<f64>()
    };
    let current_hour_volume = hour_volume(i);
    let historical: Vec<f64> = (i - 7 * 96..i)
        .filter(|&end| end >= 3)
        .map(hour_volume)
        .collect();
    let volume_ratio = current_hour_volume / median(historical).max(1.0);
    let volume_24h = bars[i - 95..=i].iter().map(|b| b.quote_volume).sum::<f64>();
    let path = &bars[i - 4..=i];
    let traveled: f64 = path
        .windows(2)
        .map(|w| (w[1].close - w[0].close).abs())
        .sum();
    let efficiency = (path[4].close - path[0].close).abs() / traveled.max(f64::EPSILON);
    let spread = bars[i].high - bars[i].low;
    let close_location = if spread > 0.0 {
        (close - bars[i].low) / spread
    } else {
        0.5
    };
    let mut blockers = Vec::new();
    if side == 0 {
        blockers.push("未突破前 24h 高低点".into());
    }
    let return_ok = if side > 0 {
        (cfg.min_return_1h..=cfg.max_return_1h).contains(&return_1h)
            && (cfg.min_return_4h..=cfg.max_return_4h).contains(&return_4h)
    } else {
        (-cfg.max_return_1h..=-cfg.min_return_1h).contains(&return_1h)
            && (-cfg.max_return_4h..=-cfg.min_return_4h).contains(&return_4h)
    };
    if !return_ok {
        blockers.push("1h/4h 涨跌幅不在启动区间".into());
    }
    if volume_ratio < cfg.min_volume_ratio {
        blockers.push("放量倍数不足".into());
    }
    if efficiency < cfg.min_efficiency {
        blockers.push("价格路径效率不足".into());
    }
    let close_ok = if side > 0 {
        close_location >= cfg.min_close_location
    } else {
        close_location <= 1.0 - cfg.min_close_location
    };
    if !close_ok {
        blockers.push("信号 K 线收盘位置不强".into());
    }
    if volume_24h < cfg.min_24h_volume_usd {
        blockers.push("24h 成交额不足".into());
    }
    Some(Candidate {
        symbol,
        signal_ms: bars[i].close_ms,
        side,
        price: close,
        return_1h,
        return_4h,
        volume_ratio,
        efficiency,
        close_location,
        volume_24h,
        score: return_1h.abs() * volume_ratio.ln_1p() * (volume_24h / 1e6).ln_1p(),
        blockers,
        spot_return_1h: None,
        oi_change_1h: None,
        funding_rate: None,
        perp_premium: None,
    })
}

async fn enrich_candidate(http: reqwest::Client, mut candidate: Candidate) -> Candidate {
    let spot_url = format!(
        "{SPOT_BASE}/api/v3/klines?symbol={}&interval=15m&limit=6",
        candidate.symbol
    );
    let oi_url = format!(
        "{FUTURES_BASE}/futures/data/openInterestHist?symbol={}&period=15m&limit=5",
        candidate.symbol
    );
    let premium_url = format!(
        "{FUTURES_BASE}/fapi/v1/premiumIndex?symbol={}",
        candidate.symbol
    );
    let (spot, oi, premium) = tokio::join!(
        get_json(&http, &spot_url),
        get_json(&http, &oi_url),
        get_json(&http, &premium_url)
    );
    if let Ok(rows) = spot {
        if let Some(values) = rows.as_array() {
            if values.len() >= 5 {
                let last = values.len() - 1;
                let current = values[last][4]
                    .as_str()
                    .and_then(|value| value.parse::<f64>().ok());
                let previous = values[last - 4][4]
                    .as_str()
                    .and_then(|value| value.parse::<f64>().ok());
                if let (Some(current), Some(previous)) = (current, previous) {
                    candidate.spot_return_1h = Some(current / previous - 1.0);
                }
            }
        }
    }
    if let Ok(rows) = oi {
        if let Some(values) = rows.as_array() {
            let first = values.first().and_then(|value| {
                value["sumOpenInterestValue"]
                    .as_str()
                    .and_then(|item| item.parse::<f64>().ok())
            });
            let last = values.last().and_then(|value| {
                value["sumOpenInterestValue"]
                    .as_str()
                    .and_then(|item| item.parse::<f64>().ok())
            });
            if let (Some(first), Some(last)) = (first, last) {
                candidate.oi_change_1h = Some(last / first.max(f64::EPSILON) - 1.0);
            }
        }
    }
    if let Ok(value) = premium {
        candidate.funding_rate = value["lastFundingRate"]
            .as_str()
            .and_then(|item| item.parse().ok());
        let mark = value["markPrice"]
            .as_str()
            .and_then(|item| item.parse::<f64>().ok());
        let index = value["indexPrice"]
            .as_str()
            .and_then(|item| item.parse::<f64>().ok());
        if let (Some(mark), Some(index)) = (mark, index) {
            candidate.perp_premium = Some(mark / index - 1.0);
        }
    }
    candidate
}

async fn get_json(http: &reqwest::Client, url: &str) -> Result<Value> {
    let mut last_error = None;
    for attempt in 0..3 {
        let request = async {
            let response = http.get(url).send().await?.error_for_status()?;
            let text = response.text().await?;
            Ok::<Value, anyhow::Error>(serde_json::from_str(&text)?)
        };
        match tokio::time::timeout(Duration::from_secs(12), request).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => last_error = Some(anyhow::anyhow!("公共行情请求 12 秒超时")),
        }
        tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("公共行情请求失败")))
        .with_context(|| url.to_owned())
}

async fn fetch_bars(http: reqwest::Client, symbol: String) -> Result<(String, Vec<Bar>)> {
    let now = chrono::Utc::now().timestamp_millis();
    let url = format!("{FUTURES_BASE}/fapi/v1/klines?symbol={symbol}&interval=15m&limit=700");
    let bars = parse_bars(get_json(&http, &url).await?, now)?;
    Ok((symbol, bars))
}

fn append_event(path: &str, event: Value) -> Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, &event)?;
    file.write_all(b"\n")?;
    Ok(())
}

fn load_recent_trades(path: &str) -> Vec<Value> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut events: Vec<Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| {
            matches!(
                event["event"].as_str(),
                Some("entry" | "exit" | "exit_detected")
            )
        })
        .collect();
    if events.len() > 100 {
        events.drain(..events.len() - 100);
    }
    events
}

fn save_state(path: &str, state: &PersistedState) -> Result<()> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let temp = format!("{path}.tmp");
    std::fs::write(&temp, serde_json::to_vec_pretty(state)?)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

fn equity(state: &PersistedState, prices: &HashMap<String, f64>) -> f64 {
    state.cash
        + state
            .positions
            .values()
            .map(|p| {
                p.side as f64
                    * p.qty
                    * (prices.get(&p.symbol).copied().unwrap_or(p.entry_price) - p.entry_price)
            })
            .sum::<f64>()
}

async fn wait_fill(
    rest: &live::RestClient,
    symbol: &str,
    order_id: i64,
) -> Result<(f64, f64, f64)> {
    for _ in 0..20 {
        let fills: Vec<_> = rest
            .user_trades(symbol, 0)
            .await?
            .into_iter()
            .filter(|fill| fill.order_id == order_id)
            .collect();
        if !fills.is_empty() {
            let qty: f64 = fills.iter().filter_map(|f| f.qty.parse::<f64>().ok()).sum();
            let quote: f64 = fills
                .iter()
                .filter_map(|f| f.quote_qty.parse::<f64>().ok())
                .sum();
            let fee: f64 = fills
                .iter()
                .filter_map(|f| f.commission.parse::<f64>().ok())
                .sum();
            if qty > 0.0 {
                return Ok((quote / qty, qty, fee));
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("{symbol} 订单 {order_id} 在 5 秒内没有成交回报")
}

async fn closing_fill(
    rest: &live::RestClient,
    position: &Position,
) -> Result<Option<(f64, f64, f64)>> {
    let closing_side = if position.side > 0 { "SELL" } else { "BUY" };
    let fills: Vec<_> = rest
        .user_trades(&position.symbol, 0)
        .await?
        .into_iter()
        .filter(|fill| fill.time >= position.entry_ms && fill.side == closing_side)
        .collect();
    let qty: f64 = fills
        .iter()
        .filter_map(|fill| fill.qty.parse::<f64>().ok())
        .sum();
    if qty <= 0.0 {
        return Ok(None);
    }
    let quote: f64 = fills
        .iter()
        .filter_map(|fill| fill.quote_qty.parse::<f64>().ok())
        .sum();
    let fee: f64 = fills
        .iter()
        .filter_map(|fill| fill.commission.parse::<f64>().ok())
        .sum();
    Ok(Some((quote / qty, qty, fee)))
}

async fn emergency_flatten(
    rest: &live::RestClient,
    symbol: &str,
    filters: &live::SymbolFilters,
) -> Result<Option<(f64, f64, f64)>> {
    let amount = rest.position_amt(symbol).await?;
    if amount.abs() <= 1e-12 {
        return Ok(None);
    }
    let side = if amount > 0.0 { "SELL" } else { "BUY" };
    let order = rest
        .place_order(
            symbol,
            side,
            "MARKET",
            amount.abs(),
            None,
            None,
            true,
            filters,
        )
        .await?;
    Ok(Some(wait_fill(rest, symbol, order).await?))
}

/// 是否为山寨币专用配置。读取失败留给正式运行器报告。
pub fn is_altcoin_strategy(path: &str) -> bool {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| toml::from_str::<toml::Value>(&text).ok())
        .and_then(|value| value.get("altcoin_impulse").cloned())
        .is_some()
}

pub async fn run_altcoin_impulse(
    args: TradeArgs,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    status_tx: Option<tokio::sync::watch::Sender<Value>>,
) -> Result<()> {
    let strategy_text = std::fs::read_to_string(&args.strategy)
        .with_context(|| format!("读取策略失败: {}", args.strategy))?;
    let cfg = toml::from_str::<StrategyFile>(&strategy_text)?.altcoin_impulse;
    let strategy_hash = format!("{:x}", Sha256::digest(strategy_text.as_bytes()));
    let git_commit = std::env::var("GREED_GIT_COMMIT").unwrap_or_else(|_| {
        std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "unknown".into())
    });
    anyhow::ensure!(cfg.enabled, "altcoin_impulse.enabled=false");
    anyhow::ensure!(
        (1..=20).contains(&cfg.exchange_leverage),
        "exchange_leverage 必须在 1..=20"
    );
    anyhow::ensure!(
        cfg.risk_per_trade > 0.0 && cfg.risk_per_trade <= 0.10,
        "山寨币策略单笔风险必须在 0%..=10%"
    );
    anyhow::ensure!(
        cfg.stop_pct >= 0.03 && cfg.stop_pct <= 0.12,
        "止损必须在 3%..=12%"
    );
    anyhow::ensure!(
        cfg.max_entry_slippage_pct > 0.0 && cfg.max_entry_slippage_pct <= 0.03,
        "最大入场滑点必须在 0%..=3%"
    );
    let base_text = std::fs::read_to_string(&args.config)?;
    let collector = CollectorConfig::from_toml_str(&base_text)?;
    let mut account = AccountConfig::from_toml_str(&base_text)?;
    let mode = args.mode.unwrap_or(if account.testnet {
        TradeMode::Paper
    } else {
        TradeMode::Live
    });
    match mode {
        TradeMode::Paper => account.testnet = true,
        TradeMode::Live => account.testnet = false,
        TradeMode::Dry => {}
    }
    anyhow::ensure!(
        mode != TradeMode::Live || cfg.allow_live,
        "该高风险策略默认禁止实盘；确认后设置 allow_live=true"
    );
    let http = data::live::build_http_client(collector.effective_proxy().as_deref());
    let rest = if mode == TradeMode::Dry {
        None
    } else {
        let (key, secret) = match (account.api_key(), account.api_secret()) {
            (Some(key), Some(secret)) => (key, secret),
            _ => anyhow::bail!("缺少 Binance Futures API 凭证"),
        };
        let mut client = live::RestClient::new(http.clone(), account.rest_base(), key, secret);
        client.sync_time().await?;
        Some(client)
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    let state_path = format!("data/journal/altcoin-{}.state.json", mode.as_str());
    let event_path = format!("data/journal/altcoin-{}.jsonl", mode.as_str());
    let initial_cash = if let Some(client) = rest.as_ref() {
        client.wallet_balance_usdt().await?.min(cfg.capital_usdt)
    } else {
        cfg.capital_usdt
    };
    let mut state = std::fs::read(&state_path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_else(|| PersistedState::new(initial_cash, now_ms));
    if state.recent_trades.is_empty() {
        state.recent_trades = load_recent_trades(&event_path);
    }
    if let Some(client) = rest.as_ref() {
        let external: Vec<_> = client
            .open_position_amounts()
            .await?
            .into_iter()
            .filter(|(symbol, _)| !state.positions.contains_key(symbol))
            .collect();
        anyhow::ensure!(
            external.is_empty(),
            "账户存在不属于山寨币策略的持仓 {:?}；为避免和 BTC/手工仓位冲突，拒绝启动",
            external
        );
    }
    let started_ms = now_ms;
    info!(
        mode = mode.as_str(),
        leverage = cfg.exchange_leverage,
        capital = initial_cash,
        "启动独立山寨币放量突破策略"
    );
    append_event(
        &event_path,
        json!({"ts_ms": now_ms, "event":"runner_start", "mode":mode.as_str(), "config":cfg}),
    )?;

    let spot_info = get_json(&http, &format!("{SPOT_BASE}/api/v3/exchangeInfo")).await?;
    let spot_symbols: HashSet<String> = spot_info["symbols"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|s| {
            s["status"] == "TRADING"
                && s["quoteAsset"] == "USDT"
                && s["isSpotTradingAllowed"] == true
        })
        .filter_map(|s| s["symbol"].as_str().map(str::to_owned))
        .collect();
    let excluded: HashSet<&str> = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
        .into_iter()
        .collect();

    loop {
        if *shutdown.borrow() {
            break;
        }
        let scan_ms = chrono::Utc::now().timestamp_millis();
        let tickers = get_json(&http, &format!("{FUTURES_BASE}/fapi/v1/ticker/24hr")).await?;
        let exchange = get_json(&http, &format!("{FUTURES_BASE}/fapi/v1/exchangeInfo")).await?;
        let active: HashSet<String> = exchange["symbols"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|s| {
                s["status"] == "TRADING"
                    && s["contractType"] == "PERPETUAL"
                    && s["quoteAsset"] == "USDT"
            })
            .filter_map(|s| s["symbol"].as_str().map(str::to_owned))
            .collect();
        let mut shortlist: Vec<(String, f64)> = tickers
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| {
                let symbol = t["symbol"].as_str()?.to_owned();
                let volume = t["quoteVolume"].as_str()?.parse::<f64>().ok()?;
                let change = t["priceChangePercent"].as_str()?.parse::<f64>().ok()?.abs();
                (active.contains(&symbol)
                    && spot_symbols.contains(&symbol)
                    && !excluded.contains(symbol.as_str())
                    && volume >= cfg.min_24h_volume_usd
                    && change >= 4.0)
                    .then_some((symbol, volume * (1.0 + change / 100.0)))
            })
            .collect();
        shortlist.sort_by(|a, b| b.1.total_cmp(&a.1));
        shortlist.truncate(cfg.scan_limit);
        // 已持仓标的即使跌出动量扫描池也必须继续取价、跟踪止损和时间退出。
        for symbol in state.positions.keys() {
            if !shortlist.iter().any(|(item, _)| item == symbol) {
                shortlist.push((symbol.clone(), f64::INFINITY));
            }
        }
        let shortlist_count = shortlist.len();

        let mut set = tokio::task::JoinSet::new();
        for (symbol, _) in shortlist {
            set.spawn(fetch_bars(http.clone(), symbol));
        }
        let mut bars_by_symbol = HashMap::new();
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok((symbol, bars))) => {
                    bars_by_symbol.insert(symbol, bars);
                }
                Ok(Err(e)) => warn!(error=%e, "候选 K 线拉取失败"),
                Err(e) => warn!(error=%e, "候选任务失败"),
            }
        }
        let prices: HashMap<String, f64> = bars_by_symbol
            .iter()
            .filter_map(|(s, b)| b.last().map(|x| (s.clone(), x.close)))
            .collect();
        let current_equity = equity(&state, &prices);
        let current_day = scan_ms / DAY_MS;
        if current_day != state.day {
            state.day = current_day;
            state.day_start_equity = current_equity;
            state.daily_entries = 0;
        }

        // 先管理已有仓位。实盘/模拟盘以交易所仓位为准，dry 用闭合 K 线模拟止损。
        let position_symbols: Vec<String> = state.positions.keys().cloned().collect();
        for symbol in position_symbols {
            let Some(mut position) = state.positions.get(&symbol).cloned() else {
                continue;
            };
            let Some(bars) = bars_by_symbol.get(&symbol) else {
                continue;
            };
            let Some(bar) = bars.last() else { continue };
            if let Some(client) = rest.as_ref() {
                if client.position_amt(&symbol).await?.abs() <= 1e-12 {
                    let (exit, exit_qty, exit_fee, reconciled) =
                        match closing_fill(client, &position).await? {
                            Some((price, qty, fee)) => (price, qty.min(position.qty), fee, true),
                            None => (bar.close, position.qty, 0.0, false),
                        };
                    let pnl = position.side as f64 * exit_qty * (exit - position.entry_price)
                        - position.entry_fee
                        - exit_fee;
                    state.cash +=
                        position.side as f64 * exit_qty * (exit - position.entry_price) - exit_fee;
                    state.realized_pnl += pnl;
                    state.fees += exit_fee;
                    state.total_exits += 1;
                    if pnl > 0.0 {
                        state.wins += 1;
                    }
                    state.positions.remove(&symbol);
                    state.cooldown_until.insert(
                        symbol.clone(),
                        scan_ms + cfg.cooldown_hours as i64 * 3_600_000,
                    );
                    let reason = detected_exit_reason(&position, exit, reconciled);
                    let event = json!({"ts_ms":scan_ms,"event":"exit_detected","symbol":symbol,"side":position.side,"price":exit,"qty":exit_qty,"pnl":pnl,"fee":exit_fee,"reason":reason,"protection_order_id":position.protection_order_id,"stop_price":position.stop_price,"trade_reconciled":reconciled});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                    continue;
                }
            }
            if bar.open_ms <= position.last_bar_ms {
                continue;
            }
            position.last_bar_ms = bar.open_ms;
            if position.side > 0 {
                position.extreme = position.extreme.max(bar.high);
            } else {
                position.extreme = position.extreme.min(bar.low);
            }
            let excursion = position.side as f64 * (position.extreme / position.entry_price - 1.0);
            if excursion >= cfg.trail_activation_pct {
                let trail = position.extreme * (1.0 - position.side as f64 * cfg.trail_pct);
                position.stop_price = if position.side > 0 {
                    position.stop_price.max(trail)
                } else {
                    position.stop_price.min(trail)
                };
                position.protection_reason = "trailing_take_profit".to_owned();
            }
            let stopped = if position.side > 0 {
                bar.low <= position.stop_price
            } else {
                bar.high >= position.stop_price
            };
            let timed = scan_ms - position.entry_ms >= cfg.max_hold_hours as i64 * 3_600_000;
            if mode == TradeMode::Dry && (stopped || timed) {
                let exit = if stopped {
                    position.stop_price
                } else {
                    bar.close
                };
                let fee = position.qty * exit * 0.0005;
                let pnl = position.side as f64 * position.qty * (exit - position.entry_price)
                    - position.entry_fee
                    - fee;
                state.cash +=
                    position.side as f64 * position.qty * (exit - position.entry_price) - fee;
                state.realized_pnl += pnl;
                state.fees += fee;
                state.total_exits += 1;
                if pnl > 0.0 {
                    state.wins += 1;
                }
                state.positions.remove(&symbol);
                state.cooldown_until.insert(
                    symbol.clone(),
                    scan_ms + cfg.cooldown_hours as i64 * 3_600_000,
                );
                let reason = if timed {
                    "time"
                } else if position.protection_reason == "trailing_take_profit" {
                    "trailing_take_profit"
                } else {
                    "initial_stop"
                };
                let event = json!({"ts_ms":scan_ms,"event":"exit","symbol":symbol,"side":position.side,"reason":reason,"price":exit,"qty":position.qty,"pnl":pnl,"fee":fee});
                append_event(&event_path, event.clone())?;
                state.record_trade(event);
            } else if let Some(client) = rest.as_ref() {
                if timed {
                    client.cancel_all_open_orders(&symbol).await?;
                    let filters = client.symbol_filters(&symbol).await?;
                    let side = if position.side > 0 { "SELL" } else { "BUY" };
                    let order = client
                        .place_order(
                            &symbol,
                            side,
                            "MARKET",
                            position.qty,
                            None,
                            None,
                            true,
                            &filters,
                        )
                        .await?;
                    let (exit, _, fee) = wait_fill(client, &symbol, order).await?;
                    let pnl = position.side as f64 * position.qty * (exit - position.entry_price)
                        - position.entry_fee
                        - fee;
                    state.cash +=
                        position.side as f64 * position.qty * (exit - position.entry_price) - fee;
                    state.realized_pnl += pnl;
                    state.fees += fee;
                    state.total_exits += 1;
                    if pnl > 0.0 {
                        state.wins += 1;
                    }
                    state.positions.remove(&symbol);
                    state.cooldown_until.insert(
                        symbol.clone(),
                        scan_ms + cfg.cooldown_hours as i64 * 3_600_000,
                    );
                    let event = json!({"ts_ms":scan_ms,"event":"exit","symbol":symbol,"side":position.side,"reason":"time","price":exit,"qty":position.qty,"pnl":pnl,"fee":fee});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                } else {
                    // 每根闭合 K 线重挂一次保护单，使交易所始终持有真实止损。
                    client.cancel_all_open_orders(&symbol).await?;
                    let filters = client.symbol_filters(&symbol).await?;
                    let side = if position.side > 0 { "SELL" } else { "BUY" };
                    let protection_order_id = client
                        .place_order(
                            &symbol,
                            side,
                            "STOP_MARKET",
                            position.qty,
                            None,
                            Some(position.stop_price),
                            true,
                            &filters,
                        )
                        .await?;
                    position.protection_order_id = Some(protection_order_id);
                    state.positions.insert(symbol, position);
                }
            } else {
                state.positions.insert(symbol, position);
            }
        }

        let mut candidates: Vec<Candidate> = bars_by_symbol
            .into_iter()
            .filter_map(|(s, bars)| evaluate(s, &bars, &cfg))
            .collect();
        candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
        // OI、现货同步与永续溢价先作为观察字段完整记录，不在尚未历史验证前
        // 偷偷增加硬门槛。仅丰富最接近的 10 个，控制公共 API 权重。
        let enrich_count = candidates.len().min(10);
        let mut enrich = tokio::task::JoinSet::new();
        for (index, candidate) in candidates.iter().take(enrich_count).cloned().enumerate() {
            let client = http.clone();
            enrich.spawn(async move { (index, enrich_candidate(client, candidate).await) });
        }
        while let Some(Ok((index, candidate))) = enrich.join_next().await {
            candidates[index] = candidate;
        }
        // 仓位管理可能刚刚产生止损/时间退出，必须用更新后的现金重新计算，
        // 避免同一扫描周期在触发日损门槛后又开出新仓。
        let managed_equity = equity(&state, &prices);
        let daily_loss_blocked =
            managed_equity < state.day_start_equity * (1.0 - cfg.daily_loss_limit);
        let mut eligible_count = 0usize;
        for candidate in candidates.iter().filter(|c| c.eligible()) {
            eligible_count += 1;
            if state.positions.len() >= cfg.max_positions
                || state.daily_entries >= cfg.max_daily_entries
                || daily_loss_blocked
            {
                break;
            }
            if state.positions.contains_key(&candidate.symbol)
                || state
                    .cooldown_until
                    .get(&candidate.symbol)
                    .copied()
                    .unwrap_or(0)
                    > scan_ms
                || state.seen_signal.get(&candidate.symbol).copied() == Some(candidate.signal_ms)
            {
                continue;
            }
            state
                .seen_signal
                .insert(candidate.symbol.clone(), candidate.signal_ms);
            let gross: f64 = state.positions.values().map(|p| p.initial_notional).sum();
            let notional = (managed_equity * cfg.risk_per_trade / cfg.stop_pct)
                .min((managed_equity * cfg.max_gross_multiple - gross).max(0.0));
            if notional < 20.0 {
                continue;
            }
            let mut entry = candidate.price;
            let mut qty = notional / entry;
            let mut fee = notional * 0.0005;
            let mut protection_order_id = None;
            if let Some(client) = rest.as_ref() {
                if let Err(error) = client
                    .set_leverage(&candidate.symbol, cfg.exchange_leverage)
                    .await
                {
                    state.note_execution_issue(
                        scan_ms,
                        &candidate.symbol,
                        "set_leverage",
                        error.to_string(),
                    );
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"set_leverage","symbol":candidate.symbol,"reason":error.to_string()}),
                    )?;
                    continue;
                }
                if let Err(error) = client.set_margin_isolated(&candidate.symbol).await {
                    state.note_execution_issue(
                        scan_ms,
                        &candidate.symbol,
                        "set_margin",
                        error.to_string(),
                    );
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"set_margin","symbol":candidate.symbol,"reason":error.to_string()}),
                    )?;
                    continue;
                }
                let filters = match client.symbol_filters(&candidate.symbol).await {
                    Ok(filters) => filters,
                    Err(e) => {
                        state.note_execution_issue(
                            scan_ms,
                            &candidate.symbol,
                            "exchange_info",
                            e.to_string(),
                        );
                        append_event(
                            &event_path,
                            json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"exchange_info","symbol":candidate.symbol,"reason":format!("demo 不支持或精度失败: {e}")}),
                        )?;
                        continue;
                    }
                };
                let requested_qty = qty;
                // 入场使用 LIMIT IOC，但随后必须用一张 STOP_MARKET 覆盖全部仓位，
                // 因此同时遵守 LOT_SIZE 与 MARKET_LOT_SIZE 的较小 maxQty。
                let entry_max_qty = filters.max_qty.min(filters.market_max_qty);
                if entry_max_qty.is_finite() && qty > entry_max_qty {
                    qty = entry_max_qty;
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_quantity_capped","symbol":candidate.symbol,"requested_qty":requested_qty,"capped_qty":qty,"max_entry_qty":entry_max_qty,"lot_max_qty":filters.max_qty,"market_max_qty":filters.market_max_qty,"requested_notional":notional,"capped_notional":qty*candidate.price}),
                    )?;
                }
                let capped_notional = qty * candidate.price;
                if capped_notional < filters.min_notional {
                    let reason = format!(
                        "名义仓位 {:.2} USDT 低于交易所最小值 {:.2} USDT",
                        capped_notional, filters.min_notional
                    );
                    state.note_execution_issue(
                        scan_ms,
                        &candidate.symbol,
                        "minimum_notional",
                        reason.clone(),
                    );
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"minimum_notional","symbol":candidate.symbol,"reason":reason}),
                    )?;
                    continue;
                }
                let side = if candidate.side > 0 { "BUY" } else { "SELL" };
                let formatted_qty = live::rest::fmt_step_with_precision(
                    qty,
                    filters.step_size,
                    filters.quantity_precision,
                );
                append_event(
                    &event_path,
                    json!({
                        "ts_ms":scan_ms,"event":"entry_attempt","symbol":candidate.symbol,
                        "side":side,"raw_qty":qty,"formatted_qty":formatted_qty,
                        "notional":capped_notional,"price":candidate.price,
                        "order_kind":"LIMIT_IOC","max_entry_slippage_pct":cfg.max_entry_slippage_pct,
                        "filters":{"lot_step":filters.step_size,"market_step":filters.market_step_size,
                            "quantity_precision":filters.quantity_precision,"tick_size":filters.tick_size,
                            "price_precision":filters.price_precision,"min_notional":filters.min_notional,
                            "lot_max_qty":filters.max_qty,"market_max_qty":filters.market_max_qty},
                        "signal":candidate
                    }),
                )?;
                let guard_price =
                    candidate.price * (1.0 + candidate.side as f64 * cfg.max_entry_slippage_pct);
                let order = match client
                    .place_order(
                        &candidate.symbol,
                        side,
                        "LIMIT_IOC",
                        qty,
                        Some(guard_price),
                        None,
                        false,
                        &filters,
                    )
                    .await
                {
                    Ok(order) => order,
                    Err(error) => {
                        let reason = format!(
                            "{error}; formatted_qty={formatted_qty}; market_step={}; quantity_precision={}; market_max_qty={}",
                            filters.market_step_size, filters.quantity_precision, filters.market_max_qty
                        );
                        state.note_execution_issue(
                            scan_ms,
                            &candidate.symbol,
                            "entry_order",
                            reason.clone(),
                        );
                        append_event(
                            &event_path,
                            json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"entry_order","symbol":candidate.symbol,"reason":reason,"formatted_qty":formatted_qty}),
                        )?;
                        continue;
                    }
                };
                match wait_fill(client, &candidate.symbol, order).await {
                    Ok(fill) => (entry, qty, fee) = fill,
                    Err(error) => {
                        match emergency_flatten(client, &candidate.symbol, &filters).await {
                            Ok(None) => {
                                append_event(
                                    &event_path,
                                    json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"price_guard_unfilled","symbol":candidate.symbol,"order_id":order,"reason":"1.5% 追价保护 IOC 未成交","detail":error.to_string()}),
                                )?;
                                continue;
                            }
                            Ok(Some(flatten)) => {
                                state.note_execution_issue(
                                    scan_ms,
                                    &candidate.symbol,
                                    "fill_reconciliation",
                                    error.to_string(),
                                );
                                append_event(
                                    &event_path,
                                    json!({"ts_ms":scan_ms,"event":"entry_reconciliation_failed","symbol":candidate.symbol,"order_id":order,"reason":error.to_string(),"emergency_flatten":format!("{flatten:?}")}),
                                )?;
                                anyhow::bail!(
                                    "{} IOC 成交回报超时，已应急平仓；需核对成交日志",
                                    candidate.symbol
                                );
                            }
                            Err(flatten_error) => {
                                anyhow::bail!(
                                    "{} IOC 成交回报不明且应急平仓失败: {flatten_error}",
                                    candidate.symbol
                                );
                            }
                        }
                    }
                }
                let stop = entry * (1.0 - candidate.side as f64 * cfg.stop_pct);
                let close_side = if candidate.side > 0 { "SELL" } else { "BUY" };
                match client
                    .place_order(
                        &candidate.symbol,
                        close_side,
                        "STOP_MARKET",
                        qty,
                        None,
                        Some(stop),
                        true,
                        &filters,
                    )
                    .await
                {
                    Ok(order_id) => protection_order_id = Some(order_id),
                    Err(error) => {
                        state.note_execution_issue(
                            scan_ms,
                            &candidate.symbol,
                            "protective_stop",
                            error.to_string(),
                        );
                        let emergency =
                            emergency_flatten(client, &candidate.symbol, &filters).await;
                        append_event(
                            &event_path,
                            json!({"ts_ms":scan_ms,"event":"protection_failed","symbol":candidate.symbol,"entry_price":entry,"qty":qty,"stop_price":stop,"reason":error.to_string(),"emergency_flatten":format!("{emergency:?}")}),
                        )?;
                        emergency.with_context(|| {
                            format!("{} 止损挂单失败且应急平仓失败", candidate.symbol)
                        })?;
                        continue;
                    }
                }
            }
            let stop = entry * (1.0 - candidate.side as f64 * cfg.stop_pct);
            state.last_execution_issue = None;
            state.cash -= fee;
            state.fees += fee;
            state.daily_entries += 1;
            state.total_entries += 1;
            state.positions.insert(
                candidate.symbol.clone(),
                Position {
                    symbol: candidate.symbol.clone(),
                    side: candidate.side,
                    qty,
                    entry_ms: scan_ms,
                    entry_price: entry,
                    entry_fee: fee,
                    initial_notional: qty * entry,
                    extreme: entry,
                    stop_price: stop,
                    last_bar_ms: candidate.signal_ms,
                    protection_order_id,
                    protection_reason: default_protection_reason(),
                },
            );
            let event = json!({"ts_ms":scan_ms,"event":"entry","symbol":candidate.symbol,"side":candidate.side,"signal":candidate,"entry_price":entry,"qty":qty,"notional":qty*entry,"margin_estimate":qty*entry/cfg.exchange_leverage as f64,"risk_usd":qty*entry*cfg.stop_pct,"fee":fee});
            append_event(&event_path, event.clone())?;
            state.record_trade(event);
        }
        save_state(&state_path, &state)?;
        let current_equity = equity(&state, &prices);
        let mut open_positions: Vec<_> = state.positions.values().collect();
        open_positions.sort_by_key(|position| position.entry_ms);
        let position_status: Vec<Value> = open_positions
            .into_iter()
            .map(|position| {
                let mark_price = prices
                    .get(&position.symbol)
                    .copied()
                    .unwrap_or(position.entry_price);
                let unrealized_pnl =
                    position.side as f64 * position.qty * (mark_price - position.entry_price);
                json!({
                    "symbol":position.symbol, "side":position.side, "qty":position.qty,
                    "entry_ms":position.entry_ms, "entry_price":position.entry_price,
                    "initial_notional":position.initial_notional, "stop_price":position.stop_price,
                    "extreme":position.extreme, "mark_price":mark_price,
                    "protection_order_id":position.protection_order_id,
                    "protection_reason":position.protection_reason,
                    "unrealized_pnl":unrealized_pnl,
                    "return_pct":position.side as f64 * (mark_price / position.entry_price - 1.0)
                })
            })
            .collect();
        append_event(
            &event_path,
            json!({
                "ts_ms":scan_ms, "event":"scan", "universe_count":spot_symbols.intersection(&active).count(),
                "shortlist_count":shortlist_count, "eligible_count":eligible_count,
                "equity":current_equity, "daily_entries":state.daily_entries,
                "daily_loss_blocked":daily_loss_blocked,
                "candidates":candidates.iter().take(10).collect::<Vec<_>>()
            }),
        )?;
        if let Some(tx) = &status_tx {
            let _ = tx.send(json!({
                "state":"running", "mode":mode.as_str(), "started_at_ms":started_ms,
                "uptime_s":(scan_ms-started_ms)/1000, "strategy_name":args.strategy,
                "strategy_hash":strategy_hash, "git_commit":git_commit,
                "equity":current_equity, "cash":state.cash, "position":Value::Null,
                "n_intents":state.total_entries, "n_fills":state.total_entries + state.total_exits,
                "altcoin_impulse": {
                    "stage": if daily_loss_blocked {"risk_blocked"} else if eligible_count>0 {"execution"} else if shortlist_count>0 {"confirmation"} else {"scan"},
                    "universe_count":spot_symbols.intersection(&active).count(), "shortlist_count":shortlist_count,
                    "eligible_count":eligible_count, "last_scan_ms":scan_ms,
                    "exchange_leverage":cfg.exchange_leverage, "risk_per_trade":cfg.risk_per_trade,
                    "stop_pct":cfg.stop_pct, "notional_per_trade_estimate":current_equity*cfg.risk_per_trade/cfg.stop_pct,
                    "margin_per_trade_estimate":current_equity*cfg.risk_per_trade/cfg.stop_pct/cfg.exchange_leverage as f64,
                    "worst_loss_per_trade_estimate":current_equity*cfg.risk_per_trade,
                    "daily_entries":state.daily_entries, "max_daily_entries":cfg.max_daily_entries,
                    "daily_loss_blocked":daily_loss_blocked, "positions":position_status,
                    "total_entries":state.total_entries, "total_exits":state.total_exits, "wins":state.wins,
                    "rejected_entries":state.rejected_entries, "last_execution_issue":state.last_execution_issue,
                    "recent_trades":state.recent_trades,
                    "realized_pnl":state.realized_pnl, "fees":state.fees,
                    "candidates":candidates.into_iter().take(10).collect::<Vec<_>>(),
                    "journal":event_path,
                }
            }));
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(cfg.poll_seconds.max(15))) => {},
            changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
        }
    }
    save_state(&state_path, &state)?;
    append_event(
        &event_path,
        json!({"ts_ms":chrono::Utc::now().timestamp_millis(),"event":"runner_stop"}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{detected_exit_reason, Position};

    #[test]
    fn exchange_leverage_does_not_change_stop_risk() {
        let equity = 1_000.0;
        let risk = 0.10;
        let stop = 0.05;
        let leverage = 10.0;
        let notional = equity * risk / stop;
        assert_eq!(notional, 2_000.0);
        assert_eq!(notional / leverage, 200.0);
        assert_eq!(notional * stop, 100.0);
    }

    #[test]
    fn detected_exit_uses_persisted_protection_stage() {
        let mut position = Position {
            symbol: "TESTUSDT".into(),
            side: 1,
            qty: 100.0,
            entry_ms: 1,
            entry_price: 1.0,
            entry_fee: 0.1,
            initial_notional: 100.0,
            extreme: 1.0,
            stop_price: 0.95,
            last_bar_ms: 1,
            protection_order_id: Some(42),
            protection_reason: "initial_stop".into(),
        };
        assert_eq!(detected_exit_reason(&position, 0.949, true), "initial_stop");
        position.stop_price = 1.10;
        position.protection_reason = "trailing_take_profit".into();
        assert_eq!(
            detected_exit_reason(&position, 1.095, true),
            "trailing_take_profit"
        );
        assert_eq!(
            detected_exit_reason(&position, 1.25, true),
            "manual_or_external"
        );
        assert_eq!(detected_exit_reason(&position, 1.10, false), "unknown");
    }
}
