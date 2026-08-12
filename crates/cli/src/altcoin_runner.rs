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

use crate::altcoin_reversal_observer::{
    observer_status, tracked_symbols, update_reversal_observer, AltcoinReversalObserverConfig,
    ReversalObserverState,
};
use crate::trade_runner::{TradeArgs, TradeMode};

const FUTURES_BASE: &str = "https://fapi.binance.com";
const SPOT_BASE: &str = "https://api.binance.com";
const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AltcoinImpulseConfig {
    pub enabled: bool,
    pub capital_usdt: f64,
    pub exchange_leverage: u32,
    #[serde(default = "default_min_exchange_leverage")]
    pub min_exchange_leverage: u32,
    pub risk_per_trade: f64,
    pub max_positions: usize,
    pub max_daily_entries: u32,
    #[serde(default = "default_max_daily_entry_bonus")]
    pub max_daily_entry_bonus: u32,
    pub daily_loss_limit: f64,
    pub max_gross_multiple: f64,
    pub stop_pct: f64,
    #[serde(default = "default_risk_execution_buffer_pct")]
    pub risk_execution_buffer_pct: f64,
    pub trail_activation_pct: f64,
    pub trail_pct: f64,
    #[serde(default = "default_partial_take_profit_fraction")]
    pub partial_take_profit_fraction: f64,
    #[serde(default = "default_recovery_lock_adverse_pct")]
    pub recovery_lock_adverse_pct: f64,
    #[serde(default = "default_recovery_lock_activation_pct")]
    pub recovery_lock_activation_pct: f64,
    #[serde(default = "default_recovery_lock_pct")]
    pub recovery_lock_pct: f64,
    pub max_hold_hours: u32,
    pub cooldown_hours: u32,
    #[serde(default = "default_max_entry_slippage_pct")]
    pub max_entry_slippage_pct: f64,
    #[serde(default = "default_max_signal_age_seconds")]
    pub max_signal_age_seconds: u64,
    pub min_24h_volume_usd: f64,
    pub min_return_1h: f64,
    pub max_return_1h: f64,
    #[serde(default = "default_overextension_long_return_1h")]
    pub overextension_long_return_1h: f64,
    #[serde(default = "default_overextension_long_return_4h")]
    pub overextension_long_return_4h: f64,
    #[serde(default = "default_overextension_reentry_lookback_hours")]
    pub overextension_reentry_lookback_hours: u32,
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
    #[serde(default)]
    altcoin_reversal_observer: AltcoinReversalObserverConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct Bar {
    pub(crate) open_ms: i64,
    pub(crate) close_ms: i64,
    pub(crate) open: f64,
    pub(crate) high: f64,
    pub(crate) low: f64,
    pub(crate) close: f64,
    pub(crate) quote_volume: f64,
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
    entry_phase: String,
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
    #[serde(default)]
    adverse_extreme: Option<f64>,
    stop_price: f64,
    last_bar_ms: i64,
    #[serde(default)]
    protection_order_id: Option<i64>,
    #[serde(default = "default_protection_reason")]
    protection_reason: String,
    #[serde(default)]
    exchange_leverage: Option<u32>,
    #[serde(default = "default_entry_phase")]
    entry_phase: String,
    #[serde(default)]
    partial_take_profit_done: bool,
    #[serde(default)]
    last_partial_exit_ms: Option<i64>,
    #[serde(default)]
    realized_partial_pnl: f64,
}

fn default_max_entry_slippage_pct() -> f64 {
    0.015
}

fn default_max_daily_entry_bonus() -> u32 {
    4
}

fn default_max_signal_age_seconds() -> u64 {
    120
}

fn default_recovery_lock_adverse_pct() -> f64 {
    0.03
}

fn default_recovery_lock_activation_pct() -> f64 {
    0.01
}

fn default_recovery_lock_pct() -> f64 {
    0.0025
}

fn default_overextension_long_return_1h() -> f64 {
    0.12
}

fn default_overextension_long_return_4h() -> f64 {
    0.15
}

fn default_overextension_reentry_lookback_hours() -> u32 {
    24
}

fn default_risk_execution_buffer_pct() -> f64 {
    0.006
}

fn default_partial_take_profit_fraction() -> f64 {
    0.5
}

fn default_entry_phase() -> String {
    "standard_impulse".to_owned()
}

fn default_min_exchange_leverage() -> u32 {
    5
}

fn default_protection_reason() -> String {
    "initial_stop".to_owned()
}

fn signal_age_ms(now_ms: i64, signal_ms: i64) -> i64 {
    now_ms.saturating_sub(signal_ms).max(0)
}

fn detected_exit_reason(position: &Position, exit_price: f64, reconciled: bool) -> &'static str {
    if !reconciled || position.stop_price <= 0.0 {
        return "unknown";
    }
    let distance = (exit_price / position.stop_price - 1.0).abs();
    if distance <= 0.02 {
        match position.protection_reason.as_str() {
            "trailing_take_profit" => "trailing_take_profit",
            "recovery_profit_lock" => "recovery_profit_lock",
            "partial_take_profit_break_even" => "partial_take_profit_break_even",
            _ => "initial_stop",
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct EquityPoint {
    ts_ms: i64,
    equity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedState {
    cash: f64,
    realized_pnl: f64,
    fees: f64,
    #[serde(default)]
    initial_equity: f64,
    #[serde(default)]
    equity_curve: Vec<EquityPoint>,
    positions: HashMap<String, Position>,
    cooldown_until: HashMap<String, i64>,
    seen_signal: HashMap<String, i64>,
    day: i64,
    day_start_equity: f64,
    daily_entries: u32,
    #[serde(default)]
    daily_entry_bonus: u32,
    #[serde(default)]
    daily_risk_resets: u32,
    #[serde(default)]
    last_daily_risk_reset_ms: Option<i64>,
    #[serde(default)]
    overextension_long_blocked: bool,
    #[serde(default)]
    overextension_long_losses: u32,
    #[serde(default)]
    last_overextension_loss_ms: Option<i64>,
    total_entries: u64,
    total_exits: u64,
    wins: u64,
    #[serde(default)]
    rejected_entries: u64,
    #[serde(default)]
    last_execution_issue: Option<ExecutionIssue>,
    #[serde(default)]
    recent_trades: Vec<Value>,
    #[serde(default)]
    reversal_observer: ReversalObserverState,
}

impl PersistedState {
    fn new(cash: f64, now_ms: i64) -> Self {
        Self {
            cash,
            realized_pnl: 0.0,
            fees: 0.0,
            initial_equity: cash,
            equity_curve: Vec::new(),
            positions: HashMap::new(),
            cooldown_until: HashMap::new(),
            seen_signal: HashMap::new(),
            day: now_ms / DAY_MS,
            day_start_equity: cash,
            daily_entries: 0,
            daily_entry_bonus: 0,
            daily_risk_resets: 0,
            last_daily_risk_reset_ms: None,
            overextension_long_blocked: false,
            overextension_long_losses: 0,
            last_overextension_loss_ms: None,
            total_entries: 0,
            total_exits: 0,
            wins: 0,
            rejected_entries: 0,
            last_execution_issue: None,
            recent_trades: Vec::new(),
            reversal_observer: ReversalObserverState::default(),
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

pub(crate) fn parse_bars(value: Value, now_ms: i64) -> Result<Vec<Bar>> {
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
                open: parse_num(row, 1)?,
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
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn entry_phase(
    side: i32,
    return_1h: f64,
    return_4h: f64,
    threshold_1h: f64,
    threshold_4h: f64,
) -> &'static str {
    if side > 0 && (return_1h >= threshold_1h || return_4h >= threshold_4h) {
        "overextended_long"
    } else {
        "standard_impulse"
    }
}

fn recently_exited_symbol(
    trades: &[Value],
    symbol: &str,
    now_ms: i64,
    lookback_hours: u32,
) -> bool {
    let cutoff = now_ms.saturating_sub(lookback_hours as i64 * 3_600_000);
    trades.iter().rev().any(|event| {
        matches!(event["event"].as_str(), Some("exit" | "exit_detected"))
            && event["symbol"] == symbol
            && event["ts_ms"]
                .as_i64()
                .is_some_and(|ts_ms| ts_ms >= cutoff && ts_ms <= now_ms)
    })
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
        entry_phase: entry_phase(
            side,
            return_1h,
            return_4h,
            cfg.overextension_long_return_1h,
            cfg.overextension_long_return_4h,
        )
        .to_owned(),
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

pub(crate) async fn get_json(http: &reqwest::Client, url: &str) -> Result<Value> {
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

pub(crate) fn append_event(path: &str, event: Value) -> Result<()> {
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
                Some("entry" | "partial_exit" | "exit" | "exit_detected")
            )
        })
        .collect();
    if events.len() > 100 {
        events.drain(..events.len() - 100);
    }
    events
}

fn record_daily_equity(curve: &mut Vec<EquityPoint>, ts_ms: i64, equity: f64) {
    if !equity.is_finite() || equity <= 0.0 {
        return;
    }
    let day = ts_ms.div_euclid(DAY_MS);
    if let Some(last) = curve.last_mut() {
        let last_day = last.ts_ms.div_euclid(DAY_MS);
        if last_day == day {
            *last = EquityPoint { ts_ms, equity };
            return;
        }
    }
    curve.push(EquityPoint { ts_ms, equity });
    // 状态接口只需要支持长期周/月统计；保留两年日线足够，同时避免轮询载荷无限增长。
    if curve.len() > 730 {
        curve.drain(..curve.len() - 730);
    }
}

fn load_daily_equity(path: &str) -> Vec<EquityPoint> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut curve = Vec::new();
    for event in text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
    {
        if event["event"] != "scan" {
            continue;
        }
        let (Some(ts_ms), Some(equity)) = (event["ts_ms"].as_i64(), event["equity"].as_f64())
        else {
            continue;
        };
        record_daily_equity(&mut curve, ts_ms, equity);
    }
    curve
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

async fn build_position_status(
    state: &PersistedState,
    prices: &HashMap<String, f64>,
    rest: Option<&live::RestClient>,
    valuation_ms: i64,
) -> Vec<Value> {
    let mut open_positions: Vec<_> = state.positions.values().collect();
    open_positions.sort_by_key(|position| position.entry_ms);
    let mut result = Vec::with_capacity(open_positions.len());
    for position in open_positions {
        let fallback_mark = prices
            .get(&position.symbol)
            .copied()
            .unwrap_or(position.entry_price);
        let fallback_pnl =
            position.side as f64 * position.qty * (fallback_mark - position.entry_price);
        let exchange = if let Some(client) = rest {
            match client.position_risk(&position.symbol).await {
                Ok(snapshot) if snapshot.position_amt.abs() > 1e-12 => Some(snapshot),
                Ok(_) => None,
                Err(error) => {
                    warn!(symbol=%position.symbol, error=%error, "实时仓位估值失败，暂用已收盘 K 线");
                    None
                }
            }
        } else {
            None
        };
        let (qty, entry_price, mark_price, unrealized_pnl, valuation_source) = match exchange {
            Some(snapshot) => (
                snapshot.position_amt.abs(),
                snapshot.entry_price,
                snapshot.mark_price,
                snapshot.unrealized_profit,
                "binance_position_risk",
            ),
            None => (
                position.qty,
                position.entry_price,
                fallback_mark,
                fallback_pnl,
                "closed_15m_fallback",
            ),
        };
        let entry_notional = qty * entry_price;
        let adverse = position.adverse_extreme.unwrap_or(position.entry_price);
        result.push(json!({
            "symbol":position.symbol, "side":position.side, "qty":qty,
            "entry_ms":position.entry_ms, "entry_price":entry_price,
            "initial_notional":position.initial_notional, "stop_price":position.stop_price,
            "extreme":position.extreme, "mark_price":mark_price,
            "protection_order_id":position.protection_order_id,
            "protection_reason":position.protection_reason,
            "entry_phase":position.entry_phase,
            "partial_take_profit_done":position.partial_take_profit_done,
            "realized_partial_pnl":position.realized_partial_pnl,
            "max_favorable_excursion_pct": position.side as f64 * (position.extreme / position.entry_price - 1.0),
            "max_adverse_excursion_pct": adverse_excursion(position.side, position.entry_price, adverse),
            "exchange_leverage":position.exchange_leverage,
            "unrealized_pnl":unrealized_pnl,
            "return_pct": if entry_notional > 0.0 { unrealized_pnl / entry_notional } else { 0.0 },
            "valuation_source":valuation_source,
            "valuation_ms":valuation_ms
        }));
    }
    result
}

fn positions_unrealized(positions: &[Value]) -> f64 {
    positions
        .iter()
        .filter_map(|position| position["unrealized_pnl"].as_f64())
        .sum()
}

fn take_control_commands(
    receiver: &mut Option<tokio::sync::watch::Receiver<u64>>,
    handled: &mut u64,
) -> u64 {
    let Some(receiver) = receiver.as_mut() else {
        return 0;
    };
    let sequence = *receiver.borrow_and_update();
    if sequence <= *handled {
        return 0;
    }
    let count = sequence - *handled;
    *handled = sequence;
    count
}

fn realtime_trailing_stop(
    side: i32,
    entry_price: f64,
    previous_extreme: f64,
    current_stop: f64,
    mark_price: f64,
    activation_pct: f64,
    trail_pct: f64,
) -> (f64, Option<f64>) {
    let extreme = if side > 0 {
        previous_extreme.max(mark_price)
    } else {
        previous_extreme.min(mark_price)
    };
    let excursion = side as f64 * (extreme / entry_price - 1.0);
    if excursion < activation_pct {
        return (extreme, None);
    }
    let proposed = extreme * (1.0 - side as f64 * trail_pct);
    let improved = if side > 0 {
        current_stop.max(proposed)
    } else {
        current_stop.min(proposed)
    };
    // 至少改善 0.10% 才换单，防止单边行情中每个微小 tick 都触发撤挂。
    let material = if side > 0 {
        improved >= current_stop * 1.001
    } else {
        improved <= current_stop * 0.999
    };
    (extreme, material.then_some(improved))
}

fn update_adverse_extreme(side: i32, previous: Option<f64>, entry: f64, price: f64) -> f64 {
    let previous = previous.unwrap_or(entry);
    if side > 0 {
        previous.min(price)
    } else {
        previous.max(price)
    }
}

fn adverse_excursion(side: i32, entry: f64, adverse_extreme: f64) -> f64 {
    (-side as f64 * (adverse_extreme / entry - 1.0)).max(0.0)
}

fn recovery_profit_lock_stop(
    side: i32,
    entry_price: f64,
    current_stop: f64,
    mark_price: f64,
    adverse_excursion: f64,
    adverse_threshold: f64,
    recovery_activation: f64,
    lock_pct: f64,
) -> Option<f64> {
    let current_return = side as f64 * (mark_price / entry_price - 1.0);
    if adverse_excursion + 1e-12 < adverse_threshold || current_return + 1e-12 < recovery_activation
    {
        return None;
    }
    let proposed = entry_price * (1.0 + side as f64 * lock_pct);
    let improved = if side > 0 {
        current_stop.max(proposed)
    } else {
        current_stop.min(proposed)
    };
    let material = if side > 0 {
        improved >= current_stop * 1.001
    } else {
        improved <= current_stop * 0.999
    };
    material.then_some(improved)
}

fn clamp_entry_guard_price(
    side: i32,
    signal_price: f64,
    max_slippage_pct: f64,
    execution_mark: f64,
    multiplier_up: f64,
    multiplier_down: f64,
    tick_size: f64,
) -> (f64, f64) {
    let raw = signal_price * (1.0 + side as f64 * max_slippage_pct);
    let clamped = if side > 0 {
        raw.min(execution_mark * multiplier_up - tick_size)
            .max(tick_size)
    } else {
        raw.max(execution_mark * multiplier_down + tick_size)
    };
    (raw, clamped)
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
        .filter(|fill| {
            fill.time
                > position
                    .last_partial_exit_ms
                    .unwrap_or(position.entry_ms - 1)
                && fill.side == closing_side
        })
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

fn record_exit(
    state: &mut PersistedState,
    position: &Position,
    now_ms: i64,
    cooldown_hours: u32,
    exit: f64,
    qty: f64,
    fee: f64,
) -> f64 {
    let pnl = position.side as f64 * qty * (exit - position.entry_price) - position.entry_fee - fee;
    let trade_pnl = position.realized_partial_pnl + pnl;
    state.cash += position.side as f64 * qty * (exit - position.entry_price) - fee;
    state.realized_pnl += pnl;
    state.fees += fee;
    state.total_exits += 1;
    if trade_pnl > 0.0 {
        state.wins += 1;
    }
    if position.entry_phase == "overextended_long" && trade_pnl < 0.0 {
        state.overextension_long_blocked = true;
        state.overextension_long_losses = state.overextension_long_losses.saturating_add(1);
        state.last_overextension_loss_ms = Some(now_ms);
    }
    state.positions.remove(&position.symbol);
    state.cooldown_until.insert(
        position.symbol.clone(),
        now_ms + cooldown_hours as i64 * 3_600_000,
    );
    pnl
}

fn record_partial_exit(
    state: &mut PersistedState,
    position: &mut Position,
    exit: f64,
    qty: f64,
    fee: f64,
) -> f64 {
    let qty = qty.min(position.qty);
    let entry_fee_share = if position.qty > 0.0 {
        position.entry_fee * (qty / position.qty)
    } else {
        0.0
    };
    let pnl = position.side as f64 * qty * (exit - position.entry_price) - entry_fee_share - fee;
    state.cash += position.side as f64 * qty * (exit - position.entry_price) - fee;
    state.realized_pnl += pnl;
    state.fees += fee;
    position.qty -= qty;
    position.entry_fee -= entry_fee_share;
    position.initial_notional = position.qty * position.entry_price;
    position.realized_partial_pnl += pnl;
    pnl
}

fn apply_daily_risk_reset(
    state: &mut PersistedState,
    current_equity: f64,
    now_ms: i64,
    event_path: &str,
) -> Result<()> {
    let previous_baseline = state.day_start_equity;
    state.day = now_ms / DAY_MS;
    state.day_start_equity = current_equity;
    state.daily_risk_resets += 1;
    state.last_daily_risk_reset_ms = Some(now_ms);
    append_event(
        event_path,
        json!({
            "ts_ms":now_ms,
            "event":"daily_risk_reset",
            "source":"manual_frontend",
            "previous_baseline_equity":previous_baseline,
            "new_baseline_equity":current_equity,
            "daily_entries_preserved":state.daily_entries,
            "reset_count":state.daily_risk_resets
        }),
    )?;
    Ok(())
}

fn apply_daily_entry_bonus(
    state: &mut PersistedState,
    base_limit: u32,
    maximum_bonus: u32,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    let previous_bonus = state.daily_entry_bonus;
    state.daily_entry_bonus = state.daily_entry_bonus.saturating_add(2).min(maximum_bonus);
    if state.daily_entry_bonus == previous_bonus {
        return Ok(false);
    }
    append_event(
        event_path,
        json!({
            "ts_ms":now_ms,
            "event":"daily_entry_limit_increased",
            "source":"manual_frontend",
            "base_limit":base_limit,
            "maximum_bonus":maximum_bonus,
            "previous_effective_limit":base_limit+previous_bonus,
            "new_effective_limit":base_limit+state.daily_entry_bonus,
            "daily_entries":state.daily_entries,
            "resets_at":"00:00 UTC"
        }),
    )?;
    Ok(true)
}

/// 模拟盘/实盘的独立持仓管理循环。信号扫描可以维持低频，但交易所仓位必须高频：
/// - 识别交易所止损成交；
/// - 用实时 markPrice 推进极值与跟踪止盈；
/// - 按墙钟执行时间退出。
async fn manage_live_positions(
    state: &mut PersistedState,
    client: &live::RestClient,
    cfg: &AltcoinImpulseConfig,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    let mut changed = false;
    let symbols: Vec<String> = state.positions.keys().cloned().collect();
    for symbol in symbols {
        let Some(mut position) = state.positions.get(&symbol).cloned() else {
            continue;
        };
        let snapshot = client.position_risk(&symbol).await?;
        let mut managed_qty = snapshot.position_amt.abs();
        if snapshot.position_amt.abs() <= 1e-12 {
            if let Err(error) = client.cancel_all_open_orders(&symbol).await {
                warn!(symbol=%symbol, error=%error, "仓位已平，但清理残留保护单失败");
            }
            let Some((exit, exit_qty, exit_fee)) = closing_fill(client, &position).await? else {
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"exit_reconciliation_pending","symbol":symbol,"protection_order_id":position.protection_order_id,"stop_price":position.stop_price}),
                )?;
                continue;
            };
            let exit_qty = exit_qty.min(position.qty);
            let reason = detected_exit_reason(&position, exit, true);
            let pnl = record_exit(
                state,
                &position,
                now_ms,
                cfg.cooldown_hours,
                exit,
                exit_qty,
                exit_fee,
            );
            let trade_pnl = position.realized_partial_pnl + pnl;
            let event = json!({"ts_ms":now_ms,"event":"exit_detected","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":exit_fee,"reason":reason,"protection_order_id":position.protection_order_id,"stop_price":position.stop_price,"trade_reconciled":true,"overextension_long_blocked":state.overextension_long_blocked});
            append_event(event_path, event.clone())?;
            state.record_trade(event);
            changed = true;
            continue;
        }

        let timed = now_ms - position.entry_ms >= cfg.max_hold_hours as i64 * 3_600_000;
        if timed {
            client.cancel_all_open_orders(&symbol).await?;
            let filters = client.symbol_filters(&symbol).await?;
            let side = if position.side > 0 { "SELL" } else { "BUY" };
            let qty = snapshot.position_amt.abs();
            let order = client
                .place_order(&symbol, side, "MARKET", qty, None, None, true, &filters)
                .await?;
            let (exit, exit_qty, fee) = wait_fill(client, &symbol, order).await?;
            let exit_qty = exit_qty.min(position.qty);
            let pnl = record_exit(
                state,
                &position,
                now_ms,
                cfg.cooldown_hours,
                exit,
                exit_qty,
                fee,
            );
            let trade_pnl = position.realized_partial_pnl + pnl;
            let event = json!({"ts_ms":now_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"reason":"time","price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":now_ms-position.entry_ms,"overextension_long_blocked":state.overextension_long_blocked});
            append_event(event_path, event.clone())?;
            state.record_trade(event);
            changed = true;
            continue;
        }

        let previous_extreme = position.extreme;
        let previous_adverse = position.adverse_extreme;
        let adverse = update_adverse_extreme(
            position.side,
            position.adverse_extreme,
            position.entry_price,
            snapshot.mark_price,
        );
        position.adverse_extreme = Some(adverse);
        let max_adverse = adverse_excursion(position.side, position.entry_price, adverse);
        let (new_extreme, improved_stop) = realtime_trailing_stop(
            position.side,
            position.entry_price,
            position.extreme,
            position.stop_price,
            snapshot.mark_price,
            cfg.trail_activation_pct,
            cfg.trail_pct,
        );
        position.extreme = new_extreme;
        let excursion = position.side as f64 * (position.extreme / position.entry_price - 1.0);
        let current_return =
            position.side as f64 * (snapshot.mark_price / position.entry_price - 1.0);
        // 只按“当前仍有的浮盈”兑现，不能因历史上曾到过 +2%、现在已回落而补卖。
        if !position.partial_take_profit_done && current_return >= cfg.trail_activation_pct {
            let filters = client.symbol_filters(&symbol).await?;
            let side = if position.side > 0 { "SELL" } else { "BUY" };
            let target_qty = snapshot.position_amt.abs() * cfg.partial_take_profit_fraction;
            let order = client
                .place_order(
                    &symbol, side, "MARKET", target_qty, None, None, true, &filters,
                )
                .await?;
            let (exit, exit_qty, fee) = wait_fill(client, &symbol, order).await?;
            let exit_qty = exit_qty.min(position.qty);
            let partial_pnl = record_partial_exit(state, &mut position, exit, exit_qty, fee);
            position.partial_take_profit_done = true;
            // 使用成交确认后的墙钟，避免最终平仓对账再次把这笔部分成交累计进去。
            position.last_partial_exit_ms = Some(chrono::Utc::now().timestamp_millis());
            // 从这一刻起交易所已经少了一段仓位；先把本地状态同步，后续任何 API
            // 调用失败时，外层错误分支保存的也不会再是旧的全仓数量。
            state.positions.insert(symbol.clone(), position.clone());

            // 剩余仓位先挂入场附近的保护，确认成功后才撤原始止损。
            let remaining = match client.position_risk(&symbol).await {
                Ok(snapshot) => snapshot.position_amt.abs(),
                Err(error) => {
                    warn!(symbol=%symbol, error=%error, "第一段止盈后仓位查询失败，暂用成交回报推算的余量");
                    position.qty
                }
            };
            managed_qty = remaining;
            let break_even_stop = position.entry_price;
            let replacement = client
                .place_order(
                    &symbol,
                    side,
                    "STOP_MARKET",
                    remaining,
                    None,
                    Some(break_even_stop),
                    true,
                    &filters,
                )
                .await;
            match replacement {
                Ok(new_order_id) => {
                    if let Some(old_order_id) = position.protection_order_id {
                        if let Err(error) = client.cancel_algo_order(&symbol, old_order_id).await {
                            warn!(symbol=%symbol, old_order_id, error=%error, "分段止盈后新保护已生效，但旧保护撤销失败");
                        }
                    }
                    position.protection_order_id = Some(new_order_id);
                    position.stop_price = break_even_stop;
                    position.protection_reason = "partial_take_profit_break_even".to_owned();
                    let event = json!({"ts_ms":now_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"reason":"partial_take_profit","price":exit,"qty":exit_qty,"remaining_qty":remaining,"pnl":partial_pnl,"fee":fee,"new_stop":break_even_stop,"new_order_id":new_order_id,"protection_replaced":true});
                    append_event(event_path, event.clone())?;
                    state.record_trade(event);
                }
                Err(error) => {
                    warn!(symbol=%symbol, error=%error, "第一段止盈已成交，保本保护替换失败；保留原交易所止损并等待下轮重试");
                    let event = json!({"ts_ms":now_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"reason":"partial_take_profit","price":exit,"qty":exit_qty,"remaining_qty":remaining,"pnl":partial_pnl,"fee":fee,"new_stop":position.stop_price,"protection_replaced":false,"protection_error":error.to_string()});
                    append_event(event_path, event.clone())?;
                    state.record_trade(event);
                }
            }
            changed = true;
        }
        let recovery_stop = recovery_profit_lock_stop(
            position.side,
            position.entry_price,
            position.stop_price,
            snapshot.mark_price,
            max_adverse,
            cfg.recovery_lock_adverse_pct,
            cfg.recovery_lock_activation_pct,
            cfg.recovery_lock_pct,
        );
        let (improved_stop, protection_reason) = match (improved_stop, recovery_stop) {
            (Some(trailing), Some(recovery)) => {
                let trailing_is_tighter = if position.side > 0 {
                    trailing >= recovery
                } else {
                    trailing <= recovery
                };
                if trailing_is_tighter {
                    (Some(trailing), "trailing_take_profit")
                } else {
                    (Some(recovery), "recovery_profit_lock")
                }
            }
            (Some(trailing), None) => (Some(trailing), "trailing_take_profit"),
            (None, Some(recovery)) => (Some(recovery), "recovery_profit_lock"),
            (None, None) => (None, "initial_stop"),
        };
        let improved_stop = improved_stop.map(|stop| {
            if position.side > 0 {
                stop.max(position.stop_price)
            } else {
                stop.min(position.stop_price)
            }
        });
        if let Some(improved_stop) = improved_stop {
            let filters = client.symbol_filters(&symbol).await?;
            let side = if position.side > 0 { "SELL" } else { "BUY" };
            // 先挂新保护，确认成功后才撤旧保护；任何下单失败都保留原止损。
            let new_order_id = client
                .place_order(
                    &symbol,
                    side,
                    "STOP_MARKET",
                    managed_qty,
                    None,
                    Some(improved_stop),
                    true,
                    &filters,
                )
                .await?;
            let old_order_id = position.protection_order_id;
            if let Some(old_order_id) = old_order_id {
                if let Err(error) = client.cancel_algo_order(&symbol, old_order_id).await {
                    warn!(symbol=%symbol, old_order_id, error=%error, "新保护已生效，但旧保护撤销失败");
                    append_event(
                        event_path,
                        json!({"ts_ms":now_ms,"event":"old_protection_cancel_failed","symbol":symbol,"old_order_id":old_order_id,"new_order_id":new_order_id,"reason":error.to_string()}),
                    )?;
                }
            }
            let old_stop = position.stop_price;
            position.stop_price = improved_stop;
            position.protection_order_id = Some(new_order_id);
            position.protection_reason = protection_reason.to_owned();
            append_event(
                event_path,
                json!({"ts_ms":now_ms,"event":"protection_updated","symbol":symbol,"side":position.side,"mark_price":snapshot.mark_price,"extreme":position.extreme,"excursion":excursion,"adverse_extreme":adverse,"max_adverse_excursion":max_adverse,"current_return":position.side as f64*(snapshot.mark_price/position.entry_price-1.0),"old_stop":old_stop,"new_stop":improved_stop,"old_order_id":old_order_id,"new_order_id":new_order_id,"reason":protection_reason}),
            )?;
            changed = true;
        }
        if position.extreme != previous_extreme || position.adverse_extreme != previous_adverse {
            changed = true;
        }
        state.positions.insert(symbol, position);
    }
    Ok(changed)
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
    mut daily_risk_reset: Option<tokio::sync::watch::Receiver<u64>>,
    mut daily_entry_bonus: Option<tokio::sync::watch::Receiver<u64>>,
) -> Result<()> {
    let strategy_text = std::fs::read_to_string(&args.strategy)
        .with_context(|| format!("读取策略失败: {}", args.strategy))?;
    let strategy_file = toml::from_str::<StrategyFile>(&strategy_text)?;
    let cfg = strategy_file.altcoin_impulse;
    let observer_cfg = strategy_file.altcoin_reversal_observer;
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
        (1..=cfg.exchange_leverage).contains(&cfg.min_exchange_leverage),
        "min_exchange_leverage 必须在 1..=exchange_leverage"
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
        cfg.risk_execution_buffer_pct >= 0.001 && cfg.risk_execution_buffer_pct <= 0.02,
        "止损执行缓冲必须在 0.1%..=2%"
    );
    anyhow::ensure!(
        cfg.trail_activation_pct >= 0.01
            && cfg.trail_activation_pct <= 0.10
            && cfg.trail_pct >= 0.005
            && cfg.trail_pct < cfg.trail_activation_pct,
        "跟踪止盈要求激活点 1%..=10%，跟踪距离 >=0.5% 且小于激活点"
    );
    anyhow::ensure!(
        cfg.partial_take_profit_fraction >= 0.25 && cfg.partial_take_profit_fraction <= 0.75,
        "第一段止盈比例必须在 25%..=75%"
    );
    anyhow::ensure!(
        cfg.recovery_lock_adverse_pct >= 0.02
            && cfg.recovery_lock_adverse_pct <= cfg.stop_pct
            && cfg.recovery_lock_activation_pct > cfg.recovery_lock_pct
            && cfg.recovery_lock_activation_pct < cfg.trail_activation_pct
            && cfg.recovery_lock_pct >= 0.0,
        "修复锁盈要求：不利波动 2%..=初始止损，且 0 <= 锁定收益 < 修复收益 < 跟踪激活点"
    );
    anyhow::ensure!(
        cfg.max_entry_slippage_pct > 0.0 && cfg.max_entry_slippage_pct <= 0.03,
        "最大入场滑点必须在 0%..=3%"
    );
    anyhow::ensure!(
        cfg.overextension_long_return_1h > cfg.min_return_1h
            && cfg.overextension_long_return_1h < cfg.max_return_1h,
        "过度延伸追多阈值必须位于 1h 启动区间内部"
    );
    anyhow::ensure!(
        cfg.overextension_long_return_4h > cfg.min_return_4h
            && cfg.overextension_long_return_4h < cfg.max_return_4h,
        "过度延伸追多阈值必须位于 4h 启动区间内部"
    );
    anyhow::ensure!(
        cfg.overextension_reentry_lookback_hours >= cfg.cooldown_hours
            && cfg.overextension_reentry_lookback_hours <= 72,
        "过度延伸重复入场回看必须不短于同币冷却且不超过 72 小时"
    );
    anyhow::ensure!(
        cfg.max_daily_entry_bonus <= cfg.max_daily_entries,
        "临时开仓加额不能超过基础每日开仓上限"
    );
    anyhow::ensure!(
        (15..=300).contains(&cfg.max_signal_age_seconds),
        "入场执行窗口必须在 15..=300 秒"
    );
    observer_cfg.validate()?;
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
    // 兼容升级前的状态文件：从现金、累计已实现盈亏和费用还原策略起始权益，
    // 并从既有 scan 日志回填日权益，部署升级后无需清空历史即可看到曲线。
    if state.initial_equity <= 0.0 || !state.initial_equity.is_finite() {
        state.initial_equity = state.cash - state.realized_pnl + state.fees;
    }
    if state.equity_curve.is_empty() {
        state.equity_curve = load_daily_equity(&event_path);
    }
    if state.daily_entry_bonus > cfg.max_daily_entry_bonus {
        let previous_bonus = state.daily_entry_bonus;
        state.daily_entry_bonus = cfg.max_daily_entry_bonus;
        append_event(
            &event_path,
            json!({
                "ts_ms":now_ms,
                "event":"daily_entry_bonus_clamped",
                "previous_bonus":previous_bonus,
                "new_bonus":state.daily_entry_bonus,
                "reason":"configured_safety_cap"
            }),
        )?;
        save_state(&state_path, &state)?;
    }
    if state.recent_trades.is_empty() {
        state.recent_trades = load_recent_trades(&event_path);
    }
    let mut phase_migrations = Vec::new();
    for position in state.positions.values_mut() {
        let historical_returns = state.recent_trades.iter().rev().find_map(|event| {
            (event["event"] == "entry"
                && event["symbol"] == position.symbol
                && event["ts_ms"].as_i64() == Some(position.entry_ms))
            .then(|| {
                Some((
                    event["signal"]["return_1h"].as_f64()?,
                    event["signal"]["return_4h"].as_f64()?,
                ))
            })
            .flatten()
        });
        let Some((return_1h, return_4h)) = historical_returns else {
            continue;
        };
        let migrated = entry_phase(
            position.side,
            return_1h,
            return_4h,
            cfg.overextension_long_return_1h,
            cfg.overextension_long_return_4h,
        );
        if position.entry_phase != migrated {
            position.entry_phase = migrated.to_owned();
            phase_migrations.push((position.symbol.clone(), return_1h, return_4h, migrated));
        }
    }
    if !phase_migrations.is_empty() {
        for (symbol, return_1h, return_4h, migrated) in phase_migrations {
            append_event(
                &event_path,
                json!({"ts_ms":now_ms,"event":"position_entry_phase_migrated","symbol":symbol,"return_1h":return_1h,"return_4h":return_4h,"entry_phase":migrated}),
            )?;
        }
        save_state(&state_path, &state)?;
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
    let mut handled_risk_reset = daily_risk_reset
        .as_ref()
        .map(|receiver| *receiver.borrow())
        .unwrap_or(0);
    let mut handled_entry_bonus = daily_entry_bonus
        .as_ref()
        .map(|receiver| *receiver.borrow())
        .unwrap_or(0);
    info!(
        mode = mode.as_str(),
        leverage = cfg.exchange_leverage,
        capital = initial_cash,
        reversal_observer = observer_cfg.enabled,
        "启动独立山寨币放量突破策略"
    );
    append_event(
        &event_path,
        json!({"ts_ms": now_ms, "event":"runner_start", "mode":mode.as_str(), "config":cfg, "reversal_observer_config":observer_cfg}),
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
        // 观察器虚拟持仓只复用公共 K 线，不计入生产策略动量池和持仓上限。
        for symbol in tracked_symbols(&state.reversal_observer) {
            if !shortlist.iter().any(|(item, _)| item == &symbol) {
                shortlist.push((symbol, f64::INFINITY));
            }
        }

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
        // 旧状态文件没有 adverse_extreme。升级时用入场后的闭合 K 线重建一次，
        // 避免正在持有的仓位因重启而丢失“曾经深跌”的价格路径记忆。
        let mut reconstructed = Vec::new();
        for position in state.positions.values_mut() {
            if position.adverse_extreme.is_some() {
                continue;
            }
            let adverse = bars_by_symbol
                .get(&position.symbol)
                .into_iter()
                .flatten()
                .filter(|bar| bar.open_ms >= position.entry_ms)
                .map(|bar| if position.side > 0 { bar.low } else { bar.high })
                .fold(position.entry_price, |value, price| {
                    if position.side > 0 {
                        value.min(price)
                    } else {
                        value.max(price)
                    }
                });
            position.adverse_extreme = Some(adverse);
            reconstructed.push((position.symbol.clone(), adverse));
        }
        if !reconstructed.is_empty() {
            for (symbol, adverse) in &reconstructed {
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":"position_path_reconstructed","symbol":symbol,"adverse_extreme":adverse,"source":"closed_15m_since_entry"}),
                )?;
            }
            save_state(&state_path, &state)?;
        }
        let mut prices: HashMap<String, f64> = bars_by_symbol
            .iter()
            .filter_map(|(s, b)| b.last().map(|x| (s.clone(), x.close)))
            .collect();
        // 已持仓标的用交易所实时 markPrice 覆盖 15m 已收盘价。这个价格不仅用于
        // 页面，也用于日损门槛和动态仓位，避免策略风险判断滞后最多 15 分钟。
        if let Some(client) = rest.as_ref() {
            for symbol in state.positions.keys() {
                match client.position_risk(symbol).await {
                    Ok(snapshot) if snapshot.position_amt.abs() > 1e-12 => {
                        prices.insert(symbol.clone(), snapshot.mark_price);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(symbol=%symbol, error=%error, "实时标记价失败，风险计算暂用已收盘 K 线")
                    }
                }
            }
        }
        let current_equity = equity(&state, &prices);
        let current_day = scan_ms / DAY_MS;
        if current_day != state.day {
            state.day = current_day;
            state.day_start_equity = current_equity;
            state.daily_entries = 0;
            state.daily_entry_bonus = 0;
            state.daily_risk_resets = 0;
            state.overextension_long_blocked = false;
            state.overextension_long_losses = 0;
            state.last_overextension_loss_ms = None;
        }
        if take_control_commands(&mut daily_risk_reset, &mut handled_risk_reset) > 0 {
            apply_daily_risk_reset(&mut state, current_equity, scan_ms, &event_path)?;
            save_state(&state_path, &state)?;
        }
        let bonus_commands =
            take_control_commands(&mut daily_entry_bonus, &mut handled_entry_bonus);
        for _ in 0..bonus_commands {
            apply_daily_entry_bonus(
                &mut state,
                cfg.max_daily_entries,
                cfg.max_daily_entry_bonus,
                scan_ms,
                &event_path,
            )?;
        }
        if bonus_commands > 0 {
            save_state(&state_path, &state)?;
        }

        // 先管理已有仓位。模拟盘/实盘走独立实时循环；dry 才使用闭合 K 线撮合。
        if let Some(client) = rest.as_ref() {
            match manage_live_positions(&mut state, client, &cfg, scan_ms, &event_path).await {
                Ok(true) => save_state(&state_path, &state)?,
                Ok(false) => {}
                Err(error) => {
                    warn!(error=%error, "实时持仓管理短暂失败；交易所原保护单保持有效");
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"position_management_error","reason":error.to_string(),"original_protection_retained":true}),
                    )?;
                    // 部分止盈可能已经在交易所成交；即使随后查询或保护单替换失败，
                    // 也必须把已发生的本地数量与盈亏变更立即持久化。
                    save_state(&state_path, &state)?;
                }
            }
        } else {
            let position_symbols: Vec<String> = state.positions.keys().cloned().collect();
            for symbol in position_symbols {
                let Some(mut position) = state.positions.get(&symbol).cloned() else {
                    continue;
                };
                let Some(bar) = bars_by_symbol.get(&symbol).and_then(|bars| bars.last()) else {
                    continue;
                };
                if bar.open_ms <= position.last_bar_ms {
                    continue;
                }
                position.last_bar_ms = bar.open_ms;
                position.extreme = if position.side > 0 {
                    position.extreme.max(bar.high)
                } else {
                    position.extreme.min(bar.low)
                };
                let adverse_price = if position.side > 0 { bar.low } else { bar.high };
                let adverse = update_adverse_extreme(
                    position.side,
                    position.adverse_extreme,
                    position.entry_price,
                    adverse_price,
                );
                position.adverse_extreme = Some(adverse);
                let excursion =
                    position.side as f64 * (position.extreme / position.entry_price - 1.0);
                if !position.partial_take_profit_done && excursion >= cfg.trail_activation_pct {
                    let exit = position.entry_price
                        * (1.0 + position.side as f64 * cfg.trail_activation_pct);
                    let qty = position.qty * cfg.partial_take_profit_fraction;
                    let fee = qty * exit * 0.0005;
                    let partial_pnl =
                        record_partial_exit(&mut state, &mut position, exit, qty, fee);
                    position.partial_take_profit_done = true;
                    position.last_partial_exit_ms = Some(scan_ms);
                    position.stop_price = position.entry_price;
                    position.protection_reason = "partial_take_profit_break_even".to_owned();
                    let event = json!({"ts_ms":scan_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"reason":"partial_take_profit","price":exit,"qty":qty,"remaining_qty":position.qty,"pnl":partial_pnl,"fee":fee,"new_stop":position.stop_price,"dry_fill":true});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                }
                if let Some(recovery_stop) = recovery_profit_lock_stop(
                    position.side,
                    position.entry_price,
                    position.stop_price,
                    bar.close,
                    adverse_excursion(position.side, position.entry_price, adverse),
                    cfg.recovery_lock_adverse_pct,
                    cfg.recovery_lock_activation_pct,
                    cfg.recovery_lock_pct,
                ) {
                    position.stop_price = recovery_stop;
                    position.protection_reason = "recovery_profit_lock".to_owned();
                }
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
                if stopped || timed {
                    let exit = if stopped {
                        position.stop_price
                    } else {
                        bar.close
                    };
                    let fee = position.qty * exit * 0.0005;
                    let pnl = record_exit(
                        &mut state,
                        &position,
                        scan_ms,
                        cfg.cooldown_hours,
                        exit,
                        position.qty,
                        fee,
                    );
                    let trade_pnl = position.realized_partial_pnl + pnl;
                    let reason = if timed {
                        "time"
                    } else {
                        match position.protection_reason.as_str() {
                            "trailing_take_profit" => "trailing_take_profit",
                            "recovery_profit_lock" => "recovery_profit_lock",
                            _ => "initial_stop",
                        }
                    };
                    let event = json!({"ts_ms":scan_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"reason":reason,"price":exit,"qty":position.qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"overextension_long_blocked":state.overextension_long_blocked});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                } else {
                    state.positions.insert(symbol, position);
                }
            }
        }

        // 影子观察器在生产策略之外独立推进。该模块没有 RestClient，无法触发订单；
        // 即使滚动门控通过，也只会在日志中标记 would-trade 状态。
        if update_reversal_observer(
            &mut state.reversal_observer,
            &observer_cfg,
            &http,
            &active,
            &spot_symbols,
            &tickers,
            &bars_by_symbol,
            scan_ms,
            &event_path,
        )
        .await?
        {
            save_state(&state_path, &state)?;
        }

        let mut candidates: Vec<Candidate> = bars_by_symbol
            .into_iter()
            .filter_map(|(s, bars)| evaluate(s, &bars, &cfg))
            .collect();
        // 历史验证假设在信号 K 线闭合后的下一根开盘成交。仓位刚释放时不得
        // 回头追一个已经运行数分钟的旧突破，否则线上执行口径会偏离回测，
        // 也容易在冲高末端接盘。过期信号保留在观测日志中，但不再具备下单资格。
        let max_signal_age_ms = cfg.max_signal_age_seconds as i64 * 1_000;
        for candidate in &mut candidates {
            if signal_age_ms(scan_ms, candidate.signal_ms) > max_signal_age_ms {
                candidate.blockers.push(format!(
                    "已超过 {} 秒入场执行窗口",
                    cfg.max_signal_age_seconds
                ));
            }
        }
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
        let overextension_long_open = state
            .positions
            .values()
            .any(|position| position.entry_phase == "overextended_long");
        for candidate in &mut candidates {
            if candidate.entry_phase != "overextended_long" {
                continue;
            }
            if state.overextension_long_blocked {
                candidate
                    .blockers
                    .push("当日过度延伸追多已发生亏损，同类入场熔断".into());
            }
            if overextension_long_open {
                candidate
                    .blockers
                    .push("已有一笔过度延伸追多票，禁止同类风险叠加".into());
            }
            if recently_exited_symbol(
                &state.recent_trades,
                &candidate.symbol,
                scan_ms,
                cfg.overextension_reentry_lookback_hours,
            ) {
                candidate.blockers.push(format!(
                    "同币 {} 小时内已平仓，禁止在过度延伸区二次追涨",
                    cfg.overextension_reentry_lookback_hours
                ));
            }
        }
        // 仓位管理可能刚刚产生止损/时间退出，必须用更新后的现金重新计算，
        // 避免同一扫描周期在触发日损门槛后又开出新仓。
        let managed_equity = equity(&state, &prices);
        let daily_loss_blocked =
            managed_equity < state.day_start_equity * (1.0 - cfg.daily_loss_limit);
        let mut eligible_count = 0usize;
        let effective_daily_limit = cfg
            .max_daily_entries
            .saturating_add(state.daily_entry_bonus);
        let mut overextension_slot_taken = overextension_long_open;
        for candidate in candidates.iter().filter(|c| c.eligible()) {
            eligible_count += 1;
            if state.positions.len() >= cfg.max_positions
                || state.daily_entries >= effective_daily_limit
                || daily_loss_blocked
            {
                break;
            }
            if state.positions.contains_key(&candidate.symbol)
                || (candidate.entry_phase == "overextended_long" && overextension_slot_taken)
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
            let risk_distance = cfg.stop_pct + cfg.risk_execution_buffer_pct;
            let notional = (managed_equity * cfg.risk_per_trade / risk_distance)
                .min((managed_equity * cfg.max_gross_multiple - gross).max(0.0));
            if notional < 20.0 {
                continue;
            }
            let mut entry = candidate.price;
            let mut qty = notional / entry;
            let mut fee = notional * 0.0005;
            let mut protection_order_id = None;
            let mut actual_leverage = cfg.exchange_leverage;
            if let Some(client) = rest.as_ref() {
                actual_leverage = match client
                    .set_leverage_up_to(
                        &candidate.symbol,
                        cfg.exchange_leverage,
                        cfg.min_exchange_leverage,
                    )
                    .await
                {
                    Ok(leverage) => leverage,
                    Err(error) => {
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
                };
                if actual_leverage != cfg.exchange_leverage {
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"leverage_adjusted","symbol":candidate.symbol,"requested_leverage":cfg.exchange_leverage,"actual_leverage":actual_leverage}),
                    )?;
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
                            "lot_max_qty":filters.max_qty,"market_max_qty":filters.market_max_qty,
                            "multiplier_up":filters.multiplier_up,"multiplier_down":filters.multiplier_down},
                        "requested_leverage":cfg.exchange_leverage,"actual_leverage":actual_leverage,
                        "signal":candidate
                    }),
                )?;
                let execution_mark = match client.mark_price(&candidate.symbol).await {
                    Ok(price) => price,
                    Err(error) => {
                        state.note_execution_issue(
                            scan_ms,
                            &candidate.symbol,
                            "execution_mark_price",
                            error.to_string(),
                        );
                        append_event(
                            &event_path,
                            json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"execution_mark_price","symbol":candidate.symbol,"reason":error.to_string()}),
                        )?;
                        continue;
                    }
                };
                let (raw_guard_price, guard_price) = clamp_entry_guard_price(
                    candidate.side,
                    candidate.price,
                    cfg.max_entry_slippage_pct,
                    execution_mark,
                    filters.multiplier_up,
                    filters.multiplier_down,
                    filters.tick_size,
                );
                if (guard_price - raw_guard_price).abs() > filters.tick_size * 0.5 {
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_price_guard_clamped","symbol":candidate.symbol,"side":side,"signal_price":candidate.price,"execution_mark_price":execution_mark,"raw_guard_price":raw_guard_price,"clamped_guard_price":guard_price,"multiplier_up":filters.multiplier_up,"multiplier_down":filters.multiplier_down}),
                    )?;
                }
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
                    adverse_extreme: Some(entry),
                    stop_price: stop,
                    last_bar_ms: candidate.signal_ms,
                    protection_order_id,
                    protection_reason: default_protection_reason(),
                    exchange_leverage: Some(actual_leverage),
                    entry_phase: candidate.entry_phase.clone(),
                    partial_take_profit_done: false,
                    last_partial_exit_ms: None,
                    realized_partial_pnl: 0.0,
                },
            );
            if candidate.entry_phase == "overextended_long" {
                overextension_slot_taken = true;
            }
            let event = json!({"ts_ms":scan_ms,"event":"entry","symbol":candidate.symbol,"side":candidate.side,"entry_phase":candidate.entry_phase,"signal_age_ms":signal_age_ms(scan_ms,candidate.signal_ms),"max_signal_age_ms":max_signal_age_ms,"signal":candidate,"entry_price":entry,"qty":qty,"notional":qty*entry,"requested_leverage":cfg.exchange_leverage,"actual_leverage":actual_leverage,"margin_estimate":qty*entry/actual_leverage as f64,"risk_usd":qty*entry*risk_distance,"price_stop_risk_usd":qty*entry*cfg.stop_pct,"risk_execution_buffer_pct":cfg.risk_execution_buffer_pct,"fee":fee});
            append_event(&event_path, event.clone())?;
            state.record_trade(event);
        }
        save_state(&state_path, &state)?;
        let valuation_ms = chrono::Utc::now().timestamp_millis();
        let position_status =
            build_position_status(&state, &prices, rest.as_ref(), valuation_ms).await;
        let current_equity = state.cash + positions_unrealized(&position_status);
        record_daily_equity(&mut state.equity_curve, valuation_ms, current_equity);
        save_state(&state_path, &state)?;
        let reversal_observer = observer_status(&state.reversal_observer, &observer_cfg, scan_ms);
        append_event(
            &event_path,
            json!({
                "ts_ms":scan_ms, "event":"scan", "universe_count":spot_symbols.intersection(&active).count(),
                "shortlist_count":shortlist_count, "eligible_count":eligible_count,
                "equity":current_equity, "daily_entries":state.daily_entries,
                "base_max_daily_entries":cfg.max_daily_entries,
                "daily_entry_bonus":state.daily_entry_bonus,
                "max_daily_entry_bonus":cfg.max_daily_entry_bonus,
                "max_daily_entries":effective_daily_limit,
                "max_signal_age_seconds":cfg.max_signal_age_seconds,
                "risk_execution_buffer_pct":cfg.risk_execution_buffer_pct,
                "partial_take_profit_fraction":cfg.partial_take_profit_fraction,
                "recovery_lock_adverse_pct":cfg.recovery_lock_adverse_pct,
                "recovery_lock_activation_pct":cfg.recovery_lock_activation_pct,
                "recovery_lock_pct":cfg.recovery_lock_pct,
                "overextension_long_return_1h":cfg.overextension_long_return_1h,
                "overextension_long_return_4h":cfg.overextension_long_return_4h,
                "overextension_reentry_lookback_hours":cfg.overextension_reentry_lookback_hours,
                "overextension_long_blocked":state.overextension_long_blocked,
                "overextension_long_losses":state.overextension_long_losses,
                "last_overextension_loss_ms":state.last_overextension_loss_ms,
                "daily_loss_blocked":daily_loss_blocked,
                "positions":position_status,
                "candidates":candidates.iter().take(10).collect::<Vec<_>>(),
                "reversal_observer":reversal_observer
            }),
        )?;
        let mut status_payload = json!({
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
                "stop_pct":cfg.stop_pct, "notional_per_trade_estimate":current_equity*cfg.risk_per_trade/(cfg.stop_pct+cfg.risk_execution_buffer_pct),
                "margin_per_trade_estimate":current_equity*cfg.risk_per_trade/(cfg.stop_pct+cfg.risk_execution_buffer_pct)/cfg.exchange_leverage as f64,
                "worst_loss_per_trade_estimate":current_equity*cfg.risk_per_trade,
                "daily_entries":state.daily_entries, "max_daily_entries":effective_daily_limit,
                "daily_loss_blocked":daily_loss_blocked, "positions":position_status,
                "last_valuation_ms":valuation_ms,
                "valuation_source": if rest.is_some() {"binance_position_risk"} else {"closed_15m_fallback"},
                "total_entries":state.total_entries, "total_exits":state.total_exits, "wins":state.wins,
                "rejected_entries":state.rejected_entries, "last_execution_issue":state.last_execution_issue,
                "recent_trades":state.recent_trades,
                "realized_pnl":state.realized_pnl, "fees":state.fees,
                "candidates":candidates.into_iter().take(10).collect::<Vec<_>>(),
                "reversal_observer":reversal_observer,
                "journal":event_path,
            }
        });
        status_payload["altcoin_impulse"]["daily_risk_baseline_equity"] =
            json!(state.day_start_equity);
        status_payload["altcoin_impulse"]["base_max_daily_entries"] = json!(cfg.max_daily_entries);
        status_payload["altcoin_impulse"]["max_daily_entry_bonus"] =
            json!(cfg.max_daily_entry_bonus);
        status_payload["altcoin_impulse"]["max_signal_age_seconds"] =
            json!(cfg.max_signal_age_seconds);
        status_payload["altcoin_impulse"]["cooldown_hours"] = json!(cfg.cooldown_hours);
        status_payload["altcoin_impulse"]["risk_execution_buffer_pct"] =
            json!(cfg.risk_execution_buffer_pct);
        status_payload["altcoin_impulse"]["partial_take_profit_fraction"] =
            json!(cfg.partial_take_profit_fraction);
        status_payload["altcoin_impulse"]["recovery_lock_adverse_pct"] =
            json!(cfg.recovery_lock_adverse_pct);
        status_payload["altcoin_impulse"]["recovery_lock_activation_pct"] =
            json!(cfg.recovery_lock_activation_pct);
        status_payload["altcoin_impulse"]["recovery_lock_pct"] = json!(cfg.recovery_lock_pct);
        status_payload["altcoin_impulse"]["overextension_long_return_1h"] =
            json!(cfg.overextension_long_return_1h);
        status_payload["altcoin_impulse"]["overextension_long_return_4h"] =
            json!(cfg.overextension_long_return_4h);
        status_payload["altcoin_impulse"]["overextension_reentry_lookback_hours"] =
            json!(cfg.overextension_reentry_lookback_hours);
        status_payload["altcoin_impulse"]["overextension_long_blocked"] =
            json!(state.overextension_long_blocked);
        status_payload["altcoin_impulse"]["overextension_long_losses"] =
            json!(state.overextension_long_losses);
        status_payload["altcoin_impulse"]["last_overextension_loss_ms"] =
            json!(state.last_overextension_loss_ms);
        status_payload["altcoin_impulse"]["daily_entry_bonus"] = json!(state.daily_entry_bonus);
        status_payload["altcoin_impulse"]["daily_risk_resets"] = json!(state.daily_risk_resets);
        status_payload["altcoin_impulse"]["last_daily_risk_reset_ms"] =
            json!(state.last_daily_risk_reset_ms);
        status_payload["altcoin_impulse"]["initial_equity"] = json!(state.initial_equity);
        status_payload["altcoin_impulse"]["equity_curve"] = json!(state.equity_curve);
        if let Some(tx) = &status_tx {
            let _ = tx.send(status_payload.clone());
        }

        // 全市场信号仍按 poll_seconds 扫描；持仓估值单独每 5 秒刷新，避免为了更新
        // 浮盈亏而高频重拉数百个交易对的 K 线。
        let next_scan = tokio::time::Instant::now() + Duration::from_secs(cfg.poll_seconds.max(15));
        loop {
            let now = tokio::time::Instant::now();
            if now >= next_scan {
                break;
            }
            let wait = (next_scan - now).min(Duration::from_secs(5));
            tokio::select! {
                _ = tokio::time::sleep(wait) => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
            }
            if *shutdown.borrow() {
                break;
            }
            if let Some(client) = rest.as_ref() {
                let refresh_ms = chrono::Utc::now().timestamp_millis();
                match manage_live_positions(&mut state, client, &cfg, refresh_ms, &event_path).await
                {
                    Ok(true) => save_state(&state_path, &state)?,
                    Ok(false) => {}
                    Err(error) => {
                        warn!(error=%error, "5 秒持仓管理短暂失败；交易所原保护单保持有效");
                        append_event(
                            &event_path,
                            json!({"ts_ms":refresh_ms,"event":"position_management_error","reason":error.to_string(),"original_protection_retained":true}),
                        )?;
                    }
                }
                let refreshed =
                    build_position_status(&state, &prices, Some(client), refresh_ms).await;
                let refreshed_equity = state.cash + positions_unrealized(&refreshed);
                record_daily_equity(&mut state.equity_curve, refresh_ms, refreshed_equity);
                if take_control_commands(&mut daily_risk_reset, &mut handled_risk_reset) > 0 {
                    apply_daily_risk_reset(&mut state, refreshed_equity, refresh_ms, &event_path)?;
                    save_state(&state_path, &state)?;
                    status_payload["altcoin_impulse"]["daily_loss_blocked"] = json!(false);
                    status_payload["altcoin_impulse"]["daily_risk_baseline_equity"] =
                        json!(state.day_start_equity);
                    status_payload["altcoin_impulse"]["daily_risk_resets"] =
                        json!(state.daily_risk_resets);
                    status_payload["altcoin_impulse"]["last_daily_risk_reset_ms"] =
                        json!(state.last_daily_risk_reset_ms);
                    status_payload["altcoin_impulse"]["stage"] = json!(if eligible_count > 0 {
                        "execution"
                    } else if shortlist_count > 0 {
                        "confirmation"
                    } else {
                        "scan"
                    });
                }
                let bonus_commands =
                    take_control_commands(&mut daily_entry_bonus, &mut handled_entry_bonus);
                for _ in 0..bonus_commands {
                    apply_daily_entry_bonus(
                        &mut state,
                        cfg.max_daily_entries,
                        cfg.max_daily_entry_bonus,
                        refresh_ms,
                        &event_path,
                    )?;
                }
                if bonus_commands > 0 {
                    save_state(&state_path, &state)?;
                    status_payload["altcoin_impulse"]["max_daily_entries"] = json!(cfg
                        .max_daily_entries
                        .saturating_add(state.daily_entry_bonus));
                    status_payload["altcoin_impulse"]["daily_entry_bonus"] =
                        json!(state.daily_entry_bonus);
                }
                status_payload["uptime_s"] = json!((refresh_ms - started_ms) / 1000);
                status_payload["equity"] = json!(refreshed_equity);
                status_payload["altcoin_impulse"]["equity_curve"] = json!(state.equity_curve);
                status_payload["altcoin_impulse"]["positions"] = json!(refreshed);
                status_payload["altcoin_impulse"]["last_valuation_ms"] = json!(refresh_ms);
                status_payload["altcoin_impulse"]["overextension_long_blocked"] =
                    json!(state.overextension_long_blocked);
                status_payload["altcoin_impulse"]["overextension_long_losses"] =
                    json!(state.overextension_long_losses);
                status_payload["altcoin_impulse"]["last_overextension_loss_ms"] =
                    json!(state.last_overextension_loss_ms);
                if let Some(tx) = &status_tx {
                    let _ = tx.send(status_payload.clone());
                }
            }
        }
        if *shutdown.borrow() {
            break;
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
    use super::{
        adverse_excursion, apply_daily_entry_bonus, apply_daily_risk_reset,
        clamp_entry_guard_price, detected_exit_reason, entry_phase, realtime_trailing_stop,
        recently_exited_symbol, record_daily_equity, record_exit, record_partial_exit,
        recovery_profit_lock_stop, signal_age_ms, update_adverse_extreme, PersistedState, Position,
    };

    #[test]
    fn exchange_leverage_does_not_change_stop_risk() {
        let equity: f64 = 1_000.0;
        let risk: f64 = 0.06;
        let stop: f64 = 0.05;
        let execution_buffer: f64 = 0.006;
        let leverage: f64 = 10.0;
        let notional = equity * risk / (stop + execution_buffer);
        assert!((notional - 1_071.4285714285713).abs() < 1e-10);
        assert!((notional / leverage - 107.14285714285714).abs() < 1e-10);
        assert!((notional * (stop + execution_buffer) - 60.0).abs() < 1e-10);
    }

    #[test]
    fn partial_exit_realizes_half_and_preserves_final_trade_accounting() {
        let now_ms = 1_800_000_000_000i64;
        let mut state = PersistedState::new(1_000.0, now_ms);
        let mut position = Position {
            symbol: "TESTUSDT".into(),
            side: 1,
            qty: 100.0,
            entry_ms: now_ms - 1_000,
            entry_price: 10.0,
            entry_fee: 0.5,
            initial_notional: 1_000.0,
            extreme: 10.2,
            adverse_extreme: Some(10.0),
            stop_price: 9.5,
            last_bar_ms: now_ms,
            protection_order_id: None,
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "standard_impulse".into(),
            partial_take_profit_done: false,
            last_partial_exit_ms: None,
            realized_partial_pnl: 0.0,
        };
        // 50 个单位在 +2% 兑现：毛利 10，扣一半入场费 0.25 和退出费 0.255。
        let partial = record_partial_exit(&mut state, &mut position, 10.2, 50.0, 0.255);
        assert!((partial - 9.495).abs() < 1e-10);
        assert!((position.qty - 50.0).abs() < 1e-10);
        assert!((position.entry_fee - 0.25).abs() < 1e-10);
        assert!((state.realized_pnl - 9.495).abs() < 1e-10);

        let final_leg = record_exit(&mut state, &position, now_ms, 4, 10.1, 50.0, 0.2525);
        assert!((final_leg - 4.4975).abs() < 1e-10);
        assert!((position.realized_partial_pnl + final_leg - 13.9925).abs() < 1e-10);
        assert_eq!(state.total_exits, 1);
        assert_eq!(state.wins, 1);
        assert!(state.positions.is_empty());
    }

    #[test]
    fn daily_equity_keeps_the_latest_point_per_utc_day() {
        let mut curve = Vec::new();
        record_daily_equity(&mut curve, 1_000, 1_000.0);
        record_daily_equity(&mut curve, 2_000, 1_025.0);
        record_daily_equity(&mut curve, super::DAY_MS + 1_000, 980.0);
        assert_eq!(curve.len(), 2);
        assert_eq!(curve[0].ts_ms, 2_000);
        assert_eq!(curve[0].equity, 1_025.0);
        assert_eq!(curve[1].equity, 980.0);
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
            adverse_extreme: Some(1.0),
            stop_price: 0.95,
            last_bar_ms: 1,
            protection_order_id: Some(42),
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "standard_impulse".into(),
            partial_take_profit_done: false,
            last_partial_exit_ms: None,
            realized_partial_pnl: 0.0,
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
        position.stop_price = 1.0025;
        position.protection_reason = "recovery_profit_lock".into();
        assert_eq!(
            detected_exit_reason(&position, 1.002, true),
            "recovery_profit_lock"
        );
        assert_eq!(detected_exit_reason(&position, 1.10, false), "unknown");
    }

    #[test]
    fn realtime_trailing_activates_and_only_tightens() {
        let (extreme, stop) = realtime_trailing_stop(1, 100.0, 100.0, 95.0, 104.9, 0.05, 0.03);
        assert_eq!(extreme, 104.9);
        assert_eq!(stop, None);

        let (extreme, stop) = realtime_trailing_stop(1, 100.0, 104.9, 95.0, 105.0, 0.05, 0.03);
        assert_eq!(extreme, 105.0);
        assert!((stop.unwrap() - 101.85).abs() < 1e-10);

        let (_, stop) = realtime_trailing_stop(1, 100.0, 105.0, 101.85, 104.0, 0.05, 0.03);
        assert_eq!(stop, None, "回落时不得放宽已经上移的保护价");
    }

    #[test]
    fn realtime_trailing_is_symmetric_for_shorts() {
        let (extreme, stop) = realtime_trailing_stop(-1, 100.0, 100.0, 105.0, 95.0, 0.05, 0.03);
        assert_eq!(extreme, 95.0);
        assert!((stop.unwrap() - 97.85).abs() < 1e-10);
    }

    #[test]
    fn recovery_lock_requires_both_adverse_path_and_recovery() {
        let adverse = update_adverse_extreme(1, Some(100.0), 100.0, 96.5);
        assert!((adverse_excursion(1, 100.0, adverse) - 0.035).abs() < 1e-10);
        assert_eq!(
            recovery_profit_lock_stop(1, 100.0, 95.0, 100.9, 0.035, 0.03, 0.01, 0.0025),
            None
        );
        let long =
            recovery_profit_lock_stop(1, 100.0, 95.0, 101.0, 0.035, 0.03, 0.01, 0.0025).unwrap();
        assert!((long - 100.25).abs() < 1e-10);
        let short =
            recovery_profit_lock_stop(-1, 100.0, 105.0, 99.0, 0.035, 0.03, 0.01, 0.0025).unwrap();
        assert!((short - 99.75).abs() < 1e-10);
    }

    #[test]
    fn manual_daily_reset_preserves_entries_and_rebases_equity() {
        let now_ms = 1_800_000_000_000i64;
        let mut state = PersistedState::new(1_000.0, now_ms);
        state.day_start_equity = 1_200.0;
        state.daily_entries = 4;
        state.overextension_long_blocked = true;
        let path = std::env::temp_dir().join(format!(
            "greed-daily-risk-reset-{}-{}.jsonl",
            std::process::id(),
            now_ms
        ));
        apply_daily_risk_reset(&mut state, 950.0, now_ms, path.to_str().unwrap()).unwrap();
        assert_eq!(state.day_start_equity, 950.0);
        assert_eq!(state.daily_entries, 4);
        assert_eq!(state.daily_risk_resets, 1);
        assert!(
            state.overextension_long_blocked,
            "手动总风控重置不得清除同类失败熔断"
        );
        let log = std::fs::read_to_string(&path).unwrap();
        assert!(log.contains("daily_risk_reset"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn losing_overextended_long_latches_only_its_entry_style() {
        assert_eq!(
            entry_phase(1, 0.1199, 0.1499, 0.12, 0.15),
            "standard_impulse"
        );
        assert_eq!(entry_phase(1, 0.12, 0.10, 0.12, 0.15), "overextended_long");
        assert_eq!(entry_phase(1, 0.05, 0.15, 0.12, 0.15), "overextended_long");
        assert_eq!(
            entry_phase(-1, -0.30, -0.50, 0.12, 0.15),
            "standard_impulse"
        );
        let now_ms = 1_800_000_000_000i64;
        let mut state = PersistedState::new(1_000.0, now_ms);
        let position = Position {
            symbol: "PUMPUSDT".into(),
            side: 1,
            qty: 100.0,
            entry_ms: now_ms - 1_000,
            entry_price: 1.0,
            entry_fee: 0.1,
            initial_notional: 100.0,
            extreme: 1.0,
            adverse_extreme: Some(0.95),
            stop_price: 0.95,
            last_bar_ms: now_ms,
            protection_order_id: None,
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "overextended_long".into(),
            partial_take_profit_done: false,
            last_partial_exit_ms: None,
            realized_partial_pnl: 0.0,
        };
        let pnl = record_exit(&mut state, &position, now_ms, 8, 0.95, 100.0, 0.0);
        assert!(pnl < 0.0);
        assert!(state.overextension_long_blocked);
        assert_eq!(state.overextension_long_losses, 1);
        assert_eq!(state.last_overextension_loss_ms, Some(now_ms));
    }

    #[test]
    fn overextended_same_symbol_reentry_is_detected_inside_lookback() {
        let now_ms = 1_800_000_000_000i64;
        let trades = vec![serde_json::json!({
            "ts_ms": now_ms - 5 * 3_600_000,
            "event": "exit_detected",
            "symbol": "HOLOUSDT"
        })];
        assert!(recently_exited_symbol(&trades, "HOLOUSDT", now_ms, 24));
        assert!(!recently_exited_symbol(&trades, "OTHERUSDT", now_ms, 24));
        assert!(!recently_exited_symbol(&trades, "HOLOUSDT", now_ms, 4));
    }

    #[test]
    fn temporary_daily_entry_bonus_increases_by_two_and_caps_at_configured_limit() {
        let now_ms = 1_800_000_000_000i64;
        let mut state = PersistedState::new(1_000.0, now_ms);
        let path = std::env::temp_dir().join(format!(
            "greed-daily-entry-bonus-{}-{}.jsonl",
            std::process::id(),
            now_ms
        ));
        for _ in 0..4 {
            apply_daily_entry_bonus(&mut state, 6, 4, now_ms, path.to_str().unwrap()).unwrap();
        }
        assert_eq!(state.daily_entry_bonus, 4);
        assert_eq!(6 + state.daily_entry_bonus, 10);
        let log = std::fs::read_to_string(&path).unwrap();
        assert!(log.contains("daily_entry_limit_increased"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn entry_guard_respects_demo_percent_price_band() {
        let (raw, clamped) =
            clamp_entry_guard_price(1, 0.02405, 0.015, 0.0226241, 1.05, 0.95, 0.00001);
        assert!(raw > 0.0244);
        assert!(clamped < 0.0237553, "必须低于交易所报告的最高限价");

        let (_, short_guard) = clamp_entry_guard_price(-1, 100.0, 0.015, 100.0, 1.05, 0.95, 0.1);
        assert!(short_guard > 95.0, "卖出限价必须高于交易所最低边界");
    }

    #[test]
    fn signal_age_never_goes_negative_and_has_an_inclusive_boundary() {
        assert_eq!(signal_age_ms(1_000, 1_001), 0);
        assert_eq!(signal_age_ms(121_000, 1_000), 120_000);
        assert!(signal_age_ms(121_001, 1_000) > 120_000);
    }

    #[test]
    fn deployed_altcoin_config_enables_observer_without_changing_live_strategy() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        assert!(strategy.altcoin_impulse.enabled);
        assert!(strategy.altcoin_reversal_observer.enabled);
        assert_eq!(strategy.altcoin_impulse.max_daily_entries, 6);
        assert_eq!(strategy.altcoin_impulse.max_daily_entry_bonus, 4);
        assert_eq!(strategy.altcoin_impulse.cooldown_hours, 4);
        assert_eq!(strategy.altcoin_impulse.max_signal_age_seconds, 120);
        assert_eq!(strategy.altcoin_impulse.recovery_lock_adverse_pct, 0.03);
        assert_eq!(strategy.altcoin_impulse.recovery_lock_activation_pct, 0.01);
        assert_eq!(strategy.altcoin_impulse.recovery_lock_pct, 0.0025);
        assert_eq!(strategy.altcoin_impulse.overextension_long_return_1h, 0.12);
        assert_eq!(strategy.altcoin_impulse.overextension_long_return_4h, 0.15);
        assert_eq!(
            strategy
                .altcoin_impulse
                .overextension_reentry_lookback_hours,
            24
        );
        assert_eq!(strategy.altcoin_impulse.risk_execution_buffer_pct, 0.006);
        assert_eq!(strategy.altcoin_reversal_observer.gross_multiple, 1.0);
        strategy.altcoin_reversal_observer.validate().unwrap();
    }
}
