//! 山寨币 7 日横截面反转的前向影子观察器。
//!
//! 该模块只使用 Binance 主网公共行情，永远不持有 RestClient，也没有下单函数。
//! 它每天形成一篮子虚拟交易并记录完整路径，用于判断历史回测发现的 alpha
//! 在未来样本中是否仍然存在；生产山寨币放量突破策略不受它影响。

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::warn;

use crate::altcoin_runner::{append_event, get_json, parse_bars, Bar};

const FUTURES_BASE: &str = "https://fapi.binance.com";
const BAR_MS: i64 = 15 * 60 * 1_000;
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
const MAJORS: [&str; 5] = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"];

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AltcoinReversalObserverConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_shadow_capital")]
    pub shadow_capital_usdt: f64,
    #[serde(default = "default_gross_multiple")]
    pub gross_multiple: f64,
    #[serde(default = "default_lookback_days")]
    pub lookback_days: usize,
    #[serde(default = "default_top_n")]
    pub top_n: usize,
    #[serde(default = "default_min_volume")]
    pub min_24h_volume_usd: f64,
    #[serde(default = "default_max_volume")]
    pub max_24h_volume_usd: f64,
    #[serde(default = "default_stop_pct")]
    pub stop_pct: f64,
    #[serde(default = "default_hold_hours")]
    pub hold_hours: u32,
    #[serde(default = "default_fee_bps")]
    pub fee_bps_per_side: f64,
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps_per_side: f64,
    #[serde(default = "default_gate_window")]
    pub gate_window: usize,
    #[serde(default = "default_gate_profit_factor")]
    pub gate_min_profit_factor: f64,
    #[serde(default)]
    pub gate_min_mean_return: f64,
    #[serde(default = "default_entry_window_minutes")]
    pub entry_window_minutes: u32,
}

impl Default for AltcoinReversalObserverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            shadow_capital_usdt: default_shadow_capital(),
            gross_multiple: default_gross_multiple(),
            lookback_days: default_lookback_days(),
            top_n: default_top_n(),
            min_24h_volume_usd: default_min_volume(),
            max_24h_volume_usd: default_max_volume(),
            stop_pct: default_stop_pct(),
            hold_hours: default_hold_hours(),
            fee_bps_per_side: default_fee_bps(),
            slippage_bps_per_side: default_slippage_bps(),
            gate_window: default_gate_window(),
            gate_min_profit_factor: default_gate_profit_factor(),
            gate_min_mean_return: 0.0,
            entry_window_minutes: default_entry_window_minutes(),
        }
    }
}

fn default_shadow_capital() -> f64 {
    1_000.0
}
fn default_gross_multiple() -> f64 {
    1.0
}
fn default_lookback_days() -> usize {
    7
}
fn default_top_n() -> usize {
    5
}
fn default_min_volume() -> f64 {
    10_000_000.0
}
fn default_max_volume() -> f64 {
    150_000_000.0
}
fn default_stop_pct() -> f64 {
    0.08
}
fn default_hold_hours() -> u32 {
    24
}
fn default_fee_bps() -> f64 {
    5.0
}
fn default_slippage_bps() -> f64 {
    5.0
}
fn default_gate_window() -> usize {
    20
}
fn default_gate_profit_factor() -> f64 {
    1.20
}
fn default_entry_window_minutes() -> u32 {
    5
}

impl AltcoinReversalObserverConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        anyhow::ensure!(self.shadow_capital_usdt > 0.0, "影子本金必须大于 0");
        anyhow::ensure!(
            self.gross_multiple > 0.0 && self.gross_multiple <= 3.0,
            "影子总名义仓位必须在 0x..=3x"
        );
        anyhow::ensure!(self.lookback_days == 7, "当前验证模型固定使用 7 日回看");
        anyhow::ensure!(self.top_n > 0 && self.top_n <= 10, "Top N 必须在 1..=10");
        anyhow::ensure!(
            self.min_24h_volume_usd > 0.0 && self.max_24h_volume_usd > self.min_24h_volume_usd,
            "影子策略成交额区间无效"
        );
        anyhow::ensure!(
            self.stop_pct > 0.0 && self.stop_pct <= 0.20,
            "影子止损必须在 0%..=20%"
        );
        anyhow::ensure!(self.hold_hours == 24, "当前验证模型固定持有 24 小时");
        anyhow::ensure!(self.gate_window >= 5, "门控样本窗口至少 5 篮子");
        anyhow::ensure!(
            self.entry_window_minutes >= 1 && self.entry_window_minutes <= 10,
            "影子入场窗口必须在 1..=10 分钟"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReversalCandidate {
    pub rank: usize,
    pub symbol: String,
    pub signal_ms: i64,
    pub signal_price: f64,
    pub return_7d: f64,
    pub volume_24h: f64,
    pub selected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowLeg {
    pub symbol: String,
    pub rank: usize,
    pub return_7d: f64,
    pub volume_24h: f64,
    pub signal_price: f64,
    pub entry_ms: i64,
    pub entry_price: f64,
    pub notional: f64,
    pub stop_price: f64,
    pub last_bar_ms: i64,
    pub latest_price: f64,
    pub mfe: f64,
    pub mae: f64,
    pub exit_ms: Option<i64>,
    pub exit_price: Option<f64>,
    pub exit_reason: Option<String>,
    pub gross_return: Option<f64>,
    pub modeled_net_return: Option<f64>,
    pub funding_return: Option<f64>,
    pub funding_complete: Option<bool>,
    #[serde(default)]
    pub exit_variants: Vec<ShadowExitVariant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowExitVariant {
    pub id: String,
    pub label: String,
    pub remaining_fraction: f64,
    pub realized_net_return: f64,
    pub extreme_price: f64,
    pub stop_price: f64,
    pub breakeven_armed: bool,
    pub trail_armed: bool,
    pub partial_taken: bool,
    pub partial_ms: Option<i64>,
    pub exit_ms: Option<i64>,
    pub exit_price: Option<f64>,
    pub exit_reason: Option<String>,
    pub modeled_net_return: Option<f64>,
    pub funding_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowVariantOutcome {
    pub id: String,
    pub label: String,
    pub basket_return: f64,
    pub pnl_usdt: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowBasket {
    pub signal_day: i64,
    pub signal_ms: i64,
    pub entry_ms: i64,
    pub start_equity: f64,
    pub gross_multiple: f64,
    pub gate_ready_at_entry: bool,
    pub gate_enabled_at_entry: bool,
    pub gate_mean_at_entry: f64,
    pub gate_profit_factor_at_entry: f64,
    pub legs: Vec<ShadowLeg>,
    #[serde(default)]
    pub exit_experiment_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowOutcome {
    pub signal_day: i64,
    pub signal_ms: i64,
    pub entry_ms: i64,
    pub exit_ms: i64,
    pub basket_return: f64,
    pub pnl_usdt: f64,
    pub equity_after: f64,
    pub stopped_legs: usize,
    pub funding_complete: bool,
    pub gate_enabled_at_entry: bool,
    pub symbols: Vec<String>,
    #[serde(default)]
    pub exit_variants: Vec<ShadowVariantOutcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReversalObserverState {
    #[serde(default)]
    pub shadow_equity: f64,
    #[serde(default)]
    pub last_evaluation_day: Option<i64>,
    #[serde(default)]
    pub last_evaluation_ms: Option<i64>,
    #[serde(default)]
    pub last_reason: String,
    #[serde(default)]
    pub universe_count: usize,
    #[serde(default)]
    pub latest_candidates: Vec<ReversalCandidate>,
    #[serde(default)]
    pub active_basket: Option<ShadowBasket>,
    #[serde(default)]
    pub outcomes: Vec<ShadowOutcome>,
}

impl Default for ReversalObserverState {
    fn default() -> Self {
        Self {
            shadow_equity: 0.0,
            last_evaluation_day: None,
            last_evaluation_ms: None,
            last_reason: "等待首次 UTC 00:15 前向评估".into(),
            universe_count: 0,
            latest_candidates: Vec::new(),
            active_basket: None,
            outcomes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct GateMetrics {
    samples: usize,
    ready: bool,
    enabled: bool,
    mean: f64,
    profit_factor: f64,
}

struct RankResult {
    candidates: Vec<ReversalCandidate>,
    requested: usize,
    successful: usize,
}

const EXIT_EXPERIMENT_VERSION: u32 = 1;

fn new_exit_variants(entry_price: f64, stop_pct: f64) -> Vec<ShadowExitVariant> {
    [
        ("baseline", "8% 止损 / 24h"),
        ("breakeven_5", "+5% 后保本"),
        ("trail_8", "+8% 后 4% 跟踪"),
        ("staged", "+5% 平 33% / +8% 跟踪"),
    ]
    .into_iter()
    .map(|(id, label)| ShadowExitVariant {
        id: id.into(),
        label: label.into(),
        remaining_fraction: 1.0,
        realized_net_return: 0.0,
        extreme_price: entry_price,
        stop_price: entry_price * (1.0 + stop_pct),
        breakeven_armed: false,
        trail_armed: false,
        partial_taken: false,
        partial_ms: None,
        exit_ms: None,
        exit_price: None,
        exit_reason: None,
        modeled_net_return: None,
        funding_complete: true,
    })
    .collect()
}

fn modeled_short_leg_return(
    entry_price: f64,
    raw_exit: f64,
    fee_bps: f64,
    slippage_bps: f64,
) -> f64 {
    let fee = fee_bps / 10_000.0;
    let slip = slippage_bps / 10_000.0;
    let modeled_entry = entry_price * (1.0 - slip);
    let modeled_exit = raw_exit * (1.0 + slip);
    1.0 - modeled_exit / modeled_entry - fee - fee * modeled_exit / modeled_entry
}

fn update_exit_variant(
    variant: &mut ShadowExitVariant,
    entry_price: f64,
    due_ms: i64,
    bar: &Bar,
    cfg: &AltcoinReversalObserverConfig,
) {
    if variant.exit_ms.is_some() {
        return;
    }
    variant.extreme_price = variant.extreme_price.min(bar.low);
    let favorable = 1.0 - variant.extreme_price / entry_price;

    if matches!(variant.id.as_str(), "breakeven_5" | "staged") && favorable >= 0.05 {
        variant.breakeven_armed = true;
        variant.stop_price = variant.stop_price.min(entry_price);
    }
    if variant.id == "staged" && favorable >= 0.05 && !variant.partial_taken {
        let fraction = 0.33;
        variant.realized_net_return += fraction
            * modeled_short_leg_return(
                entry_price,
                entry_price * 0.95,
                cfg.fee_bps_per_side,
                cfg.slippage_bps_per_side,
            );
        variant.remaining_fraction -= fraction;
        variant.partial_taken = true;
        variant.partial_ms = Some(bar.close_ms);
    }
    if matches!(variant.id.as_str(), "trail_8" | "staged") && favorable >= 0.08 {
        variant.trail_armed = true;
        variant.stop_price = variant.stop_price.min(variant.extreme_price * 1.04);
    }

    let (exit_price, reason, exit_ms) = if bar.open_ms >= due_ms {
        (bar.open, "time", bar.open_ms)
    } else if bar.high >= variant.stop_price {
        (
            if bar.open >= variant.stop_price {
                bar.open
            } else {
                variant.stop_price
            },
            if variant.trail_armed {
                "trailing"
            } else if variant.breakeven_armed {
                "breakeven"
            } else {
                "stop"
            },
            bar.close_ms,
        )
    } else {
        return;
    };
    let remaining = variant.remaining_fraction
        * modeled_short_leg_return(
            entry_price,
            exit_price,
            cfg.fee_bps_per_side,
            cfg.slippage_bps_per_side,
        );
    variant.exit_ms = Some(exit_ms);
    variant.exit_price = Some(exit_price);
    variant.exit_reason = Some(reason.into());
    variant.modeled_net_return = Some(variant.realized_net_return + remaining);
}

fn gate_metrics(state: &ReversalObserverState, cfg: &AltcoinReversalObserverConfig) -> GateMetrics {
    let values: Vec<f64> = state
        .outcomes
        .iter()
        .rev()
        .take(cfg.gate_window)
        .map(|item| item.basket_return)
        .collect();
    let samples = values.len();
    let mean = if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    };
    let profit = values
        .iter()
        .copied()
        .filter(|value| *value > 0.0)
        .sum::<f64>();
    let loss = -values
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .sum::<f64>();
    let profit_factor = if loss > 0.0 {
        profit / loss
    } else if profit > 0.0 {
        99.0
    } else {
        0.0
    };
    let ready = samples >= cfg.gate_window;
    GateMetrics {
        samples,
        ready,
        enabled: ready
            && mean >= cfg.gate_min_mean_return
            && profit_factor >= cfg.gate_min_profit_factor,
        mean,
        profit_factor,
    }
}

pub fn tracked_symbols(state: &ReversalObserverState) -> Vec<String> {
    state
        .active_basket
        .as_ref()
        .map(|basket| {
            basket
                .legs
                .iter()
                .filter(|leg| leg.exit_ms.is_none())
                .map(|leg| leg.symbol.clone())
                .collect()
        })
        .unwrap_or_default()
}

fn next_evaluation_ms(
    state: &ReversalObserverState,
    cfg: &AltcoinReversalObserverConfig,
    now_ms: i64,
) -> i64 {
    let day = now_ms / DAY_MS;
    let today = day * DAY_MS + BAR_MS;
    if state.last_evaluation_day == Some(day)
        || now_ms > today + cfg.entry_window_minutes as i64 * 60_000
    {
        (day + 1) * DAY_MS + BAR_MS
    } else {
        today
    }
}

pub fn observer_status(
    state: &ReversalObserverState,
    cfg: &AltcoinReversalObserverConfig,
    now_ms: i64,
) -> Value {
    let gate = gate_metrics(state, cfg);
    let experiment_results = [
        ("baseline", "8% 止损 / 24h"),
        ("breakeven_5", "+5% 后保本"),
        ("trail_8", "+8% 后 4% 跟踪"),
        ("staged", "+5% 平 33% / +8% 跟踪"),
    ]
    .into_iter()
    .map(|(id, label)| {
        let values = state
            .outcomes
            .iter()
            .filter_map(|outcome| {
                outcome
                    .exit_variants
                    .iter()
                    .find(|variant| variant.id == id)
                    .map(|variant| variant.basket_return)
            })
            .collect::<Vec<_>>();
        let mean = if values.is_empty() {
            0.0
        } else {
            values.iter().sum::<f64>() / values.len() as f64
        };
        let compounded_return = values
            .iter()
            .fold(1.0, |equity, value| equity * (1.0 + value))
            - 1.0;
        json!({"id":id,"label":label,"samples":values.len(),"mean_return":mean,"compounded_return":compounded_return})
    })
    .collect::<Vec<_>>();
    let stage = if state.active_basket.is_some() {
        "tracking"
    } else if state.last_evaluation_day == Some(now_ms / DAY_MS) {
        "waiting_next_day"
    } else {
        "waiting_evaluation"
    };
    json!({
        "enabled":cfg.enabled,
        "mode":"observation_only",
        "strategy_id":"R7_short_N5_mid_S8_W20_PF1.20",
        "stage":stage,
        "reason":state.last_reason,
        "last_evaluation_ms":state.last_evaluation_ms,
        "next_evaluation_ms":next_evaluation_ms(state, cfg, now_ms),
        "universe_count":state.universe_count,
        "candidate_count":state.universe_count,
        "candidates":state.latest_candidates,
        "active_basket":state.active_basket,
        "completed_baskets":state.outcomes.len(),
        "recent_outcomes":state.outcomes.iter().rev().take(10).collect::<Vec<_>>(),
        "exit_experiment":{"version":EXIT_EXPERIMENT_VERSION,"applies_from_next_basket":state.active_basket.as_ref().is_some_and(|basket| basket.exit_experiment_version==0),"results":experiment_results},
        "shadow_initial_equity":cfg.shadow_capital_usdt,
        "shadow_equity":if state.shadow_equity > 0.0 {state.shadow_equity} else {cfg.shadow_capital_usdt},
        "shadow_return":if state.shadow_equity > 0.0 {state.shadow_equity/cfg.shadow_capital_usdt-1.0} else {0.0},
        "gross_multiple":cfg.gross_multiple,
        "stop_pct":cfg.stop_pct,
        "hold_hours":cfg.hold_hours,
        "fee_bps_per_side":cfg.fee_bps_per_side,
        "slippage_bps_per_side":cfg.slippage_bps_per_side,
        "gate":{
            "samples":gate.samples,
            "window":cfg.gate_window,
            "ready":gate.ready,
            "enabled":gate.enabled,
            "rolling_mean":gate.mean,
            "profit_factor":gate.profit_factor,
            "min_mean":cfg.gate_min_mean_return,
            "min_profit_factor":cfg.gate_min_profit_factor
        }
    })
}

async fn fetch_daily_snapshot(
    http: reqwest::Client,
    symbol: String,
    signal_ms: i64,
    cfg: AltcoinReversalObserverConfig,
) -> Result<Option<ReversalCandidate>> {
    let limit = cfg.lookback_days * 96 + 2;
    let url = format!(
        "{FUTURES_BASE}/fapi/v1/klines?symbol={symbol}&interval=15m&limit={limit}&endTime={}",
        signal_ms - 1
    );
    let bars = parse_bars(get_json(&http, &url).await?, signal_ms + 1)?;
    let lookback = cfg.lookback_days * 96;
    if bars.len() <= lookback || bars.len() < 96 {
        return Ok(None);
    }
    let last = bars.len() - 1;
    if bars[last].open_ms + BAR_MS != signal_ms
        || bars[last].open_ms - bars[last - lookback].open_ms > (lookback as i64 + 1) * BAR_MS
    {
        return Ok(None);
    }
    let return_7d = bars[last].close / bars[last - lookback].close - 1.0;
    let volume_24h = bars[last + 1 - 96..=last]
        .iter()
        .map(|bar| bar.quote_volume)
        .sum::<f64>();
    if !(cfg.min_24h_volume_usd..cfg.max_24h_volume_usd).contains(&volume_24h)
        || return_7d.abs() > 1.50
    {
        return Ok(None);
    }
    Ok(Some(ReversalCandidate {
        rank: 0,
        symbol,
        signal_ms,
        signal_price: bars[last].close,
        return_7d,
        volume_24h,
        selected: false,
    }))
}

async fn rank_candidates(
    http: &reqwest::Client,
    active: &HashSet<String>,
    spot: &HashSet<String>,
    tickers: &Value,
    signal_ms: i64,
    cfg: &AltcoinReversalObserverConfig,
) -> RankResult {
    // ticker 只做宽松预筛，最终成交额仍由信号时点前 96 根 15m K 线精确计算。
    let symbols: Vec<String> = tickers
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|ticker| {
            let symbol = ticker["symbol"].as_str()?.to_owned();
            let volume = ticker["quoteVolume"].as_str()?.parse::<f64>().ok()?;
            (active.contains(&symbol)
                && spot.contains(&symbol)
                && !MAJORS.contains(&symbol.as_str())
                && volume >= cfg.min_24h_volume_usd * 0.5
                && volume < cfg.max_24h_volume_usd * 2.0)
                .then_some(symbol)
        })
        .collect();
    let requested = symbols.len();
    let mut successful = 0usize;
    let mut candidates = Vec::new();
    // 限制并发，避免每日横截面计算在 Binance 公共接口形成瞬时尖峰。
    for chunk in symbols.chunks(20) {
        let mut tasks = tokio::task::JoinSet::new();
        for symbol in chunk {
            tasks.spawn(fetch_daily_snapshot(
                http.clone(),
                symbol.clone(),
                signal_ms,
                cfg.clone(),
            ));
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(Some(candidate))) => {
                    successful += 1;
                    candidates.push(candidate);
                }
                Ok(Ok(None)) => successful += 1,
                Ok(Err(error)) => warn!(error=%error, "反转观察器日线截面拉取失败"),
                Err(error) => warn!(error=%error, "反转观察器候选任务失败"),
            }
        }
    }
    candidates.sort_by(|left, right| right.return_7d.total_cmp(&left.return_7d));
    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank = index + 1;
        candidate.selected = index < cfg.top_n && candidate.return_7d > 0.0;
    }
    RankResult {
        candidates,
        requested,
        successful,
    }
}

async fn mark_price(http: reqwest::Client, symbol: String) -> Result<(String, f64)> {
    let url = format!("{FUTURES_BASE}/fapi/v1/premiumIndex?symbol={symbol}");
    let value = get_json(&http, &url).await?;
    let mark = value["markPrice"]
        .as_str()
        .context("premiumIndex 缺 markPrice")?
        .parse::<f64>()?;
    Ok((symbol, mark))
}

async fn funding_return_for_period(
    http: &reqwest::Client,
    symbol: &str,
    entry_ms: i64,
    exit_ms: i64,
) -> Result<f64> {
    let url = format!(
        "{FUTURES_BASE}/fapi/v1/fundingRate?symbol={}&startTime={}&endTime={}&limit=100",
        symbol, entry_ms, exit_ms
    );
    let rows = get_json(http, &url).await?;
    let sum = rows
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row["fundingRate"].as_str()?.parse::<f64>().ok())
        .sum::<f64>();
    // 正资金费：多头付、空头收；本策略固定做空。
    Ok(sum)
}

async fn funding_return(http: &reqwest::Client, leg: &ShadowLeg, exit_ms: i64) -> Result<f64> {
    funding_return_for_period(http, &leg.symbol, leg.entry_ms, exit_ms).await
}

async fn finish_leg(
    http: &reqwest::Client,
    leg: &mut ShadowLeg,
    exit_ms: i64,
    raw_exit: f64,
    reason: &str,
    cfg: &AltcoinReversalObserverConfig,
) {
    let funding = funding_return(http, leg, exit_ms).await;
    let (funding_value, funding_complete) = match funding {
        Ok(value) => (value, true),
        Err(error) => {
            warn!(symbol=%leg.symbol, error=%error, "影子腿资金费拉取失败，本次暂按 0 记录");
            (0.0, false)
        }
    };
    let fee = cfg.fee_bps_per_side / 10_000.0;
    let slip = cfg.slippage_bps_per_side / 10_000.0;
    let modeled_entry = leg.entry_price * (1.0 - slip);
    let modeled_exit = raw_exit * (1.0 + slip);
    let gross_return = 1.0 - raw_exit / leg.entry_price;
    let modeled_net_return =
        1.0 - modeled_exit / modeled_entry - fee - fee * modeled_exit / modeled_entry
            + funding_value;
    leg.latest_price = raw_exit;
    leg.exit_ms = Some(exit_ms);
    leg.exit_price = Some(raw_exit);
    leg.exit_reason = Some(reason.to_owned());
    leg.gross_return = Some(gross_return);
    leg.modeled_net_return = Some(modeled_net_return);
    leg.funding_return = Some(funding_value);
    leg.funding_complete = Some(funding_complete);
}

async fn update_active_basket(
    state: &mut ReversalObserverState,
    cfg: &AltcoinReversalObserverConfig,
    http: &reqwest::Client,
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    let Some(mut basket) = state.active_basket.take() else {
        return Ok(false);
    };
    let mut changed = false;
    for leg in basket.legs.iter_mut().filter(|leg| leg.exit_ms.is_none()) {
        let Some(bars) = bars_by_symbol.get(&leg.symbol) else {
            continue;
        };
        for bar in bars {
            if bar.open_ms <= leg.last_bar_ms {
                continue;
            }
            if bar.open_ms < leg.entry_ms / BAR_MS * BAR_MS {
                continue;
            }
            leg.last_bar_ms = bar.open_ms;
            leg.latest_price = bar.close;
            leg.mfe = leg.mfe.max(1.0 - bar.low / leg.entry_price);
            leg.mae = leg.mae.max(bar.high / leg.entry_price - 1.0);
            changed = true;
            let due_ms = leg.entry_ms + cfg.hold_hours as i64 * 3_600_000;
            for variant in &mut leg.exit_variants {
                update_exit_variant(variant, leg.entry_price, due_ms, bar, cfg);
            }
            if bar.open_ms >= due_ms {
                finish_leg(http, leg, bar.open_ms, bar.open, "time", cfg).await;
            } else if bar.high >= leg.stop_price {
                let raw_exit = if bar.open >= leg.stop_price {
                    bar.open
                } else {
                    leg.stop_price
                };
                finish_leg(http, leg, bar.close_ms, raw_exit, "stop", cfg).await;
            }
            if leg.exit_ms.is_some() {
                let funding_symbol = leg.symbol.clone();
                let funding_entry_ms = leg.entry_ms;
                for variant in &mut leg.exit_variants {
                    let Some(exit_ms) = variant.exit_ms else {
                        continue;
                    };
                    let funding =
                        funding_return_for_period(http, &funding_symbol, funding_entry_ms, exit_ms)
                            .await;
                    match funding {
                        Ok(total_funding) => {
                            let weighted_funding = if let Some(partial_ms) = variant.partial_ms {
                                let before = funding_return_for_period(
                                    http,
                                    &funding_symbol,
                                    funding_entry_ms,
                                    partial_ms,
                                )
                                .await;
                                match before {
                                    Ok(before_partial) => {
                                        before_partial
                                            + (total_funding - before_partial)
                                                * variant.remaining_fraction
                                    }
                                    Err(error) => {
                                        warn!(symbol=%leg.symbol, variant=%variant.id, error=%error, "影子退出实验分段资金费拉取失败");
                                        variant.funding_complete = false;
                                        total_funding * variant.remaining_fraction
                                    }
                                }
                            } else {
                                total_funding
                            };
                            if let Some(net) = variant.modeled_net_return.as_mut() {
                                *net += weighted_funding;
                            }
                        }
                        Err(error) => {
                            warn!(symbol=%leg.symbol, variant=%variant.id, error=%error, "影子退出实验资金费拉取失败");
                            variant.funding_complete = false;
                        }
                    }
                }
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"reversal_observer_leg_exit","mode":"observation_only","leg":leg}),
                )?;
                break;
            }
        }
    }
    if basket.legs.iter().all(|leg| leg.exit_ms.is_some()) {
        let basket_return = basket
            .legs
            .iter()
            .map(|leg| leg.notional / basket.start_equity * leg.modeled_net_return.unwrap_or(0.0))
            .sum::<f64>();
        let pnl_usdt = basket.start_equity * basket_return;
        let variant_ids = basket
            .legs
            .first()
            .map(|leg| {
                leg.exit_variants
                    .iter()
                    .map(|variant| (variant.id.clone(), variant.label.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let exit_variants = variant_ids
            .into_iter()
            .map(|(id, label)| {
                let variant_return = basket
                    .legs
                    .iter()
                    .map(|leg| {
                        let value = leg
                            .exit_variants
                            .iter()
                            .find(|variant| variant.id == id)
                            .and_then(|variant| variant.modeled_net_return)
                            .unwrap_or_else(|| leg.modeled_net_return.unwrap_or(0.0));
                        leg.notional / basket.start_equity * value
                    })
                    .sum::<f64>();
                ShadowVariantOutcome {
                    id,
                    label,
                    basket_return: variant_return,
                    pnl_usdt: basket.start_equity * variant_return,
                }
            })
            .collect::<Vec<_>>();
        state.shadow_equity = (basket.start_equity + pnl_usdt).max(0.01);
        let outcome = ShadowOutcome {
            signal_day: basket.signal_day,
            signal_ms: basket.signal_ms,
            entry_ms: basket.entry_ms,
            exit_ms: basket
                .legs
                .iter()
                .filter_map(|leg| leg.exit_ms)
                .max()
                .unwrap_or(now_ms),
            basket_return,
            pnl_usdt,
            equity_after: state.shadow_equity,
            stopped_legs: basket
                .legs
                .iter()
                .filter(|leg| leg.exit_reason.as_deref() == Some("stop"))
                .count(),
            funding_complete: basket
                .legs
                .iter()
                .all(|leg| leg.funding_complete == Some(true)),
            gate_enabled_at_entry: basket.gate_enabled_at_entry,
            symbols: basket.legs.iter().map(|leg| leg.symbol.clone()).collect(),
            exit_variants,
        };
        append_event(
            event_path,
            json!({"ts_ms":now_ms,"event":"reversal_observer_basket_exit","mode":"observation_only","outcome":outcome,"legs":basket.legs}),
        )?;
        state.outcomes.push(outcome);
        if state.outcomes.len() > 100 {
            state.outcomes.drain(..state.outcomes.len() - 100);
        }
        state.last_reason = "上一篮子已完成，等待下一次 UTC 00:15 评估".into();
        changed = true;
    } else {
        state.active_basket = Some(basket);
    }
    Ok(changed)
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_and_open(
    state: &mut ReversalObserverState,
    cfg: &AltcoinReversalObserverConfig,
    http: &reqwest::Client,
    active: &HashSet<String>,
    spot: &HashSet<String>,
    tickers: &Value,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    let day = now_ms / DAY_MS;
    if state.last_evaluation_day == Some(day) {
        return Ok(false);
    }
    let signal_ms = day * DAY_MS + BAR_MS;
    if now_ms < signal_ms {
        state.last_reason = "等待 UTC 00:15（北京时间 08:15）形成 7 日排名".into();
        return Ok(false);
    }
    let window_end = signal_ms + cfg.entry_window_minutes as i64 * 60_000;
    let ranking = rank_candidates(http, active, spot, tickers, signal_ms, cfg).await;
    let minimum_successful = (ranking.requested * 19 / 20).max(cfg.top_n * 2);
    if ranking.requested == 0 || ranking.successful < minimum_successful.min(ranking.requested) {
        state.last_evaluation_ms = Some(now_ms);
        let retry = now_ms <= window_end;
        if !retry {
            state.last_evaluation_day = Some(day);
        }
        state.last_reason = if retry {
            format!(
                "横截面行情覆盖不足（成功 {}/{}），本入场窗口内将自动重试",
                ranking.successful, ranking.requested
            )
        } else {
            format!(
                "横截面行情覆盖不足（成功 {}/{}）且已过入场窗口，本日跳过",
                ranking.successful, ranking.requested
            )
        };
        append_event(
            event_path,
            json!({"ts_ms":now_ms,"event":"reversal_observer_source_incomplete","mode":"observation_only","successful":ranking.successful,"requested":ranking.requested,"retry":retry}),
        )?;
        return Ok(true);
    }
    let mut candidates = ranking.candidates;
    state.universe_count = candidates.len();
    let selected: Vec<ReversalCandidate> = candidates
        .iter()
        .filter(|candidate| candidate.selected)
        .cloned()
        .collect();
    state.latest_candidates = candidates.drain(..candidates.len().min(10)).collect();
    state.last_evaluation_day = Some(day);
    state.last_evaluation_ms = Some(now_ms);

    let mut reason = None;
    if now_ms > window_end {
        reason = Some(format!(
            "本日启动晚于 {} 分钟入场窗口，仅记录排名，不补做影子交易",
            cfg.entry_window_minutes
        ));
    } else if state.active_basket.is_some() {
        reason = Some("上一影子篮子尚未完成，本日不重叠开仓".into());
    } else if state.universe_count < cfg.top_n * 2 {
        reason = Some(format!(
            "中等流动性有效样本 {}/{}，截面覆盖不足",
            state.universe_count,
            cfg.top_n * 2
        ));
    } else if selected.len() < cfg.top_n {
        reason = Some(format!(
            "正 7 日收益候选 {}/{}，不足以组成篮子",
            selected.len(),
            cfg.top_n
        ));
    }
    if let Some(reason) = reason {
        state.last_reason = reason.clone();
        append_event(
            event_path,
            json!({"ts_ms":now_ms,"event":"reversal_observer_evaluation","mode":"observation_only","decision":"no_shadow_entry","reason":reason,"universe_count":state.universe_count,"candidates":state.latest_candidates}),
        )?;
        return Ok(true);
    }

    let mut mark_tasks = tokio::task::JoinSet::new();
    for candidate in &selected {
        mark_tasks.spawn(mark_price(http.clone(), candidate.symbol.clone()));
    }
    let mut marks = HashMap::new();
    while let Some(result) = mark_tasks.join_next().await {
        match result {
            Ok(Ok((symbol, price))) => {
                marks.insert(symbol, price);
            }
            Ok(Err(error)) => warn!(error=%error, "反转影子入场标记价拉取失败"),
            Err(error) => warn!(error=%error, "反转影子标记价任务失败"),
        }
    }
    if marks.len() != selected.len() {
        let retry = now_ms <= window_end;
        if retry {
            state.last_evaluation_day = None;
        }
        state.last_reason = format!(
            "影子入场标记价仅取得 {}/{}，为避免选择偏差整篮子{}",
            marks.len(),
            selected.len(),
            if retry { "稍后重试" } else { "跳过" }
        );
        append_event(
            event_path,
            json!({"ts_ms":now_ms,"event":"reversal_observer_evaluation","mode":"observation_only","decision":"no_shadow_entry","reason":state.last_reason,"retry":retry,"candidates":state.latest_candidates}),
        )?;
        return Ok(true);
    }

    let gate = gate_metrics(state, cfg);
    let start_equity = if state.shadow_equity > 0.0 {
        state.shadow_equity
    } else {
        cfg.shadow_capital_usdt
    };
    state.shadow_equity = start_equity;
    let notional = start_equity * cfg.gross_multiple / selected.len() as f64;
    let legs = selected
        .iter()
        .map(|candidate| {
            let entry_price = marks[&candidate.symbol];
            ShadowLeg {
                symbol: candidate.symbol.clone(),
                rank: candidate.rank,
                return_7d: candidate.return_7d,
                volume_24h: candidate.volume_24h,
                signal_price: candidate.signal_price,
                entry_ms: now_ms,
                entry_price,
                notional,
                stop_price: entry_price * (1.0 + cfg.stop_pct),
                last_bar_ms: now_ms / BAR_MS * BAR_MS - BAR_MS,
                latest_price: entry_price,
                mfe: 0.0,
                mae: 0.0,
                exit_ms: None,
                exit_price: None,
                exit_reason: None,
                gross_return: None,
                modeled_net_return: None,
                funding_return: None,
                funding_complete: None,
                exit_variants: new_exit_variants(entry_price, cfg.stop_pct),
            }
        })
        .collect();
    let basket = ShadowBasket {
        signal_day: day,
        signal_ms,
        entry_ms: now_ms,
        start_equity,
        gross_multiple: cfg.gross_multiple,
        gate_ready_at_entry: gate.ready,
        gate_enabled_at_entry: gate.enabled,
        gate_mean_at_entry: gate.mean,
        gate_profit_factor_at_entry: gate.profit_factor,
        legs,
        exit_experiment_version: EXIT_EXPERIMENT_VERSION,
    };
    state.last_reason = if gate.enabled {
        "门控通过；本模块仍只生成影子交易，不会向币安下单".into()
    } else if gate.ready {
        format!(
            "门控关闭（20 篮子均值 {:.2}% / PF {:.2}）；继续记录影子结果",
            gate.mean * 100.0,
            gate.profit_factor
        )
    } else {
        format!(
            "前向门控预热 {}/{}；继续记录影子结果",
            gate.samples, cfg.gate_window
        )
    };
    append_event(
        event_path,
        json!({"ts_ms":now_ms,"event":"reversal_observer_basket_entry","mode":"observation_only","reason":state.last_reason,"basket":basket,"cost_model":{"fee_bps_per_side":cfg.fee_bps_per_side,"slippage_bps_per_side":cfg.slippage_bps_per_side,"funding":"binance_actual"}}),
    )?;
    state.active_basket = Some(basket);
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub async fn update_reversal_observer(
    state: &mut ReversalObserverState,
    cfg: &AltcoinReversalObserverConfig,
    http: &reqwest::Client,
    active: &HashSet<String>,
    spot: &HashSet<String>,
    tickers: &Value,
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    if !cfg.enabled {
        return Ok(false);
    }
    if state.shadow_equity <= 0.0 {
        state.shadow_equity = cfg.shadow_capital_usdt;
    }
    let mut changed =
        update_active_basket(state, cfg, http, bars_by_symbol, now_ms, event_path).await?;
    changed |=
        evaluate_and_open(state, cfg, http, active, spot, tickers, now_ms, event_path).await?;
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_needs_full_forward_window() {
        let cfg = AltcoinReversalObserverConfig {
            enabled: true,
            gate_window: 5,
            ..Default::default()
        };
        let mut state = ReversalObserverState::default();
        for index in 0..4 {
            state.outcomes.push(outcome(index, 0.02));
        }
        let gate = gate_metrics(&state, &cfg);
        assert!(!gate.ready);
        assert!(!gate.enabled);
        state.outcomes.push(outcome(4, 0.02));
        let gate = gate_metrics(&state, &cfg);
        assert!(gate.ready);
        assert!(gate.enabled);
    }

    #[test]
    fn gate_closes_when_recent_profit_factor_is_weak() {
        let cfg = AltcoinReversalObserverConfig {
            enabled: true,
            gate_window: 5,
            ..Default::default()
        };
        let mut state = ReversalObserverState::default();
        for (index, value) in [0.01, 0.01, -0.02, -0.02, 0.01].into_iter().enumerate() {
            state.outcomes.push(outcome(index, value));
        }
        let gate = gate_metrics(&state, &cfg);
        assert!(gate.ready);
        assert!(!gate.enabled);
        assert!(gate.profit_factor < 1.20);
    }

    #[test]
    fn short_cost_model_matches_backtest_formula() {
        let entry = 100.0;
        let raw_exit = 95.0;
        let fee = 5.0 / 10_000.0;
        let slip = 5.0 / 10_000.0;
        let modeled_entry = entry * (1.0 - slip);
        let modeled_exit = raw_exit * (1.0 + slip);
        let value = 1.0 - modeled_exit / modeled_entry - fee - fee * modeled_exit / modeled_entry;
        assert!(value > 0.047 && value < 0.049);
    }

    #[test]
    fn exit_variants_diverge_after_profit_then_reversal() {
        let cfg = AltcoinReversalObserverConfig {
            enabled: true,
            ..Default::default()
        };
        let entry = 100.0;
        let due = DAY_MS;
        let mut variants = new_exit_variants(entry, cfg.stop_pct);
        let favorable = Bar {
            open_ms: 0,
            close_ms: BAR_MS - 1,
            open: 100.0,
            high: 101.0,
            low: 90.0,
            close: 92.0,
            quote_volume: 1.0,
        };
        for variant in &mut variants {
            update_exit_variant(variant, entry, due, &favorable, &cfg);
        }
        let reversal = Bar {
            open_ms: BAR_MS,
            close_ms: 2 * BAR_MS - 1,
            open: 92.0,
            high: 101.0,
            low: 91.0,
            close: 100.5,
            quote_volume: 1.0,
        };
        for variant in &mut variants {
            update_exit_variant(variant, entry, due, &reversal, &cfg);
        }
        let baseline = variants.iter().find(|item| item.id == "baseline").unwrap();
        let breakeven = variants
            .iter()
            .find(|item| item.id == "breakeven_5")
            .unwrap();
        let trailing = variants.iter().find(|item| item.id == "trail_8").unwrap();
        let staged = variants.iter().find(|item| item.id == "staged").unwrap();
        assert!(baseline.exit_ms.is_none());
        assert_eq!(breakeven.exit_reason.as_deref(), Some("breakeven"));
        assert_eq!(trailing.exit_reason.as_deref(), Some("trailing"));
        assert!(staged.partial_taken);
        assert!(staged.modeled_net_return.unwrap() > breakeven.modeled_net_return.unwrap());
    }

    fn outcome(index: usize, basket_return: f64) -> ShadowOutcome {
        ShadowOutcome {
            signal_day: index as i64,
            signal_ms: index as i64,
            entry_ms: index as i64,
            exit_ms: index as i64 + DAY_MS,
            basket_return,
            pnl_usdt: basket_return * 1_000.0,
            equity_after: 1_000.0 * (1.0 + basket_return),
            stopped_legs: 0,
            funding_complete: true,
            gate_enabled_at_entry: false,
            symbols: vec!["TESTUSDT".into()],
            exit_variants: Vec::new(),
        }
    }
}
