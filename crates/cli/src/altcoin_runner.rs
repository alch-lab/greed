//! 多币种山寨币放量突破执行器。
//!
//! 该执行器与 BTC 的 TRDR 引擎互斥运行：控制面同一时间只允许一个交易任务。
//! 行情来自 Binance 主网公共 REST，paper 订单仍发送到 Futures Demo。

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
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
const BEIJING_OFFSET_MS: i64 = 8 * 60 * 60 * 1_000;

fn risk_day(ts_ms: i64) -> i64 {
    ts_ms.saturating_add(BEIJING_OFFSET_MS).div_euclid(DAY_MS)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AltcoinImpulseConfig {
    pub enabled: bool,
    pub capital_usdt: f64,
    pub exchange_leverage: u32,
    #[serde(default = "default_min_exchange_leverage")]
    pub min_exchange_leverage: u32,
    pub risk_per_trade: f64,
    /// Directional multipliers applied after the base per-trade risk budget.
    /// Keeping them explicit makes the portfolio-capacity decision observable
    /// without changing the signal score or the exchange leverage.
    #[serde(default = "default_risk_scale")]
    pub long_risk_scale: f64,
    #[serde(default = "default_risk_scale")]
    pub short_risk_scale: f64,
    /// Reject entries that add risk in the already-crowded direction.  The
    /// sign is mirrored: positive funding/premium crowds longs, while negative
    /// funding/premium crowds shorts.
    #[serde(default = "default_max_directional_funding_rate")]
    pub max_directional_funding_rate: f64,
    #[serde(default = "default_max_directional_premium")]
    pub max_directional_premium: f64,
    pub max_positions: usize,
    pub max_daily_entries: u32,
    #[serde(default = "default_max_daily_entry_bonus")]
    pub max_daily_entry_bonus: u32,
    pub daily_loss_limit: f64,
    pub max_gross_multiple: f64,
    #[serde(default = "default_first_week_duration_days")]
    pub first_week_duration_days: u32,
    #[serde(default)]
    pub first_week_loss_limit: f64,
    #[serde(default = "default_dry_slippage_bps")]
    pub dry_slippage_bps: f64,
    pub stop_pct: f64,
    #[serde(default = "default_loss_trim_trigger_pct")]
    pub loss_trim_trigger_pct: f64,
    #[serde(default = "default_loss_trim_fraction")]
    pub loss_trim_fraction: f64,
    #[serde(default = "default_risk_execution_buffer_pct")]
    pub risk_execution_buffer_pct: f64,
    pub trail_activation_pct: f64,
    pub trail_pct: f64,
    #[serde(default = "default_partial_take_profit_fraction")]
    pub partial_take_profit_fraction: f64,
    #[serde(default = "default_failed_breakout_window_minutes")]
    pub failed_breakout_window_minutes: u32,
    #[serde(default = "default_failed_breakout_adverse_pct")]
    pub failed_breakout_adverse_pct: f64,
    #[serde(default = "default_failed_breakout_max_mfe_pct")]
    pub failed_breakout_max_mfe_pct: f64,
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
    #[serde(default)]
    pub direct_entry_enabled: bool,
    #[serde(default = "default_true")]
    pub loss_trim_enabled: bool,
    #[serde(default = "default_true")]
    pub failed_breakout_enabled: bool,
    #[serde(default = "default_true")]
    pub recovery_lock_enabled: bool,
    #[serde(default = "default_confirmation_window_bars")]
    pub confirmation_window_bars: u32,
    #[serde(default = "default_retest_touch_pct")]
    pub retest_touch_pct: f64,
    #[serde(default = "default_retest_invalidation_pct")]
    pub retest_invalidation_pct: f64,
    #[serde(default = "default_reclaim_pct")]
    pub reclaim_pct: f64,
    /// Large breakout candles have already moved too far away from the old
    /// 24h boundary.  For those setups, confirm a shallow pullback around the
    /// signal close instead of waiting for a structurally stale boundary.
    #[serde(default)]
    pub adaptive_retest_enabled: bool,
    #[serde(default = "default_vertical_overshoot_pct")]
    pub vertical_overshoot_pct: f64,
    #[serde(default = "default_vertical_retest_touch_pct")]
    pub vertical_retest_touch_pct: f64,
    #[serde(default = "default_vertical_retest_invalidation_pct")]
    pub vertical_retest_invalidation_pct: f64,
    #[serde(default = "default_vertical_reclaim_pct")]
    pub vertical_reclaim_pct: f64,
    #[serde(default = "default_vertical_max_entry_extension_pct")]
    pub vertical_max_entry_extension_pct: f64,
    #[serde(default)]
    pub intrabar_vertical_retest_enabled: bool,
    #[serde(default = "default_intrabar_min_pullback_pct")]
    pub intrabar_min_pullback_pct: f64,
    #[serde(default = "default_intrabar_rebound_pct")]
    pub intrabar_rebound_pct: f64,
    #[serde(default = "default_extreme_direct_enabled")]
    pub extreme_direct_enabled: bool,
    #[serde(default = "default_extreme_direct_return_1h")]
    pub extreme_direct_return_1h: f64,
    #[serde(default = "default_extreme_direct_return_4h")]
    pub extreme_direct_return_4h: f64,
    #[serde(default = "default_extreme_direct_volume_ratio")]
    pub extreme_direct_volume_ratio: f64,
    #[serde(default = "default_extreme_direct_risk_scale")]
    pub extreme_direct_risk_scale: f64,
    /// 独立小仓“杠杆衰竭做空”通道。它观察极端上涨后的第一根 15m K，
    /// 不占主策略仓位槽，也不写入主策略的同币冷却。
    #[serde(default)]
    pub pulse_exhaustion_enabled: bool,
    #[serde(default)]
    pub pulse_exhaustion_allow_live: bool,
    #[serde(default = "default_pulse_initial_return_1h")]
    pub pulse_initial_return_1h: f64,
    #[serde(default = "default_pulse_initial_volume_ratio")]
    pub pulse_initial_volume_ratio: f64,
    #[serde(default = "default_pulse_perp_return_1h")]
    pub pulse_perp_return_1h: f64,
    #[serde(default = "default_pulse_oi_change_1h")]
    pub pulse_oi_change_1h: f64,
    #[serde(default = "default_pulse_max_spot_perp_ratio")]
    pub pulse_max_spot_perp_ratio: f64,
    #[serde(default = "default_pulse_min_peak_retrace")]
    pub pulse_min_peak_retrace: f64,
    #[serde(default = "default_pulse_max_close_location")]
    pub pulse_max_close_location: f64,
    #[serde(default = "default_pulse_risk_scale")]
    pub pulse_risk_scale: f64,
    #[serde(default = "default_pulse_max_positions")]
    pub pulse_max_positions: usize,
    #[serde(default = "default_pulse_max_gross_multiple")]
    pub pulse_max_gross_multiple: f64,
    /// The exhaustion sleeve has a different payoff path from the breakout
    /// sleeve, so its stop and profit protection are configured independently.
    /// This also keeps a pulse retune from silently changing open main-strategy
    /// positions.
    #[serde(default = "default_pulse_stop_pct")]
    pub pulse_stop_pct: f64,
    #[serde(default = "default_pulse_trail_activation_pct")]
    pub pulse_trail_activation_pct: f64,
    #[serde(default = "default_pulse_trail_pct")]
    pub pulse_trail_pct: f64,
    #[serde(default = "default_pulse_max_directional_premium")]
    pub pulse_max_directional_premium: f64,
    pub min_24h_volume_usd: f64,
    #[serde(default = "default_min_contract_age_days")]
    pub min_contract_age_days: u32,
    #[serde(default = "default_max_spread_bps")]
    pub max_spread_bps: f64,
    /// 深度、冲击和成交流全部显著优于硬门槛时允许的点差上限。
    /// 普通盘口仍使用 `max_spread_bps`，避免为提高频率而全面放松流动性保护。
    #[serde(default = "default_liquid_market_max_spread_bps")]
    pub liquid_market_max_spread_bps: f64,
    #[serde(default = "default_max_entry_impact_bps")]
    pub max_entry_impact_bps: f64,
    #[serde(default = "default_max_exit_impact_bps")]
    pub max_exit_impact_bps: f64,
    #[serde(default = "default_depth_band_pct")]
    pub depth_band_pct: f64,
    #[serde(default = "default_min_depth_multiple")]
    pub min_depth_multiple: f64,
    #[serde(default = "default_recent_trade_window_seconds")]
    pub recent_trade_window_seconds: u64,
    #[serde(default = "default_min_recent_trades")]
    pub min_recent_trades: usize,
    #[serde(default = "default_min_unique_trade_prices")]
    pub min_unique_trade_prices: usize,
    /// 成交价档位数容易受币种 tick size 影响。默认仅记录告警；只有显式开启时
    /// 才能单独否决一个在成交笔数、点差、深度和冲击上均合格的候选。
    #[serde(default)]
    pub unique_trade_prices_hard: bool,
    #[serde(default = "default_max_last_trade_age_seconds")]
    pub max_last_trade_age_seconds: u64,
    pub min_return_1h: f64,
    pub max_return_1h: f64,
    #[serde(default = "default_overextension_long_return_1h")]
    pub overextension_long_return_1h: f64,
    #[serde(default = "default_overextension_long_return_4h")]
    pub overextension_long_return_4h: f64,
    #[serde(default = "default_true")]
    pub overextension_long_enabled: bool,
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
    /// 横截面策略使用独立的硬止损、分段止盈和跟踪退出参数。
    #[serde(default)]
    pub fixed_time_exit_only: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AltcoinCrossSectionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_cross_formation_hours")]
    pub formation_hours: usize,
    #[serde(default = "default_cross_hold_hours")]
    pub hold_hours: usize,
    #[serde(default = "default_cross_names")]
    pub names: usize,
    #[serde(default = "default_cross_min_volume")]
    pub min_24h_volume_usd: f64,
    #[serde(default = "default_cross_market_threshold")]
    pub market_momentum_threshold: f64,
    #[serde(default = "default_cross_gate_window")]
    pub gate_window: usize,
    #[serde(default = "default_cross_gate_pf")]
    pub gate_min_profit_factor: f64,
    #[serde(default = "default_cross_assumed_cost_bps")]
    pub assumed_cost_bps_per_side: f64,
    #[serde(default = "default_cross_base_gross")]
    pub base_gross_multiple: f64,
    #[serde(default = "default_cross_active_gross")]
    pub active_gross_multiple: f64,
    #[serde(default = "default_cross_strong_excess_return")]
    pub strong_excess_return: f64,
    #[serde(default = "default_cross_strong_gross")]
    pub strong_gross_multiple: f64,
    #[serde(default = "default_cross_stop_pct")]
    pub stop_pct: f64,
    /// Cross-section positions are re-ranked every `hold_hours`, but a
    /// same-symbol/same-side winner is carried instead of churned. Profit
    /// protection therefore belongs to the position, not to one ranking
    /// window.
    #[serde(default = "default_cross_trail_activation_pct")]
    pub trail_activation_pct: f64,
    #[serde(default = "default_cross_trail_pct")]
    pub trail_pct: f64,
    #[serde(default = "default_cross_partial_take_profit_fraction")]
    pub partial_take_profit_fraction: f64,
}

impl Default for AltcoinCrossSectionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            formation_hours: default_cross_formation_hours(),
            hold_hours: default_cross_hold_hours(),
            names: default_cross_names(),
            min_24h_volume_usd: default_cross_min_volume(),
            market_momentum_threshold: default_cross_market_threshold(),
            gate_window: default_cross_gate_window(),
            gate_min_profit_factor: default_cross_gate_pf(),
            assumed_cost_bps_per_side: default_cross_assumed_cost_bps(),
            base_gross_multiple: default_cross_base_gross(),
            active_gross_multiple: default_cross_active_gross(),
            strong_excess_return: default_cross_strong_excess_return(),
            strong_gross_multiple: default_cross_strong_gross(),
            stop_pct: default_cross_stop_pct(),
            trail_activation_pct: default_cross_trail_activation_pct(),
            trail_pct: default_cross_trail_pct(),
            partial_take_profit_fraction: default_cross_partial_take_profit_fraction(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AltcoinShockReversalConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_shock_min_return")]
    pub min_shock_return: f64,
    #[serde(default = "default_shock_max_return")]
    pub max_shock_return: f64,
    #[serde(default = "default_shock_min_reversal")]
    pub min_reversal_return: f64,
    #[serde(default = "default_shock_min_volume_ratio")]
    pub min_volume_ratio: f64,
    #[serde(default = "default_shock_min_close_location")]
    pub min_close_location: f64,
    #[serde(default = "default_shock_max_candle_range")]
    pub max_candle_range: f64,
    #[serde(default = "default_shock_strict_volume_ratio")]
    pub strict_volume_ratio: f64,
    #[serde(default = "default_shock_strict_close_location")]
    pub strict_close_location: f64,
    #[serde(default = "default_shock_strict_max_candle_range")]
    pub strict_max_candle_range: f64,
    #[serde(default = "default_shock_gate_window")]
    pub gate_window: usize,
    #[serde(default = "default_shock_gate_pf")]
    pub gate_min_profit_factor: f64,
    #[serde(default = "default_shock_assumed_fee_bps")]
    pub assumed_fee_bps_per_side: f64,
    #[serde(default = "default_shock_assumed_slippage_bps")]
    pub assumed_slippage_bps_per_side: f64,
    #[serde(default = "default_shock_stop_pct")]
    pub stop_pct: f64,
    #[serde(default = "default_shock_trail_activation_pct")]
    pub trail_activation_pct: f64,
    #[serde(default = "default_shock_trail_pct")]
    pub trail_pct: f64,
    #[serde(default = "default_shock_hold_hours")]
    pub max_hold_hours: u32,
    #[serde(default = "default_shock_broad_gross")]
    pub broad_gross_multiple: f64,
    #[serde(default = "default_shock_strict_gross")]
    pub strict_gross_multiple: f64,
    #[serde(default = "default_shock_max_gross")]
    pub max_gross_multiple: f64,
    #[serde(default = "default_shock_cooldown_hours")]
    pub cooldown_hours: u32,
    #[serde(default = "default_shock_gate_refresh_hours")]
    pub gate_refresh_hours: u32,
}

impl Default for AltcoinShockReversalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_shock_return: default_shock_min_return(),
            max_shock_return: default_shock_max_return(),
            min_reversal_return: default_shock_min_reversal(),
            min_volume_ratio: default_shock_min_volume_ratio(),
            min_close_location: default_shock_min_close_location(),
            max_candle_range: default_shock_max_candle_range(),
            strict_volume_ratio: default_shock_strict_volume_ratio(),
            strict_close_location: default_shock_strict_close_location(),
            strict_max_candle_range: default_shock_strict_max_candle_range(),
            gate_window: default_shock_gate_window(),
            gate_min_profit_factor: default_shock_gate_pf(),
            assumed_fee_bps_per_side: default_shock_assumed_fee_bps(),
            assumed_slippage_bps_per_side: default_shock_assumed_slippage_bps(),
            stop_pct: default_shock_stop_pct(),
            trail_activation_pct: default_shock_trail_activation_pct(),
            trail_pct: default_shock_trail_pct(),
            max_hold_hours: default_shock_hold_hours(),
            broad_gross_multiple: default_shock_broad_gross(),
            strict_gross_multiple: default_shock_strict_gross(),
            max_gross_multiple: default_shock_max_gross(),
            cooldown_hours: default_shock_cooldown_hours(),
            gate_refresh_hours: default_shock_gate_refresh_hours(),
        }
    }
}

fn default_shock_min_return() -> f64 {
    0.045
}
fn default_shock_max_return() -> f64 {
    0.22
}
fn default_shock_min_reversal() -> f64 {
    0.006
}
fn default_shock_min_volume_ratio() -> f64 {
    2.5
}
fn default_shock_min_close_location() -> f64 {
    0.68
}
fn default_shock_max_candle_range() -> f64 {
    0.15
}
fn default_shock_strict_volume_ratio() -> f64 {
    3.5
}
fn default_shock_strict_close_location() -> f64 {
    0.75
}
fn default_shock_strict_max_candle_range() -> f64 {
    0.12
}
fn default_shock_gate_window() -> usize {
    30
}
fn default_shock_gate_pf() -> f64 {
    1.0
}
fn default_shock_assumed_fee_bps() -> f64 {
    5.0
}
fn default_shock_assumed_slippage_bps() -> f64 {
    10.0
}
fn default_shock_stop_pct() -> f64 {
    0.02
}
fn default_shock_trail_activation_pct() -> f64 {
    0.03
}
fn default_shock_trail_pct() -> f64 {
    0.01
}
fn default_shock_hold_hours() -> u32 {
    3
}
fn default_shock_broad_gross() -> f64 {
    0.5
}
fn default_shock_strict_gross() -> f64 {
    1.0
}
fn default_shock_max_gross() -> f64 {
    2.0
}
fn default_shock_cooldown_hours() -> u32 {
    1
}
fn default_shock_gate_refresh_hours() -> u32 {
    3
}

fn default_cross_formation_hours() -> usize {
    12
}
fn default_cross_hold_hours() -> usize {
    3
}
fn default_cross_names() -> usize {
    1
}
fn default_cross_min_volume() -> f64 {
    10_000_000.0
}
fn default_cross_market_threshold() -> f64 {
    0.01
}
fn default_cross_gate_window() -> usize {
    30
}
fn default_cross_gate_pf() -> f64 {
    1.3
}
fn default_cross_assumed_cost_bps() -> f64 {
    10.0
}
fn default_cross_base_gross() -> f64 {
    0.15
}
fn default_cross_active_gross() -> f64 {
    1.0
}
fn default_cross_strong_excess_return() -> f64 {
    0.12
}
fn default_cross_strong_gross() -> f64 {
    1.5
}
fn default_cross_stop_pct() -> f64 {
    0.04
}
fn default_cross_trail_activation_pct() -> f64 {
    0.05
}
fn default_cross_trail_pct() -> f64 {
    0.02
}
fn default_cross_partial_take_profit_fraction() -> f64 {
    0.33
}

#[derive(Debug, Deserialize)]
struct StrategyFile {
    altcoin_impulse: AltcoinImpulseConfig,
    #[serde(default)]
    altcoin_cross_section: AltcoinCrossSectionConfig,
    #[serde(default)]
    altcoin_shock_reversal: AltcoinShockReversalConfig,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Candidate {
    symbol: String,
    signal_ms: i64,
    /// Stable setup origin. `signal_ms` advances to the confirmation bar,
    /// while this value keeps signal/confirmation/outcome attribution paired.
    #[serde(default)]
    setup_origin_ms: i64,
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
    #[serde(default)]
    breakout_level: f64,
    #[serde(default = "default_entry_trigger")]
    entry_trigger: String,
    #[serde(default = "default_risk_scale")]
    risk_scale: f64,
    blockers: Vec<String>,
    spot_return_1h: Option<f64>,
    oi_change_1h: Option<f64>,
    funding_rate: Option<f64>,
    perp_premium: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingEntry {
    candidate: Candidate,
    breakout_level: f64,
    #[serde(default)]
    retest_anchor: f64,
    #[serde(default = "default_confirmation_mode")]
    confirmation_mode: String,
    #[serde(default)]
    intrabar_touch_seen: bool,
    #[serde(default)]
    intrabar_touch_ms: Option<i64>,
    #[serde(default)]
    intrabar_extreme_price: Option<f64>,
    #[serde(default)]
    intrabar_last_price: Option<f64>,
    #[serde(default)]
    intrabar_confirmed_ms: Option<i64>,
    #[serde(default)]
    intrabar_confirmed_price: Option<f64>,
    expires_ms: i64,
    last_checked_close_ms: i64,
    retest_seen: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PulseExhaustionSetup {
    symbol: String,
    origin_signal_ms: i64,
    initial_price: f64,
    initial_return_1h: f64,
    initial_volume_ratio: f64,
    volume_24h: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MicrostructureTrial {
    strategy: String,
    symbol: String,
    side: i32,
    origin_signal_ms: i64,
    reference_ms: i64,
    reference_price: f64,
    expires_ms: i64,
    favorable_extreme: f64,
    adverse_extreme: f64,
    last_price: f64,
    #[serde(default)]
    executed: bool,
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
    #[serde(default)]
    setup_origin_ms: i64,
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
    /// A position can exceed Binance's MARKET_LOT_SIZE maxQty even though its
    /// LIMIT entry is valid under LOT_SIZE.  Keep every protective algo order
    /// so the full logical position remains covered in market-sized chunks.
    #[serde(default)]
    protection_order_ids: Vec<i64>,
    #[serde(default = "default_protection_reason")]
    protection_reason: String,
    #[serde(default)]
    exchange_leverage: Option<u32>,
    #[serde(default = "default_entry_phase")]
    entry_phase: String,
    #[serde(default)]
    partial_take_profit_done: bool,
    #[serde(default)]
    loss_trim_done: bool,
    #[serde(default)]
    last_partial_exit_ms: Option<i64>,
    #[serde(default)]
    realized_partial_pnl: f64,
}

fn default_max_entry_slippage_pct() -> f64 {
    0.015
}

fn default_first_week_duration_days() -> u32 {
    7
}

fn default_dry_slippage_bps() -> f64 {
    5.0
}

fn adverse_fill_price(price: f64, side: i32, slippage_bps: f64, opening: bool) -> f64 {
    let direction = if opening { side } else { -side };
    price * (1.0 + direction as f64 * slippage_bps / 10_000.0)
}

fn existing_stop_raw_fill(side: i32, stop_price: f64, bar: &Bar) -> Option<f64> {
    let hit = if side > 0 {
        bar.low <= stop_price
    } else {
        bar.high >= stop_price
    };
    if !hit {
        return None;
    }
    let gapped = (side > 0 && bar.open < stop_price) || (side < 0 && bar.open > stop_price);
    Some(if gapped { bar.open } else { stop_price })
}

fn default_max_daily_entry_bonus() -> u32 {
    4
}

fn default_max_signal_age_seconds() -> u64 {
    120
}

fn default_true() -> bool {
    true
}

fn default_confirmation_window_bars() -> u32 {
    4
}

fn default_retest_touch_pct() -> f64 {
    0.01
}

fn default_retest_invalidation_pct() -> f64 {
    0.015
}

fn default_reclaim_pct() -> f64 {
    0.002
}

fn default_vertical_overshoot_pct() -> f64 {
    0.035
}

fn default_vertical_retest_touch_pct() -> f64 {
    0.005
}

fn default_vertical_retest_invalidation_pct() -> f64 {
    0.015
}

fn default_vertical_reclaim_pct() -> f64 {
    0.002
}

fn default_vertical_max_entry_extension_pct() -> f64 {
    0.02
}

fn default_intrabar_rebound_pct() -> f64 {
    0.003
}

fn default_intrabar_min_pullback_pct() -> f64 {
    0.003
}

fn default_confirmation_mode() -> String {
    "breakout_retest".to_owned()
}

fn default_extreme_direct_return_1h() -> f64 {
    0.18
}

fn default_extreme_direct_enabled() -> bool {
    true
}

fn default_extreme_direct_return_4h() -> f64 {
    0.25
}

fn default_extreme_direct_volume_ratio() -> f64 {
    6.0
}

fn default_extreme_direct_risk_scale() -> f64 {
    0.33
}

fn default_pulse_initial_return_1h() -> f64 {
    0.06
}
fn default_pulse_initial_volume_ratio() -> f64 {
    10.0
}
fn default_pulse_perp_return_1h() -> f64 {
    0.06
}
fn default_pulse_oi_change_1h() -> f64 {
    0.20
}
fn default_pulse_max_spot_perp_ratio() -> f64 {
    1.10
}
fn default_pulse_min_peak_retrace() -> f64 {
    0.035
}
fn default_pulse_max_close_location() -> f64 {
    0.80
}
fn default_pulse_risk_scale() -> f64 {
    0.15
}
fn default_pulse_max_positions() -> usize {
    1
}
fn default_pulse_max_gross_multiple() -> f64 {
    0.50
}
fn default_pulse_stop_pct() -> f64 {
    0.01
}
fn default_pulse_trail_activation_pct() -> f64 {
    0.015
}
fn default_pulse_trail_pct() -> f64 {
    0.005
}
fn default_pulse_max_directional_premium() -> f64 {
    0.01
}

fn default_entry_trigger() -> String {
    "breakout_detected".to_owned()
}

fn default_risk_scale() -> f64 {
    1.0
}

fn default_max_directional_funding_rate() -> f64 {
    0.003
}

fn default_max_directional_premium() -> f64 {
    0.01
}

fn directional_crowding_reason(
    side: i32,
    funding_rate: Option<f64>,
    perp_premium: Option<f64>,
    max_funding: f64,
    max_premium: f64,
) -> Option<String> {
    let (Some(funding_rate), Some(perp_premium)) = (funding_rate, perp_premium) else {
        return Some("资金费率或永续溢价不可用，方向拥挤保护拒绝开仓".to_owned());
    };
    let direction = side as f64;
    let directional_funding = direction * funding_rate;
    let directional_premium = direction * perp_premium;
    if directional_funding > max_funding || directional_premium > max_premium {
        return Some(format!(
            "顺方向仓位过度拥挤：方向化资金费率 {:.4}% / 上限 {:.4}%，方向化永续偏离 {:.3}% / 上限 {:.3}%",
            directional_funding * 100.0,
            max_funding * 100.0,
            directional_premium * 100.0,
            max_premium * 100.0
        ));
    }
    None
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

fn default_max_spread_bps() -> f64 {
    10.0
}
fn default_liquid_market_max_spread_bps() -> f64 {
    15.0
}
fn default_min_contract_age_days() -> u32 {
    7
}
fn default_max_entry_impact_bps() -> f64 {
    15.0
}
fn default_max_exit_impact_bps() -> f64 {
    20.0
}
fn default_depth_band_pct() -> f64 {
    0.005
}
fn default_min_depth_multiple() -> f64 {
    10.0
}
fn default_recent_trade_window_seconds() -> u64 {
    60
}
fn default_min_recent_trades() -> usize {
    30
}
fn default_min_unique_trade_prices() -> usize {
    8
}
fn default_max_last_trade_age_seconds() -> u64 {
    5
}

fn effective_max_spread_bps(
    cfg: &AltcoinImpulseConfig,
    liquidity: &live::rest::LiquiditySnapshot,
    required_depth: f64,
) -> (f64, bool) {
    let liquid_market = liquidity.bid_depth_usd >= required_depth * 3.0
        && liquidity.ask_depth_usd >= required_depth * 3.0
        && liquidity
            .entry_impact_bps
            .is_some_and(|impact| impact <= cfg.max_entry_impact_bps * 0.5)
        && liquidity
            .exit_impact_bps
            .is_some_and(|impact| impact <= cfg.max_exit_impact_bps * 0.5)
        && liquidity.recent_trade_count >= cfg.min_recent_trades.saturating_mul(2)
        && liquidity.unique_trade_prices >= cfg.min_unique_trade_prices
        && liquidity.last_trade_age_ms <= cfg.max_last_trade_age_seconds as i64 * 1_000
        && liquidity.trade_history_span_ms >= cfg.recent_trade_window_seconds as i64 * 1_000;
    (
        if liquid_market {
            cfg.liquid_market_max_spread_bps
        } else {
            cfg.max_spread_bps
        },
        liquid_market,
    )
}

fn default_loss_trim_trigger_pct() -> f64 {
    0.01
}

fn default_loss_trim_fraction() -> f64 {
    0.33
}

fn default_partial_take_profit_fraction() -> f64 {
    0.33
}

fn default_failed_breakout_window_minutes() -> u32 {
    15
}

fn default_failed_breakout_adverse_pct() -> f64 {
    0.03
}

fn default_failed_breakout_max_mfe_pct() -> f64 {
    0.005
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

fn failed_breakout(
    position: &Position,
    mark_price: f64,
    now_ms: i64,
    window_minutes: u32,
    adverse_pct: f64,
    max_mfe_pct: f64,
) -> bool {
    if position.partial_take_profit_done || now_ms < position.entry_ms {
        return false;
    }
    let age_ms = now_ms - position.entry_ms;
    let within_window = age_ms <= window_minutes as i64 * 60_000;
    let current_return = position.side as f64 * (mark_price / position.entry_price - 1.0);
    let mfe = position.side as f64 * (position.extreme / position.entry_price - 1.0);
    within_window && current_return <= -adverse_pct && mfe < max_mfe_pct
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

#[derive(Debug, Clone, Serialize, PartialEq)]
struct FirstWeekProgress {
    started_ms: i64,
    ends_ms: i64,
    elapsed_days: f64,
    start_equity: f64,
    current_equity: f64,
    current_profit_usdt: f64,
    loss_limit_pct: f64,
    loss_limit_reached: bool,
    realized_average_per_day_usdt: f64,
    window_complete: bool,
    entries_blocked: bool,
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
    #[serde(default)]
    pending_entries: HashMap<String, PendingEntry>,
    #[serde(default)]
    pulse_exhaustion_setups: HashMap<String, PulseExhaustionSetup>,
    #[serde(default)]
    pulse_seen_signal: HashMap<String, i64>,
    #[serde(default)]
    latest_pulse_exhaustion: Value,
    day: i64,
    day_start_equity: f64,
    daily_entries: u32,
    #[serde(default)]
    daily_entry_bonus: u32,
    #[serde(default)]
    daily_risk_resets: u32,
    #[serde(default)]
    daily_loss_latched: bool,
    #[serde(default)]
    first_week_started_ms: i64,
    #[serde(default)]
    first_week_start_equity: f64,
    #[serde(default)]
    first_week_complete_latched: bool,
    #[serde(default)]
    first_week_loss_latched: bool,
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
    cross_section_status: Value,
    /// Scheduled cross-section legs retained while transient public-market
    /// liquidity is rechecked inside the signal execution window.  The custom
    /// decoder migrates the legacy single-candidate object without discarding
    /// an already-running paper state file.
    #[serde(default, deserialize_with = "deserialize_pending_candidates")]
    cross_section_pending: Vec<Candidate>,
    #[serde(default)]
    shock_reversal_status: Value,
    #[serde(default)]
    latest_signal_microstructure: Value,
    #[serde(default)]
    latest_confirmation_microstructure: Value,
    #[serde(default)]
    latest_main_signal_microstructure: Value,
    #[serde(default)]
    latest_main_confirmation_microstructure: Value,
    #[serde(default)]
    latest_pulse_signal_microstructure: Value,
    #[serde(default)]
    latest_pulse_confirmation_microstructure: Value,
    #[serde(default)]
    microstructure_trials: HashMap<String, MicrostructureTrial>,
    #[serde(default)]
    pulse_entries: u64,
    #[serde(default)]
    pulse_exits: u64,
    #[serde(default)]
    pulse_wins: u64,
    #[serde(default)]
    pulse_realized_pnl: f64,
    #[serde(default)]
    cross_entries: u64,
    #[serde(default)]
    cross_exits: u64,
    #[serde(default)]
    cross_wins: u64,
    #[serde(default)]
    cross_realized_pnl: f64,
    #[serde(default)]
    shock_entries: u64,
    #[serde(default)]
    shock_exits: u64,
    #[serde(default)]
    shock_wins: u64,
    #[serde(default)]
    shock_realized_pnl: f64,
}

fn deserialize_pending_candidates<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<Candidate>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Array(items) => items
            .into_iter()
            .map(|item| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Value::Object(_) => serde_json::from_value(value)
            .map(|candidate| vec![candidate])
            .map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom(
            "cross_section_pending must be a candidate, an array, or null",
        )),
    }
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
            pending_entries: HashMap::new(),
            pulse_exhaustion_setups: HashMap::new(),
            pulse_seen_signal: HashMap::new(),
            latest_pulse_exhaustion: Value::Null,
            day: risk_day(now_ms),
            day_start_equity: cash,
            daily_entries: 0,
            daily_entry_bonus: 0,
            daily_risk_resets: 0,
            daily_loss_latched: false,
            first_week_started_ms: 0,
            first_week_start_equity: 0.0,
            first_week_complete_latched: false,
            first_week_loss_latched: false,
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
            cross_section_status: Value::Null,
            cross_section_pending: Vec::new(),
            shock_reversal_status: Value::Null,
            latest_signal_microstructure: Value::Null,
            latest_confirmation_microstructure: Value::Null,
            latest_main_signal_microstructure: Value::Null,
            latest_main_confirmation_microstructure: Value::Null,
            latest_pulse_signal_microstructure: Value::Null,
            latest_pulse_confirmation_microstructure: Value::Null,
            microstructure_trials: HashMap::new(),
            pulse_entries: 0,
            pulse_exits: 0,
            pulse_wins: 0,
            pulse_realized_pnl: 0.0,
            cross_entries: 0,
            cross_exits: 0,
            cross_wins: 0,
            cross_realized_pnl: 0.0,
            shock_entries: 0,
            shock_exits: 0,
            shock_wins: 0,
            shock_realized_pnl: 0.0,
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

fn cross_performance(state: &PersistedState) -> Value {
    json!({
        "entries": state.cross_entries,
        "exits": state.cross_exits,
        "wins": state.cross_wins,
        "win_rate": (state.cross_exits > 0)
            .then_some(state.cross_wins as f64 / state.cross_exits as f64),
        "realized_pnl": state.cross_realized_pnl,
    })
}

fn shock_performance(state: &PersistedState) -> Value {
    json!({
        "entries": state.shock_entries,
        "exits": state.shock_exits,
        "wins": state.shock_wins,
        "win_rate": (state.shock_exits > 0)
            .then_some(state.shock_wins as f64 / state.shock_exits as f64),
        "realized_pnl": state.shock_realized_pnl,
    })
}

fn first_week_progress(
    state: &PersistedState,
    cfg: &AltcoinImpulseConfig,
    current_equity: f64,
    now_ms: i64,
) -> FirstWeekProgress {
    let duration_ms = cfg.first_week_duration_days as i64 * DAY_MS;
    let ends_ms = state.first_week_started_ms.saturating_add(duration_ms);
    let elapsed_ms = now_ms
        .saturating_sub(state.first_week_started_ms)
        .clamp(0, duration_ms);
    let elapsed_days = elapsed_ms as f64 / DAY_MS as f64;
    let current_profit_usdt = current_equity - state.first_week_start_equity;
    let window_complete = state.first_week_complete_latched || now_ms >= ends_ms;
    let loss_limit_reached = cfg.first_week_loss_limit > 0.0
        && (state.first_week_loss_latched
            || (now_ms <= ends_ms
                && current_profit_usdt
                    <= -(state.first_week_start_equity * cfg.first_week_loss_limit)));
    FirstWeekProgress {
        started_ms: state.first_week_started_ms,
        ends_ms,
        elapsed_days,
        start_equity: state.first_week_start_equity,
        current_equity,
        current_profit_usdt,
        loss_limit_pct: cfg.first_week_loss_limit,
        loss_limit_reached,
        realized_average_per_day_usdt: if elapsed_days > 0.0 {
            current_profit_usdt / elapsed_days
        } else {
            0.0
        },
        window_complete,
        entries_blocked: loss_limit_reached,
    }
}

fn update_first_week_latches(
    state: &mut PersistedState,
    cfg: &AltcoinImpulseConfig,
    current_equity: f64,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    let progress = first_week_progress(state, cfg, current_equity, now_ms);
    let mut changed = false;
    if progress.window_complete && !state.first_week_complete_latched {
        state.first_week_complete_latched = true;
        changed = true;
        append_event(
            event_path,
            json!({
                "ts_ms":now_ms,"event":"first_week_window_complete",
                "profit_usdt":progress.current_profit_usdt,
                "entries_halted":false
            }),
        )?;
    }
    if progress.loss_limit_reached && !state.first_week_loss_latched {
        state.first_week_loss_latched = true;
        changed = true;
        append_event(
            event_path,
            json!({
                "ts_ms":now_ms,"event":"first_week_loss_limit_reached",
                "profit_usdt":progress.current_profit_usdt,
                "loss_limit_pct":cfg.first_week_loss_limit,
                "equity":current_equity,
                "entries_halted":true
            }),
        )?;
    }
    Ok(changed)
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

fn is_extreme_direct(candidate: &Candidate, cfg: &AltcoinImpulseConfig) -> bool {
    let directional_1h = candidate.side as f64 * candidate.return_1h;
    let directional_4h = candidate.side as f64 * candidate.return_4h;
    candidate.volume_ratio >= cfg.extreme_direct_volume_ratio
        && (directional_1h >= cfg.extreme_direct_return_1h
            || directional_4h >= cfg.extreme_direct_return_4h)
}

fn is_pulse_position(position: &Position) -> bool {
    position.entry_phase == "pulse_exhaustion_short"
}

fn is_cross_position(position: &Position) -> bool {
    position.entry_phase == "cross_section_momentum"
}

fn is_shock_position(position: &Position) -> bool {
    position.entry_phase == "shock_reversal"
}

fn is_pulse_candidate(candidate: &Candidate) -> bool {
    candidate.entry_phase == "pulse_exhaustion_short"
}

fn is_cross_candidate(candidate: &Candidate) -> bool {
    candidate.entry_phase == "cross_section_momentum"
}

fn is_shock_candidate(candidate: &Candidate) -> bool {
    candidate.entry_phase == "shock_reversal"
}

fn execution_priority(candidate: &Candidate) -> u8 {
    if is_pulse_candidate(candidate) {
        3
    } else if is_shock_candidate(candidate) {
        // Event-driven reversals expire after one closed bar and therefore get
        // the shared regular slot before the scheduled rotation fallback.
        2
    } else if is_cross_candidate(candidate) {
        1
    } else {
        0
    }
}

fn candidate_stop_pct(
    candidate: &Candidate,
    cfg: &AltcoinImpulseConfig,
    cross: &AltcoinCrossSectionConfig,
    shock: &AltcoinShockReversalConfig,
) -> f64 {
    if is_pulse_candidate(candidate) {
        cfg.pulse_stop_pct
    } else if is_cross_candidate(candidate) {
        cross.stop_pct
    } else if is_shock_candidate(candidate) {
        shock.stop_pct
    } else {
        cfg.stop_pct
    }
}

fn position_exit_parameters(
    position: &Position,
    cfg: &AltcoinImpulseConfig,
    cross: &AltcoinCrossSectionConfig,
    shock: &AltcoinShockReversalConfig,
) -> (f64, f64) {
    if is_pulse_position(position) {
        (cfg.pulse_trail_activation_pct, cfg.pulse_trail_pct)
    } else if is_cross_position(position) {
        (cross.trail_activation_pct, cross.trail_pct)
    } else if is_shock_position(position) {
        (shock.trail_activation_pct, shock.trail_pct)
    } else {
        (cfg.trail_activation_pct, cfg.trail_pct)
    }
}

fn position_partial_take_profit_fraction(
    position: &Position,
    cfg: &AltcoinImpulseConfig,
    cross: &AltcoinCrossSectionConfig,
) -> f64 {
    if is_cross_position(position) {
        cross.partial_take_profit_fraction
    } else {
        cfg.partial_take_profit_fraction
    }
}

fn position_max_hold_hours(
    position: &Position,
    cfg: &AltcoinImpulseConfig,
    cross: &AltcoinCrossSectionConfig,
    shock: &AltcoinShockReversalConfig,
) -> i64 {
    if is_cross_position(position) {
        cross.hold_hours as i64
    } else if is_shock_position(position) {
        shock.max_hold_hours as i64
    } else {
        cfg.max_hold_hours as i64
    }
}

fn pulse_exhaustion_runtime(enabled: bool, allow_live: bool, mode: TradeMode) -> (bool, bool) {
    // The first flag owns discovery/outcome labelling; the second owns only
    // exchange execution. Live shadow research must survive a capital pause.
    (enabled, enabled && (mode != TradeMode::Live || allow_live))
}

fn position_cooldown_hours(
    position: &Position,
    cfg: &AltcoinImpulseConfig,
    shock: &AltcoinShockReversalConfig,
) -> u32 {
    if is_shock_position(position) {
        shock.cooldown_hours
    } else {
        cfg.cooldown_hours
    }
}

fn candidate_max_directional_premium(candidate: &Candidate, cfg: &AltcoinImpulseConfig) -> f64 {
    if is_pulse_candidate(candidate) {
        cfg.pulse_max_directional_premium
    } else {
        cfg.max_directional_premium
    }
}

/// Evaluate only the first independently closed 15m bar after the impulse.
/// Returning `None` means that bar is not available yet (or history is insufficient).
fn pulse_exhaustion_candidate(
    setup: &PulseExhaustionSetup,
    bars: &[Bar],
    cfg: &AltcoinImpulseConfig,
) -> Option<(Candidate, f64, f64)> {
    let origin = bars
        .iter()
        .position(|bar| bar.close_ms == setup.origin_signal_ms)?;
    let i = origin.checked_add(1)?;
    if i >= bars.len() || i < 16 {
        return None;
    }
    let bar = &bars[i];
    let return_1h = bar.close / bars[i - 4].close - 1.0;
    let return_4h = bar.close / bars[i - 16].close - 1.0;
    let peak = bars[origin].high.max(bar.high);
    let peak_retrace = 1.0 - bar.close / peak.max(f64::EPSILON);
    let range = bar.high - bar.low;
    let close_location = if range > 0.0 {
        (bar.close - bar.low) / range
    } else {
        0.5
    };
    Some((
        Candidate {
            symbol: setup.symbol.clone(),
            signal_ms: bar.close_ms,
            setup_origin_ms: setup.origin_signal_ms,
            side: -1,
            price: bar.close,
            return_1h,
            return_4h,
            volume_ratio: setup.initial_volume_ratio,
            efficiency: 1.0,
            close_location,
            volume_24h: setup.volume_24h,
            score: peak_retrace * setup.initial_volume_ratio.ln_1p(),
            entry_phase: "pulse_exhaustion_short".to_owned(),
            breakout_level: setup.initial_price,
            entry_trigger: "first_15m_leverage_exhaustion".to_owned(),
            risk_scale: cfg.pulse_risk_scale,
            blockers: Vec::new(),
            spot_return_1h: None,
            oi_change_1h: None,
            funding_rate: None,
            perp_premium: None,
        },
        peak_retrace,
        close_location,
    ))
}

fn apply_pulse_exhaustion_gates(
    candidate: &mut Candidate,
    peak_retrace: f64,
    cfg: &AltcoinImpulseConfig,
) -> Value {
    let spot_perp_ratio = candidate
        .spot_return_1h
        .filter(|spot| *spot >= 0.0 && candidate.return_1h > 0.0)
        .map(|spot| spot / candidate.return_1h);
    let perp_ok = candidate.return_1h >= cfg.pulse_perp_return_1h;
    let oi_ok = candidate
        .oi_change_1h
        .is_some_and(|oi| oi >= cfg.pulse_oi_change_1h);
    let spot_ok = spot_perp_ratio.is_some_and(|ratio| ratio <= cfg.pulse_max_spot_perp_ratio);
    let retrace_ok = peak_retrace >= cfg.pulse_min_peak_retrace;
    let close_ok = candidate.close_location <= cfg.pulse_max_close_location;
    if !perp_ok {
        candidate.blockers.push(format!(
            "衰竭确认时永续 1h 涨幅 {:.1}% < {:.1}%",
            candidate.return_1h * 100.0,
            cfg.pulse_perp_return_1h * 100.0
        ));
    }
    if !oi_ok {
        candidate.blockers.push(match candidate.oi_change_1h {
            Some(oi) => format!(
                "OI 1h 增幅 {:.1}% < {:.1}%",
                oi * 100.0,
                cfg.pulse_oi_change_1h * 100.0
            ),
            None => "OI 1h 数据不可用".to_owned(),
        });
    }
    if !spot_ok {
        candidate.blockers.push(match spot_perp_ratio {
            Some(ratio) => format!(
                "现货/永续 1h 涨幅比 {:.2} > {:.2}",
                ratio, cfg.pulse_max_spot_perp_ratio
            ),
            None => "现货 1h 数据不可用或方向不一致".to_owned(),
        });
    }
    if !retrace_ok {
        candidate.blockers.push(format!(
            "距脉冲峰值仅回落 {:.1}% < {:.1}%",
            peak_retrace * 100.0,
            cfg.pulse_min_peak_retrace * 100.0
        ));
    }
    if !close_ok {
        candidate.blockers.push(format!(
            "确认 K 收盘位置 {:.0}% > {:.0}%",
            candidate.close_location * 100.0,
            cfg.pulse_max_close_location * 100.0
        ));
    }
    json!({
        "perp_return_1h": candidate.return_1h,
        "spot_return_1h": candidate.spot_return_1h,
        "spot_perp_ratio": spot_perp_ratio,
        "oi_change_1h": candidate.oi_change_1h,
        "peak_retrace": peak_retrace,
        "close_location": candidate.close_location,
        "gates": {
            "perp_momentum": perp_ok,
            "oi_expansion": oi_ok,
            "spot_lag": spot_ok,
            "peak_retrace": retrace_ok,
            "weak_close": close_ok
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingDecision {
    Waiting,
    RetestSeen,
    Confirmed,
    Invalidated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntrabarPendingDecision {
    Waiting,
    Touched,
    Confirmed,
    Invalidated,
}

fn intrabar_pending_decision(
    side: i32,
    anchor: f64,
    price: f64,
    touch_seen: bool,
    extreme_price: Option<f64>,
    last_price: Option<f64>,
    cfg: &AltcoinImpulseConfig,
) -> IntrabarPendingDecision {
    let holds = if side > 0 {
        price >= anchor * (1.0 - cfg.vertical_retest_invalidation_pct)
    } else {
        price <= anchor * (1.0 + cfg.vertical_retest_invalidation_pct)
    };
    if !holds {
        return IntrabarPendingDecision::Invalidated;
    }
    let touch = if side > 0 {
        price <= anchor * (1.0 - cfg.intrabar_min_pullback_pct)
    } else {
        price >= anchor * (1.0 + cfg.intrabar_min_pullback_pct)
    };
    if !touch_seen {
        return if touch {
            IntrabarPendingDecision::Touched
        } else {
            IntrabarPendingDecision::Waiting
        };
    }
    let extreme = extreme_price.unwrap_or(price);
    let directional_tick = last_price.is_some_and(|previous| {
        if side > 0 {
            price > previous
        } else {
            price < previous
        }
    });
    let rebound = if side > 0 {
        price >= extreme * (1.0 + cfg.intrabar_rebound_pct)
    } else {
        price <= extreme * (1.0 - cfg.intrabar_rebound_pct)
    };
    let reclaimed = if side > 0 {
        price >= anchor * (1.0 + cfg.vertical_reclaim_pct)
            && price <= anchor * (1.0 + cfg.vertical_max_entry_extension_pct)
    } else {
        price <= anchor * (1.0 - cfg.vertical_reclaim_pct)
            && price >= anchor * (1.0 - cfg.vertical_max_entry_extension_pct)
    };
    if directional_tick && rebound && reclaimed {
        IntrabarPendingDecision::Confirmed
    } else {
        IntrabarPendingDecision::Waiting
    }
}

fn pending_decision(
    side: i32,
    anchor: f64,
    confirmation_mode: &str,
    bar: &Bar,
    retest_seen: bool,
    cfg: &AltcoinImpulseConfig,
) -> PendingDecision {
    let vertical = confirmation_mode == "vertical_impulse_retest";
    let touch_pct = if vertical {
        cfg.vertical_retest_touch_pct
    } else {
        cfg.retest_touch_pct
    };
    let invalidation_pct = if vertical {
        cfg.vertical_retest_invalidation_pct
    } else {
        cfg.retest_invalidation_pct
    };
    let reclaim_pct = if vertical {
        cfg.vertical_reclaim_pct
    } else {
        cfg.reclaim_pct
    };
    let touch = if side > 0 {
        bar.low <= anchor * (1.0 + touch_pct)
    } else {
        bar.high >= anchor * (1.0 - touch_pct)
    };
    let holds = if side > 0 {
        bar.low >= anchor * (1.0 - invalidation_pct)
    } else {
        bar.high <= anchor * (1.0 + invalidation_pct)
    };
    if !holds {
        return PendingDecision::Invalidated;
    }
    // 首次触及只负责把状态推进到 RetestSeen。重新启动必须发生在随后闭合的
    // 另一根 K 线上，避免用同一根 K 的 high/low/close 臆测盘中先后顺序。
    if !retest_seen {
        return if touch {
            PendingDecision::RetestSeen
        } else {
            PendingDecision::Waiting
        };
    }
    let mut reclaimed = if side > 0 {
        bar.close >= anchor * (1.0 + reclaim_pct) && bar.close > bar.open
    } else {
        bar.close <= anchor * (1.0 - reclaim_pct) && bar.close < bar.open
    };
    // A shallow pullback is not permission to chase a second vertical leg.
    // If the independent confirmation bar has already run too far away from
    // the signal-price anchor, keep waiting rather than entering at the top.
    if vertical {
        reclaimed &= if side > 0 {
            bar.close <= anchor * (1.0 + cfg.vertical_max_entry_extension_pct)
        } else {
            bar.close >= anchor * (1.0 - cfg.vertical_max_entry_extension_pct)
        };
    }
    if reclaimed {
        PendingDecision::Confirmed
    } else if touch {
        PendingDecision::RetestSeen
    } else {
        PendingDecision::Waiting
    }
}

fn confirmation_plan(candidate: &Candidate, cfg: &AltcoinImpulseConfig) -> (f64, String) {
    let overshoot = if candidate.breakout_level > 0.0 {
        candidate.side as f64 * (candidate.price / candidate.breakout_level - 1.0)
    } else {
        0.0
    };
    if cfg.adaptive_retest_enabled && overshoot >= cfg.vertical_overshoot_pct {
        (candidate.price, "vertical_impulse_retest".to_owned())
    } else {
        (candidate.breakout_level, default_confirmation_mode())
    }
}

async fn advance_intrabar_pending_entries(
    state: &mut PersistedState,
    client: &live::RestClient,
    cfg: &AltcoinImpulseConfig,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    if !cfg.intrabar_vertical_retest_enabled {
        return Ok(false);
    }
    let symbols: Vec<String> = state
        .pending_entries
        .iter()
        .filter(|(_, pending)| {
            pending.confirmation_mode == "vertical_impulse_retest"
                && pending.intrabar_confirmed_ms.is_none()
        })
        .map(|(symbol, _)| symbol.clone())
        .collect();
    let mut rescan_now = false;
    for symbol in symbols {
        let price = match client.mark_price(&symbol).await {
            Ok(price) if price.is_finite() && price > 0.0 => price,
            Ok(_) => continue,
            Err(error) => {
                warn!(symbol=%symbol, error=%error, "盘中浅回踩标记价读取失败");
                continue;
            }
        };
        let Some(mut pending) = state.pending_entries.get(&symbol).cloned() else {
            continue;
        };
        let anchor = if pending.retest_anchor > 0.0 {
            pending.retest_anchor
        } else {
            pending.candidate.price
        };
        let decision = intrabar_pending_decision(
            pending.candidate.side,
            anchor,
            price,
            pending.intrabar_touch_seen,
            pending.intrabar_extreme_price,
            pending.intrabar_last_price,
            cfg,
        );
        pending.intrabar_extreme_price = Some(match pending.intrabar_extreme_price {
            Some(extreme) if pending.candidate.side > 0 => extreme.min(price),
            Some(extreme) => extreme.max(price),
            None => price,
        });
        pending.intrabar_last_price = Some(price);
        match decision {
            IntrabarPendingDecision::Invalidated => {
                state.pending_entries.remove(&symbol);
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"entry_setup_invalidated","symbol":symbol,"side":pending.candidate.side,"origin_signal_ms":pending.candidate.signal_ms,"confirmation_mode":pending.confirmation_mode,"retest_anchor":anchor,"observed_price":price,"source":"realtime_mark_price","reason":"盘中价格击穿垂直脉冲浅回踩失效线"}),
                )?;
                rescan_now = true;
            }
            IntrabarPendingDecision::Touched => {
                pending.intrabar_touch_seen = true;
                pending.intrabar_touch_ms = Some(now_ms);
                pending.intrabar_extreme_price = Some(price);
                state.pending_entries.insert(symbol.clone(), pending);
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"entry_setup_intrabar_touch","symbol":symbol,"side":state.pending_entries[&symbol].candidate.side,"retest_anchor":anchor,"touch_price":price,"source":"realtime_mark_price","reason":"盘中首次触及垂直脉冲浅回踩区，等待后续实时收回"}),
                )?;
                rescan_now = true;
            }
            IntrabarPendingDecision::Confirmed => {
                pending.intrabar_confirmed_ms = Some(now_ms);
                pending.intrabar_confirmed_price = Some(price);
                state.pending_entries.insert(symbol.clone(), pending);
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"entry_setup_intrabar_confirmed","symbol":symbol,"side":state.pending_entries[&symbol].candidate.side,"retest_anchor":anchor,"touch_ms":state.pending_entries[&symbol].intrabar_touch_ms,"extreme_price":state.pending_entries[&symbol].intrabar_extreme_price,"confirmation_price":price,"rebound_pct":cfg.intrabar_rebound_pct,"max_entry_extension_pct":cfg.vertical_max_entry_extension_pct,"source":"realtime_mark_price","reason":"盘中浅回踩后按时间顺序重新收回，转交完整执行门槛"}),
                )?;
                rescan_now = true;
            }
            IntrabarPendingDecision::Waiting => {
                state.pending_entries.insert(symbol, pending);
            }
        }
    }
    Ok(rescan_now)
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

/// Prevent independent alpha sleeves from immediately reversing one another
/// on the same contract. Same-direction retries remain allowed: a stopped
/// exhaustion entry can still re-arm if a later independent event confirms,
/// but a rotation long cannot instantly fade that short (or vice versa).
fn recently_exited_opposite_side(
    trades: &[Value],
    symbol: &str,
    candidate_side: i32,
    now_ms: i64,
    lookback_hours: u32,
) -> bool {
    let cutoff = now_ms.saturating_sub(lookback_hours as i64 * 3_600_000);
    trades.iter().rev().any(|event| {
        matches!(event["event"].as_str(), Some("exit" | "exit_detected"))
            && event["symbol"] == symbol
            && event["side"]
                .as_i64()
                .is_some_and(|side| side != candidate_side as i64)
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
        setup_origin_ms: bars[i].close_ms,
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
        breakout_level: if side > 0 { prior_high } else { prior_low },
        entry_trigger: default_entry_trigger(),
        risk_scale: 1.0,
        blockers,
        spot_return_1h: None,
        oi_change_1h: None,
        funding_rate: None,
        perp_premium: None,
    })
}

/// Build the arming observation for the independent leverage-exhaustion sleeve.
///
/// This deliberately does not reuse `evaluate`: the main strategy's 24h breakout,
/// 4h return band, path efficiency and close-location rules describe continuation
/// entries, not a leveraged pulse that may later become a short after exhaustion.
fn evaluate_pulse_impulse(
    symbol: String,
    bars: &[Bar],
    cfg: &AltcoinImpulseConfig,
) -> Option<Candidate> {
    if bars.len() < 7 * 96 + 17 {
        return None;
    }
    let i = bars.len() - 1;
    let close = bars[i].close;
    let return_1h = close / bars[i - 4].close - 1.0;
    let return_4h = close / bars[i - 16].close - 1.0;
    let hour_volume = |end: usize| {
        bars[end - 3..=end]
            .iter()
            .map(|bar| bar.quote_volume)
            .sum::<f64>()
    };
    let current_hour_volume = hour_volume(i);
    let historical: Vec<f64> = (i - 7 * 96..i)
        .filter(|&end| end >= 3)
        .map(hour_volume)
        .collect();
    let volume_ratio = current_hour_volume / median(historical).max(1.0);
    let volume_24h = bars[i - 95..=i]
        .iter()
        .map(|bar| bar.quote_volume)
        .sum::<f64>();
    let spread = bars[i].high - bars[i].low;
    let close_location = if spread > 0.0 {
        (close - bars[i].low) / spread
    } else {
        0.5
    };
    let mut blockers = Vec::new();
    if return_1h < cfg.pulse_initial_return_1h {
        blockers.push(format!(
            "独立脉冲 1h 涨幅 {:.1}% < {:.1}%",
            return_1h * 100.0,
            cfg.pulse_initial_return_1h * 100.0
        ));
    }
    if volume_ratio < cfg.pulse_initial_volume_ratio {
        blockers.push(format!(
            "独立脉冲量比 {:.1}x < {:.1}x",
            volume_ratio, cfg.pulse_initial_volume_ratio
        ));
    }
    if volume_24h < cfg.min_24h_volume_usd {
        blockers.push("24h 成交额不足".into());
    }
    let progress = (return_1h / cfg.pulse_initial_return_1h).clamp(0.0, 1.0)
        + (volume_ratio / cfg.pulse_initial_volume_ratio).clamp(0.0, 1.0);
    Some(Candidate {
        symbol,
        signal_ms: bars[i].close_ms,
        setup_origin_ms: bars[i].close_ms,
        side: 1,
        price: close,
        score: progress * (volume_24h / 1_000_000.0).ln_1p(),
        return_1h,
        return_4h,
        volume_ratio,
        efficiency: 1.0,
        close_location,
        volume_24h,
        entry_phase: "pulse_impulse".to_owned(),
        breakout_level: close,
        entry_trigger: "independent_leverage_pulse".to_owned(),
        risk_scale: cfg.pulse_risk_scale,
        blockers,
        spot_return_1h: None,
        oi_change_1h: None,
        funding_rate: None,
        perp_premium: None,
    })
}

async fn enrich_candidate(http: reqwest::Client, mut candidate: Candidate) -> Candidate {
    let spot_url = format!(
        "{SPOT_BASE}/api/v3/klines?symbol={}&interval=15m&limit=5&endTime={}",
        candidate.symbol, candidate.signal_ms
    );
    let oi_url = format!(
        "{FUTURES_BASE}/futures/data/openInterestHist?symbol={}&period=15m&limit=5&endTime={}",
        candidate.symbol, candidate.signal_ms
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
    let candidates = public_url_candidates(url);
    for (attempt, candidate_url) in candidates.iter().enumerate() {
        let request = async {
            live::acquire_binance_request(live::binance_request_weight(candidate_url), false).await;
            let response = http.get(candidate_url).send().await?;
            let status = response.status();
            let headers = response.headers().clone();
            live::observe_binance_response(&headers, status).await;
            let response = response.error_for_status()?;
            let text = response.text().await?;
            Ok::<Value, anyhow::Error>(serde_json::from_str(&text)?)
        };
        match tokio::time::timeout(Duration::from_secs(4), request).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => last_error = Some(anyhow::anyhow!("公共行情端点 4 秒超时")),
        }
        if attempt + 1 < candidates.len() {
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("公共行情请求失败")))
        .with_context(|| url.to_owned())
}

fn public_url_candidates(url: &str) -> Vec<String> {
    if url.starts_with(FUTURES_BASE) {
        // 部分云厂商到 fapi.binance.com 会间歇性黑洞。优先切换 Binance
        // 官方同源入口；组合目前只开放模拟盘，最后以执行测试网公共行情兜底。
        return [
            FUTURES_BASE,
            "https://fapi1.binance.com",
            "https://testnet.binancefuture.com",
        ]
        .into_iter()
        .map(|base| url.replacen(FUTURES_BASE, base, 1))
        .collect();
    }
    if url.starts_with(SPOT_BASE) {
        return [
            SPOT_BASE,
            "https://api1.binance.com",
            "https://api2.binance.com",
        ]
        .into_iter()
        .map(|base| url.replacen(SPOT_BASE, base, 1))
        .collect();
    }
    vec![url.to_owned()]
}

async fn fetch_bars(
    http: reqwest::Client,
    symbol: String,
    history_bars: usize,
) -> Result<(String, Vec<Bar>)> {
    let now = chrono::Utc::now().timestamp_millis();
    let recent_limit = history_bars.min(1_000);
    let url =
        format!("{FUTURES_BASE}/fapi/v1/klines?symbol={symbol}&interval=15m&limit={recent_limit}");
    let recent = get_json(&http, &url).await?;
    let mut rows = recent.as_array().context("K 线响应不是数组")?.clone();
    if history_bars > recent_limit {
        let older_limit = (history_bars - recent_limit).min(500);
        let earliest = rows
            .first()
            .and_then(|row| row.get(0))
            .and_then(Value::as_i64)
            .context("K 线缺 openTime")?;
        let older_url = format!(
            "{FUTURES_BASE}/fapi/v1/klines?symbol={symbol}&interval=15m&limit={older_limit}&endTime={}",
            earliest - 1
        );
        let mut older = get_json(&http, &older_url)
            .await?
            .as_array()
            .context("较早 K 线响应不是数组")?
            .clone();
        older.append(&mut rows);
        rows = older;
    }
    let bars = parse_bars(Value::Array(rows), now)?;
    Ok((symbol, bars))
}

#[derive(Debug, Clone)]
struct CrossRank {
    symbol: String,
    trailing_return: f64,
    volume_24h: f64,
    signal_index: usize,
}

fn cross_ranks_at(
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    boundary_ms: i64,
    cfg: &AltcoinCrossSectionConfig,
) -> Vec<CrossRank> {
    let formation_bars = cfg.formation_hours * 4;
    let mut ranks = Vec::new();
    for (symbol, bars) in bars_by_symbol {
        let entry_index = bars.partition_point(|bar| bar.open_ms < boundary_ms);
        if entry_index == 0 || entry_index > bars.len() {
            continue;
        }
        let signal_index = entry_index - 1;
        if boundary_ms.saturating_sub(bars[signal_index].close_ms) > 2_000
            || signal_index < formation_bars.max(95)
        {
            continue;
        }
        let back = signal_index - formation_bars;
        if bars[signal_index].open_ms - bars[back].open_ms
            > (formation_bars as i64 + 1) * 15 * 60_000
        {
            continue;
        }
        let volume_24h: f64 = bars[signal_index - 95..=signal_index]
            .iter()
            .map(|bar| bar.quote_volume)
            .sum();
        if volume_24h < cfg.min_24h_volume_usd || bars[back].close <= 0.0 {
            continue;
        }
        let trailing_return = bars[signal_index].close / bars[back].close - 1.0;
        if trailing_return.abs() > 1.5 {
            continue;
        }
        ranks.push(CrossRank {
            symbol: symbol.clone(),
            trailing_return,
            volume_24h,
            signal_index,
        });
    }
    ranks.sort_by(|left, right| left.trailing_return.total_cmp(&right.trailing_return));
    ranks
}

fn cross_selected(
    ranks: &[CrossRank],
    names: usize,
    market_threshold: f64,
) -> Option<Vec<(CrossRank, i32)>> {
    if ranks.len() < names {
        return None;
    }
    let market_return = ranks[ranks.len() / 2].trailing_return;
    let (selected, side) = if market_return >= market_threshold {
        (
            ranks.iter().rev().take(names).cloned().collect::<Vec<_>>(),
            1,
        )
    } else {
        (ranks.iter().take(names).cloned().collect::<Vec<_>>(), -1)
    };
    if selected.iter().any(|item| {
        (side > 0 && item.trailing_return <= 0.0) || (side < 0 && item.trailing_return >= 0.0)
    }) {
        return None;
    }
    Some(selected.into_iter().map(|item| (item, side)).collect())
}

fn cross_execution_pool(
    ranks: &[CrossRank],
    selected: &[(CrossRank, i32)],
    names: usize,
) -> Vec<(CrossRank, i32, bool)> {
    let Some((_, side)) = selected.first() else {
        return Vec::new();
    };
    let primary_symbols = selected
        .iter()
        .map(|(rank, _)| rank.symbol.as_str())
        .collect::<HashSet<_>>();
    let reserve_limit = names.saturating_mul(2);
    let reserves = if *side > 0 {
        ranks
            .iter()
            .rev()
            .filter(|rank| {
                rank.trailing_return > 0.0 && !primary_symbols.contains(rank.symbol.as_str())
            })
            .take(reserve_limit)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        ranks
            .iter()
            .filter(|rank| {
                rank.trailing_return < 0.0 && !primary_symbols.contains(rank.symbol.as_str())
            })
            .take(reserve_limit)
            .cloned()
            .collect::<Vec<_>>()
    };
    selected
        .iter()
        .cloned()
        .map(|(rank, side)| (rank, side, false))
        .chain(reserves.into_iter().map(|rank| (rank, *side, true)))
        .collect()
}

fn cross_leg_return(
    bars: &[Bar],
    signal_index: usize,
    side: i32,
    hold_hours: usize,
    stop_pct: f64,
    cost_bps_per_side: f64,
) -> Option<f64> {
    let entry_index = signal_index + 1;
    let exit_index = entry_index + hold_hours * 4;
    let entry = bars.get(entry_index)?.open;
    let final_index = exit_index.min(bars.len());
    let mut exit = bars.get(exit_index).map(|bar| bar.open).or_else(|| {
        (exit_index == bars.len())
            .then(|| bars.last().map(|bar| bar.close))
            .flatten()
    })?;
    let stop = entry * (1.0 - side as f64 * stop_pct);
    for bar in &bars[entry_index..final_index] {
        let hit = if side > 0 {
            bar.low <= stop
        } else {
            bar.high >= stop
        };
        if hit {
            let gapped = if side > 0 {
                bar.open < stop
            } else {
                bar.open > stop
            };
            exit = if gapped { bar.open } else { stop };
            break;
        }
    }
    let cost = 2.0 * cost_bps_per_side / 10_000.0;
    Some(side as f64 * (exit / entry - 1.0) - cost)
}

fn cross_section_analysis(
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    boundary_ms: i64,
    cross: &AltcoinCrossSectionConfig,
    impulse: &AltcoinImpulseConfig,
    current_exclusions: &HashSet<String>,
) -> (Vec<Candidate>, Value) {
    let min_universe_size = cross.names;
    let mut history = Vec::new();
    let mut shadow_baskets = Vec::new();
    // A scheduled boundary can occasionally have no directional extreme or
    // insufficient point-in-time liquidity. Look back farther and retain the
    // latest N *completed valid signals*, matching the causal portfolio gate.
    for offset in (1..=cross.gate_window * 2).rev() {
        let entry_ms = boundary_ms - offset as i64 * cross.hold_hours as i64 * 3_600_000;
        let ranks = cross_ranks_at(bars_by_symbol, entry_ms, cross);
        if ranks.len() < min_universe_size {
            continue;
        }
        let Some(selected) = cross_selected(&ranks, cross.names, cross.market_momentum_threshold)
        else {
            continue;
        };
        let returns: Option<Vec<f64>> = selected
            .iter()
            .map(|(rank, side)| {
                cross_leg_return(
                    &bars_by_symbol[&rank.symbol],
                    rank.signal_index,
                    *side,
                    cross.hold_hours,
                    cross.stop_pct,
                    cross.assumed_cost_bps_per_side,
                )
            })
            .collect();
        if let Some(returns) = returns {
            let basket_return = returns.iter().sum::<f64>() / returns.len() as f64;
            let legs: Vec<Value> = selected
                .iter()
                .zip(&returns)
                .map(|((rank, side), modeled_return)| {
                    json!({"symbol":rank.symbol,"side":side,"return_12h":rank.trailing_return,"volume_24h":rank.volume_24h,"modeled_return":modeled_return})
                })
                .collect();
            history.push(basket_return);
            shadow_baskets.push(json!({"entry_ms":entry_ms,"exit_ms":entry_ms+cross.hold_hours as i64*3_600_000,"basket_return":basket_return,"legs":legs}));
        }
    }
    if history.len() > cross.gate_window {
        let excess = history.len() - cross.gate_window;
        history.drain(..excess);
        shadow_baskets.drain(..excess);
    }
    let gains: f64 = history.iter().copied().filter(|value| *value > 0.0).sum();
    let losses: f64 = history
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .map(f64::abs)
        .sum();
    let profit_factor = if losses > 0.0 { gains / losses } else { 99.0 };
    let gate_ready = history.len() == cross.gate_window;
    let high_conviction = gate_ready
        && history.iter().sum::<f64>() > 0.0
        && profit_factor >= cross.gate_min_profit_factor;
    let mut ranks = cross_ranks_at(bars_by_symbol, boundary_ms, cross);
    ranks.retain(|rank| !current_exclusions.contains(&rank.symbol));
    let universe_ready = ranks.len() >= min_universe_size;
    let selected = universe_ready
        .then(|| cross_selected(&ranks, cross.names, cross.market_momentum_threshold))
        .flatten()
        .unwrap_or_default();
    let market_return = ranks.get(ranks.len() / 2).map(|rank| rank.trailing_return);
    let signal_excess_return = selected
        .first()
        .zip(market_return)
        .map(|((rank, _), market)| (rank.trailing_return - market).abs());
    let strong_signal = high_conviction
        && signal_excess_return
            .map(|value| value >= cross.strong_excess_return)
            .unwrap_or(false);
    let gross_multiple = if strong_signal {
        cross.strong_gross_multiple
    } else if high_conviction {
        cross.active_gross_multiple
    } else {
        cross.base_gross_multiple
    };
    let signal_ms = boundary_ms - 1;
    let execution_pool = cross_execution_pool(&ranks, &selected, cross.names);
    let candidates = if universe_ready {
        execution_pool
            .iter()
            .map(|(rank, side, reserve)| Candidate {
                symbol: rank.symbol.clone(),
                signal_ms,
                setup_origin_ms: signal_ms,
                side: *side,
                price: bars_by_symbol[&rank.symbol][rank.signal_index].close,
                return_1h: rank.trailing_return,
                return_4h: rank.trailing_return,
                volume_ratio: 1.0,
                efficiency: 1.0,
                close_location: 0.5,
                volume_24h: rank.volume_24h,
                score: rank.trailing_return.abs(),
                entry_phase: "cross_section_momentum".to_owned(),
                breakout_level: 0.0,
                entry_trigger: if *reserve {
                    "scheduled_cross_section_momentum_reserve"
                } else {
                    "scheduled_cross_section_momentum"
                }
                .to_owned(),
                // `gross_multiple` is a basket budget.  Every selected leg
                // receives an equal share so increasing N diversifies the
                // cross-section instead of multiplying account leverage.
                risk_scale: gross_multiple / cross.names as f64,
                blockers: Vec::new(),
                spot_return_1h: None,
                oi_change_1h: None,
                funding_rate: None,
                perp_premium: None,
            })
            .collect()
    } else {
        Vec::new()
    };
    let selected_json: Vec<Value> = selected
        .iter()
        .map(|(rank, side)| {
            json!({"symbol":rank.symbol,"side":side,"return_12h":rank.trailing_return,"volume_24h":rank.volume_24h})
        })
        .collect();
    let reserve_json: Vec<Value> = execution_pool
        .iter()
        .filter(|(_, _, reserve)| *reserve)
        .map(|(rank, side, _)| {
            json!({"symbol":rank.symbol,"side":side,"return_12h":rank.trailing_return,"volume_24h":rank.volume_24h})
        })
        .collect();
    let selected_direction = selected.first().map(|(_, side)| {
        if *side > 0 {
            "long_strongest"
        } else {
            "short_weakest"
        }
    });
    let market_regime = market_return.map(|value| {
        if value >= cross.market_momentum_threshold {
            "broad_up"
        } else if value <= -cross.market_momentum_threshold {
            "broad_down_short"
        } else {
            "neutral_short_bias"
        }
    });
    let ranked_extremes: Vec<Value> = ranks
        .iter()
        .take(10)
        .chain(ranks.iter().rev().take(10))
        .map(|rank| json!({"symbol":rank.symbol,"return_12h":rank.trailing_return,"volume_24h":rank.volume_24h}))
        .collect();
    let mut reentry_exclusions = current_exclusions.iter().cloned().collect::<Vec<_>>();
    reentry_exclusions.sort();
    let next_boundary_ms = boundary_ms + cross.hold_hours as i64 * 3_600_000;
    let status = json!({
        "model":"12h_cross_section_momentum",
        "stage": if !universe_ready {"ranking_incomplete"} else if selected_json.len() < cross.names {"directional_extreme_missing"} else {"ready_to_execute"},
        "boundary_ms":boundary_ms,
        "next_boundary_ms":next_boundary_ms,
        "formation_hours":cross.formation_hours,
        "hold_hours":cross.hold_hours,
        "universe_count":ranks.len(),
        "min_universe_size":min_universe_size,
        "market_return":market_return,
        "market_threshold":cross.market_momentum_threshold,
        "direction":selected_direction,
        "market_regime":market_regime,
        "signal_excess_return":signal_excess_return,
        "strong_signal":strong_signal,
        "universe_rule":format!("同名现货与 USDT 永续、24h 成交额达标；每 3 小时等权执行截面最强或最弱的 {} 币",cross.names),
        "selected":selected_json,
        "execution_reserves":reserve_json,
        "reentry_exclusions":reentry_exclusions,
        "ranked_extremes":ranked_extremes,
        "gate":{"ready":gate_ready,"open":high_conviction,"samples":history.len(),"required_samples":cross.gate_window,"sum_return":history.iter().sum::<f64>(),"profit_factor":profit_factor,"required_profit_factor":cross.gate_min_profit_factor,"returns":history,"baskets":shadow_baskets},
        "execution":{"legs_required":cross.names,"reserve_count":reserve_json.len(),"gross_multiple":gross_multiple,"gross_per_leg":gross_multiple/cross.names as f64,"allocation":"equal_weight_shared_gross","base_gross_multiple":cross.base_gross_multiple,"active_gross_multiple":cross.active_gross_multiple,"strong_excess_return":cross.strong_excess_return,"strong_gross_multiple":cross.strong_gross_multiple,"stop_pct":cross.stop_pct,"trail_activation_pct":cross.trail_activation_pct,"trail_pct":cross.trail_pct,"partial_take_profit_fraction":cross.partial_take_profit_fraction,"rebalance_hours":cross.hold_hours,"same_signal_carry":true,"daily_loss_limit":impulse.daily_loss_limit}
    });
    (candidates, status)
}

fn cross_section_status_complete(status: &Value) -> bool {
    status.get("gate").is_some_and(Value::is_object)
        && status.get("execution").is_some_and(|execution| {
            execution.is_object()
                && execution
                    .get("gross_multiple")
                    .and_then(Value::as_f64)
                    .is_some()
                && execution.get("stop_pct").and_then(Value::as_f64).is_some()
                && execution
                    .get("rebalance_hours")
                    .and_then(Value::as_u64)
                    .is_some()
                && execution
                    .get("trail_activation_pct")
                    .and_then(Value::as_f64)
                    .is_some()
                && execution.get("trail_pct").and_then(Value::as_f64).is_some()
        })
}

fn cross_protective_exits_since(
    state: &PersistedState,
    since_ms: i64,
) -> (HashSet<String>, Option<i64>) {
    let exits = state.recent_trades.iter().filter(|trade| {
        trade["entry_phase"] == "cross_section_momentum"
            && matches!(trade["event"].as_str(), Some("exit" | "exit_detected"))
            && trade["ts_ms"].as_i64().is_some_and(|ts| ts >= since_ms)
            && !matches!(
                trade["reason"].as_str(),
                Some("cross_section_rotation" | "cross_section_no_candidate")
            )
    });
    let mut symbols = HashSet::new();
    let mut latest = None;
    for trade in exits {
        if let Some(symbol) = trade["symbol"].as_str() {
            symbols.insert(symbol.to_owned());
        }
        if let Some(ts_ms) = trade["ts_ms"].as_i64() {
            latest = Some(latest.map_or(ts_ms, |current: i64| current.max(ts_ms)));
        }
    }
    (symbols, latest)
}

#[derive(Debug, Clone)]
struct ShockShadowOutcome {
    symbol: String,
    side: i32,
    signal_ms: i64,
    exit_ms: i64,
    net_return: f64,
}

fn shock_reversal_candidate_at(
    symbol: &str,
    bars: &[Bar],
    index: usize,
    shock: &AltcoinShockReversalConfig,
    min_24h_volume_usd: f64,
) -> Option<Candidate> {
    const VOLUME_BASELINE_BARS: usize = 7 * 96;
    if index < VOLUME_BASELINE_BARS.max(96).max(8) || index >= bars.len() {
        return None;
    }
    if bars[index].open_ms - bars[index - VOLUME_BASELINE_BARS].open_ms
        > (VOLUME_BASELINE_BARS as i64 + 1) * 15 * 60_000
    {
        return None;
    }
    let volume_24h: f64 = bars[index - 95..=index]
        .iter()
        .map(|bar| bar.quote_volume)
        .sum();
    if volume_24h < min_24h_volume_usd {
        return None;
    }
    let baseline_volume: f64 = bars[index - VOLUME_BASELINE_BARS..index]
        .iter()
        .map(|bar| bar.quote_volume)
        .sum::<f64>()
        / VOLUME_BASELINE_BARS as f64;
    let volume_ratio = bars[index].quote_volume / baseline_volume.max(1.0);
    let prior_hour_return = bars[index - 1].close / bars[index - 5].close - 1.0;
    let side = if prior_hour_return > 0.0 { -1 } else { 1 };
    let reversal_return = bars[index].close / bars[index - 1].close - 1.0;
    let prior_high = bars[index - 8..index]
        .iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let prior_low = bars[index - 8..index]
        .iter()
        .map(|bar| bar.low)
        .fold(f64::INFINITY, f64::min);
    let swept = if side < 0 {
        bars[index].high >= prior_high
    } else {
        bars[index].low <= prior_low
    };
    let range = bars[index].high - bars[index].low;
    let close_location_long = if range > 0.0 {
        (bars[index].close - bars[index].low) / range
    } else {
        0.5
    };
    let directional_close_location = if side > 0 {
        close_location_long
    } else {
        1.0 - close_location_long
    };
    let candle_range = range / bars[index].close.max(f64::EPSILON);
    if !(shock.min_shock_return..=shock.max_shock_return).contains(&prior_hour_return.abs())
        || side as f64 * reversal_return < shock.min_reversal_return
        || !swept
        || volume_ratio < shock.min_volume_ratio
        || directional_close_location < shock.min_close_location
        || candle_range > shock.max_candle_range
    {
        return None;
    }
    let strict = volume_ratio >= shock.strict_volume_ratio
        && directional_close_location >= shock.strict_close_location
        && candle_range <= shock.strict_max_candle_range;
    let return_4h = bars[index].close / bars[index - 16].close - 1.0;
    Some(Candidate {
        symbol: symbol.to_owned(),
        signal_ms: bars[index].close_ms,
        setup_origin_ms: bars[index].close_ms,
        side,
        price: bars[index].close,
        return_1h: prior_hour_return,
        return_4h,
        volume_ratio,
        efficiency: side as f64 * reversal_return,
        close_location: directional_close_location,
        volume_24h,
        score: prior_hour_return.abs() * reversal_return.abs() * volume_ratio.ln_1p(),
        entry_phase: "shock_reversal".to_owned(),
        breakout_level: if side > 0 { prior_low } else { prior_high },
        entry_trigger: if strict {
            "strict_shock_reversal".to_owned()
        } else {
            "broad_shock_reversal".to_owned()
        },
        risk_scale: if strict {
            shock.strict_gross_multiple
        } else {
            shock.broad_gross_multiple
        },
        blockers: Vec::new(),
        spot_return_1h: None,
        oi_change_1h: None,
        funding_rate: None,
        perp_premium: None,
    })
}

fn shock_shadow_outcome(
    candidate: &Candidate,
    bars: &[Bar],
    signal_index: usize,
    shock: &AltcoinShockReversalConfig,
) -> Option<ShockShadowOutcome> {
    let entry_index = signal_index + 1;
    let max_bars = shock.max_hold_hours as usize * 4;
    let end = entry_index.checked_add(max_bars)?;
    if end >= bars.len() {
        return None;
    }
    let slip = shock.assumed_slippage_bps_per_side / 10_000.0;
    let fee = shock.assumed_fee_bps_per_side / 10_000.0;
    let entry = bars[entry_index].open * (1.0 + candidate.side as f64 * slip);
    let mut stop = entry * (1.0 - candidate.side as f64 * shock.stop_pct);
    let mut extreme = entry;
    let mut raw_exit = bars[end].open;
    let mut exit_ms = bars[end].open_ms;
    for bar in &bars[entry_index..end] {
        let gap = if candidate.side > 0 {
            bar.open <= stop
        } else {
            bar.open >= stop
        };
        let stopped = if candidate.side > 0 {
            bar.low <= stop
        } else {
            bar.high >= stop
        };
        if gap || stopped {
            raw_exit = if gap { bar.open } else { stop };
            exit_ms = bar.open_ms;
            break;
        }
        extreme = if candidate.side > 0 {
            extreme.max(bar.high)
        } else {
            extreme.min(bar.low)
        };
        let favorable = candidate.side as f64 * (extreme / entry - 1.0);
        if favorable >= shock.trail_activation_pct {
            let proposed = extreme * (1.0 - candidate.side as f64 * shock.trail_pct);
            stop = if candidate.side > 0 {
                stop.max(proposed)
            } else {
                stop.min(proposed)
            };
        }
    }
    let exit = raw_exit * (1.0 - candidate.side as f64 * slip);
    let ratio = exit / entry;
    Some(ShockShadowOutcome {
        symbol: candidate.symbol.clone(),
        side: candidate.side,
        signal_ms: candidate.signal_ms,
        exit_ms,
        net_return: candidate.side as f64 * (ratio - 1.0) - fee - fee * ratio,
    })
}

fn shock_gate_side(
    outcomes: &[ShockShadowOutcome],
    side: i32,
    window: usize,
    min_pf: f64,
) -> Value {
    let mut side_outcomes: Vec<_> = outcomes
        .iter()
        .filter(|outcome| outcome.side == side)
        .collect();
    side_outcomes.sort_by_key(|outcome| (outcome.exit_ms, outcome.signal_ms));
    if side_outcomes.len() > window {
        side_outcomes.drain(..side_outcomes.len() - window);
    }
    let returns: Vec<f64> = side_outcomes
        .iter()
        .map(|outcome| outcome.net_return)
        .collect();
    let gains: f64 = returns.iter().copied().filter(|value| *value > 0.0).sum();
    let losses: f64 = returns
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .map(f64::abs)
        .sum();
    let profit_factor = if losses > 0.0 { gains / losses } else { 99.0 };
    let sum_return: f64 = returns.iter().sum();
    let ready = returns.len() == window;
    json!({
        "side":side,
        "ready":ready,
        "open":ready && sum_return > 0.0 && profit_factor >= min_pf,
        "samples":returns.len(),
        "required_samples":window,
        "sum_return":sum_return,
        "profit_factor":profit_factor,
        "required_profit_factor":min_pf,
        "returns":returns,
        "latest":side_outcomes.last().map(|outcome| json!({
            "symbol":outcome.symbol,
            "signal_ms":outcome.signal_ms,
            "exit_ms":outcome.exit_ms,
            "net_return":outcome.net_return
        }))
    })
}

fn shock_gate_analysis(
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    boundary_ms: i64,
    shock: &AltcoinShockReversalConfig,
    impulse: &AltcoinImpulseConfig,
) -> Value {
    let mut outcomes = Vec::new();
    for (symbol, bars) in bars_by_symbol {
        if bars.len() < 7 * 96 + shock.max_hold_hours as usize * 4 + 2 {
            continue;
        }
        for index in 7 * 96..bars.len() - 1 {
            let Some(candidate) =
                shock_reversal_candidate_at(symbol, bars, index, shock, impulse.min_24h_volume_usd)
            else {
                continue;
            };
            let Some(outcome) = shock_shadow_outcome(&candidate, bars, index, shock) else {
                continue;
            };
            if outcome.exit_ms < boundary_ms {
                outcomes.push(outcome);
            }
        }
    }
    let long = shock_gate_side(
        &outcomes,
        1,
        shock.gate_window,
        shock.gate_min_profit_factor,
    );
    let short = shock_gate_side(
        &outcomes,
        -1,
        shock.gate_window,
        shock.gate_min_profit_factor,
    );
    json!({
        "model":"15m_shock_reversal",
        "stage":"scan",
        "boundary_ms":boundary_ms,
        "next_refresh_ms":boundary_ms + shock.gate_refresh_hours as i64 * 3_600_000,
        "gate":{"long":long,"short":short},
        "thresholds":{
            "min_shock_return":shock.min_shock_return,
            "max_shock_return":shock.max_shock_return,
            "min_reversal_return":shock.min_reversal_return,
            "min_volume_ratio":shock.min_volume_ratio,
            "min_close_location":shock.min_close_location,
            "max_candle_range":shock.max_candle_range,
            "strict_volume_ratio":shock.strict_volume_ratio,
            "strict_close_location":shock.strict_close_location,
            "strict_max_candle_range":shock.strict_max_candle_range
        },
        "execution":{
            "broad_gross_multiple":shock.broad_gross_multiple,
            "strict_gross_multiple":shock.strict_gross_multiple,
            "max_gross_multiple":shock.max_gross_multiple,
            "stop_pct":shock.stop_pct,
            "trail_activation_pct":shock.trail_activation_pct,
            "trail_pct":shock.trail_pct,
            "fixed_exit_hours":shock.max_hold_hours,
            "partial_take_profit":false,
            "daily_loss_limit":impulse.daily_loss_limit
        }
    })
}

fn shock_gate_open(status: &Value, side: i32) -> bool {
    let key = if side > 0 { "long" } else { "short" };
    status["gate"][key]["open"].as_bool().unwrap_or(false)
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

fn candidate_origin_ms(candidate: &Candidate) -> i64 {
    if candidate.setup_origin_ms > 0 {
        candidate.setup_origin_ms
    } else {
        candidate.signal_ms
    }
}

fn microstructure_trial_key(strategy: &str, symbol: &str, origin_signal_ms: i64) -> String {
    format!("{strategy}:{symbol}:{origin_signal_ms}")
}

fn observe_microstructure_trial(
    state: &mut PersistedState,
    strategy: &str,
    candidate: &Candidate,
    reference_ms: i64,
    reference_price: f64,
    horizon_ms: i64,
) {
    let origin_signal_ms = candidate_origin_ms(candidate);
    let key = microstructure_trial_key(strategy, &candidate.symbol, origin_signal_ms);
    state
        .microstructure_trials
        .entry(key)
        .or_insert(MicrostructureTrial {
            strategy: strategy.to_owned(),
            symbol: candidate.symbol.clone(),
            side: candidate.side,
            origin_signal_ms,
            reference_ms,
            reference_price,
            expires_ms: reference_ms + horizon_ms,
            favorable_extreme: reference_price,
            adverse_extreme: reference_price,
            last_price: reference_price,
            executed: false,
        });
}

fn update_microstructure_trials(
    state: &mut PersistedState,
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    now_ms: i64,
) -> Vec<Value> {
    let keys: Vec<String> = state.microstructure_trials.keys().cloned().collect();
    let mut completed = Vec::new();
    for key in keys {
        let Some(trial) = state.microstructure_trials.get_mut(&key) else {
            continue;
        };
        if let Some(bars) = bars_by_symbol.get(&trial.symbol) {
            for bar in bars
                .iter()
                .filter(|bar| bar.close_ms >= trial.reference_ms && bar.open_ms <= trial.expires_ms)
            {
                if trial.side > 0 {
                    trial.favorable_extreme = trial.favorable_extreme.max(bar.high);
                    trial.adverse_extreme = trial.adverse_extreme.min(bar.low);
                } else {
                    trial.favorable_extreme = trial.favorable_extreme.min(bar.low);
                    trial.adverse_extreme = trial.adverse_extreme.max(bar.high);
                }
                trial.last_price = bar.close;
            }
        }
        if now_ms < trial.expires_ms {
            continue;
        }
        let mfe = trial.side as f64 * (trial.favorable_extreme / trial.reference_price - 1.0);
        let mae = -trial.side as f64 * (trial.adverse_extreme / trial.reference_price - 1.0);
        let final_return = trial.side as f64 * (trial.last_price / trial.reference_price - 1.0);
        completed.push(json!({
            "ts_ms":now_ms,
            "event":"microstructure_outcome",
            "strategy":trial.strategy,
            "setup_id":key,
            "symbol":trial.symbol,
            "side":trial.side,
            "origin_signal_ms":trial.origin_signal_ms,
            "reference_ms":trial.reference_ms,
            "reference_price":trial.reference_price,
            "horizon_ms":trial.expires_ms-trial.reference_ms,
            "executed":trial.executed,
            "max_favorable_excursion":mfe.max(0.0),
            "max_adverse_excursion":mae.max(0.0),
            "final_return":final_return,
            "last_price":trial.last_price
        }));
        state.microstructure_trials.remove(&key);
    }
    completed
}

async fn build_position_status(
    state: &PersistedState,
    prices: &HashMap<String, f64>,
    rest: Option<&live::RestClient>,
    valuation_ms: i64,
    cfg: &AltcoinImpulseConfig,
    cross: &AltcoinCrossSectionConfig,
    shock: &AltcoinShockReversalConfig,
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
        let price_return_pct = if entry_notional > 0.0 {
            unrealized_pnl / entry_notional
        } else {
            0.0
        };
        // Binance paper UI reports ROI against mark-value margin, not entry
        // notional.  For a profitable short this is intentionally larger than
        // price_return * leverage because the current mark notional is lower.
        let mark_notional = qty * mark_price;
        let margin_roe_pct = position.exchange_leverage.and_then(|leverage| {
            let mark_margin = mark_notional / leverage as f64;
            (mark_margin > 0.0).then_some(unrealized_pnl / mark_margin)
        });
        let adverse = position.adverse_extreme.unwrap_or(position.entry_price);
        let (trail_activation_pct, trail_pct) =
            position_exit_parameters(position, cfg, cross, shock);
        let partial_fraction = position_partial_take_profit_fraction(position, cfg, cross);
        let activation_price =
            position.entry_price * (1.0 + position.side as f64 * trail_activation_pct);
        let trailing_stop = position.extreme * (1.0 - position.side as f64 * trail_pct);
        let (management_stage, next_trigger_price, next_action) =
            match position.protection_reason.as_str() {
                "trailing_take_profit" => (
                    "trailing_take_profit",
                    position.stop_price,
                    "Current trailing stop closes the remaining position".to_owned(),
                ),
                "recovery_profit_lock" => (
                    "recovery_profit_lock",
                    position.stop_price,
                    "Recovery lock is active; the current stop protects the recovered profit"
                        .to_owned(),
                ),
                "partial_take_profit_break_even" => (
                    "partial_profit_protected",
                    position.stop_price,
                    format!(
                        "Partial profit is realized; the remainder trails {:.2}% behind its peak",
                        trail_pct * 100.0
                    ),
                ),
                _ if partial_fraction > 0.0 && !position.partial_take_profit_done => (
                    "initial_protection",
                    activation_price,
                    format!(
                        "At the next profit trigger, close {:.0}% and protect the remainder",
                        partial_fraction * 100.0
                    ),
                ),
                _ => (
                    "initial_protection",
                    activation_price,
                    "Waiting for trailing activation; the exchange stop remains active".to_owned(),
                ),
            };
        result.push(json!({
            "symbol":position.symbol, "side":position.side, "qty":qty,
            "entry_ms":position.entry_ms, "entry_price":entry_price,
            "initial_notional":position.initial_notional, "stop_price":position.stop_price,
            "extreme":position.extreme, "mark_price":mark_price,
            "protection_order_id":position.protection_order_id,
            "protection_order_ids":protection_order_ids(position),
            "protection_order_count":protection_order_ids(position).len(),
            "protection_reason":position.protection_reason,
            "entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,
            "partial_take_profit_done":position.partial_take_profit_done,
            "loss_trim_done":position.loss_trim_done,
            "realized_partial_pnl":position.realized_partial_pnl,
            "max_favorable_excursion_pct": position.side as f64 * (position.extreme / position.entry_price - 1.0),
            "max_adverse_excursion_pct": adverse_excursion(position.side, position.entry_price, adverse),
            "exchange_leverage":position.exchange_leverage,
            "unrealized_pnl":unrealized_pnl,
            // Keep return_pct for backwards compatibility, but make both
            // denominators explicit so the UI cannot confuse price P&L with
            // Binance's leverage/margin ROE.
            "return_pct":price_return_pct,
            "price_return_pct":price_return_pct,
            "margin_roe_pct":margin_roe_pct,
            "margin_roe_approximate":true,
            "valuation_source":valuation_source,
            "valuation_ms":valuation_ms,
            "management":{
                "stage":management_stage,
                "current_stop":position.stop_price,
                "next_trigger_price":next_trigger_price,
                "next_action":next_action,
                "trail_activation_price":activation_price,
                "trail_activation_pct":trail_activation_pct,
                "trail_distance_pct":trail_pct,
                "calculated_trailing_stop":trailing_stop,
                "partial_close_fraction":partial_fraction,
                "partial_done":position.partial_take_profit_done,
                "protection_order_count":protection_order_ids(position).len()
            }
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

fn position_excursions(position: &Position) -> (f64, f64) {
    let favorable =
        (position.side as f64 * (position.extreme / position.entry_price - 1.0)).max(0.0);
    let adverse = position
        .adverse_extreme
        .map(|price| adverse_excursion(position.side, position.entry_price, price))
        .unwrap_or(0.0);
    (favorable, adverse)
}

// 将恢复锁盈的全部风控阈值保留为显式输入，便于回测与线上共用同一计算函数。
#[allow(clippy::too_many_arguments)]
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

fn protection_order_ids(position: &Position) -> Vec<i64> {
    let mut ids = position.protection_order_ids.clone();
    if let Some(id) = position.protection_order_id {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    ids
}

fn set_protection_order_ids(position: &mut Position, ids: Vec<i64>) {
    position.protection_order_id = ids.first().copied();
    position.protection_order_ids = ids;
}

fn market_qty_chunks(qty: f64, filters: &live::SymbolFilters) -> Vec<f64> {
    if qty <= 0.0 {
        return Vec::new();
    }
    let max_qty = filters.market_max_qty;
    if !max_qty.is_finite() || max_qty <= 0.0 || qty <= max_qty {
        return vec![qty];
    }
    let mut remaining = qty;
    let mut chunks = Vec::new();
    while remaining > 1e-12 {
        let chunk = remaining.min(max_qty);
        chunks.push(chunk);
        remaining -= chunk;
    }
    chunks
}

async fn place_market_reduce_only(
    rest: &live::RestClient,
    symbol: &str,
    side: &str,
    qty: f64,
    filters: &live::SymbolFilters,
) -> Result<(f64, f64, f64)> {
    let mut total_qty = 0.0;
    let mut total_quote = 0.0;
    let mut total_fee = 0.0;
    for chunk in market_qty_chunks(qty, filters) {
        let order = match rest
            .place_order(symbol, side, "MARKET", chunk, None, None, true, filters)
            .await
        {
            Ok(order) => order,
            // Demo Futures 偶尔用 PERCENT_PRICE 拒绝 reduce-only MARKET（-4131）。
            // 改用交易所当前 mark 附近、且被过滤器限制住的可成交 IOC，避免已触发
            // 的止盈止损因为测试网价格保护而留仓。
            Err(live::rest::RestError::Binance { code: -4131, .. }) => {
                let mark = rest.mark_price(symbol).await?;
                let guard = if side == "SELL" {
                    mark * filters.multiplier_down * 1.001
                } else {
                    mark * filters.multiplier_up * 0.999
                };
                rest.place_order(
                    symbol,
                    side,
                    "LIMIT_IOC",
                    chunk,
                    Some(guard),
                    None,
                    true,
                    filters,
                )
                .await?
            }
            Err(error) => return Err(error.into()),
        };
        let (price, filled_qty, fee) = wait_fill(rest, symbol, order).await?;
        total_qty += filled_qty;
        total_quote += price * filled_qty;
        total_fee += fee;
    }
    if total_qty <= 0.0 {
        anyhow::bail!("{symbol} 分片市价平仓没有成交")
    }
    Ok((total_quote / total_qty, total_qty, total_fee))
}

async fn place_protective_stops(
    rest: &live::RestClient,
    symbol: &str,
    side: &str,
    qty: f64,
    stop_price: f64,
    filters: &live::SymbolFilters,
) -> Result<Vec<i64>> {
    let mut ids = Vec::new();
    for chunk in market_qty_chunks(qty, filters) {
        match rest
            .place_order(
                symbol,
                side,
                "STOP_MARKET",
                chunk,
                None,
                Some(stop_price),
                true,
                filters,
            )
            .await
        {
            Ok(id) => ids.push(id),
            Err(error) => {
                for id in &ids {
                    let _ = rest.cancel_algo_order(symbol, *id).await;
                }
                return Err(error.into());
            }
        }
    }
    Ok(ids)
}

async fn cancel_protective_stops(
    rest: &live::RestClient,
    symbol: &str,
    position: &Position,
) -> Vec<(i64, String)> {
    let mut failures = Vec::new();
    for id in protection_order_ids(position) {
        if let Err(error) = rest.cancel_algo_order(symbol, id).await {
            failures.push((id, error.to_string()));
        }
    }
    failures
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
    if is_pulse_position(position) {
        state.pulse_exits += 1;
        state.pulse_realized_pnl += pnl;
        if trade_pnl > 0.0 {
            state.pulse_wins += 1;
        }
    } else if is_cross_position(position) {
        state.cross_exits += 1;
        state.cross_realized_pnl += pnl;
        if trade_pnl > 0.0 {
            state.cross_wins += 1;
        }
    } else if is_shock_position(position) {
        state.shock_exits += 1;
        state.shock_realized_pnl += pnl;
        if trade_pnl > 0.0 {
            state.shock_wins += 1;
        }
    }
    if position.entry_phase == "overextended_long" && trade_pnl < 0.0 {
        state.overextension_long_blocked = true;
        state.overextension_long_losses = state.overextension_long_losses.saturating_add(1);
        state.last_overextension_loss_ms = Some(now_ms);
    }
    state.positions.remove(&position.symbol);
    if !is_pulse_position(position) && !is_cross_position(position) && cooldown_hours > 0 {
        state.cooldown_until.insert(
            position.symbol.clone(),
            now_ms + cooldown_hours as i64 * 3_600_000,
        );
    }
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
    if is_pulse_position(position) {
        state.pulse_realized_pnl += pnl;
    } else if is_cross_position(position) {
        state.cross_realized_pnl += pnl;
    } else if is_shock_position(position) {
        state.shock_realized_pnl += pnl;
    }
    pnl
}

fn apply_daily_risk_reset(
    state: &mut PersistedState,
    current_equity: f64,
    now_ms: i64,
    event_path: &str,
) -> Result<()> {
    let previous_baseline = state.day_start_equity;
    state.day = risk_day(now_ms);
    state.day_start_equity = current_equity;
    state.daily_loss_latched = false;
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
    // A zero base limit explicitly means unlimited entries. There is no
    // meaningful temporary quota to increase in that mode.
    if base_limit == 0 {
        return Ok(false);
    }
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

fn daily_entry_limit_reached(daily_entries: u32, effective_limit: u32) -> bool {
    effective_limit > 0 && daily_entries >= effective_limit
}

async fn resolve_live_cross_rebalance(
    state: &mut PersistedState,
    candidates: &mut Vec<Candidate>,
    client: &live::RestClient,
    basket_equity: f64,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    let selected: Vec<Candidate> = candidates
        .iter()
        .filter(|candidate| {
            is_cross_candidate(candidate)
                && candidate.entry_trigger == "scheduled_cross_section_momentum"
        })
        .cloned()
        .collect();
    let current: Vec<Position> = state
        .positions
        .values()
        .filter(|position| is_cross_position(position))
        .cloned()
        .collect();
    let mut changed = false;
    let mut carried = Vec::new();
    let mut closed = Vec::new();
    for position in current {
        if let Some(candidate) = selected.iter().find(|candidate| {
            candidate.symbol == position.symbol && candidate.side == position.side
        }) {
            let target_notional = basket_equity * candidate.risk_scale;
            let weight_drift = if target_notional > 0.0 {
                (position.initial_notional / target_notional - 1.0).abs()
            } else {
                f64::INFINITY
            };
            if weight_drift <= 0.25 {
                candidates
                    .retain(|item| !(is_cross_candidate(item) && item.symbol == position.symbol));
                state
                    .cross_section_pending
                    .retain(|item| item.symbol != position.symbol);
                carried.push(position.symbol.clone());
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"cross_section_position_carried","symbol":position.symbol,"side":position.side,"position_entry_ms":position.entry_ms,"position_entry_price":position.entry_price,"new_signal_ms":candidate.signal_ms,"new_signal_return_12h":candidate.return_1h,"qty":position.qty,"initial_notional":position.initial_notional,"target_notional":target_notional,"weight_drift":weight_drift,"stop_price":position.stop_price,"protection_reason":position.protection_reason,"reason":"该腿仍在五币目标组合中且权重偏差不超过 25%，保留仓位和已收紧保护"}),
                )?;
                changed = true;
                continue;
            }
        }
        let filters = client.symbol_filters(&position.symbol).await?;
        let snapshot = client.position_risk(&position.symbol).await?;
        let close_side = if position.side > 0 { "SELL" } else { "BUY" };
        let (exit, exit_qty, fee) = place_market_reduce_only(
            client,
            &position.symbol,
            close_side,
            snapshot.position_amt.abs(),
            &filters,
        )
        .await?;
        if let Err(error) = client.cancel_all_open_orders(&position.symbol).await {
            warn!(symbol=%position.symbol, error=%error, "横截面轮换已平仓，但清理旧保护单失败");
        }
        let exit_qty = exit_qty.min(position.qty);
        let pnl = record_exit(state, &position, now_ms, 0, exit, exit_qty, fee);
        let trade_pnl = position.realized_partial_pnl + pnl;
        let (max_favorable_excursion, max_adverse_excursion) = position_excursions(&position);
        let event = json!({"ts_ms":now_ms,"event":"exit","symbol":position.symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":if selected.is_empty() {"cross_section_no_candidate"} else {"cross_section_rotation"},"replacement_symbols":selected.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>(),"price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":now_ms-position.entry_ms,"max_favorable_excursion":max_favorable_excursion,"max_adverse_excursion":max_adverse_excursion});
        append_event(event_path, event.clone())?;
        state.record_trade(event);
        closed.push(position.symbol);
        changed = true;
    }
    if changed {
        state.cross_section_status["execution_state"] = json!({"status":"rebalancing_basket","carried_symbols":carried,"closed_symbols":closed,"pending_symbols":candidates.iter().filter(|item| is_cross_candidate(item)).map(|item| item.symbol.clone()).collect::<Vec<_>>(),"rebalanced_ms":now_ms});
    }
    Ok(changed)
}

fn resolve_dry_cross_rebalance(
    state: &mut PersistedState,
    candidates: &mut Vec<Candidate>,
    bars_by_symbol: &HashMap<String, Vec<Bar>>,
    cfg: &AltcoinImpulseConfig,
    basket_equity: f64,
    now_ms: i64,
    event_path: &str,
) -> Result<bool> {
    let selected: Vec<Candidate> = candidates
        .iter()
        .filter(|item| {
            is_cross_candidate(item) && item.entry_trigger == "scheduled_cross_section_momentum"
        })
        .cloned()
        .collect();
    let current: Vec<Position> = state
        .positions
        .values()
        .filter(|item| is_cross_position(item))
        .cloned()
        .collect();
    let mut changed = false;
    let mut carried = Vec::new();
    let mut closed = Vec::new();
    for position in current {
        if let Some(candidate) = selected.iter().find(|candidate| {
            candidate.symbol == position.symbol && candidate.side == position.side
        }) {
            let target_notional = basket_equity * candidate.risk_scale;
            let weight_drift = if target_notional > 0.0 {
                (position.initial_notional / target_notional - 1.0).abs()
            } else {
                f64::INFINITY
            };
            if weight_drift <= 0.25 {
                candidates
                    .retain(|item| !(is_cross_candidate(item) && item.symbol == position.symbol));
                state
                    .cross_section_pending
                    .retain(|item| item.symbol != position.symbol);
                carried.push(position.symbol.clone());
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"cross_section_position_carried","symbol":position.symbol,"side":position.side,"position_entry_ms":position.entry_ms,"position_entry_price":position.entry_price,"new_signal_ms":candidate.signal_ms,"new_signal_return_12h":candidate.return_1h,"qty":position.qty,"initial_notional":position.initial_notional,"target_notional":target_notional,"weight_drift":weight_drift,"stop_price":position.stop_price,"protection_reason":position.protection_reason,"dry_fill":true,"reason":"该腿仍在五币目标组合中且权重偏差不超过 25%，保留仓位和已收紧保护"}),
                )?;
                changed = true;
                continue;
            }
        }
        let Some(raw_exit) = bars_by_symbol
            .get(&position.symbol)
            .and_then(|bars| bars.last())
            .map(|bar| bar.close)
        else {
            continue;
        };
        let exit = adverse_fill_price(raw_exit, position.side, cfg.dry_slippage_bps, false);
        let fee = position.qty * exit * 0.0005;
        let pnl = record_exit(state, &position, now_ms, 0, exit, position.qty, fee);
        let trade_pnl = position.realized_partial_pnl + pnl;
        let (max_favorable_excursion, max_adverse_excursion) = position_excursions(&position);
        let event = json!({"ts_ms":now_ms,"event":"exit","symbol":position.symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":if selected.is_empty() {"cross_section_no_candidate"} else {"cross_section_rotation"},"replacement_symbols":selected.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>(),"price":exit,"raw_price":raw_exit,"qty":position.qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":now_ms-position.entry_ms,"max_favorable_excursion":max_favorable_excursion,"max_adverse_excursion":max_adverse_excursion,"dry_slippage_bps":cfg.dry_slippage_bps,"dry_fill":true});
        append_event(event_path, event.clone())?;
        state.record_trade(event);
        closed.push(position.symbol);
        changed = true;
    }
    if changed {
        state.cross_section_status["execution_state"] = json!({"status":"rebalancing_basket","carried_symbols":carried,"closed_symbols":closed,"pending_symbols":candidates.iter().filter(|item| is_cross_candidate(item)).map(|item| item.symbol.clone()).collect::<Vec<_>>(),"rebalanced_ms":now_ms,"dry_fill":true});
    }
    Ok(changed)
}

/// 模拟盘/实盘的独立持仓管理循环。信号扫描可以维持低频，但交易所仓位必须高频：
/// - 识别交易所止损成交；
/// - 用实时 markPrice 推进极值与跟踪止盈；
/// - 按墙钟执行时间退出。
async fn manage_live_positions(
    state: &mut PersistedState,
    client: &live::RestClient,
    cfg: &AltcoinImpulseConfig,
    cross: &AltcoinCrossSectionConfig,
    shock: &AltcoinShockReversalConfig,
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
        let managed_qty = snapshot.position_amt.abs();
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
                position_cooldown_hours(&position, cfg, shock),
                exit,
                exit_qty,
                exit_fee,
            );
            let trade_pnl = position.realized_partial_pnl + pnl;
            let (max_favorable_excursion, max_adverse_excursion) = position_excursions(&position);
            let event = json!({"ts_ms":now_ms,"event":"exit_detected","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":exit_fee,"reason":reason,"protection_order_id":position.protection_order_id,"stop_price":position.stop_price,"trade_reconciled":true,"hold_ms":now_ms-position.entry_ms,"max_favorable_excursion":max_favorable_excursion,"max_adverse_excursion":max_adverse_excursion,"overextension_long_blocked":state.overextension_long_blocked});
            append_event(event_path, event.clone())?;
            state.record_trade(event);
            changed = true;
            continue;
        }

        let cross_position = is_cross_position(&position);
        let max_hold_hours = position_max_hold_hours(&position, cfg, cross, shock);
        // Cross-section windows are ranking boundaries, not mandatory round
        // trips. The boundary resolver below rotates only when the selected
        // symbol or side changes; an unchanged winner keeps its live position.
        let timed = !cross_position && now_ms - position.entry_ms >= max_hold_hours * 3_600_000;
        if timed {
            client.cancel_all_open_orders(&symbol).await?;
            let filters = client.symbol_filters(&symbol).await?;
            let side = if position.side > 0 { "SELL" } else { "BUY" };
            let qty = snapshot.position_amt.abs();
            let (exit, exit_qty, fee) =
                place_market_reduce_only(client, &symbol, side, qty, &filters).await?;
            let exit_qty = exit_qty.min(position.qty);
            let pnl = record_exit(
                state,
                &position,
                now_ms,
                position_cooldown_hours(&position, cfg, shock),
                exit,
                exit_qty,
                fee,
            );
            let trade_pnl = position.realized_partial_pnl + pnl;
            let exit_reason = if cross_position || cfg.fixed_time_exit_only {
                "scheduled_rebalance"
            } else {
                "time"
            };
            let (max_favorable_excursion, max_adverse_excursion) = position_excursions(&position);
            let event = json!({"ts_ms":now_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":exit_reason,"price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":now_ms-position.entry_ms,"max_favorable_excursion":max_favorable_excursion,"max_adverse_excursion":max_adverse_excursion,"overextension_long_blocked":state.overextension_long_blocked});
            append_event(event_path, event.clone())?;
            state.record_trade(event);
            changed = true;
            continue;
        }

        if cfg.fixed_time_exit_only {
            let previous_extreme = position.extreme;
            let previous_adverse = position.adverse_extreme;
            position.extreme = if position.side > 0 {
                position.extreme.max(snapshot.mark_price)
            } else {
                position.extreme.min(snapshot.mark_price)
            };
            position.adverse_extreme = Some(update_adverse_extreme(
                position.side,
                position.adverse_extreme,
                position.entry_price,
                snapshot.mark_price,
            ));
            changed |= position.extreme != previous_extreme
                || position.adverse_extreme != previous_adverse;
            state.positions.insert(symbol, position);
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
        let (trail_activation_pct, trail_pct) =
            position_exit_parameters(&position, cfg, cross, shock);
        let (new_extreme, improved_stop) = realtime_trailing_stop(
            position.side,
            position.entry_price,
            position.extreme,
            position.stop_price,
            snapshot.mark_price,
            trail_activation_pct,
            trail_pct,
        );
        position.extreme = new_extreme;
        let excursion = position.side as f64 * (position.extreme / position.entry_price - 1.0);
        let current_return =
            position.side as f64 * (snapshot.mark_price / position.entry_price - 1.0);
        if cfg.failed_breakout_enabled
            && failed_breakout(
                &position,
                snapshot.mark_price,
                now_ms,
                cfg.failed_breakout_window_minutes,
                cfg.failed_breakout_adverse_pct,
                cfg.failed_breakout_max_mfe_pct,
            )
        {
            let filters = client.symbol_filters(&symbol).await?;
            let side = if position.side > 0 { "SELL" } else { "BUY" };
            // reduceOnly 市价退出先成交，再清理旧保护，避免撤单与平仓之间出现裸仓窗口。
            let (exit, exit_qty, fee) = place_market_reduce_only(
                client,
                &symbol,
                side,
                snapshot.position_amt.abs(),
                &filters,
            )
            .await?;
            if let Err(error) = client.cancel_all_open_orders(&symbol).await {
                warn!(symbol=%symbol, error=%error, "失败突破已平仓，但清理旧保护单失败");
            }
            let exit_qty = exit_qty.min(position.qty);
            let pnl = record_exit(
                state,
                &position,
                now_ms,
                position_cooldown_hours(&position, cfg, shock),
                exit,
                exit_qty,
                fee,
            );
            let trade_pnl = position.realized_partial_pnl + pnl;
            let (_, max_adverse_excursion) = position_excursions(&position);
            let event = json!({"ts_ms":now_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"failed_breakout","price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":now_ms-position.entry_ms,"current_return":current_return,"max_favorable_excursion":excursion,"max_adverse_excursion":max_adverse_excursion,"failed_breakout_window_minutes":cfg.failed_breakout_window_minutes,"failed_breakout_adverse_pct":cfg.failed_breakout_adverse_pct,"failed_breakout_max_mfe_pct":cfg.failed_breakout_max_mfe_pct,"overextension_long_blocked":state.overextension_long_blocked});
            append_event(event_path, event.clone())?;
            state.record_trade(event);
            changed = true;
            continue;
        }
        // 亏损侧第一段保护：先成交减仓，再用剩余真实仓位替换交易所硬止损。
        // 新保护确认前不撤旧保护，失败时旧全量 reduceOnly 止损仍然有效。
        if cfg.loss_trim_enabled
            && !position.loss_trim_done
            && current_return <= -cfg.loss_trim_trigger_pct
        {
            let filters = client.symbol_filters(&symbol).await?;
            let side = if position.side > 0 { "SELL" } else { "BUY" };
            let target_qty = snapshot.position_amt.abs() * cfg.loss_trim_fraction;
            let trigger_mark_price = snapshot.mark_price;
            let (exit, exit_qty, fee) =
                place_market_reduce_only(client, &symbol, side, target_qty, &filters).await?;
            let exit_qty = exit_qty.min(position.qty);
            let partial_pnl = record_partial_exit(state, &mut position, exit, exit_qty, fee);
            position.loss_trim_done = true;
            position.last_partial_exit_ms = Some(chrono::Utc::now().timestamp_millis());
            state.positions.insert(symbol.clone(), position.clone());

            let remaining = match client.position_risk(&symbol).await {
                Ok(snapshot) => snapshot.position_amt.abs(),
                Err(error) => {
                    warn!(symbol=%symbol, error=%error, "亏损减仓后仓位查询失败，暂用成交回报推算余量");
                    position.qty
                }
            };
            let replacement = place_protective_stops(
                client,
                &symbol,
                side,
                remaining,
                position.stop_price,
                &filters,
            )
            .await;
            match replacement {
                Ok(new_order_ids) => {
                    let old_order_ids = protection_order_ids(&position);
                    for (old_order_id, error) in
                        cancel_protective_stops(client, &symbol, &position).await
                    {
                        warn!(symbol=%symbol, old_order_id, error=%error, "亏损减仓后新保护已生效，但旧保护撤销失败");
                    }
                    set_protection_order_ids(&mut position, new_order_ids.clone());
                    let event = json!({"ts_ms":now_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"loss_trim","trigger_return":-cfg.loss_trim_trigger_pct,"trigger_mark_price":trigger_mark_price,"execution_slippage_bps":position.side as f64*(exit/trigger_mark_price-1.0)*-10_000.0,"configured_fraction":cfg.loss_trim_fraction,"price":exit,"qty":exit_qty,"remaining_qty":remaining,"pnl":partial_pnl,"fee":fee,"stop_price":position.stop_price,"old_order_ids":old_order_ids,"new_order_ids":new_order_ids,"protection_replaced":true});
                    append_event(event_path, event.clone())?;
                    state.record_trade(event);
                }
                Err(error) => {
                    warn!(symbol=%symbol, error=%error, "亏损减仓已成交，剩余仓位保护替换失败；保留原交易所止损并等待下轮重试");
                    let event = json!({"ts_ms":now_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"loss_trim","trigger_return":-cfg.loss_trim_trigger_pct,"configured_fraction":cfg.loss_trim_fraction,"price":exit,"qty":exit_qty,"remaining_qty":remaining,"pnl":partial_pnl,"fee":fee,"stop_price":position.stop_price,"protection_replaced":false,"protection_error":error.to_string()});
                    append_event(event_path, event.clone())?;
                    state.record_trade(event);
                }
            }
            changed = true;
            // 部分成交改变了真实仓位和保护单；下一轮用新 markPrice 重新计算，
            // 避免同一轮继续使用成交前快照并挂出已被价格穿越的条件单。
            state.positions.insert(symbol, position);
            continue;
        }
        // 只按“当前仍有的浮盈”兑现，不能因历史上曾到过 +2%、现在已回落而补卖。
        if !is_shock_position(&position)
            && !position.partial_take_profit_done
            && current_return >= trail_activation_pct
        {
            let filters = client.symbol_filters(&symbol).await?;
            let side = if position.side > 0 { "SELL" } else { "BUY" };
            let partial_take_profit_fraction =
                position_partial_take_profit_fraction(&position, cfg, cross);
            let target_qty = snapshot.position_amt.abs() * partial_take_profit_fraction;
            let trigger_mark_price = snapshot.mark_price;
            let trigger_extreme = position.extreme;
            let (exit, exit_qty, fee) =
                place_market_reduce_only(client, &symbol, side, target_qty, &filters).await?;
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
            let break_even_stop = position.entry_price;
            let replacement =
                place_protective_stops(client, &symbol, side, remaining, break_even_stop, &filters)
                    .await;
            match replacement {
                Ok(new_order_ids) => {
                    let old_order_ids = protection_order_ids(&position);
                    for (old_order_id, error) in
                        cancel_protective_stops(client, &symbol, &position).await
                    {
                        warn!(symbol=%symbol, old_order_id, error=%error, "分段止盈后新保护已生效，但旧保护撤销失败");
                    }
                    set_protection_order_ids(&mut position, new_order_ids.clone());
                    position.stop_price = break_even_stop;
                    position.protection_reason = "partial_take_profit_break_even".to_owned();
                    let event = json!({"ts_ms":now_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"partial_take_profit","trigger_return":current_return,"configured_trigger_return":trail_activation_pct,"configured_fraction":partial_take_profit_fraction,"configured_trail_pct":trail_pct,"trigger_mark_price":trigger_mark_price,"trigger_extreme":trigger_extreme,"execution_slippage_bps":position.side as f64*(exit/trigger_mark_price-1.0)*-10_000.0,"price":exit,"qty":exit_qty,"remaining_qty":remaining,"pnl":partial_pnl,"fee":fee,"new_stop":break_even_stop,"old_order_ids":old_order_ids,"new_order_ids":new_order_ids,"protection_replaced":true});
                    append_event(event_path, event.clone())?;
                    state.record_trade(event);
                }
                Err(error) => {
                    warn!(symbol=%symbol, error=%error, "第一段止盈已成交，保本保护替换失败；保留原交易所止损并等待下轮重试");
                    let event = json!({"ts_ms":now_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"partial_take_profit","configured_trigger_return":trail_activation_pct,"configured_fraction":partial_take_profit_fraction,"configured_trail_pct":trail_pct,"price":exit,"qty":exit_qty,"remaining_qty":remaining,"pnl":partial_pnl,"fee":fee,"new_stop":position.stop_price,"protection_replaced":false,"protection_error":error.to_string()});
                    append_event(event_path, event.clone())?;
                    state.record_trade(event);
                }
            }
            changed = true;
            state.positions.insert(symbol, position);
            continue;
        }
        let recovery_stop = cfg
            .recovery_lock_enabled
            .then(|| {
                recovery_profit_lock_stop(
                    position.side,
                    position.entry_price,
                    position.stop_price,
                    snapshot.mark_price,
                    max_adverse,
                    cfg.recovery_lock_adverse_pct,
                    cfg.recovery_lock_activation_pct,
                    cfg.recovery_lock_pct,
                )
            })
            .flatten();
        let (mut improved_stop, mut protection_reason) = match (improved_stop, recovery_stop) {
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
        // A persisted cross-section position can have been opened under the
        // former 8% stop.  Tighten it to the current configured initial stop
        // after deployment; never loosen a break-even or trailing stop.
        if cross_position {
            let configured_stop =
                position.entry_price * (1.0 - position.side as f64 * cross.stop_pct);
            let reference = improved_stop.unwrap_or(position.stop_price);
            let tighter = if position.side > 0 {
                configured_stop > reference
            } else {
                configured_stop < reference
            };
            if tighter {
                improved_stop = Some(configured_stop);
                protection_reason = "configured_stop_migration";
            }
        }
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
            let stop_already_crossed = if position.side > 0 {
                snapshot.mark_price <= improved_stop
            } else {
                snapshot.mark_price >= improved_stop
            };
            if stop_already_crossed {
                // Binance 会以 -2021 拒绝已经被当前价格穿越的 STOP_MARKET。
                // 此时策略语义本来就是“立即兑现”，直接 reduce-only 市价退出。
                let (exit, exit_qty, fee) = place_market_reduce_only(
                    client,
                    &symbol,
                    side,
                    snapshot.position_amt.abs(),
                    &filters,
                )
                .await?;
                if let Err(error) = client.cancel_all_open_orders(&symbol).await {
                    warn!(symbol=%symbol, error=%error, "跟踪止盈市价退出后清理旧保护失败");
                }
                let exit_qty = exit_qty.min(position.qty);
                let pnl = record_exit(
                    state,
                    &position,
                    now_ms,
                    position_cooldown_hours(&position, cfg, shock),
                    exit,
                    exit_qty,
                    fee,
                );
                let trade_pnl = position.realized_partial_pnl + pnl;
                let (_, max_adverse_excursion) = position_excursions(&position);
                let event = json!({"ts_ms":now_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":protection_reason,"price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":now_ms-position.entry_ms,"mark_price":snapshot.mark_price,"crossed_stop":improved_stop,"max_favorable_excursion":excursion,"max_adverse_excursion":max_adverse_excursion,"direct_market_exit":true});
                append_event(event_path, event.clone())?;
                state.record_trade(event);
                changed = true;
                continue;
            }
            // 先挂新保护，确认成功后才撤旧保护；任何下单失败都保留原止损。
            let new_order_ids = match place_protective_stops(
                client,
                &symbol,
                side,
                managed_qty,
                improved_stop,
                &filters,
            )
            .await
            {
                Ok(order_ids) => order_ids,
                Err(error) if error.to_string().contains("错误 -2021:") => {
                    // CONTRACT_PRICE can cross the trigger between positionRisk and
                    // conditional-order submission. A rejected tighter stop means the
                    // exit condition is already true, so settle immediately instead of
                    // retrying the same invalid stop every five seconds.
                    let (exit, exit_qty, fee) =
                        place_market_reduce_only(client, &symbol, side, managed_qty, &filters)
                            .await?;
                    if let Err(cancel_error) = client.cancel_all_open_orders(&symbol).await {
                        warn!(symbol=%symbol, error=%cancel_error, "保护单竞态退出后清理旧保护失败");
                    }
                    let exit_qty = exit_qty.min(position.qty);
                    let pnl = record_exit(
                        state,
                        &position,
                        now_ms,
                        position_cooldown_hours(&position, cfg, shock),
                        exit,
                        exit_qty,
                        fee,
                    );
                    let trade_pnl = position.realized_partial_pnl + pnl;
                    let (_, max_adverse_excursion) = position_excursions(&position);
                    let event = json!({"ts_ms":now_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":protection_reason,"price":exit,"qty":exit_qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":now_ms-position.entry_ms,"mark_price":snapshot.mark_price,"crossed_stop":improved_stop,"max_favorable_excursion":excursion,"max_adverse_excursion":max_adverse_excursion,"direct_market_exit":true,"trigger_order_race":true,"rejected_trigger_error":error.to_string()});
                    append_event(event_path, event.clone())?;
                    state.record_trade(event);
                    changed = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let old_order_ids = protection_order_ids(&position);
            for (old_order_id, error) in cancel_protective_stops(client, &symbol, &position).await {
                warn!(symbol=%symbol, old_order_id, error=%error, "新保护已生效，但旧保护撤销失败");
                append_event(
                    event_path,
                    json!({"ts_ms":now_ms,"event":"old_protection_cancel_failed","symbol":symbol,"old_order_id":old_order_id,"new_order_ids":new_order_ids,"reason":error}),
                )?;
            }
            let old_stop = position.stop_price;
            position.stop_price = improved_stop;
            set_protection_order_ids(&mut position, new_order_ids.clone());
            position.protection_reason = protection_reason.to_owned();
            append_event(
                event_path,
                json!({"ts_ms":now_ms,"event":"protection_updated","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"mark_price":snapshot.mark_price,"extreme":position.extreme,"excursion":excursion,"adverse_extreme":adverse,"max_adverse_excursion":max_adverse,"current_return":position.side as f64*(snapshot.mark_price/position.entry_price-1.0),"trail_activation_pct":trail_activation_pct,"trail_pct":trail_pct,"old_stop":old_stop,"new_stop":improved_stop,"old_order_ids":old_order_ids,"new_order_ids":new_order_ids,"reason":protection_reason}),
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
    Ok(Some(
        place_market_reduce_only(rest, symbol, side, amount.abs(), filters).await?,
    ))
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
    let cross_cfg = strategy_file.altcoin_cross_section;
    let shock_cfg = strategy_file.altcoin_shock_reversal;
    let strategy_hash = format!("{:x}", Sha256::digest(strategy_text.as_bytes()));
    let git_commit = crate::BUILD_GIT_COMMIT.to_owned();
    anyhow::ensure!(cfg.enabled, "altcoin_impulse.enabled=false");
    anyhow::ensure!(
        !shock_cfg.enabled
            || ((0.03..=0.10).contains(&shock_cfg.min_shock_return)
                && shock_cfg.max_shock_return >= shock_cfg.min_shock_return
                && shock_cfg.max_shock_return <= 0.30
                && (0.003..=0.015).contains(&shock_cfg.min_reversal_return)
                && (1.5..=5.0).contains(&shock_cfg.min_volume_ratio)
                && (0.55..=0.85).contains(&shock_cfg.min_close_location)
                && (20..=80).contains(&shock_cfg.gate_window)
                && (0.8..=1.5).contains(&shock_cfg.gate_min_profit_factor)
                && (0.0..=10.0).contains(&shock_cfg.assumed_fee_bps_per_side)
                && (0.0..=25.0).contains(&shock_cfg.assumed_slippage_bps_per_side)
                && (0.01..=0.04).contains(&shock_cfg.stop_pct)
                && shock_cfg.trail_activation_pct > shock_cfg.stop_pct
                && (0.003..shock_cfg.trail_activation_pct).contains(&shock_cfg.trail_pct)
                && (1..=6).contains(&shock_cfg.max_hold_hours)
                && shock_cfg.broad_gross_multiple > 0.0
                && shock_cfg.strict_gross_multiple >= shock_cfg.broad_gross_multiple
                && shock_cfg.max_gross_multiple >= shock_cfg.strict_gross_multiple
                && shock_cfg.max_gross_multiple <= 3.0),
        "冲击反转参数超出验证边界"
    );
    anyhow::ensure!(
        !cross_cfg.enabled
            || (cross_cfg.formation_hours == 12
                && cross_cfg.hold_hours == 3
                && (1..=5).contains(&cross_cfg.names)
                && (26..=36).contains(&cross_cfg.gate_window)
                && cross_cfg.min_24h_volume_usd >= 10_000_000.0
                && (0.005..=0.02).contains(&cross_cfg.market_momentum_threshold)
                && (1.0..=2.0).contains(&cross_cfg.gate_min_profit_factor)
                && (5.0..=25.0).contains(&cross_cfg.assumed_cost_bps_per_side)),
        "横截面动量参数必须保持在已验证口径：12h/3h、1..=5 个等权标的、26..=36 个已完成信号自适应仓位、日成交额至少 1000 万美元"
    );
    anyhow::ensure!(
        !cross_cfg.enabled
            || ((0.05..=0.25).contains(&cross_cfg.base_gross_multiple)
                && cross_cfg.active_gross_multiple >= cross_cfg.base_gross_multiple
                && cross_cfg.strong_gross_multiple >= cross_cfg.active_gross_multiple
                && cross_cfg.active_gross_multiple <= 1.0
                && cross_cfg.strong_gross_multiple <= 1.5
                && (0.08..=0.20).contains(&cross_cfg.strong_excess_return)
                && (0.02..=0.08).contains(&cross_cfg.stop_pct)
                && (0.01..=0.10).contains(&cross_cfg.trail_activation_pct)
                && (0.003..cross_cfg.trail_activation_pct).contains(&cross_cfg.trail_pct)
                && (0.25..=0.75).contains(&cross_cfg.partial_take_profit_fraction)),
        "横截面动量仓位要求基础 0.05x..=0.25x、增强仓位不超过 1.0x、强信号仓位不超过 1.5x、超额动量 8%..=20%、止损 2%..=8%，且止盈/跟踪参数有效"
    );
    anyhow::ensure!(
        (0.0..=0.50).contains(&cfg.first_week_loss_limit)
            && (0.0..=100.0).contains(&cfg.dry_slippage_bps)
            && (1..=30).contains(&cfg.first_week_duration_days),
        "首周累计亏损上限必须在 0%..=50%、dry 单边滑点必须在 0..=100bp，且保护窗口必须在 1..=30 天"
    );
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
        (0.25..=1.0).contains(&cfg.long_risk_scale) && (0.25..=1.0).contains(&cfg.short_risk_scale),
        "山寨币多空风险系数必须在 0.25..=1.0"
    );
    anyhow::ensure!(
        (0.0005..=0.02).contains(&cfg.max_directional_funding_rate)
            && (0.002..=0.05).contains(&cfg.max_directional_premium),
        "方向拥挤保护要求资金费率上限 0.05%..=2%、永续偏离上限 0.2%..=5%"
    );
    anyhow::ensure!(
        cfg.stop_pct >= 0.01 && cfg.stop_pct <= 0.12,
        "止损必须在 1%..=12%"
    );
    anyhow::ensure!(
        !cfg.loss_trim_enabled
            || (cfg.loss_trim_trigger_pct >= 0.005
                && cfg.loss_trim_trigger_pct < cfg.stop_pct
                && (0.20..=0.60).contains(&cfg.loss_trim_fraction)),
        "亏损第一段要求：触发点 >=0.5% 且小于初始止损，减仓比例 20%..=60%"
    );
    anyhow::ensure!(
        cfg.risk_execution_buffer_pct >= 0.001 && cfg.risk_execution_buffer_pct <= 0.02,
        "止损执行缓冲必须在 0.1%..=2%"
    );
    anyhow::ensure!(
        cfg.max_spread_bps > 0.0
            && cfg.liquid_market_max_spread_bps >= cfg.max_spread_bps
            && cfg.max_entry_impact_bps >= cfg.liquid_market_max_spread_bps
            && cfg.max_exit_impact_bps >= cfg.max_entry_impact_bps
            && (0.001..=0.02).contains(&cfg.depth_band_pct)
            && cfg.min_depth_multiple >= 2.0
            && cfg.recent_trade_window_seconds >= 30
            && cfg.min_recent_trades > 0
            && cfg.min_unique_trade_prices > 1
            && cfg.max_last_trade_age_seconds > 0,
        "实时流动性门槛不合法"
    );
    anyhow::ensure!(
        (1..=90).contains(&cfg.min_contract_age_days),
        "合约最短上市天数必须在 1..=90 天"
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
        (5..=30).contains(&cfg.failed_breakout_window_minutes),
        "失败突破观察窗口必须在 5..=30 分钟"
    );
    anyhow::ensure!(
        !cfg.failed_breakout_enabled
            || (cfg.failed_breakout_adverse_pct >= 0.01
                && cfg.failed_breakout_adverse_pct < cfg.stop_pct),
        "失败突破逆向阈值必须 >=1% 且小于初始止损"
    );
    anyhow::ensure!(
        cfg.failed_breakout_max_mfe_pct >= 0.0
            && cfg.failed_breakout_max_mfe_pct < cfg.trail_activation_pct,
        "失败突破最大顺向幅度必须小于跟踪止盈激活点"
    );
    anyhow::ensure!(
        !cfg.recovery_lock_enabled
            || (cfg.recovery_lock_adverse_pct >= 0.01
                && cfg.recovery_lock_adverse_pct <= cfg.stop_pct
                && cfg.recovery_lock_activation_pct > cfg.recovery_lock_pct
                && cfg.recovery_lock_activation_pct < cfg.trail_activation_pct
                && cfg.recovery_lock_pct >= 0.0),
        "修复锁盈要求：不利波动 1%..=初始止损，且 0 <= 锁定收益 < 修复收益 < 跟踪激活点"
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
        (cfg.max_daily_entries > 0 && cfg.max_daily_entry_bonus <= cfg.max_daily_entries)
            || (cfg.max_daily_entries == 0 && cfg.max_daily_entry_bonus == 0),
        "临时开仓加额不能超过基础每日开仓上限"
    );
    anyhow::ensure!(
        (15..=300).contains(&cfg.max_signal_age_seconds),
        "入场执行窗口必须在 15..=300 秒"
    );
    anyhow::ensure!(
        (2..=8).contains(&cfg.confirmation_window_bars)
            && (0.002..=0.03).contains(&cfg.retest_touch_pct)
            && cfg.retest_invalidation_pct >= cfg.retest_touch_pct
            && cfg.retest_invalidation_pct <= 0.05
            && (0.0..=0.01).contains(&cfg.reclaim_pct),
        "回踩/反抽确认参数不合法"
    );
    anyhow::ensure!(
        !cfg.adaptive_retest_enabled
            || ((0.02..=0.10).contains(&cfg.vertical_overshoot_pct)
                && (0.002..=0.02).contains(&cfg.vertical_retest_touch_pct)
                && cfg.vertical_retest_invalidation_pct >= cfg.vertical_retest_touch_pct
                && cfg.vertical_retest_invalidation_pct <= 0.05
                && (0.0..=0.01).contains(&cfg.vertical_reclaim_pct)
                && (0.01..=0.05).contains(&cfg.vertical_max_entry_extension_pct)
                && (0.001..=0.02).contains(&cfg.intrabar_min_pullback_pct)
                && (0.001..=0.02).contains(&cfg.intrabar_rebound_pct)),
        "垂直脉冲自适应回踩参数不合法"
    );
    anyhow::ensure!(
        cfg.extreme_direct_return_1h > cfg.overextension_long_return_1h
            && cfg.extreme_direct_return_1h < cfg.max_return_1h
            && cfg.extreme_direct_return_4h > cfg.overextension_long_return_4h
            && cfg.extreme_direct_return_4h < cfg.max_return_4h
            && cfg.extreme_direct_volume_ratio >= cfg.min_volume_ratio
            && (0.1..=0.5).contains(&cfg.extreme_direct_risk_scale),
        "极端延续直入参数不合法"
    );
    anyhow::ensure!(
        (0.04..=0.20).contains(&cfg.pulse_initial_return_1h)
            && (4.0..=30.0).contains(&cfg.pulse_initial_volume_ratio)
            && (0.04..=0.20).contains(&cfg.pulse_perp_return_1h)
            && (0.05..=1.0).contains(&cfg.pulse_oi_change_1h)
            && (0.2..=1.5).contains(&cfg.pulse_max_spot_perp_ratio)
            && (0.01..=0.10).contains(&cfg.pulse_min_peak_retrace)
            && (0.2..=0.9).contains(&cfg.pulse_max_close_location)
            && (0.05..=1.0).contains(&cfg.pulse_risk_scale)
            && (1..=2).contains(&cfg.pulse_max_positions)
            && (0.1..=1.50).contains(&cfg.pulse_max_gross_multiple)
            && (0.01..=0.05).contains(&cfg.pulse_stop_pct)
            && (0.01..=0.05).contains(&cfg.pulse_trail_activation_pct)
            && (0.005..cfg.pulse_trail_activation_pct).contains(&cfg.pulse_trail_pct)
            && (0.002..=0.05).contains(&cfg.pulse_max_directional_premium),
        "杠杆衰竭做空参数不合法"
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
        TradeMode::Paper => account.use_standard_environment(true),
        TradeMode::Live => account.use_standard_environment(false),
        TradeMode::Dry => {}
    }
    anyhow::ensure!(
        mode != TradeMode::Live || cfg.allow_live,
        "该高风险策略默认禁止实盘；确认后设置 allow_live=true"
    );
    // Discovery and outcome labelling stay active in live mode even while the
    // sleeve is denied real capital.  `pulse_exhaustion_allow_live` controls
    // only exchange execution, not the paper/shadow research loop.
    let (pulse_exhaustion_active, pulse_live_execution_allowed) = pulse_exhaustion_runtime(
        cfg.pulse_exhaustion_enabled,
        cfg.pulse_exhaustion_allow_live,
        mode,
    );
    let http = data::live::build_http_client(collector.effective_proxy().as_deref());
    // Signal discovery and pre-trade liquidity must describe the same real
    // market. Paper orders still go to Futures Demo below, but Demo/Testnet's
    // sparse synthetic order book must never decide whether a mainnet signal
    // is tradable.
    let market_rest =
        live::RestClient::new(http.clone(), FUTURES_BASE, String::new(), String::new());
    // Public market-data time sync improves trade-age measurements, but a
    // transient mainnet 5xx must not prevent the paper engine from starting.
    // Liquidity calls are retried later inside the bounded execution window.
    if let Err(error) = market_rest.sync_time().await {
        tracing::warn!(%error, "主网公开行情对时失败，暂用本机时钟");
    }
    let rest = if mode == TradeMode::Dry {
        None
    } else {
        let (key, secret) = match (account.api_key(), account.api_secret()) {
            (Some(key), Some(secret)) => (key, secret),
            _ => anyhow::bail!("缺少 Binance Futures API 凭证"),
        };
        let client = live::RestClient::new(http.clone(), account.rest_base(), key, secret);
        client.sync_time().await?;
        Some(client)
    };
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (state_path, event_path) = if let Some(event_path) = args.journal.as_ref() {
        let state_path = event_path
            .strip_suffix(".jsonl")
            .map(|prefix| format!("{prefix}.state.json"))
            .unwrap_or_else(|| format!("{event_path}.state.json"));
        (state_path, event_path.clone())
    } else {
        (
            format!("data/journal/altcoin-{}.state.json", mode.as_str()),
            format!("data/journal/altcoin-{}.jsonl", mode.as_str()),
        )
    };
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
    // Upgrade pending entries created by a pre-adaptive binary in place.  Old
    // state files deserialize the new anchor as zero; recompute the plan from
    // the persisted candidate so deployment never requires deleting journals.
    let mut pending_migrations = Vec::new();
    for (symbol, pending) in &mut state.pending_entries {
        if pending.retest_anchor > 0.0 {
            continue;
        }
        let (anchor, mode) = confirmation_plan(&pending.candidate, &cfg);
        pending.retest_anchor = anchor;
        pending.confirmation_mode = mode.clone();
        pending_migrations.push((symbol.clone(), anchor, mode));
    }
    if !pending_migrations.is_empty() {
        for (symbol, anchor, mode) in &pending_migrations {
            append_event(
                &event_path,
                json!({"ts_ms":now_ms,"event":"pending_entry_confirmation_migrated","symbol":symbol,"retest_anchor":anchor,"confirmation_mode":mode,"reason":"旧状态文件升级到自适应回踩模型"}),
            )?;
        }
        save_state(&state_path, &state)?;
    }
    if !cross_cfg.enabled && !state.cross_section_status.is_null() {
        state.cross_section_status = Value::Null;
        state.cross_section_pending.clear();
        state.seen_signal.remove("__cross_section__");
        append_event(
            &event_path,
            json!({
                "ts_ms":now_ms,
                "event":"strategy_mode_changed",
                "from":"cross_section_momentum",
                "to":"confirmed_volume_breakout",
                "reason":"恢复 109e769 确认式放量突破主线并保留后续执行修复"
            }),
        )?;
        save_state(&state_path, &state)?;
    }
    if cross_cfg.enabled && !state.pending_entries.is_empty() {
        let discarded = state.pending_entries.len();
        state.pending_entries.clear();
        append_event(
            &event_path,
            json!({"ts_ms":now_ms,"event":"pending_entries_cleared","count":discarded,"reason":"cross_section_momentum_enabled"}),
        )?;
        save_state(&state_path, &state)?;
    }
    if cfg.direct_entry_enabled && !state.pending_entries.is_empty() {
        let discarded = state.pending_entries.len();
        state.pending_entries.clear();
        append_event(
            &event_path,
            json!({"ts_ms":now_ms,"event":"pending_entries_cleared","count":discarded,"reason":"direct_entry_enabled"}),
        )?;
        save_state(&state_path, &state)?;
    }
    let mut phase_migrations = Vec::new();
    for position in state.positions.values_mut() {
        if is_cross_position(position) || is_pulse_position(position) || is_shock_position(position)
        {
            continue;
        }
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
            .filter(|(symbol, _)| {
                !(state.positions.contains_key(symbol)
                    || args.portfolio_mode && symbol == "BTCUSDT")
            })
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
        cross_section = cross_cfg.enabled,
        shock_reversal = shock_cfg.enabled,
        strategy = if shock_cfg.enabled {
            "multi_alpha"
        } else if cross_cfg.enabled {
            "cross_section_momentum"
        } else {
            "confirmed_volume_breakout"
        },
        "启动独立山寨币策略"
    );
    append_event(
        &event_path,
        json!({"ts_ms": now_ms, "event":"runner_start", "mode":mode.as_str(), "config":cfg, "cross_section_config":cross_cfg,"shock_reversal_config":shock_cfg}),
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
    let excluded: HashSet<&str> = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT",
        "LINKUSDT", "BCHUSDT", "LTCUSDT",
    ]
    .into_iter()
    .collect();
    let execution_symbols = if let Some(client) = rest.as_ref() {
        Some(client.active_usdt_perpetual_symbols().await?)
    } else {
        None
    };

    // 首轮全市场历史窗口会持续数分钟。先把组件标记为 running，并持续上报
    // warm-up 进度；控制面不应因为研究基线尚未完成而一直停在 starting。
    if let Some(tx) = &status_tx {
        let _ = tx.send(json!({
            "state":"running", "component":"altcoin", "mode":mode.as_str(),
            "started_at_ms":started_ms, "equity":state.cash, "cash":state.cash,
            "n_intents":state.total_entries, "n_fills":state.total_entries+state.total_exits,
            "execution_healthy":true,
            "warmup":{"active":true,"stage":"loading_market_history","completed":0,"total":0,"failures":0}
        }));
    }

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
                    && s["underlyingType"] == "COIN"
                    && s["onboardDate"].as_i64().is_some_and(|onboard_ms| {
                        onboard_ms
                            <= scan_ms.saturating_sub(cfg.min_contract_age_days as i64 * DAY_MS)
                    })
            })
            .filter_map(|s| s["symbol"].as_str().map(str::to_owned))
            .collect();
        let cross_interval_ms = cross_cfg.hold_hours as i64 * 3_600_000;
        let cross_boundary_ms = scan_ms.div_euclid(cross_interval_ms) * cross_interval_ms;
        let scheduled_cross_signal_ms = cross_boundary_ms - 1;
        let scheduled_cross_due = cross_cfg.enabled
            && scan_ms.saturating_sub(cross_boundary_ms)
                <= cfg.max_signal_age_seconds as i64 * 1_000
            && state.seen_signal.get("__cross_section__").copied()
                != Some(scheduled_cross_signal_ms);
        // A full protective exit frees the slot immediately.  On the next
        // closed 15m bar rerank the remaining universe instead of idling until
        // the next 3h wall-clock boundary.  Symbols already stopped/realized
        // during this 3h window are excluded to prevent same-signal churn.
        let (cross_reentry_exclusions, latest_cross_exit_ms) =
            cross_protective_exits_since(&state, cross_boundary_ms);
        let cross_position_open = state.positions.values().any(is_cross_position);
        let cross_15m_boundary_ms = scan_ms.div_euclid(15 * 60_000) * (15 * 60_000);
        let cross_early_signal_ms = cross_15m_boundary_ms - 1;
        let early_cross_due = cross_cfg.enabled
            && !scheduled_cross_due
            && !cross_position_open
            && !cross_reentry_exclusions.is_empty()
            && latest_cross_exit_ms.is_some_and(|exit_ms| cross_15m_boundary_ms > exit_ms)
            && scan_ms.saturating_sub(cross_15m_boundary_ms)
                <= cfg.max_signal_age_seconds as i64 * 1_000
            && state.seen_signal.get("__cross_section_early__").copied()
                != Some(cross_early_signal_ms);
        let cross_execution_due = scheduled_cross_due || early_cross_due;
        let cross_evaluation_boundary_ms = if early_cross_due {
            cross_15m_boundary_ms
        } else {
            cross_boundary_ms
        };
        let cross_signal_ms = cross_evaluation_boundary_ms - 1;
        // 老版本的持久化状态可能已经带有 cross_section，但还没有 gate/execution。
        // 此时立刻拉取完整横截面重建展示状态，但不执行当前 3h 边界的陈旧候选。
        let cross_status_refresh_due =
            cross_cfg.enabled && !cross_section_status_complete(&state.cross_section_status);
        let cross_analysis_due = cross_execution_due || cross_status_refresh_due;
        let shock_interval_ms = shock_cfg.gate_refresh_hours.max(1) as i64 * 3_600_000;
        let shock_boundary_ms = scan_ms.div_euclid(shock_interval_ms) * shock_interval_ms;
        let shock_gate_key = "__shock_gate__";
        let shock_analysis_due = shock_cfg.enabled
            && (state.shock_reversal_status.is_null()
                || state.seen_signal.get(shock_gate_key).copied() != Some(shock_boundary_ms));
        let mut shortlist: Vec<(String, f64)> = tickers
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| {
                let symbol = t["symbol"].as_str()?.to_owned();
                let volume = t["quoteVolume"].as_str()?.parse::<f64>().ok()?;
                let change = t["priceChangePercent"].as_str()?.parse::<f64>().ok()?.abs();
                let common = active.contains(&symbol)
                    && execution_symbols
                        .as_ref()
                        .is_none_or(|symbols| symbols.contains(&symbol))
                    && spot_symbols.contains(&symbol)
                    && !excluded.contains(symbol.as_str());
                let selected = if (cross_cfg.enabled && cross_analysis_due) || shock_analysis_due {
                    // 重建过去 30 个已完成信号时，不能只看“当前”成交额，否则会漏掉
                    // 历史截面曾满足 1000 万门槛、现在刚掉出门槛的币，造成幸存者偏差。
                    // 每个历史截面的真实 24h 成交额由 cross_ranks_at 再因果过滤。
                    common
                } else if shock_cfg.enabled {
                    // 快反转发生在冲击已经开始回吐之后，24h 净涨跌可能刚好落回
                    // 旧的 4% 榜单外。放宽预筛到 2%，真正入场仍由 1h 冲击、
                    // 扫高/扫低、反向强收和实时可成交性共同决定。
                    common && volume >= cfg.min_24h_volume_usd && change >= 2.0
                } else {
                    common && volume >= cfg.min_24h_volume_usd && change >= 4.0
                };
                selected.then_some((symbol, volume * (1.0 + change / 100.0)))
            })
            .collect();
        shortlist.sort_by(|a, b| b.1.total_cmp(&a.1));
        if !cross_analysis_due && !shock_analysis_due {
            shortlist.truncate(cfg.scan_limit);
        }
        // 已持仓标的即使跌出动量扫描池也必须继续取价、跟踪止损和时间退出。
        for symbol in state.positions.keys() {
            if !shortlist.iter().any(|(item, _)| item == symbol) {
                shortlist.push((symbol.clone(), f64::INFINITY));
            }
        }
        // 已经进入“突破 -> 回踩/反抽确认”状态机的标的必须持续取 K 线，
        // 即使它暂时跌出 24h 动量榜，否则候选会永远卡在等待确认。
        for symbol in state.pending_entries.keys() {
            if !shortlist.iter().any(|(item, _)| item == symbol) {
                shortlist.push((symbol.clone(), f64::INFINITY));
            }
        }
        // 衰竭通道必须拿到脉冲后的第一根闭合 K；即使标的暂时掉出涨幅榜也继续追踪。
        for symbol in state.pulse_exhaustion_setups.keys() {
            if !shortlist.iter().any(|(item, _)| item == symbol) {
                shortlist.push((symbol.clone(), f64::INFINITY));
            }
        }
        // 观测样本在两小时结果窗口内持续取价，候选即使跌出榜单也不能丢失标签。
        for trial in state.microstructure_trials.values() {
            if !shortlist.iter().any(|(item, _)| item == &trial.symbol) {
                shortlist.push((trial.symbol.clone(), f64::INFINITY));
            }
        }
        let shortlist_count = shortlist.len();
        let mut set = tokio::task::JoinSet::new();
        let kline_limit = Arc::new(tokio::sync::Semaphore::new(20));
        let history_bars = if shock_analysis_due || cross_analysis_due {
            // 1000 根 15m K 已覆盖约 10.4 天，足够重建 30 个 3h 截面和
            // 冲击反转门控；保持单请求可避免首次启动超过 Binance 权重上限。
            1_000
        } else {
            700
        };
        for (symbol, _) in shortlist {
            let client = http.clone();
            let permits = kline_limit.clone();
            set.spawn(async move {
                let _permit = permits.acquire_owned().await?;
                fetch_bars(client, symbol, history_bars).await
            });
        }
        let mut bars_by_symbol = HashMap::new();
        let mut warmup_completed = 0usize;
        let mut warmup_failures = 0usize;
        while let Some(result) = set.join_next().await {
            match result {
                Ok(Ok((symbol, bars))) => {
                    bars_by_symbol.insert(symbol, bars);
                }
                Ok(Err(e)) => {
                    warmup_failures += 1;
                    warn!(error=%format!("{e:#}"), "候选 K 线拉取失败");
                }
                Err(e) => {
                    warmup_failures += 1;
                    warn!(error=%e, "候选任务失败");
                }
            }
            warmup_completed += 1;
            if cross_analysis_due || shock_analysis_due {
                if let Some(tx) = &status_tx {
                    let _ = tx.send(json!({
                        "state":"running", "component":"altcoin", "mode":mode.as_str(),
                        "started_at_ms":started_ms, "equity":state.cash, "cash":state.cash,
                        "n_intents":state.total_entries, "n_fills":state.total_entries+state.total_exits,
                        "execution_healthy":true,
                        "warmup":{
                            "active":true,"stage":"loading_market_history",
                            "completed":warmup_completed,"total":shortlist_count,
                            "failures":warmup_failures
                        }
                    }));
                }
            }
        }
        if shock_analysis_due {
            let mut status =
                shock_gate_analysis(&bars_by_symbol, shock_boundary_ms, &shock_cfg, &cfg);
            status["performance"] = shock_performance(&state);
            state.shock_reversal_status = status.clone();
            state
                .seen_signal
                .insert(shock_gate_key.to_owned(), shock_boundary_ms);
            append_event(
                &event_path,
                json!({"ts_ms":scan_ms,"event":"shock_reversal_gate_refreshed","status":status,"history_symbols":bars_by_symbol.len()}),
            )?;
            save_state(&state_path, &state)?;
        }
        for outcome in update_microstructure_trials(&mut state, &bars_by_symbol, scan_ms) {
            append_event(&event_path, outcome)?;
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
        if state.first_week_started_ms <= 0 || state.first_week_start_equity <= 0.0 {
            state.first_week_started_ms = scan_ms;
            state.first_week_start_equity = current_equity;
            state.first_week_complete_latched = false;
            state.first_week_loss_latched = false;
            append_event(
                &event_path,
                json!({
                    "ts_ms":scan_ms,"event":"first_week_window_started",
                    "start_equity":current_equity,
                    "duration_days":cfg.first_week_duration_days
                }),
            )?;
            save_state(&state_path, &state)?;
        }
        let current_day = risk_day(scan_ms);
        if current_day != state.day {
            state.day = current_day;
            state.day_start_equity = current_equity;
            state.daily_entries = 0;
            state.daily_entry_bonus = 0;
            state.daily_risk_resets = 0;
            state.daily_loss_latched = false;
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
            match manage_live_positions(
                &mut state,
                client,
                &cfg,
                &cross_cfg,
                &shock_cfg,
                scan_ms,
                &event_path,
            )
            .await
            {
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
                let current_return =
                    position.side as f64 * (bar.close / position.entry_price - 1.0);
                // 15m OHLC 无法知道同一根 K 线的高低点先后。已经生效的保护单
                // 必须优先于本根才可能触发的止盈/跟踪更新，避免用未来高点美化结果。
                if let Some(raw_exit) =
                    existing_stop_raw_fill(position.side, position.stop_price, bar)
                {
                    let exit =
                        adverse_fill_price(raw_exit, position.side, cfg.dry_slippage_bps, false);
                    let fee = position.qty * exit * 0.0005;
                    let pnl = record_exit(
                        &mut state,
                        &position,
                        scan_ms,
                        position_cooldown_hours(&position, &cfg, &shock_cfg),
                        exit,
                        position.qty,
                        fee,
                    );
                    let trade_pnl = position.realized_partial_pnl + pnl;
                    let reason = match position.protection_reason.as_str() {
                        "trailing_take_profit" => "trailing_take_profit",
                        "recovery_profit_lock" => "recovery_profit_lock",
                        "partial_take_profit_break_even" => "partial_take_profit_break_even",
                        _ => "initial_stop",
                    };
                    let (max_favorable_excursion, max_adverse_excursion) =
                        position_excursions(&position);
                    let event = json!({"ts_ms":scan_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":reason,"price":exit,"raw_price":raw_exit,"qty":position.qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":scan_ms-position.entry_ms,"max_favorable_excursion":max_favorable_excursion,"max_adverse_excursion":max_adverse_excursion,"dry_slippage_bps":cfg.dry_slippage_bps,"dry_fill":true,"intrabar_policy":"existing_stop_first","overextension_long_blocked":state.overextension_long_blocked});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                    continue;
                }
                if cfg.fixed_time_exit_only {
                    let timed =
                        scan_ms - position.entry_ms >= cfg.max_hold_hours as i64 * 3_600_000;
                    if timed {
                        let raw_exit = bar.close;
                        let exit = adverse_fill_price(
                            raw_exit,
                            position.side,
                            cfg.dry_slippage_bps,
                            false,
                        );
                        let fee = position.qty * exit * 0.0005;
                        let pnl = record_exit(
                            &mut state,
                            &position,
                            scan_ms,
                            position_cooldown_hours(&position, &cfg, &shock_cfg),
                            exit,
                            position.qty,
                            fee,
                        );
                        let (max_favorable_excursion, max_adverse_excursion) =
                            position_excursions(&position);
                        let event = json!({"ts_ms":scan_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"scheduled_rebalance","price":exit,"raw_price":raw_exit,"qty":position.qty,"pnl":pnl,"trade_pnl":pnl,"fee":fee,"hold_ms":scan_ms-position.entry_ms,"max_favorable_excursion":max_favorable_excursion,"max_adverse_excursion":max_adverse_excursion,"dry_slippage_bps":cfg.dry_slippage_bps,"dry_fill":true});
                        append_event(&event_path, event.clone())?;
                        state.record_trade(event);
                    } else {
                        state.positions.insert(symbol, position);
                    }
                    continue;
                }
                if cfg.loss_trim_enabled
                    && !position.loss_trim_done
                    && current_return <= -cfg.loss_trim_trigger_pct
                {
                    let raw_exit = position.entry_price
                        * (1.0 - position.side as f64 * cfg.loss_trim_trigger_pct);
                    let exit =
                        adverse_fill_price(raw_exit, position.side, cfg.dry_slippage_bps, false);
                    let qty = position.qty * cfg.loss_trim_fraction;
                    let fee = qty * exit * 0.0005;
                    let partial_pnl =
                        record_partial_exit(&mut state, &mut position, exit, qty, fee);
                    position.loss_trim_done = true;
                    position.last_partial_exit_ms = Some(scan_ms);
                    let event = json!({"ts_ms":scan_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"loss_trim","trigger_return":-cfg.loss_trim_trigger_pct,"configured_fraction":cfg.loss_trim_fraction,"price":exit,"raw_price":raw_exit,"qty":qty,"remaining_qty":position.qty,"pnl":partial_pnl,"fee":fee,"stop_price":position.stop_price,"dry_slippage_bps":cfg.dry_slippage_bps,"dry_fill":true});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                }
                let (trail_activation_pct, trail_pct) =
                    position_exit_parameters(&position, &cfg, &cross_cfg, &shock_cfg);
                if !is_shock_position(&position)
                    && !position.partial_take_profit_done
                    && excursion >= trail_activation_pct
                {
                    let raw_exit =
                        position.entry_price * (1.0 + position.side as f64 * trail_activation_pct);
                    let exit =
                        adverse_fill_price(raw_exit, position.side, cfg.dry_slippage_bps, false);
                    let partial_take_profit_fraction =
                        position_partial_take_profit_fraction(&position, &cfg, &cross_cfg);
                    let qty = position.qty * partial_take_profit_fraction;
                    let fee = qty * exit * 0.0005;
                    let partial_pnl =
                        record_partial_exit(&mut state, &mut position, exit, qty, fee);
                    position.partial_take_profit_done = true;
                    position.last_partial_exit_ms = Some(scan_ms);
                    position.stop_price = position.entry_price;
                    position.protection_reason = "partial_take_profit_break_even".to_owned();
                    let event = json!({"ts_ms":scan_ms,"event":"partial_exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":"partial_take_profit","configured_trigger_return":trail_activation_pct,"configured_fraction":partial_take_profit_fraction,"configured_trail_pct":trail_pct,"price":exit,"raw_price":raw_exit,"qty":qty,"remaining_qty":position.qty,"pnl":partial_pnl,"fee":fee,"new_stop":position.stop_price,"dry_slippage_bps":cfg.dry_slippage_bps,"dry_fill":true});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                }
                if cfg.recovery_lock_enabled {
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
                }
                if excursion >= trail_activation_pct {
                    let trail = position.extreme * (1.0 - position.side as f64 * trail_pct);
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
                let failed = cfg.failed_breakout_enabled
                    && failed_breakout(
                        &position,
                        bar.close,
                        scan_ms,
                        cfg.failed_breakout_window_minutes,
                        cfg.failed_breakout_adverse_pct,
                        cfg.failed_breakout_max_mfe_pct,
                    );
                let timed = !is_cross_position(&position)
                    && scan_ms - position.entry_ms
                        >= position_max_hold_hours(&position, &cfg, &cross_cfg, &shock_cfg)
                            * 3_600_000;
                if failed || stopped || timed {
                    let raw_exit = if failed {
                        bar.close
                    } else if stopped {
                        position.stop_price
                    } else {
                        bar.close
                    };
                    let exit =
                        adverse_fill_price(raw_exit, position.side, cfg.dry_slippage_bps, false);
                    let fee = position.qty * exit * 0.0005;
                    let pnl = record_exit(
                        &mut state,
                        &position,
                        scan_ms,
                        position_cooldown_hours(&position, &cfg, &shock_cfg),
                        exit,
                        position.qty,
                        fee,
                    );
                    let trade_pnl = position.realized_partial_pnl + pnl;
                    let reason = if failed {
                        "failed_breakout"
                    } else if timed {
                        "time"
                    } else {
                        match position.protection_reason.as_str() {
                            "trailing_take_profit" => "trailing_take_profit",
                            "recovery_profit_lock" => "recovery_profit_lock",
                            "partial_take_profit_break_even" => "partial_take_profit_break_even",
                            _ => "initial_stop",
                        }
                    };
                    let (max_favorable_excursion, max_adverse_excursion) =
                        position_excursions(&position);
                    let event = json!({"ts_ms":scan_ms,"event":"exit","symbol":symbol,"side":position.side,"entry_phase":position.entry_phase,"origin_signal_ms":position.setup_origin_ms,"reason":reason,"price":exit,"raw_price":raw_exit,"qty":position.qty,"pnl":pnl,"trade_pnl":trade_pnl,"fee":fee,"hold_ms":scan_ms-position.entry_ms,"max_favorable_excursion":max_favorable_excursion,"max_adverse_excursion":max_adverse_excursion,"dry_slippage_bps":cfg.dry_slippage_bps,"dry_fill":true,"overextension_long_blocked":state.overextension_long_blocked});
                    append_event(&event_path, event.clone())?;
                    state.record_trade(event);
                } else {
                    state.positions.insert(symbol, position);
                }
            }
        }

        let mut candidates: Vec<Candidate> = if cross_cfg.enabled {
            if cross_analysis_due {
                // Position management runs after the initial scheduling check.
                // Re-read exits here so a stop filled exactly on a 3h boundary
                // cannot be immediately reopened from the same signal.
                let (current_reentry_exclusions, _) =
                    cross_protective_exits_since(&state, cross_boundary_ms);
                let (mut candidates, mut status) = cross_section_analysis(
                    &bars_by_symbol,
                    cross_evaluation_boundary_ms,
                    &cross_cfg,
                    &cfg,
                    &current_reentry_exclusions,
                );
                let post_exit_excess = status["signal_excess_return"].as_f64();
                let post_exit_quality_passed = !early_cross_due
                    || post_exit_excess.is_some_and(|excess| {
                        excess + f64::EPSILON >= cross_cfg.strong_excess_return
                    });
                if !post_exit_quality_passed {
                    candidates.clear();
                    status["stage"] = json!("post_exit_quality_missing");
                }
                let live_gate_open = status["gate"]["open"].as_bool().unwrap_or(false);
                if mode == TradeMode::Live && !live_gate_open {
                    for candidate in &mut candidates {
                        candidate
                            .blockers
                            .push("实盘影子门控未开启：仅继续观测，不用真钱探索".into());
                    }
                    status["stage"] = json!("live_gate_blocked");
                }
                status["performance"] = cross_performance(&state);
                status["evaluation_kind"] = json!(if early_cross_due {
                    "post_exit_15m_rerank"
                } else {
                    "scheduled_3h_rerank"
                });
                status["next_boundary_ms"] = json!(cross_boundary_ms + cross_interval_ms);
                status["post_exit_min_excess_return"] = json!(cross_cfg.strong_excess_return);
                status["post_exit_quality_passed"] = json!(post_exit_quality_passed);
                if cross_execution_due {
                    let signal_key = if early_cross_due {
                        "__cross_section_early__"
                    } else {
                        "__cross_section__"
                    };
                    state
                        .seen_signal
                        .insert(signal_key.to_owned(), cross_signal_ms);
                    state.cross_section_pending = candidates.clone();
                    let primary_symbols = candidates
                        .iter()
                        .filter(|item| item.entry_trigger == "scheduled_cross_section_momentum")
                        .map(|item| item.symbol.clone())
                        .collect::<Vec<_>>();
                    let reserve_symbols = candidates
                        .iter()
                        .filter(|item| {
                            item.entry_trigger == "scheduled_cross_section_momentum_reserve"
                        })
                        .map(|item| item.symbol.clone())
                        .collect::<Vec<_>>();
                    status["execution_state"] = if let Some(candidate) = candidates.first() {
                        json!({
                            "status":"awaiting_liquidity",
                            "symbol":candidate.symbol,
                            "side":candidate.side,
                            "symbols":primary_symbols,
                            "reserve_symbols":reserve_symbols,
                            "legs_total":cross_cfg.names,
                            "candidates_pending":candidates.len(),
                            "retry_count":0,
                            "created_ms":scan_ms,
                            "expires_ms":cross_signal_ms + cfg.max_signal_age_seconds as i64 * 1_000,
                            "next_retry_ms":scan_ms
                        })
                    } else {
                        json!({"status":"no_candidate","created_ms":scan_ms})
                    };
                } else {
                    candidates.clear();
                }
                state.cross_section_status = status.clone();
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":if early_cross_due {"cross_section_post_exit_rerank"} else if cross_execution_due {"cross_section_decision"} else {"cross_section_status_migrated"},"status":status,"candidate_count":candidates.len(),"execution_due":cross_execution_due,"evaluation_kind":if early_cross_due {"post_exit_15m_rerank"} else {"scheduled_3h_rerank"}}),
                )?;
                save_state(&state_path, &state)?;
                candidates
            } else if !state.cross_section_pending.is_empty() {
                let pending = state.cross_section_pending.clone();
                let candidate = pending[0].clone();
                let expires_ms = candidate.signal_ms + cfg.max_signal_age_seconds as i64 * 1_000;
                if scan_ms <= expires_ms {
                    pending
                } else {
                    state.cross_section_pending.clear();
                    state.cross_section_status["execution_state"] = json!({
                        "status":"expired",
                        "symbol":candidate.symbol,
                        "side":candidate.side,
                        "symbols":pending.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>(),
                        "legs_expired":pending.len(),
                        "expired_ms":scan_ms,
                        "expires_ms":expires_ms,
                        "next_boundary_ms":cross_boundary_ms + cross_interval_ms
                    });
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"cross_section_execution_expired","symbol":candidate.symbol,"side":candidate.side,"symbols":pending.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>(),"legs_expired":pending.len(),"signal_ms":candidate.signal_ms,"expires_ms":expires_ms,"next_boundary_ms":cross_boundary_ms+cross_interval_ms}),
                    )?;
                    save_state(&state_path, &state)?;
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        } else {
            bars_by_symbol
                .iter()
                .filter_map(|(s, bars)| evaluate(s.clone(), bars, &cfg))
                .collect()
        };
        if cross_execution_due {
            let rebalance_equity = equity(&state, &prices);
            let changed = if let Some(client) = rest.as_ref() {
                resolve_live_cross_rebalance(
                    &mut state,
                    &mut candidates,
                    client,
                    rebalance_equity,
                    scan_ms,
                    &event_path,
                )
                .await?
            } else {
                resolve_dry_cross_rebalance(
                    &mut state,
                    &mut candidates,
                    &bars_by_symbol,
                    &cfg,
                    rebalance_equity,
                    scan_ms,
                    &event_path,
                )?
            };
            if changed {
                save_state(&state_path, &state)?;
            }
        }
        let mut shock_candidates = if shock_cfg.enabled {
            bars_by_symbol
                .iter()
                .filter_map(|(symbol, bars)| {
                    shock_reversal_candidate_at(
                        symbol,
                        bars,
                        bars.len().checked_sub(1)?,
                        &shock_cfg,
                        cfg.min_24h_volume_usd,
                    )
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        shock_candidates.sort_by(|left, right| right.score.total_cmp(&left.score));
        for candidate in &mut shock_candidates {
            let gate_open = shock_gate_open(&state.shock_reversal_status, candidate.side);
            if !gate_open {
                let side = if candidate.side > 0 { "long" } else { "short" };
                let gate = &state.shock_reversal_status["gate"][side];
                candidate.blockers.push(format!(
                    "{} 侧滚动门控未开启：{}/{} 样本，PF {:.2}",
                    side,
                    gate["samples"].as_u64().unwrap_or(0),
                    gate["required_samples"]
                        .as_u64()
                        .unwrap_or(shock_cfg.gate_window as u64),
                    gate["profit_factor"].as_f64().unwrap_or(0.0)
                ));
            }
            let seen_key = format!("__shock__{}", candidate.symbol);
            if state.seen_signal.get(&seen_key).copied() == Some(candidate.signal_ms) {
                candidate.blockers.push("本根冲击反转 K 已评估".into());
            } else {
                state.seen_signal.insert(seen_key, candidate.signal_ms);
                observe_microstructure_trial(
                    &mut state,
                    "shock_reversal",
                    candidate,
                    candidate.signal_ms,
                    candidate.price,
                    shock_cfg.max_hold_hours as i64 * 3_600_000,
                );
                append_event(
                    &event_path,
                    json!({
                        "ts_ms":scan_ms,"event":"shock_reversal_signal",
                        "symbol":candidate.symbol,"side":candidate.side,
                        "signal_ms":candidate.signal_ms,"gate_open":gate_open,
                        "quality":if candidate.entry_trigger=="strict_shock_reversal" {"strict"} else {"broad"},
                        "signal":candidate
                    }),
                )?;
            }
        }
        if shock_cfg.enabled {
            state.shock_reversal_status["stage"] =
                json!(if state.positions.values().any(is_shock_position) {
                    "position"
                } else if shock_candidates.iter().any(Candidate::eligible) {
                    "execution"
                } else if shock_candidates.is_empty() {
                    "scan"
                } else {
                    "gate_blocked"
                });
            state.shock_reversal_status["latest_candidates"] =
                json!(shock_candidates.iter().take(10).collect::<Vec<_>>());
            state.shock_reversal_status["last_scan_ms"] = json!(scan_ms);
            state.shock_reversal_status["performance"] = shock_performance(&state);
            candidates.extend(shock_candidates.iter().cloned());
        }
        let mut pulse_initial_candidates: Vec<Candidate> = if pulse_exhaustion_active {
            bars_by_symbol
                .iter()
                .filter_map(|(symbol, bars)| evaluate_pulse_impulse(symbol.clone(), bars, &cfg))
                .collect()
        } else {
            Vec::new()
        };
        let broad_market_up = state.cross_section_status["market_regime"]
            .as_str()
            .is_some_and(|regime| regime == "broad_up");
        if broad_market_up {
            for candidate in &mut pulse_initial_candidates {
                candidate
                    .blockers
                    .push("全市场 12h 中位数处于 broad_up，禁止逆势衰竭做空".into());
            }
        }
        if let Some(symbols) = execution_symbols.as_ref() {
            for candidate in &mut candidates {
                if !symbols.contains(&candidate.symbol) {
                    candidate
                        .blockers
                        .push("当前 Binance 执行端点不支持该永续合约".into());
                }
            }
        }
        // 原始突破只能在刚闭合时登记为待确认候选；极端延续探针也只能在这个
        // 窗口直接执行。回踩/反抽确认产生的是一枚新的闭合 K 线信号，因此同样
        // 受此执行时效约束，避免重启或仓位释放后回头追旧结构。
        let max_signal_age_ms = cfg.max_signal_age_seconds as i64 * 1_000;
        for candidate in &mut candidates {
            if signal_age_ms(scan_ms, candidate.signal_ms) > max_signal_age_ms {
                candidate.blockers.push(format!(
                    "已超过 {} 秒入场执行窗口",
                    cfg.max_signal_age_seconds
                ));
            }
        }
        for candidate in &mut pulse_initial_candidates {
            if signal_age_ms(scan_ms, candidate.signal_ms) > max_signal_age_ms {
                candidate.blockers.push(format!(
                    "独立脉冲 K 已超过 {} 秒实时窗口",
                    cfg.max_signal_age_seconds
                ));
            }
        }
        candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
        pulse_initial_candidates.sort_by(|a, b| b.score.total_cmp(&a.score));
        let nearest_pulse_initial = pulse_initial_candidates
            .iter()
            .find(|candidate| candidate.return_1h > 0.0)
            .cloned();
        // OI、现货同步与永续溢价先作为观察字段完整记录，不在尚未历史验证前
        // 偷偷增加硬门槛。仅丰富最接近的 10 个，控制公共 API 权重。
        let enrich_count = if cross_cfg.enabled {
            0
        } else {
            candidates.len().min(10)
        };
        let mut enrich = tokio::task::JoinSet::new();
        for (index, candidate) in candidates.iter().take(enrich_count).cloned().enumerate() {
            let client = http.clone();
            enrich.spawn(async move { (index, enrich_candidate(client, candidate).await) });
        }
        while let Some(Ok((index, candidate))) = enrich.join_next().await {
            candidates[index] = candidate;
        }

        let mut pulse_execution_candidates = Vec::new();
        if pulse_exhaustion_active {
            // Arm from a dedicated pulse observation.  Main-strategy continuation gates
            // (24h breakout, 4h band, path efficiency and strong close) are intentionally
            // absent: only the closed-bar pulse thresholds and the shared tradable
            // universe are relevant before the next-bar exhaustion confirmation.
            for candidate in pulse_initial_candidates
                .iter()
                .filter(|candidate| candidate.eligible())
            {
                if state.positions.contains_key(&candidate.symbol)
                    || state
                        .pulse_exhaustion_setups
                        .contains_key(&candidate.symbol)
                    || state.pulse_seen_signal.get(&candidate.symbol).copied()
                        == Some(candidate.signal_ms)
                {
                    continue;
                }
                let setup = PulseExhaustionSetup {
                    symbol: candidate.symbol.clone(),
                    origin_signal_ms: candidate.signal_ms,
                    initial_price: candidate.price,
                    initial_return_1h: candidate.return_1h,
                    initial_volume_ratio: candidate.volume_ratio,
                    volume_24h: candidate.volume_24h,
                };
                state
                    .pulse_seen_signal
                    .insert(candidate.symbol.clone(), candidate.signal_ms);
                state
                    .pulse_exhaustion_setups
                    .insert(candidate.symbol.clone(), setup.clone());
                if rest.is_some() {
                    let stop_pct = candidate_stop_pct(candidate, &cfg, &cross_cfg, &shock_cfg);
                    let observation_notional = current_equity
                        * cfg.risk_per_trade
                        * cfg.pulse_risk_scale
                        * cfg.short_risk_scale
                        / (stop_pct + cfg.risk_execution_buffer_pct);
                    match market_rest
                        .liquidity_snapshot(
                            &candidate.symbol,
                            -1,
                            observation_notional.max(20.0),
                            cfg.depth_band_pct,
                            cfg.recent_trade_window_seconds as i64 * 1_000,
                        )
                        .await
                    {
                        Ok(snapshot) => {
                            let setup_id = microstructure_trial_key(
                                "pulse_exhaustion_short",
                                &candidate.symbol,
                                candidate.signal_ms,
                            );
                            let observation = json!({
                                "ts_ms":scan_ms,
                                "event":"entry_microstructure_observation",
                                "strategy":"pulse_exhaustion_short",
                                "setup_id":setup_id,
                                "stage":"signal",
                                "symbol":candidate.symbol,
                                "side":-1,
                                "origin_signal_ms":candidate.signal_ms,
                                "target_notional":observation_notional,
                                "source":"binance_mainnet_public_rest_agg_trades_and_depth",
                                "snapshot":snapshot
                            });
                            state.latest_pulse_signal_microstructure = observation.clone();
                            append_event(&event_path, observation)?;
                        }
                        Err(error) => append_event(
                            &event_path,
                            json!({
                                "ts_ms":scan_ms,
                                "event":"entry_microstructure_observation",
                                "strategy":"pulse_exhaustion_short",
                                "stage":"signal",
                                "symbol":candidate.symbol,
                                "side":-1,
                                "origin_signal_ms":candidate.signal_ms,
                                "available":false,
                                "error":error.to_string()
                            }),
                        )?,
                    }
                }
                append_event(
                    &event_path,
                    json!({
                        "ts_ms":scan_ms,"event":"pulse_exhaustion_armed",
                        "symbol":candidate.symbol,"origin_signal_ms":candidate.signal_ms,
                        "initial_return_1h":candidate.return_1h,
                        "initial_volume_ratio":candidate.volume_ratio,
                        "next_bar_only":true,"entry_side":-1
                    }),
                )?;
            }

            let setup_symbols: Vec<String> =
                state.pulse_exhaustion_setups.keys().cloned().collect();
            for symbol in setup_symbols {
                let Some(setup) = state.pulse_exhaustion_setups.get(&symbol).cloned() else {
                    continue;
                };
                let evaluation = bars_by_symbol
                    .get(&symbol)
                    .and_then(|bars| pulse_exhaustion_candidate(&setup, bars, &cfg));
                let Some((candidate, peak_retrace, _)) = evaluation else {
                    if scan_ms > setup.origin_signal_ms + 15 * 60_000 + max_signal_age_ms {
                        state.pulse_exhaustion_setups.remove(&symbol);
                        state.latest_pulse_exhaustion = json!({
                            "ts_ms":scan_ms,"symbol":symbol,"stage":"expired",
                            "origin_signal_ms":setup.origin_signal_ms,
                            "reason":"未在实时执行窗口取得脉冲后的第一根闭合 15m K"
                        });
                        append_event(
                            &event_path,
                            json!({"ts_ms":scan_ms,"event":"pulse_exhaustion_expired","symbol":symbol,"origin_signal_ms":setup.origin_signal_ms,"reason":"missing_first_closed_bar"}),
                        )?;
                    }
                    continue;
                };
                // Only the immediately following bar is valid, and only while it is fresh.
                state.pulse_exhaustion_setups.remove(&symbol);
                let mut candidate = enrich_candidate(http.clone(), candidate).await;
                let metrics = apply_pulse_exhaustion_gates(&mut candidate, peak_retrace, &cfg);
                if state.cross_section_status["market_regime"].as_str() == Some("broad_up") {
                    candidate
                        .blockers
                        .push("全市场 12h 中位数处于 broad_up，禁止逆势衰竭做空".into());
                }
                if signal_age_ms(scan_ms, candidate.signal_ms) > max_signal_age_ms {
                    candidate.blockers.push(format!(
                        "确认 K 已超过 {} 秒实时执行窗口",
                        cfg.max_signal_age_seconds
                    ));
                }
                let eligible = candidate.eligible();
                let stop_pct = candidate_stop_pct(&candidate, &cfg, &cross_cfg, &shock_cfg);
                let pulse_observation_notional = current_equity
                    * cfg.risk_per_trade
                    * cfg.pulse_risk_scale
                    * cfg.short_risk_scale
                    / (stop_pct + cfg.risk_execution_buffer_pct);
                observe_microstructure_trial(
                    &mut state,
                    "pulse_exhaustion_short",
                    &candidate,
                    candidate.signal_ms,
                    candidate.price,
                    cfg.max_hold_hours as i64 * 3_600_000,
                );
                if rest.is_some() {
                    match market_rest
                        .liquidity_snapshot(
                            &candidate.symbol,
                            candidate.side,
                            pulse_observation_notional.max(20.0),
                            cfg.depth_band_pct,
                            cfg.recent_trade_window_seconds as i64 * 1_000,
                        )
                        .await
                    {
                        Ok(snapshot) => {
                            let setup_id = microstructure_trial_key(
                                "pulse_exhaustion_short",
                                &candidate.symbol,
                                candidate_origin_ms(&candidate),
                            );
                            let observation = json!({
                                "ts_ms":scan_ms,
                                "event":"entry_microstructure_observation",
                                "strategy":"pulse_exhaustion_short",
                                "setup_id":setup_id,
                                "stage":"confirmation",
                                "symbol":candidate.symbol,
                                "side":candidate.side,
                                "origin_signal_ms":candidate_origin_ms(&candidate),
                                "confirmation_ms":candidate.signal_ms,
                                "target_notional":pulse_observation_notional,
                                "eligible":eligible,
                                "source":"binance_mainnet_public_rest_agg_trades_and_depth",
                                "snapshot":snapshot
                            });
                            state.latest_pulse_confirmation_microstructure = observation.clone();
                            append_event(&event_path, observation)?;
                        }
                        Err(error) => append_event(
                            &event_path,
                            json!({
                                "ts_ms":scan_ms,
                                "event":"entry_microstructure_observation",
                                "strategy":"pulse_exhaustion_short",
                                "stage":"confirmation",
                                "symbol":candidate.symbol,
                                "side":candidate.side,
                                "origin_signal_ms":candidate_origin_ms(&candidate),
                                "confirmation_ms":candidate.signal_ms,
                                "eligible":eligible,
                                "available":false,
                                "error":error.to_string()
                            }),
                        )?,
                    }
                }
                state.latest_pulse_exhaustion = json!({
                    "ts_ms":scan_ms,"stage":if eligible && pulse_live_execution_allowed {"execution"} else if eligible {"shadow_observation"} else {"rejected"},
                    "symbol":symbol,"origin_signal_ms":setup.origin_signal_ms,
                    "confirmation_ms":candidate.signal_ms,
                    "initial_return_1h":setup.initial_return_1h,
                    "initial_volume_ratio":setup.initial_volume_ratio,
                    "metrics":metrics,"blockers":candidate.blockers,"eligible":eligible
                });
                append_event(
                    &event_path,
                    json!({
                        "ts_ms":scan_ms,"event":"pulse_exhaustion_evaluated",
                        "symbol":symbol,"origin_signal_ms":setup.origin_signal_ms,
                        "confirmation_ms":candidate.signal_ms,
                        "initial_return_1h":setup.initial_return_1h,
                        "initial_volume_ratio":setup.initial_volume_ratio,
                        "metrics":metrics,"blockers":candidate.blockers,"eligible":eligible,
                        "signal":candidate
                    }),
                )?;
                if eligible && !pulse_live_execution_allowed {
                    append_event(
                        &event_path,
                        json!({
                            "ts_ms":scan_ms,"event":"pulse_exhaustion_shadow_entry",
                            "strategy":"pulse_exhaustion_short","symbol":symbol,
                            "side":candidate.side,"entry_price":candidate.price,
                            "origin_signal_ms":candidate_origin_ms(&candidate),
                            "confirmation_ms":candidate.signal_ms,
                            "real_order_submitted":false,
                            "reason":"实盘资金资格关闭，继续影子跟踪 MFE/MAE/到期收益"
                        }),
                    )?;
                }
                if eligible && pulse_live_execution_allowed {
                    // A confirmed exhaustion short has precedence over an unfinished
                    // continuation-long setup for the same symbol.  Keep the sleeves
                    // independent while preventing contradictory orders on one account.
                    let cancelled_main_pending = state.pending_entries.remove(&symbol).is_some();
                    for regular in candidates
                        .iter_mut()
                        .filter(|regular| regular.symbol == symbol)
                    {
                        regular
                            .blockers
                            .push("独立衰竭做空已确认，取消同标的顺势候选".into());
                    }
                    if cancelled_main_pending {
                        append_event(
                            &event_path,
                            json!({
                                "ts_ms":scan_ms,"event":"strategy_conflict_resolved",
                                "symbol":symbol,"winner":"pulse_exhaustion_short",
                                "cancelled":"main_pending_continuation"
                            }),
                        )?;
                    }
                    pulse_execution_candidates.push(candidate);
                }
            }
        } else if !state.pulse_exhaustion_setups.is_empty() {
            state.pulse_exhaustion_setups.clear();
        }
        let overextension_long_open = state
            .positions
            .values()
            .any(|position| position.entry_phase == "overextended_long");
        for candidate in &mut candidates {
            if candidate.entry_phase != "overextended_long" {
                continue;
            }
            if !cfg.overextension_long_enabled {
                candidate
                    .blockers
                    .push("Paper 探索期关闭过度延伸追多".into());
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
        let crossed_daily_loss =
            managed_equity < state.day_start_equity * (1.0 - cfg.daily_loss_limit);
        if crossed_daily_loss && !state.daily_loss_latched {
            state.daily_loss_latched = true;
            append_event(
                &event_path,
                json!({"ts_ms":scan_ms,"event":"daily_loss_latched","equity":managed_equity,"baseline_equity":state.day_start_equity,"limit_pct":cfg.daily_loss_limit,"risk_day_timezone":"Asia/Shanghai","reason":"当日回撤门槛触发，锁死新开仓直到下个北京时间自然日或手动重置"}),
            )?;
            save_state(&state_path, &state)?;
        }
        if update_first_week_latches(&mut state, &cfg, managed_equity, scan_ms, &event_path)? {
            save_state(&state_path, &state)?;
        }
        let daily_loss_blocked = state.daily_loss_latched;
        let first_week = first_week_progress(&state, &cfg, managed_equity, scan_ms);
        let effective_daily_limit = cfg
            .max_daily_entries
            .saturating_add(state.daily_entry_bonus);
        let mut overextension_slot_taken = overextension_long_open;
        let mut execution_candidates = pulse_execution_candidates;
        execution_candidates.extend(
            shock_candidates
                .iter()
                .filter(|candidate| candidate.eligible())
                .cloned(),
        );
        if cross_cfg.enabled {
            execution_candidates.extend(
                candidates
                    .iter()
                    .filter(|candidate| is_cross_candidate(candidate) && candidate.eligible())
                    .cloned(),
            );
        }

        // 先推进已有候选。确认只读取信号之后已经闭合的 K 线，绝不使用未来数据：
        // 多头要求回踩突破位后重新收强，空头镜像为反抽跌破位后重新收弱。
        let pending_symbols: Vec<String> = state.pending_entries.keys().cloned().collect();
        for symbol in pending_symbols {
            let Some(mut pending) = state.pending_entries.get(&symbol).cloned() else {
                continue;
            };
            let mut terminal = None;
            if let (Some(confirmed_ms), Some(confirmed_price)) = (
                pending.intrabar_confirmed_ms,
                pending.intrabar_confirmed_price,
            ) {
                if signal_age_ms(scan_ms, confirmed_ms) > max_signal_age_ms {
                    terminal = Some((
                        "entry_setup_expired",
                        None,
                        "盘中收回确认已超过实时执行窗口",
                    ));
                } else {
                    let mut confirmed = pending.candidate.clone();
                    confirmed.signal_ms = confirmed_ms;
                    confirmed.price = confirmed_price;
                    confirmed.entry_trigger =
                        "vertical_impulse_intrabar_reclaim_confirmed".to_owned();
                    confirmed.risk_scale = 1.0;
                    confirmed.blockers.clear();
                    terminal = Some((
                        "entry_setup_intrabar_execution",
                        Some(confirmed),
                        "盘中浅回踩按时间顺序收回，进入完整执行检查",
                    ));
                }
            }
            if terminal.is_none() {
                if let Some(bars) = bars_by_symbol.get(&symbol) {
                    let unchecked: Vec<&Bar> = bars
                        .iter()
                        .filter(|bar| {
                            bar.close_ms > pending.last_checked_close_ms
                                && bar.close_ms > pending.candidate.signal_ms
                        })
                        .collect();
                    for bar in unchecked {
                        if bar.close_ms > pending.expires_ms {
                            terminal = Some((
                                "entry_setup_expired",
                                None,
                                "确认窗口内没有形成回踩/反抽后的重新启动",
                            ));
                            break;
                        }
                        pending.last_checked_close_ms = bar.close_ms;
                        let retest_anchor = if pending.retest_anchor > 0.0 {
                            pending.retest_anchor
                        } else {
                            pending.breakout_level
                        };
                        match pending_decision(
                            pending.candidate.side,
                            retest_anchor,
                            &pending.confirmation_mode,
                            bar,
                            pending.retest_seen,
                            &cfg,
                        ) {
                            PendingDecision::Invalidated => {
                                terminal = Some((
                                    "entry_setup_invalidated",
                                    None,
                                    "价格穿透突破位容忍区，原启动结构失效",
                                ));
                                break;
                            }
                            PendingDecision::Confirmed => {
                                if signal_age_ms(scan_ms, bar.close_ms) > max_signal_age_ms {
                                    terminal = Some((
                                        "entry_setup_expired",
                                        None,
                                        "确认 K 线已超过实时执行窗口，不在重启后追旧确认",
                                    ));
                                    break;
                                }
                                let mut confirmed = pending.candidate.clone();
                                confirmed.signal_ms = bar.close_ms;
                                confirmed.price = bar.close;
                                confirmed.entry_trigger =
                                    if pending.confirmation_mode == "vertical_impulse_retest" {
                                        "vertical_impulse_reclaim_confirmed".to_owned()
                                    } else {
                                        "retest_reclaim_confirmed".to_owned()
                                    };
                                confirmed.risk_scale = 1.0;
                                confirmed.blockers.clear();
                                terminal = Some((
                                    "entry_setup_confirmed",
                                    Some(confirmed),
                                    "回踩/反抽守住突破位并重新顺向收盘",
                                ));
                                break;
                            }
                            PendingDecision::RetestSeen => {
                                if !pending.retest_seen {
                                    pending.retest_seen = true;
                                    append_event(
                                        &event_path,
                                        json!({"ts_ms":scan_ms,"event":"entry_setup_retest_seen","symbol":symbol,"side":pending.candidate.side,"breakout_level":pending.breakout_level,"retest_anchor":retest_anchor,"confirmation_mode":pending.confirmation_mode,"bar":{"open_ms":bar.open_ms,"close_ms":bar.close_ms,"open":bar.open,"high":bar.high,"low":bar.low,"close":bar.close},"reason":"已触及自适应回踩区，等待后续独立 K 线重新顺向收盘"}),
                                    )?;
                                }
                            }
                            PendingDecision::Waiting => {}
                        }
                    }
                }
            }
            if terminal.is_none() && scan_ms > pending.expires_ms {
                terminal = Some(("entry_setup_expired", None, "确认窗口到期"));
            }
            if let Some((event_name, confirmed, reason)) = terminal {
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":event_name,"symbol":symbol,"side":pending.candidate.side,"origin_signal_ms":pending.candidate.signal_ms,"breakout_level":pending.breakout_level,"retest_anchor":pending.retest_anchor,"confirmation_mode":pending.confirmation_mode,"expires_ms":pending.expires_ms,"retest_seen":pending.retest_seen,"reason":reason}),
                )?;
                state.pending_entries.remove(&symbol);
                if let Some(confirmed) = confirmed {
                    execution_candidates.push(confirmed);
                }
            } else {
                state.pending_entries.insert(symbol, pending);
            }
        }

        // 新突破先登记候选。只有真正极端且放量足够的延续段允许直接小仓探路；
        // 该分支对多空完全镜像，并通过 risk_scale 降低单次错误追价的代价。
        for candidate in candidates.iter().filter(|candidate| {
            !cross_cfg.enabled && !is_shock_candidate(candidate) && candidate.eligible()
        }) {
            if state.positions.contains_key(&candidate.symbol)
                || state.pending_entries.contains_key(&candidate.symbol)
                || state.seen_signal.get(&candidate.symbol).copied() == Some(candidate.signal_ms)
            {
                continue;
            }
            observe_microstructure_trial(
                &mut state,
                "confirmed_volume_breakout",
                candidate,
                candidate.signal_ms,
                candidate.price,
                cfg.max_hold_hours as i64 * 3_600_000,
            );
            // Capture the tape and book when the setup first appears. This is
            // observation-only: historical tests show that same-direction
            // second-level Delta is often terminal crowding, not a universally
            // valid entry gate. The paired execution-time snapshot below lets
            // us learn whether flow strengthening, fading or flipping improves
            // this exact confirmed-retest strategy without suppressing trades.
            if rest.is_some() {
                let direction_risk_scale = if candidate.side > 0 {
                    cfg.long_risk_scale
                } else {
                    cfg.short_risk_scale
                };
                let observation_notional = managed_equity
                    * cfg.risk_per_trade
                    * candidate.risk_scale
                    * direction_risk_scale
                    / (candidate_stop_pct(candidate, &cfg, &cross_cfg, &shock_cfg)
                        + cfg.risk_execution_buffer_pct);
                match market_rest
                    .liquidity_snapshot(
                        &candidate.symbol,
                        candidate.side,
                        observation_notional.max(20.0),
                        cfg.depth_band_pct,
                        cfg.recent_trade_window_seconds as i64 * 1_000,
                    )
                    .await
                {
                    Ok(snapshot) => {
                        let observation = json!({
                            "ts_ms":scan_ms,
                            "stage":"signal",
                            "strategy":"confirmed_volume_breakout",
                            "setup_id":microstructure_trial_key("confirmed_volume_breakout", &candidate.symbol, candidate_origin_ms(candidate)),
                            "symbol":candidate.symbol,
                            "side":candidate.side,
                            "origin_signal_ms":candidate_origin_ms(candidate),
                            "target_notional":observation_notional,
                            "source":"binance_mainnet_public_rest_agg_trades_and_depth",
                            "snapshot":snapshot
                        });
                        state.latest_signal_microstructure = observation.clone();
                        state.latest_main_signal_microstructure = observation.clone();
                        let mut event = observation;
                        event["event"] = json!("entry_microstructure_observation");
                        append_event(&event_path, event)?;
                    }
                    Err(error) => append_event(
                        &event_path,
                        json!({
                            "ts_ms":scan_ms,
                            "event":"entry_microstructure_observation",
                            "stage":"signal",
                            "strategy":"confirmed_volume_breakout",
                            "symbol":candidate.symbol,
                            "side":candidate.side,
                            "origin_signal_ms":candidate_origin_ms(candidate),
                            "available":false,
                            "error":error.to_string()
                        }),
                    )?,
                }
            }
            state
                .seen_signal
                .insert(candidate.symbol.clone(), candidate.signal_ms);
            if cfg.direct_entry_enabled {
                let mut direct = candidate.clone();
                direct.entry_trigger = "direct_breakout".to_owned();
                direct.risk_scale = 1.0;
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":"direct_breakout_selected","symbol":direct.symbol,"side":direct.side,"signal_ms":direct.signal_ms,"breakout_level":direct.breakout_level,"return_1h":direct.return_1h,"return_4h":direct.return_4h,"volume_ratio":direct.volume_ratio}),
                )?;
                execution_candidates.push(direct);
            } else if cfg.extreme_direct_enabled && is_extreme_direct(candidate, &cfg) {
                let mut direct = candidate.clone();
                direct.entry_trigger = "extreme_continuation_probe".to_owned();
                direct.risk_scale = cfg.extreme_direct_risk_scale;
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":"extreme_direct_probe_selected","symbol":direct.symbol,"side":direct.side,"signal_ms":direct.signal_ms,"breakout_level":direct.breakout_level,"risk_scale":direct.risk_scale,"return_1h":direct.return_1h,"return_4h":direct.return_4h,"volume_ratio":direct.volume_ratio}),
                )?;
                execution_candidates.push(direct);
            } else if is_extreme_direct(candidate, &cfg) {
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":"extreme_direct_observed","symbol":candidate.symbol,"side":candidate.side,"signal_ms":candidate.signal_ms,"return_1h":candidate.return_1h,"return_4h":candidate.return_4h,"volume_ratio":candidate.volume_ratio,"reason":"极端延续直接追单已关闭，仅记录观察"}),
                )?;
            } else {
                let expires_ms =
                    candidate.signal_ms + cfg.confirmation_window_bars as i64 * 15 * 60_000;
                let (retest_anchor, confirmation_mode) = confirmation_plan(candidate, &cfg);
                let vertical = confirmation_mode == "vertical_impulse_retest";
                state.pending_entries.insert(
                    candidate.symbol.clone(),
                    PendingEntry {
                        candidate: candidate.clone(),
                        breakout_level: candidate.breakout_level,
                        retest_anchor,
                        confirmation_mode: confirmation_mode.clone(),
                        intrabar_touch_seen: false,
                        intrabar_touch_ms: None,
                        intrabar_extreme_price: None,
                        intrabar_last_price: None,
                        intrabar_confirmed_ms: None,
                        intrabar_confirmed_price: None,
                        expires_ms,
                        last_checked_close_ms: candidate.signal_ms,
                        retest_seen: false,
                    },
                );
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":"entry_setup_pending","symbol":candidate.symbol,"side":candidate.side,"signal_ms":candidate.signal_ms,"breakout_level":candidate.breakout_level,"signal_price":candidate.price,"retest_anchor":retest_anchor,"confirmation_mode":confirmation_mode,"overshoot_pct":candidate.side as f64*(candidate.price/candidate.breakout_level-1.0),"expires_ms":expires_ms,"confirmation_window_bars":cfg.confirmation_window_bars,"retest_touch_pct":if vertical {cfg.vertical_retest_touch_pct} else {cfg.retest_touch_pct},"retest_invalidation_pct":if vertical {cfg.vertical_retest_invalidation_pct} else {cfg.retest_invalidation_pct},"reclaim_pct":if vertical {cfg.vertical_reclaim_pct} else {cfg.reclaim_pct},"signal":candidate}),
                )?;
            }
        }

        // Confirmation can take up to an hour, so funding and basis must be
        // refreshed at execution time. Fail closed when either datum is
        // unavailable; stale positioning data must never bypass this gate.
        if !execution_candidates.is_empty() {
            let mut crowding_checked = Vec::with_capacity(execution_candidates.len());
            for candidate in execution_candidates {
                if is_cross_candidate(&candidate) {
                    crowding_checked.push(candidate);
                    continue;
                }
                let candidate = enrich_candidate(http.clone(), candidate).await;
                let max_directional_premium = candidate_max_directional_premium(&candidate, &cfg);
                if let Some(reason) = directional_crowding_reason(
                    candidate.side,
                    candidate.funding_rate,
                    candidate.perp_premium,
                    cfg.max_directional_funding_rate,
                    max_directional_premium,
                ) {
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"directional_crowding","symbol":candidate.symbol,"side":candidate.side,"entry_phase":candidate.entry_phase,"reason":&reason,"funding_rate":candidate.funding_rate,"perp_premium":candidate.perp_premium,"limits":{"max_directional_funding_rate":cfg.max_directional_funding_rate,"max_directional_premium":max_directional_premium}}),
                    )?;
                    state.note_execution_issue(
                        scan_ms,
                        &candidate.symbol,
                        "directional_crowding",
                        reason,
                    );
                    continue;
                }
                crowding_checked.push(candidate);
            }
            execution_candidates = crowding_checked;
        }
        execution_candidates.sort_by(|a, b| {
            execution_priority(b)
                .cmp(&execution_priority(a))
                .then_with(|| b.score.total_cmp(&a.score))
        });
        let pulse_eligible_count = execution_candidates
            .iter()
            .filter(|candidate| candidate.entry_phase == "pulse_exhaustion_short")
            .count();
        let regular_eligible_count = execution_candidates.len() - pulse_eligible_count;
        for candidate in &execution_candidates {
            if daily_entry_limit_reached(state.daily_entries, effective_daily_limit)
                || daily_loss_blocked
                || first_week.entries_blocked
            {
                break;
            }
            let pulse_candidate = candidate.entry_phase == "pulse_exhaustion_short";
            let cross_candidate = is_cross_candidate(candidate);
            let shock_candidate = is_shock_candidate(candidate);
            let micro_strategy = if pulse_candidate {
                "pulse_exhaustion_short"
            } else if cross_candidate {
                "cross_section_momentum"
            } else if shock_candidate {
                "shock_reversal"
            } else {
                "confirmed_volume_breakout"
            };
            let pulse_position_count = state
                .positions
                .values()
                .filter(|position| is_pulse_position(position))
                .count();
            let regular_position_count = state.positions.len().saturating_sub(pulse_position_count);
            if (pulse_candidate && pulse_position_count >= cfg.pulse_max_positions)
                || (!pulse_candidate && regular_position_count >= cfg.max_positions)
            {
                if !pulse_candidate {
                    let owner = state
                        .positions
                        .values()
                        .find(|position| !is_pulse_position(position))
                        .map(|position| {
                            json!({
                                "symbol":position.symbol,
                                "side":position.side,
                                "strategy":position.entry_phase
                            })
                        });
                    append_event(
                        &event_path,
                        json!({
                            "ts_ms":scan_ms,"event":"strategy_slot_conflict",
                            "candidate_symbol":candidate.symbol,"candidate_side":candidate.side,
                            "candidate_strategy":micro_strategy,"slot_owner":owner,
                            "resolution":"existing_position_kept",
                            "shared_regular_slots":cfg.max_positions
                        }),
                    )?;
                }
                continue;
            }
            if let Some(owner) = state.positions.get(&candidate.symbol) {
                append_event(
                    &event_path,
                    json!({
                        "ts_ms":scan_ms,"event":"strategy_symbol_conflict",
                        "symbol":candidate.symbol,
                        "candidate_side":candidate.side,"candidate_strategy":micro_strategy,
                        "existing_side":owner.side,"existing_strategy":owner.entry_phase,
                        "resolution":"existing_position_kept"
                    }),
                )?;
                continue;
            }
            if recently_exited_opposite_side(
                &state.recent_trades,
                &candidate.symbol,
                candidate.side,
                scan_ms,
                cfg.cooldown_hours,
            ) {
                append_event(
                    &event_path,
                    json!({
                        "ts_ms":scan_ms,"event":"strategy_direction_cooldown",
                        "symbol":candidate.symbol,"candidate_side":candidate.side,
                        "candidate_strategy":micro_strategy,
                        "cooldown_hours":cfg.cooldown_hours,
                        "reason":"同币刚由另一方向退出，禁止不同 alpha 立即反手"
                    }),
                )?;
                continue;
            }
            if (candidate.entry_phase == "overextended_long"
                && (!cfg.overextension_long_enabled
                    || overextension_slot_taken
                    || state.overextension_long_blocked))
                || (!pulse_candidate
                    && !cross_candidate
                    && state
                        .cooldown_until
                        .get(&candidate.symbol)
                        .copied()
                        .unwrap_or(0)
                        > scan_ms)
            {
                continue;
            }
            let gross: f64 = state
                .positions
                .values()
                .filter(|position| is_pulse_position(position) == pulse_candidate)
                .map(|position| position.initial_notional)
                .sum();
            let gross_limit = if pulse_candidate {
                cfg.pulse_max_gross_multiple
            } else if shock_cfg.enabled {
                // Cross-section and shock reversal share the regular sleeve.
                // Their own risk_scale sets each order size; the combined cap
                // must be common, otherwise entry order alone changes whether
                // the second strategy is allowed to participate.
                shock_cfg.max_gross_multiple
            } else {
                cfg.max_gross_multiple
            };
            let stop_pct = candidate_stop_pct(candidate, &cfg, &cross_cfg, &shock_cfg);
            let risk_distance = stop_pct + cfg.risk_execution_buffer_pct;
            let direction_risk_scale = if shock_candidate {
                1.0
            } else if candidate.side > 0 {
                cfg.long_risk_scale
            } else {
                cfg.short_risk_scale
            };
            let effective_risk_scale = candidate.risk_scale * direction_risk_scale;
            let requested_notional = if cross_candidate || shock_candidate {
                managed_equity * candidate.risk_scale
            } else {
                managed_equity * cfg.risk_per_trade * effective_risk_scale / risk_distance
            };
            let notional = requested_notional.min((managed_equity * gross_limit - gross).max(0.0));
            // A closed quality gate deliberately runs the five-name basket at
            // 0.05x total gross (~$10 per leg on the initial sleeve).  Let the
            // exchange's symbol-specific MIN_NOTIONAL make the final decision
            // instead of silently reducing the basket to zero legs here.
            let local_min_notional = if cross_candidate { 5.0 } else { 20.0 };
            if notional < local_min_notional {
                continue;
            }
            if rest.is_some() {
                let liquidity = match market_rest
                    .liquidity_snapshot(
                        &candidate.symbol,
                        candidate.side,
                        notional,
                        cfg.depth_band_pct,
                        cfg.recent_trade_window_seconds as i64 * 1_000,
                    )
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        let reason = format!("无法取得主网公开实时盘口/成交: {error}");
                        state.note_execution_issue(
                            scan_ms,
                            &candidate.symbol,
                            "liquidity_check",
                            reason.clone(),
                        );
                        append_event(
                            &event_path,
                            json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"liquidity_check","symbol":candidate.symbol,"reason":reason,"target_notional":notional}),
                        )?;
                        if cross_candidate {
                            state
                                .cross_section_pending
                                .retain(|item| item.symbol != candidate.symbol);
                            let retry_count = state.cross_section_status["execution_state"]
                                ["retry_count"]
                                .as_u64()
                                .unwrap_or(0)
                                + 1;
                            state.cross_section_status["execution_state"] = json!({
                                "status":"trying_next_candidate",
                                "symbol":candidate.symbol,
                                "side":candidate.side,
                                "retry_count":retry_count,
                                "last_check_ms":scan_ms,
                                "next_symbol":state.cross_section_pending.first().map(|item| item.symbol.clone()),
                                "expires_ms":candidate.signal_ms + max_signal_age_ms,
                                "reason":reason,
                                "resolution":"candidate_skipped_next_rank_immediate",
                                "market_data_source":"binance_mainnet_public"
                            });
                            save_state(&state_path, &state)?;
                        }
                        continue;
                    }
                };
                let required_depth = notional * cfg.min_depth_multiple;
                let (effective_spread_limit, liquid_market_spread_relaxation) =
                    effective_max_spread_bps(&cfg, &liquidity, required_depth);
                let mut blockers = Vec::new();
                if liquidity.spread_bps > effective_spread_limit {
                    blockers.push(format!(
                        "价差 {:.1}bps > {:.1}bps",
                        liquidity.spread_bps, effective_spread_limit
                    ));
                }
                if liquidity
                    .entry_impact_bps
                    .is_none_or(|impact| impact > cfg.max_entry_impact_bps)
                {
                    blockers.push(format!(
                        "入场冲击 {} > {:.1}bps",
                        liquidity
                            .entry_impact_bps
                            .map(|v| format!("{v:.1}bps"))
                            .unwrap_or_else(|| "盘口不足".into()),
                        cfg.max_entry_impact_bps
                    ));
                }
                if liquidity
                    .exit_impact_bps
                    .is_none_or(|impact| impact > cfg.max_exit_impact_bps)
                {
                    blockers.push(format!(
                        "退出冲击 {} > {:.1}bps",
                        liquidity
                            .exit_impact_bps
                            .map(|v| format!("{v:.1}bps"))
                            .unwrap_or_else(|| "盘口不足".into()),
                        cfg.max_exit_impact_bps
                    ));
                }
                if liquidity.bid_depth_usd < required_depth
                    || liquidity.ask_depth_usd < required_depth
                {
                    blockers.push(format!(
                        "±{:.1}% 双边深度 ${:.0}/${:.0}，要求各 ≥${:.0}",
                        cfg.depth_band_pct * 100.0,
                        liquidity.bid_depth_usd,
                        liquidity.ask_depth_usd,
                        required_depth
                    ));
                }
                if liquidity.recent_trade_count < cfg.min_recent_trades {
                    blockers.push(format!(
                        "近 {} 秒仅 {} 笔成交，要求 ≥{}",
                        cfg.recent_trade_window_seconds,
                        liquidity.recent_trade_count,
                        cfg.min_recent_trades
                    ));
                }
                let unique_trade_prices_warning =
                    liquidity.unique_trade_prices < cfg.min_unique_trade_prices;
                if cfg.unique_trade_prices_hard && unique_trade_prices_warning {
                    blockers.push(format!(
                        "近 {} 秒仅 {} 个成交价，要求 ≥{}",
                        cfg.recent_trade_window_seconds,
                        liquidity.unique_trade_prices,
                        cfg.min_unique_trade_prices
                    ));
                }
                if liquidity.last_trade_age_ms > cfg.max_last_trade_age_seconds as i64 * 1_000 {
                    blockers.push(format!(
                        "最近成交已过去 {:.1} 秒，要求 ≤{} 秒",
                        liquidity.last_trade_age_ms as f64 / 1_000.0,
                        cfg.max_last_trade_age_seconds
                    ));
                }
                state.latest_confirmation_microstructure = json!({
                    "ts_ms":scan_ms,
                    "stage":"confirmation",
                    "strategy":micro_strategy,
                    "setup_id":microstructure_trial_key(micro_strategy, &candidate.symbol, candidate_origin_ms(candidate)),
                    "symbol":candidate.symbol,
                    "side":candidate.side,
                    "origin_signal_ms":candidate_origin_ms(candidate),
                    "target_notional":notional,
                    "source":"binance_mainnet_public_rest_agg_trades_and_depth",
                    "passed":blockers.is_empty(),
                    "effective_max_spread_bps":effective_spread_limit,
                    "liquid_market_spread_relaxation":liquid_market_spread_relaxation,
                    "snapshot":liquidity
                });
                if pulse_candidate {
                    state.latest_pulse_confirmation_microstructure =
                        state.latest_confirmation_microstructure.clone();
                } else {
                    state.latest_main_confirmation_microstructure =
                        state.latest_confirmation_microstructure.clone();
                }
                append_event(
                    &event_path,
                    json!({"ts_ms":scan_ms,"event":"liquidity_check","strategy":micro_strategy,"setup_id":microstructure_trial_key(micro_strategy, &candidate.symbol, candidate_origin_ms(candidate)),"microstructure_stage":"confirmation","microstructure_source":"binance_mainnet_public_rest_agg_trades_and_depth","origin_signal_ms":candidate_origin_ms(candidate),"confirmation_ms":candidate.signal_ms,"symbol":candidate.symbol,"side":candidate.side,"target_notional":notional,"passed":blockers.is_empty(),"blockers":&blockers,"observations":if unique_trade_prices_warning {vec![format!("近 {} 秒仅 {} 个成交价，参考值 ≥{}；其他可成交性指标合格时不单独否决",cfg.recent_trade_window_seconds,liquidity.unique_trade_prices,cfg.min_unique_trade_prices)]} else {Vec::<String>::new()},"spread_policy":if liquid_market_spread_relaxation {"liquid_market_relaxed"} else {"default"},"effective_max_spread_bps":effective_spread_limit,"snapshot":liquidity,"limits":{"max_spread_bps":cfg.max_spread_bps,"liquid_market_max_spread_bps":cfg.liquid_market_max_spread_bps,"max_entry_impact_bps":cfg.max_entry_impact_bps,"max_exit_impact_bps":cfg.max_exit_impact_bps,"depth_band_pct":cfg.depth_band_pct,"min_depth_multiple":cfg.min_depth_multiple,"liquid_market_depth_multiple":cfg.min_depth_multiple*3.0,"recent_trade_window_seconds":cfg.recent_trade_window_seconds,"min_recent_trades":cfg.min_recent_trades,"liquid_market_min_recent_trades":cfg.min_recent_trades.saturating_mul(2),"min_unique_trade_prices":cfg.min_unique_trade_prices,"unique_trade_prices_hard":cfg.unique_trade_prices_hard,"max_last_trade_age_seconds":cfg.max_last_trade_age_seconds}}),
                )?;
                if !blockers.is_empty() {
                    let reason = blockers.join(" / ");
                    state.note_execution_issue(
                        scan_ms,
                        &candidate.symbol,
                        "liquidity_check",
                        reason,
                    );
                    if cross_candidate {
                        state
                            .cross_section_pending
                            .retain(|item| item.symbol != candidate.symbol);
                        let retry_count = state.cross_section_status["execution_state"]
                            ["retry_count"]
                            .as_u64()
                            .unwrap_or(0)
                            + 1;
                        state.cross_section_status["execution_state"] = json!({
                            "status":"trying_next_candidate",
                            "symbol":candidate.symbol,
                            "side":candidate.side,
                            "symbols":state.cross_section_pending.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>(),
                            "legs_pending":state.cross_section_pending.len(),
                            "retry_count":retry_count,
                            "last_check_ms":scan_ms,
                            "next_symbol":state.cross_section_pending.first().map(|item| item.symbol.clone()),
                            "expires_ms":candidate.signal_ms + max_signal_age_ms,
                            "reason":blockers.join(" / "),
                            "resolution":"candidate_skipped_next_rank_immediate",
                            "market_data_source":"binance_mainnet_public"
                        });
                        save_state(&state_path, &state)?;
                    }
                    continue;
                }
            }
            if cross_candidate {
                // Liquidity passed. Consume the retry ticket before touching the
                // exchange so an order-side rejection cannot duplicate entries.
                state
                    .cross_section_pending
                    .retain(|item| item.symbol != candidate.symbol);
                state.cross_section_status["execution_state"] = json!({
                    "status":"liquidity_passed",
                    "symbol":candidate.symbol,
                    "side":candidate.side,
                    "symbols_pending":state.cross_section_pending.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>(),
                    "legs_pending":state.cross_section_pending.len(),
                    "passed_ms":scan_ms,
                    "market_data_source":"binance_mainnet_public"
                });
                save_state(&state_path, &state)?;
            }
            let mut entry = candidate.price;
            let mut qty = notional / entry;
            let fee;
            let mut protection_order_id = None;
            let mut protection_order_ids = Vec::new();
            let mut actual_leverage = cfg.exchange_leverage;
            let mut execution_mark_at_order = None;
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
                // LIMIT IOC 服从 LOT_SIZE；若数量超过 MARKET_LOT_SIZE，保护单和
                // 后续市价退出会分片执行，不能在这里把目标仓位静默砍小。
                let entry_max_qty = filters.max_qty;
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
                execution_mark_at_order = Some(execution_mark);
                let adverse_signal_drift =
                    candidate.side as f64 * (execution_mark / candidate.price - 1.0);
                let signal_drift_limit = if cross_candidate {
                    cfg.max_entry_slippage_pct.min(0.0075)
                } else {
                    cfg.max_entry_slippage_pct
                };
                if adverse_signal_drift > signal_drift_limit {
                    let reason = format!(
                        "信号后不利漂移 {:.1}bps > {:.1}bps",
                        adverse_signal_drift * 10_000.0,
                        signal_drift_limit * 10_000.0
                    );
                    state.note_execution_issue(
                        scan_ms,
                        &candidate.symbol,
                        "signal_drift",
                        reason.clone(),
                    );
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_rejected","stage":"signal_drift","symbol":candidate.symbol,"side":candidate.side,"signal_price":candidate.price,"execution_mark_price":execution_mark,"adverse_signal_drift_bps":adverse_signal_drift*10_000.0,"limit_bps":signal_drift_limit*10_000.0,"reason":reason,"resolution":if cross_candidate {"candidate_skipped_next_rank_immediate"} else {"signal_rejected"}}),
                    )?;
                    continue;
                }
                let execution_slippage_limit = if cross_candidate {
                    cfg.max_entry_slippage_pct.min(0.002)
                } else {
                    cfg.max_entry_slippage_pct
                };
                let (raw_guard_price, guard_price) = clamp_entry_guard_price(
                    candidate.side,
                    execution_mark,
                    execution_slippage_limit,
                    execution_mark,
                    filters.multiplier_up,
                    filters.multiplier_down,
                    filters.tick_size,
                );
                if (guard_price - raw_guard_price).abs() > filters.tick_size * 0.5 {
                    append_event(
                        &event_path,
                        json!({"ts_ms":scan_ms,"event":"entry_price_guard_clamped","symbol":candidate.symbol,"side":side,"signal_price":candidate.price,"execution_mark_price":execution_mark,"adverse_signal_drift_bps":adverse_signal_drift*10_000.0,"execution_slippage_limit_bps":execution_slippage_limit*10_000.0,"raw_guard_price":raw_guard_price,"clamped_guard_price":guard_price,"multiplier_up":filters.multiplier_up,"multiplier_down":filters.multiplier_down}),
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
                        if error.execution_may_be_unknown() {
                            anyhow::bail!(
                                "{} 下单结果未知，已停止执行器以避免重复下单；重启时将与交易所仓位对账: {}",
                                candidate.symbol,
                                reason
                            );
                        }
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
                let stop = entry * (1.0 - candidate.side as f64 * stop_pct);
                let close_side = if candidate.side > 0 { "SELL" } else { "BUY" };
                match place_protective_stops(
                    client,
                    &candidate.symbol,
                    close_side,
                    qty,
                    stop,
                    &filters,
                )
                .await
                {
                    Ok(order_ids) => {
                        protection_order_id = order_ids.first().copied();
                        protection_order_ids = order_ids;
                    }
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
            } else {
                entry =
                    adverse_fill_price(candidate.price, candidate.side, cfg.dry_slippage_bps, true);
                qty = notional / entry;
                fee = qty * entry * 0.0005;
            }
            let stop = entry * (1.0 - candidate.side as f64 * stop_pct);
            state.last_execution_issue = None;
            state.cash -= fee;
            state.fees += fee;
            state.daily_entries += 1;
            state.total_entries += 1;
            if pulse_candidate {
                state.pulse_entries += 1;
            } else if cross_candidate {
                state.cross_entries += 1;
                let mut open_symbols = state
                    .positions
                    .values()
                    .filter(|position| is_cross_position(position))
                    .map(|position| position.symbol.clone())
                    .collect::<Vec<_>>();
                open_symbols.push(candidate.symbol.clone());
                open_symbols.sort();
                let legs_open = open_symbols.len();
                state.cross_section_status["execution_state"] = json!({
                    "status":if state.cross_section_pending.is_empty() {"basket_executed"} else {"basket_partially_executed"},
                    "symbol":candidate.symbol,
                    "side":candidate.side,
                    "open_symbols":open_symbols,
                    "legs_open":legs_open,
                    "legs_required":cross_cfg.names,
                    "pending_symbols":state.cross_section_pending.iter().map(|item| item.symbol.clone()).collect::<Vec<_>>(),
                    "legs_pending":state.cross_section_pending.len(),
                    "executed_ms":scan_ms,
                    "next_boundary_ms":cross_boundary_ms + cross_interval_ms
                });
            } else if shock_candidate {
                state.shock_entries += 1;
            }
            let setup_origin_ms = candidate_origin_ms(candidate);
            let trial_key =
                microstructure_trial_key(micro_strategy, &candidate.symbol, setup_origin_ms);
            if let Some(trial) = state.microstructure_trials.get_mut(&trial_key) {
                trial.executed = true;
            }
            state.positions.insert(
                candidate.symbol.clone(),
                Position {
                    symbol: candidate.symbol.clone(),
                    side: candidate.side,
                    setup_origin_ms,
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
                    protection_order_ids,
                    protection_reason: default_protection_reason(),
                    exchange_leverage: Some(actual_leverage),
                    entry_phase: candidate.entry_phase.clone(),
                    partial_take_profit_done: false,
                    loss_trim_done: false,
                    last_partial_exit_ms: None,
                    realized_partial_pnl: 0.0,
                },
            );
            if candidate.entry_phase == "overextended_long" {
                overextension_slot_taken = true;
            }
            let event = json!({"ts_ms":scan_ms,"event":"entry","strategy":micro_strategy,"setup_id":trial_key,"origin_signal_ms":setup_origin_ms,"symbol":candidate.symbol,"side":candidate.side,"entry_phase":candidate.entry_phase,"entry_trigger":candidate.entry_trigger,"risk_scale":candidate.risk_scale,"direction_risk_scale":direction_risk_scale,"effective_risk_scale":effective_risk_scale,"signal_age_ms":signal_age_ms(scan_ms,candidate.signal_ms),"max_signal_age_ms":max_signal_age_ms,"signal":candidate,"entry_price":entry,"execution_mark_price":execution_mark_at_order,"execution_slippage_bps":execution_mark_at_order.map(|mark| candidate.side as f64*(entry/mark-1.0)*10_000.0),"signal_to_fill_bps":candidate.side as f64*(entry/candidate.price-1.0)*10_000.0,"qty":qty,"notional":qty*entry,"requested_leverage":cfg.exchange_leverage,"actual_leverage":actual_leverage,"margin_estimate":qty*entry/actual_leverage as f64,"risk_usd":qty*entry*risk_distance,"price_stop_risk_usd":qty*entry*stop_pct,"stop_pct":stop_pct,"trail_activation_pct":if pulse_candidate {Some(cfg.pulse_trail_activation_pct)} else if cross_candidate {Some(cross_cfg.trail_activation_pct)} else if shock_candidate {Some(shock_cfg.trail_activation_pct)} else {Some(cfg.trail_activation_pct)},"trail_pct":if pulse_candidate {Some(cfg.pulse_trail_pct)} else if cross_candidate {Some(cross_cfg.trail_pct)} else if shock_candidate {Some(shock_cfg.trail_pct)} else {Some(cfg.trail_pct)},"partial_take_profit_fraction":if cross_candidate {Some(cross_cfg.partial_take_profit_fraction)} else if shock_candidate {None} else {Some(cfg.partial_take_profit_fraction)},"max_hold_hours":if cross_candidate {Value::Null} else if shock_candidate {json!(shock_cfg.max_hold_hours)} else {json!(cfg.max_hold_hours)},"rebalance_hours":if cross_candidate {Some(cross_cfg.hold_hours as u32)} else {None},"risk_execution_buffer_pct":cfg.risk_execution_buffer_pct,"fee":fee});
            append_event(&event_path, event.clone())?;
            state.record_trade(event);
        }
        save_state(&state_path, &state)?;
        let valuation_ms = chrono::Utc::now().timestamp_millis();
        let position_status = build_position_status(
            &state,
            &prices,
            rest.as_ref(),
            valuation_ms,
            &cfg,
            &cross_cfg,
            &shock_cfg,
        )
        .await;
        let current_equity = state.cash + positions_unrealized(&position_status);
        record_daily_equity(&mut state.equity_curve, valuation_ms, current_equity);
        save_state(&state_path, &state)?;
        let pulse_position_count = state
            .positions
            .values()
            .filter(|position| is_pulse_position(position))
            .count();
        let pulse_notional_estimate =
            (current_equity * cfg.risk_per_trade * cfg.pulse_risk_scale * cfg.short_risk_scale
                / (cfg.pulse_stop_pct + cfg.risk_execution_buffer_pct))
                .min(current_equity * cfg.pulse_max_gross_multiple);
        let latest_pulse_fresh = state.latest_pulse_exhaustion["eligible"] == true
            && state.latest_pulse_exhaustion["confirmation_ms"]
                .as_i64()
                .is_some_and(|ts| signal_age_ms(scan_ms, ts) <= max_signal_age_ms);
        let latest_pulse_rejected = state.latest_pulse_exhaustion["stage"] == "rejected"
            && state.latest_pulse_exhaustion["ts_ms"]
                .as_i64()
                .is_some_and(|ts| signal_age_ms(scan_ms, ts) <= 15 * 60_000);
        let pulse_status = json!({
            "enabled":pulse_exhaustion_active,
            "configured":cfg.pulse_exhaustion_enabled,
            "paper_only":mode==TradeMode::Live && !cfg.pulse_exhaustion_allow_live,
            "shadow_discovery_active":pulse_exhaustion_active,
            "live_execution_allowed":pulse_live_execution_allowed,
            "stage":if !pulse_exhaustion_active {"disabled"}
                else if pulse_position_count>0 {"position"}
                else if latest_pulse_fresh && pulse_live_execution_allowed {"execution"}
                else if latest_pulse_fresh {"shadow_observation"}
                else if !state.pulse_exhaustion_setups.is_empty() {"armed"}
                else if latest_pulse_rejected {"rejected"}
                else {"scan"},
            "active_setups":state.pulse_exhaustion_setups.values().collect::<Vec<_>>(),
            "initial_candidate_count":pulse_initial_candidates.len(),
            "initial_candidates":pulse_initial_candidates.iter().take(10).collect::<Vec<_>>(),
            "nearest_initial_candidate":nearest_pulse_initial,
            "eligible_count":pulse_eligible_count,
            "latest_evaluation":state.latest_pulse_exhaustion,
            "position_count":pulse_position_count,
            "max_positions":cfg.pulse_max_positions,
            "max_gross_multiple":cfg.pulse_max_gross_multiple,
            "risk_scale":cfg.pulse_risk_scale,
            "notional_estimate":pulse_notional_estimate,
            "stop_pct":cfg.pulse_stop_pct,
            "trail_activation_pct":cfg.pulse_trail_activation_pct,
            "trail_pct":cfg.pulse_trail_pct,
            "partial_take_profit_fraction":cfg.partial_take_profit_fraction,
            "max_directional_premium":cfg.pulse_max_directional_premium,
            "entries":state.pulse_entries,"exits":state.pulse_exits,"wins":state.pulse_wins,
            "win_rate":if state.pulse_exits>0 {Some(state.pulse_wins as f64/state.pulse_exits as f64)} else {None},
            "realized_pnl":state.pulse_realized_pnl,
            "thresholds":{
                "initial_return_1h":cfg.pulse_initial_return_1h,
                "initial_volume_ratio":cfg.pulse_initial_volume_ratio,
                "perp_return_1h":cfg.pulse_perp_return_1h,
                "oi_change_1h":cfg.pulse_oi_change_1h,
                "max_spot_perp_ratio":cfg.pulse_max_spot_perp_ratio,
                "min_peak_retrace":cfg.pulse_min_peak_retrace,
                "max_close_location":cfg.pulse_max_close_location,
                "max_directional_funding_rate":cfg.max_directional_funding_rate,
                "max_directional_premium":cfg.pulse_max_directional_premium
            }
        });
        if cross_cfg.enabled && state.cross_section_status.is_object() {
            state.cross_section_status["performance"] = cross_performance(&state);
        }
        if shock_cfg.enabled && state.shock_reversal_status.is_object() {
            state.shock_reversal_status["performance"] = shock_performance(&state);
        }
        let mut scan_event = json!({
            "ts_ms":scan_ms, "event":"scan", "universe_count":spot_symbols.intersection(&active).filter(|symbol| !excluded.contains(symbol.as_str())).count(),
            "shortlist_count":shortlist_count, "eligible_count":regular_eligible_count,
            "pulse_eligible_count":pulse_eligible_count,
            "equity":current_equity, "daily_entries":state.daily_entries,
            "base_max_daily_entries":cfg.max_daily_entries,
            "daily_entry_bonus":state.daily_entry_bonus,
            "max_daily_entry_bonus":cfg.max_daily_entry_bonus,
            "max_daily_entries":effective_daily_limit,
            "max_signal_age_seconds":cfg.max_signal_age_seconds,
            "confirmation_window_bars":cfg.confirmation_window_bars,
            "retest_touch_pct":cfg.retest_touch_pct,
            "retest_invalidation_pct":cfg.retest_invalidation_pct,
            "reclaim_pct":cfg.reclaim_pct,
            "extreme_direct_return_1h":cfg.extreme_direct_return_1h,
            "extreme_direct_return_4h":cfg.extreme_direct_return_4h,
            "extreme_direct_volume_ratio":cfg.extreme_direct_volume_ratio,
            "extreme_direct_risk_scale":cfg.extreme_direct_risk_scale,
            "risk_execution_buffer_pct":cfg.risk_execution_buffer_pct,
            "loss_trim_trigger_pct":cfg.loss_trim_trigger_pct,
            "loss_trim_fraction":cfg.loss_trim_fraction,
            "partial_take_profit_fraction":cfg.partial_take_profit_fraction,
            "failed_breakout_window_minutes":cfg.failed_breakout_window_minutes,
            "failed_breakout_adverse_pct":cfg.failed_breakout_adverse_pct,
            "failed_breakout_max_mfe_pct":cfg.failed_breakout_max_mfe_pct,
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
            "pending_entries":state.pending_entries.values().collect::<Vec<_>>(),
            "positions":position_status,
            "candidates":candidates.iter().take(10).collect::<Vec<_>>()
        });
        scan_event["max_positions"] = json!(cfg.max_positions);
        scan_event["max_gross_multiple"] = json!(cfg.max_gross_multiple);
        scan_event["execution_model"] = json!(if shock_cfg.enabled {
            "multi_alpha"
        } else if cross_cfg.enabled {
            "cross_section_momentum"
        } else {
            "confirmed_volume_breakout"
        });
        scan_event["long_risk_scale"] = json!(cfg.long_risk_scale);
        scan_event["short_risk_scale"] = json!(cfg.short_risk_scale);
        scan_event["max_directional_funding_rate"] = json!(cfg.max_directional_funding_rate);
        scan_event["max_directional_premium"] = json!(cfg.max_directional_premium);
        scan_event["adaptive_retest_enabled"] = json!(cfg.adaptive_retest_enabled);
        scan_event["vertical_overshoot_pct"] = json!(cfg.vertical_overshoot_pct);
        scan_event["vertical_retest_touch_pct"] = json!(cfg.vertical_retest_touch_pct);
        scan_event["vertical_retest_invalidation_pct"] =
            json!(cfg.vertical_retest_invalidation_pct);
        scan_event["vertical_reclaim_pct"] = json!(cfg.vertical_reclaim_pct);
        scan_event["vertical_max_entry_extension_pct"] =
            json!(cfg.vertical_max_entry_extension_pct);
        scan_event["intrabar_vertical_retest_enabled"] =
            json!(cfg.intrabar_vertical_retest_enabled);
        scan_event["intrabar_min_pullback_pct"] = json!(cfg.intrabar_min_pullback_pct);
        scan_event["intrabar_rebound_pct"] = json!(cfg.intrabar_rebound_pct);
        scan_event["trail_activation_pct"] = json!(cfg.trail_activation_pct);
        scan_event["trail_pct"] = json!(cfg.trail_pct);
        scan_event["max_hold_hours"] = json!(cfg.max_hold_hours);
        scan_event["overextension_long_enabled"] = json!(cfg.overextension_long_enabled);
        scan_event["min_volume_ratio"] = json!(cfg.min_volume_ratio);
        scan_event["min_efficiency"] = json!(cfg.min_efficiency);
        scan_event["min_close_location"] = json!(cfg.min_close_location);
        scan_event["first_week"] = json!(&first_week);
        scan_event["cross_section"] = state.cross_section_status.clone();
        scan_event["shock_reversal"] = state.shock_reversal_status.clone();
        scan_event["pulse_exhaustion"] = pulse_status.clone();
        append_event(&event_path, scan_event)?;
        let microstructure_status = json!({
            "observation_only":true,
            "delta_flow_observation_only":true,
            "execution_liquidity_gates_enabled":true,
            "outcome_horizon_ms":cfg.max_hold_hours as i64*3_600_000,
            "active_outcome_trials":state.microstructure_trials.len(),
            "strategies":{
                "main_breakout":{
                    "signal":state.latest_main_signal_microstructure,
                    "confirmation":state.latest_main_confirmation_microstructure
                },
                "pulse_exhaustion":{
                    "signal":state.latest_pulse_signal_microstructure,
                    "confirmation":state.latest_pulse_confirmation_microstructure
                }
            },
            "signal":state.latest_signal_microstructure,
            "confirmation":state.latest_confirmation_microstructure
        });
        let mut status_payload = json!({
            "state":"running", "mode":mode.as_str(), "started_at_ms":started_ms,
            "uptime_s":(scan_ms-started_ms)/1000, "strategy_name":args.strategy,
            "strategy_hash":strategy_hash, "git_commit":git_commit,
            "equity":current_equity, "cash":state.cash, "position":Value::Null,
            "n_intents":state.total_entries, "n_fills":state.total_entries + state.total_exits,
            "altcoin_impulse": {
                "stage": if daily_loss_blocked || first_week.entries_blocked {"risk_blocked"} else if regular_eligible_count>0 {"execution"} else if !state.pending_entries.is_empty() {"confirmation"} else {"scan"},
                "universe_count":spot_symbols.intersection(&active).filter(|symbol| !excluded.contains(symbol.as_str())).count(), "shortlist_count":shortlist_count,
                "eligible_count":regular_eligible_count, "last_scan_ms":scan_ms,
                "exchange_leverage":cfg.exchange_leverage, "risk_per_trade":cfg.risk_per_trade,
                "stop_pct":cfg.stop_pct, "notional_per_trade_estimate":current_equity*cfg.risk_per_trade/(cfg.stop_pct+cfg.risk_execution_buffer_pct),
                "margin_per_trade_estimate":current_equity*cfg.risk_per_trade/(cfg.stop_pct+cfg.risk_execution_buffer_pct)/cfg.exchange_leverage as f64,
                "worst_loss_per_trade_estimate":current_equity*cfg.risk_per_trade,
                "daily_entries":state.daily_entries, "max_daily_entries":effective_daily_limit,
                "daily_loss_blocked":daily_loss_blocked,
                "positions":position_status,
                "last_valuation_ms":valuation_ms,
                "valuation_source": if rest.is_some() {"binance_position_risk"} else {"closed_15m_fallback"},
                "total_entries":state.total_entries, "total_exits":state.total_exits, "wins":state.wins,
                "rejected_entries":state.rejected_entries, "last_execution_issue":state.last_execution_issue,
                "recent_trades":state.recent_trades,
                "microstructure":microstructure_status,
                "realized_pnl":state.realized_pnl, "fees":state.fees,
                "candidates":candidates.into_iter().take(10).collect::<Vec<_>>(),
                "journal":event_path,
            }
        });
        status_payload["altcoin_impulse"]["pulse_exhaustion"] = pulse_status;
        status_payload["altcoin_impulse"]["entry_blocked"] =
            json!(daily_loss_blocked || first_week.entries_blocked);
        status_payload["altcoin_impulse"]["daily_loss_limit"] = json!(cfg.daily_loss_limit);
        status_payload["altcoin_impulse"]["cross_section"] = state.cross_section_status.clone();
        status_payload["altcoin_impulse"]["shock_reversal"] = state.shock_reversal_status.clone();
        status_payload["altcoin_impulse"]["execution_model"] = json!(if shock_cfg.enabled {
            "multi_alpha"
        } else if cross_cfg.enabled {
            "cross_section_momentum"
        } else {
            "confirmed_volume_breakout"
        });
        let regular_slot_owners = state
            .positions
            .values()
            .filter(|position| !is_pulse_position(position))
            .map(|position| {
                json!({
                    "symbol":position.symbol,
                    "side":position.side,
                    "strategy":position.entry_phase
                })
            })
            .collect::<Vec<_>>();
        status_payload["altcoin_impulse"]["strategy_coordination"] = json!({
            "capital_pool":"shared_altcoin_sleeve",
            "regular_slots":cfg.max_positions,
            "regular_slot_owners":regular_slot_owners,
            "regular_priority":["cross_section_momentum"],
            "existing_position_wins":true,
            "same_symbol_opposite_orders_allowed":false,
            "pulse_slot_independent":true,
            "pulse_same_symbol_conflict_blocked":true
        });
        status_payload["altcoin_impulse"]["max_positions"] = json!(cfg.max_positions);
        status_payload["altcoin_impulse"]["max_gross_multiple"] = json!(cfg.max_gross_multiple);
        status_payload["altcoin_impulse"]["shock_max_gross_multiple"] =
            json!(shock_cfg.max_gross_multiple);
        status_payload["altcoin_impulse"]["long_risk_scale"] = json!(cfg.long_risk_scale);
        status_payload["altcoin_impulse"]["short_risk_scale"] = json!(cfg.short_risk_scale);
        status_payload["altcoin_impulse"]["max_directional_funding_rate"] =
            json!(cfg.max_directional_funding_rate);
        status_payload["altcoin_impulse"]["max_directional_premium"] =
            json!(cfg.max_directional_premium);
        status_payload["altcoin_impulse"]["long_notional_estimate"] = json!(
            current_equity * cfg.risk_per_trade * cfg.long_risk_scale
                / (cfg.stop_pct + cfg.risk_execution_buffer_pct)
        );
        status_payload["altcoin_impulse"]["short_notional_estimate"] = json!(
            current_equity * cfg.risk_per_trade * cfg.short_risk_scale
                / (cfg.stop_pct + cfg.risk_execution_buffer_pct)
        );
        status_payload["altcoin_impulse"]["first_week"] = json!(&first_week);
        status_payload["altcoin_impulse"]["daily_risk_baseline_equity"] =
            json!(state.day_start_equity);
        status_payload["altcoin_impulse"]["pending_entries"] =
            json!(state.pending_entries.values().collect::<Vec<_>>());
        status_payload["altcoin_impulse"]["base_max_daily_entries"] = json!(cfg.max_daily_entries);
        status_payload["altcoin_impulse"]["max_daily_entry_bonus"] =
            json!(cfg.max_daily_entry_bonus);
        status_payload["altcoin_impulse"]["max_signal_age_seconds"] =
            json!(cfg.max_signal_age_seconds);
        status_payload["altcoin_impulse"]["direct_entry_enabled"] = json!(cfg.direct_entry_enabled);
        status_payload["altcoin_impulse"]["loss_trim_enabled"] = json!(cfg.loss_trim_enabled);
        status_payload["altcoin_impulse"]["failed_breakout_enabled"] =
            json!(cfg.failed_breakout_enabled);
        status_payload["altcoin_impulse"]["recovery_lock_enabled"] =
            json!(cfg.recovery_lock_enabled);
        status_payload["altcoin_impulse"]["confirmation_window_bars"] =
            json!(cfg.confirmation_window_bars);
        status_payload["altcoin_impulse"]["retest_touch_pct"] = json!(cfg.retest_touch_pct);
        status_payload["altcoin_impulse"]["retest_invalidation_pct"] =
            json!(cfg.retest_invalidation_pct);
        status_payload["altcoin_impulse"]["reclaim_pct"] = json!(cfg.reclaim_pct);
        status_payload["altcoin_impulse"]["adaptive_retest_enabled"] =
            json!(cfg.adaptive_retest_enabled);
        status_payload["altcoin_impulse"]["vertical_overshoot_pct"] =
            json!(cfg.vertical_overshoot_pct);
        status_payload["altcoin_impulse"]["vertical_retest_touch_pct"] =
            json!(cfg.vertical_retest_touch_pct);
        status_payload["altcoin_impulse"]["vertical_retest_invalidation_pct"] =
            json!(cfg.vertical_retest_invalidation_pct);
        status_payload["altcoin_impulse"]["vertical_reclaim_pct"] = json!(cfg.vertical_reclaim_pct);
        status_payload["altcoin_impulse"]["vertical_max_entry_extension_pct"] =
            json!(cfg.vertical_max_entry_extension_pct);
        status_payload["altcoin_impulse"]["intrabar_vertical_retest_enabled"] =
            json!(cfg.intrabar_vertical_retest_enabled);
        status_payload["altcoin_impulse"]["intrabar_min_pullback_pct"] =
            json!(cfg.intrabar_min_pullback_pct);
        status_payload["altcoin_impulse"]["intrabar_rebound_pct"] = json!(cfg.intrabar_rebound_pct);
        status_payload["altcoin_impulse"]["extreme_direct_return_1h"] =
            json!(cfg.extreme_direct_return_1h);
        status_payload["altcoin_impulse"]["extreme_direct_return_4h"] =
            json!(cfg.extreme_direct_return_4h);
        status_payload["altcoin_impulse"]["extreme_direct_volume_ratio"] =
            json!(cfg.extreme_direct_volume_ratio);
        status_payload["altcoin_impulse"]["extreme_direct_risk_scale"] =
            json!(cfg.extreme_direct_risk_scale);
        status_payload["altcoin_impulse"]["cooldown_hours"] = json!(cfg.cooldown_hours);
        status_payload["altcoin_impulse"]["risk_execution_buffer_pct"] =
            json!(cfg.risk_execution_buffer_pct);
        status_payload["altcoin_impulse"]["loss_trim_trigger_pct"] =
            json!(cfg.loss_trim_trigger_pct);
        status_payload["altcoin_impulse"]["loss_trim_fraction"] = json!(cfg.loss_trim_fraction);
        status_payload["altcoin_impulse"]["extreme_direct_enabled"] =
            json!(cfg.extreme_direct_enabled);
        status_payload["altcoin_impulse"]["max_spread_bps"] = json!(cfg.max_spread_bps);
        status_payload["altcoin_impulse"]["liquid_market_max_spread_bps"] =
            json!(cfg.liquid_market_max_spread_bps);
        status_payload["altcoin_impulse"]["min_contract_age_days"] =
            json!(cfg.min_contract_age_days);
        status_payload["altcoin_impulse"]["max_entry_impact_bps"] = json!(cfg.max_entry_impact_bps);
        status_payload["altcoin_impulse"]["max_exit_impact_bps"] = json!(cfg.max_exit_impact_bps);
        status_payload["altcoin_impulse"]["depth_band_pct"] = json!(cfg.depth_band_pct);
        status_payload["altcoin_impulse"]["min_depth_multiple"] = json!(cfg.min_depth_multiple);
        status_payload["altcoin_impulse"]["recent_trade_window_seconds"] =
            json!(cfg.recent_trade_window_seconds);
        status_payload["altcoin_impulse"]["min_recent_trades"] = json!(cfg.min_recent_trades);
        status_payload["altcoin_impulse"]["min_unique_trade_prices"] =
            json!(cfg.min_unique_trade_prices);
        status_payload["altcoin_impulse"]["unique_trade_prices_hard"] =
            json!(cfg.unique_trade_prices_hard);
        status_payload["altcoin_impulse"]["max_last_trade_age_seconds"] =
            json!(cfg.max_last_trade_age_seconds);
        status_payload["altcoin_impulse"]["partial_take_profit_fraction"] =
            json!(cfg.partial_take_profit_fraction);
        status_payload["altcoin_impulse"]["trail_activation_pct"] = json!(cfg.trail_activation_pct);
        status_payload["altcoin_impulse"]["trail_pct"] = json!(cfg.trail_pct);
        status_payload["altcoin_impulse"]["max_hold_hours"] = json!(cfg.max_hold_hours);
        status_payload["altcoin_impulse"]["failed_breakout_window_minutes"] =
            json!(cfg.failed_breakout_window_minutes);
        status_payload["altcoin_impulse"]["failed_breakout_adverse_pct"] =
            json!(cfg.failed_breakout_adverse_pct);
        status_payload["altcoin_impulse"]["failed_breakout_max_mfe_pct"] =
            json!(cfg.failed_breakout_max_mfe_pct);
        status_payload["altcoin_impulse"]["recovery_lock_adverse_pct"] =
            json!(cfg.recovery_lock_adverse_pct);
        status_payload["altcoin_impulse"]["recovery_lock_activation_pct"] =
            json!(cfg.recovery_lock_activation_pct);
        status_payload["altcoin_impulse"]["recovery_lock_pct"] = json!(cfg.recovery_lock_pct);
        status_payload["altcoin_impulse"]["overextension_long_return_1h"] =
            json!(cfg.overextension_long_return_1h);
        status_payload["altcoin_impulse"]["overextension_long_return_4h"] =
            json!(cfg.overextension_long_return_4h);
        status_payload["altcoin_impulse"]["overextension_long_enabled"] =
            json!(cfg.overextension_long_enabled);
        status_payload["altcoin_impulse"]["min_volume_ratio"] = json!(cfg.min_volume_ratio);
        status_payload["altcoin_impulse"]["min_efficiency"] = json!(cfg.min_efficiency);
        status_payload["altcoin_impulse"]["min_close_location"] = json!(cfg.min_close_location);
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
                match manage_live_positions(
                    &mut state,
                    client,
                    &cfg,
                    &cross_cfg,
                    &shock_cfg,
                    refresh_ms,
                    &event_path,
                )
                .await
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
                match advance_intrabar_pending_entries(
                    &mut state,
                    client,
                    &cfg,
                    refresh_ms,
                    &event_path,
                )
                .await
                {
                    Ok(true) => {
                        save_state(&state_path, &state)?;
                        // A touch invalidation or a confirmed reclaim changes the
                        // decision pipeline. Re-enter the outer loop immediately so
                        // confirmation is executed without waiting for the 60s scan.
                        break;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        warn!(error=%error, "盘中浅回踩状态更新失败，保留原候选等待下轮");
                        append_event(
                            &event_path,
                            json!({"ts_ms":refresh_ms,"event":"intrabar_confirmation_error","reason":error.to_string(),"pending_preserved":true}),
                        )?;
                    }
                }
                let refreshed = build_position_status(
                    &state,
                    &prices,
                    Some(client),
                    refresh_ms,
                    &cfg,
                    &cross_cfg,
                    &shock_cfg,
                )
                .await;
                let refreshed_equity = state.cash + positions_unrealized(&refreshed);
                record_daily_equity(&mut state.equity_curve, refresh_ms, refreshed_equity);
                if take_control_commands(&mut daily_risk_reset, &mut handled_risk_reset) > 0 {
                    apply_daily_risk_reset(&mut state, refreshed_equity, refresh_ms, &event_path)?;
                    save_state(&state_path, &state)?;
                    status_payload["altcoin_impulse"]["daily_loss_blocked"] =
                        json!(state.daily_loss_latched);
                    status_payload["altcoin_impulse"]["daily_risk_baseline_equity"] =
                        json!(state.day_start_equity);
                    status_payload["altcoin_impulse"]["daily_risk_resets"] =
                        json!(state.daily_risk_resets);
                    status_payload["altcoin_impulse"]["last_daily_risk_reset_ms"] =
                        json!(state.last_daily_risk_reset_ms);
                    status_payload["altcoin_impulse"]["stage"] =
                        json!(if regular_eligible_count > 0 {
                            "execution"
                        } else if !state.pending_entries.is_empty() {
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
                if update_first_week_latches(
                    &mut state,
                    &cfg,
                    refreshed_equity,
                    refresh_ms,
                    &event_path,
                )? {
                    save_state(&state_path, &state)?;
                }
                let refreshed_first_week =
                    first_week_progress(&state, &cfg, refreshed_equity, refresh_ms);
                status_payload["altcoin_impulse"]["equity_curve"] = json!(state.equity_curve);
                status_payload["altcoin_impulse"]["first_week"] = json!(&refreshed_first_week);
                status_payload["altcoin_impulse"]["entry_blocked"] =
                    json!(state.daily_loss_latched || refreshed_first_week.entries_blocked);
                if state.daily_loss_latched || refreshed_first_week.entries_blocked {
                    status_payload["altcoin_impulse"]["stage"] = json!("risk_blocked");
                }
                status_payload["altcoin_impulse"]["positions"] = json!(refreshed);
                status_payload["altcoin_impulse"]["last_valuation_ms"] = json!(refresh_ms);
                let refreshed_pulse_positions = state
                    .positions
                    .values()
                    .filter(|position| is_pulse_position(position))
                    .count();
                status_payload["altcoin_impulse"]["pulse_exhaustion"]["position_count"] =
                    json!(refreshed_pulse_positions);
                status_payload["altcoin_impulse"]["pulse_exhaustion"]["entries"] =
                    json!(state.pulse_entries);
                status_payload["altcoin_impulse"]["pulse_exhaustion"]["exits"] =
                    json!(state.pulse_exits);
                status_payload["altcoin_impulse"]["pulse_exhaustion"]["wins"] =
                    json!(state.pulse_wins);
                status_payload["altcoin_impulse"]["pulse_exhaustion"]["win_rate"] =
                    json!((state.pulse_exits > 0)
                        .then_some(state.pulse_wins as f64 / state.pulse_exits as f64));
                status_payload["altcoin_impulse"]["pulse_exhaustion"]["realized_pnl"] =
                    json!(state.pulse_realized_pnl);
                if cross_cfg.enabled {
                    status_payload["altcoin_impulse"]["cross_section"]["performance"] =
                        cross_performance(&state);
                }
                if refreshed_pulse_positions > 0 {
                    status_payload["altcoin_impulse"]["pulse_exhaustion"]["stage"] =
                        json!("position");
                } else if status_payload["altcoin_impulse"]["pulse_exhaustion"]["stage"]
                    == "position"
                {
                    status_payload["altcoin_impulse"]["pulse_exhaustion"]["stage"] = json!("scan");
                }
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
        adverse_excursion, adverse_fill_price, apply_daily_entry_bonus, apply_daily_risk_reset,
        clamp_entry_guard_price, confirmation_plan, daily_entry_limit_reached,
        default_entry_trigger, detected_exit_reason, directional_crowding_reason,
        effective_max_spread_bps, entry_phase, evaluate, evaluate_pulse_impulse,
        existing_stop_raw_fill, failed_breakout, first_week_progress, intrabar_pending_decision,
        market_qty_chunks, observe_microstructure_trial, pending_decision, position_excursions,
        pulse_exhaustion_candidate, pulse_exhaustion_runtime, realtime_trailing_stop,
        recently_exited_opposite_side, recently_exited_symbol, record_daily_equity, record_exit,
        record_partial_exit, recovery_profit_lock_stop, signal_age_ms, update_adverse_extreme,
        update_microstructure_trials, Bar, Candidate, IntrabarPendingDecision, PendingDecision,
        PersistedState, Position, PulseExhaustionSetup,
    };

    #[test]
    fn live_capital_pause_keeps_pulse_shadow_discovery_running() {
        assert_eq!(
            pulse_exhaustion_runtime(true, false, super::TradeMode::Live),
            (true, false)
        );
        assert_eq!(
            pulse_exhaustion_runtime(true, true, super::TradeMode::Live),
            (true, true)
        );
        assert_eq!(
            pulse_exhaustion_runtime(false, true, super::TradeMode::Paper),
            (false, false)
        );
    }

    #[test]
    fn public_market_requests_rotate_away_from_a_stalled_primary_domain() {
        let futures =
            super::public_url_candidates("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT");
        assert_eq!(futures.len(), 3);
        assert!(futures[1].starts_with("https://fapi1.binance.com/"));
        assert!(futures[2].starts_with("https://testnet.binancefuture.com/"));

        let unrelated = super::public_url_candidates("https://example.com/data");
        assert_eq!(unrelated, ["https://example.com/data"]);
    }

    #[test]
    fn spread_relaxes_only_for_an_exceptionally_liquid_book() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        let cfg = strategy.altcoin_impulse;
        let mut snapshot = live::rest::LiquiditySnapshot {
            measured_at_ms: 1_800_000_000_000,
            bid: 0.01713,
            ask: 0.01715,
            spread_bps: 11.675,
            bid_depth_usd: 1_218_238.0,
            ask_depth_usd: 1_289_030.0,
            entry_impact_bps: Some(0.0),
            exit_impact_bps: Some(0.0),
            recent_trade_count: 145,
            unique_trade_prices: 15,
            last_trade_age_ms: 1_030,
            trade_history_span_ms: 384_686,
            depth_imbalance: None,
            trade_notional_60s: None,
            delta_usd_60s: None,
            delta_share_10s: None,
            delta_share_30s: None,
            delta_share_60s: None,
            large_trade_delta_share_60s: None,
            flow_persistence_60s: None,
            volume_acceleration_60s: None,
            trade_count_acceleration_60s: None,
            price_return_60s: None,
        };
        let (limit, relaxed) = effective_max_spread_bps(&cfg, &snapshot, 11_369.0);
        assert!(relaxed);
        assert_eq!(limit, 15.0);

        snapshot.recent_trade_count = cfg.min_recent_trades;
        let (limit, relaxed) = effective_max_spread_bps(&cfg, &snapshot, 11_369.0);
        assert!(!relaxed);
        assert_eq!(limit, 10.0);
    }

    #[test]
    fn directional_crowding_guard_is_mirrored_and_fails_closed() {
        assert!(
            directional_crowding_reason(-1, Some(-0.00996952), Some(-0.01257), 0.003, 0.01)
                .is_some()
        );
        assert!(directional_crowding_reason(1, Some(0.004), Some(0.002), 0.003, 0.01).is_some());
        assert!(directional_crowding_reason(1, Some(-0.004), Some(-0.02), 0.003, 0.01).is_none());
        assert!(directional_crowding_reason(-1, Some(0.004), Some(0.02), 0.003, 0.01).is_none());
        assert!(directional_crowding_reason(1, None, Some(0.0), 0.003, 0.01).is_some());
    }

    #[test]
    fn pulse_exhaustion_uses_only_the_first_bar_after_the_impulse() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        let cfg = strategy.altcoin_impulse;
        let mut bars: Vec<Bar> = (0..17)
            .map(|i| Bar {
                open_ms: i * 900_000,
                close_ms: i * 900_000 + 899_999,
                open: 99.0,
                high: 100.0,
                low: 98.0,
                close: 99.0,
                quote_volume: 1_000_000.0,
            })
            .collect();
        bars[15] = Bar {
            open: 105.0,
            high: 110.0,
            low: 104.0,
            close: 110.0,
            ..bars[15].clone()
        };
        bars[16] = Bar {
            open: 108.0,
            high: 109.0,
            low: 100.0,
            close: 105.0,
            ..bars[16].clone()
        };
        let setup = PulseExhaustionSetup {
            symbol: "PIXELUSDT".into(),
            origin_signal_ms: bars[15].close_ms,
            initial_price: 110.0,
            initial_return_1h: 0.12,
            initial_volume_ratio: 20.0,
            volume_24h: 50_000_000.0,
        };
        let (mut candidate, retrace, close_location) =
            pulse_exhaustion_candidate(&setup, &bars, &cfg).unwrap();
        assert_eq!(candidate.signal_ms, bars[16].close_ms);
        assert_eq!(candidate.side, -1);
        assert_eq!(candidate.entry_phase, "pulse_exhaustion_short");
        assert!(candidate.return_1h >= 0.06);
        assert!(retrace >= 0.035);
        assert!(close_location <= 0.80);
        candidate.spot_return_1h = Some(0.05);
        candidate.oi_change_1h = Some(0.30);
        let metrics = super::apply_pulse_exhaustion_gates(&mut candidate, retrace, &cfg);
        assert!(candidate.eligible(), "{:?}", candidate.blockers);
        assert_eq!(metrics["gates"]["oi_expansion"], true);
    }

    #[test]
    fn pulse_arming_does_not_inherit_main_continuation_gates() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        let cfg = strategy.altcoin_impulse;
        let mut bars: Vec<Bar> = (0..(7 * 96 + 17))
            .map(|i| Bar {
                open_ms: i as i64 * 900_000,
                close_ms: i as i64 * 900_000 + 899_999,
                open: 100.0,
                high: 100.2,
                low: 99.8,
                close: 100.0,
                quote_volume: 100_000.0,
            })
            .collect();
        let last = bars.len() - 1;
        // Keep a higher 24h high so the main breakout model rejects this symbol.
        bars[last - 20].high = 110.0;
        for bar in &mut bars[last - 3..=last] {
            bar.quote_volume = 2_000_000.0;
        }
        bars[last].open = 105.5;
        bars[last].high = 107.0;
        bars[last].low = 105.0;
        bars[last].close = 106.5;

        let main = evaluate("PULSEUSDT".into(), &bars, &cfg).unwrap();
        assert!(!main.eligible());
        assert!(main
            .blockers
            .iter()
            .any(|reason| reason == "未突破前 24h 高低点"));

        let pulse = evaluate_pulse_impulse("PULSEUSDT".into(), &bars, &cfg).unwrap();
        assert!(pulse.eligible(), "{:?}", pulse.blockers);
        assert!(pulse.return_1h >= cfg.pulse_initial_return_1h);
        assert!(pulse.volume_ratio >= cfg.pulse_initial_volume_ratio);
        assert_eq!(pulse.entry_trigger, "independent_leverage_pulse");
    }

    #[test]
    fn shock_reversal_detects_a_swept_pump_and_strong_rejection() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        let shock = strategy.altcoin_shock_reversal;
        let mut bars: Vec<Bar> = (0..(7 * 96 + 17))
            .map(|i| Bar {
                open_ms: i as i64 * 900_000,
                close_ms: i as i64 * 900_000 + 899_999,
                open: 100.0,
                high: 100.2,
                low: 99.8,
                close: 100.0,
                quote_volume: 100_000.0,
            })
            .collect();
        let index = bars.len() - 1;
        for bar in &mut bars[index - 4..index] {
            bar.open = 104.0;
            bar.high = 106.5;
            bar.low = 103.5;
            bar.close = 106.0;
        }
        bars[index].open = 106.0;
        bars[index].high = 108.0;
        bars[index].low = 104.0;
        bars[index].close = 105.0;
        bars[index].quote_volume = 400_000.0;

        let candidate =
            super::shock_reversal_candidate_at("SHOCKUSDT", &bars, index, &shock, 5_000_000.0)
                .unwrap();
        assert_eq!(candidate.side, -1);
        assert_eq!(candidate.entry_phase, "shock_reversal");
        assert_eq!(candidate.entry_trigger, "strict_shock_reversal");
        assert_eq!(candidate.risk_scale, 1.0);
        assert!(candidate.return_1h >= 0.045);
        assert!(candidate.efficiency >= 0.006);
    }

    #[test]
    fn shock_gate_is_side_specific_and_requires_only_completed_outcomes() {
        let outcomes: Vec<_> = (0..30)
            .map(|index| super::ShockShadowOutcome {
                symbol: format!("S{index}USDT"),
                side: -1,
                signal_ms: index * 1_000,
                exit_ms: index * 1_000 + 500,
                net_return: if index % 3 == 0 { -0.01 } else { 0.012 },
            })
            .collect();
        let short = super::shock_gate_side(&outcomes, -1, 30, 1.0);
        let long = super::shock_gate_side(&outcomes, 1, 30, 1.0);
        assert_eq!(short["ready"], true);
        assert_eq!(short["open"], true);
        assert_eq!(long["ready"], false);
        assert_eq!(long["open"], false);
    }

    #[test]
    fn exchange_leverage_does_not_change_stop_risk() {
        let equity: f64 = 1_000.0;
        let risk: f64 = 0.08;
        let stop: f64 = 0.038;
        let execution_buffer: f64 = 0.006;
        let leverage: f64 = 10.0;
        let notional = equity * risk / (stop + execution_buffer);
        assert!((notional - 1_818.181818181818).abs() < 1e-10);
        assert!((notional / leverage - 181.8181818181818).abs() < 1e-10);
        assert!((notional * (stop + execution_buffer) - 80.0).abs() < 1e-10);
    }

    #[test]
    fn completed_trade_excursions_preserve_mfe_and_mae() {
        let position = Position {
            symbol: "PATHUSDT".into(),
            side: 1,
            setup_origin_ms: 1,
            qty: 1.0,
            entry_ms: 1,
            entry_price: 100.0,
            entry_fee: 0.0,
            initial_notional: 100.0,
            extreme: 106.0,
            adverse_extreme: Some(97.0),
            stop_price: 99.0,
            last_bar_ms: 1,
            protection_order_id: None,
            protection_order_ids: Vec::new(),
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "pulse_exhaustion_short".into(),
            partial_take_profit_done: false,
            loss_trim_done: false,
            last_partial_exit_ms: None,
            realized_partial_pnl: 0.0,
        };
        let (mfe, mae) = position_excursions(&position);
        assert!((mfe - 0.06).abs() < 1e-10);
        assert!((mae - 0.03).abs() < 1e-10);
    }

    #[test]
    fn dry_slippage_is_adverse_for_every_side_and_fill_direction() {
        assert_eq!(adverse_fill_price(100.0, 1, 5.0, true), 100.05);
        assert_eq!(adverse_fill_price(100.0, 1, 5.0, false), 99.95);
        assert_eq!(adverse_fill_price(100.0, -1, 5.0, true), 99.95);
        assert_eq!(adverse_fill_price(100.0, -1, 5.0, false), 100.05);
    }

    #[test]
    fn market_quantity_is_chunked_without_capping_limit_entry_size() {
        let filters = live::SymbolFilters {
            tick_size: 0.00001,
            step_size: 0.1,
            market_step_size: 0.1,
            price_precision: 5,
            quantity_precision: 1,
            max_qty: 1_000_000.0,
            market_max_qty: 30_000.0,
            min_notional: 5.0,
            multiplier_up: 1.05,
            multiplier_down: 0.95,
        };
        let chunks = market_qty_chunks(61_988.7, &filters);
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|qty| *qty <= filters.market_max_qty));
        assert!((chunks.iter().sum::<f64>() - 61_988.7).abs() < 1e-9);
        assert_eq!(market_qty_chunks(20_000.0, &filters), vec![20_000.0]);
    }

    #[test]
    fn microstructure_trial_labels_untraded_candidate_path() {
        let start = 1_800_000_000_000i64;
        let candidate = Candidate {
            symbol: "TESTUSDT".into(),
            signal_ms: start,
            setup_origin_ms: start,
            side: 1,
            price: 100.0,
            return_1h: 0.05,
            return_4h: 0.08,
            volume_ratio: 5.0,
            efficiency: 0.8,
            close_location: 0.9,
            volume_24h: 10_000_000.0,
            score: 1.0,
            entry_phase: "standard_impulse".into(),
            breakout_level: 99.0,
            entry_trigger: default_entry_trigger(),
            risk_scale: 1.0,
            blockers: Vec::new(),
            spot_return_1h: None,
            oi_change_1h: None,
            funding_rate: None,
            perp_premium: None,
        };
        let mut state = PersistedState::new(1_000.0, start);
        observe_microstructure_trial(
            &mut state,
            "confirmed_volume_breakout",
            &candidate,
            start,
            100.0,
            7_200_000,
        );
        let bars = std::collections::HashMap::from([(
            "TESTUSDT".to_owned(),
            vec![Bar {
                open_ms: start,
                close_ms: start + 899_999,
                open: 100.0,
                high: 110.0,
                low: 95.0,
                close: 105.0,
                quote_volume: 1.0,
            }],
        )]);
        let outcomes = update_microstructure_trials(&mut state, &bars, start + 7_200_000);
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0]["executed"], false);
        assert!((outcomes[0]["max_favorable_excursion"].as_f64().unwrap() - 0.10).abs() < 1e-10);
        assert!((outcomes[0]["max_adverse_excursion"].as_f64().unwrap() - 0.05).abs() < 1e-10);
        assert!((outcomes[0]["final_return"].as_f64().unwrap() - 0.05).abs() < 1e-10);
        assert!(state.microstructure_trials.is_empty());
    }

    #[test]
    fn dry_existing_stop_wins_over_same_bar_profit_excursion() {
        let bar = Bar {
            open_ms: 1,
            close_ms: 2,
            open: 100.0,
            high: 103.0,
            low: 98.0,
            close: 102.0,
            quote_volume: 1.0,
        };
        assert_eq!(existing_stop_raw_fill(1, 99.0, &bar), Some(99.0));
        let gap = Bar { open: 98.0, ..bar };
        assert_eq!(existing_stop_raw_fill(1, 99.0, &gap), Some(98.0));
    }

    #[test]
    fn partial_exit_realizes_half_and_preserves_final_trade_accounting() {
        let now_ms = 1_800_000_000_000i64;
        let mut state = PersistedState::new(1_000.0, now_ms);
        let mut position = Position {
            symbol: "TESTUSDT".into(),
            side: 1,
            setup_origin_ms: now_ms - 1_000,
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
            protection_order_ids: Vec::new(),
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "cross_section_momentum".into(),
            partial_take_profit_done: false,
            loss_trim_done: false,
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
        assert_eq!(state.cross_exits, 1);
        assert!((state.cross_realized_pnl - 13.9925).abs() < 1e-10);
        assert!(state.positions.is_empty());
    }

    #[test]
    fn failed_breakout_only_fires_early_without_prior_follow_through() {
        let now_ms = 1_800_000_000_000i64;
        let mut position = Position {
            symbol: "TESTUSDT".into(),
            side: 1,
            setup_origin_ms: now_ms,
            qty: 100.0,
            entry_ms: now_ms,
            entry_price: 100.0,
            entry_fee: 0.1,
            initial_notional: 10_000.0,
            extreme: 100.4,
            adverse_extreme: Some(97.0),
            stop_price: 95.0,
            last_bar_ms: now_ms,
            protection_order_id: None,
            protection_order_ids: Vec::new(),
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "standard_impulse".into(),
            partial_take_profit_done: false,
            loss_trim_done: false,
            last_partial_exit_ms: None,
            realized_partial_pnl: 0.0,
        };
        assert!(failed_breakout(
            &position,
            97.0,
            now_ms + 10 * 60_000,
            15,
            0.03,
            0.005
        ));
        position.extreme = 100.6;
        assert!(!failed_breakout(
            &position,
            97.0,
            now_ms + 10 * 60_000,
            15,
            0.03,
            0.005
        ));
        position.extreme = 100.4;
        assert!(!failed_breakout(
            &position,
            97.0,
            now_ms + 16 * 60_000,
            15,
            0.03,
            0.005
        ));
        position.partial_take_profit_done = true;
        assert!(!failed_breakout(
            &position,
            96.0,
            now_ms + 10 * 60_000,
            15,
            0.03,
            0.005
        ));
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
    fn risk_day_rolls_over_at_beijing_midnight() {
        // 2026-08-13 15:59:59 UTC = 23:59:59 Asia/Shanghai.
        let before = 1_786_636_799_999i64;
        let after = before + 1;
        assert_eq!(super::risk_day(after), super::risk_day(before) + 1);
    }

    #[test]
    fn detected_exit_uses_persisted_protection_stage() {
        let mut position = Position {
            symbol: "TESTUSDT".into(),
            side: 1,
            setup_origin_ms: 1,
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
            protection_order_ids: vec![42],
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "standard_impulse".into(),
            partial_take_profit_done: false,
            loss_trim_done: false,
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
        state.daily_loss_latched = true;
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
        assert!(!state.daily_loss_latched);
        assert!(
            state.overextension_long_blocked,
            "手动总风控重置不得清除同类失败熔断"
        );
        let log = std::fs::read_to_string(&path).unwrap();
        assert!(log.contains("daily_risk_reset"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn open_ended_paper_run_has_no_profit_or_window_entry_cap() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        let cfg = strategy.altcoin_impulse;
        let started_ms = 1_800_000_000_000i64;
        let mut state = PersistedState::new(1_000.0, started_ms);
        state.first_week_started_ms = started_ms;
        state.first_week_start_equity = 1_000.0;

        let profitable =
            first_week_progress(&state, &cfg, 10_000.0, started_ms + 6 * super::DAY_MS);
        assert!(!profitable.entries_blocked);

        let expired = first_week_progress(&state, &cfg, 1_100.0, started_ms + 7 * super::DAY_MS);
        assert!(expired.window_complete);
        assert!(!expired.entries_blocked);

        let loss_limit = first_week_progress(&state, &cfg, 880.0, started_ms + 2 * super::DAY_MS);
        assert!(loss_limit.loss_limit_reached);
        assert!(loss_limit.entries_blocked);

        let late_loss = first_week_progress(&state, &cfg, 800.0, started_ms + 8 * super::DAY_MS);
        assert!(!late_loss.loss_limit_reached);
        assert!(!late_loss.entries_blocked);
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
            setup_origin_ms: now_ms - 1_000,
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
            protection_order_ids: Vec::new(),
            protection_reason: "initial_stop".into(),
            exchange_leverage: Some(10),
            entry_phase: "overextended_long".into(),
            partial_take_profit_done: false,
            loss_trim_done: false,
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
            "symbol": "HOLOUSDT",
            "side": 1
        })];
        assert!(recently_exited_symbol(&trades, "HOLOUSDT", now_ms, 24));
        assert!(!recently_exited_symbol(&trades, "OTHERUSDT", now_ms, 24));
        assert!(!recently_exited_symbol(&trades, "HOLOUSDT", now_ms, 4));
        assert!(recently_exited_opposite_side(
            &trades, "HOLOUSDT", -1, now_ms, 24
        ));
        assert!(!recently_exited_opposite_side(
            &trades, "HOLOUSDT", 1, now_ms, 24
        ));
        assert!(!recently_exited_opposite_side(
            &trades, "HOLOUSDT", -1, now_ms, 4
        ));
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
    fn zero_daily_entry_limit_is_unlimited() {
        assert!(!daily_entry_limit_reached(0, 0));
        assert!(!daily_entry_limit_reached(u32::MAX, 0));
        assert!(!daily_entry_limit_reached(9, 10));
        assert!(daily_entry_limit_reached(10, 10));

        let now_ms = 1_800_000_000_000i64;
        let mut state = PersistedState::new(1_000.0, now_ms);
        let path = std::env::temp_dir().join(format!(
            "greed-unlimited-entry-bonus-{}-{}.jsonl",
            std::process::id(),
            now_ms
        ));
        assert!(
            !apply_daily_entry_bonus(&mut state, 0, 0, now_ms, path.to_str().unwrap()).unwrap()
        );
        assert_eq!(state.daily_entry_bonus, 0);
        assert!(!path.exists());
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
    fn retest_confirmation_is_mirrored_for_long_and_short() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        let cfg = strategy.altcoin_impulse;
        let bar = |open: f64, high: f64, low: f64, close: f64| Bar {
            open_ms: 1,
            close_ms: 2,
            open,
            high,
            low,
            close,
            quote_volume: 1.0,
        };
        assert_eq!(
            pending_decision(
                1,
                100.0,
                "breakout_retest",
                &bar(99.8, 101.0, 99.5, 100.4),
                false,
                &cfg
            ),
            PendingDecision::RetestSeen
        );
        assert_eq!(
            pending_decision(
                -1,
                100.0,
                "breakout_retest",
                &bar(100.2, 100.5, 99.0, 99.6),
                false,
                &cfg
            ),
            PendingDecision::RetestSeen
        );
        assert_eq!(
            pending_decision(
                1,
                100.0,
                "breakout_retest",
                &bar(100.1, 101.0, 100.05, 100.4),
                true,
                &cfg
            ),
            PendingDecision::Confirmed
        );
        assert_eq!(
            pending_decision(
                1,
                107.5,
                "vertical_impulse_retest",
                &bar(107.7, 111.0, 107.6, 110.0),
                true,
                &cfg
            ),
            PendingDecision::RetestSeen
        );

        let vertical_candidate = Candidate {
            symbol: "GIGGLEUSDT".to_owned(),
            signal_ms: 1,
            setup_origin_ms: 1,
            side: 1,
            price: 33.50,
            return_1h: 0.0859,
            return_4h: 0.0916,
            volume_ratio: 8.65,
            efficiency: 0.98,
            close_location: 0.86,
            volume_24h: 1_000_000.0,
            score: 1.0,
            entry_phase: "standard_impulse".to_owned(),
            breakout_level: 31.18,
            entry_trigger: default_entry_trigger(),
            risk_scale: 1.0,
            blockers: vec![],
            spot_return_1h: None,
            oi_change_1h: None,
            funding_rate: None,
            perp_premium: None,
        };
        let (anchor, mode) = confirmation_plan(&vertical_candidate, &cfg);
        assert_eq!(anchor, vertical_candidate.price);
        assert_eq!(mode, "vertical_impulse_retest");
        assert_eq!(
            intrabar_pending_decision(1, 33.50, 33.55, false, None, None, &cfg),
            IntrabarPendingDecision::Waiting
        );
        assert_eq!(
            intrabar_pending_decision(1, 33.50, 33.20, false, None, None, &cfg),
            IntrabarPendingDecision::Touched
        );
        assert_eq!(
            intrabar_pending_decision(1, 33.50, 33.60, true, Some(33.20), Some(33.40), &cfg),
            IntrabarPendingDecision::Confirmed
        );
        assert_eq!(
            intrabar_pending_decision(1, 33.50, 36.08, true, Some(33.20), Some(35.80), &cfg),
            IntrabarPendingDecision::Waiting
        );
        assert_eq!(
            intrabar_pending_decision(1, 33.50, 32.47, true, Some(33.20), Some(33.07), &cfg),
            IntrabarPendingDecision::Invalidated
        );
        assert_eq!(
            pending_decision(
                -1,
                100.0,
                "breakout_retest",
                &bar(99.9, 99.95, 99.0, 99.6),
                true,
                &cfg
            ),
            PendingDecision::Confirmed
        );
        assert_eq!(
            pending_decision(
                1,
                100.0,
                "breakout_retest",
                &bar(100.0, 100.5, 98.4, 98.8),
                false,
                &cfg
            ),
            PendingDecision::Invalidated
        );
        assert_eq!(
            pending_decision(
                -1,
                100.0,
                "breakout_retest",
                &bar(100.0, 101.6, 99.5, 101.2),
                false,
                &cfg
            ),
            PendingDecision::Invalidated
        );

        // A vertical impulse is anchored at its signal close rather than the
        // stale 24h boundary. The first shallow pullback only arms the setup;
        // a later independent bar must still confirm direction.
        assert_eq!(
            pending_decision(
                1,
                107.5,
                "vertical_impulse_retest",
                &bar(107.6, 108.0, 107.1, 107.8),
                false,
                &cfg
            ),
            PendingDecision::RetestSeen
        );
        assert_eq!(
            pending_decision(
                1,
                107.5,
                "vertical_impulse_retest",
                &bar(107.7, 108.4, 107.6, 108.1),
                true,
                &cfg
            ),
            PendingDecision::Confirmed
        );
    }

    #[test]
    fn deployed_altcoin_config_is_live_ready_with_daily_cap() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        assert!(strategy.altcoin_impulse.enabled);
        assert!(strategy.altcoin_cross_section.enabled);
        assert_eq!(strategy.altcoin_impulse.capital_usdt, 1_500.0);
        assert_eq!(strategy.altcoin_impulse.max_daily_entries, 15);
        assert!(strategy.altcoin_impulse.allow_live);
        assert_eq!(strategy.altcoin_impulse.max_positions, 5);
        assert_eq!(strategy.altcoin_impulse.scan_limit, 120);
        assert_eq!(strategy.altcoin_impulse.max_daily_entry_bonus, 0);
        assert_eq!(strategy.altcoin_impulse.risk_per_trade, 0.04);
        assert_eq!(strategy.altcoin_impulse.long_risk_scale, 0.75);
        assert_eq!(strategy.altcoin_impulse.short_risk_scale, 1.0);
        assert_eq!(strategy.altcoin_impulse.max_directional_funding_rate, 0.003);
        assert_eq!(strategy.altcoin_impulse.max_directional_premium, 0.01);
        assert_eq!(strategy.altcoin_impulse.stop_pct, 0.010);
        assert_eq!(strategy.altcoin_impulse.max_gross_multiple, 0.50);
        assert_eq!(strategy.altcoin_impulse.first_week_duration_days, 7);
        assert_eq!(strategy.altcoin_impulse.daily_loss_limit, 0.025);
        assert_eq!(strategy.altcoin_impulse.first_week_loss_limit, 0.05);
        assert_eq!(strategy.altcoin_impulse.dry_slippage_bps, 5.0);
        assert_eq!(strategy.altcoin_impulse.trail_activation_pct, 0.018);
        assert_eq!(strategy.altcoin_impulse.trail_pct, 0.0075);
        assert_eq!(strategy.altcoin_impulse.partial_take_profit_fraction, 0.40);
        assert_eq!(strategy.altcoin_impulse.max_hold_hours, 2);
        assert_eq!(strategy.altcoin_impulse.loss_trim_trigger_pct, 0.01);
        assert!(!strategy.altcoin_impulse.loss_trim_enabled);
        assert!(!strategy.altcoin_impulse.failed_breakout_enabled);
        assert!(!strategy.altcoin_impulse.recovery_lock_enabled);
        assert!(!strategy.altcoin_impulse.direct_entry_enabled);
        assert!(!strategy.altcoin_impulse.fixed_time_exit_only);
        assert_eq!(strategy.altcoin_impulse.loss_trim_fraction, 0.50);
        assert!(!strategy.altcoin_impulse.extreme_direct_enabled);
        assert_eq!(strategy.altcoin_impulse.max_spread_bps, 10.0);
        assert_eq!(strategy.altcoin_impulse.liquid_market_max_spread_bps, 15.0);
        assert_eq!(strategy.altcoin_impulse.min_contract_age_days, 7);
        assert_eq!(strategy.altcoin_impulse.max_entry_impact_bps, 15.0);
        assert_eq!(strategy.altcoin_impulse.max_exit_impact_bps, 20.0);
        assert_eq!(strategy.altcoin_impulse.min_depth_multiple, 10.0);
        assert_eq!(strategy.altcoin_impulse.min_recent_trades, 30);
        assert_eq!(strategy.altcoin_impulse.min_unique_trade_prices, 8);
        assert!(!strategy.altcoin_impulse.unique_trade_prices_hard);
        assert_eq!(strategy.altcoin_impulse.cooldown_hours, 4);
        assert_eq!(strategy.altcoin_impulse.max_signal_age_seconds, 300);
        assert_eq!(strategy.altcoin_impulse.confirmation_window_bars, 4);
        assert_eq!(strategy.altcoin_impulse.retest_touch_pct, 0.01);
        assert_eq!(strategy.altcoin_impulse.retest_invalidation_pct, 0.015);
        assert_eq!(strategy.altcoin_impulse.reclaim_pct, 0.002);
        assert!(strategy.altcoin_impulse.adaptive_retest_enabled);
        assert_eq!(strategy.altcoin_impulse.vertical_overshoot_pct, 0.035);
        assert_eq!(strategy.altcoin_impulse.vertical_retest_touch_pct, 0.005);
        assert_eq!(
            strategy.altcoin_impulse.vertical_retest_invalidation_pct,
            0.015
        );
        assert_eq!(strategy.altcoin_impulse.vertical_reclaim_pct, 0.002);
        assert_eq!(
            strategy.altcoin_impulse.vertical_max_entry_extension_pct,
            0.02
        );
        assert!(strategy.altcoin_impulse.intrabar_vertical_retest_enabled);
        assert_eq!(strategy.altcoin_impulse.intrabar_min_pullback_pct, 0.003);
        assert_eq!(strategy.altcoin_impulse.intrabar_rebound_pct, 0.003);
        assert_eq!(strategy.altcoin_impulse.extreme_direct_risk_scale, 0.33);
        assert!(strategy.altcoin_impulse.pulse_exhaustion_enabled);
        assert!(!strategy.altcoin_impulse.pulse_exhaustion_allow_live);
        assert_eq!(strategy.altcoin_impulse.pulse_initial_return_1h, 0.06);
        assert_eq!(strategy.altcoin_impulse.pulse_initial_volume_ratio, 10.0);
        assert_eq!(strategy.altcoin_impulse.pulse_oi_change_1h, 0.20);
        assert_eq!(strategy.altcoin_impulse.pulse_min_peak_retrace, 0.035);
        assert_eq!(strategy.altcoin_impulse.pulse_risk_scale, 0.40);
        assert_eq!(strategy.altcoin_impulse.pulse_max_positions, 1);
        assert_eq!(strategy.altcoin_impulse.pulse_max_gross_multiple, 1.00);
        assert_eq!(strategy.altcoin_impulse.pulse_stop_pct, 0.020);
        assert_eq!(strategy.altcoin_impulse.pulse_trail_activation_pct, 0.020);
        assert_eq!(strategy.altcoin_impulse.pulse_trail_pct, 0.0075);
        assert_eq!(
            strategy.altcoin_impulse.pulse_max_directional_premium,
            0.015
        );
        assert_eq!(strategy.altcoin_impulse.recovery_lock_adverse_pct, 0.03);
        assert_eq!(strategy.altcoin_impulse.recovery_lock_activation_pct, 0.01);
        assert_eq!(strategy.altcoin_impulse.recovery_lock_pct, 0.0025);
        assert_eq!(strategy.altcoin_impulse.overextension_long_return_1h, 0.12);
        assert_eq!(strategy.altcoin_impulse.overextension_long_return_4h, 0.15);
        assert!(!strategy.altcoin_impulse.overextension_long_enabled);
        assert_eq!(strategy.altcoin_impulse.min_volume_ratio, 4.0);
        assert_eq!(strategy.altcoin_impulse.min_efficiency, 0.45);
        assert_eq!(strategy.altcoin_impulse.min_close_location, 0.70);
        assert_eq!(
            strategy
                .altcoin_impulse
                .overextension_reentry_lookback_hours,
            24
        );
        assert_eq!(strategy.altcoin_impulse.risk_execution_buffer_pct, 0.005);
        assert_eq!(strategy.altcoin_cross_section.formation_hours, 12);
        assert_eq!(strategy.altcoin_cross_section.hold_hours, 3);
        assert_eq!(strategy.altcoin_cross_section.names, 5);
        assert_eq!(strategy.altcoin_cross_section.gate_window, 30);
        assert_eq!(
            strategy.altcoin_cross_section.min_24h_volume_usd,
            10_000_000.0
        );
        assert_eq!(
            strategy.altcoin_cross_section.market_momentum_threshold,
            0.01
        );
        assert_eq!(strategy.altcoin_cross_section.base_gross_multiple, 0.15);
        assert_eq!(strategy.altcoin_cross_section.active_gross_multiple, 0.50);
        assert_eq!(strategy.altcoin_cross_section.strong_excess_return, 0.12);
        assert_eq!(strategy.altcoin_cross_section.strong_gross_multiple, 0.50);
        assert_eq!(
            strategy.altcoin_cross_section.assumed_cost_bps_per_side,
            10.0
        );
        assert_eq!(strategy.altcoin_cross_section.stop_pct, 0.04);
        assert_eq!(strategy.altcoin_cross_section.trail_activation_pct, 0.015);
        assert_eq!(strategy.altcoin_cross_section.trail_pct, 0.005);
        assert_eq!(
            strategy.altcoin_cross_section.partial_take_profit_fraction,
            0.40
        );
        // The failed 15m shock-reversal alpha is intentionally absent from
        // the deployed configuration. The default remains disabled only so
        // older journals/configurations continue to deserialize safely.
        assert!(!strategy.altcoin_shock_reversal.enabled);
    }

    #[test]
    fn cross_section_momentum_follows_market_and_selects_ranked_extremes() {
        let ranks = [-0.30, -0.10, 0.02, 0.05, 0.20, 0.40]
            .into_iter()
            .enumerate()
            .map(|(index, trailing_return)| super::CrossRank {
                symbol: format!("S{index}USDT"),
                trailing_return,
                volume_24h: 100_000_000.0,
                signal_index: 100,
            })
            .collect::<Vec<_>>();
        let selected = super::cross_selected(&ranks, 1, 0.01).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].0.symbol, "S5USDT");
        assert_eq!(selected[0].1, 1);
        let five_ranks = [0.01, 0.02, 0.03, 0.05, 0.20, 0.40]
            .into_iter()
            .enumerate()
            .map(|(index, trailing_return)| super::CrossRank {
                symbol: format!("P{index}USDT"),
                trailing_return,
                volume_24h: 100_000_000.0,
                signal_index: 100,
            })
            .collect::<Vec<_>>();
        let selected = super::cross_selected(&five_ranks, 5, 0.01).unwrap();
        assert_eq!(selected.len(), 5);
        assert_eq!(selected[0].0.symbol, "P5USDT");
        assert_eq!(selected[4].0.symbol, "P1USDT");
        assert!(selected.iter().all(|(_, side)| *side == 1));

        let falling = [-0.40, -0.20, -0.05, -0.02, 0.005, 0.008]
            .into_iter()
            .enumerate()
            .map(|(index, trailing_return)| super::CrossRank {
                symbol: format!("F{index}USDT"),
                trailing_return,
                volume_24h: 100_000_000.0,
                signal_index: 100,
            })
            .collect::<Vec<_>>();
        let selected = super::cross_selected(&falling, 1, 0.01).unwrap();
        assert_eq!(selected[0].0.symbol, "F0USDT");
        assert_eq!(selected[0].1, -1);
    }

    #[test]
    fn cross_section_neutral_band_keeps_the_validated_short_bias() {
        let ranks = [-0.03, -0.01, 0.004, 0.008, 0.04, 0.25]
            .into_iter()
            .enumerate()
            .map(|(index, trailing_return)| super::CrossRank {
                symbol: format!("N{index}USDT"),
                trailing_return,
                volume_24h: 100_000_000.0,
                signal_index: 100,
            })
            .collect::<Vec<_>>();
        let selected = super::cross_selected(&ranks, 1, 0.01).unwrap();
        assert_eq!(selected[0].0.symbol, "N0USDT");
        assert_eq!(selected[0].1, -1);

        let ranks = [-0.30, -0.02, -0.005, 0.002, 0.02, 0.05]
            .into_iter()
            .enumerate()
            .map(|(index, trailing_return)| super::CrossRank {
                symbol: format!("W{index}USDT"),
                trailing_return,
                volume_24h: 100_000_000.0,
                signal_index: 100,
            })
            .collect::<Vec<_>>();
        let selected = super::cross_selected(&ranks, 1, 0.01).unwrap();
        assert_eq!(selected[0].0.symbol, "W0USDT");
        assert_eq!(selected[0].1, -1);
    }

    #[test]
    fn cross_section_post_exit_rerank_excludes_only_protective_exits_in_window() {
        let boundary = 1_800_000_000_000i64;
        let mut state = super::PersistedState::new(1_000.0, boundary);
        for event in [
            serde_json::json!({"ts_ms":boundary+1,"event":"exit","entry_phase":"cross_section_momentum","symbol":"STOPUSDT","reason":"initial_stop"}),
            serde_json::json!({"ts_ms":boundary+2,"event":"exit_detected","entry_phase":"cross_section_momentum","symbol":"TPUSDT","reason":"trailing_take_profit"}),
            serde_json::json!({"ts_ms":boundary+3,"event":"exit","entry_phase":"cross_section_momentum","symbol":"ROTATEUSDT","reason":"cross_section_rotation"}),
            serde_json::json!({"ts_ms":boundary-1,"event":"exit","entry_phase":"cross_section_momentum","symbol":"OLDUSDT","reason":"initial_stop"}),
            serde_json::json!({"ts_ms":boundary+4,"event":"exit","entry_phase":"shock_reversal","symbol":"SHOCKUSDT","reason":"initial_stop"}),
        ] {
            state.record_trade(event);
        }
        let (symbols, latest) = super::cross_protective_exits_since(&state, boundary);
        assert_eq!(symbols.len(), 2);
        assert!(symbols.contains("STOPUSDT"));
        assert!(symbols.contains("TPUSDT"));
        assert_eq!(latest, Some(boundary + 2));
    }

    #[test]
    fn cross_section_gate_rebuilds_thirty_completed_shadow_signals() {
        let strategy: super::StrategyFile = toml::from_str(include_str!(
            "../../../config/strategy-altcoin-impulse.toml"
        ))
        .unwrap();
        let mut bars_by_symbol = std::collections::HashMap::new();
        for (symbol, slope) in [
            ("AUSDT", 0.001),
            ("BUSDT", 0.0009),
            ("CUSDT", 0.0008),
            ("DUSDT", 0.0007),
            ("EUSDT", 0.0006),
            ("FUSDT", 0.0005),
        ] {
            let bars = (0..1_100)
                .map(|index| {
                    let open_ms = index * 15 * 60_000;
                    let price = (slope * index as f64).exp();
                    Bar {
                        open_ms,
                        close_ms: open_ms + 15 * 60_000 - 1,
                        open: price,
                        high: price * 1.001,
                        low: price * 0.999,
                        close: price,
                        quote_volume: 1_000_000.0,
                    }
                })
                .collect();
            bars_by_symbol.insert(symbol.to_owned(), bars);
        }
        let boundary_ms = 1_100 * 15 * 60_000;
        let (candidates, status) = super::cross_section_analysis(
            &bars_by_symbol,
            boundary_ms,
            &strategy.altcoin_cross_section,
            &strategy.altcoin_impulse,
            &std::collections::HashSet::new(),
        );
        assert_eq!(status["gate"]["samples"], 30);
        assert_eq!(status["min_universe_size"], 5);
        assert_eq!(status["selected"].as_array().unwrap().len(), 5);
        assert_eq!(candidates.len(), 6);
        assert_eq!(status["execution"]["reserve_count"], 1);
        let basket_gross = status["execution"]["gross_multiple"].as_f64().unwrap();
        assert!(
            (candidates
                .iter()
                .filter(|candidate| {
                    candidate.entry_trigger == "scheduled_cross_section_momentum"
                })
                .map(|candidate| candidate.risk_scale)
                .sum::<f64>()
                - basket_gross)
                .abs()
                < f64::EPSILON
        );
        assert_eq!(candidates[0].entry_phase, "cross_section_momentum");
        assert_eq!(
            candidates.last().unwrap().entry_trigger,
            "scheduled_cross_section_momentum_reserve"
        );
        assert_eq!(candidates[0].side, 1);
        assert!(super::cross_section_status_complete(&status));
        let excluded = std::collections::HashSet::from([candidates[0].symbol.clone()]);
        let (replacement, replacement_status) = super::cross_section_analysis(
            &bars_by_symbol,
            boundary_ms,
            &strategy.altcoin_cross_section,
            &strategy.altcoin_impulse,
            &excluded,
        );
        assert_eq!(replacement.len(), 5);
        assert!(replacement
            .iter()
            .all(|candidate| candidate.symbol != candidates[0].symbol));
        assert_eq!(
            replacement_status["reentry_exclusions"][0],
            excluded.iter().next().unwrap().as_str()
        );
        assert!(!super::cross_section_status_complete(&serde_json::json!({
            "model":"12h_cross_section_momentum",
            "universe_count":4
        })));
    }
}
