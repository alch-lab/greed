use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategyConfig {
    pub symbols: Vec<String>,
    pub universe: UniverseConfig,
    pub lanes: LaneConfig,
    pub risk: RiskConfig,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            symbols: vec!["BTCUSDT".into(), "ETHUSDT".into(), "SOLUSDT".into()],
            universe: UniverseConfig::default(),
            lanes: LaneConfig::default(),
            risk: RiskConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UniverseConfig {
    pub dynamic_enabled: bool,
    pub max_symbols: usize,
    pub top_liquidity_names: usize,
    pub top_mover_names: usize,
    pub min_24h_quote_volume_usd: f64,
    pub surge_min_24h_quote_volume_usd: f64,
    pub surge_min_abs_change_24h: f64,
    pub surge_min_abs_return_15m: f64,
    pub refresh_seconds: u32,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            dynamic_enabled: true,
            max_symbols: 30,
            top_liquidity_names: 18,
            top_mover_names: 12,
            min_24h_quote_volume_usd: 15_000_000.0,
            surge_min_24h_quote_volume_usd: 2_000_000.0,
            surge_min_abs_change_24h: 0.08,
            surge_min_abs_return_15m: 0.015,
            refresh_seconds: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LaneConfig {
    pub max_candidates_per_lane: usize,
    pub max_spread_bps: f64,
    pub fast_activation_enabled: bool,
    pub fast_min_market_return_1h: f64,
    pub fast_min_market_breadth: f64,
    pub fast_min_body_return_5m: f64,
    pub fast_min_volume_ratio_5m: f64,
    pub fast_min_flow_5m: f64,
    pub fast_max_compression_ratio: f64,
    /// A second, stricter ignition shape for a market that is already
    /// trending. This avoids forcing every valid acceleration through the
    /// compression-breakout gate while still refusing weak late chases.
    pub fast_reacceleration_enabled: bool,
    pub fast_reacceleration_max_compression_ratio: f64,
    pub fast_reacceleration_min_body_return_5m: f64,
    pub fast_reacceleration_min_volume_ratio_5m: f64,
    pub fast_reacceleration_min_flow_5m: f64,
    /// Directly crossing the spread after an already-running impulse is only
    /// allowed when the broader altcoin tape confirms the direction and the
    /// combined pre-break plus ignition move is still bounded. Otherwise the
    /// recipe must obtain a genuine one-minute reclaim.
    pub fast_reacceleration_direct_min_market_breadth: f64,
    pub fast_reacceleration_direct_min_market_return_1h: f64,
    pub fast_reacceleration_direct_max_extension: f64,
    pub fast_reacceleration_direct_early_extension: f64,
    pub fast_max_prebreak_return_1h: f64,
    pub fast_max_return_4h: f64,
    pub fast_max_oi_change_15m: f64,
    pub fast_risk_per_trade_pct: f64,
    pub fast_min_fill_ratio: f64,
    pub fast_min_managed_fill_ratio: f64,
    pub liquidation_reversal_enabled: bool,
    pub liquidation_window_seconds: u32,
    pub liquidation_min_dominance: f64,
    pub liquidation_min_depth_ratio: f64,
    pub liquidation_min_aligned_return_bps: f64,
    pub liquidation_min_reversal_bps: f64,
    pub liquidation_min_entry_delay_seconds: u32,
    pub liquidation_max_signal_age_seconds: u32,
    pub liquidation_cooldown_seconds: u32,
    pub liquidation_stop_pct: f64,
    pub liquidation_hold_minutes: u32,
    pub liquidation_managed_exit_enabled: bool,
    pub liquidation_profit_shield_activation_bps: f64,
    pub liquidation_profit_shield_floor_bps: f64,
    pub liquidation_trailing_activation_bps: f64,
    pub liquidation_trailing_distance_bps: f64,
    pub liquidation_risk_per_trade_pct: f64,
    /// Maximum absolute difference between the strategy reference and the
    /// executable quote on the configured execution venue. This is primarily
    /// an execution-safety boundary for Demo, whose altcoin books can diverge
    /// materially from mainnet market data.
    pub liquidation_max_execution_divergence_bps: f64,
    pub trend_min_return_4h: f64,
    pub trend_min_efficiency: f64,
    pub trend_min_hour_volume_ratio: f64,
    pub trend_min_flow_imbalance: f64,
    pub trend_max_age_bars: usize,
    pub trend_max_extension_atr: f64,
    pub trend_max_reclaim_body_atr: f64,
    pub trend_max_signal_age_seconds: u32,
    pub trend_limit_offset_atr: f64,
    pub trend_entry_timeout_seconds: u32,
    pub trend_max_entry_adverse_bps: f64,
    pub trend_min_fill_ratio: f64,
    pub trend_min_managed_fill_ratio: f64,
    pub trend_entry_invalidation_bps: f64,
    pub trend_max_post_signal_extension_bps: f64,
    pub trend_max_post_signal_favorable_bps: f64,
    pub trend_max_opposing_micro_flow: f64,
    pub trend_max_opposing_micro_return_bps: f64,
    pub trend_risk_per_trade_pct: f64,
    pub trend_profit_shield_activation_r: f64,
    pub trend_profit_shield_buffer_pct: f64,
    pub trend_pre_tp_trailing_activation_r: f64,
    pub trend_early_failure_seconds: u32,
    pub trend_early_failure_grace_seconds: u32,
    pub trend_early_failure_grace_min_body_atr: f64,
    pub trend_early_failure_adverse_r: f64,
    pub trend_early_failure_max_mfe_r: f64,
    pub trend_reentry_enabled: bool,
    pub trend_reentry_window_minutes: u32,
    pub trend_reentry_reset_pct: f64,
    pub trend_reentry_lookback_bars: usize,
    pub trend_reentry_min_body_pct: f64,
    pub trend_reentry_min_flow: f64,
    pub trend_reentry_min_stop_pct: f64,
    pub trend_reentry_max_stop_pct: f64,
    pub trend_reentry_profit_shield_pct: f64,
    pub trend_reentry_trailing_activation_pct: f64,
    pub trend_reentry_trailing_distance_pct: f64,
    pub trend_reentry_entry_timeout_seconds: u32,
}

impl Default for LaneConfig {
    fn default() -> Self {
        Self {
            max_candidates_per_lane: 2,
            max_spread_bps: 6.0,
            fast_activation_enabled: true,
            fast_min_market_return_1h: 0.002,
            fast_min_market_breadth: 0.50,
            fast_min_body_return_5m: 0.005,
            fast_min_volume_ratio_5m: 2.0,
            fast_min_flow_5m: 0.15,
            fast_max_compression_ratio: 1.05,
            fast_reacceleration_enabled: true,
            fast_reacceleration_max_compression_ratio: 3.0,
            fast_reacceleration_min_body_return_5m: 0.0075,
            fast_reacceleration_min_volume_ratio_5m: 4.0,
            fast_reacceleration_min_flow_5m: 0.25,
            fast_reacceleration_direct_min_market_breadth: 0.60,
            fast_reacceleration_direct_min_market_return_1h: 0.002,
            fast_reacceleration_direct_max_extension: 0.025,
            fast_reacceleration_direct_early_extension: 0.015,
            fast_max_prebreak_return_1h: 0.03,
            fast_max_return_4h: 0.06,
            fast_max_oi_change_15m: 0.03,
            fast_risk_per_trade_pct: 0.005,
            fast_min_fill_ratio: 0.80,
            fast_min_managed_fill_ratio: 0.20,
            liquidation_reversal_enabled: true,
            liquidation_window_seconds: 3,
            liquidation_min_dominance: 0.80,
            liquidation_min_depth_ratio: 1.20,
            liquidation_min_aligned_return_bps: 3.0,
            liquidation_min_reversal_bps: 12.0,
            liquidation_min_entry_delay_seconds: 1,
            liquidation_max_signal_age_seconds: 12,
            liquidation_cooldown_seconds: 30,
            liquidation_stop_pct: 0.020,
            liquidation_hold_minutes: 1,
            liquidation_managed_exit_enabled: false,
            liquidation_profit_shield_activation_bps: 25.0,
            liquidation_profit_shield_floor_bps: 10.0,
            liquidation_trailing_activation_bps: 50.0,
            liquidation_trailing_distance_bps: 30.0,
            liquidation_risk_per_trade_pct: 0.0025,
            liquidation_max_execution_divergence_bps: 15.0,
            trend_min_return_4h: 0.025,
            trend_min_efficiency: 0.45,
            trend_min_hour_volume_ratio: 0.65,
            trend_min_flow_imbalance: 0.0,
            trend_max_age_bars: 2,
            trend_max_extension_atr: 3.5,
            trend_max_reclaim_body_atr: 2.5,
            trend_max_signal_age_seconds: 300,
            trend_limit_offset_atr: 0.03,
            trend_entry_timeout_seconds: 90,
            trend_max_entry_adverse_bps: 8.0,
            trend_min_fill_ratio: 0.80,
            trend_min_managed_fill_ratio: 0.20,
            trend_entry_invalidation_bps: 30.0,
            trend_max_post_signal_extension_bps: 12.0,
            trend_max_post_signal_favorable_bps: 30.0,
            trend_max_opposing_micro_flow: 0.10,
            trend_max_opposing_micro_return_bps: 3.0,
            trend_risk_per_trade_pct: 0.015,
            trend_profit_shield_activation_r: 0.32,
            trend_profit_shield_buffer_pct: 0.0015,
            trend_pre_tp_trailing_activation_r: 1.0,
            trend_early_failure_seconds: 180,
            trend_early_failure_grace_seconds: 180,
            trend_early_failure_grace_min_body_atr: 2.0,
            trend_early_failure_adverse_r: 0.50,
            trend_early_failure_max_mfe_r: 0.32,
            trend_reentry_enabled: true,
            trend_reentry_window_minutes: 180,
            trend_reentry_reset_pct: 0.006,
            trend_reentry_lookback_bars: 2,
            trend_reentry_min_body_pct: 0.002,
            trend_reentry_min_flow: 0.05,
            trend_reentry_min_stop_pct: 0.003,
            trend_reentry_max_stop_pct: 0.0125,
            trend_reentry_profit_shield_pct: 0.008,
            trend_reentry_trailing_activation_pct: 0.010,
            trend_reentry_trailing_distance_pct: 0.005,
            trend_reentry_entry_timeout_seconds: 30,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    pub risk_per_trade_pct: f64,
    pub high_confidence_risk_per_trade_pct: f64,
    pub high_confidence_threshold: f64,
    pub max_notional_per_trade_multiple: f64,
    pub max_total_gross_multiple: f64,
    /// Never consume more than this fraction of the executable top-20 side.
    pub max_book_participation_pct: f64,
    /// Maximum simulated VWAP impact for a protective/taker exit.
    pub max_book_slippage_bps: f64,
    /// A liquidity-reduced order must retain this fraction of desired size.
    pub min_liquidity_size_ratio: f64,
    /// And it must remain at least this fraction of current strategy equity.
    pub min_liquidity_notional_multiple: f64,
    pub max_positions: usize,
    pub initial_stop_pct: f64,
    pub first_take_profit_r: f64,
    pub first_take_profit_fraction: f64,
    pub runner_take_profit_r: f64,
    pub break_even_buffer_pct: f64,
    pub profit_shield_activation_r: f64,
    pub pre_tp_trailing_activation_r: f64,
    pub trailing_distance_pct: f64,
    pub max_hold_minutes: u32,
    pub daily_loss_limit_pct: f64,
    pub peak_drawdown_halt_pct: f64,
    pub rolling_pf_window: usize,
    pub rolling_pf_min_trades: usize,
    pub rolling_pf_floor: f64,
    pub rolling_pf_cooldown_minutes: u32,
    pub loss_cooldown_minutes: u32,
    pub rolling_pf_probe_size_multiplier: f64,
    pub rolling_pf_epoch: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            risk_per_trade_pct: 0.010,
            high_confidence_risk_per_trade_pct: 0.010,
            high_confidence_threshold: 0.85,
            max_notional_per_trade_multiple: 1.50,
            max_total_gross_multiple: 4.0,
            max_book_participation_pct: 0.35,
            max_book_slippage_bps: 8.0,
            min_liquidity_size_ratio: 0.50,
            min_liquidity_notional_multiple: 0.20,
            max_positions: 3,
            initial_stop_pct: 0.0125,
            first_take_profit_r: 2.0,
            first_take_profit_fraction: 0.40,
            runner_take_profit_r: 0.0,
            break_even_buffer_pct: 0.0018,
            profit_shield_activation_r: 0.5,
            pre_tp_trailing_activation_r: 0.8,
            trailing_distance_pct: 0.005,
            max_hold_minutes: 0,
            daily_loss_limit_pct: 0.025,
            peak_drawdown_halt_pct: 0.10,
            rolling_pf_window: 20,
            rolling_pf_min_trades: 8,
            rolling_pf_floor: 1.0,
            rolling_pf_cooldown_minutes: 360,
            loss_cooldown_minutes: 180,
            rolling_pf_probe_size_multiplier: 1.0,
            rolling_pf_epoch: 6,
        }
    }
}

impl StrategyConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.symbols.is_empty() {
            return Err("at least one seed symbol is required".into());
        }
        if self.universe.max_symbols < self.symbols.len()
            || self.universe.max_symbols > 50
            || self.universe.top_liquidity_names + self.universe.top_mover_names
                < self.universe.max_symbols
            || self.universe.min_24h_quote_volume_usd <= 0.0
            || self.universe.surge_min_24h_quote_volume_usd <= 0.0
            || self.universe.surge_min_24h_quote_volume_usd > self.universe.min_24h_quote_volume_usd
            || !(0.01..=0.50).contains(&self.universe.surge_min_abs_change_24h)
            || !(0.005..=0.10).contains(&self.universe.surge_min_abs_return_15m)
            || !(5..=60).contains(&self.universe.refresh_seconds)
        {
            return Err("unified universe parameters are invalid".into());
        }
        let lanes = &self.lanes;
        if !(1..=5).contains(&lanes.max_candidates_per_lane)
            || !(0.0..=0.02).contains(&lanes.fast_min_market_return_1h)
            || !(0.40..=0.90).contains(&lanes.fast_min_market_breadth)
            || !(0.002..=0.02).contains(&lanes.fast_min_body_return_5m)
            || !(1.0..=10.0).contains(&lanes.fast_min_volume_ratio_5m)
            || !(0.0..=0.80).contains(&lanes.fast_min_flow_5m)
            || !(0.5..=1.5).contains(&lanes.fast_max_compression_ratio)
            || !(lanes.fast_max_compression_ratio..=4.0)
                .contains(&lanes.fast_reacceleration_max_compression_ratio)
            || !(lanes.fast_min_body_return_5m..=0.03)
                .contains(&lanes.fast_reacceleration_min_body_return_5m)
            || !(lanes.fast_min_volume_ratio_5m..=15.0)
                .contains(&lanes.fast_reacceleration_min_volume_ratio_5m)
            || !(lanes.fast_min_flow_5m..=0.80).contains(&lanes.fast_reacceleration_min_flow_5m)
            || !(0.50..=0.90).contains(&lanes.fast_reacceleration_direct_min_market_breadth)
            || !(0.0..=0.02).contains(&lanes.fast_reacceleration_direct_min_market_return_1h)
            || !(lanes.fast_reacceleration_min_body_return_5m..=0.05)
                .contains(&lanes.fast_reacceleration_direct_max_extension)
            || !(lanes.fast_reacceleration_min_body_return_5m
                ..=lanes.fast_reacceleration_direct_max_extension)
                .contains(&lanes.fast_reacceleration_direct_early_extension)
            || !(0.0..=0.05).contains(&lanes.fast_max_prebreak_return_1h)
            || !(0.01..=0.10).contains(&lanes.fast_max_return_4h)
            || !(0.001..=0.05).contains(&lanes.fast_max_oi_change_15m)
            || !(0.001..=0.01).contains(&lanes.fast_risk_per_trade_pct)
            || !(0.50..=1.0).contains(&lanes.fast_min_fill_ratio)
            || !(0.0..=lanes.fast_min_fill_ratio).contains(&lanes.fast_min_managed_fill_ratio)
            || lanes.liquidation_window_seconds != 3
            || !(0.50..=1.0).contains(&lanes.liquidation_min_dominance)
            || !(0.10..=5.0).contains(&lanes.liquidation_min_depth_ratio)
            || !(1.0..=50.0).contains(&lanes.liquidation_min_aligned_return_bps)
            || !(0.0..=50.0).contains(&lanes.liquidation_min_reversal_bps)
            || !(1..=5).contains(&lanes.liquidation_min_entry_delay_seconds)
            || lanes.liquidation_max_signal_age_seconds <= lanes.liquidation_min_entry_delay_seconds
            || lanes.liquidation_max_signal_age_seconds > 30
            || !(10..=600).contains(&lanes.liquidation_cooldown_seconds)
            || !(0.005..=0.03).contains(&lanes.liquidation_stop_pct)
            || !(1..=60).contains(&lanes.liquidation_hold_minutes)
            || !(5.0..=100.0).contains(&lanes.liquidation_profit_shield_activation_bps)
            || !(0.0..lanes.liquidation_profit_shield_activation_bps)
                .contains(&lanes.liquidation_profit_shield_floor_bps)
            || !(lanes.liquidation_profit_shield_activation_bps..=200.0)
                .contains(&lanes.liquidation_trailing_activation_bps)
            || !(5.0..=100.0).contains(&lanes.liquidation_trailing_distance_bps)
            || lanes.liquidation_trailing_distance_bps >= lanes.liquidation_trailing_activation_bps
            || !(0.001..=0.01).contains(&lanes.liquidation_risk_per_trade_pct)
            || !(1.0..=50.0).contains(&lanes.liquidation_max_execution_divergence_bps)
            || !(0.02..=0.20).contains(&lanes.trend_min_return_4h)
            || !(0.10..=0.90).contains(&lanes.trend_min_efficiency)
            || lanes.trend_min_hour_volume_ratio <= 0.0
            || !(-0.50..=0.50).contains(&lanes.trend_min_flow_imbalance)
            || !(1..=8).contains(&lanes.trend_max_age_bars)
            || !(1.0..=10.0).contains(&lanes.trend_max_extension_atr)
            || !(0.5..=10.0).contains(&lanes.trend_max_reclaim_body_atr)
            || !(0.0..=0.5).contains(&lanes.trend_limit_offset_atr)
            || !(5..=120).contains(&lanes.trend_entry_timeout_seconds)
            || !(0.0..=12.0).contains(&lanes.trend_max_entry_adverse_bps)
            || !(0.50..=1.0).contains(&lanes.trend_min_fill_ratio)
            || !(0.0..=lanes.trend_min_fill_ratio).contains(&lanes.trend_min_managed_fill_ratio)
            || !(5.0..=100.0).contains(&lanes.trend_entry_invalidation_bps)
            || !(0.0..=100.0).contains(&lanes.trend_max_post_signal_extension_bps)
            || !(0.0..=200.0).contains(&lanes.trend_max_post_signal_favorable_bps)
            || !(0.0..=0.80).contains(&lanes.trend_max_opposing_micro_flow)
            || !(0.0..=25.0).contains(&lanes.trend_max_opposing_micro_return_bps)
            || !(0.001..=0.015).contains(&lanes.trend_risk_per_trade_pct)
            || !(0.2..=0.5).contains(&lanes.trend_profit_shield_activation_r)
            || !(0.0005..=0.003).contains(&lanes.trend_profit_shield_buffer_pct)
            || !(0.5..=2.0).contains(&lanes.trend_pre_tp_trailing_activation_r)
            || !(60..=900).contains(&lanes.trend_early_failure_seconds)
            || !(lanes.trend_early_failure_seconds..=900)
                .contains(&lanes.trend_early_failure_grace_seconds)
            || !(0.5..=5.0).contains(&lanes.trend_early_failure_grace_min_body_atr)
            || !(0.1..=0.9).contains(&lanes.trend_early_failure_adverse_r)
            || !(0.0..=0.5).contains(&lanes.trend_early_failure_max_mfe_r)
            || lanes.trend_early_failure_max_mfe_r > lanes.trend_profit_shield_activation_r
            || !(30..=360).contains(&lanes.trend_reentry_window_minutes)
            || !(0.003..=0.02).contains(&lanes.trend_reentry_reset_pct)
            || !(1..=4).contains(&lanes.trend_reentry_lookback_bars)
            || !(0.001..=0.01).contains(&lanes.trend_reentry_min_body_pct)
            || !(0.0..=0.50).contains(&lanes.trend_reentry_min_flow)
            || !(0.002..=0.01).contains(&lanes.trend_reentry_min_stop_pct)
            || !(lanes.trend_reentry_min_stop_pct..=0.03)
                .contains(&lanes.trend_reentry_max_stop_pct)
            || !(0.003..=0.02).contains(&lanes.trend_reentry_profit_shield_pct)
            || !(lanes.trend_reentry_profit_shield_pct..=0.05)
                .contains(&lanes.trend_reentry_trailing_activation_pct)
            || !(0.002..=0.02).contains(&lanes.trend_reentry_trailing_distance_pct)
            || !(5..=120).contains(&lanes.trend_reentry_entry_timeout_seconds)
            || lanes.max_spread_bps <= 0.0
        {
            return Err("alpha lane parameters are invalid".into());
        }
        let risk = &self.risk;
        if !(0.001..=0.01).contains(&risk.risk_per_trade_pct)
            || risk.high_confidence_risk_per_trade_pct < risk.risk_per_trade_pct
            || risk.high_confidence_risk_per_trade_pct > 0.015
            || !(0.20..=1.5).contains(&risk.max_notional_per_trade_multiple)
            || !(0.5..=4.0).contains(&risk.max_total_gross_multiple)
            || !(0.05..=0.50).contains(&risk.max_book_participation_pct)
            || !(1.0..=25.0).contains(&risk.max_book_slippage_bps)
            || !(0.10..=1.0).contains(&risk.min_liquidity_size_ratio)
            || !(0.05..=0.50).contains(&risk.min_liquidity_notional_multiple)
            || !(1..=5).contains(&risk.max_positions)
            || !(0.003..=0.03).contains(&risk.initial_stop_pct)
            || risk.first_take_profit_r <= 0.0
            || (risk.profit_shield_activation_r != 0.0
                && !(0.5..risk.first_take_profit_r).contains(&risk.profit_shield_activation_r))
            || !(risk.profit_shield_activation_r..risk.first_take_profit_r)
                .contains(&risk.pre_tp_trailing_activation_r)
            || (risk.runner_take_profit_r > 0.0
                && risk.runner_take_profit_r <= risk.first_take_profit_r)
            || !(0.1..=0.9).contains(&risk.first_take_profit_fraction)
            || risk.daily_loss_limit_pct > 0.04
            || risk.peak_drawdown_halt_pct > 0.10
        {
            return Err("portfolio risk parameters are outside demo limits".into());
        }
        if risk.rolling_pf_window < risk.rolling_pf_min_trades
            || risk.rolling_pf_min_trades < 3
            || !(60..=1_440).contains(&risk.loss_cooldown_minutes)
            || risk.rolling_pf_epoch == 0
        {
            return Err("rolling PF parameters are invalid".into());
        }
        Ok(())
    }
}
