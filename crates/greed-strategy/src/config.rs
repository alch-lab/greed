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
    pub refresh_seconds: u32,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            dynamic_enabled: true,
            max_symbols: 30,
            top_liquidity_names: 18,
            top_mover_names: 12,
            min_24h_quote_volume_usd: 25_000_000.0,
            refresh_seconds: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LaneConfig {
    pub sfp_reversal_enabled: bool,
    pub trend_continuation_enabled: bool,
    pub ignition_sprint_enabled: bool,
    pub max_candidates_per_lane: usize,
    pub max_spread_bps: f64,
    pub min_depth_usd: f64,
    pub sfp_lookback_hours: usize,
    pub sfp_min_sweep_atr: f64,
    pub sfp_max_sweep_atr: f64,
    pub sfp_min_volume_ratio: f64,
    pub sfp_confirmation_hours: u32,
    pub sfp_max_signal_age_seconds: u32,
    pub sfp_target_r: f64,
    pub sfp_max_hold_minutes: u32,
    pub sfp_risk_per_trade_pct: f64,
    pub trend_min_return_4h: f64,
    pub trend_min_efficiency: f64,
    pub trend_min_hour_volume_ratio: f64,
    pub trend_min_flow_imbalance: f64,
    pub trend_max_age_bars: usize,
    pub trend_max_signal_age_seconds: u32,
    pub trend_limit_offset_atr: f64,
    pub trend_entry_timeout_seconds: u32,
    pub trend_max_entry_adverse_bps: f64,
    pub trend_taker_fallback_max_adverse_bps: f64,
    pub trend_taker_fallback_size_multiplier: f64,
    pub trend_max_post_signal_extension_bps: f64,
    pub trend_max_opposing_micro_flow: f64,
    pub trend_max_opposing_micro_return_bps: f64,
    pub ignition_min_return_5m: f64,
    pub ignition_min_volume_ratio: f64,
    pub ignition_min_flow_imbalance: f64,
    pub ignition_reclaim_flow_imbalance: f64,
    pub ignition_max_extension_30m: f64,
    pub ignition_max_wait_seconds: u32,
    pub ignition_limit_offset_atr: f64,
    pub ignition_entry_timeout_seconds: u32,
    pub ignition_stop_atr_multiple: f64,
    pub ignition_target_r: f64,
    pub ignition_max_hold_minutes: u32,
    pub ignition_risk_per_trade_pct: f64,
}

impl Default for LaneConfig {
    fn default() -> Self {
        Self {
            sfp_reversal_enabled: true,
            trend_continuation_enabled: true,
            ignition_sprint_enabled: true,
            max_candidates_per_lane: 2,
            max_spread_bps: 6.0,
            min_depth_usd: 20_000.0,
            sfp_lookback_hours: 288,
            sfp_min_sweep_atr: 0.10,
            sfp_max_sweep_atr: 1.25,
            sfp_min_volume_ratio: 1.0,
            sfp_confirmation_hours: 3,
            sfp_max_signal_age_seconds: 300,
            sfp_target_r: 2.0,
            sfp_max_hold_minutes: 180,
            sfp_risk_per_trade_pct: 0.005,
            trend_min_return_4h: 0.06,
            trend_min_efficiency: 0.45,
            trend_min_hour_volume_ratio: 0.65,
            trend_min_flow_imbalance: 0.0,
            trend_max_age_bars: 2,
            trend_max_signal_age_seconds: 300,
            trend_limit_offset_atr: 0.30,
            trend_entry_timeout_seconds: 60,
            trend_max_entry_adverse_bps: 8.0,
            trend_taker_fallback_max_adverse_bps: 20.0,
            trend_taker_fallback_size_multiplier: 0.05,
            trend_max_post_signal_extension_bps: 12.0,
            trend_max_opposing_micro_flow: 0.15,
            trend_max_opposing_micro_return_bps: 3.0,
            ignition_min_return_5m: 0.010,
            ignition_min_volume_ratio: 3.0,
            ignition_min_flow_imbalance: 0.20,
            ignition_reclaim_flow_imbalance: 0.10,
            ignition_max_extension_30m: 0.03,
            ignition_max_wait_seconds: 180,
            ignition_limit_offset_atr: 0.05,
            ignition_entry_timeout_seconds: 30,
            ignition_stop_atr_multiple: 1.0,
            ignition_target_r: 1.25,
            ignition_max_hold_minutes: 10,
            ignition_risk_per_trade_pct: 0.0025,
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
            || !(5..=60).contains(&self.universe.refresh_seconds)
        {
            return Err("unified universe parameters are invalid".into());
        }
        let lanes = &self.lanes;
        if !(1..=5).contains(&lanes.max_candidates_per_lane)
            || !(48..=480).contains(&lanes.sfp_lookback_hours)
            || !(0.01..=1.0).contains(&lanes.sfp_min_sweep_atr)
            || lanes.sfp_max_sweep_atr <= lanes.sfp_min_sweep_atr
            || lanes.sfp_max_sweep_atr > 3.0
            || !(0.5..=5.0).contains(&lanes.sfp_min_volume_ratio)
            || !(1..=6).contains(&lanes.sfp_confirmation_hours)
            || !(60..=900).contains(&lanes.sfp_max_signal_age_seconds)
            || !(0.5..=3.0).contains(&lanes.sfp_target_r)
            || !(30..=720).contains(&lanes.sfp_max_hold_minutes)
            || !(0.001..=0.005).contains(&lanes.sfp_risk_per_trade_pct)
            || !(0.02..=0.20).contains(&lanes.trend_min_return_4h)
            || !(0.10..=0.90).contains(&lanes.trend_min_efficiency)
            || lanes.trend_min_hour_volume_ratio <= 0.0
            || !(-0.50..=0.50).contains(&lanes.trend_min_flow_imbalance)
            || !(1..=8).contains(&lanes.trend_max_age_bars)
            || !(0.0..=0.5).contains(&lanes.trend_limit_offset_atr)
            || !(5..=120).contains(&lanes.trend_entry_timeout_seconds)
            || !(0.0..=12.0).contains(&lanes.trend_max_entry_adverse_bps)
            || !(0.0..=50.0).contains(&lanes.trend_taker_fallback_max_adverse_bps)
            || !(0.01..=0.25).contains(&lanes.trend_taker_fallback_size_multiplier)
            || !(0.0..=100.0).contains(&lanes.trend_max_post_signal_extension_bps)
            || !(0.0..=0.80).contains(&lanes.trend_max_opposing_micro_flow)
            || !(0.0..=25.0).contains(&lanes.trend_max_opposing_micro_return_bps)
            || lanes.max_spread_bps <= 0.0
            || lanes.min_depth_usd <= 0.0
            || !(0.003..=0.03).contains(&lanes.ignition_min_return_5m)
            || !(1.0..=10.0).contains(&lanes.ignition_min_volume_ratio)
            || !(0.0..=0.8).contains(&lanes.ignition_min_flow_imbalance)
            || !(0.0..=0.8).contains(&lanes.ignition_reclaim_flow_imbalance)
            || !(0.01..=0.10).contains(&lanes.ignition_max_extension_30m)
            || !(60..=300).contains(&lanes.ignition_max_wait_seconds)
            || !(0.0..=0.5).contains(&lanes.ignition_limit_offset_atr)
            || !(5..=120).contains(&lanes.ignition_entry_timeout_seconds)
            || !(0.5..=3.0).contains(&lanes.ignition_stop_atr_multiple)
            || !(0.5..=3.0).contains(&lanes.ignition_target_r)
            || !(3..=30).contains(&lanes.ignition_max_hold_minutes)
            || !(0.001..=0.005).contains(&lanes.ignition_risk_per_trade_pct)
        {
            return Err("alpha lane parameters are invalid".into());
        }
        let risk = &self.risk;
        if !(0.001..=0.01).contains(&risk.risk_per_trade_pct)
            || risk.high_confidence_risk_per_trade_pct < risk.risk_per_trade_pct
            || risk.high_confidence_risk_per_trade_pct > 0.015
            || !(0.20..=1.5).contains(&risk.max_notional_per_trade_multiple)
            || !(0.5..=4.0).contains(&risk.max_total_gross_multiple)
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
