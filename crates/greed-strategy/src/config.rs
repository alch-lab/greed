use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategyConfig {
    pub majors: Vec<String>,
    pub altcoins: Vec<String>,
    pub universe: UniverseConfig,
    pub primitives: PrimitiveConfig,
    pub recipes: RecipeConfig,
    pub risk: RiskConfig,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            majors: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            altcoins: vec![],
            universe: UniverseConfig::default(),
            primitives: PrimitiveConfig::default(),
            recipes: RecipeConfig::default(),
            risk: RiskConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UniverseConfig {
    pub dynamic_enabled: bool,
    pub max_altcoins: usize,
    pub top_liquidity_names: usize,
    pub top_mover_names: usize,
    pub min_24h_quote_volume_usd: f64,
    pub refresh_minutes: u32,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            dynamic_enabled: false,
            max_altcoins: 30,
            top_liquidity_names: 12,
            top_mover_names: 18,
            min_24h_quote_volume_usd: 25_000_000.0,
            refresh_minutes: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrimitiveConfig {
    pub trend_horizon_bars: usize,
    pub trend_min_return_pct: f64,
    pub trend_min_efficiency: f64,
    pub flow_window_bars: usize,
    pub min_spot_delta_share: f64,
    pub min_spot_directional_share: f64,
    pub max_spread_bps: f64,
    pub min_depth_usd: f64,
    pub breadth_horizon_bars: usize,
    pub breadth_threshold: f64,
    pub breadth_min_participation: f64,
    pub alt_min_24h_volume_usd: f64,
    pub alt_min_open_interest_usd: f64,
    pub volume_profile_bars: usize,
    pub wall_min_notional_usd: f64,
    pub wall_min_polls: u32,
}

impl Default for PrimitiveConfig {
    fn default() -> Self {
        Self {
            trend_horizon_bars: 24,
            trend_min_return_pct: 0.006,
            trend_min_efficiency: 0.10,
            flow_window_bars: 4,
            min_spot_delta_share: 0.01,
            min_spot_directional_share: 0.15,
            max_spread_bps: 8.0,
            min_depth_usd: 100_000.0,
            breadth_horizon_bars: 24,
            breadth_threshold: 0.01,
            breadth_min_participation: 0.55,
            alt_min_24h_volume_usd: 25_000_000.0,
            alt_min_open_interest_usd: 5_000_000.0,
            volume_profile_bars: 96,
            wall_min_notional_usd: 2_000_000.0,
            wall_min_polls: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RecipeConfig {
    pub major_trend_pullback_enabled: bool,
    pub major_exhaustion_enabled: bool,
    pub require_coinbase_premium: bool,
    pub alt_cross_section_enabled: bool,
    pub alt_outlier_momentum_enabled: bool,
    pub alt_early_impulse_enabled: bool,
    pub alt_shock_reversal_enabled: bool,
    pub pullback_min_pct: f64,
    pub exhaustion_move_pct: f64,
    pub cross_names: usize,
    pub cross_rebalance_bars: usize,
    pub cross_opportunity_driven: bool,
    pub alt_neutral_anchor_allowed: bool,
    pub alt_shock_neutral_anchor_allowed: bool,
    pub outlier_names: usize,
    pub outlier_min_return_1h_pct: f64,
    pub outlier_min_return_4h_pct: f64,
    pub outlier_min_volume_ratio: f64,
    pub outlier_min_efficiency: f64,
    pub outlier_confirmation_min_5m_pct: f64,
    pub outlier_confirmation_max_5m_pct: f64,
    pub outlier_max_directional_wick_ratio: f64,
    pub outlier_max_climax_range_ratio: f64,
    pub outlier_pullback_min_pct: f64,
    pub outlier_pullback_max_pct: f64,
    pub outlier_candidate_expiry_minutes: u32,
    pub impulse_names: usize,
    pub impulse_min_15m_pct: f64,
    pub impulse_max_15m_pct: f64,
    pub impulse_min_volume_ratio: f64,
    pub impulse_max_1h_pct: f64,
    pub shock_min_return_pct: f64,
    pub shock_min_reversal_pct: f64,
    pub shock_min_volume_ratio: f64,
}

impl Default for RecipeConfig {
    fn default() -> Self {
        Self {
            major_trend_pullback_enabled: true,
            major_exhaustion_enabled: true,
            require_coinbase_premium: false,
            alt_cross_section_enabled: true,
            alt_outlier_momentum_enabled: true,
            alt_early_impulse_enabled: true,
            alt_shock_reversal_enabled: true,
            pullback_min_pct: 0.002,
            exhaustion_move_pct: 0.018,
            cross_names: 3,
            cross_rebalance_bars: 24,
            cross_opportunity_driven: true,
            alt_neutral_anchor_allowed: false,
            alt_shock_neutral_anchor_allowed: false,
            outlier_names: 2,
            outlier_min_return_1h_pct: 0.025,
            outlier_min_return_4h_pct: 0.05,
            outlier_min_volume_ratio: 1.50,
            outlier_min_efficiency: 0.45,
            outlier_confirmation_min_5m_pct: 0.001,
            outlier_confirmation_max_5m_pct: 0.012,
            outlier_max_directional_wick_ratio: 0.35,
            outlier_max_climax_range_ratio: 2.8,
            outlier_pullback_min_pct: 0.004,
            outlier_pullback_max_pct: 0.015,
            outlier_candidate_expiry_minutes: 10,
            impulse_names: 2,
            impulse_min_15m_pct: 0.006,
            impulse_max_15m_pct: 0.025,
            impulse_min_volume_ratio: 1.60,
            impulse_max_1h_pct: 0.040,
            shock_min_return_pct: 0.045,
            shock_min_reversal_pct: 0.006,
            shock_min_volume_ratio: 2.5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RiskConfig {
    pub major_gross_per_trade: f64,
    pub alt_gross_per_trade: f64,
    pub alt_neutral_anchor_size_multiplier: f64,
    pub alt_neutral_anchor_stop_pct: f64,
    pub alt_neutral_anchor_take_profit_pct: f64,
    pub alt_neutral_anchor_max_hold_minutes: u32,
    pub alt_outlier_gross_per_trade: f64,
    pub alt_outlier_opposed_size_multiplier: f64,
    pub alt_outlier_neutral_size_multiplier: f64,
    pub alt_outlier_breadth_opposed_size_multiplier: f64,
    pub alt_outlier_breadth_neutral_size_multiplier: f64,
    pub alt_outlier_stop_pct: f64,
    pub alt_outlier_take_profit_pct: f64,
    pub alt_outlier_max_hold_minutes: u32,
    pub alt_intraday_gross_per_trade: f64,
    pub alt_intraday_stop_pct: f64,
    pub alt_intraday_max_hold_minutes: u32,
    pub major_max_gross: f64,
    pub alt_max_gross: f64,
    pub max_total_gross: f64,
    pub initial_stop_pct: f64,
    pub first_take_profit_pct: f64,
    pub alt_cross_stop_pct: f64,
    pub alt_cross_take_profit_pct: f64,
    pub alt_cross_max_hold_minutes: u32,
    pub first_take_profit_fraction: f64,
    pub risk_shield_r_multiple: f64,
    pub risk_shield_fraction: f64,
    pub second_take_profit_r_multiple: f64,
    pub second_take_profit_fraction: f64,
    pub runner_take_profit_r_multiple: f64,
    pub break_even_cost_buffer_pct: f64,
    pub trailing_activation_pct: f64,
    pub trailing_distance_pct: f64,
    pub max_hold_minutes: u32,
    pub daily_loss_limit_pct: f64,
    pub peak_drawdown_halt_pct: f64,
    pub rolling_pf_window: usize,
    pub rolling_pf_min_trades: usize,
    pub rolling_pf_floor: f64,
    pub rolling_pf_cooldown_minutes: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            major_gross_per_trade: 0.20,
            alt_gross_per_trade: 0.10,
            alt_neutral_anchor_size_multiplier: 0.60,
            alt_neutral_anchor_stop_pct: 0.012,
            alt_neutral_anchor_take_profit_pct: 0.018,
            alt_neutral_anchor_max_hold_minutes: 240,
            alt_outlier_gross_per_trade: 0.05,
            alt_outlier_opposed_size_multiplier: 0.50,
            alt_outlier_neutral_size_multiplier: 0.75,
            alt_outlier_breadth_opposed_size_multiplier: 0.35,
            alt_outlier_breadth_neutral_size_multiplier: 0.65,
            alt_outlier_stop_pct: 0.012,
            alt_outlier_take_profit_pct: 0.020,
            alt_outlier_max_hold_minutes: 180,
            alt_intraday_gross_per_trade: 0.08,
            alt_intraday_stop_pct: 0.009,
            alt_intraday_max_hold_minutes: 90,
            major_max_gross: 0.50,
            alt_max_gross: 0.50,
            max_total_gross: 1.0,
            initial_stop_pct: 0.008,
            first_take_profit_pct: 0.012,
            alt_cross_stop_pct: 0.015,
            alt_cross_take_profit_pct: 0.025,
            alt_cross_max_hold_minutes: 720,
            first_take_profit_fraction: 0.50,
            risk_shield_r_multiple: 0.75,
            risk_shield_fraction: 0.25,
            second_take_profit_r_multiple: 1.0,
            second_take_profit_fraction: 0.30,
            runner_take_profit_r_multiple: 2.5,
            break_even_cost_buffer_pct: 0.0015,
            trailing_activation_pct: 0.012,
            trailing_distance_pct: 0.006,
            max_hold_minutes: 240,
            daily_loss_limit_pct: 0.025,
            peak_drawdown_halt_pct: 0.05,
            rolling_pf_window: 10,
            rolling_pf_min_trades: 5,
            rolling_pf_floor: 0.80,
            rolling_pf_cooldown_minutes: 480,
        }
    }
}

impl StrategyConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.majors.is_empty() {
            return Err("at least one major symbol is required".into());
        }
        if self.universe.max_altcoins < 5
            || self.universe.max_altcoins > 50
            || self.universe.top_liquidity_names + self.universe.top_mover_names
                < self.universe.max_altcoins
            || self.universe.min_24h_quote_volume_usd <= 0.0
            || !(1..=60).contains(&self.universe.refresh_minutes)
        {
            return Err("dynamic universe parameters are outside safe API ranges".into());
        }
        if !(4..=96).contains(&self.primitives.trend_horizon_bars) {
            return Err("trend_horizon_bars must be between 4 and 96".into());
        }
        if !(0.50..=0.90).contains(&self.primitives.breadth_min_participation) {
            return Err("breadth_min_participation must be between 0.50 and 0.90".into());
        }
        if !(1..=10).contains(&self.recipes.cross_names) {
            return Err("cross_names must be between 1 and 10".into());
        }
        if !(1..=5).contains(&self.recipes.outlier_names)
            || !(0.005..=0.15).contains(&self.recipes.outlier_min_return_1h_pct)
            || !(0.01..=0.30).contains(&self.recipes.outlier_min_return_4h_pct)
            || !(1.0..=10.0).contains(&self.recipes.outlier_min_volume_ratio)
            || !(0.10..=0.95).contains(&self.recipes.outlier_min_efficiency)
            || !(0.0005..=0.01).contains(&self.recipes.outlier_confirmation_min_5m_pct)
            || !(0.003..=0.03).contains(&self.recipes.outlier_confirmation_max_5m_pct)
            || self.recipes.outlier_confirmation_min_5m_pct
                >= self.recipes.outlier_confirmation_max_5m_pct
            || !(0.10..=0.80).contains(&self.recipes.outlier_max_directional_wick_ratio)
            || !(1.2..=6.0).contains(&self.recipes.outlier_max_climax_range_ratio)
            || self.recipes.outlier_pullback_min_pct <= 0.0
            || self.recipes.outlier_pullback_min_pct >= self.recipes.outlier_pullback_max_pct
            || self.recipes.outlier_pullback_max_pct > 0.05
            || !(3..=30).contains(&self.recipes.outlier_candidate_expiry_minutes)
        {
            return Err("outlier recipe parameters are outside safe demo ranges".into());
        }
        if !(1..=5).contains(&self.recipes.impulse_names)
            || !(0.002..=0.03).contains(&self.recipes.impulse_min_15m_pct)
            || self.recipes.impulse_min_15m_pct >= self.recipes.impulse_max_15m_pct
            || self.recipes.impulse_max_15m_pct > 0.05
            || !(1.0..=5.0).contains(&self.recipes.impulse_min_volume_ratio)
            || self.recipes.impulse_max_1h_pct <= self.recipes.impulse_max_15m_pct
        {
            return Err("early impulse parameters are outside safe demo ranges".into());
        }
        if !(4..=96).contains(&self.recipes.cross_rebalance_bars) {
            return Err("cross_rebalance_bars must be between 4 and 96".into());
        }
        if !(0.0..=0.03).contains(&self.risk.initial_stop_pct) || self.risk.initial_stop_pct == 0.0
        {
            return Err("initial_stop_pct must be in (0, 0.03]".into());
        }
        if self.risk.max_total_gross > 1.0
            || self.risk.major_max_gross > 0.5
            || self.risk.alt_max_gross > 0.5
        {
            return Err(
                "demo gross caps may not exceed 100% total or 50% per capital bucket".into(),
            );
        }
        if self.risk.major_gross_per_trade > self.risk.major_max_gross
            || self.risk.alt_gross_per_trade > self.risk.alt_max_gross
            || self.risk.alt_outlier_gross_per_trade > self.risk.alt_max_gross
            || self.risk.alt_intraday_gross_per_trade > self.risk.alt_max_gross
        {
            return Err("per-trade gross may not exceed its capital bucket".into());
        }
        if !(0.0..=1.0).contains(&self.risk.alt_outlier_opposed_size_multiplier)
            || self.risk.alt_outlier_opposed_size_multiplier == 0.0
            || !(0.0..=1.0).contains(&self.risk.alt_outlier_neutral_size_multiplier)
            || self.risk.alt_outlier_neutral_size_multiplier == 0.0
            || !(0.0..=1.0).contains(&self.risk.alt_outlier_breadth_opposed_size_multiplier)
            || self.risk.alt_outlier_breadth_opposed_size_multiplier == 0.0
            || !(0.0..=1.0).contains(&self.risk.alt_outlier_breadth_neutral_size_multiplier)
            || self.risk.alt_outlier_breadth_neutral_size_multiplier == 0.0
            || !(0.0..=0.03).contains(&self.risk.alt_outlier_stop_pct)
            || self.risk.alt_outlier_stop_pct == 0.0
            || !(0.0..=0.06).contains(&self.risk.alt_outlier_take_profit_pct)
            || self.risk.alt_outlier_take_profit_pct == 0.0
            || self.risk.alt_outlier_max_hold_minutes == 0
        {
            return Err("outlier risk parameters are outside safe demo ranges".into());
        }
        let staged_fraction =
            self.risk.risk_shield_fraction + self.risk.second_take_profit_fraction;
        if !(0.25..1.0).contains(&staged_fraction)
            || !(0.25..=1.5).contains(&self.risk.risk_shield_r_multiple)
            || self.risk.second_take_profit_r_multiple <= self.risk.risk_shield_r_multiple
            || self.risk.runner_take_profit_r_multiple <= self.risk.second_take_profit_r_multiple
            || !(0.0..=0.005).contains(&self.risk.break_even_cost_buffer_pct)
            || self.risk.alt_intraday_stop_pct <= 0.0
            || self.risk.alt_intraday_max_hold_minutes == 0
        {
            return Err("staged exit or intraday risk parameters are invalid".into());
        }
        if !(0.0..=1.0).contains(&self.risk.alt_neutral_anchor_size_multiplier)
            || self.risk.alt_neutral_anchor_size_multiplier == 0.0
        {
            return Err("alt_neutral_anchor_size_multiplier must be in (0, 1]".into());
        }
        if !(0.0..=0.03).contains(&self.risk.alt_neutral_anchor_stop_pct)
            || self.risk.alt_neutral_anchor_stop_pct == 0.0
            || !(0.0..=0.05).contains(&self.risk.alt_neutral_anchor_take_profit_pct)
            || self.risk.alt_neutral_anchor_take_profit_pct == 0.0
            || self.risk.alt_neutral_anchor_max_hold_minutes == 0
        {
            return Err(
                "neutral-anchor stop/take-profit must be positive and hold time must be non-zero"
                    .into(),
            );
        }
        if self.risk.rolling_pf_window < self.risk.rolling_pf_min_trades
            || self.risk.rolling_pf_min_trades < 3
            || !(0.0..=2.0).contains(&self.risk.rolling_pf_floor)
        {
            return Err(
                "rolling PF gate requires window >= min_trades >= 3 and floor in [0, 2]".into(),
            );
        }
        Ok(())
    }
}
