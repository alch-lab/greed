use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StrategyConfig {
    pub majors: Vec<String>,
    pub altcoins: Vec<String>,
    pub primitives: PrimitiveConfig,
    pub recipes: RecipeConfig,
    pub risk: RiskConfig,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            majors: vec!["BTCUSDT".into(), "ETHUSDT".into()],
            altcoins: vec![],
            primitives: PrimitiveConfig::default(),
            recipes: RecipeConfig::default(),
            risk: RiskConfig::default(),
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
    pub alt_shock_reversal_enabled: bool,
    pub pullback_min_pct: f64,
    pub exhaustion_move_pct: f64,
    pub cross_names: usize,
    pub cross_rebalance_bars: usize,
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
            alt_shock_reversal_enabled: true,
            pullback_min_pct: 0.002,
            exhaustion_move_pct: 0.018,
            cross_names: 3,
            cross_rebalance_bars: 24,
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
    pub major_max_gross: f64,
    pub alt_max_gross: f64,
    pub max_total_gross: f64,
    pub initial_stop_pct: f64,
    pub first_take_profit_pct: f64,
    pub alt_cross_stop_pct: f64,
    pub alt_cross_take_profit_pct: f64,
    pub alt_cross_max_hold_minutes: u32,
    pub first_take_profit_fraction: f64,
    pub trailing_activation_pct: f64,
    pub trailing_distance_pct: f64,
    pub max_hold_minutes: u32,
    pub daily_loss_limit_pct: f64,
    pub peak_drawdown_halt_pct: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            major_gross_per_trade: 0.20,
            alt_gross_per_trade: 0.10,
            major_max_gross: 0.50,
            alt_max_gross: 0.50,
            max_total_gross: 1.0,
            initial_stop_pct: 0.008,
            first_take_profit_pct: 0.012,
            alt_cross_stop_pct: 0.015,
            alt_cross_take_profit_pct: 0.025,
            alt_cross_max_hold_minutes: 720,
            first_take_profit_fraction: 0.50,
            trailing_activation_pct: 0.012,
            trailing_distance_pct: 0.006,
            max_hold_minutes: 240,
            daily_loss_limit_pct: 0.025,
            peak_drawdown_halt_pct: 0.05,
        }
    }
}

impl StrategyConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.majors.is_empty() {
            return Err("at least one major symbol is required".into());
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
                "paper gross caps may not exceed 100% total or 50% per capital bucket".into(),
            );
        }
        if self.risk.major_gross_per_trade > self.risk.major_max_gross
            || self.risk.alt_gross_per_trade > self.risk.alt_max_gross
        {
            return Err("per-trade gross may not exceed its capital bucket".into());
        }
        Ok(())
    }
}
