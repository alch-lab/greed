use crate::{
    primitives::{
        cross_section::{MarketBreadthNode, UniverseEligibilityNode},
        derivatives::LeverageRegimeNode,
        external::{CoinbasePremiumNode, EtfFlowNode},
        flow::SpotPerpCvdNode,
        liquidity::{LiquidityRegimeNode, OrderWallNode},
        market_profile::VolumeProfileNode,
        price::TrendRegimeNode,
    },
    recipes::{
        alt_cross_section::AltCrossSectionNode, alt_early_impulse::AltEarlyImpulseNode,
        alt_outlier_momentum::AltOutlierMomentumNode, alt_shock_reversal::AltShockReversalNode,
        major_exhaustion::MajorExhaustionNode, major_trend_pullback::MajorTrendPullbackNode,
    },
    risk::PositionPlannerNode,
    StrategyConfig,
};
use greed_kernel::{GraphError, StrategyGraph, StrategyNode};

pub fn build_graph(config: &StrategyConfig) -> Result<StrategyGraph, GraphError> {
    let mut nodes: Vec<Box<dyn StrategyNode>> = Vec::new();
    let p = &config.primitives;
    let r = &config.recipes;
    for symbol in &config.majors {
        nodes.push(Box::new(TrendRegimeNode::new(
            symbol,
            p.trend_horizon_bars,
            p.trend_min_return_pct,
            p.trend_min_efficiency,
        )));
        nodes.push(Box::new(SpotPerpCvdNode::new(
            symbol,
            p.flow_window_bars,
            p.min_spot_delta_share,
            p.min_spot_directional_share,
        )));
        nodes.push(Box::new(LeverageRegimeNode::new(symbol)));
        nodes.push(Box::new(LiquidityRegimeNode::new(
            symbol,
            p.max_spread_bps,
            p.min_depth_usd,
        )));
        nodes.push(Box::new(CoinbasePremiumNode::new(symbol)));
        nodes.push(Box::new(VolumeProfileNode::new(
            symbol,
            p.volume_profile_bars,
        )));
        nodes.push(Box::new(OrderWallNode::new(
            symbol,
            p.wall_min_notional_usd,
            p.wall_min_polls,
        )));
        if symbol.starts_with("BTC") {
            nodes.push(Box::new(EtfFlowNode::new(symbol)));
        }
        if r.major_trend_pullback_enabled {
            nodes.push(Box::new(MajorTrendPullbackNode::new(
                symbol,
                r.pullback_min_pct,
                r.require_coinbase_premium,
            )));
        }
        if r.major_exhaustion_enabled {
            nodes.push(Box::new(MajorExhaustionNode::new(
                symbol,
                r.exhaustion_move_pct,
            )));
        }
    }
    let mut recipe_ids = Vec::new();
    for symbol in &config.majors {
        if r.major_trend_pullback_enabled {
            recipe_ids.push(format!("{symbol}.recipe.trend_pullback"));
        }
        if r.major_exhaustion_enabled {
            recipe_ids.push(format!("{symbol}.recipe.exhaustion"));
        }
    }
    if !config.altcoins.is_empty() {
        let anchor = config
            .majors
            .first()
            .expect("validated strategy has at least one major");
        for symbol in &config.altcoins {
            nodes.push(Box::new(UniverseEligibilityNode::new(
                symbol,
                p.alt_min_24h_volume_usd,
                p.alt_min_open_interest_usd,
            )));
        }
        nodes.push(Box::new(MarketBreadthNode::new(
            p.breadth_horizon_bars,
            p.breadth_threshold,
            p.breadth_min_participation,
            &config.altcoins,
        )));
        if r.alt_cross_section_enabled {
            nodes.push(Box::new(AltCrossSectionNode::new(
                r.cross_names,
                p.breadth_horizon_bars,
                r.cross_rebalance_bars,
                r.cross_opportunity_driven,
                r.alt_neutral_anchor_allowed,
                anchor,
                &config.altcoins,
            )));
            recipe_ids.push("alt.recipe.cross_section".into());
        }
        if r.alt_outlier_momentum_enabled {
            nodes.push(Box::new(AltOutlierMomentumNode::new(
                r.outlier_names,
                r.outlier_min_return_1h_pct,
                r.outlier_min_return_4h_pct,
                r.outlier_min_volume_ratio,
                r.outlier_min_efficiency,
                r.outlier_confirmation_min_5m_pct,
                r.outlier_confirmation_max_5m_pct,
                r.outlier_max_directional_wick_ratio,
                r.outlier_max_climax_range_ratio,
                r.outlier_pullback_min_pct,
                r.outlier_pullback_max_pct,
                r.outlier_candidate_expiry_minutes,
                r.outlier_short_threshold_multiplier,
                anchor,
                &config.altcoins,
            )));
            recipe_ids.push("alt.recipe.outlier_momentum".into());
        }
        if r.alt_early_impulse_enabled {
            nodes.push(Box::new(AltEarlyImpulseNode::new(
                r.impulse_names,
                r.impulse_min_15m_pct,
                r.impulse_max_15m_pct,
                r.impulse_min_volume_ratio,
                r.impulse_max_1h_pct,
                r.impulse_min_return_z,
                r.impulse_pullback_min_fraction,
                r.impulse_pullback_max_fraction,
                r.impulse_short_threshold_multiplier,
                r.outlier_max_directional_wick_ratio,
                r.outlier_max_climax_range_ratio,
                anchor,
                &config.altcoins,
            )));
            recipe_ids.push("alt.recipe.early_impulse".into());
        }
        if r.alt_shock_reversal_enabled {
            nodes.push(Box::new(AltShockReversalNode::new(
                r.shock_min_return_pct,
                r.shock_min_reversal_pct,
                r.shock_min_volume_ratio,
                r.alt_shock_neutral_anchor_allowed,
                anchor,
                &config.altcoins,
            )));
            recipe_ids.push("alt.recipe.shock_reversal".into());
        }
    }
    nodes.push(Box::new(PositionPlannerNode::new(
        recipe_ids,
        config.risk.clone(),
    )));
    StrategyGraph::build(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_graph_builds() {
        let config = StrategyConfig::default();
        config.validate().unwrap();
        build_graph(&config).unwrap();
    }
}
