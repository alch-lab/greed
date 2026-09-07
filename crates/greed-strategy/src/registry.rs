use crate::{
    recipes::{
        fast_trend_activation::FastTrendActivationNode,
        liquidation_exhaustion_reversal::LiquidationExhaustionReversalNode,
        trend_continuation::TrendContinuationNode,
    },
    risk::PositionPlannerNode,
    StrategyConfig,
};
use greed_kernel::{GraphError, StrategyGraph, StrategyNode};

pub fn build_graph(config: &StrategyConfig) -> Result<StrategyGraph, GraphError> {
    let mut lanes = vec!["lane.trend_continuation".into()];
    let mut nodes: Vec<Box<dyn StrategyNode>> = vec![Box::new(TrendContinuationNode::new(
        &config.symbols,
        config.lanes.clone(),
    ))];
    if config.lanes.fast_activation_enabled {
        nodes.push(Box::new(FastTrendActivationNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.fast_trend_activation".into());
    }
    if config.lanes.liquidation_reversal_enabled {
        nodes.push(Box::new(LiquidationExhaustionReversalNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.liquidation_exhaustion_reversal".into());
    }
    nodes.push(Box::new(PositionPlannerNode::new(
        lanes,
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
