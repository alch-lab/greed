use crate::{
    recipes::{
        cross_venue_crowding::CrossVenueCrowdingNode, liquidation_impulse::LiquidationImpulseNode,
        trend_continuation::TrendContinuationNode,
    },
    risk::PositionPlannerNode,
    StrategyConfig,
};
use greed_kernel::{GraphError, StrategyGraph, StrategyNode};

pub fn build_graph(config: &StrategyConfig) -> Result<StrategyGraph, GraphError> {
    let mut nodes: Vec<Box<dyn StrategyNode>> = Vec::new();
    let mut lanes = Vec::new();
    if config.lanes.trend_continuation_enabled {
        nodes.push(Box::new(TrendContinuationNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.trend_continuation".into());
    }
    if config.lanes.liquidation_impulse_enabled {
        nodes.push(Box::new(LiquidationImpulseNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.liquidation_impulse".into());
    }
    if config.lanes.cross_venue_crowding_enabled {
        nodes.push(Box::new(CrossVenueCrowdingNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.cross_venue_crowding".into());
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
