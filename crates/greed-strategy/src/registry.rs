use crate::{
    recipes::{
        intraday_sweep_reversal::IntradaySweepReversalNode, sfp_reversal::SfpReversalNode,
        trend_continuation::TrendContinuationNode,
    },
    risk::PositionPlannerNode,
    StrategyConfig,
};
use greed_kernel::{GraphError, StrategyGraph, StrategyNode};

pub fn build_graph(config: &StrategyConfig) -> Result<StrategyGraph, GraphError> {
    let mut nodes: Vec<Box<dyn StrategyNode>> = Vec::new();
    let mut lanes = Vec::new();
    if config.lanes.sfp_reversal_enabled {
        nodes.push(Box::new(SfpReversalNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.sfp_reversal".into());
    }
    if config.lanes.trend_continuation_enabled {
        nodes.push(Box::new(TrendContinuationNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.trend_continuation".into());
    }
    if config.lanes.intraday_sweep_reversal_enabled {
        nodes.push(Box::new(IntradaySweepReversalNode::new(
            &config.symbols,
            config.lanes.clone(),
        )));
        lanes.push("lane.intraday_sweep_reversal".into());
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
