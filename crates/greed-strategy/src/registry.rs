use crate::{
    recipes::trend_continuation::TrendContinuationNode, risk::PositionPlannerNode, StrategyConfig,
};
use greed_kernel::{GraphError, StrategyGraph, StrategyNode};

pub fn build_graph(config: &StrategyConfig) -> Result<StrategyGraph, GraphError> {
    let lanes = vec!["lane.trend_continuation".into()];
    let mut nodes: Vec<Box<dyn StrategyNode>> = vec![Box::new(TrendContinuationNode::new(
        &config.symbols,
        config.lanes.clone(),
    ))];
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
