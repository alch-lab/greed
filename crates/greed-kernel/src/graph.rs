use crate::{Artifact, ArtifactRecord, MarketFrame};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("duplicate node id: {0}")]
    DuplicateNode(String),
    #[error("node {node} depends on missing node {dependency}")]
    MissingDependency { node: String, dependency: String },
    #[error("strategy graph contains a cycle")]
    Cycle,
    #[error("node {node} failed: {message}")]
    Node { node: String, message: String },
}

pub struct NodeContext<'a> {
    pub frame: &'a MarketFrame,
    pub artifacts: &'a BTreeMap<String, ArtifactRecord>,
}

impl NodeContext<'_> {
    pub fn artifact(&self, key: &str) -> Option<&Artifact> {
        self.artifacts.get(key).map(|record| &record.artifact)
    }
}

pub trait StrategyNode: Send {
    fn id(&self) -> &str;
    fn dependencies(&self) -> &[String];
    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String>;
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct GraphEvaluation {
    pub artifacts: BTreeMap<String, ArtifactRecord>,
    pub node_order: Vec<String>,
}

pub struct StrategyGraph {
    nodes: BTreeMap<String, Box<dyn StrategyNode>>,
    order: Vec<String>,
}

impl StrategyGraph {
    pub fn build(nodes: Vec<Box<dyn StrategyNode>>) -> Result<Self, GraphError> {
        let mut by_id = BTreeMap::new();
        for node in nodes {
            let id = node.id().to_owned();
            if by_id.insert(id.clone(), node).is_some() {
                return Err(GraphError::DuplicateNode(id));
            }
        }
        for (id, node) in &by_id {
            for dependency in node.dependencies() {
                if !by_id.contains_key(dependency) {
                    return Err(GraphError::MissingDependency {
                        node: id.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
        let mut complete = BTreeSet::new();
        let mut order = Vec::with_capacity(by_id.len());
        while order.len() < by_id.len() {
            let before = order.len();
            for (id, node) in &by_id {
                if complete.contains(id)
                    || !node
                        .dependencies()
                        .iter()
                        .all(|dependency| complete.contains(dependency))
                {
                    continue;
                }
                complete.insert(id.clone());
                order.push(id.clone());
            }
            if order.len() == before {
                return Err(GraphError::Cycle);
            }
        }
        Ok(Self {
            nodes: by_id,
            order,
        })
    }

    pub fn evaluate(&mut self, frame: &MarketFrame) -> Result<GraphEvaluation, GraphError> {
        let mut artifacts = BTreeMap::new();
        for id in self.order.clone() {
            let ctx = NodeContext {
                frame,
                artifacts: &artifacts,
            };
            let records = self
                .nodes
                .get_mut(&id)
                .expect("validated graph order")
                .evaluate(&ctx)
                .map_err(|message| GraphError::Node {
                    node: id.clone(),
                    message,
                })?;
            for record in records {
                artifacts.insert(record.key.clone(), record);
            }
        }
        Ok(GraphEvaluation {
            artifacts,
            node_order: self.order.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountFrame, ArtifactMeta, DataQuality, FeatureArtifact};

    struct ConstantNode {
        id: String,
        dependencies: Vec<String>,
    }

    impl StrategyNode for ConstantNode {
        fn id(&self) -> &str {
            &self.id
        }
        fn dependencies(&self) -> &[String] {
            &self.dependencies
        }
        fn evaluate(&mut self, _ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
            Ok(vec![ArtifactRecord {
                key: self.id.clone(),
                producer: self.id.clone(),
                artifact: Artifact::Feature(FeatureArtifact {
                    value: 1.0,
                    unit: "ratio".into(),
                    side: None,
                    meta: ArtifactMeta {
                        as_of_ms: 1,
                        expires_ms: 2,
                        quality: DataQuality::Complete,
                        confidence: 1.0,
                        lineage: vec![],
                    },
                }),
            }])
        }
    }

    fn frame() -> MarketFrame {
        MarketFrame {
            as_of_ms: 1,
            instruments: Default::default(),
            account: AccountFrame {
                equity_usd: 3_000.0,
                cash_usd: 3_000.0,
                realized_pnl_usd: 0.0,
                peak_equity_usd: 3_000.0,
                risk_day_start_equity_usd: 3_000.0,
                gross_exposure_usd: 0.0,
                open_positions: 0,
            },
        }
    }

    #[test]
    fn graph_topologically_orders_nodes() {
        let mut graph = StrategyGraph::build(vec![
            Box::new(ConstantNode {
                id: "recipe".into(),
                dependencies: vec!["feature".into()],
            }),
            Box::new(ConstantNode {
                id: "feature".into(),
                dependencies: vec![],
            }),
        ])
        .unwrap();
        let result = graph.evaluate(&frame()).unwrap();
        assert_eq!(result.node_order, vec!["feature", "recipe"]);
    }

    #[test]
    fn graph_rejects_cycles() {
        let result = StrategyGraph::build(vec![
            Box::new(ConstantNode {
                id: "a".into(),
                dependencies: vec!["b".into()],
            }),
            Box::new(ConstantNode {
                id: "b".into(),
                dependencies: vec!["a".into()],
            }),
        ]);
        assert!(matches!(result, Err(GraphError::Cycle)));
    }
}
