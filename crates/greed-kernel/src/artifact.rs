use crate::{DataQuality, Side};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Pass,
    Block,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub as_of_ms: i64,
    pub expires_ms: i64,
    pub quality: DataQuality,
    pub confidence: f64,
    pub lineage: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureArtifact {
    pub value: f64,
    pub unit: String,
    pub side: Option<Side>,
    pub meta: ArtifactMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateArtifact {
    pub state: String,
    pub score: f64,
    pub side: Option<Side>,
    pub verdict: Verdict,
    pub reasons: Vec<String>,
    pub metrics: BTreeMap<String, f64>,
    pub meta: ArtifactMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeCandidate {
    pub id: String,
    pub recipe: String,
    pub symbol: String,
    pub side: Side,
    pub signal_ms: i64,
    pub expires_ms: i64,
    pub reference_price: f64,
    pub score: f64,
    pub confidence: f64,
    pub verdict: Verdict,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionPlan {
    pub candidate_id: String,
    pub symbol: String,
    pub side: Side,
    pub reference_price: f64,
    pub notional_usd: f64,
    pub entry_limit: Option<f64>,
    pub stop_price: f64,
    pub take_profit_prices: Vec<(f64, f64)>,
    pub trailing_activation_pct: Option<f64>,
    pub trailing_distance_pct: Option<f64>,
    pub max_hold_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Artifact {
    Feature(FeatureArtifact),
    State(StateArtifact),
    Candidate(TradeCandidate),
    PositionPlan(PositionPlan),
}

impl Artifact {
    pub fn feature(&self) -> Option<&FeatureArtifact> {
        match self {
            Self::Feature(value) => Some(value),
            _ => None,
        }
    }

    pub fn state(&self) -> Option<&StateArtifact> {
        match self {
            Self::State(value) => Some(value),
            _ => None,
        }
    }

    pub fn candidate(&self) -> Option<&TradeCandidate> {
        match self {
            Self::Candidate(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub key: String,
    pub producer: String,
    pub artifact: Artifact,
}
