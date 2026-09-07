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
    /// Immutable signal-time measurements copied from the candidate. These
    /// are journaled with the eventual entry so later research can separate
    /// market regimes without reconstructing a historical live frame.
    #[serde(default)]
    pub signal_context: BTreeMap<String, String>,
    pub symbol: String,
    pub side: Side,
    pub reference_price: f64,
    pub notional_usd: f64,
    pub entry_limit: Option<f64>,
    pub entry_timeout_ms: i64,
    #[serde(default)]
    pub taker_fallback: bool,
    #[serde(default)]
    pub max_entry_adverse_bps: f64,
    #[serde(default)]
    pub taker_fallback_max_adverse_bps: f64,
    #[serde(default = "default_entry_size_multiplier")]
    pub taker_fallback_size_multiplier: f64,
    /// Fill fraction at which the executor stops waiting for the remaining
    /// maker quantity and promotes the fill to a managed position.
    #[serde(default)]
    pub min_fill_ratio: f64,
    /// Minimum fill fraction that is worth managing if the entry deadline or
    /// signal guard ends before `min_fill_ratio` is reached. Exchange quantity,
    /// step and notional rules are checked independently.
    #[serde(default)]
    pub min_managed_fill_ratio: f64,
    /// Cancel a resting entry when the strategy-market midpoint overshoots the
    /// intended pullback limit by more than this many basis points.
    #[serde(default)]
    pub entry_invalidation_bps: f64,
    /// While a trend maker order rests, the same live-flow veto used to build
    /// the candidate must remain valid. Zero disables the runtime guard.
    #[serde(default)]
    pub entry_guard_max_opposing_flow: f64,
    #[serde(default)]
    pub entry_guard_max_opposing_return_bps: f64,
    pub stop_price: f64,
    pub take_profit_prices: Vec<(f64, f64)>,
    /// Fraction of the original position intentionally left without a fixed
    /// take-profit after the preceding staged exits complete. The executor
    /// keeps the hard stop until this runner is actually reached.
    #[serde(default)]
    pub unprotected_runner_fraction: Option<f64>,
    pub break_even_after_fraction: Option<f64>,
    pub break_even_buffer_pct: f64,
    #[serde(default)]
    pub profit_shield_activation_pct: Option<f64>,
    pub trailing_activation_pct: Option<f64>,
    pub trailing_distance_pct: Option<f64>,
    #[serde(default)]
    pub early_failure_after_ms: i64,
    #[serde(default)]
    pub early_failure_adverse_pct: f64,
    #[serde(default)]
    pub early_failure_max_favorable_pct: f64,
    pub max_hold_ms: i64,
    /// A fixed-horizon research position must close at `max_hold_ms` without
    /// inheriting recipe-specific grace periods or adaptive hold extensions.
    #[serde(default)]
    pub fixed_time_exit: bool,
}

/// A strategy-level request to flatten its positions when the market state
/// that justified the entry has disappeared.  Keeping this in the graph makes
/// replay and live execution use the same exit decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionExitIntent {
    pub recipe: String,
    pub side: Side,
    pub reason: String,
}

fn default_entry_size_multiplier() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Artifact {
    Feature(FeatureArtifact),
    State(StateArtifact),
    Candidate(TradeCandidate),
    PositionPlan(PositionPlan),
    PositionExitIntent(PositionExitIntent),
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
