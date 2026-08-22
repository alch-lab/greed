use super::{closed_bars, meta, path_efficiency};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, FeatureArtifact, NodeContext, Side, StateArtifact,
    StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct TrendRegimeNode {
    id: String,
    symbol: String,
    horizon_bars: usize,
    min_return: f64,
    min_efficiency: f64,
    dependencies: Vec<String>,
}

impl TrendRegimeNode {
    pub fn new(
        symbol: impl Into<String>,
        horizon_bars: usize,
        min_return: f64,
        min_efficiency: f64,
    ) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.trend_regime"),
            symbol,
            horizon_bars,
            min_return,
            min_efficiency,
            dependencies: vec![],
        }
    }
}

impl StrategyNode for TrendRegimeNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let instrument = ctx
            .frame
            .instrument(&self.symbol)
            .ok_or_else(|| format!("missing instrument {}", self.symbol))?;
        let bars = closed_bars(&instrument.perpetual);
        let producer = self.id.clone();
        if bars.len() <= self.horizon_bars {
            return Ok(vec![ArtifactRecord {
                key: format!("{}.trend", self.symbol),
                producer,
                artifact: Artifact::State(StateArtifact {
                    state: "insufficient_history".into(),
                    score: 0.0,
                    side: None,
                    verdict: Verdict::Unknown,
                    reasons: vec!["closed candle history is incomplete".into()],
                    metrics: BTreeMap::new(),
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        DataQuality::Missing,
                        0.0,
                        vec![format!("{}.perpetual.candles", self.symbol)],
                    ),
                }),
            }]);
        }
        let window = &bars[bars.len() - self.horizon_bars - 1..];
        let return_pct = window.last().unwrap().close / window.first().unwrap().close - 1.0;
        let efficiency = path_efficiency(window);
        let side = (return_pct.abs() >= self.min_return && efficiency >= self.min_efficiency)
            .then_some(if return_pct > 0.0 {
                Side::Buy
            } else {
                Side::Sell
            });
        let mut metrics = BTreeMap::new();
        metrics.insert("return_pct".into(), return_pct);
        metrics.insert("efficiency".into(), efficiency);
        let state = match side {
            Some(Side::Buy) => "trend_up",
            Some(Side::Sell) => "trend_down",
            None => "range",
        };
        Ok(vec![
            ArtifactRecord {
                key: format!("{}.trend_return", self.symbol),
                producer: producer.clone(),
                artifact: Artifact::Feature(FeatureArtifact {
                    value: return_pct,
                    unit: "ratio".into(),
                    side,
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        instrument.perpetual.meta.quality,
                        efficiency,
                        vec![format!("{}.perpetual.candles", self.symbol)],
                    ),
                }),
            },
            ArtifactRecord {
                key: format!("{}.trend", self.symbol),
                producer,
                artifact: Artifact::State(StateArtifact {
                    state: state.into(),
                    score: return_pct.abs() * efficiency,
                    side,
                    verdict: if side.is_some() {
                        Verdict::Pass
                    } else {
                        Verdict::Block
                    },
                    reasons: if side.is_some() {
                        vec![]
                    } else {
                        vec!["trend strength or path efficiency below threshold".into()]
                    },
                    metrics,
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        instrument.perpetual.meta.quality,
                        efficiency,
                        vec![format!("{}.perpetual.candles", self.symbol)],
                    ),
                }),
            },
        ])
    }
}
