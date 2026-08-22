use super::meta;
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, Side, StateArtifact, StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct LeverageRegimeNode {
    id: String,
    symbol: String,
    dependencies: Vec<String>,
}

impl LeverageRegimeNode {
    pub fn new(symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        let trend_dependency = format!("{symbol}.trend_regime");
        Self {
            id: format!("{symbol}.leverage_regime"),
            symbol,
            dependencies: vec![trend_dependency.clone()],
        }
    }
}

impl StrategyNode for LeverageRegimeNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let trend = ctx
            .artifact(&format!("{}.trend", self.symbol))
            .and_then(|a| a.state());
        let instrument = ctx
            .frame
            .instrument(&self.symbol)
            .ok_or_else(|| format!("missing instrument {}", self.symbol))?;
        let derivatives = instrument.derivatives.as_ref();
        let oi_change = derivatives.and_then(|d| d.open_interest_change_pct);
        let price_side = trend.and_then(|state| state.side);
        let (state, side, verdict) = match (price_side, oi_change) {
            (Some(Side::Buy), Some(v)) if v > 0.0 => ("new_longs", Some(Side::Buy), Verdict::Pass),
            (Some(Side::Buy), Some(_)) => ("short_cover", Some(Side::Buy), Verdict::Block),
            (Some(Side::Sell), Some(v)) if v > 0.0 => {
                ("new_shorts", Some(Side::Sell), Verdict::Pass)
            }
            (Some(Side::Sell), Some(_)) => ("long_liquidation", Some(Side::Sell), Verdict::Block),
            (_, Some(_)) => ("neutral", None, Verdict::Block),
            _ => ("unavailable", None, Verdict::Unknown),
        };
        let mut metrics = BTreeMap::new();
        if let Some(v) = oi_change {
            metrics.insert("oi_change_pct".into(), v);
        }
        if let Some(v) = derivatives.and_then(|d| d.funding_rate) {
            metrics.insert("funding_rate".into(), v);
        }
        Ok(vec![ArtifactRecord {
            key: format!("{}.leverage", self.symbol),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: oi_change.unwrap_or(0.0).abs(),
                side,
                verdict,
                reasons: if verdict == Verdict::Pass {
                    vec![]
                } else {
                    vec![if verdict == Verdict::Unknown {
                        "OI observation unavailable"
                    } else {
                        "price move is not supported by newly established OI"
                    }
                    .into()]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    120_000,
                    derivatives
                        .map(|d| d.meta.quality)
                        .unwrap_or(DataQuality::Missing),
                    if oi_change.is_some() { 0.8 } else { 0.0 },
                    vec![
                        format!("{}.open_interest", self.symbol),
                        format!("{}.trend", self.symbol),
                    ],
                ),
            }),
        }])
    }
}
