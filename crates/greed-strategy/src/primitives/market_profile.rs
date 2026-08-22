use super::{closed_bars, meta};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, FeatureArtifact, NodeContext, Side, StateArtifact,
    StrategyNode, Verdict,
};
use std::collections::BTreeMap;

/// Candle-volume approximation used until trade-level profile replay is wired.
/// Partial quality prevents recipes from confusing it with a true footprint POC.
pub struct VolumeProfileNode {
    id: String,
    symbol: String,
    bars: usize,
    dependencies: Vec<String>,
}

impl VolumeProfileNode {
    pub fn new(symbol: impl Into<String>, bars: usize) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.volume_profile"),
            symbol,
            bars,
            dependencies: vec![],
        }
    }
}

impl StrategyNode for VolumeProfileNode {
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
        if bars.len() < self.bars {
            return Ok(vec![]);
        }
        let source = &bars[bars.len() - self.bars..];
        let low = source
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min);
        let high = source
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);
        if high <= low {
            return Ok(vec![]);
        }
        const BINS: usize = 64;
        let width = (high - low) / BINS as f64;
        let mut volume = [0.0; BINS];
        for bar in source {
            let typical = (bar.high + bar.low + bar.close) / 3.0;
            let index = ((typical - low) / width)
                .floor()
                .clamp(0.0, (BINS - 1) as f64) as usize;
            volume[index] += bar.quote_volume;
        }
        let poc_index = volume
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.total_cmp(b.1))
            .map(|(index, _)| index)
            .unwrap_or(0);
        let poc = low + (poc_index as f64 + 0.5) * width;
        let side = if instrument.price > poc {
            Some(Side::Buy)
        } else if instrument.price < poc {
            Some(Side::Sell)
        } else {
            None
        };
        let mut metrics = BTreeMap::new();
        metrics.insert("poc_price".into(), poc);
        metrics.insert("distance_from_poc_pct".into(), instrument.price / poc - 1.0);
        Ok(vec![
            ArtifactRecord {
                key: format!("{}.poc", self.symbol),
                producer: self.id.clone(),
                artifact: Artifact::Feature(FeatureArtifact {
                    value: poc,
                    unit: "price".into(),
                    side,
                    meta: meta(
                        ctx.frame.as_of_ms,
                        15 * 60_000,
                        DataQuality::Partial,
                        0.5,
                        vec![
                            format!("{}.perpetual.candles", self.symbol),
                            "candle_volume_profile_approximation".into(),
                        ],
                    ),
                }),
            },
            ArtifactRecord {
                key: format!("{}.value_location", self.symbol),
                producer: self.id.clone(),
                artifact: Artifact::State(StateArtifact {
                    state: if side == Some(Side::Buy) {
                        "above_poc"
                    } else if side == Some(Side::Sell) {
                        "below_poc"
                    } else {
                        "at_poc"
                    }
                    .into(),
                    score: (instrument.price / poc - 1.0).abs(),
                    side,
                    verdict: Verdict::Pass,
                    reasons: vec!["POC is a candle-volume approximation".into()],
                    metrics,
                    meta: meta(
                        ctx.frame.as_of_ms,
                        15 * 60_000,
                        DataQuality::Partial,
                        0.5,
                        vec![format!("{}.poc", self.symbol)],
                    ),
                }),
            },
        ])
    }
}
