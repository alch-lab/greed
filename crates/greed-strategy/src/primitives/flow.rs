use super::{closed_bars, meta};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, FeatureArtifact, NodeContext, Side, StateArtifact,
    StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct SpotPerpCvdNode {
    id: String,
    symbol: String,
    window_bars: usize,
    min_spot_delta_share: f64,
    min_spot_directional_share: f64,
    dependencies: Vec<String>,
}

impl SpotPerpCvdNode {
    pub fn new(
        symbol: impl Into<String>,
        window_bars: usize,
        min_spot_delta_share: f64,
        min_spot_directional_share: f64,
    ) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.spot_perp_cvd"),
            symbol,
            window_bars,
            min_spot_delta_share,
            min_spot_directional_share,
            dependencies: vec![],
        }
    }
}

fn delta(bars: &[&greed_kernel::Candle], window: usize) -> Option<(f64, f64)> {
    if bars.len() < window {
        return None;
    }
    let source = &bars[bars.len() - window..];
    let volume: f64 = source.iter().map(|bar| bar.quote_volume).sum();
    let buy: f64 = source
        .iter()
        .map(|bar| bar.taker_buy_quote)
        .collect::<Option<Vec<_>>>()?
        .iter()
        .sum();
    Some((2.0 * buy - volume, volume))
}

impl StrategyNode for SpotPerpCvdNode {
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
        let perp_bars = closed_bars(&instrument.perpetual);
        let spot_values = instrument
            .spot
            .as_ref()
            .and_then(|series| delta(&closed_bars(series), self.window_bars));
        let perp_values = delta(&perp_bars, self.window_bars);
        let producer = self.id.clone();
        let (Some((spot_delta, spot_volume)), Some((perp_delta, perp_volume))) =
            (spot_values, perp_values)
        else {
            return Ok(vec![ArtifactRecord {
                key: format!("{}.flow", self.symbol),
                producer,
                artifact: Artifact::State(StateArtifact {
                    state: "unavailable".into(),
                    score: 0.0,
                    side: None,
                    verdict: Verdict::Unknown,
                    reasons: vec!["spot/perpetual aggressor attribution is incomplete".into()],
                    metrics: BTreeMap::new(),
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        DataQuality::Missing,
                        0.0,
                        vec![
                            format!("{}.spot.cvd", self.symbol),
                            format!("{}.perpetual.cvd", self.symbol),
                        ],
                    ),
                }),
            }]);
        };
        let side = if spot_delta > 0.0 && perp_delta > 0.0 {
            Some(Side::Buy)
        } else if spot_delta < 0.0 && perp_delta < 0.0 {
            Some(Side::Sell)
        } else {
            None
        };
        let spot_delta_share = spot_delta.abs() / spot_volume.max(1.0);
        let directional_share = spot_delta.abs() / (spot_delta.abs() + perp_delta.abs()).max(1.0);
        let meaningful_spot = spot_delta_share >= self.min_spot_delta_share
            && directional_share >= self.min_spot_directional_share;
        let state = match (side, meaningful_spot) {
            (Some(Side::Buy), true) => "spot_confirmed_buy",
            (Some(Side::Sell), true) => "spot_confirmed_sell",
            (Some(_), false) => "perp_led",
            (None, _) => "cross_market_conflict",
        };
        let verdict = if side.is_some() && meaningful_spot {
            Verdict::Pass
        } else {
            Verdict::Block
        };
        let mut metrics = BTreeMap::new();
        metrics.insert("spot_delta_usd".into(), spot_delta);
        metrics.insert("perp_delta_usd".into(), perp_delta);
        metrics.insert("spot_volume_usd".into(), spot_volume);
        metrics.insert("perp_volume_usd".into(), perp_volume);
        metrics.insert("spot_delta_share".into(), spot_delta_share);
        metrics.insert("spot_directional_share".into(), directional_share);
        let quality = instrument
            .spot
            .as_ref()
            .map(|spot| spot.meta.quality.min(instrument.perpetual.meta.quality))
            .unwrap_or(DataQuality::Missing);
        Ok(vec![
            ArtifactRecord {
                key: format!("{}.spot_cvd", self.symbol),
                producer: producer.clone(),
                artifact: Artifact::Feature(FeatureArtifact {
                    value: spot_delta,
                    unit: "usd".into(),
                    side,
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        quality,
                        spot_delta_share.min(1.0),
                        vec![format!("{}.spot.candles", self.symbol)],
                    ),
                }),
            },
            ArtifactRecord {
                key: format!("{}.perp_cvd", self.symbol),
                producer: producer.clone(),
                artifact: Artifact::Feature(FeatureArtifact {
                    value: perp_delta,
                    unit: "usd".into(),
                    side,
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        quality,
                        (perp_delta.abs() / perp_volume.max(1.0)).min(1.0),
                        vec![format!("{}.perpetual.candles", self.symbol)],
                    ),
                }),
            },
            ArtifactRecord {
                key: format!("{}.flow", self.symbol),
                producer,
                artifact: Artifact::State(StateArtifact {
                    state: state.into(),
                    score: (spot_delta.abs() + perp_delta.abs())
                        / (spot_volume + perp_volume).max(1.0),
                    side,
                    verdict,
                    reasons: if verdict == Verdict::Pass {
                        vec![]
                    } else {
                        vec!["spot/perpetual flow is conflicting or perp-led".into()]
                    },
                    metrics,
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        quality,
                        if meaningful_spot { 1.0 } else { 0.4 },
                        vec![
                            format!("{}.spot.cvd", self.symbol),
                            format!("{}.perpetual.cvd", self.symbol),
                        ],
                    ),
                }),
            },
        ])
    }
}
