use super::{closed_bars, meta};
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, DataQuality, NodeContext, Side, StateArtifact,
    StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct UniverseEligibilityNode {
    id: String,
    symbol: String,
    min_volume_24h: f64,
    min_open_interest: f64,
    dependencies: Vec<String>,
}

impl UniverseEligibilityNode {
    pub fn new(symbol: impl Into<String>, min_volume_24h: f64, min_open_interest: f64) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.universe_eligibility"),
            symbol,
            min_volume_24h,
            min_open_interest,
            dependencies: vec![],
        }
    }
}

impl StrategyNode for UniverseEligibilityNode {
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
        let volume_24h = (bars.len() >= 96).then(|| {
            bars[bars.len() - 96..]
                .iter()
                .map(|bar| bar.quote_volume)
                .sum::<f64>()
        });
        let oi = instrument
            .derivatives
            .as_ref()
            .and_then(|value| value.open_interest_usd);
        let book_ok = instrument.book.as_ref().is_some_and(|book| {
            book.meta.usable_at(ctx.frame.as_of_ms) && book.bid > 0.0 && book.ask >= book.bid
        });
        let complete = volume_24h.is_some() && oi.is_some() && instrument.book.is_some();
        let pass = volume_24h.is_some_and(|value| value >= self.min_volume_24h)
            && oi.is_some_and(|value| value >= self.min_open_interest)
            && book_ok;
        let mut metrics = BTreeMap::new();
        if let Some(value) = volume_24h {
            metrics.insert("volume_24h_usd".into(), value);
        }
        if let Some(value) = oi {
            metrics.insert("open_interest_usd".into(), value);
        }
        Ok(vec![ArtifactRecord {
            key: format!("{}.universe", self.symbol),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if pass {
                    "eligible"
                } else if complete {
                    "ineligible"
                } else {
                    "unavailable"
                }
                .into(),
                score: volume_24h.unwrap_or(0.0) / self.min_volume_24h.max(1.0),
                side: None,
                verdict: if pass {
                    Verdict::Pass
                } else if complete {
                    Verdict::Block
                } else {
                    Verdict::Unknown
                },
                reasons: if pass {
                    vec![]
                } else {
                    vec!["24h volume, OI or executable book below universe threshold".into()]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    if complete {
                        DataQuality::Complete
                    } else {
                        DataQuality::Missing
                    },
                    if complete { 1.0 } else { 0.0 },
                    vec![
                        format!("{}.perpetual.candles", self.symbol),
                        format!("{}.open_interest", self.symbol),
                        format!("{}.book", self.symbol),
                    ],
                ),
            }),
        }])
    }
}

pub struct MarketBreadthNode {
    id: String,
    horizon_bars: usize,
    threshold: f64,
    min_participation: f64,
    dependencies: Vec<String>,
}
impl MarketBreadthNode {
    pub fn new(
        horizon_bars: usize,
        threshold: f64,
        min_participation: f64,
        symbols: &[String],
    ) -> Self {
        Self {
            id: "alt.market_breadth".into(),
            horizon_bars,
            threshold,
            min_participation,
            dependencies: symbols
                .iter()
                .map(|symbol| format!("{symbol}.universe_eligibility"))
                .collect(),
        }
    }
}
impl StrategyNode for MarketBreadthNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }
    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut returns = Vec::new();
        for instrument in ctx
            .frame
            .instruments
            .values()
            .filter(|i| i.asset_class == AssetClass::Altcoin)
        {
            if ctx
                .artifact(&format!("{}.universe", instrument.symbol))
                .and_then(|value| value.state())
                .is_none_or(|state| state.verdict != Verdict::Pass)
            {
                continue;
            }
            let bars = closed_bars(&instrument.perpetual);
            if bars.len() > self.horizon_bars {
                returns.push(
                    bars.last().unwrap().close / bars[bars.len() - self.horizon_bars - 1].close
                        - 1.0,
                );
            }
        }
        if returns.len() < 5 {
            return Ok(vec![ArtifactRecord {
                key: "alt.breadth".into(),
                producer: self.id.clone(),
                artifact: Artifact::State(StateArtifact {
                    state: "unavailable".into(),
                    score: 0.0,
                    side: None,
                    verdict: Verdict::Unknown,
                    reasons: vec!["fewer than five eligible altcoins".into()],
                    metrics: BTreeMap::new(),
                    meta: meta(
                        ctx.frame.as_of_ms,
                        90_000,
                        DataQuality::Missing,
                        0.0,
                        vec!["alt.perpetual.candles".into()],
                    ),
                }),
            }]);
        }
        returns.sort_by(f64::total_cmp);
        let median = returns[returns.len() / 2];
        let positive = returns.iter().filter(|v| **v > 0.0).count() as f64 / returns.len() as f64;
        let negative = returns.iter().filter(|v| **v < 0.0).count() as f64 / returns.len() as f64;
        let side = if median >= self.threshold && positive >= self.min_participation {
            Some(Side::Buy)
        } else if median <= -self.threshold && negative >= self.min_participation {
            Some(Side::Sell)
        } else {
            None
        };
        let state = match side {
            Some(Side::Buy) => "broad_up",
            Some(Side::Sell) => "broad_down",
            None => "neutral",
        };
        let mut metrics = BTreeMap::new();
        metrics.insert("median_return_pct".into(), median);
        metrics.insert("positive_breadth".into(), positive);
        metrics.insert("negative_breadth".into(), negative);
        metrics.insert("universe_size".into(), returns.len() as f64);
        Ok(vec![ArtifactRecord {
            key: "alt.breadth".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: median.abs() * positive.max(negative),
                side,
                verdict: if side.is_some() {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if side.is_some() {
                    vec![]
                } else {
                    vec!["median return and participation do not define a broad regime".into()]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["alt.perpetual.candles".into()],
                ),
            }),
        }])
    }
}
