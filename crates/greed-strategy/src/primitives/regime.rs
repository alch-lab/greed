use super::{closed_bars, meta};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, Side, StateArtifact, StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct MarketRegimeNode {
    id: String,
    anchor_symbol: String,
    volatility_window: usize,
    shock_volatility_ratio: f64,
    dependencies: Vec<String>,
}

impl MarketRegimeNode {
    pub fn new(anchor_symbol: &str, volatility_window: usize, shock_volatility_ratio: f64) -> Self {
        Self {
            id: "portfolio.market_regime".into(),
            anchor_symbol: anchor_symbol.into(),
            volatility_window,
            shock_volatility_ratio,
            dependencies: vec![
                format!("{anchor_symbol}.trend_regime"),
                "alt.market_breadth".into(),
            ],
        }
    }
}

fn realized_volatility(bars: &[&greed_kernel::Candle]) -> f64 {
    if bars.len() < 3 {
        return 0.0;
    }
    let returns: Vec<_> = bars
        .windows(2)
        .map(|pair| pair[1].close / pair[0].close.max(f64::EPSILON) - 1.0)
        .collect();
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    (returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (returns.len() - 1) as f64)
        .sqrt()
}

impl StrategyNode for MarketRegimeNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let anchor = ctx
            .artifact(&format!("{}.trend", self.anchor_symbol))
            .and_then(|artifact| artifact.state())
            .ok_or("anchor trend artifact missing")?;
        let breadth = ctx
            .artifact("alt.breadth")
            .and_then(|artifact| artifact.state())
            .ok_or("breadth artifact missing")?;
        let instrument = ctx
            .frame
            .instrument(&self.anchor_symbol)
            .ok_or("anchor instrument missing")?;
        let bars = closed_bars(&instrument.perpetual);
        let required = self.volatility_window * 3 + 1;
        let (recent_volatility, baseline_volatility) = if bars.len() >= required {
            (
                realized_volatility(&bars[bars.len() - self.volatility_window - 1..]),
                realized_volatility(
                    &bars[bars.len() - required..bars.len() - self.volatility_window],
                ),
            )
        } else {
            (0.0, 0.0)
        };
        let volatility_ratio = recent_volatility / baseline_volatility.max(0.000_01);
        let aligned = anchor.side.is_some() && anchor.side == breadth.side;
        let state = if volatility_ratio >= self.shock_volatility_ratio {
            "shock"
        } else if aligned && anchor.side == Some(Side::Buy) {
            "risk_on"
        } else if aligned && anchor.side == Some(Side::Sell) {
            "risk_off"
        } else if anchor.side.is_some() && breadth.side.is_some() {
            "transition"
        } else {
            "neutral"
        };
        let side = match state {
            "risk_on" => Some(Side::Buy),
            "risk_off" => Some(Side::Sell),
            _ => None,
        };
        Ok(vec![ArtifactRecord {
            key: "portfolio.regime".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: volatility_ratio.max(anchor.score + breadth.score),
                side,
                verdict: if state == "shock" {
                    Verdict::Block
                } else if bars.len() < required {
                    Verdict::Unknown
                } else {
                    Verdict::Pass
                },
                reasons: match state {
                    "shock" => vec!["anchor realized volatility is in a shock regime".into()],
                    "transition" => vec!["BTC trend and altcoin breadth disagree".into()],
                    "neutral" => vec!["market direction lacks broad confirmation".into()],
                    _ => vec![],
                },
                metrics: BTreeMap::from([
                    ("recent_volatility".into(), recent_volatility),
                    ("baseline_volatility".into(), baseline_volatility),
                    ("volatility_ratio".into(), volatility_ratio),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    if bars.len() >= required {
                        DataQuality::Complete
                    } else {
                        DataQuality::Missing
                    },
                    if bars.len() >= required { 1.0 } else { 0.0 },
                    vec![
                        format!("{}.perpetual.candles", self.anchor_symbol),
                        format!("{}.trend", self.anchor_symbol),
                        "alt.breadth".into(),
                    ],
                ),
            }),
        }])
    }
}
