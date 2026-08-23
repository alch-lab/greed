use crate::primitives::{closed_bars, meta, path_efficiency};
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, DataQuality, NodeContext, Side, StateArtifact,
    StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

const BAR_MS: i64 = 15 * 60_000;

pub struct AltOutlierMomentumNode {
    id: String,
    names: usize,
    min_return_1h: f64,
    min_return_4h: f64,
    min_volume_ratio: f64,
    min_efficiency: f64,
    anchor_symbol: String,
    dependencies: Vec<String>,
}

impl AltOutlierMomentumNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        names: usize,
        min_return_1h: f64,
        min_return_4h: f64,
        min_volume_ratio: f64,
        min_efficiency: f64,
        anchor_symbol: &str,
        symbols: &[String],
    ) -> Self {
        Self {
            id: "alt.recipe.outlier_momentum".into(),
            names,
            min_return_1h,
            min_return_4h,
            min_volume_ratio,
            min_efficiency,
            anchor_symbol: anchor_symbol.into(),
            dependencies: std::iter::once("alt.market_breadth".into())
                .chain(std::iter::once(format!("{anchor_symbol}.trend_regime")))
                .chain(
                    symbols
                        .iter()
                        .map(|symbol| format!("{symbol}.universe_eligibility")),
                )
                .collect(),
        }
    }

    fn status(
        &self,
        ctx: &NodeContext<'_>,
        state: &str,
        verdict: Verdict,
        reasons: Vec<String>,
        metrics: BTreeMap<String, f64>,
    ) -> ArtifactRecord {
        ArtifactRecord {
            key: "alt.outlier_momentum".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: metrics.get("best_score").copied().unwrap_or(0.0),
                side: None,
                verdict,
                reasons,
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "alt.perpetual.candles".into(),
                        "alt.universe".into(),
                        format!("{}.trend", self.anchor_symbol),
                    ],
                ),
            }),
        }
    }
}

struct Setup<'a> {
    instrument: &'a greed_kernel::InstrumentFrame,
    side: Side,
    return_1h: f64,
    return_4h: f64,
    volume_ratio: f64,
    efficiency: f64,
    score: f64,
}

impl StrategyNode for AltOutlierMomentumNode {
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
        let mut eligible = 0u64;
        let mut move_hits = 0u64;
        let mut volume_hits = 0u64;
        let mut trend_hits = 0u64;
        let mut best_return_1h: f64 = 0.0;
        let mut best_return_4h: f64 = 0.0;
        let mut best_volume_ratio: f64 = 0.0;
        let mut setups = Vec::new();

        for instrument in ctx
            .frame
            .instruments
            .values()
            .filter(|instrument| instrument.asset_class == AssetClass::Altcoin)
        {
            if ctx
                .artifact(&format!("{}.universe", instrument.symbol))
                .and_then(|value| value.state())
                .is_none_or(|state| state.verdict != Verdict::Pass)
            {
                continue;
            }
            let bars = closed_bars(&instrument.perpetual);
            if bars.len() < 97 {
                continue;
            }
            eligible += 1;
            let i = bars.len() - 1;
            let return_1h = bars[i].close / bars[i - 4].close - 1.0;
            let return_4h = bars[i].close / bars[i - 16].close - 1.0;
            best_return_1h = best_return_1h.max(return_1h.abs());
            best_return_4h = best_return_4h.max(return_4h.abs());
            let same_direction = return_1h.signum() == return_4h.signum();
            if !same_direction
                || (return_1h.abs() < self.min_return_1h && return_4h.abs() < self.min_return_4h)
            {
                continue;
            }
            move_hits += 1;
            let recent_volume = bars[i - 3..=i]
                .iter()
                .map(|bar| bar.quote_volume)
                .sum::<f64>()
                / 4.0;
            let baseline_volume = bars[i - 96..i - 4]
                .iter()
                .map(|bar| bar.quote_volume)
                .sum::<f64>()
                / 92.0;
            let volume_ratio = recent_volume / baseline_volume.max(1.0);
            best_volume_ratio = best_volume_ratio.max(volume_ratio);
            if volume_ratio < self.min_volume_ratio {
                continue;
            }
            volume_hits += 1;
            let window = &bars[i - 16..=i];
            let efficiency = path_efficiency(window);
            if efficiency < self.min_efficiency {
                continue;
            }
            let side = if return_1h > 0.0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let at_edge = match side {
                Side::Buy => {
                    let high = window.iter().map(|bar| bar.high).fold(0.0, f64::max);
                    bars[i].close >= high * 0.995
                }
                Side::Sell => {
                    let low = window
                        .iter()
                        .map(|bar| bar.low)
                        .fold(f64::INFINITY, f64::min);
                    bars[i].close <= low * 1.005
                }
            };
            if !at_edge {
                continue;
            }
            trend_hits += 1;
            let score =
                return_1h.abs() * 2.0 + return_4h.abs() + volume_ratio.ln_1p() * efficiency * 0.01;
            setups.push(Setup {
                instrument,
                side,
                return_1h,
                return_4h,
                volume_ratio,
                efficiency,
                score,
            });
        }
        setups.sort_by(|a, b| b.score.total_cmp(&a.score));
        let best_score = setups.first().map(|setup| setup.score).unwrap_or(0.0);
        let metrics = BTreeMap::from([
            ("eligible_symbols".into(), eligible as f64),
            ("move_hits".into(), move_hits as f64),
            ("volume_hits".into(), volume_hits as f64),
            ("trend_hits".into(), trend_hits as f64),
            (
                "selected_symbols".into(),
                setups.len().min(self.names) as f64,
            ),
            ("return_1h_threshold".into(), self.min_return_1h),
            ("return_4h_threshold".into(), self.min_return_4h),
            ("volume_threshold".into(), self.min_volume_ratio),
            ("best_return_1h".into(), best_return_1h),
            ("best_return_4h".into(), best_return_4h),
            ("best_volume_ratio".into(), best_volume_ratio),
            ("best_score".into(), best_score),
        ]);
        let mut out = vec![self.status(
            ctx,
            if setups.is_empty() {
                "scanning_for_outlier"
            } else {
                "outlier_ready"
            },
            if setups.is_empty() {
                Verdict::Block
            } else {
                Verdict::Pass
            },
            if setups.is_empty() {
                vec![
                    "no liquid altcoin has confirmed individual momentum and volume expansion"
                        .into(),
                ]
            } else {
                vec![]
            },
            metrics,
        )];

        for setup in setups.into_iter().take(self.names) {
            let anchor_context =
                if anchor.side == Some(setup.side) && anchor.verdict == Verdict::Pass {
                    "confirmed"
                } else if anchor.side.is_some() && anchor.verdict == Verdict::Pass {
                    "opposed"
                } else {
                    "neutral"
                };
            let breadth_context =
                if breadth.side == Some(setup.side) && breadth.verdict == Verdict::Pass {
                    "confirmed"
                } else if breadth.side.is_some() && breadth.verdict == Verdict::Pass {
                    "opposed"
                } else {
                    "neutral"
                };
            let signal_ms = closed_bars(&setup.instrument.perpetual)
                .last()
                .expect("outlier setup has bars")
                .close_ms;
            let cycle = signal_ms.div_euclid(4 * BAR_MS);
            let candidate = TradeCandidate {
                id: format!("{}:{}:cycle-{cycle}", self.id, setup.instrument.symbol),
                recipe: "alt_outlier_momentum".into(),
                symbol: setup.instrument.symbol.clone(),
                side: setup.side,
                signal_ms,
                expires_ms: signal_ms + BAR_MS,
                reference_price: setup.instrument.price,
                score: setup.score,
                confidence: (setup.efficiency
                    * (setup.volume_ratio / self.min_volume_ratio).min(1.5)
                    / 1.5)
                    .clamp(0.35, 1.0),
                verdict: Verdict::Pass,
                blockers: vec![],
                evidence: vec![
                    format!("{}.outlier_return", setup.instrument.symbol),
                    format!("{}.volume_expansion", setup.instrument.symbol),
                    format!("{}.trend", self.anchor_symbol),
                    "alt.breadth".into(),
                ],
                tags: BTreeMap::from([
                    ("return_1h".into(), format!("{:.8}", setup.return_1h)),
                    ("return_4h".into(), format!("{:.8}", setup.return_4h)),
                    ("volume_ratio".into(), format!("{:.4}", setup.volume_ratio)),
                    ("stop_profile".into(), "alt_outlier".into()),
                    ("hold_profile".into(), "alt_outlier".into()),
                    ("anchor_context".into(), anchor_context.into()),
                    ("breadth_context".into(), breadth_context.into()),
                ]),
            };
            out.push(ArtifactRecord {
                key: format!("candidate.{}", candidate.id),
                producer: self.id.clone(),
                artifact: Artifact::Candidate(candidate),
            });
        }
        Ok(out)
    }
}
