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
    confirmation_min_5m: f64,
    confirmation_max_5m: f64,
    max_directional_wick_ratio: f64,
    max_climax_range_ratio: f64,
    pullback_min: f64,
    pullback_max: f64,
    candidate_expiry_ms: i64,
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
        confirmation_min_5m: f64,
        confirmation_max_5m: f64,
        max_directional_wick_ratio: f64,
        max_climax_range_ratio: f64,
        pullback_min: f64,
        pullback_max: f64,
        candidate_expiry_minutes: u32,
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
            confirmation_min_5m,
            confirmation_max_5m,
            max_directional_wick_ratio,
            max_climax_range_ratio,
            pullback_min,
            pullback_max,
            candidate_expiry_ms: i64::from(candidate_expiry_minutes) * 60_000,
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
    confirmation_5m: f64,
    directional_wick_ratio: f64,
    pattern: &'static str,
    score: f64,
}

fn directional_wick(bar: &greed_kernel::Candle, side: Side) -> f64 {
    let range = (bar.high - bar.low).max(f64::EPSILON);
    match side {
        Side::Buy => (bar.high - bar.open.max(bar.close)) / range,
        Side::Sell => (bar.open.min(bar.close) - bar.low) / range,
    }
    .clamp(0.0, 1.0)
}

fn ordered_pullback(bars: &[&greed_kernel::Candle], side: Side) -> f64 {
    match side {
        Side::Buy => bars
            .iter()
            .enumerate()
            .map(|(index, bar)| {
                let low_after = bars[index..]
                    .iter()
                    .map(|value| value.low)
                    .fold(f64::INFINITY, f64::min);
                (bar.high - low_after) / bar.high.max(f64::EPSILON)
            })
            .fold(0.0, f64::max),
        Side::Sell => bars
            .iter()
            .enumerate()
            .map(|(index, bar)| {
                let high_after = bars[index..]
                    .iter()
                    .map(|value| value.high)
                    .fold(0.0, f64::max);
                (high_after - bar.low) / bar.low.max(f64::EPSILON)
            })
            .fold(0.0, f64::max),
    }
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
        let mut fast_missing = 0u64;
        let mut climax_blocks = 0u64;
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
            let Some(fast_series) = instrument.fast_perpetual.as_ref() else {
                fast_missing += 1;
                continue;
            };
            let fast = closed_bars(fast_series);
            if fast.len() < 25 {
                fast_missing += 1;
                continue;
            }
            let f = fast.len() - 1;
            let last = fast[f];
            let confirmation_5m = side.sign() * (last.close / fast[f - 1].close - 1.0);
            let wick_ratio = directional_wick(last, side);
            let average_range = fast[f - 20..f]
                .iter()
                .map(|bar| (bar.high - bar.low) / bar.close.max(f64::EPSILON))
                .sum::<f64>()
                / 20.0;
            let current_range = (last.high - last.low) / last.close.max(f64::EPSILON);
            let range_ratio = current_range / average_range.max(f64::EPSILON);
            let climax = wick_ratio > self.max_directional_wick_ratio
                || range_ratio > self.max_climax_range_ratio
                || confirmation_5m > self.confirmation_max_5m;
            if climax {
                climax_blocks += 1;
                continue;
            }
            let near_fast_edge = match side {
                Side::Buy => {
                    last.close
                        >= fast[f - 5..=f]
                            .iter()
                            .map(|bar| bar.high)
                            .fold(0.0, f64::max)
                            * 0.997
                }
                Side::Sell => {
                    last.close
                        <= fast[f - 5..=f]
                            .iter()
                            .map(|bar| bar.low)
                            .fold(f64::INFINITY, f64::min)
                            * 1.003
                }
            };
            let continuation = confirmation_5m >= self.confirmation_min_5m
                && confirmation_5m <= self.confirmation_max_5m
                && near_fast_edge;
            let pullback = ordered_pullback(&fast[f - 5..=f], side);
            let reclaimed = match side {
                Side::Buy => last.close > fast[f - 1].high,
                Side::Sell => last.close < fast[f - 1].low,
            };
            let reclaim = pullback >= self.pullback_min
                && pullback <= self.pullback_max
                && confirmation_5m >= self.confirmation_min_5m
                && reclaimed
                && last.quote_volume >= fast[f - 1].quote_volume * 0.9;
            let pattern = if continuation {
                "continuation"
            } else if reclaim {
                "pullback_reclaim"
            } else {
                continue;
            };
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
                confirmation_5m,
                directional_wick_ratio: wick_ratio,
                pattern,
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
            ("fast_data_missing".into(), fast_missing as f64),
            ("climax_blocks".into(), climax_blocks as f64),
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
                if move_hits > 0 {
                    "waiting_for_fast_confirmation"
                } else {
                    "scanning_for_outlier"
                }
            } else {
                "outlier_ready"
            },
            if setups.is_empty() {
                Verdict::Block
            } else {
                Verdict::Pass
            },
            if setups.is_empty() {
                vec![if move_hits > 0 {
                    "radar triggered, but no 5m continuation or pullback reclaim is safe to enter"
                        .into()
                } else {
                    "no liquid altcoin has reached the early-momentum radar".into()
                }]
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
            let signal_ms = setup
                .instrument
                .fast_perpetual
                .as_ref()
                .map(closed_bars)
                .and_then(|bars| bars.last().map(|bar| bar.close_ms))
                .unwrap_or(ctx.frame.as_of_ms);
            let cycle = signal_ms.div_euclid(BAR_MS);
            let candidate = TradeCandidate {
                id: format!("{}:{}:cycle-{cycle}", self.id, setup.instrument.symbol),
                recipe: match setup.pattern {
                    "pullback_reclaim" => "alt_outlier_pullback_reclaim",
                    _ => "alt_outlier_continuation",
                }
                .into(),
                symbol: setup.instrument.symbol.clone(),
                side: setup.side,
                signal_ms,
                expires_ms: signal_ms + self.candidate_expiry_ms,
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
                    (
                        "confirmation_5m".into(),
                        format!("{:.8}", setup.confirmation_5m),
                    ),
                    (
                        "directional_wick_ratio".into(),
                        format!("{:.4}", setup.directional_wick_ratio),
                    ),
                    ("entry_pattern".into(), setup.pattern.into()),
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
