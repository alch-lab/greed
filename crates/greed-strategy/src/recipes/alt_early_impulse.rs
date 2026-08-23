use crate::primitives::{closed_bars, meta};
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, DataQuality, NodeContext, Side, StateArtifact,
    StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct AltEarlyImpulseNode {
    id: String,
    names: usize,
    min_15m: f64,
    max_15m: f64,
    min_volume_ratio: f64,
    max_1h: f64,
    max_wick_ratio: f64,
    max_range_ratio: f64,
    anchor_symbol: String,
    dependencies: Vec<String>,
}

impl AltEarlyImpulseNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        names: usize,
        min_15m: f64,
        max_15m: f64,
        min_volume_ratio: f64,
        max_1h: f64,
        max_wick_ratio: f64,
        max_range_ratio: f64,
        anchor_symbol: &str,
        symbols: &[String],
    ) -> Self {
        Self {
            id: "alt.recipe.early_impulse".into(),
            names,
            min_15m,
            max_15m,
            min_volume_ratio,
            max_1h,
            max_wick_ratio,
            max_range_ratio,
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
}

struct Setup<'a> {
    instrument: &'a greed_kernel::InstrumentFrame,
    side: Side,
    move_15m: f64,
    move_1h: f64,
    volume_ratio: f64,
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

impl StrategyNode for AltEarlyImpulseNode {
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
        let mut missing_fast = 0u64;
        let mut impulse_hits = 0u64;
        let mut volume_hits = 0u64;
        let mut structure_hits = 0u64;
        let mut climax_blocks = 0u64;
        let mut best_move: f64 = 0.0;
        let mut best_volume: f64 = 0.0;
        let mut setups = Vec::new();

        for instrument in ctx
            .frame
            .instruments
            .values()
            .filter(|instrument| instrument.asset_class == AssetClass::Altcoin)
        {
            if ctx
                .artifact(&format!("{}.universe", instrument.symbol))
                .and_then(|artifact| artifact.state())
                .is_none_or(|state| state.verdict != Verdict::Pass)
            {
                continue;
            }
            eligible += 1;
            let slow = closed_bars(&instrument.perpetual);
            let Some(fast_series) = instrument.fast_perpetual.as_ref() else {
                missing_fast += 1;
                continue;
            };
            let fast = closed_bars(fast_series);
            if slow.len() < 5 || fast.len() < 24 {
                missing_fast += 1;
                continue;
            }
            let f = fast.len() - 1;
            let move_15m = fast[f].close / fast[f - 3].close - 1.0;
            let move_1h = fast[f].close / fast[f - 12].close - 1.0;
            best_move = best_move.max(move_15m.abs());
            if move_15m.abs() < self.min_15m
                || move_15m.abs() > self.max_15m
                || move_1h.abs() > self.max_1h
                || move_15m.signum() != move_1h.signum()
            {
                continue;
            }
            impulse_hits += 1;
            let side = if move_15m > 0.0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let baseline_volume = fast[f - 20..f]
                .iter()
                .map(|bar| bar.quote_volume)
                .sum::<f64>()
                / 20.0;
            let volume_ratio = fast[f].quote_volume / baseline_volume.max(1.0);
            best_volume = best_volume.max(volume_ratio);
            if volume_ratio < self.min_volume_ratio {
                continue;
            }
            volume_hits += 1;
            let higher_structure = match side {
                Side::Buy => fast[f].close > fast[f - 1].high && fast[f].low > fast[f - 2].low,
                Side::Sell => fast[f].close < fast[f - 1].low && fast[f].high < fast[f - 2].high,
            };
            if !higher_structure {
                continue;
            }
            structure_hits += 1;
            let average_range = fast[f - 20..f]
                .iter()
                .map(|bar| (bar.high - bar.low) / bar.close.max(f64::EPSILON))
                .sum::<f64>()
                / 20.0;
            let current_range = (fast[f].high - fast[f].low) / fast[f].close.max(f64::EPSILON);
            if directional_wick(fast[f], side) > self.max_wick_ratio
                || current_range / average_range.max(f64::EPSILON) > self.max_range_ratio
            {
                climax_blocks += 1;
                continue;
            }
            setups.push(Setup {
                instrument,
                side,
                move_15m,
                move_1h,
                volume_ratio,
                score: move_15m.abs() * 3.0 + move_1h.abs() + volume_ratio.ln_1p() * 0.01,
            });
        }
        setups.sort_by(|a, b| b.score.total_cmp(&a.score));
        let selected = setups.len().min(self.names);
        let metrics = BTreeMap::from([
            ("eligible_symbols".into(), eligible as f64),
            ("missing_fast_data".into(), missing_fast as f64),
            ("impulse_hits".into(), impulse_hits as f64),
            ("volume_hits".into(), volume_hits as f64),
            ("structure_hits".into(), structure_hits as f64),
            ("climax_blocks".into(), climax_blocks as f64),
            ("selected_symbols".into(), selected as f64),
            ("best_move_15m".into(), best_move),
            ("best_volume_ratio".into(), best_volume),
        ]);
        let mut out = vec![ArtifactRecord {
            key: "alt.early_impulse".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if selected > 0 {
                    "impulse_ready"
                } else if missing_fast == eligible && eligible > 0 {
                    "waiting_for_fast_data"
                } else {
                    "scanning_for_early_impulse"
                }
                .into(),
                score: setups.first().map(|setup| setup.score).unwrap_or(0.0),
                side: setups.first().map(|setup| setup.side),
                verdict: if selected > 0 {
                    Verdict::Pass
                } else if missing_fast == eligible && eligible > 0 {
                    Verdict::Unknown
                } else {
                    Verdict::Block
                },
                reasons: if selected > 0 {
                    vec![]
                } else {
                    vec![format!(
                        "no safe early impulse: impulse={impulse_hits}, volume={volume_hits}, structure={structure_hits}, climax={climax_blocks}, missing_fast={missing_fast}"
                    )]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    if missing_fast == eligible && eligible > 0 {
                        DataQuality::Missing
                    } else {
                        DataQuality::Complete
                    },
                    if eligible > 0 { 1.0 } else { 0.0 },
                    vec!["alt.fast_perpetual.candles".into(), "alt.universe".into()],
                ),
            }),
        }];
        for setup in setups.into_iter().take(self.names) {
            let context = |state: &StateArtifact| {
                if state.verdict == Verdict::Pass && state.side == Some(setup.side) {
                    "confirmed"
                } else if state.verdict == Verdict::Pass && state.side.is_some() {
                    "opposed"
                } else {
                    "neutral"
                }
            };
            let signal_ms = setup
                .instrument
                .fast_perpetual
                .as_ref()
                .map(closed_bars)
                .and_then(|bars| bars.last().map(|bar| bar.close_ms))
                .unwrap_or(ctx.frame.as_of_ms);
            let anchor_context = context(anchor);
            let breadth_context = context(breadth);
            let context_confirmed = anchor_context == "confirmed" || breadth_context == "confirmed";
            let candidate = TradeCandidate {
                id: format!("{}:{}:{}", self.id, setup.instrument.symbol, signal_ms),
                recipe: "alt_early_impulse".into(),
                symbol: setup.instrument.symbol.clone(),
                side: setup.side,
                signal_ms,
                expires_ms: signal_ms + 7 * 60_000,
                reference_price: setup.instrument.price,
                score: setup.score,
                confidence: (0.45 + setup.volume_ratio.min(3.0) * 0.15).min(0.9),
                verdict: if context_confirmed {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                blockers: if context_confirmed {
                    vec![]
                } else {
                    vec!["early impulse needs BTC trend or alt breadth confirmation".into()]
                },
                evidence: vec![
                    format!("{}.fast_perpetual", setup.instrument.symbol),
                    format!("{}.universe", setup.instrument.symbol),
                    format!("{}.trend", self.anchor_symbol),
                    "alt.breadth".into(),
                ],
                tags: BTreeMap::from([
                    ("move_15m".into(), format!("{:.8}", setup.move_15m)),
                    ("move_1h".into(), format!("{:.8}", setup.move_1h)),
                    ("volume_ratio".into(), format!("{:.4}", setup.volume_ratio)),
                    ("stop_profile".into(), "alt_intraday".into()),
                    ("hold_profile".into(), "alt_intraday".into()),
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
