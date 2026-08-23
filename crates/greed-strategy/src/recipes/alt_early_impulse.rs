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
    min_return_z: f64,
    pullback_min_fraction: f64,
    pullback_max_fraction: f64,
    short_threshold_multiplier: f64,
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
        min_return_z: f64,
        pullback_min_fraction: f64,
        pullback_max_fraction: f64,
        short_threshold_multiplier: f64,
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
            min_return_z,
            pullback_min_fraction,
            pullback_max_fraction,
            short_threshold_multiplier,
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
    return_z: f64,
    pullback_fraction: f64,
    discovery_latency_ms: i64,
    score: f64,
}

fn return_sigma(bars: &[&greed_kernel::Candle]) -> f64 {
    let returns: Vec<_> = bars
        .windows(2)
        .map(|pair| pair[1].close / pair[0].close.max(f64::EPSILON) - 1.0)
        .collect();
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    (returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (returns.len() - 1) as f64)
        .sqrt()
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
        let mut pullback_hits = 0u64;
        let mut reclaim_hits = 0u64;
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
            if slow.len() < 5 || fast.len() < 40 {
                missing_fast += 1;
                continue;
            }
            let f = fast.len() - 1;
            let move_1h = fast[f].close / fast[f - 12].close - 1.0;
            let sigma_5m = return_sigma(&fast[f - 36..f]);
            let mut detected = None;
            for impulse_index in (f.saturating_sub(4)..f).rev() {
                if impulse_index < 27 {
                    continue;
                }
                let move_15m = fast[impulse_index].close / fast[impulse_index - 3].close - 1.0;
                let side = if move_15m > 0.0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let multiplier = if side == Side::Sell {
                    self.short_threshold_multiplier
                } else {
                    1.0
                };
                let expected_sigma = sigma_5m.max(0.000_1) * 3.0_f64.sqrt();
                let return_z = move_15m.abs() / expected_sigma;
                let effective_min =
                    self.min_15m.max(self.min_return_z * expected_sigma) * multiplier;
                best_move = best_move.max(move_15m.abs());
                if move_15m.abs() < effective_min
                    || move_15m.abs() > self.max_15m
                    || move_1h.abs() > self.max_1h
                    || move_15m.signum() != move_1h.signum()
                {
                    continue;
                }
                impulse_hits += 1;
                let baseline_volume = fast[impulse_index - 24..impulse_index]
                    .iter()
                    .map(|bar| bar.quote_volume)
                    .sum::<f64>()
                    / 24.0;
                let volume_ratio = fast[impulse_index].quote_volume / baseline_volume.max(1.0);
                best_volume = best_volume.max(volume_ratio);
                if volume_ratio < self.min_volume_ratio * multiplier
                    || fast[f].quote_volume < baseline_volume * 0.90
                {
                    continue;
                }
                volume_hits += 1;
                let origin = fast[impulse_index - 3].close;
                let impulse_size = (fast[impulse_index].close - origin).abs().max(f64::EPSILON);
                let pullback_fraction = match side {
                    Side::Buy => {
                        let peak = fast[impulse_index..f]
                            .iter()
                            .map(|bar| bar.high)
                            .fold(0.0, f64::max);
                        let low = fast[impulse_index + 1..=f]
                            .iter()
                            .map(|bar| bar.low)
                            .fold(f64::INFINITY, f64::min);
                        (peak - low) / impulse_size
                    }
                    Side::Sell => {
                        let low = fast[impulse_index..f]
                            .iter()
                            .map(|bar| bar.low)
                            .fold(f64::INFINITY, f64::min);
                        let high = fast[impulse_index + 1..=f]
                            .iter()
                            .map(|bar| bar.high)
                            .fold(0.0, f64::max);
                        (high - low) / impulse_size
                    }
                };
                let held_origin = match side {
                    Side::Buy => fast[impulse_index + 1..=f]
                        .iter()
                        .all(|bar| bar.low > origin),
                    Side::Sell => fast[impulse_index + 1..=f]
                        .iter()
                        .all(|bar| bar.high < origin),
                };
                if !held_origin
                    || pullback_fraction < self.pullback_min_fraction
                    || pullback_fraction > self.pullback_max_fraction
                {
                    continue;
                }
                pullback_hits += 1;
                let reclaimed = match side {
                    Side::Buy => fast[f].close > fast[f - 1].high,
                    Side::Sell => fast[f].close < fast[f - 1].low,
                };
                if !reclaimed {
                    continue;
                }
                reclaim_hits += 1;
                detected = Some((
                    move_15m,
                    side,
                    volume_ratio,
                    return_z,
                    pullback_fraction,
                    impulse_index,
                ));
                break;
            }
            let Some((move_15m, side, volume_ratio, return_z, pullback_fraction, impulse_index)) =
                detected
            else {
                continue;
            };
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
                return_z,
                pullback_fraction,
                discovery_latency_ms: (ctx.frame.as_of_ms - fast[impulse_index].close_ms).max(0),
                score: return_z * 0.02 + move_1h.abs() + volume_ratio.ln_1p() * 0.01,
            });
        }
        setups.sort_by(|a, b| b.score.total_cmp(&a.score));
        let selected = setups.len().min(self.names);
        let metrics = BTreeMap::from([
            ("eligible_symbols".into(), eligible as f64),
            ("missing_fast_data".into(), missing_fast as f64),
            ("impulse_hits".into(), impulse_hits as f64),
            ("volume_hits".into(), volume_hits as f64),
            ("pullback_hits".into(), pullback_hits as f64),
            ("reclaim_hits".into(), reclaim_hits as f64),
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
                        "no staged early impulse: impulse={impulse_hits}, volume={volume_hits}, pullback={pullback_hits}, reclaim={reclaim_hits}, climax={climax_blocks}, missing_fast={missing_fast}"
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
            let context_confirmed = match setup.side {
                Side::Buy => anchor_context == "confirmed" || breadth_context == "confirmed",
                Side::Sell => {
                    anchor_context != "opposed"
                        && (anchor_context == "confirmed" || breadth_context == "confirmed")
                }
            };
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
                    ("return_z".into(), format!("{:.4}", setup.return_z)),
                    (
                        "pullback_fraction".into(),
                        format!("{:.4}", setup.pullback_fraction),
                    ),
                    (
                        "discovery_latency_ms".into(),
                        setup.discovery_latency_ms.to_string(),
                    ),
                    ("entry_pattern".into(), "impulse_pullback_reclaim".into()),
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
