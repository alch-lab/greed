use crate::primitives::{closed_bars, meta};
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, DataQuality, NodeContext, Side, StateArtifact,
    StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct AltShockReversalNode {
    id: String,
    min_return: f64,
    min_reversal: f64,
    min_volume_ratio: f64,
    neutral_anchor_allowed: bool,
    anchor_symbol: String,
    dependencies: Vec<String>,
}

impl AltShockReversalNode {
    pub fn new(
        min_return: f64,
        min_reversal: f64,
        min_volume_ratio: f64,
        neutral_anchor_allowed: bool,
        anchor_symbol: &str,
        symbols: &[String],
    ) -> Self {
        Self {
            id: "alt.recipe.shock_reversal".into(),
            min_return,
            min_reversal,
            min_volume_ratio,
            neutral_anchor_allowed,
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
        side: Option<Side>,
        verdict: Verdict,
        reasons: Vec<String>,
        metrics: BTreeMap<String, f64>,
    ) -> ArtifactRecord {
        ArtifactRecord {
            key: "alt.shock_reversal".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: metrics.get("best_setup_score").copied().unwrap_or(0.0),
                side,
                verdict,
                reasons,
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["alt.perpetual.candles".into(), "alt.breadth".into()],
                ),
            }),
        }
    }
}

impl StrategyNode for AltShockReversalNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &self.dependencies
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let breadth = ctx
            .artifact("alt.breadth")
            .and_then(|artifact| artifact.state())
            .ok_or("breadth artifact missing")?;
        let anchor = ctx
            .artifact(&format!("{}.trend", self.anchor_symbol))
            .and_then(|artifact| artifact.state())
            .ok_or("anchor trend artifact missing")?;
        let mut candidates = Vec::new();
        let mut eligible = 0u64;
        let mut move_hits = 0u64;
        let mut reversal_hits = 0u64;
        let mut volume_hits = 0u64;
        let mut pass_candidates = 0u64;
        let mut best_move: f64 = 0.0;
        let mut best_reversal: f64 = 0.0;
        let mut best_volume: f64 = 0.0;
        let mut best_score: f64 = 0.0;
        let mut observed_side = None;

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
            if bars.len() < 101 {
                continue;
            }
            eligible += 1;
            let i = bars.len() - 1;
            let prior = bars[i - 1].close / bars[i - 5].close - 1.0;
            best_move = best_move.max(prior.abs());
            if prior.abs() < self.min_return {
                continue;
            }
            move_hits += 1;
            let side = if prior > 0.0 { Side::Sell } else { Side::Buy };
            let reversal = side.sign() * (bars[i].close / bars[i - 1].close - 1.0);
            best_reversal = best_reversal.max(reversal);
            if reversal < self.min_reversal {
                continue;
            }
            reversal_hits += 1;
            let baseline: f64 = bars[i - 96..i]
                .iter()
                .map(|bar| bar.quote_volume)
                .sum::<f64>()
                / 96.0;
            let volume_ratio = bars[i].quote_volume / baseline.max(1.0);
            best_volume = best_volume.max(volume_ratio);
            if volume_ratio < self.min_volume_ratio {
                continue;
            }
            volume_hits += 1;
            observed_side = Some(side);
            let breadth_opposes = breadth.verdict == Verdict::Pass
                && breadth
                    .side
                    .is_some_and(|breadth_side| breadth_side != side);
            let anchor_opposes = anchor.verdict == Verdict::Pass
                && anchor.side.is_some_and(|anchor_side| anchor_side != side);
            let strict_context_missing = !self.neutral_anchor_allowed
                && (breadth.verdict != Verdict::Pass
                    || breadth.side != Some(side)
                    || anchor.verdict != Verdict::Pass
                    || anchor.side != Some(side));
            let mut blockers = Vec::new();
            if strict_context_missing || (breadth_opposes && anchor_opposes) {
                blockers.push("shock reversal is opposed by both breadth and BTC trend".into());
            }
            let signal_ms = bars[i].close_ms;
            let score = prior.abs() * reversal * volume_ratio.ln_1p();
            best_score = best_score.max(score);
            if blockers.is_empty() {
                pass_candidates += 1;
            }
            let candidate = TradeCandidate {
                id: format!("{}:{}:{}", self.id, instrument.symbol, signal_ms),
                recipe: "alt_shock_reversal".into(),
                symbol: instrument.symbol.clone(),
                side,
                signal_ms,
                expires_ms: signal_ms + 15 * 60_000,
                reference_price: instrument.price,
                score,
                confidence: if breadth.side == Some(side) || anchor.side == Some(side) {
                    0.85
                } else {
                    0.65
                },
                verdict: if blockers.is_empty() {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                blockers,
                evidence: vec![
                    "alt.breadth".into(),
                    format!("{}.shock_return", instrument.symbol),
                    format!("{}.volume_burst", instrument.symbol),
                    format!("{}.trend", self.anchor_symbol),
                ],
                tags: BTreeMap::from([
                    ("prior_return_pct".into(), format!("{prior:.8}")),
                    ("reversal_pct".into(), format!("{reversal:.8}")),
                    ("volume_ratio".into(), format!("{volume_ratio:.4}")),
                    ("stop_profile".into(), "alt_shock".into()),
                ]),
            };
            candidates.push(ArtifactRecord {
                key: format!("candidate.{}", candidate.id),
                producer: self.id.clone(),
                artifact: Artifact::Candidate(candidate),
            });
        }

        let metrics = BTreeMap::from([
            ("eligible_symbols".into(), eligible as f64),
            ("move_threshold".into(), self.min_return),
            ("reversal_threshold".into(), self.min_reversal),
            ("volume_threshold".into(), self.min_volume_ratio),
            ("move_hits".into(), move_hits as f64),
            ("reversal_hits".into(), reversal_hits as f64),
            ("volume_hits".into(), volume_hits as f64),
            ("pass_candidates".into(), pass_candidates as f64),
            ("best_move_pct".into(), best_move),
            ("best_reversal_pct".into(), best_reversal),
            ("best_volume_ratio".into(), best_volume),
            ("best_setup_score".into(), best_score),
        ]);
        let (state, verdict, reasons) = if eligible == 0 {
            (
                "waiting_for_eligible_symbols",
                Verdict::Unknown,
                vec!["no eligible altcoin has enough closed candle history".into()],
            )
        } else if pass_candidates > 0 {
            ("opportunity_ready", Verdict::Pass, vec![])
        } else if volume_hits > 0 {
            (
                "blocked_by_market_context",
                Verdict::Block,
                vec!["raw shock setup exists but both breadth and BTC oppose it".into()],
            )
        } else if reversal_hits > 0 {
            (
                "waiting_for_volume_confirmation",
                Verdict::Block,
                vec!["shock reversed but volume expansion is below threshold".into()],
            )
        } else if move_hits > 0 {
            (
                "waiting_for_reversal_confirmation",
                Verdict::Block,
                vec!["large move exists but the latest candle has not reversed enough".into()],
            )
        } else {
            (
                "scanning_for_shock",
                Verdict::Block,
                vec!["no altcoin exceeds the one-hour shock threshold".into()],
            )
        };
        let mut out = vec![self.status(ctx, state, observed_side, verdict, reasons, metrics)];
        out.extend(candidates);
        Ok(out)
    }
}
