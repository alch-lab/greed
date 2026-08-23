use crate::primitives::{closed_bars, meta};
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, DataQuality, NodeContext, Side, StateArtifact,
    StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

const BAR_MS: i64 = 15 * 60_000;

pub struct AltCrossSectionNode {
    id: String,
    names: usize,
    horizon_bars: usize,
    rebalance_bars: usize,
    opportunity_driven: bool,
    neutral_anchor_allowed: bool,
    anchor_symbol: String,
    dependencies: Vec<String>,
}

impl AltCrossSectionNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        names: usize,
        horizon_bars: usize,
        rebalance_bars: usize,
        opportunity_driven: bool,
        neutral_anchor_allowed: bool,
        anchor_symbol: &str,
        symbols: &[String],
    ) -> Self {
        Self {
            id: "alt.recipe.cross_section".into(),
            names,
            horizon_bars,
            rebalance_bars,
            opportunity_driven,
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
            key: "alt.cross_section".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: metrics.get("breadth_score").copied().unwrap_or(0.0),
                side,
                verdict,
                reasons,
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "alt.breadth".into(),
                        format!("{}.trend", self.anchor_symbol),
                    ],
                ),
            }),
        }
    }
}

impl StrategyNode for AltCrossSectionNode {
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
        let bar_number = (ctx.frame.as_of_ms + 1) / BAR_MS;
        let cycle = bar_number.div_euclid(self.rebalance_bars as i64);
        let fixed_window_due = bar_number % self.rebalance_bars as i64 == 0;
        let mut metrics = BTreeMap::from([
            ("breadth_score".into(), breadth.score),
            ("cycle".into(), cycle as f64),
            ("cooldown_bars".into(), self.rebalance_bars as f64),
            (
                "opportunity_driven".into(),
                if self.opportunity_driven { 1.0 } else { 0.0 },
            ),
            (
                "fixed_window_due".into(),
                if fixed_window_due { 1.0 } else { 0.0 },
            ),
        ]);

        let Some(side) = breadth.side else {
            return Ok(vec![self.status(
                ctx,
                "waiting_for_market_breadth",
                None,
                breadth.verdict,
                vec!["market breadth has no actionable direction".into()],
                metrics,
            )]);
        };
        if !self.opportunity_driven && !fixed_window_due {
            return Ok(vec![self.status(
                ctx,
                "waiting_for_fixed_rebalance",
                Some(side),
                Verdict::Block,
                vec!["fixed rebalance window is not due".into()],
                metrics,
            )]);
        }

        let anchor_conflict = anchor.verdict == Verdict::Pass
            && anchor.side.is_some_and(|anchor_side| anchor_side != side);
        let anchor_neutral = anchor.verdict != Verdict::Pass || anchor.side.is_none();
        metrics.insert(
            "anchor_conflict".into(),
            if anchor_conflict { 1.0 } else { 0.0 },
        );
        metrics.insert(
            "anchor_neutral".into(),
            if anchor_neutral { 1.0 } else { 0.0 },
        );
        let neutral_short = anchor_neutral && side == Side::Sell;
        metrics.insert(
            "neutral_short".into(),
            if neutral_short { 1.0 } else { 0.0 },
        );
        if anchor_conflict || (anchor_neutral && (!self.neutral_anchor_allowed || neutral_short)) {
            let reason = if anchor_conflict {
                "BTC trend explicitly opposes altcoin market breadth"
            } else if neutral_short && self.neutral_anchor_allowed {
                "BTC is neutral; exploratory altcoin shorts require explicit BTC confirmation"
            } else {
                "BTC trend is neutral and strict anchor confirmation is enabled"
            };
            return Ok(vec![self.status(
                ctx,
                "blocked_by_btc_anchor",
                Some(side),
                Verdict::Block,
                vec![reason.into()],
                metrics,
            )]);
        }

        let mut ranks = Vec::new();
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
            if bars.len() > self.horizon_bars {
                let return_pct = bars.last().expect("non-empty bars").close
                    / bars[bars.len() - self.horizon_bars - 1].close
                    - 1.0;
                ranks.push((return_pct, instrument));
            }
        }
        ranks.sort_by(|a, b| a.0.total_cmp(&b.0));
        metrics.insert("eligible_ranked_symbols".into(), ranks.len() as f64);
        if ranks.is_empty() {
            return Ok(vec![self.status(
                ctx,
                "waiting_for_eligible_symbols",
                Some(side),
                Verdict::Unknown,
                vec!["no eligible altcoin has enough closed candle history".into()],
                metrics,
            )]);
        }

        let selected: Vec<_> = match side {
            Side::Buy => ranks.iter().rev().take(self.names).collect(),
            Side::Sell => ranks.iter().take(self.names).collect(),
        };
        metrics.insert("selected_symbols".into(), selected.len() as f64);
        let confidence = breadth.meta.confidence * if anchor_neutral { 0.8 } else { 1.0 };
        let mut out = vec![self.status(
            ctx,
            if anchor_neutral {
                "opportunity_ready_breadth_only"
            } else {
                "opportunity_ready_confirmed"
            },
            Some(side),
            Verdict::Pass,
            if anchor_neutral {
                vec!["BTC is neutral; strong altcoin breadth is allowed at reduced size".into()]
            } else {
                vec![]
            },
            metrics,
        )];
        for (rank, (return_pct, instrument)) in selected.into_iter().enumerate() {
            let bars = closed_bars(&instrument.perpetual);
            let signal_ms = bars.last().expect("ranked instrument has bars").close_ms;
            let mut blockers = Vec::new();
            if (side == Side::Buy && *return_pct <= 0.0)
                || (side == Side::Sell && *return_pct >= 0.0)
            {
                blockers.push("ranked symbol is not moving in market direction".into());
            }
            let candidate_key = if self.opportunity_driven {
                format!(
                    "cycle-{cycle}-{}",
                    if anchor_neutral {
                        "neutral"
                    } else {
                        "confirmed"
                    }
                )
            } else {
                signal_ms.to_string()
            };
            let candidate = TradeCandidate {
                id: format!("{}:{}:{candidate_key}", self.id, instrument.symbol),
                recipe: if anchor_neutral {
                    "alt_cross_section_probe"
                } else {
                    "alt_cross_section_momentum"
                }
                .into(),
                symbol: instrument.symbol.clone(),
                side,
                signal_ms,
                expires_ms: signal_ms + BAR_MS,
                reference_price: instrument.price,
                score: breadth.score + return_pct.abs(),
                confidence,
                verdict: if blockers.is_empty() {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                blockers,
                evidence: vec![
                    "alt.breadth".into(),
                    format!("{}.relative_strength", instrument.symbol),
                    format!("{}.trend", self.anchor_symbol),
                ],
                tags: BTreeMap::from([
                    ("rank".into(), (rank + 1).to_string()),
                    ("return_pct".into(), format!("{return_pct:.8}")),
                    ("stop_profile".into(), "alt_cross".into()),
                    ("hold_profile".into(), "alt_cross".into()),
                    (
                        "anchor_confirmation".into(),
                        if anchor_neutral {
                            "neutral"
                        } else {
                            "confirmed"
                        }
                        .into(),
                    ),
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
