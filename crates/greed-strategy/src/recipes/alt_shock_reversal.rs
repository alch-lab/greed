use crate::primitives::closed_bars;
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, NodeContext, Side, StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct AltShockReversalNode {
    id: String,
    min_return: f64,
    min_reversal: f64,
    min_volume_ratio: f64,
    anchor_symbol: String,
    dependencies: Vec<String>,
}
impl AltShockReversalNode {
    pub fn new(
        min_return: f64,
        min_reversal: f64,
        min_volume_ratio: f64,
        anchor_symbol: &str,
        symbols: &[String],
    ) -> Self {
        Self {
            id: "alt.recipe.shock_reversal".into(),
            min_return,
            min_reversal,
            min_volume_ratio,
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
            .and_then(|a| a.state())
            .ok_or("breadth artifact missing")?;
        let anchor = ctx
            .artifact(&format!("{}.trend", self.anchor_symbol))
            .and_then(|artifact| artifact.state())
            .ok_or("anchor trend artifact missing")?;
        let mut out = Vec::new();
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
            if bars.len() < 101 {
                continue;
            }
            let i = bars.len() - 1;
            let prior = bars[i - 1].close / bars[i - 5].close - 1.0;
            if prior.abs() < self.min_return {
                continue;
            }
            let side = if prior > 0.0 { Side::Sell } else { Side::Buy };
            let reversal = bars[i].close / bars[i - 1].close - 1.0;
            if side.sign() * reversal < self.min_reversal {
                continue;
            }
            let baseline: f64 = bars[i - 96..i].iter().map(|b| b.quote_volume).sum::<f64>() / 96.0;
            let volume_ratio = bars[i].quote_volume / baseline.max(1.0);
            if volume_ratio < self.min_volume_ratio {
                continue;
            }
            let mut blockers = Vec::new();
            if breadth.verdict != Verdict::Pass
                || breadth.side != Some(side)
                || anchor.verdict != Verdict::Pass
                || anchor.side != Some(side)
            {
                blockers.push("shock direction conflicts with breadth or BTC trend".into());
            }
            let signal_ms = bars[i].close_ms;
            let candidate = TradeCandidate {
                id: format!("{}:{}:{}", self.id, instrument.symbol, signal_ms),
                recipe: "alt_shock_reversal".into(),
                symbol: instrument.symbol.clone(),
                side,
                signal_ms,
                expires_ms: signal_ms + 15 * 60_000,
                reference_price: instrument.price,
                score: prior.abs() * reversal.abs() * volume_ratio.ln_1p(),
                confidence: breadth.meta.confidence,
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
                ],
                tags: BTreeMap::from([
                    ("prior_return_pct".into(), format!("{prior:.8}")),
                    ("reversal_pct".into(), format!("{reversal:.8}")),
                    ("volume_ratio".into(), format!("{volume_ratio:.4}")),
                    ("stop_profile".into(), "alt_shock".into()),
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
