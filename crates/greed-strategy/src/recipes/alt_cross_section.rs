use crate::primitives::closed_bars;
use greed_kernel::{
    Artifact, ArtifactRecord, AssetClass, NodeContext, Side, StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct AltCrossSectionNode {
    id: String,
    names: usize,
    horizon_bars: usize,
    rebalance_bars: usize,
    anchor_symbol: String,
    dependencies: Vec<String>,
}
impl AltCrossSectionNode {
    pub fn new(
        names: usize,
        horizon_bars: usize,
        rebalance_bars: usize,
        anchor_symbol: &str,
        symbols: &[String],
    ) -> Self {
        Self {
            id: "alt.recipe.cross_section".into(),
            names,
            horizon_bars,
            rebalance_bars,
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
            .and_then(|a| a.state())
            .ok_or("breadth artifact missing")?;
        let bar_number = (ctx.frame.as_of_ms + 1) / (15 * 60_000);
        if bar_number % self.rebalance_bars as i64 != 0 {
            return Ok(vec![]);
        }
        let Some(side) = breadth.side else {
            return Ok(vec![]);
        };
        let anchor = ctx
            .artifact(&format!("{}.trend", self.anchor_symbol))
            .and_then(|artifact| artifact.state())
            .ok_or("anchor trend artifact missing")?;
        if anchor.verdict != Verdict::Pass || anchor.side != Some(side) {
            return Ok(vec![]);
        }
        let mut ranks = Vec::new();
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
                let value = bars.last().unwrap().close
                    / bars[bars.len() - self.horizon_bars - 1].close
                    - 1.0;
                ranks.push((value, instrument));
            }
        }
        ranks.sort_by(|a, b| a.0.total_cmp(&b.0));
        let selected: Vec<_> = match side {
            Side::Buy => ranks.iter().rev().take(self.names).collect(),
            Side::Sell => ranks.iter().take(self.names).collect(),
        };
        let mut out = Vec::new();
        for (rank, (return_pct, instrument)) in selected.into_iter().enumerate() {
            let bars = closed_bars(&instrument.perpetual);
            let signal_ms = bars.last().unwrap().close_ms;
            let mut blockers = Vec::new();
            if breadth.verdict != Verdict::Pass {
                blockers.push("market breadth is neutral".into());
            }
            if (side == Side::Buy && *return_pct <= 0.0)
                || (side == Side::Sell && *return_pct >= 0.0)
            {
                blockers.push("ranked symbol is not moving in market direction".into());
            }
            let verdict = if blockers.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Block
            };
            let candidate = TradeCandidate {
                id: format!("{}:{}:{}", self.id, instrument.symbol, signal_ms),
                recipe: "alt_cross_section_momentum".into(),
                symbol: instrument.symbol.clone(),
                side,
                signal_ms,
                expires_ms: signal_ms + 15 * 60_000,
                reference_price: instrument.price,
                score: breadth.score + return_pct.abs(),
                confidence: breadth.meta.confidence,
                verdict,
                blockers,
                evidence: vec![
                    "alt.breadth".into(),
                    format!("{}.relative_strength", instrument.symbol),
                ],
                tags: BTreeMap::from([
                    ("rank".into(), (rank + 1).to_string()),
                    ("return_pct".into(), format!("{return_pct:.8}")),
                    ("stop_profile".into(), "alt_cross".into()),
                    ("hold_profile".into(), "alt_cross".into()),
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
