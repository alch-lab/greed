use crate::primitives::closed_bars;
use greed_kernel::{
    Artifact, ArtifactRecord, NodeContext, Side, StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct MajorExhaustionNode {
    id: String,
    symbol: String,
    min_move: f64,
    dependencies: Vec<String>,
}
impl MajorExhaustionNode {
    pub fn new(symbol: impl Into<String>, min_move: f64) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.recipe.exhaustion"),
            dependencies: vec![
                format!("{symbol}.trend_regime"),
                format!("{symbol}.spot_perp_cvd"),
                format!("{symbol}.leverage_regime"),
                format!("{symbol}.liquidity_regime"),
            ],
            symbol,
            min_move,
        }
    }
}
impl StrategyNode for MajorExhaustionNode {
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
        if bars.len() < 9 {
            return Ok(vec![]);
        }
        let current = bars[bars.len() - 1];
        let move_pct = current.close / bars[bars.len() - 9].close - 1.0;
        if move_pct.abs() < self.min_move {
            return Ok(vec![]);
        }
        let side = if move_pct > 0.0 {
            Side::Sell
        } else {
            Side::Buy
        };
        let flow = ctx
            .artifact(&format!("{}.flow", self.symbol))
            .and_then(|a| a.state())
            .ok_or("flow artifact missing")?;
        let leverage = ctx
            .artifact(&format!("{}.leverage", self.symbol))
            .and_then(|a| a.state())
            .ok_or("leverage artifact missing")?;
        let liquidity = ctx
            .artifact(&format!("{}.liquidity", self.symbol))
            .and_then(|a| a.state())
            .ok_or("liquidity artifact missing")?;
        let expected_leverage = matches!(
            (side, leverage.state.as_str()),
            (Side::Sell, "short_cover") | (Side::Buy, "long_liquidation")
        );
        let flow_reversal = flow.side == Some(side) && flow.verdict == Verdict::Pass;
        let mut blockers = Vec::new();
        if !flow_reversal {
            blockers.push("cross-market CVD has not reversed".into());
        }
        if !expected_leverage {
            blockers.push("OI does not show a completed deleveraging move".into());
        }
        if liquidity.verdict != Verdict::Pass {
            blockers.push("liquidity gate closed".into());
        }
        let verdict = if blockers.is_empty() {
            Verdict::Pass
        } else if [flow.verdict, leverage.verdict, liquidity.verdict].contains(&Verdict::Unknown) {
            Verdict::Unknown
        } else {
            Verdict::Block
        };
        let signal_ms = current.close_ms;
        let candidate = TradeCandidate {
            id: format!("{}:{}:{}", self.id, self.symbol, signal_ms),
            recipe: "major_exhaustion_reversal".into(),
            symbol: self.symbol.clone(),
            side,
            signal_ms,
            expires_ms: signal_ms + 15 * 60_000,
            reference_price: current.close,
            score: move_pct.abs() + flow.score + leverage.score,
            confidence: flow
                .meta
                .confidence
                .min(leverage.meta.confidence)
                .min(liquidity.meta.confidence),
            verdict,
            blockers,
            evidence: vec![
                format!("{}.flow", self.symbol),
                format!("{}.leverage", self.symbol),
                format!("{}.liquidity", self.symbol),
            ],
            tags: BTreeMap::from([
                ("move_pct".into(), format!("{move_pct:.8}")),
                ("stop_profile".into(), "exhaustion".into()),
            ]),
        };
        Ok(vec![ArtifactRecord {
            key: format!("candidate.{}", candidate.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(candidate),
        }])
    }
}
