use super::aligned;
use crate::primitives::closed_bars;
use greed_kernel::{
    Artifact, ArtifactRecord, NodeContext, Side, StrategyNode, TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct MajorTrendPullbackNode {
    id: String,
    symbol: String,
    pullback_min_pct: f64,
    require_premium: bool,
    dependencies: Vec<String>,
}
impl MajorTrendPullbackNode {
    pub fn new(symbol: impl Into<String>, pullback_min_pct: f64, require_premium: bool) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.recipe.trend_pullback"),
            dependencies: vec![
                format!("{symbol}.trend_regime"),
                format!("{symbol}.spot_perp_cvd"),
                format!("{symbol}.leverage_regime"),
                format!("{symbol}.liquidity_regime"),
                format!("{symbol}.coinbase_premium"),
            ],
            symbol,
            pullback_min_pct,
            require_premium,
        }
    }
}
impl StrategyNode for MajorTrendPullbackNode {
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
        let trend = ctx
            .artifact(&format!("{}.trend", self.symbol))
            .and_then(|a| a.state())
            .ok_or("trend artifact missing")?;
        let Some(side) = trend.side else {
            return Ok(vec![]);
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
        let premium = ctx
            .artifact(&format!("{}.premium", self.symbol))
            .and_then(|a| a.state())
            .ok_or("premium artifact missing")?;
        let current = bars[bars.len() - 1];
        let previous = bars[bars.len() - 2];
        let recent = &bars[bars.len() - 9..];
        let high = recent
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max);
        let low = recent
            .iter()
            .map(|bar| bar.low)
            .fold(f64::INFINITY, f64::min);
        let pullback = match side {
            Side::Buy => high / current.close - 1.0,
            Side::Sell => current.close / low - 1.0,
        };
        let reclaim = match side {
            Side::Buy => current.close > previous.close,
            Side::Sell => current.close < previous.close,
        };
        let required = if self.require_premium {
            vec![trend, flow, leverage, liquidity, premium]
        } else {
            vec![trend, flow, leverage, liquidity]
        };
        let (mut verdict, mut blockers) = aligned(&required, side);
        if pullback < self.pullback_min_pct {
            blockers.push(format!("pullback {:.3}% below threshold", pullback * 100.0));
            verdict = Verdict::Block;
        }
        if !reclaim {
            blockers.push("latest closed candle has not reclaimed in trend direction".into());
            verdict = Verdict::Block;
        }
        let signal_ms = current.close_ms;
        let candidate = TradeCandidate {
            id: format!("{}:{}:{}", self.id, self.symbol, signal_ms),
            recipe: "major_trend_pullback".into(),
            symbol: self.symbol.clone(),
            side,
            signal_ms,
            expires_ms: signal_ms + 15 * 60_000,
            reference_price: current.close,
            score: trend.score + flow.score + pullback,
            confidence: [
                trend.meta.confidence,
                flow.meta.confidence,
                leverage.meta.confidence,
                liquidity.meta.confidence,
                premium.meta.confidence,
            ]
            .into_iter()
            .fold(1.0, f64::min),
            verdict,
            blockers,
            evidence: vec![
                format!("{}.trend", self.symbol),
                format!("{}.flow", self.symbol),
                format!("{}.leverage", self.symbol),
                format!("{}.liquidity", self.symbol),
                format!("{}.premium", self.symbol),
            ],
            tags: BTreeMap::from([
                ("pullback_pct".into(), format!("{pullback:.8}")),
                ("stop_profile".into(), "major".into()),
            ]),
        };
        Ok(vec![ArtifactRecord {
            key: format!("candidate.{}", candidate.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(candidate),
        }])
    }
}
