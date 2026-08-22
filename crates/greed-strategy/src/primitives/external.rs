use super::meta;
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, Side, StateArtifact, StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct CoinbasePremiumNode {
    id: String,
    symbol: String,
    dependencies: Vec<String>,
}

pub struct EtfFlowNode {
    id: String,
    symbol: String,
    dependencies: Vec<String>,
}

impl EtfFlowNode {
    pub fn new(symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.etf_flow"),
            symbol,
            dependencies: vec![],
        }
    }
}

impl StrategyNode for EtfFlowNode {
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
        let external = instrument.external.as_ref();
        let rolling = external.and_then(|value| value.etf_rolling_5d_flow_usd);
        let (state, side, verdict) = match rolling {
            Some(value) if value > 0.0 => ("inflow", Some(Side::Buy), Verdict::Pass),
            Some(value) if value < 0.0 => ("outflow", Some(Side::Sell), Verdict::Pass),
            Some(_) => ("neutral", None, Verdict::Block),
            None => ("unavailable", None, Verdict::Unknown),
        };
        let mut metrics = BTreeMap::new();
        if let Some(value) = external.and_then(|value| value.etf_daily_flow_usd) {
            metrics.insert("daily_flow_usd".into(), value);
        }
        if let Some(value) = rolling {
            metrics.insert("rolling_5d_flow_usd".into(), value);
        }
        Ok(vec![ArtifactRecord {
            key: format!("{}.etf", self.symbol),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: rolling.unwrap_or(0.0).abs(),
                side,
                verdict,
                reasons: if verdict == Verdict::Unknown {
                    vec!["confirmed daily ETF flow unavailable or stale".into()]
                } else {
                    vec![]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    26 * 3_600_000,
                    external
                        .map(|value| value.meta.quality)
                        .unwrap_or(DataQuality::Missing),
                    if rolling.is_some() { 0.8 } else { 0.0 },
                    vec!["confirmed_etf_daily_flow".into()],
                ),
            }),
        }])
    }
}
impl CoinbasePremiumNode {
    pub fn new(symbol: impl Into<String>) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.coinbase_premium"),
            symbol,
            dependencies: vec![],
        }
    }
}
impl StrategyNode for CoinbasePremiumNode {
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
        let external = instrument.external.as_ref();
        let premium = external.and_then(|e| e.coinbase_true_premium_pct);
        let (state, side, verdict) = match premium {
            Some(v) if v > 0.0002 => ("positive", Some(Side::Buy), Verdict::Pass),
            Some(v) if v < -0.0002 => ("negative", Some(Side::Sell), Verdict::Pass),
            Some(_) => ("neutral", None, Verdict::Block),
            None => ("unavailable", None, Verdict::Unknown),
        };
        let mut metrics = BTreeMap::new();
        if let Some(v) = premium {
            metrics.insert("true_premium_pct".into(), v);
        }
        if let Some(v) = external.and_then(|e| e.coinbase_raw_premium_pct) {
            metrics.insert("raw_premium_pct".into(), v);
        }
        Ok(vec![ArtifactRecord {
            key: format!("{}.premium", self.symbol),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: state.into(),
                score: premium.unwrap_or(0.0).abs(),
                side,
                verdict,
                reasons: if verdict == Verdict::Unknown {
                    vec!["USDT/USD-normalized Coinbase premium unavailable".into()]
                } else {
                    vec![]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    60_000,
                    external
                        .map(|e| e.meta.quality)
                        .unwrap_or(DataQuality::Missing),
                    if premium.is_some() { 0.9 } else { 0.0 },
                    vec![
                        format!("{}.coinbase_btc_usd", self.symbol),
                        "usdt_usd".into(),
                    ],
                ),
            }),
        }])
    }
}
