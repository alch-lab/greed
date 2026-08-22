use super::meta;
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, Side, StateArtifact, StrategyNode, Verdict,
};
use std::collections::BTreeMap;

pub struct LiquidityRegimeNode {
    id: String,
    symbol: String,
    max_spread_bps: f64,
    min_depth_usd: f64,
    dependencies: Vec<String>,
}
impl LiquidityRegimeNode {
    pub fn new(symbol: impl Into<String>, max_spread_bps: f64, min_depth_usd: f64) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.liquidity_regime"),
            symbol,
            max_spread_bps,
            min_depth_usd,
            dependencies: vec![],
        }
    }
}
impl StrategyNode for LiquidityRegimeNode {
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
        let Some(book) = &instrument.book else {
            return Ok(vec![ArtifactRecord {
                key: format!("{}.liquidity", self.symbol),
                producer: self.id.clone(),
                artifact: Artifact::State(StateArtifact {
                    state: "unavailable".into(),
                    score: 0.0,
                    side: None,
                    verdict: Verdict::Unknown,
                    reasons: vec!["order book unavailable".into()],
                    metrics: BTreeMap::new(),
                    meta: meta(
                        ctx.frame.as_of_ms,
                        30_000,
                        DataQuality::Missing,
                        0.0,
                        vec![format!("{}.book", self.symbol)],
                    ),
                }),
            }]);
        };
        let mid = (book.bid + book.ask) / 2.0;
        let spread = if mid > 0.0 {
            (book.ask - book.bid) / mid * 10_000.0
        } else {
            f64::INFINITY
        };
        let depth = book.bid_depth_usd.min(book.ask_depth_usd);
        let pass = book.meta.usable_at(ctx.frame.as_of_ms)
            && spread <= self.max_spread_bps
            && depth >= self.min_depth_usd;
        let mut metrics = BTreeMap::new();
        metrics.insert("spread_bps".into(), spread);
        metrics.insert("two_sided_depth_usd".into(), depth);
        Ok(vec![ArtifactRecord {
            key: format!("{}.liquidity", self.symbol),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if pass { "liquid" } else { "illiquid" }.into(),
                score: (depth / self.min_depth_usd).min(3.0) / (1.0 + spread),
                side: None,
                verdict: if pass { Verdict::Pass } else { Verdict::Block },
                reasons: if pass {
                    vec![]
                } else {
                    vec!["spread, depth or freshness outside execution limits".into()]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    30_000,
                    book.meta.quality,
                    if pass { 1.0 } else { 0.3 },
                    vec![format!("{}.book", self.symbol)],
                ),
            }),
        }])
    }
}

#[derive(Default)]
struct WallMemory {
    bid_price: f64,
    ask_price: f64,
    bid_polls: u32,
    ask_polls: u32,
}

pub struct OrderWallNode {
    id: String,
    symbol: String,
    min_notional: f64,
    min_polls: u32,
    memory: WallMemory,
    dependencies: Vec<String>,
}

impl OrderWallNode {
    pub fn new(symbol: impl Into<String>, min_notional: f64, min_polls: u32) -> Self {
        let symbol = symbol.into();
        Self {
            id: format!("{symbol}.order_wall"),
            symbol,
            min_notional,
            min_polls,
            memory: WallMemory::default(),
            dependencies: vec![],
        }
    }
}

impl StrategyNode for OrderWallNode {
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
        let Some(book) = instrument.book.as_ref() else {
            return Ok(vec![]);
        };
        let strongest = |levels: &[greed_kernel::PriceLevel]| {
            levels
                .iter()
                .map(|level| (level.price, level.price * level.quantity))
                .max_by(|a, b| a.1.total_cmp(&b.1))
        };
        let bid = strongest(&book.bids);
        let ask = strongest(&book.asks);
        let update = |previous: f64, polls: &mut u32, current: Option<(f64, f64)>| -> f64 {
            let Some((price, notional)) = current else {
                *polls = 0;
                return 0.0;
            };
            if notional < self.min_notional {
                *polls = 0;
                return price;
            }
            if previous > 0.0 && (price / previous - 1.0).abs() <= 0.0005 {
                *polls += 1
            } else {
                *polls = 1
            }
            price
        };
        self.memory.bid_price = update(self.memory.bid_price, &mut self.memory.bid_polls, bid);
        self.memory.ask_price = update(self.memory.ask_price, &mut self.memory.ask_polls, ask);
        let bid_active = self.memory.bid_polls >= self.min_polls;
        let ask_active = self.memory.ask_polls >= self.min_polls;
        let mut metrics = BTreeMap::new();
        if let Some((price, notional)) = bid {
            metrics.insert("bid_wall_price".into(), price);
            metrics.insert("bid_wall_notional_usd".into(), notional);
        }
        if let Some((price, notional)) = ask {
            metrics.insert("ask_wall_price".into(), price);
            metrics.insert("ask_wall_notional_usd".into(), notional);
        }
        metrics.insert("bid_persistence_polls".into(), self.memory.bid_polls as f64);
        metrics.insert("ask_persistence_polls".into(), self.memory.ask_polls as f64);
        let side = match (bid_active, ask_active) {
            (true, false) => Some(Side::Buy),
            (false, true) => Some(Side::Sell),
            _ => None,
        };
        Ok(vec![ArtifactRecord {
            key: format!("{}.wall", self.symbol),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: match (bid_active, ask_active) {
                    (true, true) => "two_sided_walls",
                    (true, false) => "bid_wall",
                    (false, true) => "ask_wall",
                    _ => "unconfirmed",
                }
                .into(),
                score: (self.memory.bid_polls.max(self.memory.ask_polls) as f64
                    / self.min_polls.max(1) as f64)
                    .min(2.0),
                side,
                verdict: if bid_active || ask_active {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if bid_active || ask_active {
                    vec![]
                } else {
                    vec!["large level has not persisted for the required polls".into()]
                },
                metrics,
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    book.meta.quality,
                    if bid_active || ask_active { 0.8 } else { 0.3 },
                    vec![format!("{}.book.levels", self.symbol)],
                ),
            }),
        }])
    }
}
