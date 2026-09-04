use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

const HOUR_MS: i64 = 3_600_000;

#[derive(Debug, Clone, Copy)]
struct Zone {
    center: f64,
    touches: usize,
    low: f64,
    high: f64,
}

pub struct BtcKeyZoneNode {
    id: String,
    config: LaneConfig,
}

impl BtcKeyZoneNode {
    pub fn new(config: LaneConfig) -> Self {
        Self {
            id: "lane.btc_key_zone".into(),
            config,
        }
    }
}

fn true_range(values: &[&Candle], index: usize) -> f64 {
    let previous = if index == 0 {
        values[index].close
    } else {
        values[index - 1].close
    };
    (values[index].high - values[index].low)
        .max((values[index].high - previous).abs())
        .max((values[index].low - previous).abs())
}

fn atr(values: &[&Candle], period: usize) -> f64 {
    let start = values.len().saturating_sub(period);
    (start..values.len())
        .map(|index| true_range(values, index))
        .sum::<f64>()
        / values.len().saturating_sub(start).max(1) as f64
}

fn pivot_prices(values: &[&Candle], radius: usize, resistance: bool) -> Vec<f64> {
    if values.len() < radius * 2 + 1 {
        return vec![];
    }
    (radius..values.len() - radius)
        .filter_map(|index| {
            let price = if resistance {
                values[index].high
            } else {
                values[index].low
            };
            let window = &values[index - radius..=index + radius];
            let pivot = if resistance {
                window.iter().all(|value| value.high <= price)
            } else {
                window.iter().all(|value| value.low >= price)
            };
            pivot.then_some(price)
        })
        .collect()
}

fn cluster_zones(mut prices: Vec<f64>, tolerance: f64, min_touches: usize) -> Vec<Zone> {
    prices.retain(|value| value.is_finite() && *value > 0.0);
    prices.sort_by(f64::total_cmp);
    let mut groups: Vec<Vec<f64>> = Vec::new();
    for price in prices {
        if let Some(group) = groups.last_mut() {
            let center = group.iter().sum::<f64>() / group.len() as f64;
            if (price - center).abs() <= tolerance {
                group.push(price);
                continue;
            }
        }
        groups.push(vec![price]);
    }
    groups
        .into_iter()
        .filter(|group| group.len() >= min_touches)
        .map(|group| {
            let center = group.iter().sum::<f64>() / group.len() as f64;
            Zone {
                center,
                touches: group.len(),
                low: center - tolerance * 0.5,
                high: center + tolerance * 0.5,
            }
        })
        .collect()
}

impl StrategyNode for BtcKeyZoneNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let Some(instrument) = ctx.frame.instrument("BTCUSDT") else {
            return Ok(vec![]);
        };
        let Some(hourly) = instrument.hourly_perpetual.as_ref() else {
            return Ok(vec![]);
        };
        let mut closed: Vec<_> = hourly.values.iter().filter(|value| value.closed).collect();
        if closed.len() < self.config.btc_key_zone_lookback_hours.min(168) {
            return Ok(vec![]);
        }
        if closed.len() > self.config.btc_key_zone_lookback_hours {
            closed = closed[closed.len() - self.config.btc_key_zone_lookback_hours..].to_vec();
        }
        let current = instrument.price;
        let atr = atr(&closed, 24).max(current * 0.001);
        let anchor = closed
            .iter()
            .rev()
            .take(72)
            .map(|value| value.close)
            .sum::<f64>()
            / closed.len().min(72) as f64;
        let signal = *closed.last().expect("closed history is non-empty");
        let tolerance = atr * self.config.btc_key_zone_cluster_atr;
        let resistance = cluster_zones(
            pivot_prices(&closed, self.config.btc_key_zone_pivot_bars, true),
            tolerance,
            self.config.btc_key_zone_min_touches,
        )
        .into_iter()
        .filter(|zone| zone.high > current)
        .min_by(|left, right| {
            (left.low - current)
                .max(0.0)
                .total_cmp(&(right.low - current).max(0.0))
        });
        let support = cluster_zones(
            pivot_prices(&closed, self.config.btc_key_zone_pivot_bars, false),
            tolerance,
            self.config.btc_key_zone_min_touches,
        )
        .into_iter()
        .filter(|zone| zone.low < current)
        .min_by(|left, right| {
            (current - left.high)
                .max(0.0)
                .total_cmp(&(current - right.high).max(0.0))
        });

        let mut candidates = Vec::new();
        for (side, zone) in [(Side::Sell, resistance), (Side::Buy, support)] {
            let Some(zone) = zone else { continue };
            let zone_entry = match side {
                Side::Sell => zone.low,
                Side::Buy => zone.high,
            };
            let distance_atr = (zone_entry - current).abs() / atr;
            let inside_zone = (zone.low..=zone.high).contains(&current);
            let entry = instrument.book.as_ref().map_or(current, |book| {
                if side == Side::Buy {
                    book.bid
                } else {
                    book.ask
                }
            });
            let invalidation = match side {
                Side::Sell => zone.high + self.config.btc_key_zone_invalidation_atr * atr,
                Side::Buy => zone.low - self.config.btc_key_zone_invalidation_atr * atr,
            };
            let stop_pct = (invalidation / entry - 1.0).abs();
            let extension_atr = match side {
                Side::Sell => (signal.close - anchor) / atr,
                Side::Buy => (anchor - signal.close) / atr,
            };
            let body = (signal.close - signal.open).abs().max(current * 0.0002);
            let rejection_wick_body = match side {
                Side::Sell => (signal.high - signal.open.max(signal.close)) / body,
                Side::Buy => (signal.open.min(signal.close) - signal.low) / body,
            };
            let weak_close = match side {
                Side::Sell => signal.close <= (signal.high + signal.low) * 0.5,
                Side::Buy => signal.close >= (signal.high + signal.low) * 0.5,
            };
            let mut blockers = Vec::new();
            if !inside_zone {
                blockers.push(format!(
                    "waiting for BTC to enter {:.2}-{:.2}",
                    zone.low, zone.high
                ));
            }
            if distance_atr > self.config.btc_key_zone_max_distance_atr {
                blockers.push(format!(
                    "nearest structural zone is {distance_atr:.2} ATR away / max {:.2}",
                    self.config.btc_key_zone_max_distance_atr
                ));
            }
            if extension_atr < self.config.btc_key_zone_min_extension_atr {
                blockers.push(format!(
                    "extension {extension_atr:.2} ATR / need {:.2} ATR into the zone",
                    self.config.btc_key_zone_min_extension_atr
                ));
            }
            if rejection_wick_body < self.config.btc_key_zone_min_rejection_wick_body || !weak_close
            {
                blockers.push(format!(
                    "waiting for a closed 1h rejection (wick/body {rejection_wick_body:.2})"
                ));
            }
            if !(0.003..=0.03).contains(&stop_pct) {
                blockers.push(format!(
                    "zone invalidation distance {:.2}% is outside 0.30%-3.00%",
                    stop_pct * 100.0
                ));
            }
            match instrument.book.as_ref() {
                None => blockers.push("order book is not ready".into()),
                Some(book) if !book.meta.usable_at(ctx.frame.as_of_ms) => {
                    blockers.push("order book is stale".into())
                }
                Some(book) => {
                    let spread = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    if spread > self.config.max_spread_bps {
                        blockers.push(format!(
                            "spread {spread:.1} bps / max {:.1} bps",
                            self.config.max_spread_bps
                        ));
                    }
                }
            }
            let verdict = if blockers.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Block
            };
            let score = zone.touches as f64 / (1.0 + distance_atr);
            let candidate = TradeCandidate {
                id: format!(
                    "btc_key_zone:{}:{:.0}:{}",
                    if side == Side::Buy { "long" } else { "short" },
                    zone.center,
                    signal.close_ms / HOUR_MS
                ),
                recipe: "btc_key_zone".into(),
                symbol: "BTCUSDT".into(),
                side,
                signal_ms: signal.close_ms,
                expires_ms: signal.close_ms + 300_000,
                reference_price: entry,
                score,
                confidence: (0.60 + zone.touches as f64 * 0.05).min(0.90),
                verdict,
                blockers,
                evidence: vec![
                    "BTCUSDT.binance_1h_structural_pivots".into(),
                    "BTCUSDT.book".into(),
                ],
                tags: BTreeMap::from([
                    ("lane".into(), "btc_key_zone".into()),
                    ("priority".into(), "5".into()),
                    ("zone_center".into(), zone.center.to_string()),
                    ("zone_low".into(), zone.low.to_string()),
                    ("zone_high".into(), zone.high.to_string()),
                    ("zone_touches".into(), zone.touches.to_string()),
                    ("distance_atr".into(), distance_atr.to_string()),
                    ("extension_atr".into(), extension_atr.to_string()),
                    (
                        "rejection_wick_body".into(),
                        rejection_wick_body.to_string(),
                    ),
                    ("entry_limit".into(), entry.to_string()),
                    ("entry_timeout_ms".into(), "120000".into()),
                    ("taker_fallback".into(), "false".into()),
                    ("stop_pct".into(), stop_pct.to_string()),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.btc_key_zone_risk_per_trade_pct.to_string(),
                    ),
                    ("max_notional_multiple".into(), "2.0".into()),
                    (
                        "take_profit_ladder".into(),
                        "0.8:0.20,1.5:0.25,2.5:0.25,3.5:0.20".into(),
                    ),
                    ("unprotected_runner_fraction".into(), "0.10".into()),
                    ("max_hold_ms".into(), "0".into()),
                ]),
            };
            candidates.push(candidate);
        }

        candidates.sort_by(|left, right| right.score.total_cmp(&left.score));
        let actionable = candidates
            .iter()
            .any(|value| value.verdict == Verdict::Pass);
        let nearest = candidates.first();
        let mut out = vec![ArtifactRecord {
            key: "lane.btc_key_zone.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable {
                    "resting_near_structural_zone"
                } else {
                    "mapping_structural_zones"
                }
                .into(),
                score: nearest.map_or(0.0, |value| value.score),
                side: nearest.map(|value| value.side),
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: nearest.map_or_else(
                    || vec!["no repeated 1h structural zone is available".into()],
                    |value| value.blockers.clone(),
                ),
                metrics: BTreeMap::from([
                    ("hourly_bars".into(), closed.len() as f64),
                    ("atr_1h".into(), atr),
                    ("candidate_zones".into(), candidates.len() as f64),
                    (
                        "nearest_zone_center".into(),
                        nearest
                            .and_then(|value| value.tags.get("zone_center"))
                            .and_then(|value| value.parse().ok())
                            .unwrap_or_default(),
                    ),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "binance_1h_structural_pivots".into(),
                        "binance_ws_depth".into(),
                    ],
                ),
            }),
        }];
        out.extend(candidates.into_iter().map(|candidate| ArtifactRecord {
            key: format!("candidate.{}", candidate.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(candidate),
        }));
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clusters_repeated_levels_and_rejects_singletons() {
        let zones = cluster_zones(vec![100.0, 100.3, 105.0], 0.5, 2);
        assert_eq!(zones.len(), 1);
        assert_eq!(zones[0].touches, 2);
        assert!((zones[0].center - 100.15).abs() < 1e-9);
    }
}
