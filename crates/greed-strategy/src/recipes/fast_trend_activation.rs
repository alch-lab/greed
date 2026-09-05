use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

const FAST_TAKE_PROFIT_LADDER: &str = "1.0:0.30,2.0:0.40";
const FAST_PROFIT_SHIELD_ACTIVATION_R: &str = "0.5";
const FAST_TRAILING_ACTIVATION_R: &str = "1.0";
const FAST_TRAILING_DISTANCE_PCT: &str = "0.0035";

pub struct FastTrendActivationNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl FastTrendActivationNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.fast_trend_activation".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn is_major(symbol: &str) -> bool {
    ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"].contains(&symbol)
}

fn median(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut values: Vec<_> = values.filter(|value| value.is_finite()).collect();
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    match values.len() {
        0 => None,
        length if length % 2 == 0 => Some((values[middle - 1] + values[middle]) * 0.5),
        _ => Some(values[middle]),
    }
}

fn price_range(values: &[&Candle]) -> f64 {
    let high = values.iter().map(|bar| bar.high).fold(0.0, f64::max);
    let low = values
        .iter()
        .map(|bar| bar.low)
        .fold(f64::INFINITY, f64::min);
    high / low.max(f64::EPSILON) - 1.0
}

fn atr(values: &[&Candle], end: usize, length: usize) -> f64 {
    let start = end.saturating_sub(length.saturating_sub(1)).max(1);
    median((start..=end).map(|index| {
        let bar = values[index];
        (bar.high - bar.low)
            .max((bar.high - values[index - 1].close).abs())
            .max((bar.low - values[index - 1].close).abs())
    }))
    .unwrap_or_default()
}

fn oi_change(values: &[(i64, f64)], at_ms: i64, lookback_ms: i64) -> Option<f64> {
    let current = values.iter().rev().find(|(ts, _)| *ts <= at_ms)?.1;
    let prior = values
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= at_ms - lookback_ms)?
        .1;
    (prior > 0.0).then_some(current / prior - 1.0)
}

#[allow(clippy::too_many_arguments)]
fn is_ignition(
    breakout: bool,
    body_return: f64,
    volume_ratio: f64,
    flow: Option<f64>,
    compression_ratio: f64,
    prebreak_return_1h: f64,
    return_4h: f64,
    config: &LaneConfig,
) -> bool {
    breakout
        && body_return >= config.fast_min_body_return_5m
        && volume_ratio >= config.fast_min_volume_ratio_5m
        && flow.is_some_and(|value| value >= config.fast_min_flow_5m)
        && compression_ratio <= config.fast_max_compression_ratio
        && prebreak_return_1h <= config.fast_max_prebreak_return_1h
        && return_4h <= config.fast_max_return_4h
}

impl StrategyNode for FastTrendActivationNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let market_returns: Vec<_> = self
            .symbols
            .iter()
            .filter(|symbol| !is_major(symbol))
            .filter_map(|symbol| ctx.frame.instrument(symbol))
            .filter_map(|instrument| instrument.fast_perpetual.as_ref())
            .filter_map(|series| {
                let closed: Vec<_> = series.values.iter().filter(|bar| bar.closed).collect();
                (closed.len() >= 13).then(|| {
                    closed.last().expect("length checked").close / closed[closed.len() - 13].close
                        - 1.0
                })
            })
            .collect();
        let market_return_1h = median(market_returns.iter().copied()).unwrap_or_default();
        let market_breadth = if market_returns.is_empty() {
            0.0
        } else {
            market_returns.iter().filter(|value| **value > 0.0).count() as f64
                / market_returns.len() as f64
        };
        let market_ready = market_return_1h >= self.config.fast_min_market_return_1h
            && market_breadth >= self.config.fast_min_market_breadth;

        let mut passed = Vec::new();
        let mut observations = Vec::new();
        let mut inspected = 0_u64;
        let mut ignition_hits = 0_u64;
        let mut confirmation_hits = 0_u64;
        let mut oi_ready = 0_u64;

        for symbol in self.symbols.iter().filter(|symbol| !is_major(symbol)) {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let Some(fast) = instrument.fast_perpetual.as_ref() else {
                continue;
            };
            let closed: Vec<_> = fast.values.iter().filter(|bar| bar.closed).collect();
            if closed.len() < 90 {
                continue;
            }
            inspected += 1;
            let index = closed.len() - 1;
            let bar = closed[index];
            let prior = &closed[index - 12..index];
            let breakout_level = prior.iter().map(|value| value.high).fold(0.0, f64::max);
            let body_return = bar.close / bar.open.max(f64::EPSILON) - 1.0;
            let baseline_volume = median(
                closed[index - 36..index]
                    .iter()
                    .map(|value| value.quote_volume),
            )
            .unwrap_or_default();
            let volume_ratio = bar.quote_volume / baseline_volume.max(1.0);
            let flow = bar
                .taker_buy_quote
                .map(|buy| 2.0 * buy / bar.quote_volume.max(1.0) - 1.0);
            let current_range = price_range(prior);
            let compression_ratio = current_range
                / median((1..=6).map(|window| {
                    let end = index - 12 * window;
                    price_range(&closed[end - 12..end])
                }))
                .unwrap_or_default()
                .max(f64::EPSILON);
            let prebreak_return_1h = bar.open / closed[index - 12].open - 1.0;
            let return_4h = bar.close / closed[index - 48].close - 1.0;
            let ignition = is_ignition(
                bar.close > breakout_level,
                body_return,
                volume_ratio,
                flow,
                compression_ratio,
                prebreak_return_1h,
                return_4h,
                &self.config,
            );
            ignition_hits += u64::from(ignition);

            let confirmation = instrument
                .micro_perpetual
                .as_ref()
                .into_iter()
                .flat_map(|series| series.values.iter())
                .filter(|minute| {
                    minute.closed
                        && minute.close_ms > bar.close_ms
                        && minute.close_ms <= bar.close_ms + 3 * 60_000
                })
                .find(|minute| {
                    let minute_flow = minute
                        .taker_buy_quote
                        .map(|buy| 2.0 * buy / minute.quote_volume.max(1.0) - 1.0)
                        .unwrap_or(-1.0);
                    minute.low <= breakout_level * 1.0015
                        && minute.close >= breakout_level
                        && minute_flow >= 0.02
                });
            confirmation_hits += u64::from(ignition && confirmation.is_some());

            let oi_values: Vec<_> = instrument
                .open_interest
                .as_ref()
                .map(|series| {
                    series
                        .values
                        .iter()
                        .map(|value| (value.timestamp_ms, value.value_usd))
                        .collect()
                })
                .unwrap_or_default();
            let oi_15m = oi_change(&oi_values, bar.close_ms, 15 * 60_000);
            let oi_60m = oi_change(&oi_values, bar.close_ms, 60 * 60_000);
            let oi_ok = oi_15m
                .is_some_and(|value| (0.0..=self.config.fast_max_oi_change_15m).contains(&value))
                && oi_60m.is_some_and(|value| value >= 0.0);
            oi_ready += u64::from(oi_15m.is_some() && oi_60m.is_some());

            let mut blockers = Vec::new();
            if !market_ready {
                blockers.push(format!(
                    "altcoin market 1h median {:.2}% / breadth {:.0}% is below the risk-on gate",
                    market_return_1h * 100.0,
                    market_breadth * 100.0
                ));
            }
            if bar.close <= breakout_level {
                blockers.push("waiting for a completed 5m range breakout".into());
            }
            if body_return < self.config.fast_min_body_return_5m {
                blockers.push(format!(
                    "5m body {:.2}% is below activation",
                    body_return * 100.0
                ));
            }
            if volume_ratio < self.config.fast_min_volume_ratio_5m {
                blockers.push(format!("5m volume {volume_ratio:.2}x is below activation"));
            }
            match flow {
                Some(value) if value < self.config.fast_min_flow_5m => blockers.push(format!(
                    "5m taker pressure {:.0}% is below activation",
                    value * 100.0
                )),
                None => blockers.push("5m taker flow is warming".into()),
                _ => {}
            }
            if compression_ratio > self.config.fast_max_compression_ratio {
                blockers.push(format!(
                    "prior range {compression_ratio:.2}x is not compressed"
                ));
            }
            if prebreak_return_1h > self.config.fast_max_prebreak_return_1h
                || return_4h > self.config.fast_max_return_4h
            {
                blockers.push("move is already mature; the fast lane will not chase it".into());
            }
            if ignition && confirmation.is_none() {
                blockers.push("waiting up to 3m for the first 1m shallow reclaim".into());
            }
            if oi_15m.is_none() || oi_60m.is_none() {
                blockers.push("open-interest history is warming".into());
            } else if !oi_ok {
                blockers.push(format!(
                    "OI must rise moderately: 15m 0..{:.1}%, 60m non-negative",
                    self.config.fast_max_oi_change_15m * 100.0
                ));
            }

            match instrument.book.as_ref() {
                Some(book) if book.meta.usable_at(ctx.frame.as_of_ms) => {
                    let spread = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    if spread > self.config.max_spread_bps {
                        blockers.push(format!("spread {spread:.1} bps is too wide"));
                    }
                }
                _ => blockers.push("order book is warming".into()),
            }

            let signal_ms = confirmation.map_or(bar.close_ms, |value| value.close_ms);
            if ctx.frame.as_of_ms - signal_ms > 120_000 {
                blockers.push("the 1m confirmation expired".into());
            }
            let reference_price = confirmation.map_or(instrument.price, |value| value.close);
            let verdict = if blockers.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Block
            };
            let stop_pct =
                (1.25 * atr(&closed, index, 20) / reference_price.max(f64::EPSILON)).max(0.0035);
            let score = body_return.max(0.0)
                * volume_ratio
                * (1.0 + flow.unwrap_or_default().max(0.0))
                * (1.0 + market_return_1h * 10.0);
            let progress = (11_usize.saturating_sub(blockers.len()).min(11) as f64) / 11.0;
            let mut tags = BTreeMap::from([
                ("lane".into(), "fast_trend_activation".into()),
                ("priority".into(), "0.8".into()),
                ("entry_pattern".into(), "one_minute_shallow_reclaim".into()),
                ("market_return_1h".into(), market_return_1h.to_string()),
                ("market_breadth_1h".into(), market_breadth.to_string()),
                ("body_return_5m".into(), body_return.to_string()),
                ("volume_ratio_5m".into(), volume_ratio.to_string()),
                (
                    "directional_flow_5m".into(),
                    flow.unwrap_or_default().to_string(),
                ),
                ("compression_ratio".into(), compression_ratio.to_string()),
                ("prebreak_return_1h".into(), prebreak_return_1h.to_string()),
                ("return_4h".into(), return_4h.to_string()),
                (
                    "oi_change_15m".into(),
                    oi_15m.unwrap_or_default().to_string(),
                ),
                (
                    "oi_change_60m".into(),
                    oi_60m.unwrap_or_default().to_string(),
                ),
                ("stop_pct".into(), stop_pct.to_string()),
                ("take_profit_ladder".into(), FAST_TAKE_PROFIT_LADDER.into()),
                ("target_r".into(), "1.0".into()),
                ("take_profit_fraction".into(), "0.30".into()),
                (
                    "profit_shield_activation_r".into(),
                    FAST_PROFIT_SHIELD_ACTIVATION_R.into(),
                ),
                (
                    "pre_tp_trailing_activation_r".into(),
                    FAST_TRAILING_ACTIVATION_R.into(),
                ),
                (
                    "trailing_distance_pct".into(),
                    FAST_TRAILING_DISTANCE_PCT.into(),
                ),
                (
                    "risk_per_trade_pct".into(),
                    self.config.fast_risk_per_trade_pct.to_string(),
                ),
                ("max_notional_multiple".into(), "1.0".into()),
                ("entry_timeout_ms".into(), "120000".into()),
                ("min_fill_ratio".into(), "0.80".into()),
                ("entry_invalidation_bps".into(), "25".into()),
                ("max_entry_adverse_bps".into(), "6".into()),
                ("taker_fallback".into(), "false".into()),
                ("max_hold_ms".into(), (30 * 60_000).to_string()),
            ]);
            if verdict == Verdict::Pass {
                if let Some(book) = instrument.book.as_ref() {
                    tags.insert(
                        "entry_limit".into(),
                        (reference_price * 0.9996).min(book.bid).to_string(),
                    );
                }
            }
            let candidate = TradeCandidate {
                id: format!("fast_trend_activation:{symbol}:{signal_ms}"),
                recipe: "fast_trend_activation".into(),
                symbol: symbol.clone(),
                side: Side::Buy,
                signal_ms,
                expires_ms: signal_ms + 120_000,
                reference_price,
                score,
                confidence: if verdict == Verdict::Pass {
                    0.78
                } else {
                    progress
                },
                verdict,
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_5m_ignition"),
                    format!("{symbol}.binance_1m_reclaim"),
                    format!("{symbol}.binance_open_interest"),
                    format!("{symbol}.book"),
                ],
                tags,
            };
            if verdict == Verdict::Pass {
                passed.push((score, candidate));
            } else {
                observations.push((progress, score, candidate));
            }
        }

        passed.sort_by(|left, right| right.0.total_cmp(&left.0));
        passed.truncate(self.config.max_candidates_per_lane);
        observations.sort_by(|left, right| {
            right
                .0
                .total_cmp(&left.0)
                .then_with(|| right.1.total_cmp(&left.1))
        });
        observations.truncate(6);
        let actionable = !passed.is_empty();
        let reason = observations
            .first()
            .map(|value| format!("{}: {}", value.2.symbol, value.2.blockers.join(" · ")))
            .unwrap_or_else(|| "waiting for complete 5m, 1m, OI and book data".into());
        let mut output = vec![ArtifactRecord {
            key: "lane.fast_trend_activation.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable { "ready" } else { "scanning" }.into(),
                score: passed
                    .first()
                    .map(|value| value.0)
                    .or_else(|| observations.first().map(|value| value.1))
                    .unwrap_or_default(),
                side: Some(Side::Buy),
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if actionable { vec![] } else { vec![reason] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("market_return_1h".into(), market_return_1h),
                    ("market_breadth_1h".into(), market_breadth),
                    ("market_ready".into(), if market_ready { 1.0 } else { 0.0 }),
                    ("ignition_hits".into(), ignition_hits as f64),
                    ("confirmation_hits".into(), confirmation_hits as f64),
                    ("oi_ready_symbols".into(), oi_ready as f64),
                    ("pass_candidates".into(), passed.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "binance_5m".into(),
                        "binance_1m".into(),
                        "binance_oi".into(),
                    ],
                ),
            }),
        }];
        for (_, candidate) in passed.into_iter().chain(
            observations
                .into_iter()
                .map(|(_, score, candidate)| (score, candidate)),
        ) {
            output.push(ArtifactRecord {
                key: format!("candidate.{}", candidate.id),
                producer: self.id.clone(),
                artifact: Artifact::Candidate(candidate),
            });
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_even_and_odd_samples() {
        assert_eq!(median([3.0, 1.0, 2.0].into_iter()), Some(2.0));
        assert_eq!(median([4.0, 1.0, 3.0, 2.0].into_iter()), Some(2.5));
        assert_eq!(median(std::iter::empty()), None);
    }

    #[test]
    fn oi_change_is_causal() {
        let values = [(0, 100.0), (300_000, 101.0), (900_000, 102.0)];
        let change = oi_change(&values, 900_000, 900_000).unwrap();
        assert!((change - 0.02).abs() < 1e-9);
    }

    #[test]
    fn early_surge_thresholds_admit_the_first_4usdt_leg() {
        let config = LaneConfig::default();
        assert!(is_ignition(
            true,
            0.0092,
            2.1,
            Some(0.20),
            1.033,
            0.029,
            0.052,
            &config,
        ));
        assert!(0.0233 <= config.fast_max_oi_change_15m);
    }
}
