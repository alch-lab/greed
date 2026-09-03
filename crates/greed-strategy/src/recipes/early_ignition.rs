use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct EarlyIgnitionNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl EarlyIgnitionNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.early_ignition".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or_default()
}

fn range(values: &[&Candle]) -> f64 {
    let high = values.iter().map(|bar| bar.high).fold(0.0, f64::max);
    let low = values
        .iter()
        .map(|bar| bar.low)
        .fold(f64::INFINITY, f64::min);
    high / low.max(f64::EPSILON) - 1.0
}

fn atr(values: &[&Candle], end: usize, length: usize) -> f64 {
    let start = end.saturating_sub(length - 1).max(1);
    let values: Vec<_> = (start..=end)
        .map(|i| {
            let bar = values[i];
            (bar.high - bar.low)
                .max((bar.high - values[i - 1].close).abs())
                .max((bar.low - values[i - 1].close).abs())
        })
        .collect();
    median(values)
}

fn change(values: &[(i64, f64)], at_ms: i64, lookback_ms: i64) -> Option<f64> {
    let current = values.iter().rev().find(|(ts, _)| *ts <= at_ms)?.1;
    let prior = values
        .iter()
        .rev()
        .find(|(ts, _)| *ts <= at_ms - lookback_ms)?
        .1;
    (prior > 0.0).then_some(current / prior - 1.0)
}

fn is_major(symbol: &str) -> bool {
    ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"].contains(&symbol)
}

impl StrategyNode for EarlyIgnitionNode {
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
                (closed.len() >= 13)
                    .then(|| closed.last().unwrap().close / closed[closed.len() - 13].close - 1.0)
            })
            .collect();
        let market_return_1h = median(market_returns);
        // Breadth is useful for sizing, but isolated altcoin ignitions are common.
        // Treat it as a confidence/risk modifier instead of rejecting the signal.
        let market_supportive = market_return_1h >= self.config.early_ignition_min_market_return_1h;

        let mut passed = Vec::new();
        let mut observations = Vec::new();
        let mut inspected = 0u64;
        let mut breakout_hits = 0u64;
        let mut retest_hits = 0u64;
        let mut acceleration_hits = 0u64;
        let mut oi_ready = 0u64;

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
            let i = closed.len() - 1;
            let bar = closed[i];
            let prior = &closed[i - 12..i];
            let breakout_level = prior.iter().map(|value| value.high).fold(0.0, f64::max);
            let body_return = bar.close / bar.open.max(f64::EPSILON) - 1.0;
            let volume_ratio = bar.quote_volume
                / median(
                    closed[i - 36..i]
                        .iter()
                        .map(|value| value.quote_volume)
                        .collect(),
                )
                .max(f64::EPSILON);
            let flow = bar
                .taker_buy_quote
                .map(|buy| 2.0 * buy / bar.quote_volume.max(f64::EPSILON) - 1.0)
                .unwrap_or_default();
            let current_range = range(prior);
            let older_ranges: Vec<_> = (1..=6)
                .map(|window| {
                    let end = i - 12 * window;
                    range(&closed[end - 12..end])
                })
                .collect();
            let compression_ratio = current_range / median(older_ranges).max(f64::EPSILON);
            let prebreak_return_1h = bar.open / closed[i - 12].open - 1.0;
            let return_4h = bar.close / closed[i - 48].close - 1.0;
            let breakout = bar.close > breakout_level
                && body_return >= self.config.early_ignition_min_body_return
                && volume_ratio >= self.config.early_ignition_min_volume_ratio
                && flow >= self.config.early_ignition_min_flow
                && compression_ratio <= self.config.early_ignition_max_compression_ratio
                && prebreak_return_1h <= self.config.early_ignition_max_prebreak_return_1h
                && return_4h <= self.config.early_ignition_max_return_4h;
            breakout_hits += u64::from(breakout);

            let confirmation_minutes: Vec<_> = instrument
                .micro_perpetual
                .as_ref()
                .map(|series| {
                    series
                        .values
                        .iter()
                        .filter(|minute| {
                            minute.closed
                                && minute.close_ms > bar.close_ms
                                && minute.close_ms <= bar.close_ms + 3 * 60_000
                        })
                        .collect()
                })
                .unwrap_or_default();
            let retest_confirmation = confirmation_minutes.iter().copied().find(|minute| {
                let minute_flow = minute
                    .taker_buy_quote
                    .map(|buy| 2.0 * buy / minute.quote_volume.max(f64::EPSILON) - 1.0)
                    .unwrap_or_default();
                minute.low <= breakout_level * 1.0015
                    && minute.close >= breakout_level
                    && minute_flow >= 0.02
            });
            // A genuine ignition does not always revisit the breakout level. In
            // that case accept the first compact 1m continuation close while
            // flow is still expanding. This is deliberately unavailable after
            // a deep rejection, so it is not a generic late-pump chase.
            let acceleration_confirmation = confirmation_minutes.iter().copied().find(|minute| {
                let minute_body = minute.close / minute.open.max(f64::EPSILON) - 1.0;
                let minute_range = (minute.high - minute.low).max(f64::EPSILON);
                let close_location = (minute.close - minute.low) / minute_range;
                let minute_flow = minute
                    .taker_buy_quote
                    .map(|buy| 2.0 * buy / minute.quote_volume.max(f64::EPSILON) - 1.0)
                    .unwrap_or_default();
                minute.low >= breakout_level * 0.997
                    && minute.close >= bar.close * 1.001
                    && minute_body >= 0.0015
                    && close_location >= 0.70
                    && minute_flow >= (self.config.early_ignition_min_flow * 0.67).max(0.08)
            });
            let (confirmation, entry_pattern) = if let Some(value) = retest_confirmation {
                (Some(value), "shallow_retest")
            } else if let Some(value) = acceleration_confirmation {
                (Some(value), "acceleration")
            } else {
                (None, "awaiting_confirmation")
            };
            retest_hits += u64::from(breakout && retest_confirmation.is_some());
            acceleration_hits += u64::from(
                breakout && retest_confirmation.is_none() && acceleration_confirmation.is_some(),
            );

            let oi_changes = instrument.open_interest.as_ref().map(|series| {
                let values: Vec<_> = series
                    .values
                    .iter()
                    .map(|value| (value.timestamp_ms, value.value_usd))
                    .collect();
                (
                    change(&values, bar.close_ms, 15 * 60_000),
                    change(&values, bar.close_ms, 60 * 60_000),
                )
            });
            let oi_15m = oi_changes.and_then(|value| value.0);
            let oi_60m = oi_changes.and_then(|value| value.1);
            let oi_upper = if entry_pattern == "acceleration" {
                self.config.early_ignition_max_oi_change_15m * 2.0
            } else {
                self.config.early_ignition_max_oi_change_15m
            };
            let oi_ok = oi_15m.is_some_and(|value| value >= -0.005 && value <= oi_upper)
                && oi_60m.is_some_and(|value| value >= -0.005);
            oi_ready += u64::from(oi_15m.is_some() && oi_60m.is_some());

            let mut blockers = Vec::new();
            if bar.close <= breakout_level {
                blockers.push("waiting for a completed 5m range breakout".into());
            }
            if body_return < self.config.early_ignition_min_body_return {
                blockers.push(format!("5m body {:.2}% is too small", body_return * 100.0));
            }
            if volume_ratio < self.config.early_ignition_min_volume_ratio {
                blockers.push(format!("5m volume {volume_ratio:.2}x is below ignition"));
            }
            if flow < self.config.early_ignition_min_flow {
                blockers.push(format!(
                    "directional taker pressure {:.1}% is too weak",
                    flow * 100.0
                ));
            }
            if compression_ratio > self.config.early_ignition_max_compression_ratio {
                blockers.push(format!(
                    "prior range compression {compression_ratio:.2}x is too loose"
                ));
            }
            if prebreak_return_1h > self.config.early_ignition_max_prebreak_return_1h
                || return_4h > self.config.early_ignition_max_return_4h
            {
                blockers.push("move is already mature; do not chase".into());
            }
            if breakout && confirmation.is_none() {
                blockers.push(
                    "waiting up to 3m for either a shallow reclaim or compact acceleration close"
                        .into(),
                );
            }
            if oi_15m.is_none() || oi_60m.is_none() {
                blockers.push("open-interest history is warming".into());
            } else if !oi_ok {
                blockers.push(format!(
                    "OI change is outside the -0.5%..{:.1}% ignition band",
                    oi_upper * 100.0
                ));
            }
            let expected_notional = ctx.frame.account.equity_usd * 1.5;
            let required_depth = self
                .config
                .min_depth_usd
                .min((expected_notional * 8.0).max(5_000.0));
            match instrument.book.as_ref() {
                Some(book) if book.meta.usable_at(ctx.frame.as_of_ms) => {
                    let spread = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    if spread > self.config.max_spread_bps {
                        blockers.push(format!("spread {spread:.1} bps is too wide"));
                    }
                    if book.bid_depth_usd.min(book.ask_depth_usd) < required_depth {
                        blockers.push(format!(
                            "order-book depth is below the ${required_depth:.0} size-aware floor"
                        ));
                    }
                }
                _ => blockers.push("order book is warming".into()),
            }

            let signal_ms = confirmation.map_or(bar.close_ms, |value| value.close_ms);
            let reference_price = confirmation.map_or(instrument.price, |value| value.close);
            if ctx.frame.as_of_ms - signal_ms > 120_000 {
                blockers.push("retest confirmation expired".into());
            }
            let verdict = if blockers.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Block
            };
            let stop_pct =
                (1.25 * atr(&closed, i, 20) / bar.close.max(f64::EPSILON)).clamp(0.003, 0.10);
            let score = body_return.max(0.0)
                * volume_ratio
                * (1.0 + flow.max(0.0))
                * if entry_pattern == "acceleration" {
                    1.1
                } else {
                    1.0
                };
            let progress = (10usize.saturating_sub(blockers.len()).min(10) as f64) / 10.0;
            let mut tags = BTreeMap::from([
                ("lane".into(), "early_ignition".into()),
                ("priority".into(), "1.15".into()),
                ("entry_pattern".into(), entry_pattern.into()),
                ("market_supportive".into(), market_supportive.to_string()),
                ("market_return_1h".into(), market_return_1h.to_string()),
                ("body_return_5m".into(), body_return.to_string()),
                ("volume_ratio_5m".into(), volume_ratio.to_string()),
                ("directional_flow_5m".into(), flow.to_string()),
                ("compression_ratio".into(), compression_ratio.to_string()),
                (
                    "oi_change_15m".into(),
                    oi_15m.unwrap_or_default().to_string(),
                ),
                (
                    "oi_change_60m".into(),
                    oi_60m.unwrap_or_default().to_string(),
                ),
                ("stop_pct".into(), stop_pct.to_string()),
                ("target_r".into(), "2.0".into()),
                ("take_profit_fraction".into(), "1.0".into()),
                (
                    "risk_per_trade_pct".into(),
                    (self.config.early_ignition_risk_per_trade_pct
                        * if market_supportive { 1.0 } else { 0.7 })
                    .to_string(),
                ),
                ("max_notional_multiple".into(), "1.5".into()),
                ("entry_timeout_ms".into(), "120000".into()),
                ("taker_fallback".into(), "false".into()),
                ("max_hold_ms".into(), (30 * 60_000).to_string()),
            ]);
            if verdict == Verdict::Pass {
                if let Some(book) = instrument.book.as_ref() {
                    tags.insert(
                        "entry_limit".into(),
                        (reference_price * (1.0 - 0.0004)).min(book.bid).to_string(),
                    );
                }
            }
            let candidate = TradeCandidate {
                id: format!("early_ignition:{symbol}:{signal_ms}"),
                recipe: "early_ignition".into(),
                symbol: symbol.clone(),
                side: Side::Buy,
                signal_ms,
                expires_ms: signal_ms + 120_000,
                reference_price,
                score,
                confidence: if verdict == Verdict::Pass {
                    0.82
                } else {
                    progress
                },
                verdict,
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_5m_breakout"),
                    format!("{symbol}.binance_1m_retest"),
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

        passed.sort_by(|a, b| b.0.total_cmp(&a.0));
        passed.truncate(self.config.max_candidates_per_lane);
        observations.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| b.1.total_cmp(&a.1)));
        observations.truncate(8);
        let actionable = !passed.is_empty();
        let nearest = observations
            .first()
            .map(|value| {
                format!(
                    "{} is closest: {}",
                    value.2.symbol,
                    value.2.blockers.join(" · ")
                )
            })
            .unwrap_or_else(|| "waiting for complete 5m, 1m, OI and book history".into());
        let mut out = vec![ArtifactRecord {
            key: "lane.early_ignition.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable {
                    "ready_to_execute"
                } else {
                    "scanning_early_ignition"
                }
                .into(),
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
                reasons: if actionable { vec![] } else { vec![nearest] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("market_return_1h".into(), market_return_1h),
                    ("breakout_hits".into(), breakout_hits as f64),
                    ("retest_hits".into(), retest_hits as f64),
                    ("acceleration_hits".into(), acceleration_hits as f64),
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
                        "binance_open_interest".into(),
                        "binance_book".into(),
                    ],
                ),
            }),
        }];
        for (_, candidate) in passed.into_iter().chain(
            observations
                .into_iter()
                .map(|(_, score, candidate)| (score, candidate)),
        ) {
            out.push(ArtifactRecord {
                key: format!("candidate.{}", candidate.id),
                producer: self.id.clone(),
                artifact: Artifact::Candidate(candidate),
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greed_kernel::{
        AccountFrame, BookState, CandleSeries, InstrumentFrame, MarketFrame, MarketKind,
        ObservationMeta, OpenInterestPoint, OpenInterestSeries,
    };

    fn observation_meta(now: i64) -> ObservationMeta {
        ObservationMeta {
            event_ms: now,
            received_ms: now,
            expires_ms: now + 120_000,
            source: "test".into(),
            quality: DataQuality::Complete,
        }
    }

    #[test]
    fn accepts_retest_or_compact_acceleration_without_a_breadth_veto() {
        let mut bars: Vec<_> = (0..90)
            .map(|index| Candle {
                open_ms: index * 300_000,
                close_ms: (index + 1) * 300_000 - 1,
                open: 100.0,
                high: 100.1,
                low: 99.9,
                close: 100.0,
                quote_volume: 100.0,
                taker_buy_quote: Some(50.0),
                closed: true,
            })
            .collect();
        let breakout = bars.last_mut().unwrap();
        breakout.high = 100.7;
        breakout.close = 100.6;
        breakout.quote_volume = 300.0;
        breakout.taker_buy_quote = Some(180.0);
        let breakout_close = breakout.close_ms;
        let confirm_close = breakout_close + 60_000;
        let now = confirm_close + 1_000;
        let fast = CandleSeries {
            venue: "binance".into(),
            market: MarketKind::Perpetual,
            interval_ms: 300_000,
            meta: observation_meta(now),
            values: bars,
        };
        let micro = CandleSeries {
            venue: "binance".into(),
            market: MarketKind::Perpetual,
            interval_ms: 60_000,
            meta: observation_meta(now),
            values: vec![Candle {
                open_ms: breakout_close + 1,
                close_ms: confirm_close,
                open: 100.15,
                high: 100.25,
                low: 100.05,
                close: 100.2,
                quote_volume: 100.0,
                taker_buy_quote: Some(55.0),
                closed: true,
            }],
        };
        let oi_values = (0..24)
            .map(|index| OpenInterestPoint {
                timestamp_ms: breakout_close - (23 - index) * 300_000,
                value_usd: 1_000_000.0 * (1.0 + index as f64 * 0.0002),
            })
            .collect();
        let instrument = InstrumentFrame {
            symbol: "TESTUSDT".into(),
            price: 100.2,
            perpetual: fast.clone(),
            hourly_perpetual: None,
            fast_perpetual: Some(fast),
            micro_perpetual: Some(micro),
            open_interest: Some(OpenInterestSeries {
                interval_ms: 300_000,
                meta: observation_meta(now),
                values: oi_values,
            }),
            book: Some(BookState {
                meta: observation_meta(now),
                bid: 100.15,
                ask: 100.16,
                bid_depth_usd: 100_000.0,
                ask_depth_usd: 100_000.0,
                expected_buy_slippage_bps: Some(1.0),
                expected_sell_slippage_bps: Some(1.0),
                bids: vec![],
                asks: vec![],
            }),
            microstructure: None,
        };
        let mut frame = MarketFrame {
            as_of_ms: now,
            instruments: BTreeMap::from([("TESTUSDT".into(), instrument)]),
            account: AccountFrame {
                equity_usd: 1_000.0,
                cash_usd: 1_000.0,
                realized_pnl_usd: 0.0,
                peak_equity_usd: 1_000.0,
                risk_day_start_equity_usd: 1_000.0,
                gross_exposure_usd: 0.0,
                open_positions: 0,
            },
        };
        let config = LaneConfig {
            early_ignition_min_market_return_1h: 0.02,
            ..LaneConfig::default()
        };
        let mut node = EarlyIgnitionNode::new(&["TESTUSDT".into()], config.clone());
        let artifacts = BTreeMap::new();
        let output = node
            .evaluate(&NodeContext {
                frame: &frame,
                artifacts: &artifacts,
            })
            .unwrap();
        let candidate = output
            .iter()
            .find_map(|record| record.artifact.candidate())
            .unwrap();
        assert_eq!(candidate.verdict, Verdict::Pass);
        assert_eq!(candidate.reference_price, 100.2);
        assert_eq!(
            candidate.tags.get("taker_fallback").map(String::as_str),
            Some("false")
        );
        assert_eq!(
            candidate.tags.get("entry_pattern").map(String::as_str),
            Some("shallow_retest")
        );
        assert_eq!(
            candidate.tags.get("market_supportive").map(String::as_str),
            Some("false")
        );

        let minute = frame
            .instruments
            .get_mut("TESTUSDT")
            .unwrap()
            .micro_perpetual
            .as_mut()
            .unwrap()
            .values
            .first_mut()
            .unwrap();
        minute.open = 100.62;
        minute.high = 100.85;
        minute.low = 100.55;
        minute.close = 100.82;
        minute.taker_buy_quote = Some(60.0);
        let mut acceleration_node = EarlyIgnitionNode::new(&["TESTUSDT".into()], config);
        let acceleration_output = acceleration_node
            .evaluate(&NodeContext {
                frame: &frame,
                artifacts: &artifacts,
            })
            .unwrap();
        let acceleration = acceleration_output
            .iter()
            .find_map(|record| record.artifact.candidate())
            .unwrap();
        assert_eq!(acceleration.verdict, Verdict::Pass);
        assert_eq!(
            acceleration.tags.get("entry_pattern").map(String::as_str),
            Some("acceleration")
        );
    }
}
