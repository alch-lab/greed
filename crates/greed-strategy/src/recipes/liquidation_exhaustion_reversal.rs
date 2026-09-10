use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct LiquidationExhaustionReversalNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl LiquidationExhaustionReversalNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.liquidation_exhaustion_reversal".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn is_major(symbol: &str) -> bool {
    ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"].contains(&symbol)
}

impl StrategyNode for LiquidationExhaustionReversalNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut passed = Vec::new();
        let mut observations = Vec::new();
        let mut fresh_events = 0_u64;
        let mut direction_hits = 0_u64;
        let mut depth_hits = 0_u64;
        let mut price_hits = 0_u64;
        let mut reversal_hits = 0_u64;

        for symbol in self.symbols.iter().filter(|symbol| !is_major(symbol)) {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let Some(micro) = instrument.microstructure.as_ref() else {
                continue;
            };
            let event_ms = micro.liquidation_event_ms.unwrap_or_default();
            if event_ms <= 0 {
                continue;
            }
            // Exchange clocks are useful for identity and audit, but the
            // local receive time is the first instant at which an order could
            // causally react to this event.
            let received_ms = micro.liquidation_received_ms.unwrap_or(event_ms);
            let age_ms = ctx.frame.as_of_ms.saturating_sub(received_ms);
            let long_liquidations = micro.long_liquidations_3s.unwrap_or_default();
            let short_liquidations = micro.short_liquidations_3s.unwrap_or_default();
            let dominance = micro.liquidation_dominance_3s.unwrap_or_default();
            let depth_ratio = micro.liquidation_depth_ratio_3s.unwrap_or_default();
            let aligned_return_bps = micro.liquidation_aligned_return_bps_3s;
            let reversal_bps = micro.liquidation_reversal_bps;
            let long_dominant = long_liquidations >= short_liquidations;
            // Forced long liquidation is aggressive selling, so exhaustion is
            // bought. Forced short liquidation is aggressive buying, so the
            // reversal trade is sold.
            let side = if long_dominant { Side::Buy } else { Side::Sell };
            let min_age_ms = i64::from(self.config.liquidation_min_entry_delay_seconds) * 1_000;
            let max_age_ms = i64::from(self.config.liquidation_max_signal_age_seconds) * 1_000;
            let fresh = age_ms >= min_age_ms && age_ms <= max_age_ms;
            fresh_events += u64::from(fresh);
            direction_hits += u64::from(dominance >= self.config.liquidation_min_dominance);
            depth_hits += u64::from(depth_ratio >= self.config.liquidation_min_depth_ratio);
            price_hits += u64::from(
                aligned_return_bps
                    .is_some_and(|value| value >= self.config.liquidation_min_aligned_return_bps),
            );
            reversal_hits += u64::from(
                reversal_bps.is_some_and(|value| value >= self.config.liquidation_min_reversal_bps),
            );

            let mut blockers = Vec::new();
            let mut executable_reference = None;
            let mut quote_received_ms = None;
            if !micro.meta.usable_at(ctx.frame.as_of_ms) {
                blockers.push("trade and liquidation stream is stale".into());
            }
            match instrument.book.as_ref() {
                Some(book) if book.meta.usable_at(ctx.frame.as_of_ms) => {
                    executable_reference = Some(match side {
                        Side::Buy => book.ask,
                        Side::Sell => book.bid,
                    });
                    quote_received_ms = Some(book.meta.received_ms);
                    let spread = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    if spread > self.config.max_spread_bps {
                        blockers.push(format!("spread {spread:.1} bps is too wide"));
                    }
                }
                _ => blockers.push("order book is warming".into()),
            }
            if age_ms < min_age_ms {
                blockers.push("waiting for the one-second anti-lookahead delay".into());
            } else if age_ms > max_age_ms {
                blockers.push("liquidation event is too old for executable reversal".into());
            }
            if dominance < self.config.liquidation_min_dominance {
                blockers.push(format!(
                    "three-second liquidation dominance {:.0}% is below {:.0}%",
                    dominance * 100.0,
                    self.config.liquidation_min_dominance * 100.0
                ));
            }
            if depth_ratio < self.config.liquidation_min_depth_ratio {
                blockers.push(format!(
                    "liquidation/depth {:.2}x is below {:.2}x",
                    depth_ratio, self.config.liquidation_min_depth_ratio
                ));
            }
            match aligned_return_bps {
                Some(value) if value < self.config.liquidation_min_aligned_return_bps => {
                    blockers.push(format!(
                        "aligned three-second move {value:.1} bps is below {:.1} bps",
                        self.config.liquidation_min_aligned_return_bps
                    ));
                }
                None => blockers.push("three-second midpoint path is incomplete".into()),
                _ => {}
            }
            match reversal_bps {
                Some(value) if value < self.config.liquidation_min_reversal_bps => {
                    blockers.push(format!(
                        "post-liquidation recovery {value:.1} bps is below {:.1} bps",
                        self.config.liquidation_min_reversal_bps
                    ));
                }
                None => blockers.push("post-liquidation recovery path is incomplete".into()),
                _ => {}
            }

            let verdict = if blockers.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Block
            };
            let progress = (6_usize.saturating_sub(blockers.len()).min(6) as f64) / 6.0;
            let score = dominance
                * depth_ratio.min(10.0)
                * aligned_return_bps.unwrap_or_default().max(0.0)
                * reversal_bps.unwrap_or_default().max(0.0);
            let candidate = TradeCandidate {
                id: format!("liquidation_exhaustion_reversal:{symbol}:{event_ms}"),
                recipe: "liquidation_exhaustion_reversal".into(),
                symbol: symbol.clone(),
                side,
                signal_ms: received_ms,
                expires_ms: received_ms + max_age_ms,
                // This is a seconds-scale taker setup.  A candle close is not
                // an executable reference and can lag the live book by tens
                // of bps during the exact shock we are trying to trade.
                reference_price: executable_reference.unwrap_or(instrument.price),
                score,
                confidence: if verdict == Verdict::Pass {
                    0.80
                } else {
                    progress
                },
                verdict,
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_force_order_3s"),
                    format!("{symbol}.binance_depth20"),
                    format!("{symbol}.binance_midpoint_3s"),
                ],
                tags: BTreeMap::from([
                    ("lane".into(), "liquidation_exhaustion_reversal".into()),
                    ("priority".into(), "2.0".into()),
                    (
                        "entry_pattern".into(),
                        "liquidation_exhaustion_reversal".into(),
                    ),
                    (
                        "window_seconds".into(),
                        self.config.liquidation_window_seconds.to_string(),
                    ),
                    ("liquidation_event_ms".into(), event_ms.to_string()),
                    ("liquidation_received_ms".into(), received_ms.to_string()),
                    ("signal_age_ms".into(), age_ms.to_string()),
                    (
                        "reference_quote_received_ms".into(),
                        quote_received_ms.unwrap_or_default().to_string(),
                    ),
                    (
                        "reference_price_source".into(),
                        if executable_reference.is_some() {
                            "executable_book"
                        } else {
                            "candle_fallback_unusable"
                        }
                        .into(),
                    ),
                    (
                        "long_liquidations_3s_usd".into(),
                        long_liquidations.to_string(),
                    ),
                    (
                        "short_liquidations_3s_usd".into(),
                        short_liquidations.to_string(),
                    ),
                    ("liquidation_dominance_3s".into(), dominance.to_string()),
                    ("liquidation_depth_ratio_3s".into(), depth_ratio.to_string()),
                    (
                        "liquidation_aligned_return_bps_3s".into(),
                        aligned_return_bps.unwrap_or_default().to_string(),
                    ),
                    (
                        "liquidation_reversal_bps".into(),
                        reversal_bps.unwrap_or_default().to_string(),
                    ),
                    (
                        "stop_pct".into(),
                        self.config.liquidation_stop_pct.to_string(),
                    ),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.liquidation_risk_per_trade_pct.to_string(),
                    ),
                    ("max_notional_multiple".into(), "1.0".into()),
                    // This lane has no fixed price target, but it is not an
                    // unmanaged fixed-horizon bet: favorable excursion owns
                    // a profit shield and a trailing stop before the deadline.
                    (
                        "managed_exit_only".into(),
                        self.config.liquidation_managed_exit_enabled.to_string(),
                    ),
                    (
                        "disable_take_profit".into(),
                        (!self.config.liquidation_managed_exit_enabled).to_string(),
                    ),
                    ("take_profit_fraction".into(), "1.0".into()),
                    (
                        "fixed_time_exit".into(),
                        (!self.config.liquidation_managed_exit_enabled).to_string(),
                    ),
                    (
                        "profit_shield_activation_r".into(),
                        (self.config.liquidation_profit_shield_activation_bps
                            / 10_000.0
                            / self.config.liquidation_stop_pct)
                            .to_string(),
                    ),
                    (
                        "break_even_buffer_pct".into(),
                        (self.config.liquidation_profit_shield_floor_bps / 10_000.0).to_string(),
                    ),
                    (
                        "pre_tp_trailing_activation_r".into(),
                        (self.config.liquidation_trailing_activation_bps
                            / 10_000.0
                            / self.config.liquidation_stop_pct)
                            .to_string(),
                    ),
                    (
                        "trailing_distance_pct".into(),
                        (self.config.liquidation_trailing_distance_bps / 10_000.0).to_string(),
                    ),
                    (
                        "max_hold_ms".into(),
                        (i64::from(self.config.liquidation_hold_minutes) * 60_000).to_string(),
                    ),
                    ("entry_timeout_ms".into(), "0".into()),
                    (
                        "max_entry_adverse_bps".into(),
                        self.config
                            .liquidation_max_execution_divergence_bps
                            .to_string(),
                    ),
                    ("bounded_taker_ioc".into(), "true".into()),
                    ("min_fill_ratio".into(), "1.0".into()),
                    ("min_managed_fill_ratio".into(), "0.0".into()),
                    (
                        "cooldown_seconds".into(),
                        self.config.liquidation_cooldown_seconds.to_string(),
                    ),
                ]),
            };
            if verdict == Verdict::Pass {
                passed.push((score, candidate));
            } else {
                observations.push((progress, score, candidate));
            }
        }

        passed.sort_by(|left, right| right.0.total_cmp(&left.0));
        passed.truncate(self.config.max_candidates_per_lane.min(1));
        observations.sort_by(|left, right| {
            right
                .0
                .total_cmp(&left.0)
                .then_with(|| right.1.total_cmp(&left.1))
        });
        observations.truncate(4);
        let actionable = !passed.is_empty();
        let reason = observations
            .first()
            .map(|value| format!("{}: {}", value.2.symbol, value.2.blockers.join(" · ")))
            .unwrap_or_else(|| "waiting for a complete three-second liquidation event".into());
        let mut output = vec![ArtifactRecord {
            key: "lane.liquidation_exhaustion_reversal.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable { "ready" } else { "scanning" }.into(),
                score: passed
                    .first()
                    .map(|value| value.0)
                    .or_else(|| observations.first().map(|value| value.1))
                    .unwrap_or_default(),
                side: passed.first().map(|value| value.1.side),
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if actionable { vec![] } else { vec![reason] },
                metrics: BTreeMap::from([
                    ("fresh_events".into(), fresh_events as f64),
                    ("dominance_hits".into(), direction_hits as f64),
                    ("depth_hits".into(), depth_hits as f64),
                    ("aligned_price_hits".into(), price_hits as f64),
                    ("reversal_hits".into(), reversal_hits as f64),
                    ("pass_candidates".into(), passed.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    15_000,
                    DataQuality::Complete,
                    1.0,
                    vec![
                        "binance_force_order".into(),
                        "binance_depth20".into(),
                        "binance_midpoint".into(),
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
    use greed_kernel::{
        AccountFrame, BookState, CandleSeries, InstrumentFrame, MarketFrame, MarketKind,
        MicrostructureState, ObservationMeta,
    };

    fn observation(expires_ms: i64) -> ObservationMeta {
        ObservationMeta {
            event_ms: 8_000,
            received_ms: 8_000,
            expires_ms,
            source: "test".into(),
            quality: DataQuality::Complete,
        }
    }

    fn frame(long_liquidations: f64, short_liquidations: f64) -> MarketFrame {
        let meta = observation(20_000);
        let symbol = "ALTUSDT".to_string();
        MarketFrame {
            as_of_ms: 10_000,
            instruments: BTreeMap::from([(
                symbol.clone(),
                InstrumentFrame {
                    symbol,
                    price: 99.9,
                    perpetual: CandleSeries {
                        venue: "test".into(),
                        market: MarketKind::Perpetual,
                        interval_ms: 900_000,
                        meta: meta.clone(),
                        values: vec![],
                    },
                    hourly_perpetual: None,
                    fast_perpetual: None,
                    micro_perpetual: None,
                    open_interest: None,
                    book: Some(BookState {
                        meta: meta.clone(),
                        bid: 99.89,
                        ask: 99.91,
                        bid_depth_usd: 20_000.0,
                        ask_depth_usd: 20_000.0,
                        expected_buy_slippage_bps: Some(1.0),
                        expected_sell_slippage_bps: Some(1.0),
                        bids: vec![],
                        asks: vec![],
                    }),
                    microstructure: Some(MicrostructureState {
                        meta,
                        buy_notional_60s: 10_000.0,
                        sell_notional_60s: 10_000.0,
                        long_liquidations_60s: long_liquidations,
                        short_liquidations_60s: short_liquidations,
                        long_liquidations_3s: Some(long_liquidations),
                        short_liquidations_3s: Some(short_liquidations),
                        liquidation_dominance_3s: Some(0.90),
                        liquidation_depth_ratio_3s: Some(1.30),
                        liquidation_aligned_return_bps_3s: Some(4.0),
                        liquidation_reversal_bps: Some(15.0),
                        liquidation_event_ms: Some(8_000),
                        liquidation_received_ms: Some(8_000),
                        snapshot_ofi_10s: None,
                        snapshot_ofi_60s: None,
                        mid_return_bps_10s: None,
                        mid_return_bps_60s: None,
                        price_impact_bps_per_ofi_10s: None,
                        book_updates_10s: 0,
                        book_updates_60s: 0,
                    }),
                },
            )]),
            account: AccountFrame {
                equity_usd: 5_000.0,
                cash_usd: 5_000.0,
                realized_pnl_usd: 0.0,
                peak_equity_usd: 5_000.0,
                risk_day_start_equity_usd: 5_000.0,
                gross_exposure_usd: 0.0,
                open_positions: 0,
            },
        }
    }

    #[test]
    fn long_liquidation_exhaustion_creates_a_managed_horizon_buy() {
        let market = frame(9_000.0, 1_000.0);
        let mut node = LiquidationExhaustionReversalNode::new(
            &["ALTUSDT".into()],
            LaneConfig {
                liquidation_managed_exit_enabled: true,
                liquidation_hold_minutes: 15,
                ..LaneConfig::default()
            },
        );
        let records = node
            .evaluate(&NodeContext {
                frame: &market,
                artifacts: &BTreeMap::new(),
            })
            .unwrap();
        let candidate = records
            .iter()
            .find_map(|record| record.artifact.candidate())
            .expect("qualifying event should produce a candidate");
        assert_eq!(candidate.verdict, Verdict::Pass);
        assert_eq!(candidate.side, Side::Buy);
        assert_eq!(candidate.reference_price, 99.91);
        assert_eq!(candidate.tags["reference_price_source"], "executable_book");
        assert_eq!(candidate.tags["reference_quote_received_ms"], "8000");
        assert_eq!(candidate.tags["fixed_time_exit"], "false");
        assert_eq!(candidate.tags["managed_exit_only"], "true");
        assert_eq!(candidate.tags["break_even_buffer_pct"], "0.001");
    }

    #[test]
    fn short_liquidation_exhaustion_reverses_to_sell() {
        let market = frame(1_000.0, 9_000.0);
        let mut node =
            LiquidationExhaustionReversalNode::new(&["ALTUSDT".into()], LaneConfig::default());
        let records = node
            .evaluate(&NodeContext {
                frame: &market,
                artifacts: &BTreeMap::new(),
            })
            .unwrap();
        let candidate = records
            .iter()
            .find_map(|record| record.artifact.candidate())
            .unwrap();
        assert_eq!(candidate.side, Side::Sell);
        assert_eq!(candidate.reference_price, 99.89);
    }

    #[test]
    fn forced_move_without_recovery_remains_observation_only() {
        let mut market = frame(9_000.0, 1_000.0);
        market
            .instruments
            .get_mut("ALTUSDT")
            .and_then(|instrument| instrument.microstructure.as_mut())
            .unwrap()
            .liquidation_reversal_bps = Some(4.0);
        let mut node =
            LiquidationExhaustionReversalNode::new(&["ALTUSDT".into()], LaneConfig::default());
        let records = node
            .evaluate(&NodeContext {
                frame: &market,
                artifacts: &BTreeMap::new(),
            })
            .unwrap();
        let candidate = records
            .iter()
            .find_map(|record| record.artifact.candidate())
            .unwrap();
        assert_eq!(candidate.verdict, Verdict::Block);
        assert!(candidate
            .blockers
            .iter()
            .any(|reason| reason.contains("post-liquidation recovery")));
    }

    #[test]
    fn freshness_uses_causal_receive_time_not_exchange_clock() {
        let mut market = frame(9_000.0, 1_000.0);
        let micro = market
            .instruments
            .get_mut("ALTUSDT")
            .and_then(|instrument| instrument.microstructure.as_mut())
            .unwrap();
        // Simulate an exchange clock ahead of the local process. The event was
        // observable locally at 8s, so a 10s frame may safely act on it.
        micro.liquidation_event_ms = Some(12_000);
        micro.liquidation_received_ms = Some(8_000);
        let mut node =
            LiquidationExhaustionReversalNode::new(&["ALTUSDT".into()], LaneConfig::default());
        let records = node
            .evaluate(&NodeContext {
                frame: &market,
                artifacts: &BTreeMap::new(),
            })
            .unwrap();
        let candidate = records
            .iter()
            .find_map(|record| record.artifact.candidate())
            .unwrap();
        assert_eq!(candidate.verdict, Verdict::Pass);
        assert_eq!(candidate.signal_ms, 8_000);
        assert_eq!(candidate.tags["liquidation_event_ms"], "12000");
        assert_eq!(candidate.tags["liquidation_received_ms"], "8000");
    }
}
