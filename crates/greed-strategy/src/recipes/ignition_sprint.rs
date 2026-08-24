use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct IgnitionSprintNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl IgnitionSprintNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.ignition_sprint".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or_default()
}

fn atr(values: &[&Candle], end: usize, window: usize) -> f64 {
    let start = end.saturating_sub(window - 1).max(1);
    let rows: Vec<_> = (start..=end)
        .map(|index| {
            let bar = values[index];
            let previous = values[index - 1].close;
            (bar.high - bar.low)
                .max((bar.high - previous).abs())
                .max((bar.low - previous).abs())
        })
        .collect();
    rows.iter().sum::<f64>() / rows.len().max(1) as f64
}

impl StrategyNode for IgnitionSprintNode {
    fn id(&self) -> &str {
        &self.id
    }

    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut candidates = Vec::new();
        let mut inspected = 0u64;
        let mut ignition_hits = 0u64;
        let mut reclaim_hits = 0u64;
        for symbol in &self.symbols {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let Some(fast) = instrument.fast_perpetual.as_ref() else {
                continue;
            };
            let closed: Vec<_> = fast.values.iter().filter(|bar| bar.closed).collect();
            if closed.len() < 38 {
                continue;
            }
            inspected += 1;
            let index = closed.len() - 1;
            let ignition = closed[index];
            let age_ms = ctx.frame.as_of_ms - ignition.close_ms;
            let side = if ignition.close >= ignition.open {
                Side::Buy
            } else {
                Side::Sell
            };
            let sign = side.sign();
            let return_5m = ignition.close / ignition.open - 1.0;
            let return_10m = ignition.close / closed[index - 1].open - 1.0;
            let return_30m = ignition.close / closed[index - 6].open - 1.0;
            let baseline = median(
                closed[index - 36..index]
                    .iter()
                    .map(|bar| bar.quote_volume)
                    .collect(),
            );
            let volume_ratio = ignition.quote_volume / baseline.max(1.0);
            let flow = ignition
                .taker_buy_quote
                .map(|buy| 2.0 * buy / ignition.quote_volume.max(1.0) - 1.0);
            let spread = (ignition.high - ignition.low).max(f64::EPSILON);
            let close_location = (ignition.close - ignition.low) / spread;
            let directional_close = if side == Side::Buy {
                close_location >= 0.72
            } else {
                close_location <= 0.28
            };
            let prior_high = closed[index - 6..index]
                .iter()
                .map(|bar| bar.high)
                .fold(f64::NEG_INFINITY, f64::max);
            let prior_low = closed[index - 6..index]
                .iter()
                .map(|bar| bar.low)
                .fold(f64::INFINITY, f64::min);
            let broke_range = if side == Side::Buy {
                ignition.close > prior_high
            } else {
                ignition.close < prior_low
            };
            let atr = atr(&closed, index, 14);
            let ignition_ready = sign * return_5m >= self.config.ignition_min_return_5m
                && sign * return_10m >= self.config.ignition_min_return_5m * 1.15
                && sign * return_30m <= self.config.ignition_max_extension_30m
                && volume_ratio >= self.config.ignition_min_volume_ratio
                && flow
                    .is_some_and(|value| sign * value >= self.config.ignition_min_flow_imbalance)
                && directional_close
                && broke_range
                && return_5m.abs() <= 0.035
                && (ignition.close - ignition.open).abs() <= 2.5 * atr;
            ignition_hits += u64::from(ignition_ready);

            let mut blockers = Vec::new();
            if !ignition_ready {
                if sign * return_5m < self.config.ignition_min_return_5m {
                    blockers.push(format!(
                        "5m speed {:.2}% / need {:.2}%",
                        sign * return_5m * 100.0,
                        self.config.ignition_min_return_5m * 100.0
                    ));
                }
                if volume_ratio < self.config.ignition_min_volume_ratio {
                    blockers.push(format!(
                        "5m volume {:.1}x / need {:.1}x",
                        volume_ratio, self.config.ignition_min_volume_ratio
                    ));
                }
                if !directional_close || !broke_range {
                    blockers.push("waiting for a directional micro-range break".into());
                }
            }
            if age_ms < 0 || age_ms > i64::from(self.config.ignition_max_wait_seconds) * 1_000 {
                blockers.push("no live ignition inside the 3-minute entry window".into());
            }

            let micro: Vec<_> = instrument
                .micro_perpetual
                .as_ref()
                .map(|series| {
                    series
                        .values
                        .iter()
                        .filter(|bar| bar.open_ms >= ignition.close_ms)
                        .collect()
                })
                .unwrap_or_default();
            let retest_seen = micro.iter().any(|bar| {
                if side == Side::Buy {
                    ignition.close - bar.low >= 0.15 * atr
                } else {
                    bar.high - ignition.close >= 0.15 * atr
                }
            });
            let invalid = micro.iter().any(|bar| {
                if side == Side::Buy {
                    bar.low < ignition.close - atr
                } else {
                    bar.high > ignition.close + atr
                }
            });
            if ignition_ready && !retest_seen {
                blockers.push("waiting for the first 0.15 ATR micro pullback".into());
            }
            if invalid {
                blockers.push("ignition invalidated by a pullback beyond 1 ATR".into());
            }
            let latest = micro.last().copied();
            let reclaim = latest.is_some_and(|bar| {
                let range = (bar.high - bar.low).max(f64::EPSILON);
                let location = (bar.close - bar.low) / range;
                let directional = if side == Side::Buy {
                    bar.close > bar.open && location >= 0.65
                } else {
                    bar.close < bar.open && location <= 0.35
                };
                let flow = bar
                    .taker_buy_quote
                    .map(|buy| 2.0 * buy / bar.quote_volume.max(1.0) - 1.0)
                    .unwrap_or_default();
                let location_atr = sign * (bar.close - ignition.close) / atr.max(f64::EPSILON);
                directional
                    && sign * flow >= self.config.ignition_reclaim_flow_imbalance
                    && (-0.25..=0.75).contains(&location_atr)
            });
            if ignition_ready && retest_seen && !reclaim {
                blockers.push("waiting for 1m taker flow to reclaim the ignition direction".into());
            }
            let microstructure = instrument.microstructure.as_ref();
            let ofi_ready = microstructure
                .and_then(|value| value.snapshot_ofi_10s)
                .is_some_and(|value| sign * value > 0.0);
            let trade_ready = microstructure
                .and_then(|value| value.trade_imbalance())
                .is_some_and(|value| sign * value >= self.config.ignition_reclaim_flow_imbalance);
            if ignition_ready && (!ofi_ready || !trade_ready) {
                blockers.push("10s OFI and 60s aggressive flow have not reconfirmed".into());
            }
            let mut entry_limit = instrument.price;
            match instrument.book.as_ref() {
                None => blockers.push("order book is not ready".into()),
                Some(book) if !book.meta.usable_at(ctx.frame.as_of_ms) => {
                    blockers.push("order book is stale".into())
                }
                Some(book) => {
                    let spread_bps = (book.ask - book.bid)
                        / ((book.ask + book.bid) * 0.5).max(f64::EPSILON)
                        * 10_000.0;
                    let depth = book.bid_depth_usd.min(book.ask_depth_usd);
                    if spread_bps > self.config.max_spread_bps {
                        blockers.push(format!("spread {spread_bps:.1} bps is too wide"));
                    }
                    if depth < self.config.min_depth_usd {
                        blockers.push(format!("book depth ${depth:.0} is too thin"));
                    }
                    let passive = if side == Side::Buy {
                        book.bid
                    } else {
                        book.ask
                    };
                    let offset = self.config.ignition_limit_offset_atr * atr;
                    entry_limit = if side == Side::Buy {
                        passive.min(instrument.price - offset)
                    } else {
                        passive.max(instrument.price + offset)
                    };
                }
            }
            let ready = ignition_ready
                && age_ms >= 0
                && age_ms <= i64::from(self.config.ignition_max_wait_seconds) * 1_000
                && retest_seen
                && !invalid
                && reclaim
                && ofi_ready
                && trade_ready
                && blockers.is_empty();
            reclaim_hits += u64::from(ready);
            let stop_pct =
                self.config.ignition_stop_atr_multiple * atr / entry_limit.max(f64::EPSILON);
            let progress = 8usize.saturating_sub(blockers.len()).min(8) as f64 / 8.0;
            let signal_ms = latest.map_or(ignition.close_ms, |bar| {
                bar.close_ms.min(ctx.frame.as_of_ms)
            });
            let candidate = TradeCandidate {
                id: format!("ignition_sprint:{symbol}:{}", ignition.close_ms),
                recipe: "ignition_sprint".into(),
                symbol: symbol.clone(),
                side,
                signal_ms,
                expires_ms: ctx.frame.as_of_ms
                    + i64::from(self.config.ignition_entry_timeout_seconds) * 1_000,
                reference_price: entry_limit,
                score: sign * return_5m * volume_ratio * (1.0 + sign * flow.unwrap_or_default()),
                confidence: if ready { 0.78 } else { progress },
                verdict: if ready { Verdict::Pass } else { Verdict::Block },
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_ws_5m_ignition"),
                    format!("{symbol}.binance_ws_1m_reclaim"),
                    format!("{symbol}.binance_ws_ofi"),
                    format!("{symbol}.book"),
                ],
                tags: BTreeMap::from([
                    ("lane".into(), "ignition_sprint".into()),
                    ("entry_limit".into(), entry_limit.to_string()),
                    (
                        "entry_timeout_ms".into(),
                        (i64::from(self.config.ignition_entry_timeout_seconds) * 1_000).to_string(),
                    ),
                    (
                        "risk_per_trade_pct".into(),
                        self.config.ignition_risk_per_trade_pct.to_string(),
                    ),
                    ("stop_pct".into(), stop_pct.to_string()),
                    ("target_r".into(), self.config.ignition_target_r.to_string()),
                    ("take_profit_fraction".into(), "1.0".into()),
                    (
                        "max_hold_ms".into(),
                        (i64::from(self.config.ignition_max_hold_minutes) * 60_000).to_string(),
                    ),
                ]),
            };
            candidates.push((ready, progress, candidate));
        }
        candidates.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.total_cmp(&a.1))
                .then_with(|| b.2.score.total_cmp(&a.2.score))
        });
        let passed = candidates.iter().filter(|value| value.0).count();
        candidates.truncate(8);
        let nearest = candidates
            .first()
            .map(|value| format!("{}: {}", value.2.symbol, value.2.blockers.join(" · ")))
            .unwrap_or_else(|| "waiting for complete 5m, 1m, trade and order-book data".into());
        let mut out = vec![ArtifactRecord {
            key: "lane.ignition_sprint.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if passed > 0 {
                    "post_only_entry_ready"
                } else if ignition_hits > 0 {
                    "confirming_micro_pullback"
                } else {
                    "scanning_for_ignition"
                }
                .into(),
                score: candidates.first().map_or(0.0, |value| value.2.score),
                side: candidates.first().map(|value| value.2.side),
                verdict: if passed > 0 {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if passed > 0 { vec![] } else { vec![nearest] },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("ignition_hits".into(), ignition_hits as f64),
                    ("reclaim_hits".into(), reclaim_hits as f64),
                    ("pass_candidates".into(), passed as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    30_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["binance_ws_5m_1m_aggtrade_depth".into()],
                ),
            }),
        }];
        out.extend(
            candidates
                .into_iter()
                .map(|(_, _, candidate)| ArtifactRecord {
                    key: format!("candidate.{}", candidate.id),
                    producer: self.id.clone(),
                    artifact: Artifact::Candidate(candidate),
                }),
        );
        Ok(out)
    }
}
