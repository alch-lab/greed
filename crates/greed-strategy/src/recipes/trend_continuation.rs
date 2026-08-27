use crate::{primitives::meta, LaneConfig};
use greed_kernel::{
    Artifact, ArtifactRecord, Candle, DataQuality, NodeContext, Side, StateArtifact, StrategyNode,
    TradeCandidate, Verdict,
};
use std::collections::BTreeMap;

pub struct TrendContinuationNode {
    id: String,
    symbols: Vec<String>,
    config: LaneConfig,
}

impl TrendContinuationNode {
    pub fn new(symbols: &[String], config: LaneConfig) -> Self {
        Self {
            id: "lane.trend_continuation".into(),
            symbols: symbols.to_vec(),
            config,
        }
    }
}

fn ema(values: &[&Candle], period: usize) -> Vec<f64> {
    let alpha = 2.0 / (period as f64 + 1.0);
    let mut output = Vec::with_capacity(values.len());
    let mut current = values.first().map_or(0.0, |value| value.close);
    for value in values {
        current = alpha * value.close + (1.0 - alpha) * current;
        output.push(current);
    }
    output
}

fn trend_age_bars(values: &[&Candle], end: usize, sign: f64, threshold: f64) -> usize {
    let mut age = 0;
    for cursor in (16..=end).rev() {
        let return_4h = values[cursor].close / values[cursor - 16].close - 1.0;
        if sign * return_4h < threshold {
            break;
        }
        age += 1;
    }
    age
}

fn passive_pullback_limit(close: f64, atr: f64, sign: f64, offset_atr: f64, book: f64) -> f64 {
    let desired = close - sign * offset_atr * atr;
    if sign > 0.0 {
        desired.min(book)
    } else {
        desired.max(book)
    }
}

impl StrategyNode for TrendContinuationNode {
    fn id(&self) -> &str {
        &self.id
    }
    fn dependencies(&self) -> &[String] {
        &[]
    }

    fn evaluate(&mut self, ctx: &NodeContext<'_>) -> Result<Vec<ArtifactRecord>, String> {
        let mut passed = Vec::new();
        let mut observations = Vec::new();
        let mut inspected = 0u64;
        let mut trend_hits = 0u64;
        let mut reclaim_hits = 0u64;
        for symbol in &self.symbols {
            let Some(instrument) = ctx.frame.instrument(symbol) else {
                continue;
            };
            let closed: Vec<_> = instrument
                .perpetual
                .values
                .iter()
                .filter(|value| value.closed)
                .collect();
            if closed.len() < 97 {
                continue;
            }
            inspected += 1;
            let i = closed.len() - 1;
            let bar = closed[i];
            let age = ctx.frame.as_of_ms - bar.close_ms;
            if age < 0 || age > instrument.perpetual.interval_ms {
                continue;
            }
            let return_4h = bar.close / closed[i - 16].close - 1.0;
            let return_12h = bar.close / closed[i - 48].close - 1.0;
            let path: f64 = (i - 15..=i)
                .map(|j| (closed[j].close / closed[j - 1].close - 1.0).abs())
                .sum();
            let efficiency = return_4h.abs() / path.max(f64::EPSILON);
            let ema8 = ema(&closed, 8);
            let ema21 = ema(&closed, 21);
            let ema36 = ema(&closed, 36);
            let side = if return_4h >= 0.0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let sign = side.sign();
            let trend_age_bars = trend_age_bars(&closed, i, sign, self.config.trend_min_return_4h);
            let atr = (i.saturating_sub(19)..=i)
                .map(|index| {
                    let prior_close = if index == 0 {
                        closed[index].close
                    } else {
                        closed[index - 1].close
                    };
                    (closed[index].high - closed[index].low)
                        .max((closed[index].high - prior_close).abs())
                        .max((closed[index].low - prior_close).abs())
                })
                .sum::<f64>()
                / 20.0;
            let extension_atr = sign * (bar.close - ema21[i]) / atr.max(f64::EPSILON);
            let reclaim_body_atr = (bar.close - bar.open).abs() / atr.max(f64::EPSILON);
            let touched = if side == Side::Buy {
                closed[i - 3..i]
                    .iter()
                    .any(|value| value.low <= ema21[i - 1])
            } else {
                closed[i - 3..i]
                    .iter()
                    .any(|value| value.high >= ema21[i - 1])
            };
            let reclaimed = if side == Side::Buy {
                bar.close > ema8[i] && bar.close > closed[i - 1].close
            } else {
                bar.close < ema8[i] && bar.close < closed[i - 1].close
            };
            let imbalance = bar.taker_buy_quote.map(|taker_buy| {
                (2.0 * taker_buy / bar.quote_volume.max(1.0) - 1.0).clamp(-1.0, 1.0)
            });
            let hour_volume: f64 = closed[i - 3..=i]
                .iter()
                .map(|value| value.quote_volume)
                .sum();
            let baseline: f64 = closed[i - 95..=i]
                .iter()
                .map(|value| value.quote_volume)
                .sum::<f64>()
                / 24.0;
            let volume_ratio = hour_volume / baseline.max(1.0);
            let mut blockers = Vec::new();
            if age > i64::from(self.config.trend_max_signal_age_seconds) * 1_000 {
                blockers
                    .push("entry window expired; waiting for the next completed 15m candle".into());
            }
            if return_4h.abs() < self.config.trend_min_return_4h {
                blockers.push(format!(
                    "4h move {:.2}% / need {:.2}%",
                    return_4h.abs() * 100.0,
                    self.config.trend_min_return_4h * 100.0
                ));
            }
            if trend_age_bars > self.config.trend_max_age_bars {
                blockers.push(format!(
                    "trend is {trend_age_bars} bars old / max {} bars for a fresh entry",
                    self.config.trend_max_age_bars
                ));
            }
            if sign * return_12h <= 0.0 {
                blockers.push(format!(
                    "12h trend is not aligned with the {} direction",
                    if side == Side::Buy { "long" } else { "short" }
                ));
            }
            let ema_aligned = if side == Side::Buy {
                ema21[i] > ema36[i]
            } else {
                ema21[i] < ema36[i]
            };
            if !ema_aligned {
                blockers.push("EMA21 / EMA36 trend structure is not aligned".into());
            }
            if efficiency < self.config.trend_min_efficiency {
                blockers.push(format!(
                    "trend efficiency {:.0}% / need {:.0}%",
                    efficiency * 100.0,
                    self.config.trend_min_efficiency * 100.0
                ));
            }
            if !touched {
                blockers.push("waiting for a pullback to EMA21".into());
            }
            if !reclaimed {
                blockers
                    .push("waiting for the 15m close to reclaim EMA8 and the prior close".into());
            }
            match imbalance {
                Some(value) if sign * value < self.config.trend_min_flow_imbalance => blockers
                    .push(format!(
                        "taker flow {:.1}% is against the setup",
                        value * 100.0
                    )),
                None => blockers.push("taker-flow observation is not ready".into()),
                _ => {}
            }
            if volume_ratio < self.config.trend_min_hour_volume_ratio {
                blockers.push(format!(
                    "1h volume {:.0}% of baseline / need {:.0}%",
                    volume_ratio * 100.0,
                    self.config.trend_min_hour_volume_ratio * 100.0
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
                    let depth = book.bid_depth_usd.min(book.ask_depth_usd);
                    if spread > self.config.max_spread_bps {
                        blockers.push(format!(
                            "spread {:.1} bps / max {:.1} bps",
                            spread, self.config.max_spread_bps
                        ));
                    }
                    if depth < self.config.min_depth_usd {
                        blockers.push(format!(
                            "book depth ${depth:.0} / need ${:.0}",
                            self.config.min_depth_usd
                        ));
                    }
                }
            }
            let trend_ready = return_4h.abs() >= self.config.trend_min_return_4h
                && trend_age_bars <= self.config.trend_max_age_bars
                && sign * return_12h > 0.0
                && ema_aligned
                && efficiency >= self.config.trend_min_efficiency;
            trend_hits += u64::from(trend_ready);
            reclaim_hits += u64::from(trend_ready && touched && reclaimed);
            let score = return_4h.abs() * efficiency * volume_ratio;
            let progress = (11usize.saturating_sub(blockers.len()).min(11) as f64) / 11.0;
            let verdict = if blockers.is_empty() {
                Verdict::Pass
            } else {
                Verdict::Block
            };
            let mut tags = BTreeMap::from([
                ("lane".into(), "trend_continuation".into()),
                ("return_4h".into(), return_4h.to_string()),
                ("trend_efficiency".into(), efficiency.to_string()),
                ("trend_age_bars".into(), trend_age_bars.to_string()),
                ("trend_extension_atr".into(), extension_atr.to_string()),
                (
                    "trend_reclaim_body_atr".into(),
                    reclaim_body_atr.to_string(),
                ),
            ]);
            if verdict == Verdict::Pass {
                if let Some(book) = instrument.book.as_ref() {
                    // A fresh 15m reclaim is the signal, not the executable
                    // price. Rest one level deeper for a brief retest and do
                    // not chase when the continuation leaves without us.
                    let passive_limit = passive_pullback_limit(
                        bar.close,
                        atr,
                        sign,
                        self.config.trend_limit_offset_atr,
                        if side == Side::Buy {
                            book.bid
                        } else {
                            book.ask
                        },
                    );
                    tags.insert("entry_limit".into(), passive_limit.to_string());
                    tags.insert(
                        "entry_timeout_ms".into(),
                        (i64::from(self.config.trend_entry_timeout_seconds) * 1_000).to_string(),
                    );
                    tags.insert("taker_fallback".into(), "false".into());
                    tags.insert(
                        "max_entry_adverse_bps".into(),
                        self.config.trend_max_entry_adverse_bps.to_string(),
                    );
                    tags.insert(
                        "entry_offset_atr".into(),
                        self.config.trend_limit_offset_atr.to_string(),
                    );
                }
            }
            let candidate = TradeCandidate {
                id: format!("trend_continuation:{symbol}:{}", bar.close_ms),
                recipe: "trend_continuation".into(),
                symbol: symbol.clone(),
                side,
                signal_ms: bar.close_ms,
                expires_ms: bar.close_ms
                    + i64::from(self.config.trend_max_signal_age_seconds) * 1_000,
                reference_price: instrument.price,
                score,
                confidence: if verdict == Verdict::Pass {
                    (0.65 + efficiency * 0.25 + (volume_ratio - 0.65).max(0.0) * 0.05).min(0.95)
                } else {
                    progress
                },
                verdict,
                blockers,
                evidence: vec![
                    format!("{symbol}.binance_15m_trend"),
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
        let nearest_reason = observations
            .first()
            .map(|value| {
                format!(
                    "{} is closest: {}",
                    value.2.symbol,
                    value.2.blockers.join(" · ")
                )
            })
            .unwrap_or_else(|| "waiting for complete 15m and order-book observations".into());
        let status_score = passed
            .first()
            .map(|value| value.0)
            .or_else(|| observations.first().map(|value| value.0))
            .unwrap_or_default();
        let status_side = passed
            .first()
            .map(|value| value.1.side)
            .or_else(|| observations.first().map(|value| value.2.side));
        let mut out = vec![ArtifactRecord {
            key: "lane.trend_continuation.status".into(),
            producer: self.id.clone(),
            artifact: Artifact::State(StateArtifact {
                state: if actionable {
                    "ready_to_size_and_execute"
                } else {
                    "scanning_for_strict_trend_reclaim"
                }
                .into(),
                score: status_score,
                side: status_side,
                verdict: if actionable {
                    Verdict::Pass
                } else {
                    Verdict::Block
                },
                reasons: if actionable {
                    vec![]
                } else {
                    vec![nearest_reason]
                },
                metrics: BTreeMap::from([
                    ("inspected_symbols".into(), inspected as f64),
                    ("trend_hits".into(), trend_hits as f64),
                    ("reclaim_hits".into(), reclaim_hits as f64),
                    ("pass_candidates".into(), passed.len() as f64),
                    ("candidate_observations".into(), observations.len() as f64),
                ]),
                meta: meta(
                    ctx.frame.as_of_ms,
                    90_000,
                    DataQuality::Complete,
                    1.0,
                    vec!["binance_ws_15m".into(), "binance_ws_depth".into()],
                ),
            }),
        }];
        out.extend(passed.into_iter().map(|(_, candidate)| ArtifactRecord {
            key: format!("candidate.{}", candidate.id),
            producer: self.id.clone(),
            artifact: Artifact::Candidate(candidate),
        }));
        out.extend(
            observations
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

#[cfg(test)]
mod tests {
    use super::*;

    fn candle(open_ms: i64, close: f64) -> Candle {
        Candle {
            open_ms,
            close_ms: open_ms + 899_999,
            open: close,
            high: close,
            low: close,
            close,
            quote_volume: 1.0,
            taker_buy_quote: Some(0.5),
            closed: true,
        }
    }

    #[test]
    fn trend_age_counts_only_consecutive_threshold_bars() {
        let mut values: Vec<_> = (0..17)
            .map(|index| candle(index * 900_000, 100.0))
            .collect();
        values.extend((17..20).map(|index| candle(index * 900_000, 106.0)));
        let references: Vec<_> = values.iter().collect();
        assert_eq!(trend_age_bars(&references, 19, 1.0, 0.06), 3);
        assert_eq!(trend_age_bars(&references, 19, -1.0, 0.06), 0);
    }

    #[test]
    fn pullback_limit_is_deeper_and_never_crosses_the_book() {
        assert_eq!(passive_pullback_limit(100.0, 2.0, 1.0, 0.30, 99.9), 99.4);
        assert_eq!(passive_pullback_limit(100.0, 2.0, -1.0, 0.30, 100.1), 100.6);
        assert_eq!(passive_pullback_limit(100.0, 2.0, 1.0, 0.0, 99.9), 99.9);
        assert_eq!(passive_pullback_limit(100.0, 2.0, -1.0, 0.0, 100.1), 100.1);
    }
}
