use crate::config::PaperConfig;
use greed_kernel::{
    AccountFrame, Artifact, AssetClass, GraphEvaluation, MarketFrame, PositionPlan, Side,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub candidate_id: String,
    pub symbol: String,
    pub side: Side,
    pub entry_ms: i64,
    pub entry_price: f64,
    pub quantity: f64,
    pub remaining_quantity: f64,
    pub stop_price: f64,
    pub take_profit_prices: Vec<(f64, f64)>,
    pub trailing_activation_pct: Option<f64>,
    pub trailing_distance_pct: Option<f64>,
    pub max_hold_ms: i64,
    pub extreme_price: f64,
    pub realized_pnl_usd: f64,
    pub last_bar_ms: i64,
}

pub struct BrokerEvent {
    pub kind: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperBroker {
    config: PaperConfig,
    cash: f64,
    realized: f64,
    peak_equity: f64,
    risk_day_start: f64,
    risk_day: String,
    positions: BTreeMap<String, PaperPosition>,
    seen: BTreeSet<String>,
}
impl PaperBroker {
    pub fn new(config: PaperConfig) -> Self {
        let cash = config.initial_cash_usd;
        Self {
            config,
            cash,
            realized: 0.0,
            peak_equity: cash,
            risk_day_start: cash,
            risk_day: String::new(),
            positions: BTreeMap::new(),
            seen: BTreeSet::new(),
        }
    }
    pub fn load_or_new(config: PaperConfig, path: &str) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let mut broker: Self = serde_json::from_str(&text)?;
                // Runtime costs are configuration, while positions/accounting
                // are durable state.  A deliberate config change takes effect
                // after restart without rewriting historical positions.
                broker.config = config;
                Ok(broker)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::new(config)),
            Err(error) => Err(error.into()),
        }
    }
    pub fn save(&self, path: &str) -> anyhow::Result<()> {
        let path = std::path::Path::new(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp = path.with_extension("tmp");
        std::fs::write(&temp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(temp, path)?;
        Ok(())
    }
    fn current_day(ms: i64) -> String {
        chrono::DateTime::from_timestamp_millis(ms + 8 * 3_600_000)
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_default()
    }
    pub fn positions(&self) -> &BTreeMap<String, PaperPosition> {
        &self.positions
    }
    pub fn account_frame(&self) -> AccountFrame {
        AccountFrame {
            equity_usd: self.cash,
            cash_usd: self.cash,
            realized_pnl_usd: self.realized,
            peak_equity_usd: self.peak_equity,
            risk_day_start_equity_usd: self.risk_day_start,
            gross_exposure_usd: self
                .positions
                .values()
                .map(|position| position.entry_price * position.remaining_quantity)
                .sum(),
            major_gross_exposure_usd: 0.0,
            alt_gross_exposure_usd: 0.0,
            open_positions: self.positions.len(),
        }
    }
    pub fn marked_account(&self, frame: &MarketFrame) -> AccountFrame {
        let unrealized: f64 = self
            .positions
            .values()
            .filter_map(|p| {
                frame.instrument(&p.symbol).map(|i| {
                    p.side.sign()
                        * (i.price / p.entry_price - 1.0)
                        * p.entry_price
                        * p.remaining_quantity
                })
            })
            .sum();
        let equity = self.cash + unrealized;
        let gross_exposure_usd = self
            .positions
            .values()
            .filter_map(|position| {
                frame
                    .instrument(&position.symbol)
                    .map(|instrument| instrument.price * position.remaining_quantity)
            })
            .sum();
        let major_gross_exposure_usd = self
            .positions
            .values()
            .filter_map(|position| {
                let instrument = frame.instrument(&position.symbol)?;
                (instrument.asset_class == AssetClass::Major)
                    .then_some(instrument.price * position.remaining_quantity)
            })
            .sum();
        let alt_gross_exposure_usd = self
            .positions
            .values()
            .filter_map(|position| {
                let instrument = frame.instrument(&position.symbol)?;
                (instrument.asset_class == AssetClass::Altcoin)
                    .then_some(instrument.price * position.remaining_quantity)
            })
            .sum();
        AccountFrame {
            equity_usd: equity,
            cash_usd: self.cash,
            realized_pnl_usd: self.realized,
            peak_equity_usd: self.peak_equity.max(equity),
            risk_day_start_equity_usd: self.risk_day_start,
            gross_exposure_usd,
            major_gross_exposure_usd,
            alt_gross_exposure_usd,
            open_positions: self.positions.len(),
        }
    }
    pub fn mark_to_market(&mut self, frame: &MarketFrame) -> Vec<BrokerEvent> {
        let day = Self::current_day(frame.as_of_ms);
        if day != self.risk_day {
            self.risk_day = day;
            self.risk_day_start = self.marked_account(frame).equity_usd;
        }
        let mut events = Vec::new();
        let symbols: Vec<_> = self.positions.keys().cloned().collect();
        for symbol in symbols {
            let mut position = self
                .positions
                .remove(&symbol)
                .expect("position key collected from map");
            let Some(instrument) = frame.instrument(&symbol) else {
                self.positions.insert(symbol, position);
                continue;
            };
            let Some(last) = instrument
                .perpetual
                .values
                .iter()
                .rev()
                .find(|bar| bar.closed)
            else {
                self.positions.insert(symbol, position);
                continue;
            };
            if last.close_ms <= position.last_bar_ms {
                self.positions.insert(symbol, position);
                continue;
            }
            position.last_bar_ms = last.close_ms;
            // Existing protection wins when the same candle also traverses a
            // profit level; the intrabar path is unknowable from OHLC alone.
            let stop_hit = match position.side {
                Side::Buy => last.low <= position.stop_price,
                Side::Sell => last.high >= position.stop_price,
            };
            if stop_hit {
                let stop_price = position.stop_price;
                events.push(self.close_position(
                    position,
                    stop_price,
                    "protective_stop",
                    frame.as_of_ms,
                ));
                continue;
            }

            let mut remaining_targets = Vec::new();
            for (target, fraction) in std::mem::take(&mut position.take_profit_prices) {
                let hit = match position.side {
                    Side::Buy => last.high >= target,
                    Side::Sell => last.low <= target,
                };
                if hit && position.remaining_quantity > f64::EPSILON {
                    let quantity = (position.quantity * fraction)
                        .min(position.remaining_quantity)
                        .max(0.0);
                    if quantity > f64::EPSILON {
                        events.push(self.settle_leg(
                            &mut position,
                            target,
                            quantity,
                            "partial_take_profit",
                            frame.as_of_ms,
                        ));
                    }
                } else {
                    remaining_targets.push((target, fraction));
                }
            }
            position.take_profit_prices = remaining_targets;
            if position.remaining_quantity <= f64::EPSILON {
                continue;
            }

            position.extreme_price = match position.side {
                Side::Buy => position.extreme_price.max(last.high),
                Side::Sell => position.extreme_price.min(last.low),
            };
            if let (Some(activation), Some(distance)) = (
                position.trailing_activation_pct,
                position.trailing_distance_pct,
            ) {
                let favorable =
                    position.side.sign() * (position.extreme_price / position.entry_price - 1.0);
                if favorable >= activation {
                    let proposed = position.extreme_price * (1.0 - position.side.sign() * distance);
                    position.stop_price = match position.side {
                        Side::Buy => position.stop_price.max(proposed),
                        Side::Sell => position.stop_price.min(proposed),
                    };
                }
            }
            if frame.as_of_ms - position.entry_ms >= position.max_hold_ms {
                events.push(self.close_position(
                    position,
                    instrument.price,
                    "time_exit",
                    frame.as_of_ms,
                ));
            } else {
                self.positions.insert(symbol, position);
            }
        }
        let equity = self.marked_account(frame).equity_usd;
        self.peak_equity = self.peak_equity.max(equity);
        events
    }
    pub fn close_all(&mut self, frame: &MarketFrame, reason: &str) -> Vec<BrokerEvent> {
        let symbols: Vec<_> = self.positions.keys().cloned().collect();
        let mut events = Vec::new();
        for symbol in symbols {
            let Some(position) = self.positions.remove(&symbol) else {
                continue;
            };
            let price = frame
                .instrument(&symbol)
                .map(|instrument| instrument.price)
                .unwrap_or(position.entry_price);
            events.push(self.close_position(position, price, reason, frame.as_of_ms));
        }
        events
    }
    fn close_position(
        &mut self,
        mut position: PaperPosition,
        raw_price: f64,
        reason: &str,
        ts_ms: i64,
    ) -> BrokerEvent {
        let quantity = position.remaining_quantity;
        self.settle_leg(&mut position, raw_price, quantity, reason, ts_ms)
    }
    fn settle_leg(
        &mut self,
        position: &mut PaperPosition,
        raw_price: f64,
        quantity: f64,
        reason: &str,
        ts_ms: i64,
    ) -> BrokerEvent {
        let slip = self.config.slippage_bps_per_side / 10_000.0;
        let fee = self.config.fee_bps_per_side / 10_000.0;
        let exit = raw_price * (1.0 - position.side.sign() * slip);
        let gross = position.side.sign() * (exit - position.entry_price) * quantity;
        let exit_fee = exit * quantity * fee;
        let pnl = gross - exit_fee;
        self.cash += pnl;
        self.realized += pnl;
        position.realized_pnl_usd += pnl;
        position.remaining_quantity = (position.remaining_quantity - quantity).max(0.0);
        BrokerEvent {
            kind: if position.remaining_quantity > f64::EPSILON {
                "paper_partial_exit".into()
            } else {
                "paper_exit".into()
            },
            payload: serde_json::json!({"ts_ms":ts_ms,"candidate_id":position.candidate_id,"symbol":position.symbol,"side":position.side,"entry_price":position.entry_price,"exit_price":exit,"quantity":quantity,"remaining_quantity":position.remaining_quantity,"gross_pnl_usd":gross,"fee_usd":exit_fee,"pnl_usd":pnl,"reason":reason}),
        }
    }
    pub fn apply_plans(
        &mut self,
        frame: &MarketFrame,
        evaluation: &GraphEvaluation,
    ) -> Vec<BrokerEvent> {
        let mut events = Vec::new();
        for record in evaluation.artifacts.values() {
            let Artifact::PositionPlan(plan) = &record.artifact else {
                continue;
            };
            if self.positions.len() >= self.config.max_positions
                || self.positions.contains_key(&plan.symbol)
                || self.seen.contains(&plan.candidate_id)
            {
                continue;
            }
            if let Some(event) = self.open(frame, plan) {
                events.push(event);
            }
        }
        events
    }
    fn open(&mut self, frame: &MarketFrame, plan: &PositionPlan) -> Option<BrokerEvent> {
        let instrument = frame.instrument(&plan.symbol)?;
        if instrument.price <= 0.0 || plan.reference_price <= 0.0 {
            return None;
        }
        let slip = self.config.slippage_bps_per_side / 10_000.0;
        let fee = self.config.fee_bps_per_side / 10_000.0;
        let entry = instrument.price * (1.0 + plan.side.sign() * slip);
        let quantity = plan.notional_usd / entry;
        let reference = plan.reference_price;
        let stop_distance = (plan.stop_price / reference - 1.0).abs();
        let rebased_stop = entry * (1.0 - plan.side.sign() * stop_distance);
        let rebased_targets = plan
            .take_profit_prices
            .iter()
            .map(|(price, fraction)| {
                let distance = (price / reference - 1.0).abs();
                (entry * (1.0 + plan.side.sign() * distance), *fraction)
            })
            .collect();
        let entry_fee = entry * quantity * fee;
        self.cash -= entry_fee;
        self.realized -= entry_fee;
        self.seen.insert(plan.candidate_id.clone());
        self.positions.insert(
            plan.symbol.clone(),
            PaperPosition {
                candidate_id: plan.candidate_id.clone(),
                symbol: plan.symbol.clone(),
                side: plan.side,
                entry_ms: frame.as_of_ms,
                entry_price: entry,
                quantity,
                remaining_quantity: quantity,
                stop_price: rebased_stop,
                take_profit_prices: rebased_targets,
                trailing_activation_pct: plan.trailing_activation_pct,
                trailing_distance_pct: plan.trailing_distance_pct,
                max_hold_ms: plan.max_hold_ms,
                extreme_price: entry,
                realized_pnl_usd: -entry_fee,
                last_bar_ms: 0,
            },
        );
        Some(BrokerEvent {
            kind: "paper_entry".into(),
            payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"symbol":plan.symbol,"side":plan.side,"entry_price":entry,"quantity":quantity,"notional_usd":plan.notional_usd,"fee_usd":entry_fee,"stop_price":rebased_stop,"paper_only":true}),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greed_kernel::{
        ArtifactRecord, AssetClass, BookState, Candle, CandleSeries, DataQuality, InstrumentFrame,
        MarketKind, ObservationMeta,
    };

    fn frame(ts: i64, price: f64, low: f64, high: f64) -> MarketFrame {
        let meta = ObservationMeta {
            event_ms: ts,
            received_ms: ts,
            expires_ms: ts + 60_000,
            source: "test".into(),
            quality: DataQuality::Complete,
        };
        let candle = Candle {
            open_ms: ts - 900_000,
            close_ms: ts,
            open: price,
            high,
            low,
            close: price,
            quote_volume: 1_000_000.0,
            taker_buy_quote: Some(500_000.0),
            closed: true,
        };
        let instrument = InstrumentFrame {
            symbol: "BTCUSDT".into(),
            asset_class: AssetClass::Major,
            price,
            spot: None,
            perpetual: CandleSeries {
                venue: "test".into(),
                market: MarketKind::Perpetual,
                interval_ms: 900_000,
                meta: meta.clone(),
                values: vec![candle],
            },
            book: Some(BookState {
                meta,
                bid: price,
                ask: price,
                bid_depth_usd: 1_000_000.0,
                ask_depth_usd: 1_000_000.0,
                expected_buy_slippage_bps: Some(0.0),
                expected_sell_slippage_bps: Some(0.0),
                bids: vec![],
                asks: vec![],
            }),
            derivatives: None,
            external: None,
        };
        MarketFrame {
            as_of_ms: ts,
            instruments: BTreeMap::from([("BTCUSDT".into(), instrument)]),
            account: AccountFrame {
                equity_usd: 3_000.0,
                cash_usd: 3_000.0,
                realized_pnl_usd: 0.0,
                peak_equity_usd: 3_000.0,
                risk_day_start_equity_usd: 3_000.0,
                gross_exposure_usd: 0.0,
                major_gross_exposure_usd: 0.0,
                alt_gross_exposure_usd: 0.0,
                open_positions: 0,
            },
        }
    }

    fn evaluation() -> GraphEvaluation {
        let plan = PositionPlan {
            candidate_id: "candidate-1".into(),
            symbol: "BTCUSDT".into(),
            side: Side::Buy,
            reference_price: 100.0,
            notional_usd: 600.0,
            entry_limit: None,
            stop_price: 99.0,
            take_profit_prices: vec![(102.0, 0.5)],
            trailing_activation_pct: Some(0.012),
            trailing_distance_pct: Some(0.006),
            max_hold_ms: 3_600_000,
        };
        GraphEvaluation {
            artifacts: BTreeMap::from([(
                "plan.candidate-1".into(),
                ArtifactRecord {
                    key: "plan.candidate-1".into(),
                    producer: "test".into(),
                    artifact: Artifact::PositionPlan(plan),
                },
            )]),
            node_order: vec![],
        }
    }

    #[test]
    fn existing_stop_wins_over_same_bar_take_profit() {
        let mut broker = PaperBroker::new(PaperConfig {
            fee_bps_per_side: 0.0,
            slippage_bps_per_side: 0.0,
            ..PaperConfig::default()
        });
        assert_eq!(
            broker
                .apply_plans(&frame(1_000_000, 100.0, 100.0, 100.0), &evaluation())
                .len(),
            1
        );
        let events = broker.mark_to_market(&frame(1_900_000, 100.0, 98.0, 103.0));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["reason"], "protective_stop");
        assert!(broker.positions().is_empty());
    }

    #[test]
    fn partial_take_profit_preserves_a_protected_runner() {
        let mut broker = PaperBroker::new(PaperConfig {
            fee_bps_per_side: 0.0,
            slippage_bps_per_side: 0.0,
            ..PaperConfig::default()
        });
        broker.apply_plans(&frame(1_000_000, 100.0, 100.0, 100.0), &evaluation());
        let events = broker.mark_to_market(&frame(1_900_000, 102.0, 100.0, 103.0));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "paper_partial_exit");
        let position = broker.positions().get("BTCUSDT").unwrap();
        assert!((position.remaining_quantity - position.quantity * 0.5).abs() < 1e-9);
        assert!(position.stop_price > position.entry_price);
    }

    #[test]
    fn state_roundtrip_keeps_positions_and_seen_candidates() {
        let mut broker = PaperBroker::new(PaperConfig::default());
        broker.apply_plans(&frame(1_000_000, 100.0, 100.0, 100.0), &evaluation());
        let path = std::env::temp_dir().join(format!("greed-paper-{}.json", std::process::id()));
        broker.save(path.to_str().unwrap()).unwrap();
        let restored =
            PaperBroker::load_or_new(PaperConfig::default(), path.to_str().unwrap()).unwrap();
        assert!(restored.positions().contains_key("BTCUSDT"));
        assert!(restored.seen.contains("candidate-1"));
        std::fs::remove_file(path).unwrap();
    }
}
