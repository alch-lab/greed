use crate::config::PaperConfig;
use greed_kernel::{
    AccountFrame, Artifact, AssetClass, GraphEvaluation, MarketFrame, PositionPlan, Side,
};
use greed_strategy::RiskConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub candidate_id: String,
    #[serde(default = "unknown_recipe")]
    pub recipe: String,
    #[serde(default = "default_asset_class")]
    pub asset_class: AssetClass,
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

#[derive(Debug, Serialize)]
pub struct PositionSnapshot<'a> {
    #[serde(flatten)]
    pub position: &'a PaperPosition,
    pub current_price: Option<f64>,
    pub current_notional_usd: Option<f64>,
    pub unrealized_pnl_usd: Option<f64>,
    pub unrealized_pnl_pct: Option<f64>,
}

fn unknown_recipe() -> String {
    "unknown".into()
}

fn default_asset_class() -> AssetClass {
    AssetClass::Major
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleeveLedger {
    pub initial_equity_usd: f64,
    pub realized_pnl_usd: f64,
    pub fees_usd: f64,
    pub peak_equity_usd: f64,
    pub risk_day_start_equity_usd: f64,
    pub risk_day: String,
}

impl SleeveLedger {
    fn new(initial: f64) -> Self {
        Self {
            initial_equity_usd: initial,
            realized_pnl_usd: 0.0,
            fees_usd: 0.0,
            peak_equity_usd: initial,
            risk_day_start_equity_usd: initial,
            risk_day: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleeveLedgers {
    pub major: SleeveLedger,
    pub altcoin: SleeveLedger,
    #[serde(default)]
    pub unattributed_realized_pnl_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SleeveSnapshot {
    pub initial_equity_usd: f64,
    pub equity_usd: f64,
    pub cash_usd: f64,
    pub realized_pnl_usd: f64,
    pub unrealized_pnl_usd: f64,
    pub fees_usd: f64,
    pub peak_equity_usd: f64,
    pub drawdown_pct: f64,
    pub daily_loss_pct: f64,
    pub gross_exposure_usd: f64,
    pub open_positions: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SleeveSnapshots {
    pub major: SleeveSnapshot,
    pub altcoin: SleeveSnapshot,
    pub unattributed_realized_pnl_usd: f64,
}

pub struct BrokerEvent {
    pub kind: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RecipeOutcome {
    exit_ms: i64,
    pnl_usd: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RecipePerformance {
    outcomes: Vec<RecipeOutcome>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecipeGateStatus {
    pub allowed: bool,
    pub completed_trades: usize,
    pub rolling_profit_factor: Option<f64>,
    pub rolling_net_pnl_usd: f64,
    pub next_probe_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperBroker {
    config: PaperConfig,
    #[serde(skip)]
    risk: RiskConfig,
    cash: f64,
    realized: f64,
    peak_equity: f64,
    risk_day_start: f64,
    risk_day: String,
    positions: BTreeMap<String, PaperPosition>,
    seen: BTreeSet<String>,
    #[serde(default)]
    sleeves: Option<SleeveLedgers>,
    #[serde(default)]
    recipe_performance: BTreeMap<String, RecipePerformance>,
}
impl PaperBroker {
    pub fn with_risk(config: PaperConfig, risk: RiskConfig) -> Self {
        let cash = config.initial_cash_usd;
        Self {
            config,
            risk,
            cash,
            realized: 0.0,
            peak_equity: cash,
            risk_day_start: cash,
            risk_day: String::new(),
            positions: BTreeMap::new(),
            seen: BTreeSet::new(),
            sleeves: Some(Self::new_sleeves(cash)),
            recipe_performance: BTreeMap::new(),
        }
    }
    fn new_sleeves(total: f64) -> SleeveLedgers {
        let major = total / 2.0;
        SleeveLedgers {
            major: SleeveLedger::new(major),
            altcoin: SleeveLedger::new(total - major),
            unattributed_realized_pnl_usd: 0.0,
        }
    }
    pub fn load_or_new(
        config: PaperConfig,
        risk: RiskConfig,
        path: &str,
        major_symbols: &[String],
    ) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let mut broker: Self = serde_json::from_str(&text)?;
                // Runtime costs are configuration, while positions/accounting
                // are durable state.  A deliberate config change takes effect
                // after restart without rewriting historical positions.
                broker.config = config;
                broker.risk = risk;
                for position in broker.positions.values_mut() {
                    position.asset_class = if major_symbols.contains(&position.symbol) {
                        AssetClass::Major
                    } else {
                        AssetClass::Altcoin
                    };
                    if position.recipe == "unknown" {
                        position.recipe = recipe_from_candidate(&position.candidate_id).into();
                    }
                }
                if broker.sleeves.is_none() {
                    let mut sleeves = Self::new_sleeves(broker.config.initial_cash_usd);
                    let attributed: f64 = broker
                        .positions
                        .values()
                        .map(|position| {
                            let ledger = match position.asset_class {
                                AssetClass::Major => &mut sleeves.major,
                                AssetClass::Altcoin => &mut sleeves.altcoin,
                            };
                            ledger.realized_pnl_usd += position.realized_pnl_usd;
                            position.realized_pnl_usd
                        })
                        .sum();
                    sleeves.unattributed_realized_pnl_usd = broker.realized - attributed;
                    broker.sleeves = Some(sleeves);
                }
                Ok(broker)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::with_risk(config, risk))
            }
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
    fn ledgers(&self) -> &SleeveLedgers {
        self.sleeves
            .as_ref()
            .expect("sleeve ledgers initialized by constructor or migration")
    }
    fn ledger_mut(&mut self, asset_class: AssetClass) -> &mut SleeveLedger {
        let sleeves = self
            .sleeves
            .as_mut()
            .expect("sleeve ledgers initialized by constructor or migration");
        match asset_class {
            AssetClass::Major => &mut sleeves.major,
            AssetClass::Altcoin => &mut sleeves.altcoin,
        }
    }
    fn sleeve_snapshot(&self, frame: &MarketFrame, asset_class: AssetClass) -> SleeveSnapshot {
        let ledger = match asset_class {
            AssetClass::Major => &self.ledgers().major,
            AssetClass::Altcoin => &self.ledgers().altcoin,
        };
        let mut unrealized = 0.0;
        let mut gross = 0.0;
        let mut open_positions = 0;
        for position in self
            .positions
            .values()
            .filter(|position| position.asset_class == asset_class)
        {
            let Some(instrument) = frame.instrument(&position.symbol) else {
                continue;
            };
            unrealized += position.side.sign()
                * (instrument.price - position.entry_price)
                * position.remaining_quantity;
            gross += instrument.price * position.remaining_quantity;
            open_positions += 1;
        }
        let cash = ledger.initial_equity_usd + ledger.realized_pnl_usd;
        let equity = cash + unrealized;
        let peak = ledger.peak_equity_usd.max(equity);
        SleeveSnapshot {
            initial_equity_usd: ledger.initial_equity_usd,
            equity_usd: equity,
            cash_usd: cash,
            realized_pnl_usd: ledger.realized_pnl_usd,
            unrealized_pnl_usd: unrealized,
            fees_usd: ledger.fees_usd,
            peak_equity_usd: peak,
            drawdown_pct: ((peak - equity) / peak.max(1.0)).max(0.0),
            daily_loss_pct: ((ledger.risk_day_start_equity_usd - equity)
                / ledger.risk_day_start_equity_usd.max(1.0))
            .max(0.0),
            gross_exposure_usd: gross,
            open_positions,
        }
    }
    pub fn sleeve_snapshots(&self, frame: &MarketFrame) -> SleeveSnapshots {
        SleeveSnapshots {
            major: self.sleeve_snapshot(frame, AssetClass::Major),
            altcoin: self.sleeve_snapshot(frame, AssetClass::Altcoin),
            unattributed_realized_pnl_usd: self.ledgers().unattributed_realized_pnl_usd,
        }
    }
    fn roll_sleeve_day(&mut self, frame: &MarketFrame) {
        let day = Self::current_day(frame.as_of_ms);
        let major_equity = self.sleeve_snapshot(frame, AssetClass::Major).equity_usd;
        let alt_equity = self.sleeve_snapshot(frame, AssetClass::Altcoin).equity_usd;
        for (asset_class, equity) in [
            (AssetClass::Major, major_equity),
            (AssetClass::Altcoin, alt_equity),
        ] {
            let ledger = self.ledger_mut(asset_class);
            if ledger.risk_day != day {
                ledger.risk_day.clone_from(&day);
                ledger.risk_day_start_equity_usd = equity;
            }
        }
    }
    fn update_sleeve_peaks(&mut self, frame: &MarketFrame) {
        for asset_class in [AssetClass::Major, AssetClass::Altcoin] {
            let equity = self.sleeve_snapshot(frame, asset_class).equity_usd;
            let ledger = self.ledger_mut(asset_class);
            ledger.peak_equity_usd = ledger.peak_equity_usd.max(equity);
        }
    }
    pub fn positions(&self) -> &BTreeMap<String, PaperPosition> {
        &self.positions
    }
    pub fn position_snapshots<'a>(
        &'a self,
        frame: &MarketFrame,
    ) -> BTreeMap<&'a str, PositionSnapshot<'a>> {
        self.positions
            .iter()
            .map(|(symbol, position)| {
                let current_price = frame.instrument(symbol).map(|instrument| instrument.price);
                let current_notional_usd =
                    current_price.map(|price| price * position.remaining_quantity);
                let unrealized_pnl_usd = current_price.map(|price| {
                    position.side.sign()
                        * (price - position.entry_price)
                        * position.remaining_quantity
                });
                let entry_notional_usd = position.entry_price * position.remaining_quantity;
                let unrealized_pnl_pct = unrealized_pnl_usd
                    .filter(|_| entry_notional_usd > f64::EPSILON)
                    .map(|pnl| pnl / entry_notional_usd);
                (
                    symbol.as_str(),
                    PositionSnapshot {
                        position,
                        current_price,
                        current_notional_usd,
                        unrealized_pnl_usd,
                        unrealized_pnl_pct,
                    },
                )
            })
            .collect()
    }
    pub fn has_seen(&self, candidate_id: &str) -> bool {
        self.seen.contains(candidate_id)
    }
    pub fn recipe_gate_status(&self, recipe: &str, now_ms: i64) -> RecipeGateStatus {
        let outcomes = self
            .recipe_performance
            .get(recipe)
            .map(|performance| performance.outcomes.as_slice())
            .unwrap_or_default();
        let start = outcomes.len().saturating_sub(self.risk.rolling_pf_window);
        let window = &outcomes[start..];
        let profit: f64 = window
            .iter()
            .filter(|outcome| outcome.pnl_usd > 0.0)
            .map(|outcome| outcome.pnl_usd)
            .sum();
        let loss: f64 = -window
            .iter()
            .filter(|outcome| outcome.pnl_usd < 0.0)
            .map(|outcome| outcome.pnl_usd)
            .sum::<f64>();
        let rolling_profit_factor = (loss > f64::EPSILON).then_some(profit / loss);
        let gate_failed = window.len() >= self.risk.rolling_pf_min_trades
            && loss > f64::EPSILON
            && rolling_profit_factor.unwrap_or(0.0) < self.risk.rolling_pf_floor;
        let next_probe_ms = gate_failed.then(|| {
            window.last().map(|outcome| outcome.exit_ms).unwrap_or(0)
                + i64::from(self.risk.rolling_pf_cooldown_minutes) * 60_000
        });
        RecipeGateStatus {
            allowed: !gate_failed || next_probe_ms.is_some_and(|probe| now_ms >= probe),
            completed_trades: window.len(),
            rolling_profit_factor,
            rolling_net_pnl_usd: window.iter().map(|outcome| outcome.pnl_usd).sum(),
            next_probe_ms,
        }
    }
    pub fn recipe_gate_snapshots(&self, now_ms: i64) -> BTreeMap<String, RecipeGateStatus> {
        [
            "major_trend_pullback",
            "major_exhaustion_reversal",
            "alt_cross_section_momentum",
            "alt_shock_reversal",
        ]
        .into_iter()
        .map(|recipe| (recipe.into(), self.recipe_gate_status(recipe, now_ms)))
        .collect()
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
        self.roll_sleeve_day(frame);
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
        self.update_sleeve_peaks(frame);
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
        let ledger = self.ledger_mut(position.asset_class);
        ledger.realized_pnl_usd += pnl;
        ledger.fees_usd += exit_fee;
        position.realized_pnl_usd += pnl;
        position.remaining_quantity = (position.remaining_quantity - quantity).max(0.0);
        let complete = position.remaining_quantity <= f64::EPSILON;
        if complete {
            let performance = self
                .recipe_performance
                .entry(position.recipe.clone())
                .or_default();
            performance.outcomes.push(RecipeOutcome {
                exit_ms: ts_ms,
                pnl_usd: position.realized_pnl_usd,
            });
            if performance.outcomes.len() > 100 {
                performance.outcomes.remove(0);
            }
        }
        BrokerEvent {
            kind: if complete {
                "paper_exit".into()
            } else {
                "paper_partial_exit".into()
            },
            payload: serde_json::json!({"ts_ms":ts_ms,"candidate_id":position.candidate_id,"recipe":position.recipe,"asset_class":position.asset_class,"symbol":position.symbol,"side":position.side,"entry_price":position.entry_price,"exit_price":exit,"quantity":quantity,"remaining_quantity":position.remaining_quantity,"gross_pnl_usd":gross,"fee_usd":exit_fee,"pnl_usd":pnl,"reason":reason}),
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
            let recipe = evaluation
                .artifacts
                .values()
                .filter_map(|record| record.artifact.candidate())
                .find(|candidate| candidate.id == plan.candidate_id)
                .map(|candidate| candidate.recipe.as_str())
                .unwrap_or_else(|| recipe_from_candidate(&plan.candidate_id));
            let gate = self.recipe_gate_status(recipe, frame.as_of_ms);
            if !gate.allowed {
                self.seen.insert(plan.candidate_id.clone());
                let asset_class = frame
                    .instrument(&plan.symbol)
                    .map(|instrument| instrument.asset_class);
                events.push(BrokerEvent {
                    kind: "paper_plan_rejected".into(),
                    payload: serde_json::json!({
                        "ts_ms":frame.as_of_ms,
                        "candidate_id":plan.candidate_id,
                        "recipe":recipe,
                        "asset_class":asset_class,
                        "symbol":plan.symbol,
                        "side":plan.side,
                        "reason":"rolling_profit_factor_gate",
                        "rolling_profit_factor":gate.rolling_profit_factor,
                        "rolling_net_pnl_usd":gate.rolling_net_pnl_usd,
                        "completed_trades":gate.completed_trades,
                        "next_probe_ms":gate.next_probe_ms,
                        "paper_only":true,
                    }),
                });
                continue;
            }
            if let Some(event) = self.open(frame, plan, recipe) {
                events.push(event);
            }
        }
        events
    }
    fn open(
        &mut self,
        frame: &MarketFrame,
        plan: &PositionPlan,
        recipe: &str,
    ) -> Option<BrokerEvent> {
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
        let ledger = self.ledger_mut(instrument.asset_class);
        ledger.realized_pnl_usd -= entry_fee;
        ledger.fees_usd += entry_fee;
        self.seen.insert(plan.candidate_id.clone());
        self.positions.insert(
            plan.symbol.clone(),
            PaperPosition {
                candidate_id: plan.candidate_id.clone(),
                recipe: recipe.into(),
                asset_class: instrument.asset_class,
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
            payload: serde_json::json!({"ts_ms":frame.as_of_ms,"candidate_id":plan.candidate_id,"recipe":recipe,"asset_class":instrument.asset_class,"symbol":plan.symbol,"side":plan.side,"entry_price":entry,"quantity":quantity,"notional_usd":plan.notional_usd,"fee_usd":entry_fee,"stop_price":rebased_stop,"paper_only":true}),
        })
    }
}

fn recipe_from_candidate(candidate_id: &str) -> &'static str {
    if candidate_id.contains("trend_pullback") || candidate_id.contains("trend-pullback") {
        "major_trend_pullback"
    } else if candidate_id.contains("exhaustion") {
        "major_exhaustion_reversal"
    } else if candidate_id.contains("cross_section") || candidate_id.contains("cross-section") {
        "alt_cross_section_momentum"
    } else if candidate_id.contains("shock_reversal") || candidate_id.contains("shock-reversal") {
        "alt_shock_reversal"
    } else {
        "unknown"
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
        let mut broker = PaperBroker::with_risk(
            PaperConfig {
                fee_bps_per_side: 0.0,
                slippage_bps_per_side: 0.0,
                ..PaperConfig::default()
            },
            RiskConfig::default(),
        );
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
        let mut broker = PaperBroker::with_risk(
            PaperConfig {
                fee_bps_per_side: 0.0,
                slippage_bps_per_side: 0.0,
                ..PaperConfig::default()
            },
            RiskConfig::default(),
        );
        broker.apply_plans(&frame(1_000_000, 100.0, 100.0, 100.0), &evaluation());
        let events = broker.mark_to_market(&frame(1_900_000, 102.0, 100.0, 103.0));
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, "paper_partial_exit");
        let position = broker.positions().get("BTCUSDT").unwrap();
        assert!((position.remaining_quantity - position.quantity * 0.5).abs() < 1e-9);
        assert!(position.stop_price > position.entry_price);
    }

    #[test]
    fn position_snapshot_marks_unrealized_pnl_at_the_current_frame() {
        let mut broker = PaperBroker::with_risk(
            PaperConfig {
                fee_bps_per_side: 0.0,
                slippage_bps_per_side: 0.0,
                ..PaperConfig::default()
            },
            RiskConfig::default(),
        );
        broker.apply_plans(&frame(1_000_000, 100.0, 100.0, 100.0), &evaluation());

        let marked_frame = frame(1_900_000, 101.0, 100.0, 101.0);
        let snapshots = broker.position_snapshots(&marked_frame);
        let snapshot = snapshots.get("BTCUSDT").unwrap();
        assert_eq!(snapshot.current_price, Some(101.0));
        assert!((snapshot.current_notional_usd.unwrap() - 606.0).abs() < 1e-9);
        assert!((snapshot.unrealized_pnl_usd.unwrap() - 6.0).abs() < 1e-9);
        assert!((snapshot.unrealized_pnl_pct.unwrap() - 0.01).abs() < 1e-9);
    }

    #[test]
    fn state_roundtrip_keeps_positions_and_seen_candidates() {
        let mut broker = PaperBroker::with_risk(PaperConfig::default(), RiskConfig::default());
        broker.apply_plans(&frame(1_000_000, 100.0, 100.0, 100.0), &evaluation());
        let path = std::env::temp_dir().join(format!("greed-paper-{}.json", std::process::id()));
        broker.save(path.to_str().unwrap()).unwrap();
        let restored = PaperBroker::load_or_new(
            PaperConfig::default(),
            RiskConfig::default(),
            path.to_str().unwrap(),
            &["BTCUSDT".into()],
        )
        .unwrap();
        assert!(restored.positions().contains_key("BTCUSDT"));
        assert!(restored.seen.contains("candidate-1"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn sleeve_ledger_attributes_fees_and_pnl_to_major() {
        let mut broker = PaperBroker::with_risk(PaperConfig::default(), RiskConfig::default());
        let entry_frame = frame(1_000_000, 100.0, 100.0, 100.0);
        let events = broker.apply_plans(&entry_frame, &evaluation());
        assert_eq!(events[0].payload["asset_class"], "major");
        assert_eq!(events[0].payload["recipe"], "unknown");
        let after_entry = broker.sleeve_snapshots(&entry_frame);
        assert!(after_entry.major.realized_pnl_usd < 0.0);
        assert_eq!(after_entry.altcoin.realized_pnl_usd, 0.0);

        let exit_frame = frame(1_900_000, 103.0, 103.0, 103.0);
        broker.close_all(&exit_frame, "test_exit");
        let after_exit = broker.sleeve_snapshots(&exit_frame);
        assert!(after_exit.major.realized_pnl_usd > 0.0);
        assert_eq!(after_exit.altcoin.realized_pnl_usd, 0.0);
    }

    #[test]
    fn rolling_pf_gate_halts_then_allows_a_timed_probe() {
        let risk = RiskConfig {
            rolling_pf_window: 5,
            rolling_pf_min_trades: 3,
            rolling_pf_floor: 0.8,
            rolling_pf_cooldown_minutes: 60,
            ..RiskConfig::default()
        };
        let mut broker = PaperBroker::with_risk(PaperConfig::default(), risk);
        broker.recipe_performance.insert(
            "alt_cross_section_momentum".into(),
            RecipePerformance {
                outcomes: vec![
                    RecipeOutcome {
                        exit_ms: 1_000_000,
                        pnl_usd: -2.0,
                    },
                    RecipeOutcome {
                        exit_ms: 2_000_000,
                        pnl_usd: 1.0,
                    },
                    RecipeOutcome {
                        exit_ms: 3_000_000,
                        pnl_usd: -2.0,
                    },
                ],
            },
        );
        let halted = broker.recipe_gate_status("alt_cross_section_momentum", 3_000_001);
        assert!(!halted.allowed);
        assert_eq!(halted.rolling_profit_factor, Some(0.25));
        let probe =
            broker.recipe_gate_status("alt_cross_section_momentum", 3_000_000 + 60 * 60_000);
        assert!(probe.allowed);
    }
}
