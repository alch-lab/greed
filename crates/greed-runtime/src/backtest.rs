use crate::{broker::PaperBroker, config::AppConfig};
use anyhow::{anyhow, Context, Result};
use chrono::{Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use greed_kernel::{
    AccountFrame, AssetClass, BookState, Candle, CandleSeries, DataQuality, DerivativesState,
    ExternalState, InstrumentFrame, MarketFrame, MarketKind, ObservationMeta,
};
use greed_strategy::{build_graph, StrategyConfig};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Semaphore;

const INTERVAL_MS: i64 = 15 * 60_000;
const WARMUP_DAYS: i64 = 2;

#[derive(Clone)]
struct SeriesData {
    perpetual: Vec<Candle>,
    spot: Vec<Candle>,
    oi: BTreeMap<i64, f64>,
    depth: BTreeMap<i64, (f64, f64)>,
}

#[derive(Clone)]
struct DownloadSpec {
    url: String,
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct ResultRow {
    profile: String,
    from: String,
    to: String,
    starting_equity_usd: f64,
    ending_equity_usd: f64,
    net_pnl_usd: f64,
    return_pct: f64,
    max_drawdown_pct: f64,
    entries: u64,
    completed_trades: usize,
    win_rate: Option<f64>,
    profit_factor: Option<f64>,
    fees_usd: f64,
    pnl_by_recipe: BTreeMap<String, f64>,
    pnl_by_recipe_and_side: BTreeMap<String, f64>,
    entries_by_recipe: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub struct BacktestReport {
    source: &'static str,
    data_limitations: Vec<&'static str>,
    training: Vec<ResultRow>,
    selected_profile: String,
    validation: ResultRow,
    validation_profiles: Vec<ResultRow>,
    recommended_parameters: StrategyConfig,
}

#[derive(Default)]
struct Ledger {
    entry_fees: BTreeMap<String, f64>,
    recipe_by_candidate: BTreeMap<String, String>,
    side_by_candidate: BTreeMap<String, String>,
    trade_pnl: BTreeMap<String, f64>,
    fees: f64,
    entries_by_recipe: BTreeMap<String, u64>,
}

pub async fn run(
    config: AppConfig,
    train_from: &str,
    split: &str,
    to: &str,
    cache_dir: &str,
) -> Result<BacktestReport> {
    let train_from = date(train_from)?;
    let split = date(split)?;
    let to = date(to)?;
    if !(train_from < split && split <= to) {
        return Err(anyhow!("expected train_from < split <= to"));
    }
    let download_from = train_from - Duration::days(WARMUP_DAYS);
    let archive = Archive::new(cache_dir)?;
    archive
        .prefetch(
            &config.strategy.majors,
            &config.strategy.altcoins,
            download_from,
            to,
        )
        .await?;
    let data = archive.load(
        &config.strategy.majors,
        &config.strategy.altcoins,
        download_from,
        to,
    )?;
    let profiles = profiles(&config.strategy);
    let mut training = Vec::new();
    for (name, strategy) in &profiles {
        training.push(simulate(
            name,
            strategy,
            &config,
            &data,
            train_from,
            split - Duration::days(1),
        )?);
    }
    let selected = training
        .iter()
        .enumerate()
        .filter(|(_, row)| row.completed_trades >= 5)
        .max_by(|(_, a), (_, b)| score(a).total_cmp(&score(b)))
        .map(|(index, _)| index)
        .unwrap_or_else(|| {
            training
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.net_pnl_usd.total_cmp(&b.net_pnl_usd))
                .map(|(index, _)| index)
                .unwrap_or(0)
        });
    let (selected_name, selected_strategy) = &profiles[selected];
    let mut validation_profiles = Vec::new();
    for (name, strategy) in &profiles {
        validation_profiles.push(simulate(name, strategy, &config, &data, split, to)?);
    }
    let validation = validation_profiles[selected].clone();
    Ok(BacktestReport {
        source: "Binance public data archive (data.binance.vision)",
        data_limitations: vec![
            "15m OHLC cannot reveal intrabar path; stop is conservatively evaluated before take-profit",
            "historical depth is percentage-band depth, not a reconstructable level-2 book",
            "Coinbase premium, ETF flow and liquidation websocket are observation-only because no reliable archive is available",
            "a limited date range is a parameter smoke test, not evidence of durable alpha",
        ],
        training,
        selected_profile: selected_name.clone(),
        validation,
        validation_profiles,
        recommended_parameters: selected_strategy.clone(),
    })
}

fn score(row: &ResultRow) -> f64 {
    row.net_pnl_usd - row.max_drawdown_pct * row.starting_equity_usd * 2.0
        + row.profit_factor.unwrap_or(0.0).min(3.0)
}

fn profiles(base: &StrategyConfig) -> Vec<(String, StrategyConfig)> {
    let mut legacy = base.clone();
    legacy.recipes.cross_opportunity_driven = false;
    legacy.recipes.alt_neutral_anchor_allowed = false;
    let mut balanced = base.clone();
    balanced.recipes.cross_opportunity_driven = true;
    balanced.recipes.alt_neutral_anchor_allowed = true;
    let mut responsive = base.clone();
    responsive.recipes.cross_opportunity_driven = true;
    responsive.recipes.alt_neutral_anchor_allowed = true;
    responsive.primitives.trend_min_return_pct = 0.0045;
    responsive.primitives.breadth_threshold = 0.006;
    responsive.primitives.breadth_horizon_bars = 48;
    responsive.primitives.breadth_min_participation = 0.55;
    responsive.recipes.pullback_min_pct = 0.0015;
    responsive.recipes.exhaustion_move_pct = 0.014;
    responsive.recipes.shock_min_return_pct = 0.035;
    responsive.recipes.shock_min_reversal_pct = 0.004;
    responsive.recipes.shock_min_volume_ratio = 1.8;
    responsive.recipes.cross_rebalance_bars = 16;
    responsive.risk.alt_cross_stop_pct = 0.012;
    responsive.risk.alt_cross_take_profit_pct = 0.020;
    responsive.risk.alt_cross_max_hold_minutes = 480;
    let mut selective = base.clone();
    selective.recipes.cross_opportunity_driven = true;
    selective.recipes.alt_neutral_anchor_allowed = false;
    selective.primitives.trend_min_return_pct = 0.008;
    selective.primitives.trend_horizon_bars = 96;
    selective.primitives.trend_min_efficiency = 0.20;
    selective.primitives.breadth_threshold = 0.012;
    selective.primitives.breadth_horizon_bars = 96;
    selective.primitives.breadth_min_participation = 0.65;
    selective.recipes.pullback_min_pct = 0.003;
    selective.recipes.exhaustion_move_pct = 0.024;
    selective.recipes.shock_min_return_pct = 0.06;
    selective.recipes.shock_min_reversal_pct = 0.008;
    selective.recipes.shock_min_volume_ratio = 3.0;
    selective.recipes.cross_names = 2;
    selective.recipes.cross_rebalance_bars = 32;
    selective.risk.alt_cross_stop_pct = 0.018;
    selective.risk.alt_cross_take_profit_pct = 0.03;
    selective.risk.alt_cross_max_hold_minutes = 960;
    vec![
        ("legacy_fixed_strict".into(), legacy),
        ("opportunity_balanced".into(), balanced),
        ("opportunity_responsive".into(), responsive),
        ("opportunity_selective".into(), selective),
    ]
}

fn simulate(
    profile: &str,
    strategy: &StrategyConfig,
    app: &AppConfig,
    data: &BTreeMap<String, SeriesData>,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<ResultRow> {
    let start_ms = day_start(from);
    let end_ms = day_start(to + Duration::days(1)) - 1;
    let mut times = BTreeSet::new();
    for value in data.values() {
        times.extend(
            value
                .perpetual
                .iter()
                .filter(|bar| bar.close_ms >= start_ms && bar.close_ms <= end_ms)
                .map(|bar| bar.close_ms),
        );
    }
    let mut graph = build_graph(strategy)?;
    let mut broker = PaperBroker::with_risk(app.paper.clone(), strategy.risk.clone());
    let mut ledger = Ledger::default();
    let mut peak = app.paper.initial_cash_usd;
    let mut max_drawdown: f64 = 0.0;
    let mut last_frame = None;
    for ts in times {
        let mut frame = frame_at(ts, strategy, data, broker.account_frame());
        frame.account = broker.marked_account(&frame);
        for event in broker.mark_to_market(&frame) {
            ledger.record(&event.kind, &event.payload);
        }
        frame.account = broker.marked_account(&frame);
        let evaluation = graph.evaluate(&frame)?;
        for event in broker.apply_plans(&frame, &evaluation) {
            ledger.record(&event.kind, &event.payload);
        }
        let equity = broker.marked_account(&frame).equity_usd;
        peak = peak.max(equity);
        max_drawdown = max_drawdown.max((peak - equity) / peak.max(1.0));
        last_frame = Some(frame);
    }
    if let Some(frame) = last_frame.as_ref() {
        for event in broker.close_all(frame, "backtest_end") {
            ledger.record(&event.kind, &event.payload);
        }
    }
    let ending = broker.account_frame().equity_usd;
    let completed: Vec<_> = ledger
        .trade_pnl
        .values()
        .filter(|value| value.is_finite())
        .copied()
        .collect();
    let wins = completed.iter().filter(|value| **value > 0.0).count();
    let profit: f64 = completed.iter().filter(|v| **v > 0.0).sum();
    let loss: f64 = -completed.iter().filter(|v| **v < 0.0).sum::<f64>();
    let mut pnl_by_recipe = BTreeMap::new();
    let mut pnl_by_recipe_and_side = BTreeMap::new();
    for (candidate, pnl) in &ledger.trade_pnl {
        if let Some(recipe) = ledger.recipe_by_candidate.get(candidate) {
            *pnl_by_recipe.entry(recipe.clone()).or_default() += pnl;
            let side = ledger
                .side_by_candidate
                .get(candidate)
                .map(String::as_str)
                .unwrap_or("unknown");
            *pnl_by_recipe_and_side
                .entry(format!("{recipe}:{side}"))
                .or_default() += pnl;
        }
    }
    Ok(ResultRow {
        profile: profile.into(),
        from: from.to_string(),
        to: to.to_string(),
        starting_equity_usd: app.paper.initial_cash_usd,
        ending_equity_usd: ending,
        net_pnl_usd: ending - app.paper.initial_cash_usd,
        return_pct: ending / app.paper.initial_cash_usd - 1.0,
        max_drawdown_pct: max_drawdown,
        entries: ledger.entry_fees.len() as u64,
        completed_trades: completed.len(),
        win_rate: (!completed.is_empty()).then_some(wins as f64 / completed.len() as f64),
        profit_factor: (loss > 0.0).then_some(profit / loss),
        fees_usd: ledger.fees,
        pnl_by_recipe,
        pnl_by_recipe_and_side,
        entries_by_recipe: ledger.entries_by_recipe,
    })
}

impl Ledger {
    fn record(&mut self, kind: &str, payload: &serde_json::Value) {
        let id = payload["candidate_id"].as_str().unwrap_or("").to_owned();
        let fee = payload["fee_usd"].as_f64().unwrap_or(0.0);
        self.fees += fee;
        match kind {
            "paper_entry" => {
                self.entry_fees.insert(id.clone(), fee);
                let recipe = candidate_recipe(&id);
                self.recipe_by_candidate.insert(id, recipe.clone());
                self.side_by_candidate.insert(
                    payload["candidate_id"].as_str().unwrap_or("").into(),
                    payload["side"].as_str().unwrap_or("unknown").into(),
                );
                *self.entries_by_recipe.entry(recipe).or_default() += 1;
            }
            "paper_partial_exit" | "paper_exit" => {
                *self.trade_pnl.entry(id.clone()).or_default() +=
                    payload["pnl_usd"].as_f64().unwrap_or(0.0);
                if kind == "paper_exit" {
                    *self.trade_pnl.entry(id.clone()).or_default() -=
                        self.entry_fees.get(&id).copied().unwrap_or(0.0);
                }
            }
            _ => {}
        }
    }
}

fn candidate_recipe(id: &str) -> String {
    if id.contains("trend_pullback") {
        "major_trend_pullback"
    } else if id.contains("exhaustion") {
        "major_exhaustion_reversal"
    } else if id.contains("cross_section") {
        "alt_cross_section_momentum"
    } else if id.contains("shock_reversal") {
        "alt_shock_reversal"
    } else {
        "unknown"
    }
    .into()
}

fn frame_at(
    ts: i64,
    strategy: &StrategyConfig,
    data: &BTreeMap<String, SeriesData>,
    account: AccountFrame,
) -> MarketFrame {
    let mut instruments = BTreeMap::new();
    for symbol in strategy.majors.iter().chain(strategy.altcoins.iter()) {
        let Some(source) = data.get(symbol) else {
            continue;
        };
        let perpetual = history(&source.perpetual, ts);
        let Some(last) = perpetual.last() else {
            continue;
        };
        let price = last.close;
        let spot_values = history(&source.spot, ts);
        let oi_now = source.oi.range(..=ts).next_back().map(|(_, value)| *value);
        let oi_prior = source
            .oi
            .range(..=ts - 10 * 60_000)
            .next_back()
            .map(|(_, value)| *value);
        let oi_change = oi_now.zip(oi_prior).map(|(now, prior)| now / prior - 1.0);
        let depth = source
            .depth
            .range(..=ts)
            .next_back()
            .filter(|(depth_ts, _)| ts - **depth_ts <= 30 * 60_000)
            .map(|(_, value)| *value);
        let meta = |quality| ObservationMeta {
            event_ms: ts,
            received_ms: ts,
            expires_ms: ts + INTERVAL_MS,
            source: "binance_archive".into(),
            quality,
        };
        let major = strategy.majors.contains(symbol);
        instruments.insert(
            symbol.clone(),
            InstrumentFrame {
                symbol: symbol.clone(),
                asset_class: if major {
                    AssetClass::Major
                } else {
                    AssetClass::Altcoin
                },
                price,
                spot: major.then_some(CandleSeries {
                    venue: "binance".into(),
                    market: MarketKind::Spot,
                    interval_ms: INTERVAL_MS,
                    meta: meta(if spot_values.is_empty() {
                        DataQuality::Missing
                    } else {
                        DataQuality::Complete
                    }),
                    values: spot_values,
                }),
                perpetual: CandleSeries {
                    venue: "binance".into(),
                    market: MarketKind::Perpetual,
                    interval_ms: INTERVAL_MS,
                    meta: meta(DataQuality::Complete),
                    values: perpetual,
                },
                book: depth.map(|(bid_depth, ask_depth)| BookState {
                    meta: meta(DataQuality::Partial),
                    bid: price * 0.99995,
                    ask: price * 1.00005,
                    // One-percent band depth is discounted heavily so it is
                    // not mistaken for immediately executable top-book depth.
                    bid_depth_usd: bid_depth * 0.05,
                    ask_depth_usd: ask_depth * 0.05,
                    expected_buy_slippage_bps: None,
                    expected_sell_slippage_bps: None,
                    bids: vec![],
                    asks: vec![],
                }),
                derivatives: oi_now.map(|oi| DerivativesState {
                    meta: meta(DataQuality::Complete),
                    open_interest_usd: Some(oi),
                    open_interest_change_pct: oi_change,
                    funding_rate: None,
                    basis_pct: None,
                    long_liquidations_usd: None,
                    short_liquidations_usd: None,
                }),
                external: major.then_some(ExternalState {
                    meta: meta(DataQuality::Missing),
                    coinbase_raw_premium_pct: None,
                    coinbase_true_premium_pct: None,
                    etf_daily_flow_usd: None,
                    etf_rolling_5d_flow_usd: None,
                    cme_basis_pct: None,
                }),
            },
        );
    }
    MarketFrame {
        as_of_ms: ts,
        instruments,
        account,
    }
}

fn history(source: &[Candle], ts: i64) -> Vec<Candle> {
    source
        .iter()
        .filter(|bar| bar.close_ms <= ts)
        .rev()
        .take(200)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
}

struct Archive {
    cache: PathBuf,
    http: reqwest::Client,
}

impl Archive {
    fn new(cache: &str) -> Result<Self> {
        Ok(Self {
            cache: cache.into(),
            http: reqwest::Client::builder()
                .user_agent("greed-backtest/0.1")
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
        })
    }

    async fn prefetch(
        &self,
        majors: &[String],
        alts: &[String],
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<()> {
        let specs = self.specs(majors, alts, from, to);
        let semaphore = Arc::new(Semaphore::new(8));
        let mut tasks = tokio::task::JoinSet::new();
        for spec in specs.into_iter().filter(|spec| !spec.path.exists()) {
            let permit = semaphore.clone().acquire_owned().await?;
            let http = self.http.clone();
            tasks.spawn(async move {
                let _permit = permit;
                let response = http.get(&spec.url).send().await?;
                if !response.status().is_success() {
                    return Err(anyhow!("{} returned {}", spec.url, response.status()));
                }
                let bytes = response.bytes().await?;
                if let Some(parent) = spec.path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let temp = spec.path.with_extension("tmp");
                std::fs::write(&temp, &bytes)?;
                std::fs::rename(temp, &spec.path)?;
                Result::<()>::Ok(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(())
    }

    fn specs(
        &self,
        majors: &[String],
        alts: &[String],
        from: NaiveDate,
        to: NaiveDate,
    ) -> Vec<DownloadSpec> {
        let all: Vec<_> = majors.iter().chain(alts).cloned().collect();
        let mut out = Vec::new();
        let mut day = from;
        while day <= to {
            for symbol in &all {
                out.push(self.spec(
                    "futures-klines",
                    symbol,
                    day,
                    format!("data/futures/um/daily/klines/{symbol}/15m/{symbol}-15m-{day}.zip"),
                ));
                out.push(self.spec(
                    "metrics",
                    symbol,
                    day,
                    format!("data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{day}.zip"),
                ));
                out.push(self.spec(
                    "depth",
                    symbol,
                    day,
                    format!(
                        "data/futures/um/daily/bookDepth/{symbol}/{symbol}-bookDepth-{day}.zip"
                    ),
                ));
            }
            for symbol in majors {
                out.push(self.spec(
                    "spot-klines",
                    symbol,
                    day,
                    format!("data/spot/daily/klines/{symbol}/15m/{symbol}-15m-{day}.zip"),
                ));
            }
            day += Duration::days(1);
        }
        out
    }

    fn spec(&self, kind: &str, symbol: &str, day: NaiveDate, remote: String) -> DownloadSpec {
        DownloadSpec {
            url: format!("https://data.binance.vision/{remote}"),
            path: self
                .cache
                .join(kind)
                .join(symbol)
                .join(format!("{day}.zip")),
        }
    }

    fn load(
        &self,
        majors: &[String],
        alts: &[String],
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<BTreeMap<String, SeriesData>> {
        let mut result = BTreeMap::new();
        for symbol in majors.iter().chain(alts) {
            let mut data = SeriesData {
                perpetual: vec![],
                spot: vec![],
                oi: BTreeMap::new(),
                depth: BTreeMap::new(),
            };
            let mut day = from;
            while day <= to {
                data.perpetual.extend(parse_klines(&read_zip(
                    &self
                        .cache
                        .join("futures-klines")
                        .join(symbol)
                        .join(format!("{day}.zip")),
                )?)?);
                if majors.contains(symbol) {
                    data.spot.extend(parse_klines(&read_zip(
                        &self
                            .cache
                            .join("spot-klines")
                            .join(symbol)
                            .join(format!("{day}.zip")),
                    )?)?);
                }
                data.oi.extend(parse_metrics(&read_zip(
                    &self
                        .cache
                        .join("metrics")
                        .join(symbol)
                        .join(format!("{day}.zip")),
                )?)?);
                data.depth.extend(parse_depth(&read_zip(
                    &self
                        .cache
                        .join("depth")
                        .join(symbol)
                        .join(format!("{day}.zip")),
                )?)?);
                day += Duration::days(1);
            }
            data.perpetual.sort_by_key(|bar| bar.close_ms);
            data.spot.sort_by_key(|bar| bar.close_ms);
            result.insert(symbol.clone(), data);
        }
        Ok(result)
    }
}

fn read_zip(path: &Path) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut archive = zip::ZipArchive::new(file)?;
    let mut bytes = Vec::new();
    archive.by_index(0)?.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn parse_klines(bytes: &[u8]) -> Result<Vec<Candle>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(Cursor::new(bytes));
    let mut out = Vec::new();
    for row in reader.records() {
        let row = row?;
        let Some(mut open_ms) = row.get(0).and_then(|v| v.parse::<i64>().ok()) else {
            continue;
        };
        let mut close_ms = row.get(6).context("kline close time")?.parse::<i64>()?;
        if open_ms > 10_000_000_000_000 {
            open_ms /= 1_000;
            close_ms /= 1_000;
        }
        out.push(Candle {
            open_ms,
            open: number(&row, 1)?,
            high: number(&row, 2)?,
            low: number(&row, 3)?,
            close: number(&row, 4)?,
            quote_volume: number(&row, 7)?,
            close_ms,
            taker_buy_quote: Some(number(&row, 10)?),
            closed: true,
        });
    }
    Ok(out)
}

fn parse_metrics(bytes: &[u8]) -> Result<BTreeMap<i64, f64>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(bytes));
    let mut out = BTreeMap::new();
    for row in reader.records() {
        let row = row?;
        let ts = NaiveDateTime::parse_from_str(&row[0], "%Y-%m-%d %H:%M:%S")?
            .and_utc()
            .timestamp_millis();
        out.insert(ts, row[3].parse()?);
    }
    Ok(out)
}

fn parse_depth(bytes: &[u8]) -> Result<BTreeMap<i64, (f64, f64)>> {
    let mut reader = csv::Reader::from_reader(Cursor::new(bytes));
    let mut raw: BTreeMap<i64, (Option<f64>, Option<f64>)> = BTreeMap::new();
    for row in reader.records() {
        let row = row?;
        let pct: f64 = row[1].parse()?;
        if (pct.abs() - 1.0).abs() > 0.01 {
            continue;
        }
        let ts = NaiveDateTime::parse_from_str(&row[0], "%Y-%m-%d %H:%M:%S")?
            .and_utc()
            .timestamp_millis();
        let value: f64 = row[3].parse()?;
        let entry = raw.entry(ts).or_default();
        if pct < 0.0 {
            entry.0 = Some(value);
        } else {
            entry.1 = Some(value);
        }
    }
    Ok(raw
        .into_iter()
        .filter_map(|(ts, (bid, ask))| Some((ts, (bid?, ask?))))
        .collect())
}

fn number(row: &csv::StringRecord, index: usize) -> Result<f64> {
    Ok(row.get(index).context("missing numeric column")?.parse()?)
}

fn date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(Into::into)
}

fn day_start(value: NaiveDate) -> i64 {
    Utc.from_utc_datetime(&value.and_hms_opt(0, 0, 0).unwrap())
        .timestamp_millis()
}
