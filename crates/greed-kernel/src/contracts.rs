use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    Perpetual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn sign(self) -> f64 {
        match self {
            Self::Buy => 1.0,
            Self::Sell => -1.0,
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataQuality {
    Missing,
    Stale,
    Partial,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservationMeta {
    pub event_ms: i64,
    pub received_ms: i64,
    pub expires_ms: i64,
    pub source: String,
    pub quality: DataQuality,
}

impl ObservationMeta {
    pub fn usable_at(&self, now_ms: i64) -> bool {
        self.quality >= DataQuality::Partial && now_ms <= self.expires_ms
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub open_ms: i64,
    pub close_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub quote_volume: f64,
    /// Quote notional initiated by buyers.  `None` means the venue did not
    /// provide aggressor attribution, not zero buying.
    pub taker_buy_quote: Option<f64>,
    pub closed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleSeries {
    pub venue: String,
    pub market: MarketKind,
    pub interval_ms: i64,
    pub meta: ObservationMeta,
    pub values: Vec<Candle>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookState {
    pub meta: ObservationMeta,
    pub bid: f64,
    pub ask: f64,
    pub bid_depth_usd: f64,
    pub ask_depth_usd: f64,
    pub expected_buy_slippage_bps: Option<f64>,
    pub expected_sell_slippage_bps: Option<f64>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativesState {
    pub meta: ObservationMeta,
    pub open_interest_usd: Option<f64>,
    pub open_interest_change_pct: Option<f64>,
    pub funding_rate: Option<f64>,
    pub basis_pct: Option<f64>,
    pub long_liquidations_usd: Option<f64>,
    pub short_liquidations_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicrostructureState {
    pub meta: ObservationMeta,
    pub buy_notional_60s: f64,
    pub sell_notional_60s: f64,
    pub long_liquidations_60s: f64,
    pub short_liquidations_60s: f64,
}

impl MicrostructureState {
    pub fn trade_imbalance(&self) -> Option<f64> {
        let total = self.buy_notional_60s + self.sell_notional_60s;
        (total > 0.0).then_some((self.buy_notional_60s - self.sell_notional_60s) / total)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossVenueState {
    pub meta: ObservationMeta,
    pub hyper_mark_price: f64,
    pub hyper_open_interest_usd: f64,
    pub hyper_funding_per_hour: f64,
    pub hyper_premium_pct: Option<f64>,
    pub binance_funding_per_hour: Option<f64>,
    pub funding_gap_per_hour: Option<f64>,
    pub mark_premium_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentFrame {
    pub symbol: String,
    pub price: f64,
    pub perpetual: CandleSeries,
    /// Optional short-interval perpetual candles used for execution timing.
    pub fast_perpetual: Option<CandleSeries>,
    /// Optional one-minute websocket observations collected for execution research.
    /// These remain observation-only until a survivorship-safe backtest supports them.
    pub micro_perpetual: Option<CandleSeries>,
    pub book: Option<BookState>,
    pub derivatives: Option<DerivativesState>,
    pub microstructure: Option<MicrostructureState>,
    pub cross_venue: Option<CrossVenueState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountFrame {
    pub equity_usd: f64,
    pub cash_usd: f64,
    pub realized_pnl_usd: f64,
    pub peak_equity_usd: f64,
    pub risk_day_start_equity_usd: f64,
    pub gross_exposure_usd: f64,
    pub open_positions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFrame {
    pub as_of_ms: i64,
    pub instruments: BTreeMap<String, InstrumentFrame>,
    pub account: AccountFrame,
}

impl MarketFrame {
    pub fn instrument(&self, symbol: &str) -> Option<&InstrumentFrame> {
        self.instruments.get(symbol)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalEvent {
    Candle {
        symbol: String,
        venue: String,
        market: MarketKind,
        value: Candle,
        meta: ObservationMeta,
    },
    OpenInterest {
        symbol: String,
        value_usd: f64,
        meta: ObservationMeta,
    },
    Funding {
        symbol: String,
        value: f64,
        meta: ObservationMeta,
    },
    Account {
        value: AccountFrame,
        meta: ObservationMeta,
    },
}
