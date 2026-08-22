use anyhow::{Context, Result};
use greed_kernel::{MarketFrame, MarketKind};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

#[derive(Default)]
pub struct SampleRecorder {
    seen_candles: BTreeSet<String>,
}

impl SampleRecorder {
    pub fn record(&mut self, journal: &Journal, frame: &MarketFrame) -> Result<()> {
        for instrument in frame.instruments.values() {
            for series in instrument
                .spot
                .iter()
                .chain(std::iter::once(&instrument.perpetual))
            {
                for bar in series.values.iter().filter(|bar| bar.closed) {
                    let market = match series.market {
                        MarketKind::Spot => "spot",
                        MarketKind::Perpetual => "perpetual",
                        MarketKind::Futures => "futures",
                        MarketKind::Etf => "etf",
                    };
                    let key = format!("{}:{market}:{}", instrument.symbol, bar.close_ms);
                    if self.seen_candles.insert(key) {
                        journal.append(
                            "market_candle",
                            serde_json::json!({"symbol":instrument.symbol,"venue":series.venue,"market":series.market,"bar":bar,"meta":series.meta}),
                        )?;
                    }
                }
            }
            journal.append(
                "market_snapshot",
                serde_json::json!({
                    "as_of_ms":frame.as_of_ms,
                    "symbol":instrument.symbol,
                    "asset_class":instrument.asset_class,
                    "price":instrument.price,
                    "book":instrument.book,
                    "derivatives":instrument.derivatives,
                    "external":instrument.external
                }),
            )?;
        }
        Ok(())
    }
}

pub struct Journal {
    file: Mutex<File>,
}
impl Journal {
    pub fn new(path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open journal {path}"))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
    pub fn append(&self, kind: &str, payload: Value) -> Result<()> {
        let value = serde_json::json!({"recorded_ms":chrono::Utc::now().timestamp_millis(),"kind":kind,"payload":payload});
        let mut file = self.file.lock().expect("journal mutex poisoned");
        serde_json::to_writer(&mut *file, &value)?;
        file.write_all(b"\n")?;
        file.flush()?;
        Ok(())
    }
}

pub struct StatusWriter {
    path: PathBuf,
}
impl StatusWriter {
    pub fn new(path: &str) -> Self {
        Self { path: path.into() }
    }
    pub fn write<T: Serialize>(&self, value: &T) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("tmp");
        let data = serde_json::to_vec_pretty(value)?;
        std::fs::write(&temp, data)?;
        std::fs::rename(temp, &self.path)?;
        Ok(())
    }
}
