use anyhow::{Context, Result};
use greed_kernel::{MarketFrame, MarketKind};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

const SNAPSHOT_INTERVAL_MS: i64 = 60_000;
const JOURNAL_MAX_BYTES: u64 = 256 * 1024 * 1024;
const JOURNAL_ROTATIONS: usize = 4;

#[derive(Default)]
pub struct SampleRecorder {
    seen_candles: BTreeSet<String>,
    last_snapshot_ms: BTreeMap<String, i64>,
}

impl SampleRecorder {
    pub fn record(&mut self, journal: &Journal, frame: &MarketFrame) -> Result<()> {
        for instrument in frame.instruments.values() {
            for series in std::iter::once(&instrument.perpetual)
                .chain(instrument.fast_perpetual.iter())
                .chain(instrument.micro_perpetual.iter())
            {
                // The REST bootstrap can contain 1,200 bars per interval. Those
                // bars are inputs, not observations made by this run. Persist
                // only the newest closed bar and then append each newly closed
                // bar once as the websocket advances.
                if let Some(bar) = series.values.iter().rev().find(|bar| bar.closed) {
                    let market = match series.market {
                        MarketKind::Perpetual => "perpetual",
                    };
                    let key = format!(
                        "{}:{market}:{}:{}",
                        instrument.symbol, series.interval_ms, bar.close_ms
                    );
                    if self.seen_candles.insert(key) {
                        journal.append(
                            "market_candle",
                            serde_json::json!({"symbol":instrument.symbol,"venue":series.venue,"market":series.market,"interval_ms":series.interval_ms,"bar":bar,"meta":series.meta}),
                        )?;
                    }
                }
            }
            let last_snapshot = self
                .last_snapshot_ms
                .get(&instrument.symbol)
                .copied()
                .unwrap_or_default();
            if frame.as_of_ms - last_snapshot >= SNAPSHOT_INTERVAL_MS {
                journal.append(
                    "market_snapshot",
                    serde_json::json!({
                        "as_of_ms":frame.as_of_ms,
                        "symbol":instrument.symbol,
                        "price":instrument.price,
                        "last_5m":instrument.fast_perpetual.as_ref().and_then(|series|series.values.last()),
                        "last_1m":instrument.micro_perpetual.as_ref().and_then(|series|series.values.last()),
                        "book":instrument.book,
                        "microstructure":instrument.microstructure
                    }),
                )?;
                self.last_snapshot_ms
                    .insert(instrument.symbol.clone(), frame.as_of_ms);
            }
        }
        Ok(())
    }
}

pub struct Journal {
    path: PathBuf,
    file: Mutex<JournalFile>,
    max_bytes: u64,
    rotations: usize,
}

struct JournalFile {
    file: File,
    bytes: u64,
}
impl Journal {
    pub fn new(path: &str) -> Result<Self> {
        Self::with_limits(path, JOURNAL_MAX_BYTES, JOURNAL_ROTATIONS)
    }

    fn with_limits(path: &str, max_bytes: u64, rotations: usize) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open journal {path}"))?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            path: path.into(),
            file: Mutex::new(JournalFile { file, bytes }),
            max_bytes,
            rotations,
        })
    }
    pub fn append(&self, kind: &str, payload: Value) -> Result<()> {
        let value = serde_json::json!({"recorded_ms":chrono::Utc::now().timestamp_millis(),"kind":kind,"payload":payload});
        let mut line = serde_json::to_vec(&value)?;
        line.push(b'\n');
        let mut state = self.file.lock().expect("journal mutex poisoned");
        if state.bytes > 0 && state.bytes + line.len() as u64 > self.max_bytes {
            state.file.flush()?;
            rotate_files(&self.path, self.rotations)?;
            state.file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
            state.bytes = 0;
        }
        state.file.write_all(&line)?;
        state.file.flush()?;
        state.bytes += line.len() as u64;
        Ok(())
    }
}

fn rotate_files(path: &Path, rotations: usize) -> Result<()> {
    if rotations == 0 {
        std::fs::remove_file(path).ok();
        return Ok(());
    }
    for index in (1..=rotations).rev() {
        let source = if index == 1 {
            path.to_path_buf()
        } else {
            PathBuf::from(format!("{}.{}", path.display(), index - 1))
        };
        let destination = PathBuf::from(format!("{}.{}", path.display(), index));
        if source.exists() {
            if destination.exists() {
                std::fs::remove_file(&destination)?;
            }
            std::fs::rename(source, destination)?;
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_rotates_before_exceeding_its_bound() {
        let directory = std::env::temp_dir().join(format!(
            "greed-journal-test-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("events.jsonl");
        let journal = Journal::with_limits(path.to_str().unwrap(), 180, 2).unwrap();
        for index in 0..10 {
            journal
                .append("test", serde_json::json!({"index":index,"value":"payload"}))
                .unwrap();
        }
        assert!(path.exists());
        assert!(PathBuf::from(format!("{}.1", path.display())).exists());
        assert!(PathBuf::from(format!("{}.2", path.display())).exists());
        assert!(std::fs::metadata(&path).unwrap().len() <= 180);
        drop(journal);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
