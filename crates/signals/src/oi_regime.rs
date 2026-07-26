//! OI 四象限信号（PR-9）。
//!
//! 30m 柱：`ΔOI% = (OI_t − OI_{t−1}) / OI_{t−1}`（±0.5% 噪音带内视为 0），
//! 结合柱内价格收益 r 的符号划分象限：
//!
//! | ΔOI | r   | 象限 | 含义 |
//! |-----|-----|------|------|
//! | >0  | >0  | long_open     | 多头开仓（增仓上涨） |
//! | >0  | <0  | short_open    | 空头开仓（增仓下跌） |
//! | <0  | >0  | short_squeeze | 逼空（减仓上涨）     |
//! | <0  | <0  | long_kill     | 杀多（减仓下跌）     |
//!
//! **重置检测**：单柱 ΔOI < −oi_reset_pct(3%) → 当日观望（reset=true，当日不再发象限信号）。
//!
//! 消费 `Event::Oi`（持仓量刻度）与 `Event::Trade`（价格刻度）。
//! 回测无 OI 数据时保持静默；实盘/含 OI 数据时按 30m 柱收盘发 `OiQuadrant` 信号。

use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::Event;

const BUCKET_MS: i64 = 30 * 60_000;
const DAY_MS: i64 = 86_400_000;

pub struct OiQuadrant {
    /// 重置阈值（%）：单柱 ΔOI 跌幅超过该值 → 当日观望
    oi_reset_pct: f64,
    /// ΔOI 噪音带（%）：|ΔOI| 小于该值视为 0（无象限）
    noise_band_pct: f64,

    // ---- 内部状态 ----
    /// 最新价（Trade 事件维护）
    last_price: f64,
    /// 最新 OI（USD）
    last_oi: f64,
    /// 当前柱
    cur_bucket: Option<i64>,
    bucket_open_price: f64,
    bucket_open_oi: f64,
    /// 重置当日（UTC 日序号）；None = 未重置
    reset_day: Option<i64>,
}

impl OiQuadrant {
    pub fn new(oi_reset_pct: f64) -> Self {
        Self {
            oi_reset_pct,
            noise_band_pct: 0.5,
            last_price: 0.0,
            last_oi: 0.0,
            cur_bucket: None,
            bucket_open_price: 0.0,
            bucket_open_oi: 0.0,
            reset_day: None,
        }
    }

    /// 柱收盘评估；返回可选信号。
    fn close_bucket(&mut self, ts: tcore::types::Timestamp) -> Option<Signal> {
        let day = self.cur_bucket? / DAY_MS;

        let doi_pct = if self.bucket_open_oi > 0.0 && self.last_oi > 0.0 {
            (self.last_oi - self.bucket_open_oi) / self.bucket_open_oi * 100.0
        } else {
            0.0
        };
        let ret_pct = if self.bucket_open_price > 0.0 && self.last_price > 0.0 {
            (self.last_price - self.bucket_open_price) / self.bucket_open_price * 100.0
        } else {
            0.0
        };

        // 重置检测优先：单柱 ΔOI 暴跌 → 当日观望
        if doi_pct < -self.oi_reset_pct {
            self.reset_day = Some(day);
            return Some(Signal::new(
                SignalKind::OiQuadrant,
                ts,
                self.name(),
                serde_json::json!({
                    "quadrant": "reset",
                    "doi_pct": doi_pct,
                    "ret_pct": ret_pct,
                    "reset": true,
                }),
            ));
        }
        // 重置日当天不再发象限信号
        if self.reset_day == Some(day) {
            return None;
        }
        // 噪音带内不发
        if doi_pct.abs() < self.noise_band_pct || ret_pct == 0.0 {
            return None;
        }
        let quadrant = match (doi_pct > 0.0, ret_pct > 0.0) {
            (true, true) => "long_open",
            (true, false) => "short_open",
            (false, true) => "short_squeeze",
            (false, false) => "long_kill",
        };
        Some(Signal::new(
            SignalKind::OiQuadrant,
            ts,
            self.name(),
            serde_json::json!({
                "quadrant": quadrant,
                "doi_pct": doi_pct,
                "ret_pct": ret_pct,
                "reset": false,
            }),
        ))
    }

    /// 事件时间推进柱状态；跨柱时收盘旧柱。
    fn advance(&mut self, ev_ts: tcore::types::Timestamp) -> Option<Signal> {
        let bucket = ev_ts.as_millis() / BUCKET_MS * BUCKET_MS;
        if self.cur_bucket == Some(bucket) {
            return None;
        }
        let sig = if self.cur_bucket.is_some() {
            self.close_bucket(ev_ts)
        } else {
            None
        };
        self.cur_bucket = Some(bucket);
        self.bucket_open_price = self.last_price;
        self.bucket_open_oi = self.last_oi;
        sig
    }
}

impl SignalPlugin for OiQuadrant {
    fn name(&self) -> &'static str {
        "OiQuadrant"
    }
    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => {
                self.last_price = t.price.to_f64();
                self.advance(t.ts).into_iter().collect()
            }
            Event::Oi(o) => {
                self.last_oi = o.oi_usd;
                self.advance(o.ts).into_iter().collect()
            }
            _ => Vec::new(),
        }
    }
}

impl OiQuadrant {
    pub fn from_params(p: &serde_json::Value) -> Self {
        Self::new(
            p.get("oi_reset_pct")
                .and_then(|v| v.as_f64())
                .unwrap_or(3.0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Exchange, Price, Qty, Symbol, Timestamp};

    fn trade(ts_ms: i64, price: f64) -> tcore::Trade {
        tcore::Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(0.01),
            is_buyer_maker: false,
        }
    }

    fn oi(ts_ms: i64, oi_usd: f64) -> tcore::event::OiTick {
        tcore::event::OiTick {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            oi_usd,
        }
    }

    fn quadrant_of(sig: &Signal) -> String {
        sig.payload.get("quadrant").unwrap().as_str().unwrap().to_string()
    }

    /// 增仓上涨 → long_open。
    #[test]
    fn long_open_quadrant() {
        let mut s = OiQuadrant::new(3.0);
        // 柱0：建立基准
        s.on_event(&Event::Trade(trade(1_000, 67_000.0)), &Ctx::default());
        s.on_event(&Event::Oi(oi(2_000, 1.0e9)), &Ctx::default());
        // 柱1：价格 +1%、OI +2%
        s.on_event(&Event::Trade(trade(BUCKET_MS + 1_000, 67_670.0)), &Ctx::default());
        let sigs = s.on_event(&Event::Oi(oi(BUCKET_MS + 2_000, 1.02e9)), &Ctx::default());
        // 柱2 首事件触发柱1收盘
        let sigs2 = s.on_event(
            &Event::Trade(trade(2 * BUCKET_MS + 1_000, 67_700.0)),
            &Ctx::default(),
        );
        let all: Vec<String> = sigs.iter().chain(sigs2.iter()).map(quadrant_of).collect();
        // 柱0 收盘时 open_oi=0（基准未建立）→ 静默；柱1 收盘 → long_open
        assert!(all.iter().any(|q| q == "long_open"), "应发 long_open: {:?}", all);
    }

    /// 减仓下跌 → long_kill；OI 暴跌超阈值 → reset 且当日沉默。
    #[test]
    fn reset_suppresses_rest_of_day() {
        let mut s = OiQuadrant::new(3.0);
        s.on_event(&Event::Trade(trade(1_000, 67_000.0)), &Ctx::default());
        s.on_event(&Event::Oi(oi(2_000, 1.0e9)), &Ctx::default());
        // 柱1：OI −5%（超 −3% 重置线），价格 −1%
        s.on_event(&Event::Trade(trade(BUCKET_MS + 1_000, 66_330.0)), &Ctx::default());
        s.on_event(&Event::Oi(oi(BUCKET_MS + 2_000, 0.95e9)), &Ctx::default());
        let sigs = s.on_event(
            &Event::Trade(trade(2 * BUCKET_MS + 1_000, 66_300.0)),
            &Ctx::default(),
        );
        assert!(sigs.iter().any(|s| quadrant_of(s) == "reset"), "应发 reset: {:?}", sigs.iter().map(quadrant_of).collect::<Vec<_>>());
        // 当日后续柱（OI 恢复平稳）不再发象限信号
        s.on_event(&Event::Oi(oi(2 * BUCKET_MS + 2_000, 0.951e9)), &Ctx::default());
        let sigs2 = s.on_event(
            &Event::Trade(trade(3 * BUCKET_MS + 1_000, 66_400.0)),
            &Ctx::default(),
        );
        assert!(sigs2.is_empty(), "重置当日应沉默: {:?}", sigs2.iter().map(quadrant_of).collect::<Vec<_>>());
    }
}
