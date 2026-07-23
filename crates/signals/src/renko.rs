//! renko 反转砖引擎
//!
//! 对应教程 Exocharts `Trend Reversal 200-124`：
//! 顺当前砖方向移动满 T_ren（100 美元）收新砖；
//! 逆向回撤满 R_ren（62 美元）收一根反向砖（形成影线）。
//! 每砖聚合 volume / delta / duration / footprint（分价位买卖量）。
//! 对应教程 Exocharts `Trend Reversal 200-124` 的非对称反转砖，默认参数 100/62 美元：
//! - 顺当前砖方向移动满 `T_ren`（100 美元）收新砖；
//! - 逆向回撤满 `R_ren`（62 美元）收一根反向砖（实体=R_ren，run-up 形成影线）；
//! - 首砖（尚无方向）两侧都需满 `T_ren`。
//!
//! 关键语义（与回测引擎保持一致，务必同步修改）：
//! - 锚点 = 上一块砖的收盘价（定点 raw）；延续阈值 T_ren / 反转阈值 R_ren 都从锚点量。
//! - 反转砖收在 `锚点 ∓ R_ren`，方向翻转，新锚点 = 反转砖收盘价；
//!   之后延续新方向仍需满 T_ren（"反转便宜、延续贵"）。
//! - 一笔大单可连续收多块砖：第一块获得该笔（及之前累计）的成交量聚合，
//!   其余为"跳空砖"（零成交、时间戳相同）——成交量守恒不受影响。
//! - 影线归属：收砖砖的高低点取「触发前桶内极值 ∪ {open, close}」；触发价越出收盘价
//!   的部分（run-up / 砸穿段）播种为下一桶的起始极值，使随后的**反转砖**记录到影线
//!   （"小实体/长影线"力竭检测的输入）。跳空砖（同笔第 2 块起）不带影线。
//! - 每块砖聚合：volume / delta / duration / trades / footprint（分价位买卖量）/ net（净主动名义额）。
//!
//! 附带：砖方向序列统计 [`brick_stats`]（连续同向砖分布 vs 2⁻ⁿ 零假设、反转后延续
//! 条件概率），供手册 马尔可夫链基线检验；CSV 导出 [`bricks_to_csv`]。

use serde::Serialize;
use std::collections::BTreeMap;
use std::io::Write;
use tcore::event::Trade;
use tcore::plugin::{Ctx, Signal, SignalKind, SignalPlugin};
use tcore::types::{Price, Qty, Timestamp, PRICE_SCALE};
use tcore::Event;

// ============================================================================
// 配置
// ============================================================================

/// renko 参数（定点 raw，1 单位 = 1e-8 美元）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenkoConfig {
    /// 顺向砖尺寸 T_ren（raw）
    pub trend_raw: i64,
    /// 反转砖尺寸 R_ren（raw）
    pub reversal_raw: i64,
}

impl RenkoConfig {
    /// 以美元构造（如 `trend_reversal_usd(100.0, 62.0)`）。
    pub fn trend_reversal_usd(trend: f64, reversal: f64) -> Self {
        Self {
            trend_raw: (trend * PRICE_SCALE as f64).round() as i64,
            reversal_raw: (reversal * PRICE_SCALE as f64).round() as i64,
        }
    }
    /// 教程默认 100/62 美元。
    pub fn default_100_62() -> Self {
        Self::trend_reversal_usd(100.0, 62.0)
    }
}

// ============================================================================
// 砖
// ============================================================================

/// footprint 一档：某价位上的主买/主卖量（定点 raw 合计）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FootprintLevel {
    pub buy_raw: i64,
    pub sell_raw: i64,
}

/// 一块已收的 renko 砖。
#[derive(Debug, Clone)]
pub struct Brick {
    /// 砖序号（0 起）
    pub idx: u64,
    /// 方向：+1 涨砖 / -1 跌砖
    pub dir: i8,
    /// 是否反转砖（由 R_ren 阈值收出、方向相对上一砖翻转）
    pub is_reversal: bool,
    /// 在当前同向链中的位置（1 起；反转砖=1）
    pub chain_index: u32,
    pub open: Price,
    pub close: Price,
    pub high: Price,
    pub low: Price,
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
    /// 砖内成交笔数
    pub trades: u64,
    /// 主买量合计（定点 raw）
    pub buy_raw: i64,
    /// 主卖量合计（定点 raw）
    pub sell_raw: i64,
    /// 净主动名义额（USD，仅展示用，不参与决策）
    pub net_notional: f64,
    /// 分价位 footprint（price_raw → 买卖量）
    pub footprint: BTreeMap<i64, FootprintLevel>,
}

impl Brick {
    /// 成交量（币，浮点展示用）
    pub fn volume(&self) -> f64 {
        Qty::from_raw(self.buy_raw + self.sell_raw).to_f64()
    }
    /// Delta（主买−主卖，币）
    pub fn delta(&self) -> f64 {
        Qty::from_raw(self.buy_raw - self.sell_raw).to_f64()
    }
    /// 实体大小（美元）
    pub fn body_usd(&self) -> f64 {
        self.open.abs_diff(self.close).to_f64()
    }
    /// 上影线（美元）
    pub fn wick_up_usd(&self) -> f64 {
        let top = if self.close.raw() > self.open.raw() {
            self.close
        } else {
            self.open
        };
        self.high.abs_diff(top).to_f64()
    }
    /// 下影线（美元）
    pub fn wick_down_usd(&self) -> f64 {
        let bot = if self.close.raw() < self.open.raw() {
            self.close
        } else {
            self.open
        };
        bot.abs_diff(self.low).to_f64()
    }
    /// 时长（毫秒）
    pub fn duration_ms(&self) -> i64 {
        self.end_ts.diff_ms(self.start_ts)
    }
}

// ============================================================================
// 引擎
// ============================================================================

/// 当前桶（自上一块砖收盘以来累计的成交聚合）。
#[derive(Debug, Default)]
struct BrickAgg {
    start_ts: Option<Timestamp>,
    last_ts: Option<Timestamp>,
    hi: Option<i64>,
    lo: Option<i64>,
    trades: u64,
    buy_raw: i64,
    sell_raw: i64,
    net_notional: f64,
    footprint: BTreeMap<i64, FootprintLevel>,
}

impl BrickAgg {
    fn absorb(&mut self, t: &Trade) {
        let p = t.price.raw();
        let q = t.qty.raw();
        if self.start_ts.is_none() {
            self.start_ts = Some(t.ts);
        }
        self.last_ts = Some(t.ts);
        self.hi = Some(self.hi.map_or(p, |h: i64| h.max(p)));
        self.lo = Some(self.lo.map_or(p, |l: i64| l.min(p)));
        self.trades += 1;
        // is_buyer_maker=true → 卖方主动 → 计入 sell
        if t.is_buyer_maker {
            self.sell_raw += q;
        } else {
            self.buy_raw += q;
        }
        self.net_notional += t.signed_notional();
        let lvl = self.footprint.entry(p).or_default();
        if t.is_buyer_maker {
            lvl.sell_raw += q;
        } else {
            lvl.buy_raw += q;
        }
    }
}

/// renko 砖引擎：逐笔消费 Trade，产出 Brick。
///
/// 回测与实盘共用（信号侧经 [`RenkoBricks`] 插件接入）。
#[derive(Debug)]
pub struct RenkoEngine {
    cfg: RenkoConfig,
    /// 当前砖流方向：+1/-1；0 = 尚未出首砖
    dir: i8,
    /// 锚点 = 上一块砖收盘价（raw）
    anchor: Option<i64>,
    cur: BrickAgg,
    bricks: u64,
    chain_len: u32,
    chain_volume_raw: i64,
}

impl RenkoEngine {
    pub fn new(cfg: RenkoConfig) -> Self {
        Self {
            cfg,
            dir: 0,
            anchor: None,
            cur: BrickAgg::default(),
            bricks: 0,
            chain_len: 0,
            chain_volume_raw: 0,
        }
    }

    /// 当前砖流方向（0 = 未出首砖）。
    pub fn dir(&self) -> i8 {
        self.dir
    }
    /// 已收砖数。
    pub fn bricks_closed(&self) -> u64 {
        self.bricks
    }
    /// 当前同向链长度（≥1；未出砖为 0）。
    pub fn chain_len(&self) -> u32 {
        self.chain_len
    }
    /// 当前同向链累计成交量（定点 raw）。
    pub fn chain_volume_raw(&self) -> i64 {
        self.chain_volume_raw
    }
    /// 当前未收桶的 (buy, sell) 量（定点 raw；守恒对账用：Σ已收砖 + 未收桶 = Σ输入）。
    pub fn pending_raw(&self) -> (i64, i64) {
        (self.cur.buy_raw, self.cur.sell_raw)
    }

    /// 消费一笔成交，返回本次收出的砖（一笔大单可能收多块）。
    pub fn on_trade(&mut self, t: &Trade) -> Vec<Brick> {
        let mut out = Vec::new();
        let p = t.price.raw();
        if self.anchor.is_none() {
            // 首笔定锚（不出砖，仅初始化）
            self.anchor = Some(p);
        }
        // 触发前的桶内极值：收砖砖的影线只计到触发前（触发价的越出段归下一桶）。
        let pre_hi = self.cur.hi;
        let pre_lo = self.cur.lo;
        self.cur.absorb(t);

        let mut first = true;
        loop {
            let a = self.anchor.expect("anchor set");
            // (上行阈值, 下行阈值)：延续需 T_ren，反转需 R_ren；首砖两侧都需 T_ren
            let (up_need, down_need) = match self.dir {
                1 => (self.cfg.trend_raw, self.cfg.reversal_raw),
                -1 => (self.cfg.reversal_raw, self.cfg.trend_raw),
                _ => (self.cfg.trend_raw, self.cfg.trend_raw),
            };
            let (dir, close_raw) = if p >= a + up_need {
                (1, a + up_need)
            } else if p <= a - down_need {
                (-1, a - down_need)
            } else {
                break;
            };
            out.push(self.close_brick(
                dir,
                close_raw,
                t.ts,
                if first {
                    (pre_hi, pre_lo)
                } else {
                    (None, None)
                },
                p,
            ));
            first = false;
        }
        out
    }

    /// 收一块砖：方向 `dir`、收盘价 `close_raw`、触发时间 `ts`。
    ///
    /// `pre_extremes` 为触发前桶内极值（仅本笔收的第一块砖携带，其余传 None）；
    /// `trigger_p` 为触发价，播种为下一桶的起始极值（反转砖影线的来源）。
    fn close_brick(
        &mut self,
        dir: i8,
        close_raw: i64,
        ts: Timestamp,
        pre_extremes: (Option<i64>, Option<i64>),
        trigger_p: i64,
    ) -> Brick {
        let open_raw = self.anchor.expect("anchor set");
        let is_reversal = self.dir != 0 && dir != self.dir;
        if dir == self.dir {
            self.chain_len += 1;
        } else {
            self.chain_len = 1;
        }

        let agg = std::mem::take(&mut self.cur);
        // 触发价播种为新桶的起始极值（其越出 close 的段 = 后续反转砖的影线）。
        self.cur.hi = Some(trigger_p);
        self.cur.lo = Some(trigger_p);
        let (pre_hi, pre_lo) = pre_extremes;
        let hi = pre_hi
            .into_iter()
            .chain([open_raw, close_raw])
            .max()
            .expect("nonempty");
        let lo = pre_lo
            .into_iter()
            .chain([open_raw, close_raw])
            .min()
            .expect("nonempty");
        let brick = Brick {
            idx: self.bricks,
            dir,
            is_reversal,
            chain_index: self.chain_len,
            open: Price::from_raw(open_raw),
            close: Price::from_raw(close_raw),
            high: Price::from_raw(hi),
            low: Price::from_raw(lo),
            start_ts: agg.start_ts.unwrap_or(ts),
            end_ts: agg.last_ts.unwrap_or(ts),
            trades: agg.trades,
            buy_raw: agg.buy_raw,
            sell_raw: agg.sell_raw,
            net_notional: agg.net_notional,
            footprint: agg.footprint,
        };
        if self.chain_len == 1 {
            self.chain_volume_raw = brick.buy_raw + brick.sell_raw;
        } else {
            self.chain_volume_raw += brick.buy_raw + brick.sell_raw;
        }
        self.anchor = Some(close_raw);
        self.dir = dir;
        self.bricks += 1;
        brick
    }
}

// ============================================================================
// SignalPlugin 接入
// ============================================================================

/// renko 砖信号插件：每收一块砖发出 [`SignalKind::BrickClosed`]。
pub struct RenkoBricks {
    pub engine: RenkoEngine,
}

impl RenkoBricks {
    pub fn new(cfg: RenkoConfig) -> Self {
        Self {
            engine: RenkoEngine::new(cfg),
        }
    }
}

/// 砖 → 信号载荷（JSON；浮点仅用于展示/扳机读取，不回流决策精度）。
fn brick_payload(b: &Brick) -> serde_json::Value {
    serde_json::json!({
        "idx": b.idx,
        "dir": b.dir,
        "is_reversal": b.is_reversal,
        "chain_index": b.chain_index,
        "open": b.open.to_f64(),
        "close": b.close.to_f64(),
        "high": b.high.to_f64(),
        "low": b.low.to_f64(),
        "body_usd": b.body_usd(),
        "wick_up_usd": b.wick_up_usd(),
        "wick_down_usd": b.wick_down_usd(),
        "start_ts": b.start_ts.as_millis(),
        "end_ts": b.end_ts.as_millis(),
        "duration_ms": b.duration_ms(),
        "trades": b.trades,
        "volume": b.volume(),
        "delta": b.delta(),
        "net_notional": b.net_notional,
    })
}

impl SignalPlugin for RenkoBricks {
    fn name(&self) -> &'static str {
        "RenkoBricks"
    }

    fn on_event(&mut self, ev: &Event, _ctx: &Ctx) -> Vec<Signal> {
        match ev {
            Event::Trade(t) => self
                .engine
                .on_trade(t)
                .iter()
                .map(|b| {
                    Signal::new(
                        SignalKind::BrickClosed,
                        b.end_ts,
                        self.name(),
                        brick_payload(b),
                    )
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}

// ============================================================================
// 砖序列统计（手册 7.3：马尔可夫链基线检验输入）
// ============================================================================

/// 砖方向序列统计。
#[derive(Debug, Clone, Serialize)]
pub struct BrickStats {
    pub n_bricks: usize,
    pub up_bricks: usize,
    pub down_bricks: usize,
    pub n_chains: usize,
    /// 链长 → 观测次数（链 = 极大同向序列）
    pub run_hist: BTreeMap<usize, usize>,
    /// 2⁻ⁿ 零假设下的期望次数（E = n_chains × 2⁻ⁿ，假设砖方向 iid 且 p=0.5）
    pub expected_run_hist: BTreeMap<usize, f64>,
    /// 最长同向链
    pub max_run: usize,
    /// P(反转后延续) = 反转砖的下一块砖延续反转方向的比例（样本=`reversal_samples`）
    pub p_continue_after_reversal: Option<f64>,
    pub reversal_samples: usize,
    pub mean_duration_ms: f64,
    pub mean_volume: f64,
}

/// 从砖序列计算统计量（7.3 基线：与 2⁻ⁿ 分布对照，判断方向序列是否显著偏离 iid）。
pub fn brick_stats(bricks: &[Brick]) -> BrickStats {
    let n = bricks.len();
    let up = bricks.iter().filter(|b| b.dir == 1).count();
    let mut run_hist: BTreeMap<usize, usize> = BTreeMap::new();
    let mut n_chains = 0usize;
    let mut max_run = 0usize;
    let mut i = 0;
    while i < n {
        let mut len = 1;
        while i + len < n && bricks[i + len].dir == bricks[i].dir {
            len += 1;
        }
        *run_hist.entry(len).or_default() += 1;
        n_chains += 1;
        max_run = max_run.max(len);
        i += len;
    }
    let expected_run_hist: BTreeMap<usize, f64> = run_hist
        .keys()
        .map(|&len| (len, n_chains as f64 * 2f64.powi(-(len as i32))))
        .collect();

    // 反转后延续：对每块反转砖（非末砖），看下一块是否延续反转方向
    let mut continued = 0usize;
    let mut samples = 0usize;
    for (k, b) in bricks.iter().enumerate() {
        if b.is_reversal && k + 1 < n {
            samples += 1;
            if bricks[k + 1].dir == b.dir {
                continued += 1;
            }
        }
    }
    let p_continue = if samples > 0 {
        Some(continued as f64 / samples as f64)
    } else {
        None
    };

    let mean_dur = if n > 0 {
        bricks.iter().map(|b| b.duration_ms()).sum::<i64>() as f64 / n as f64
    } else {
        0.0
    };
    let mean_vol = if n > 0 {
        bricks.iter().map(|b| b.volume()).sum::<f64>() / n as f64
    } else {
        0.0
    };

    BrickStats {
        n_bricks: n,
        up_bricks: up,
        down_bricks: n - up,
        n_chains,
        run_hist,
        expected_run_hist,
        max_run,
        p_continue_after_reversal: p_continue,
        reversal_samples: samples,
        mean_duration_ms: mean_dur,
        mean_volume: mean_vol,
    }
}

/// 统计 → markdown 报告片段（PR-7 报告复用）。
pub fn stats_to_markdown(s: &BrickStats) -> String {
    let mut md = String::new();
    md.push_str(&format!(
        "## Renko 砖序列统计（7.3 马尔可夫基线）\n\n\
         - 砖数：{}（涨 {} / 跌 {}），链数：{}，最长同向链：{}\n\
         - 平均砖时长：{:.0} ms，平均砖成交量：{:.4}\n",
        s.n_bricks,
        s.up_bricks,
        s.down_bricks,
        s.n_chains,
        s.max_run,
        s.mean_duration_ms,
        s.mean_volume
    ));
    match s.p_continue_after_reversal {
        Some(p) => md.push_str(&format!(
            "- P(反转后延续) = {:.3}（n={}；iid 零假设 0.5）\n",
            p, s.reversal_samples
        )),
        None => md.push_str("- P(反转后延续)：样本不足\n"),
    }
    md.push_str("\n| 链长 n | 观测次数 | 2⁻ⁿ 期望 | 观测/期望 |\n|---:|---:|---:|---:|\n");
    for (len, obs) in &s.run_hist {
        let exp = s.expected_run_hist.get(len).copied().unwrap_or(0.0);
        let ratio = if exp > 0.0 {
            *obs as f64 / exp
        } else {
            f64::NAN
        };
        md.push_str(&format!(
            "| {} | {} | {:.2} | {:.2} |\n",
            len, obs, exp, ratio
        ));
    }
    md
}

/// 砖序列导出 CSV（不含 footprint 明细；供方向序列/链统计外部复核）。
pub fn bricks_to_csv<W: Write>(mut w: W, bricks: &[Brick]) -> std::io::Result<()> {
    writeln!(
        w,
        "idx,dir,is_reversal,chain_index,open,close,high,low,body_usd,wick_up_usd,wick_down_usd,start_ts,end_ts,duration_ms,trades,volume,delta,net_notional"
    )?;
    for b in bricks {
        writeln!(
            w,
            "{},{},{},{},{},{},{},{},{:.2},{:.2},{:.2},{},{},{},{},{:.8},{:.8},{:.2}",
            b.idx,
            b.dir,
            b.is_reversal,
            b.chain_index,
            b.open.to_f64(),
            b.close.to_f64(),
            b.high.to_f64(),
            b.low.to_f64(),
            b.body_usd(),
            b.wick_up_usd(),
            b.wick_down_usd(),
            b.start_ts.as_millis(),
            b.end_ts.as_millis(),
            b.duration_ms(),
            b.trades,
            b.volume(),
            b.delta(),
            b.net_notional,
        )?;
    }
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tcore::types::{Exchange, Qty, Symbol};

    fn tr(ts_ms: i64, price: f64, qty: f64, is_buyer_maker: bool) -> Trade {
        Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(qty),
            is_buyer_maker,
        }
    }

    fn feed(eng: &mut RenkoEngine, trades: &[Trade]) -> Vec<Brick> {
        let mut out = Vec::new();
        for t in trades {
            out.extend(eng.on_trade(t));
        }
        out
    }

    /// 黄金样本（手工演算，100/62 美元）：
    /// 67000 → 67100（涨砖#0）→ 67038（反转跌砖#1）→ 66938（跌砖#2，延续需 100）
    /// → 67000（反转涨砖#3）→ 67100（涨砖#4）。
    /// 教程要点：反转后 67038→66976 的 62 美元**不再出砖**——延续必须满 100。
    #[test]
    fn golden_100_62_hand_computed() {
        let mut eng = RenkoEngine::new(RenkoConfig::default_100_62());
        let trades = vec![
            tr(0, 67000.0, 1.0, false), // 定锚
            tr(1, 67100.0, 1.0, false), // +100 → 涨砖
            tr(2, 67038.0, 1.0, true),  // -62 → 反转跌砖
            tr(3, 66976.0, 1.0, true),  // -62 不出砖（延续需100）
            tr(4, 66938.0, 1.0, true),  // 累计-100 → 跌砖
            tr(5, 67000.0, 1.0, false), // +62 → 反转涨砖
            tr(6, 67100.0, 1.0, false), // +100 → 涨砖
        ];
        let b = feed(&mut eng, &trades);
        assert_eq!(b.len(), 5, "出砖数");
        assert_eq!(
            b.iter().map(|x| x.dir).collect::<Vec<_>>(),
            vec![1, -1, -1, 1, 1]
        );
        assert_eq!(
            b.iter().map(|x| x.is_reversal).collect::<Vec<_>>(),
            vec![false, true, false, true, false]
        );
        assert_eq!(
            b.iter().map(|x| x.chain_index).collect::<Vec<_>>(),
            vec![1, 1, 2, 1, 2]
        );
        // 砖体与锚点
        assert!((b[0].open.to_f64() - 67000.0).abs() < 1e-6);
        assert!((b[0].close.to_f64() - 67100.0).abs() < 1e-6);
        assert!((b[1].open.to_f64() - 67100.0).abs() < 1e-6);
        assert!((b[1].close.to_f64() - 67038.0).abs() < 1e-6);
        assert!((b[2].close.to_f64() - 66938.0).abs() < 1e-6);
        assert!((b[3].close.to_f64() - 67000.0).abs() < 1e-6);
        assert!((b[4].close.to_f64() - 67100.0).abs() < 1e-6);
        // 实体：反转砖 62，趋势砖 100
        assert!((b[1].body_usd() - 62.0).abs() < 1e-6);
        assert!((b[4].body_usd() - 100.0).abs() < 1e-6);
        // 成交量守恒：7 笔 × 1.0
        let vol: f64 = b.iter().map(|x| x.volume()).sum();
        assert!((vol - 7.0).abs() < 1e-6);
        // 引擎链状态：末尾为 2 连涨
        assert_eq!(eng.chain_len(), 2);
        assert!((Qty::from_raw(eng.chain_volume_raw()).to_f64() - 2.0).abs() < 1e-6);
    }

    /// 影线：67100 收涨砖后继续冲到 67260（收 67200 趋势砖），随后回撤 62，
    /// 反转砖 high 必须记录 67260（run-up 段播种的上影线），实体 62 无下影。
    #[test]
    fn reversal_brick_records_wick() {
        let mut eng = RenkoEngine::new(RenkoConfig::default_100_62());
        let b = feed(
            &mut eng,
            &[
                tr(0, 67000.0, 0.5, false),
                tr(1, 67100.0, 0.5, false), // 涨砖#0
                tr(2, 67260.0, 0.3, false), // 触发涨砖#1（收67200），冲高67260播种
                tr(3, 67198.0, 0.3, true),  // 未达 -62
                tr(4, 67138.0, 0.4, true),  // -62 → 反转跌砖#2
            ],
        );
        assert_eq!(b.len(), 3);
        assert!((b[1].close.to_f64() - 67200.0).abs() < 1e-6);
        assert!(
            (b[1].high.to_f64() - 67200.0).abs() < 1e-6,
            "趋势砖自身不带影线"
        );
        assert!((b[2].close.to_f64() - 67138.0).abs() < 1e-6);
        assert!(
            (b[2].high.to_f64() - 67260.0).abs() < 1e-6,
            "上影线须记录冲高段"
        );
        assert!((b[2].wick_up_usd() - 60.0).abs() < 1e-6);
        assert!((b[2].body_usd() - 62.0).abs() < 1e-6);
        assert!((b[2].wick_down_usd() - 0.0).abs() < 1e-6);
    }

    /// 跳空：一笔 +250 连续收 2 块涨砖；首块得全部成交量，次块为零成交跳空砖。
    #[test]
    fn gap_trade_closes_multiple_bricks() {
        let mut eng = RenkoEngine::new(RenkoConfig::default_100_62());
        let b = feed(
            &mut eng,
            &[tr(0, 67000.0, 1.0, false), tr(1, 67250.0, 2.0, false)],
        );
        assert_eq!(b.len(), 2);
        assert!((b[0].close.to_f64() - 67100.0).abs() < 1e-6);
        assert!((b[1].close.to_f64() - 67200.0).abs() < 1e-6);
        assert!((b[0].volume() - 3.0).abs() < 1e-6, "首砖含全部 3.0");
        assert_eq!(b[1].trades, 0, "跳空砖零成交");
        assert!((b[1].volume() - 0.0).abs() < 1e-12);
        // 跳空砖不带影线
        assert_eq!(b[1].high.raw(), b[1].close.raw());
        assert_eq!(b[1].low.raw(), b[1].open.raw());
        let total: f64 = b.iter().map(|x| x.volume()).sum();
        assert!((total - 3.0).abs() < 1e-6, "守恒");
    }

    /// 首砖两侧都需满 T_ren（未定向时 ±62 不出砖）。
    #[test]
    fn first_brick_requires_full_trend() {
        let mut eng = RenkoEngine::new(RenkoConfig::default_100_62());
        assert!(feed(
            &mut eng,
            &[tr(0, 67000.0, 1.0, false), tr(1, 67038.0, 1.0, true)]
        )
        .is_empty());
        let b = feed(&mut eng, &[tr(2, 66900.0, 1.0, true)]);
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].dir, -1);
        assert!(!b[0].is_reversal, "首砖不算反转");
        assert!((b[0].close.to_f64() - 66900.0).abs() < 1e-6);
    }

    /// SignalPlugin：砖以 BrickClosed 信号发出，载荷字段齐备。
    #[test]
    fn signal_plugin_emits_brick_closed() {
        let mut plugin = RenkoBricks::new(RenkoConfig::default_100_62());
        let ctx = Ctx::default();
        let mut sigs = Vec::new();
        for t in [
            tr(0, 67000.0, 1.0, false),
            tr(1, 67100.0, 1.5, false),
            tr(2, 67038.0, 2.0, true),
        ] {
            sigs.extend(plugin.on_event(&Event::Trade(t), &ctx));
        }
        assert_eq!(sigs.len(), 2);
        assert_eq!(sigs[0].kind, SignalKind::BrickClosed);
        assert_eq!(sigs[0].source, "RenkoBricks");
        assert_eq!(sigs[0].payload["dir"], 1);
        assert_eq!(sigs[1].payload["dir"], -1);
        assert_eq!(sigs[1].payload["is_reversal"], true);
        assert!((sigs[1].payload["close"].as_f64().unwrap() - 67038.0).abs() < 1e-6);
        // 非 Trade 事件不出信号
        assert!(plugin
            .on_event(&Event::Timer(Timestamp::from_millis(3)), &ctx)
            .is_empty());
    }

    /// 统计：已知方向序列的链长分布与反转延续概率。
    #[test]
    fn stats_known_sequence() {
        // 方向 [+,+,-,-,-,+] → 链长 [2,3,1]；反转砖：第3块(跌)后延续跌→continued，
        // 末块(涨)无后继→不计。P=1/1=1.0
        let mut eng = RenkoEngine::new(RenkoConfig::default_100_62());
        let b = feed(
            &mut eng,
            &[
                tr(0, 67000.0, 1.0, false),
                tr(1, 67100.0, 1.0, false), // +
                tr(2, 67200.0, 1.0, false), // +
                tr(3, 67138.0, 1.0, true),  // -（反转）
                tr(4, 67038.0, 1.0, true),  // -
                tr(5, 66938.0, 1.0, true),  // -
                tr(6, 67000.0, 1.0, false), // +（反转）
            ],
        );
        assert_eq!(
            b.iter().map(|x| x.dir).collect::<Vec<_>>(),
            vec![1, 1, -1, -1, -1, 1]
        );
        let s = brick_stats(&b);
        assert_eq!(s.n_chains, 3);
        assert_eq!(s.run_hist.get(&2), Some(&1));
        assert_eq!(s.run_hist.get(&3), Some(&1));
        assert_eq!(s.run_hist.get(&1), Some(&1));
        assert_eq!(s.max_run, 3);
        assert_eq!(s.p_continue_after_reversal, Some(1.0));
        assert_eq!(s.reversal_samples, 1);
        // 零假设：E(链长2) = 3 × 2⁻² = 0.75
        assert!((s.expected_run_hist[&2] - 0.75).abs() < 1e-12);
        let md = stats_to_markdown(&s);
        assert!(md.contains("P(反转后延续) = 1.000"));
    }

    // footprint / 总量守恒 proptest：随机游走序列下
    // Σ砖buy+sell == Σ输入qty；砖内 footprint 合计 == 砖买卖量。
    proptest::proptest! {
        #[test]
        fn footprint_and_volume_conservation(
            steps in proptest::collection::vec(-500i64..500, 1..200)
        ) {
            let mut eng = RenkoEngine::new(RenkoConfig::default_100_62());
            let mut price = 67000.0f64;
            let mut total_buy = 0i64;
            let mut total_sell = 0i64;
            let mut all: Vec<Brick> = Vec::new();
            for (i, st) in steps.iter().enumerate() {
                price = (price + *st as f64 * 0.5).max(1000.0);
                let qty = 0.01 + (i % 7) as f64 * 0.01;
                let ibm = i % 3 == 0;
                let t = tr(i as i64, price, qty, ibm);
                if ibm {
                    total_sell += t.qty.raw();
                } else {
                    total_buy += t.qty.raw();
                }
                all.extend(eng.on_trade(&t));
            }
            let bsum: i64 = all.iter().map(|b| b.buy_raw).sum();
            let ssum: i64 = all.iter().map(|b| b.sell_raw).sum();
            // 守恒口径：Σ已收砖 + 当前未收桶 == Σ输入（未收桶的量尚未归属任何砖）
            let (pend_buy, pend_sell) = eng.pending_raw();
            proptest::prop_assert_eq!(bsum + pend_buy, total_buy, "buy 守恒");
            proptest::prop_assert_eq!(ssum + pend_sell, total_sell, "sell 守恒");
            for b in &all {
                let fb: i64 = b.footprint.values().map(|l| l.buy_raw).sum();
                let fs: i64 = b.footprint.values().map(|l| l.sell_raw).sum();
                proptest::prop_assert_eq!(fb, b.buy_raw, "砖#{} footprint buy 守恒", b.idx);
                proptest::prop_assert_eq!(fs, b.sell_raw, "砖#{} footprint sell 守恒", b.idx);
                proptest::prop_assert!(b.high.raw() >= b.low.raw());
                proptest::prop_assert!(b.high.raw() >= b.open.raw().max(b.close.raw()));
                proptest::prop_assert!(b.low.raw() <= b.open.raw().min(b.close.raw()));
            }
        }
    }
}