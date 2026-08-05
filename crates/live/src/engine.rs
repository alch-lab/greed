//! 实盘执行引擎：与回测同一决策代码路径（信号 → 扳机 → 过滤 → 仓位 → 出场），
//! 仅把撮合层换成 [`AnyBroker`]（模拟 / testnet REST）。
//!
//! 事件流：
//! - `on_trade`：行情 WS 逐笔 → 与回测 `BacktestEngine::on_trade` 同序处理
//!   （时钟/价格 → 撮合回报 → 信号 → 持仓管理/开仓评估 → 熔断）。
//! - `on_timer`：1s 节拍。testnet 成交轮询（2s）、过期限价撤单、
//!   资金费率标志刷新（5min）、权益采样（60s）、钱包对账（1h）。
//!
//! Journal：每次意图/成交/权益采样后原子重写（tmp + rename），
//! 前端随时读到最新状态；进程崩溃最多丢最后一笔未落盘事件。
//!
//! 启动安全：CLI 在构建引擎前已确认「无持仓 + 无遗留挂单 + 杠杆/逐仓就绪」；
//! 运行中收到 SIGINT 只落盘退出，**不撤交易所挂单**（止损单是持仓的保护）。

use std::path::PathBuf;

use backtest::{
    Account, EquityPoint, FillRequest, Journal, JournalEval, JournalIntent, JournalMeta,
};
use strategy::Strategy;
use tcore::plugin::{Ctx, ExitAction, OrderIntent, Signal, Verdict};
use tcore::types::{Price, Qty, Symbol, Timestamp};
use tcore::{Event, EventClock, Trade};
use tracing::{error, info, warn};

use crate::broker::AnyBroker;
use crate::rest::floor_to_step;

#[derive(Debug, Clone)]
pub struct LiveConfig {
    pub symbol: String,
    /// 固定风险百分比（同回测）
    pub risk_pct: f64,
    pub max_risk_pct: f64,
    pub max_leverage: f64,
    pub entry_ttl_ms: i64,
    pub cb_max_daily_losses: u32,
    pub cb_daily_dd_pct: f64,
    /// testnet 数量步长（dry 模式无约束，用 1e-8）
    pub qty_step: f64,
    /// 最小名义价值（USDT；dry 模式 0）
    pub min_notional: f64,
    /// journal 输出路径（原子重写）
    pub journal_path: PathBuf,
    /// 评估流水全量落盘路径（JSONL append；journal 只留环形 300 条，分析用全量）
    pub eval_log_path: PathBuf,
    /// 每日滚动的研究事件目录（运行、信号、影子结果、订单生命周期）。
    pub research_log_dir: PathBuf,
    /// 策略描述（写进 journal meta，前端展示）
    pub strategy_name: String,
    pub strategy_hash: String,
    pub strategy_snapshot: String,
    pub git_commit: String,
    pub run_id: String,
    pub mode: String,
    /// 影子信号估算净收益时采用的往返费用（bps）。
    pub estimated_roundtrip_fee_bps: f64,
}

/// 挂起中的入场（testnet：已下单待成交；dry：限价单待触发）。
#[derive(Debug, Clone)]
struct PendingEntry {
    stop_price: Price,
    tp1_price: Option<Price>,
    expire_ts: Timestamp,
    intent_id: String,
    event_id: Option<i64>,
    order_id: Option<i64>,
    submitted_ts_ms: i64,
    qty: f64,
    reason: String,
}

/// 已提交、等待交易所成交回报的减仓单。
///
/// testnet 的 REST 下单只返回“已受理”，真实成交由 userTrades 异步回传。
/// 在回报到达前必须阻止同一止盈条件重复下单；部分成交完成后，再按剩余仓位
/// 撤换保护性止损，避免旧止损数量大于实际持仓。
#[derive(Debug, Clone)]
struct PendingExit {
    order_id: Option<i64>,
    reason: String,
    requested_qty: f64,
    filled_qty: f64,
    submitted_ts_ms: i64,
    rearm_stop: Option<Price>,
    timeout_noted: bool,
}

/// 每一个已确认信号都跟踪到 4 小时，不论其是否获准交易。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ShadowSignal {
    event_id: i64,
    signal_ts_ms: i64,
    side: String,
    profile: String,
    strength: String,
    event_price: f64,
    confirm_price: f64,
    stop_anchor: Option<f64>,
    context_score: Option<u64>,
    location_confirmed: Option<bool>,
    /// 观察信号触发时的完整特征，随各期限结果一起落盘供离线归因。
    #[serde(default)]
    features: serde_json::Value,
    mfe_pct: f64,
    mae_pct: f64,
    next_horizon: usize,
}

/// 引擎状态快照（控制面 `/api/trade/status` 的数据源）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct EngineSnapshot {
    pub last_price: Option<f64>,
    pub equity: f64,
    pub cash: f64,
    pub position: Option<PositionSnap>,
    pub n_intents: usize,
    pub n_fills: usize,
    /// 最近一次策略评估说明（信号插件 eval_note）
    pub last_eval: Option<serde_json::Value>,
    /// TRDR 市场地图的独立实时快照，避免被高频入场评估覆盖。
    pub market_map: Option<serde_json::Value>,
    /// 不依赖盘口墙的 30 分钟过度延伸 MR 腿。
    pub intraday_reversion: Option<serde_json::Value>,
    pub run_id: String,
    pub strategy_name: String,
    pub strategy_hash: String,
    pub git_commit: String,
    pub research_log_dir: String,
    pub active_shadow_signals: usize,
    pub confirmed_signals_run: usize,
    pub observation_signals_run: usize,
    pub shadow_outcomes_run: usize,
    /// userTrades 是否在 30 秒内成功轮询过；false 时引擎禁止新开仓。
    pub execution_healthy: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PositionSnap {
    pub side: String,
    pub qty: f64,
    pub entry_price: f64,
    pub stop_price: Option<f64>,
    pub unrealized_pnl: f64,
}

/// 一笔持仓的绩效追踪：开仓建立、平仓结算成 trip 记录。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PosTrack {
    side: String,
    entry_price: f64,
    entry_ts_ms: i64,
    /// 最大有利偏移 %（MFE：浮盈峰值）
    mfe_pct: f64,
    /// 最大不利偏移 %（MAE：浮亏峰值，负值）
    mae_pct: f64,
    /// 开仓时的 fill 下标（平仓时汇总本 trip 的已实现盈亏）
    start_fill_idx: usize,
    /// 上次观察到的持仓数量（识别加仓）
    last_qty: f64,
    adds: u32,
}

/// 引擎持久化状态（写进 journal 的 `engine` 字段，进程重启续跑用）。
///
/// 设计约束：出场/扳机插件全部无状态（从 `Position` 视图推导），
/// 因此只要恢复 Account（cash/position/fills）+ 熔断计数，
/// 策略即可无缝接管重启前的持仓，不会因重启被强平。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EngineState {
    version: u32,
    symbol: String,
    strategy: String,
    strategy_hash: String,
    initial_cash: f64,
    cash: f64,
    position: Option<backtest::OpenPosition>,
    /// 一致性哨兵：必须等于同一 journal 的 fills 长度，否则放弃恢复
    fills_len: usize,
    cb_day: i64,
    cb_day_start_equity: f64,
    cb_consec_losses: u32,
    cb_on: bool,
    cb_noted_fills: usize,
    pos_track: Option<PosTrack>,
    #[serde(default)]
    shadow_signals: Vec<ShadowSignal>,
    #[serde(default)]
    confirmed_signals_run: usize,
    #[serde(default)]
    observation_signals_run: usize,
    #[serde(default)]
    shadow_outcomes_run: usize,
}

const ENGINE_STATE_VERSION: u32 = 2;
const SHADOW_HORIZONS_MIN: [i64; 6] = [5, 15, 30, 60, 120, 240];

pub struct LiveEngine {
    strategy: Strategy,
    broker: AnyBroker,
    account: Account,
    ctx: Ctx,
    clock: EventClock,
    symbol: Symbol,
    latest_price: Option<Price>,
    config: LiveConfig,
    // ---- journal 状态 ----
    started_at: String,
    intents: Vec<JournalIntent>,
    equity_curve: Vec<EquityPoint>,
    last_equity_sample_ms: i64,
    // ---- 策略评估流水（可观测性：为什么下单/不下单）----
    evals: Vec<JournalEval>,
    last_eval_keys: Vec<Option<(i64, String)>>,
    latest_eval: Option<serde_json::Value>,
    // ---- 持仓绩效追踪（MFE/MAE，优化止盈止损的核心数据）----
    pos_track: Option<PosTrack>,
    shadow_signals: Vec<ShadowSignal>,
    confirmed_signals_run: usize,
    observation_signals_run: usize,
    shadow_outcomes_run: usize,
    // ---- 入场挂起 ----
    pending_entry: Option<PendingEntry>,
    // ---- 减仓挂起（等待 userTrades 确认后重挂剩余仓位止损）----
    pending_exit: Option<PendingExit>,
    // ---- 熔断状态（同回测）----
    cb_day: i64,
    cb_day_start_equity: f64,
    cb_consec_losses: u32,
    cb_on: bool,
    cb_noted_fills: usize,
    // ---- 定时任务节拍 ----
    last_fill_poll_ms: i64,
    last_funding_poll_ms: i64,
    last_reconcile_ms: i64,
    last_time_sync_ms: i64,
    /// 重启恢复持仓后，首个行情节拍按 journal 止损价重挂保护性止损
    needs_stop_rearm: bool,
}

impl LiveEngine {
    pub fn new(
        strategy: Strategy,
        broker: AnyBroker,
        config: LiveConfig,
        initial_cash: f64,
        started_at: String,
    ) -> Self {
        let symbol = Symbol::new(&config.symbol);
        let n_signals = strategy.signals.len();
        let engine = LiveEngine {
            strategy,
            broker,
            account: Account::new(initial_cash),
            ctx: Ctx::default(),
            clock: EventClock::new(),
            symbol,
            latest_price: None,
            config,
            started_at,
            intents: Vec::new(),
            equity_curve: Vec::new(),
            last_equity_sample_ms: 0,
            evals: Vec::new(),
            last_eval_keys: vec![None; n_signals],
            latest_eval: None,
            pos_track: None,
            shadow_signals: Vec::new(),
            confirmed_signals_run: 0,
            observation_signals_run: 0,
            shadow_outcomes_run: 0,
            pending_entry: None,
            pending_exit: None,
            cb_day: i64::MIN,
            cb_day_start_equity: 0.0,
            cb_consec_losses: 0,
            cb_on: false,
            cb_noted_fills: 0,
            last_fill_poll_ms: 0,
            last_funding_poll_ms: 0,
            last_reconcile_ms: 0,
            last_time_sync_ms: 0,
            needs_stop_rearm: false,
        };
        engine.append_research(
            "run_start",
            chrono::Utc::now().timestamp_millis(),
            serde_json::json!({
                "strategy_snapshot": engine.config.strategy_snapshot,
                "risk_pct": engine.config.risk_pct,
                "max_risk_pct": engine.config.max_risk_pct,
                "max_leverage": engine.config.max_leverage,
                "entry_ttl_ms": engine.config.entry_ttl_ms,
                "estimated_roundtrip_fee_bps": engine.config.estimated_roundtrip_fee_bps,
                "initial_cash": initial_cash,
            }),
        );
        engine
    }

    pub fn account(&self) -> &Account {
        &self.account
    }

    /// 追加一条可长期分析的结构化事件。按 UTC 日期滚动，写入失败不影响交易。
    fn append_research(&self, event_type: &str, ts_ms: i64, data: serde_json::Value) {
        use std::io::Write;

        let date = chrono::DateTime::from_timestamp_millis(ts_ms)
            .unwrap_or_else(chrono::Utc::now)
            .format("%Y-%m-%d");
        if let Err(e) = std::fs::create_dir_all(&self.config.research_log_dir) {
            warn!(error = %e, "research 日志目录创建失败");
            return;
        }
        let path = self.config.research_log_dir.join(format!("{date}.jsonl"));
        let line = serde_json::json!({
            "schema_version": 1,
            "event_type": event_type,
            "ts_ms": ts_ms,
            "run_id": self.config.run_id,
            "mode": self.config.mode,
            "symbol": self.config.symbol,
            "strategy_name": self.config.strategy_name,
            "strategy_hash": self.config.strategy_hash,
            "git_commit": self.config.git_commit,
            "data": data,
        });
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(mut file) => {
                if let Err(e) = writeln!(file, "{line}") {
                    warn!(error = %e, "research JSONL 写入失败");
                }
            }
            Err(e) => warn!(error = %e, "research JSONL 打开失败"),
        }
    }

    /// 从既有 journal 恢复引擎状态（进程重启续跑）。
    ///
    /// 返回 true 表示完成恢复。恢复内容：账户（cash/持仓/fills）、熔断计数、
    /// MFE/MAE 追踪、journal 流水（intents/权益曲线/评估）。挂起的入场单不恢复
    /// （broker 是新建的，挂单已不存在；信号会重新评估）。
    ///
    /// `allow_position`：仅组合模式（dry 撮合 + 执行器镜像真实净仓）传 true。
    /// 非组合模式的持仓是交易所真实仓位，启动前的持仓检查已保证交易所为空，
    /// 此时 journal 里的持仓是人工平仓后的残留，必须丢弃。
    pub fn try_restore(&mut self, allow_position: bool) -> bool {
        let path = &self.config.journal_path;
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(_) => return false, // 首次启动，无 journal
        };
        let journal: Journal = match serde_json::from_str(&text) {
            Ok(j) => j,
            Err(e) => {
                warn!(error = %e, "journal 解析失败，全新开局");
                return false;
            }
        };
        let Some(engine_value) = journal.engine else {
            info!("journal 无引擎状态（旧版本或回测产物），全新开局");
            return false;
        };
        let state: EngineState = match serde_json::from_value(engine_value) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "引擎状态解析失败，全新开局");
                return false;
            }
        };
        if state.version != ENGINE_STATE_VERSION {
            warn!(version = state.version, "引擎状态版本不匹配，全新开局");
            return false;
        }
        if state.symbol != self.config.symbol
            || state.strategy != self.config.strategy_name
            || state.strategy_hash != self.config.strategy_hash
        {
            warn!(
                state_symbol = %state.symbol,
                state_strategy = %state.strategy,
                state_strategy_hash = %state.strategy_hash,
                current_strategy_hash = %self.config.strategy_hash,
                "引擎状态与当前配置/版本不匹配，全新开局"
            );
            return false;
        }
        if state.fills_len != journal.fills.len() {
            warn!(
                state_fills = state.fills_len,
                journal_fills = journal.fills.len(),
                "引擎状态与 journal fills 不一致（上次落盘不完整？），全新开局"
            );
            return false;
        }

        let dropped_position = state.position.is_some() && !allow_position;
        if dropped_position {
            warn!("非组合模式恢复：丢弃 journal 中的持仓（交易所侧已由人工/启动检查处理）");
        }
        let position = if allow_position { state.position } else { None };
        let needs_rearm = position.and_then(|p| p.stop_price).is_some();

        self.account = Account::from_parts(state.initial_cash, state.cash, position, journal.fills);
        self.intents = journal.intents;
        self.equity_curve = journal.equity_curve;
        self.evals = journal.evals;
        self.started_at = journal.meta.from;
        self.cb_day = state.cb_day;
        self.cb_day_start_equity = state.cb_day_start_equity;
        self.cb_consec_losses = state.cb_consec_losses;
        self.cb_on = state.cb_on;
        self.cb_noted_fills = state.cb_noted_fills;
        self.pos_track = state.pos_track;
        self.shadow_signals = state.shadow_signals;
        self.confirmed_signals_run = state.confirmed_signals_run;
        self.observation_signals_run = state.observation_signals_run;
        self.shadow_outcomes_run = state.shadow_outcomes_run;
        self.needs_stop_rearm = needs_rearm;
        self.sync_position_flag();

        info!(
            fills = self.account.fills().len(),
            cash = self.account.cash(),
            has_position = position.is_some(),
            needs_stop_rearm = self.needs_stop_rearm,
            "已从 journal 恢复引擎状态（重启续跑）"
        );
        true
    }

    /// 喂入订单簿/OI 等公共市场上下文；它们只更新信号状态，不直接触发下单。
    pub fn on_context_event(&mut self, ev: Event) {
        let ts = ev.ts();
        self.clock.advance_to(ts);
        self.ctx.now = Some(ts);
        for sp in self.strategy.signals.iter_mut() {
            for sig in sp.on_event(&ev, &self.ctx) {
                self.ctx.set_latest(sig);
            }
        }
        self.refresh_eval_notes(true);
    }

    /// 评估记录入环形流水（journal 展示，cap 300）并追加全量 JSONL（分析用）。
    fn push_eval(&mut self, entry: JournalEval) {
        self.latest_eval = Some(entry.note.clone());
        self.evals.push(entry.clone());
        if self.evals.len() > 300 {
            let excess = self.evals.len() - 300;
            self.evals.drain(0..excess);
        }
        // 全量 JSONL（append-only；失败不阻塞交易）
        let line = serde_json::json!({
            "ts_ms": entry.ts_ms,
            "run_id": self.config.run_id,
            "mode": self.config.mode,
            "symbol": self.config.symbol,
            "strategy_hash": self.config.strategy_hash,
            "git_commit": self.config.git_commit,
            "source": entry.source,
            "note": entry.note,
        });
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.eval_log_path)
        {
            use std::io::Write;
            if let Err(e) = writeln!(f, "{}", line) {
                warn!(error = %e, "eval JSONL 写入失败");
            }
        }
    }

    /// 收集各信号插件的评估说明（eval_note）。
    /// `append=true` 时把新评估追加进流水；`false`（预热）只更新最新快照与去重键。
    fn refresh_eval_notes(&mut self, append: bool) {
        let mut pending: Vec<JournalEval> = Vec::new();
        for (i, sp) in self.strategy.signals.iter().enumerate() {
            let Some(note) = sp.eval_note() else { continue };
            let key = (
                note.get("ts_ms").and_then(|v| v.as_i64()).unwrap_or(0),
                note.get("decision")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            );
            // 多路盘口/OI 的事件时间并不严格同序。只接受每个插件单调前进的新桶，
            // 避免不同来源在相邻/旧桶之间跳动时每秒写十几条重复市场地图。
            let changed = match self.last_eval_keys.get(i) {
                Some(Some(k)) => key.0 > k.0,
                _ => true,
            };
            if !changed {
                continue;
            }
            if let Some(slot) = self.last_eval_keys.get_mut(i) {
                *slot = Some(key);
            }
            if append {
                pending.push(JournalEval {
                    ts_ms: note.get("ts_ms").and_then(|v| v.as_i64()).unwrap_or(0),
                    source: sp.name().to_string(),
                    note,
                });
            } else {
                self.latest_eval = Some(note);
            }
        }
        for entry in pending {
            self.push_eval(entry);
        }
    }

    fn track_new_shadow_signals(&mut self, signals: &[Signal]) {
        for signal in signals {
            let p = &signal.payload;
            let stage = p.get("stage").and_then(|v| v.as_str()).unwrap_or("");
            if stage != "confirmed" && stage != "observation" {
                continue;
            }
            let Some(event_id) = p.get("event_id").and_then(|v| v.as_i64()) else {
                continue;
            };
            let confirm_price = p.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            if confirm_price <= 0.0
                || self
                    .shadow_signals
                    .iter()
                    .any(|existing| existing.event_id == event_id)
            {
                continue;
            }
            let shadow = ShadowSignal {
                event_id,
                signal_ts_ms: signal.ts.as_millis(),
                side: p
                    .get("side")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                profile: p
                    .get("profile")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                strength: p
                    .get("strength")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                event_price: p
                    .get("zone")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(confirm_price),
                confirm_price,
                stop_anchor: p.get("stop_anchor").and_then(|v| v.as_f64()),
                context_score: p.get("context_score").and_then(|v| v.as_u64()),
                location_confirmed: p.get("location_confirmed").and_then(|v| v.as_bool()),
                features: p
                    .get("observation")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                mfe_pct: 0.0,
                mae_pct: 0.0,
                next_horizon: 0,
            };
            self.append_research(
                if stage == "confirmed" {
                    "signal_confirmed"
                } else {
                    "observation_detected"
                },
                signal.ts.as_millis(),
                serde_json::json!({ "signal": p }),
            );
            self.shadow_signals.push(shadow);
            if stage == "confirmed" {
                self.confirmed_signals_run += 1;
            } else {
                self.observation_signals_run += 1;
            }
        }
    }

    fn update_shadow_outcomes(&mut self, ts: Timestamp, price: Price) {
        let now_ms = ts.as_millis();
        let px = price.to_f64();
        let fee_pct = self.config.estimated_roundtrip_fee_bps / 100.0;
        let mut completed = Vec::new();
        let mut events = Vec::new();
        for shadow in &mut self.shadow_signals {
            let sign = if shadow.side == "buy" { 1.0 } else { -1.0 };
            let gross = sign * (px / shadow.confirm_price - 1.0) * 100.0;
            shadow.mfe_pct = shadow.mfe_pct.max(gross);
            shadow.mae_pct = shadow.mae_pct.min(gross);
            while shadow.next_horizon < SHADOW_HORIZONS_MIN.len()
                && now_ms - shadow.signal_ts_ms >= SHADOW_HORIZONS_MIN[shadow.next_horizon] * 60_000
            {
                let horizon_min = SHADOW_HORIZONS_MIN[shadow.next_horizon];
                events.push((
                    now_ms,
                    serde_json::json!({
                        "event_id": shadow.event_id,
                        "signal_ts_ms": shadow.signal_ts_ms,
                        "horizon_min": horizon_min,
                        "side": shadow.side,
                        "profile": shadow.profile,
                        "strength": shadow.strength,
                        "event_price": shadow.event_price,
                        "confirm_price": shadow.confirm_price,
                        "mark_price": px,
                        "stop_anchor": shadow.stop_anchor,
                        "context_score": shadow.context_score,
                        "location_confirmed": shadow.location_confirmed,
                        "features": shadow.features,
                        "gross_return_pct": gross,
                        "estimated_net_return_pct": gross - fee_pct,
                        "mfe_pct": shadow.mfe_pct,
                        "mae_pct": shadow.mae_pct,
                    }),
                ));
                shadow.next_horizon += 1;
            }
            if shadow.next_horizon == SHADOW_HORIZONS_MIN.len() {
                completed.push(shadow.event_id);
            }
        }
        for (event_ts, data) in events {
            self.append_research("shadow_outcome", event_ts, data);
            self.shadow_outcomes_run += 1;
        }
        self.shadow_signals
            .retain(|shadow| !completed.contains(&shadow.event_id));
    }

    /// 持仓绩效追踪：开仓建档 → 持仓中更新 MFE/MAE → 平仓写 trip 记录。
    /// 平仓可能发生在 on_trade（dry 撮合）或 on_timer（testnet 轮询），两处都调。
    fn track_position(&mut self, ts: Timestamp, price: Price) {
        match (self.account.position().copied(), self.pos_track.take()) {
            (Some(p), None) => {
                // 建档即记录当前偏离（首根 K 线可能就是峰值，不能漏）
                let px = price.to_f64();
                let entry = p.entry_price.to_f64();
                let sign = if format!("{:?}", p.side) == "Buy" {
                    1.0
                } else {
                    -1.0
                };
                let dev = (px - entry) / entry * 100.0 * sign;
                self.pos_track = Some(PosTrack {
                    side: format!("{:?}", p.side),
                    entry_price: entry,
                    entry_ts_ms: ts.as_millis(),
                    mfe_pct: dev.max(0.0),
                    mae_pct: dev.min(0.0),
                    start_fill_idx: self.account.fills().len(),
                    last_qty: p.qty.to_f64(),
                    adds: 0,
                });
            }
            (Some(p), Some(mut t)) => {
                let px = price.to_f64();
                let sign = if t.side == "Buy" { 1.0 } else { -1.0 };
                let dev = (px - t.entry_price) / t.entry_price * 100.0 * sign;
                t.mfe_pct = t.mfe_pct.max(dev);
                t.mae_pct = t.mae_pct.min(dev);
                let q = p.qty.to_f64();
                if q > t.last_qty + 1e-9 {
                    t.adds += 1;
                }
                t.last_qty = q;
                self.pos_track = Some(t);
            }
            (None, Some(mut t)) => {
                // 平仓：平仓这笔成交的价格也是偏移路径的一部分（先更新再结算）
                let px = price.to_f64();
                let sign = if t.side == "Buy" { 1.0 } else { -1.0 };
                let dev = (px - t.entry_price) / t.entry_price * 100.0 * sign;
                t.mfe_pct = t.mfe_pct.max(dev);
                t.mae_pct = t.mae_pct.min(dev);
                // 汇总本 trip 的 fills，写 trip 记录（MFE/MAE 是优化 TP/SL 的核心数据）
                let fills = self.account.fills();
                let trip_fills = &fills[t.start_fill_idx.min(fills.len())..];
                let exit = trip_fills.last();
                let exit_price = exit.map(|f| f.price.to_f64());
                let exit_reason = exit.map(|f| f.reason.clone()).unwrap_or_default();
                let realized: f64 = trip_fills.iter().map(|f| f.realized_pnl - f.fee).sum();
                let holding_min = (ts.as_millis() - t.entry_ts_ms) as f64 / 60_000.0;
                let side_cn = if t.side == "Buy" { "多单" } else { "空单" };
                let reason = format!(
                    "{}出场（{}）：持仓 {:.0}min，净盈亏 {:+.2} USD（MFE {:+.2}% / MAE {:+.2}%，加仓 {} 次）",
                    side_cn, exit_reason, holding_min, realized, t.mfe_pct, t.mae_pct, t.adds
                );
                self.push_eval(JournalEval {
                    ts_ms: ts.as_millis(),
                    source: "engine".into(),
                    note: serde_json::json!({
                        "ts_ms": ts.as_millis(),
                        "decision": "trip_closed",
                        "side": t.side,
                        "entry_price": t.entry_price,
                        "exit_price": exit_price,
                        "exit_reason": exit_reason,
                        "pnl_usd": realized,
                        "mfe_pct": t.mfe_pct,
                        "mae_pct": t.mae_pct,
                        "holding_min": holding_min,
                        "adds": t.adds,
                        "reason": reason,
                    }),
                });
            }
            (None, None) => {}
        }
    }

    /// 状态快照（控制面轮询用）。
    pub fn snapshot(&self) -> EngineSnapshot {
        let px = self.latest_price;
        let plugin_note = |name: &str| {
            self.strategy
                .signals
                .iter()
                .find(|sp| sp.name() == name)
                .and_then(|sp| sp.eval_note())
        };
        EngineSnapshot {
            last_price: px.map(|p| p.to_f64()),
            equity: self.account.equity(px.unwrap_or(Price::ZERO)),
            cash: self.account.cash(),
            position: self.account.position().map(|p| PositionSnap {
                side: format!("{:?}", p.side),
                qty: p.qty.to_f64(),
                entry_price: p.entry_price.to_f64(),
                stop_price: p.stop_price.map(|s| s.to_f64()),
                unrealized_pnl: px.map(|x| p.unrealized(x)).unwrap_or(0.0),
            }),
            n_intents: self.intents.len(),
            n_fills: self.account.fills().len(),
            last_eval: plugin_note("OrderFlowExhaustion").or_else(|| self.latest_eval.clone()),
            market_map: plugin_note("TrdrMarketMap"),
            intraday_reversion: plugin_note("IntradayExtensionReversion"),
            run_id: self.config.run_id.clone(),
            strategy_name: self.config.strategy_name.clone(),
            strategy_hash: self.config.strategy_hash.clone(),
            git_commit: self.config.git_commit.clone(),
            research_log_dir: self.config.research_log_dir.display().to_string(),
            active_shadow_signals: self.shadow_signals.len(),
            confirmed_signals_run: self.confirmed_signals_run,
            observation_signals_run: self.observation_signals_run,
            shadow_outcomes_run: self.shadow_outcomes_run,
            execution_healthy: self.broker.execution_healthy(),
        }
    }

    // ==================================================================
    // 事件入口
    // ==================================================================

    /// 行情逐笔：与回测同序处理。
    pub async fn on_trade(&mut self, trade: &Trade) {
        self.clock.advance_to(trade.ts);
        self.latest_price = Some(trade.price);
        self.update_shadow_outcomes(trade.ts, trade.price);
        self.update_env_flags(trade.ts, trade.price);
        self.cb_update(trade.ts, trade.price);

        // 1) 撮合/成交回报（dry：本地模拟撮合；testnet：本地挂单不消费价格）
        let execs = self.broker.on_trade_price(trade.ts, trade.price).await;
        for ex in execs {
            self.log_execution(&ex, None, None);
            self.account.apply_fill(FillRequest {
                ts: ex.ts,
                side: ex.side,
                price: ex.price,
                qty: ex.qty,
                fee: ex.fee,
                is_maker: ex.is_maker,
                reason: ex.reason,
            });
        }
        self.settle_pending_entry(trade.ts).await;

        // 重启恢复：broker 是新建的（模拟/交易所挂单均不存在），
        // 首个行情节拍按 journal 记录的止损价重挂保护性止损
        if self.needs_stop_rearm {
            self.needs_stop_rearm = false;
            if let Some(p) = self.account.position().copied() {
                if let Some(stop) = p.stop_price {
                    info!(stop = stop.to_f64(), "恢复持仓：重挂保护性止损");
                    self.place_protective_stop(trade.ts, trade.price, stop)
                        .await;
                }
            }
        }

        // 2) 信号插件
        let ev = Event::Trade(trade.clone());
        self.ctx.now = Some(trade.ts);
        self.sync_position_flag();
        let mut new_signals: Vec<Signal> = Vec::new();
        for sp in self.strategy.signals.iter_mut() {
            for sig in sp.on_event(&ev, &self.ctx) {
                self.ctx.set_latest(sig.clone());
                new_signals.push(sig);
            }
        }
        self.track_new_shadow_signals(&new_signals);
        // 2.1) 评估说明入流水（为什么下单/不下单，前端展示）
        self.refresh_eval_notes(true);

        // 3) 持仓管理 / 开仓评估
        if self.account.position().is_some() {
            self.manage_position(trade).await;
            self.try_add(trade, &new_signals).await;
        } else {
            self.try_enter(trade, &new_signals).await;
        }

        // 4) 熔断统计
        self.cb_note_fills();
        self.cb_update(trade.ts, trade.price);
        // 5) 持仓绩效追踪（MFE/MAE，平仓时写 trip 记录）
        self.track_position(trade.ts, trade.price);
        self.persist_journal();
    }

    /// 1s 节拍：成交轮询 / 限价过期 / 资金费率 / 权益采样 / 对账。
    pub async fn on_timer(&mut self, now_ms: i64) {
        let now = Timestamp::from_millis(now_ms);

        // 成交轮询（2s）：testnet 的 fill 全部从这里来
        if now_ms - self.last_fill_poll_ms >= 2_000 {
            self.last_fill_poll_ms = now_ms;
            let execs = self.broker.poll_fills().await;
            let mut got_fill = false;
            for ex in execs {
                got_fill = true;
                let exit_match = self.pending_exit.as_ref().is_some_and(|pending| {
                    pending
                        .order_id
                        .zip(ex.order_id)
                        .map(|(expected, actual)| expected == actual)
                        .unwrap_or_else(|| pending.reason == ex.reason)
                });
                let exit_fill_qty = ex.qty.to_f64();
                let pending = self.pending_entry.as_ref();
                self.log_execution(
                    &ex,
                    pending.map(|p| p.intent_id.as_str()),
                    pending.and_then(|p| p.event_id),
                );
                self.account.apply_fill(FillRequest {
                    ts: ex.ts,
                    side: ex.side,
                    price: ex.price,
                    qty: ex.qty,
                    fee: ex.fee,
                    is_maker: ex.is_maker,
                    reason: ex.reason,
                });
                if exit_match {
                    if let Some(pending) = self.pending_exit.as_mut() {
                        pending.filled_qty += exit_fill_qty;
                    }
                }
            }
            if got_fill {
                self.settle_pending_entry(now).await;
                self.settle_pending_exit(now).await;
                self.cb_note_fills();
                if let Some(px) = self.latest_price {
                    self.track_position(now, px);
                }
                self.persist_journal();
            }
        }

        // 市价减仓通常数秒内成交。超时不盲目重发（否则可能重复减仓），且旧保护
        // 止损仍留在交易所；只记录一次明确告警，等待 userTrades 恢复后自动收敛。
        if let Some(pending) = self.pending_exit.as_mut() {
            if !pending.timeout_noted && now_ms - pending.submitted_ts_ms >= 30_000 {
                pending.timeout_noted = true;
                let note = pending.clone();
                warn!(
                    order_id = note.order_id,
                    reason = %note.reason,
                    requested_qty = note.requested_qty,
                    filled_qty = note.filled_qty,
                    "减仓成交回报超过 30 秒，保留原保护止损并停止重复减仓"
                );
                self.append_research(
                    "exit_fill_timeout",
                    now_ms,
                    serde_json::json!({
                        "order_id": note.order_id, "reason": note.reason,
                        "requested_qty": note.requested_qty, "filled_qty": note.filled_qty,
                        "submitted_ts_ms": note.submitted_ts_ms,
                        "protective_stop_preserved": true,
                    }),
                );
            }
        }

        // 过期限价入场：撤单 + 清挂起
        if let Some(pe) = self.pending_entry.clone() {
            if now_ms > pe.expire_ts.as_millis() && self.account.position().is_none() {
                info!("限价入场单过期，撤单");
                self.append_research(
                    "order_expired",
                    now_ms,
                    serde_json::json!({
                        "intent_id": pe.intent_id, "event_id": pe.event_id,
                        "order_id": pe.order_id, "submitted_ts_ms": pe.submitted_ts_ms,
                        "qty": pe.qty, "reason": pe.reason,
                    }),
                );
                self.broker.cancel_all().await;
                self.append_research(
                    "order_canceled",
                    now_ms,
                    serde_json::json!({
                        "intent_id": pe.intent_id, "event_id": pe.event_id,
                        "order_id": pe.order_id, "cause": "entry_ttl_expired",
                    }),
                );
                self.pending_entry = None;
            }
        }

        // 资金费率标志（5min）：与回测 Funding 事件对齐
        if now_ms - self.last_funding_poll_ms >= 300_000 {
            self.last_funding_poll_ms = now_ms;
            if let Some(rest) = self.broker.rest() {
                match rest.premium_index(&self.config.symbol).await {
                    Ok((_mark, rate)) => {
                        self.ctx
                            .flags
                            .insert("funding_rate".into(), format!("{}", rate));
                    }
                    Err(e) => warn!(error = %e, "premiumIndex 查询失败"),
                }
            }
        }

        // 权益采样（60s，前端曲线粒度；首个节拍立即采样）
        if let Some(px) = self.latest_price {
            if self.equity_curve.is_empty() || now_ms - self.last_equity_sample_ms >= 60_000 {
                self.last_equity_sample_ms = now_ms;
                self.equity_curve.push(EquityPoint {
                    ts_ms: now_ms,
                    equity: self.account.equity(px),
                });
                self.persist_journal();
            }
        }

        // 时钟漂移防护（6h）：长跑后本地时钟可能漂出 recvWindow（-1021）
        if now_ms - self.last_time_sync_ms >= 6 * 3_600_000 {
            self.last_time_sync_ms = now_ms;
            self.broker.resync_time().await;
        }

        // 钱包对账（1h）：本地记账 vs testnet 钱包，漂移告警
        if now_ms - self.last_reconcile_ms >= 3_600_000 {
            self.last_reconcile_ms = now_ms;
            if let Some(rest) = self.broker.rest() {
                match rest.wallet_balance_usdt().await {
                    Ok(wallet) => {
                        let drift = (wallet - self.account.cash()).abs();
                        if drift > 1.0 {
                            warn!(
                                wallet,
                                local = self.account.cash(),
                                drift,
                                "钱包与本地记账漂移（资金费率/外部操作？）"
                            );
                        } else {
                            info!(wallet, local = self.account.cash(), "钱包对账一致");
                        }
                    }
                    Err(e) => warn!(error = %e, "钱包对账查询失败"),
                }
            }
        }
    }

    /// 优雅退出：落盘（不撤交易所挂单——止损单是保护）。
    pub async fn shutdown(&mut self) {
        self.persist_journal();
        self.append_research(
            "run_stop",
            chrono::Utc::now().timestamp_millis(),
            serde_json::json!({
                "fills": self.account.fills().len(),
                "intents": self.intents.len(),
                "cash": self.account.cash(),
            }),
        );
        info!(
            fills = self.account.fills().len(),
            intents = self.intents.len(),
            "引擎退出，journal 已落盘；交易所挂单保留（止损保护）"
        );
    }

    // ==================================================================
    // 决策（与 BacktestEngine 同构）
    // ==================================================================

    async fn manage_position(&mut self, trade: &Trade) {
        // REST 已受理但 userTrades 尚未确认时，账户仍显示旧仓位。此时再次运行
        // ExitPlugin 会重复触发同一 TP，因此必须等成交状态收敛后再评估。
        if self.pending_exit.is_some() {
            return;
        }
        let pos_view = match self.account.position_view(&self.symbol) {
            Some(p) => p,
            None => return,
        };
        let mut actions = Vec::new();
        for ep in self.strategy.exits.iter() {
            actions.extend(ep.manage(&pos_view, &self.ctx));
        }
        let mut actions = actions.into_iter().peekable();
        while let Some(act) = actions.next() {
            match act {
                ExitAction::MoveStop(px) => {
                    self.place_protective_stop(trade.ts, trade.price, px).await;
                }
                ExitAction::ClosePartial(frac) => {
                    if let Some(p) = self.account.position().copied() {
                        let close_qty = Qty::from_f64(p.qty.to_f64() * frac);
                        // TP1 同一批动作中的 MoveStop(entry) 必须延后到部分成交确认后；
                        // TP2 没有 MoveStop，则沿用当前止损并按剩余数量重挂。
                        let rearm_stop = match actions.peek() {
                            Some(ExitAction::MoveStop(px)) => {
                                let px = *px;
                                actions.next();
                                Some(px)
                            }
                            _ => p.stop_price,
                        };
                        self.market_close(trade, close_qty, "tp_partial", rearm_stop)
                            .await;
                        // closed_frac 由 Account::apply_fill 按原始仓位口径维护。
                    }
                    return;
                }
                ExitAction::CloseAll => {
                    if let Some(p) = self.account.position().copied() {
                        self.market_close(trade, p.qty, "close_all", None).await;
                    }
                    return;
                }
                ExitAction::Reverse(intent) => {
                    if let Some(p) = self.account.position().copied() {
                        self.market_close(trade, p.qty, "reverse_out", None).await;
                    }
                    // 异步实盘不能在旧仓成交确认前反向开仓。当前模型不使用 Reverse；
                    // 若将来启用，应把 intent 持久化为成交后的 deferred entry。
                    self.append_research(
                        "reverse_entry_deferred",
                        trade.ts.as_millis(),
                        serde_json::json!({
                            "side": format!("{:?}", intent.side),
                            "reason": intent.reason,
                            "cause": "wait_exit_fill",
                        }),
                    );
                    return;
                }
            }
        }
    }

    async fn try_enter(&mut self, trade: &Trade, signals: &[Signal]) {
        if self.pending_entry.is_some() {
            return;
        }
        let intent = self
            .strategy
            .trigger
            .should_fire(signals, &self.ctx)
            .or_else(|| {
                self.strategy
                    .trigger
                    .on_signals(signals, &self.ctx, &self.symbol)
            });
        let Some(intent) = intent else { return };
        let event_id = signals
            .iter()
            .find_map(|signal| signal.payload.get("event_id").and_then(|v| v.as_i64()));
        if !self.broker.execution_healthy() {
            warn!(event_id, "成交回报通道不健康，禁止新开仓");
            self.append_research(
                "order_skipped",
                trade.ts.as_millis(),
                serde_json::json!({
                    "event_id": event_id, "cause": "execution_unhealthy",
                    "strategy_reason": intent.reason,
                }),
            );
            return;
        }
        let mut scale = 1.0f64;
        for fp in self.strategy.filters.iter() {
            match fp.check(&intent, &self.ctx) {
                Verdict::Allow => {}
                Verdict::Scale(s) => scale = scale.min(s),
                Verdict::Veto(reason) => {
                    self.append_research(
                        "order_skipped",
                        trade.ts.as_millis(),
                        serde_json::json!({
                            "event_id": event_id, "cause": "filter_veto", "detail": reason,
                            "strategy_reason": intent.reason,
                        }),
                    );
                    return;
                }
            }
        }
        let mut intent = intent;
        intent.qty = Qty::from_f64(intent.qty.to_f64() * scale);
        self.enter(trade, intent, event_id).await;
    }

    async fn try_add(&mut self, trade: &Trade, signals: &[Signal]) {
        if self.pending_entry.is_some() {
            return;
        }
        let Some(pos) = self.account.position_view(&self.symbol) else {
            return;
        };
        let Some(intent) = self
            .strategy
            .trigger
            .on_add(&pos, signals, &self.ctx, &self.symbol)
        else {
            return;
        };
        let event_id = signals
            .iter()
            .find_map(|signal| signal.payload.get("event_id").and_then(|v| v.as_i64()));
        if !self.broker.execution_healthy() {
            warn!(event_id, "成交回报通道不健康，禁止加仓");
            self.append_research(
                "order_skipped",
                trade.ts.as_millis(),
                serde_json::json!({
                    "event_id": event_id, "cause": "execution_unhealthy",
                    "strategy_reason": intent.reason, "is_add": true,
                }),
            );
            return;
        }
        let mut scale = 1.0f64;
        for fp in self.strategy.filters.iter() {
            match fp.check(&intent, &self.ctx) {
                Verdict::Allow => {}
                Verdict::Scale(s) => scale = scale.min(s),
                Verdict::Veto(reason) => {
                    self.append_research(
                        "order_skipped",
                        trade.ts.as_millis(),
                        serde_json::json!({
                            "event_id": event_id, "cause": "filter_veto", "detail": reason,
                            "strategy_reason": intent.reason, "is_add": true,
                        }),
                    );
                    return;
                }
            }
        }
        let mut intent = intent;
        intent.qty = Qty::from_f64(intent.qty.to_f64() * scale);
        self.enter(trade, intent, event_id).await;
    }

    /// 开仓：仓位计算（与回测同式）→ 精度约束 → 记录意图 → 下单。
    async fn enter(&mut self, trade: &Trade, intent: OrderIntent, event_id: Option<i64>) {
        let entry_ref = intent.limit_price.unwrap_or(trade.price);
        let stop_dist = (intent.stop_price.to_f64() - entry_ref.to_f64()).abs();
        let qty = if stop_dist > 1e-9 {
            let risk_frac = self.config.risk_pct.min(self.config.max_risk_pct);
            let risk_usd =
                self.account.equity(trade.price) * risk_frac * intent.risk_scale.clamp(0.0, 1.0);
            let q = (risk_usd / stop_dist).min(
                self.account.equity(trade.price).max(0.0) * self.config.max_leverage
                    / entry_ref.to_f64(),
            );
            if intent.qty.to_f64() > 1e-9 {
                q.min(intent.qty.to_f64())
            } else {
                q
            }
        } else {
            intent.qty.to_f64()
        };
        // 精度约束：数量向下取整到步长；名义价值过低放弃
        let qty = floor_to_step(qty, self.config.qty_step);
        if qty <= 1e-9 {
            self.append_research(
                "order_skipped",
                trade.ts.as_millis(),
                serde_json::json!({
                    "event_id": event_id, "cause": "quantity_zero", "raw_qty": qty,
                    "strategy_reason": intent.reason,
                }),
            );
            return;
        }
        if self.config.min_notional > 0.0 && qty * trade.price.to_f64() < self.config.min_notional {
            warn!(
                qty,
                notional = qty * trade.price.to_f64(),
                min = self.config.min_notional,
                "名义价值低于最小约束，放弃下单"
            );
            self.append_research(
                "order_skipped",
                trade.ts.as_millis(),
                serde_json::json!({
                    "event_id": event_id, "cause": "min_notional", "qty": qty,
                    "notional": qty * trade.price.to_f64(), "minimum": self.config.min_notional,
                    "strategy_reason": intent.reason,
                }),
            );
            return;
        }
        let qty = Qty::from_f64(qty);
        let intent_id = format!("{}-{}", self.config.run_id, self.intents.len() + 1);

        self.intents.push(JournalIntent {
            ts_ms: trade.ts.as_millis(),
            side: format!("{:?}", intent.side),
            qty: qty.to_f64(),
            limit_price: intent.limit_price.map(|p| p.to_f64()),
            stop_price: intent.stop_price.to_f64(),
            tp1_price: intent.tp1_price.map(|p| p.to_f64()),
            reason: intent.reason.clone(),
        });
        self.persist_journal();
        self.append_research(
            "order_intent",
            trade.ts.as_millis(),
            serde_json::json!({
                "intent_id": intent_id, "event_id": event_id,
                "side": format!("{:?}", intent.side), "qty": qty.to_f64(),
                "limit_price": intent.limit_price.map(|p| p.to_f64()),
                "stop_price": intent.stop_price.to_f64(),
                "tp1_price": intent.tp1_price.map(|p| p.to_f64()),
                "reason": intent.reason, "risk_scale": intent.risk_scale,
                "mark_price": trade.price.to_f64(),
            }),
        );

        let is_limit = intent.limit_price.is_some();
        // 市价单成交回报也应很快到达：testnet 给 120s 兜底窗口；限价用 entry_ttl
        let ttl = if is_limit {
            self.config.entry_ttl_ms
        } else {
            120_000
        };
        let expire_ts = Timestamp::from_millis(trade.ts.as_millis() + ttl);

        let order = backtest::Order {
            side: intent.side,
            qty,
            kind: match intent.limit_price {
                Some(lp) => backtest::OrderKind::Limit(lp),
                None => backtest::OrderKind::Market,
            },
            reason: intent.reason.clone(),
            expire_ts: if is_limit { Some(expire_ts) } else { None },
        };
        match self.broker.submit(trade.ts, trade.price, order).await {
            Ok(Some(ex)) => {
                // dry 市价单立即成交
                self.log_execution(&ex, Some(&intent_id), event_id);
                self.account.apply_fill(FillRequest {
                    ts: ex.ts,
                    side: ex.side,
                    price: ex.price,
                    qty: ex.qty,
                    fee: ex.fee,
                    is_maker: ex.is_maker,
                    reason: ex.reason,
                });
                self.place_protective_stop(trade.ts, trade.price, intent.stop_price)
                    .await;
            }
            Ok(None) => {
                let order_id = self.broker.last_submitted_order_id();
                self.append_research(
                    "order_accepted",
                    trade.ts.as_millis(),
                    serde_json::json!({
                        "intent_id": intent_id, "event_id": event_id, "order_id": order_id,
                        "qty": qty.to_f64(), "reason": intent.reason,
                        "expires_ts_ms": expire_ts.as_millis(),
                    }),
                );
                self.pending_entry = Some(PendingEntry {
                    stop_price: intent.stop_price,
                    tp1_price: intent.tp1_price,
                    expire_ts,
                    intent_id,
                    event_id,
                    order_id,
                    submitted_ts_ms: trade.ts.as_millis(),
                    qty: qty.to_f64(),
                    reason: intent.reason.clone(),
                });
            }
            Err(e) => {
                error!(error = %e, reason = %intent.reason, "下单失败");
                self.append_research(
                    "order_rejected",
                    trade.ts.as_millis(),
                    serde_json::json!({
                        "intent_id": intent_id, "event_id": event_id,
                        "reason": intent.reason, "error": e.to_string(),
                    }),
                );
            }
        }
    }

    /// 持仓建立后挂保护性止损（先清旧单，按总仓重挂）。
    async fn place_protective_stop(&mut self, ts: Timestamp, ref_price: Price, stop: Price) {
        if let Some(p) = self.account.position_mut() {
            p.stop_price = Some(stop);
            if p.initial_stop_price.is_none() {
                p.initial_stop_price = Some(stop);
            }
            if (stop.to_f64() - p.entry_price.to_f64()).abs() < 1e-9 {
                p.breakeven_moved = true;
            }
            let side = p.side;
            let q = p.qty;
            self.broker.cancel_all().await;
            match self
                .broker
                .submit(
                    ts,
                    ref_price,
                    backtest::Order {
                        side: side.opposite(),
                        qty: q,
                        kind: backtest::OrderKind::StopMarket(stop),
                        reason: "stop".into(),
                        expire_ts: None,
                    },
                )
                .await
            {
                Ok(_) => self.append_research(
                    "protective_stop_accepted",
                    ts.as_millis(),
                    serde_json::json!({
                        "order_id": self.broker.last_submitted_order_id(),
                        "side": format!("{:?}", side.opposite()), "qty": q.to_f64(),
                        "stop_price": stop.to_f64(),
                    }),
                ),
                Err(e) => {
                    error!(error = %e, "保护性止损挂单失败");
                    self.append_research(
                        "protective_stop_rejected",
                        ts.as_millis(),
                        serde_json::json!({
                            "side": format!("{:?}", side.opposite()), "qty": q.to_f64(),
                            "stop_price": stop.to_f64(), "error": e.to_string(),
                        }),
                    );
                }
            }
        }
    }

    /// 挂起入场的善后：成交 → 补挂止损；过期由 on_timer 处理。
    async fn settle_pending_entry(&mut self, now: Timestamp) {
        let Some(pe) = self.pending_entry.clone() else {
            return;
        };
        if self.account.position().is_some() {
            self.append_research(
                "entry_settled",
                now.as_millis(),
                serde_json::json!({
                    "intent_id": pe.intent_id, "event_id": pe.event_id,
                    "order_id": pe.order_id, "latency_ms": now.as_millis() - pe.submitted_ts_ms,
                    "qty": pe.qty, "reason": pe.reason,
                }),
            );
            if let Some(p) = self.account.position_mut() {
                p.tp1_price = pe.tp1_price;
            }
            let ref_price = self.latest_price.unwrap_or(pe.stop_price);
            self.place_protective_stop(now, ref_price, pe.stop_price)
                .await;
            self.pending_entry = None;
        }
    }

    async fn market_close(
        &mut self,
        trade: &Trade,
        qty: Qty,
        reason: &str,
        rearm_stop: Option<Price>,
    ) {
        let Some(p) = self.account.position() else {
            return;
        };
        let side = p.side.opposite();
        // 不在提交市价减仓前撤保护止损。交易所成交回报可能延迟；先撤会制造一个
        // 无保护窗口。成交确认后 settle_pending_exit 会撤旧止损并按剩余数量重挂。
        let order = backtest::Order {
            side,
            qty,
            kind: backtest::OrderKind::Market,
            reason: reason.to_string(),
            expire_ts: None,
        };
        match self.broker.submit(trade.ts, trade.price, order).await {
            Ok(Some(ex)) => {
                self.log_execution(&ex, None, None);
                self.account.apply_fill(FillRequest {
                    ts: ex.ts,
                    side: ex.side,
                    price: ex.price,
                    qty: ex.qty,
                    fee: ex.fee,
                    is_maker: ex.is_maker,
                    reason: ex.reason,
                });
                if self.account.position().is_some() {
                    if let Some(stop) = rearm_stop {
                        self.place_protective_stop(trade.ts, trade.price, stop)
                            .await;
                    }
                } else {
                    self.broker.cancel_all().await;
                }
            }
            Ok(None) => {
                let order_id = self.broker.last_submitted_order_id();
                self.append_research(
                    "exit_order_accepted",
                    trade.ts.as_millis(),
                    serde_json::json!({
                        "order_id": order_id, "reason": reason,
                        "side": format!("{:?}", side), "qty": qty.to_f64(),
                        "rearm_stop": rearm_stop.map(|x| x.to_f64()),
                        "protective_stop_preserved": true,
                    }),
                );
                self.pending_exit = Some(PendingExit {
                    order_id,
                    reason: reason.to_string(),
                    requested_qty: qty.to_f64(),
                    filled_qty: 0.0,
                    submitted_ts_ms: trade.ts.as_millis(),
                    rearm_stop,
                    timeout_noted: false,
                });
            }
            Err(e) => {
                error!(error = %e, reason, "平仓下单失败");
                self.append_research(
                    "exit_order_rejected",
                    trade.ts.as_millis(),
                    serde_json::json!({
                        "reason": reason, "error": e.to_string(), "qty": qty.to_f64(),
                    }),
                );
            }
        }
    }

    /// userTrades 已确认减仓数量后，撤掉旧总仓止损，并按真实剩余仓位重新挂单。
    async fn settle_pending_exit(&mut self, now: Timestamp) {
        let Some(pending) = self.pending_exit.as_ref() else {
            return;
        };
        let complete =
            self.account.position().is_none() || pending.filled_qty + 1e-9 >= pending.requested_qty;
        if !complete {
            return;
        }
        let pending = self
            .pending_exit
            .take()
            .expect("pending exit checked above");
        let remaining_qty = self
            .account
            .position()
            .map(|position| position.qty.to_f64())
            .unwrap_or(0.0);
        self.append_research(
            "exit_settled",
            now.as_millis(),
            serde_json::json!({
                "order_id": pending.order_id, "reason": pending.reason,
                "requested_qty": pending.requested_qty, "filled_qty": pending.filled_qty,
                "remaining_qty": remaining_qty,
                "latency_ms": now.as_millis() - pending.submitted_ts_ms,
                "rearm_stop": pending.rearm_stop.map(|x| x.to_f64()),
            }),
        );
        if remaining_qty > 1e-9 {
            if let Some(stop) = pending.rearm_stop {
                let ref_price = self.latest_price.unwrap_or(stop);
                self.place_protective_stop(now, ref_price, stop).await;
            } else {
                // 理论上部分减仓始终携带当前止损；若状态缺失，保留旧保护单并告警。
                warn!("部分减仓完成但缺少重挂止损价格，保留交易所原保护单");
            }
        } else {
            // 全平后清掉仍挂在交易所的旧保护止损。
            self.broker.cancel_all().await;
        }
    }

    // ==================================================================
    // 环境/熔断/流水（与回测同逻辑）
    // ==================================================================

    fn log_execution(
        &self,
        ex: &backtest::Execution,
        intent_id: Option<&str>,
        event_id: Option<i64>,
    ) {
        self.append_research(
            "order_filled",
            ex.ts.as_millis(),
            serde_json::json!({
                "intent_id": intent_id,
                "event_id": event_id,
                "order_id": ex.order_id,
                "trade_id": ex.trade_id,
                "side": format!("{:?}", ex.side),
                "price": ex.price.to_f64(),
                "qty": ex.qty.to_f64(),
                "fee": ex.fee,
                "is_maker": ex.is_maker,
                "reason": ex.reason,
            }),
        );
    }

    fn sync_position_flag(&mut self) {
        self.ctx.position = self.account.position_view(&self.symbol);
    }

    fn update_env_flags(&mut self, ts: Timestamp, price: Price) {
        use chrono::{Datelike, Timelike};
        let secs = ts.as_millis() / 1000;
        let dt = chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default();
        let hour = dt.hour();
        let weekday = dt.weekday();
        let session = if matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun) {
            "weekend"
        } else if hour < 7 {
            "asia"
        } else if hour < 13 {
            "europe"
        } else {
            "us"
        };
        self.ctx.flags.insert("session".into(), session.into());
        self.ctx
            .flags
            .insert("last_price".into(), format!("{}", price.to_f64()));
    }

    fn cb_update(&mut self, ts: Timestamp, price: Price) {
        let day = ts.as_millis() / 86_400_000;
        if day != self.cb_day {
            self.cb_day = day;
            self.cb_day_start_equity = self.account.equity(price);
            self.cb_consec_losses = 0;
            self.cb_on = false;
        }
        let eq = self.account.equity(price);
        if self.config.cb_daily_dd_pct > 0.0
            && self.cb_day_start_equity > 0.0
            && eq < self.cb_day_start_equity * (1.0 - self.config.cb_daily_dd_pct)
        {
            self.cb_on = true;
        }
        if self.config.cb_max_daily_losses > 0
            && self.cb_consec_losses >= self.config.cb_max_daily_losses
        {
            self.cb_on = true;
        }
        let v = if self.cb_on { "on" } else { "off" };
        self.ctx.flags.insert("circuit_breaker".into(), v.into());
    }

    fn cb_note_fills(&mut self) {
        let fills = self.account.fills();
        while self.cb_noted_fills < fills.len() {
            let f = &fills[self.cb_noted_fills];
            self.cb_noted_fills += 1;
            if f.realized_pnl.abs() > 1e-9 {
                if f.realized_pnl - f.fee < 0.0 {
                    self.cb_consec_losses += 1;
                } else {
                    self.cb_consec_losses = 0;
                }
            }
        }
    }

    /// 原子重写 journal（tmp + rename）。
    pub fn persist_journal(&self) {
        let px = self.latest_price.unwrap_or(Price::ZERO);
        let engine_state = EngineState {
            version: ENGINE_STATE_VERSION,
            symbol: self.config.symbol.clone(),
            strategy: self.config.strategy_name.clone(),
            strategy_hash: self.config.strategy_hash.clone(),
            initial_cash: self.account.initial_cash(),
            cash: self.account.cash(),
            position: self.account.position().copied(),
            fills_len: self.account.fills().len(),
            cb_day: self.cb_day,
            cb_day_start_equity: self.cb_day_start_equity,
            cb_consec_losses: self.cb_consec_losses,
            cb_on: self.cb_on,
            cb_noted_fills: self.cb_noted_fills,
            pos_track: self.pos_track.clone(),
            shadow_signals: self.shadow_signals.clone(),
            confirmed_signals_run: self.confirmed_signals_run,
            observation_signals_run: self.observation_signals_run,
            shadow_outcomes_run: self.shadow_outcomes_run,
        };
        let journal = Journal {
            meta: JournalMeta {
                symbol: self.config.symbol.clone(),
                from: self.started_at.clone(),
                to: "live".into(),
                strategy: self.config.strategy_name.clone(),
                initial_cash: self.account.initial_cash(),
                final_equity: self.account.equity(px),
            },
            intents: self.intents.clone(),
            fills: self.account.fills().to_vec(),
            equity_curve: self.equity_curve.clone(),
            evals: self.evals.clone(),
            engine: serde_json::to_value(engine_state).ok(),
        };
        let path = &self.config.journal_path;
        let tmp = path.with_extension("tmp");
        match serde_json::to_string_pretty(&journal) {
            Ok(text) => {
                if let Err(e) = std::fs::write(&tmp, text) {
                    warn!(error = %e, "journal tmp 写入失败");
                    return;
                }
                if let Err(e) = std::fs::rename(&tmp, path) {
                    warn!(error = %e, "journal rename 失败");
                }
            }
            Err(e) => warn!(error = %e, "journal 序列化失败"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use backtest::FeeModel;
    use strategy::{assemble_from_toml, builtin_registry};
    use tcore::plugin::SignalKind;
    use tcore::types::{Exchange, Side};

    fn trade(ts_ms: i64, price: f64) -> Trade {
        Trade {
            ts: Timestamp::from_millis(ts_ms),
            exchange: Exchange::BinanceFutures,
            symbol: Symbol::new("BTCUSDT"),
            price: Price::from_f64(price),
            qty: Qty::from_f64(0.01),
            is_buyer_maker: false,
        }
    }

    fn noop_strategy() -> Strategy {
        let toml = r#"
[strategy]
trigger = "OrderFlowEntry"
"#;
        assemble_from_toml(toml, &builtin_registry()).unwrap()
    }

    fn test_config(tag: &str) -> LiveConfig {
        LiveConfig {
            symbol: "BTCUSDT".into(),
            risk_pct: 0.0075,
            max_risk_pct: 0.015,
            max_leverage: 3.0,
            entry_ttl_ms: 4 * 3_600_000,
            cb_max_daily_losses: 0,
            cb_daily_dd_pct: 0.0,
            qty_step: 1e-8,
            min_notional: 0.0,
            journal_path: std::env::temp_dir().join(format!("greed-live-test-{}.json", tag)),
            eval_log_path: std::env::temp_dir().join(format!("greed-live-test-{}.jsonl", tag)),
            research_log_dir: std::env::temp_dir().join(format!("greed-live-research-{tag}")),
            strategy_name: "test".into(),
            strategy_hash: "test-hash".into(),
            strategy_snapshot: "[strategy]".into(),
            git_commit: "test-commit".into(),
            run_id: format!("test-{tag}"),
            mode: "dry".into(),
            estimated_roundtrip_fee_bps: 6.0,
        }
    }

    /// 干跑冒烟：noop 策略吃 1000 笔逐笔，不交易、权益守恒、journal 可解析。
    #[tokio::test]
    async fn dry_run_smoke_conserves() {
        let cfg = test_config("smoke");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        for i in 0..1000i64 {
            eng.on_trade(&trade(i * 100, 67000.0 + (i % 50) as f64))
                .await;
        }
        assert!(eng.account().fills().is_empty());
        assert!(eng.account().conservation_error(Price::from_f64(67200.0)) < 1e-9);
        // journal 已落盘且契约完整（前端三要素：intents/fills/equity_curve）
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(j["meta"]["symbol"], "BTCUSDT");
        assert_eq!(j["meta"]["to"], "live");
        assert!(j["intents"].is_array());
        assert!(j["fills"].is_array());
        assert!(j["equity_curve"].is_array());
        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    /// 干跑全链路：手动开仓（市价）→ 止损单自动挂出 → 价格击穿 → 止损成交。
    #[tokio::test]
    async fn dry_run_entry_then_stop() {
        let cfg = test_config("stop");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        let t0 = trade(0, 67000.0);
        eng.on_trade(&t0).await;
        let intent = OrderIntent {
            symbol: Symbol::new("BTCUSDT"),
            side: Side::Buy,
            qty: Qty::from_f64(0.1),
            risk_scale: 1.0,
            limit_price: None,
            stop_price: Price::from_f64(66800.0),
            tp1_price: None,
            reason: "manual".into(),
            ts: t0.ts,
        };
        eng.enter(&t0, intent, None).await;
        assert!(eng.account().position().is_some());
        // 意图已入流水（含止损价与原因）
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(j["intents"][0]["reason"], "manual");
        assert!((j["intents"][0]["stop_price"].as_f64().unwrap() - 66800.0).abs() < 1e-9);

        // 价格跌破止损 → 平仓
        eng.on_trade(&trade(1000, 66900.0)).await;
        eng.on_trade(&trade(2000, 66750.0)).await;
        assert!(eng.account().position().is_none());
        assert_eq!(eng.account().fills().len(), 2);
        assert_eq!(eng.account().fills()[1].reason, "stop");
        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    /// testnet 异步部分止盈的状态机回归：只有实际成交确认后才按剩余数量重挂止损，
    /// 且新的止损触发时不会使用减仓前的旧数量反向开仓。
    #[tokio::test]
    async fn async_partial_exit_rearms_stop_for_remaining_qty() {
        let cfg = test_config("partial-exit");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-08-04".into(),
        );
        eng.latest_price = Some(Price::from_f64(101.0));
        eng.account.apply_fill(FillRequest {
            ts: Timestamp::from_millis(1_000),
            side: Side::Buy,
            price: Price::from_f64(100.0),
            qty: Qty::from_f64(1.0),
            fee: 0.0,
            is_maker: false,
            reason: "open".into(),
        });
        eng.place_protective_stop(
            Timestamp::from_millis(1_000),
            Price::from_f64(100.0),
            Price::from_f64(99.0),
        )
        .await;
        eng.pending_exit = Some(PendingExit {
            order_id: Some(42),
            reason: "tp_partial".into(),
            requested_qty: 0.5,
            filled_qty: 0.5,
            submitted_ts_ms: 1_500,
            rearm_stop: Some(Price::from_f64(100.0)),
            timeout_noted: false,
        });
        eng.account.apply_fill(FillRequest {
            ts: Timestamp::from_millis(2_000),
            side: Side::Sell,
            price: Price::from_f64(101.5),
            qty: Qty::from_f64(0.5),
            fee: 0.0,
            is_maker: false,
            reason: "tp_partial".into(),
        });
        eng.settle_pending_exit(Timestamp::from_millis(2_000)).await;
        let remaining = eng.account.position().expect("应保留一半仓位");
        assert!((remaining.qty.to_f64() - 0.5).abs() < 1e-9);
        assert_eq!(remaining.stop_price, Some(Price::from_f64(100.0)));
        assert!(eng.pending_exit.is_none());

        // 重挂后的止损只平剩余 0.5，不会按原始 1.0 数量形成反向仓位。
        eng.on_trade(&trade(3_000, 99.9)).await;
        assert!(eng.account.position().is_none());
        assert!((eng.account.fills().last().unwrap().qty.to_f64() - 0.5).abs() < 1e-9);
        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    /// 定时器路径：权益采样（60s 节律）。
    #[tokio::test]
    async fn timer_samples_equity() {
        let cfg = test_config("timer");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        eng.on_trade(&trade(0, 67000.0)).await;
        eng.on_timer(1_000).await; // 首个采样点
        eng.on_timer(30_000).await; // 未到 60s，不采样
        eng.on_timer(61_000).await; // 第二个采样点
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(j["equity_curve"].as_array().unwrap().len(), 2);
        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    /// 持仓绩效：开仓→先涨 1% 再击穿止损，trip 记录含 MFE/MAE、出场原因，且 JSONL 全量落盘。
    #[tokio::test]
    async fn trip_records_mfe_mae() {
        let cfg = test_config("trip");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let _ = std::fs::remove_file(&cfg.eval_log_path);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        let t0 = trade(0, 67000.0);
        eng.on_trade(&t0).await;
        let intent = OrderIntent {
            symbol: Symbol::new("BTCUSDT"),
            side: Side::Buy,
            qty: Qty::from_f64(0.1),
            risk_scale: 1.0,
            limit_price: None,
            stop_price: Price::from_f64(66800.0),
            tp1_price: None,
            reason: "manual".into(),
            ts: t0.ts,
        };
        eng.enter(&t0, intent, None).await;
        // 先涨 1%（MFE 峰值），再回落击穿止损
        eng.on_trade(&trade(1000, 67670.0)).await;
        eng.on_trade(&trade(2000, 67200.0)).await;
        eng.on_trade(&trade(3000, 66750.0)).await;
        assert!(eng.account().position().is_none());

        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        let trips: Vec<_> = j["evals"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["note"]["decision"] == "trip_closed")
            .collect();
        assert_eq!(trips.len(), 1, "应恰好一条 trip 记录");
        let note = &trips[0]["note"];
        assert!(
            note["mfe_pct"].as_f64().unwrap() > 0.9,
            "MFE 应约 +1%: {}",
            note["mfe_pct"]
        );
        assert!(note["mae_pct"].as_f64().unwrap() < 0.0, "MAE 应为负");
        assert_eq!(note["exit_reason"], "stop");
        assert_eq!(note["side"], "Buy");
        assert!(note["pnl_usd"].as_f64().unwrap() < 0.0, "止损单净盈亏为负");
        assert!(note["reason"].as_str().unwrap().contains("MFE"));

        // JSONL 全量落盘含同一条记录
        let jsonl = std::fs::read_to_string(&cfg.eval_log_path).unwrap();
        assert!(jsonl.contains("trip_closed"));
        let _ = std::fs::remove_file(&cfg.journal_path);
        let _ = std::fs::remove_file(&cfg.eval_log_path);
    }

    /// 评估流水：订单流模型积累真实成交桶后，journal 与快照均可解释。
    #[tokio::test]
    async fn eval_notes_flow_to_journal() {
        let toml = r#"
[strategy]
signals = ["OrderFlowExhaustion"]
trigger = "OrderFlowEntry"

[strategy.plugins.OrderFlowExhaustion]
bucket_ms = 10000
min_baseline_buckets = 5
baseline_buckets = 10
location_buckets = 10
"#;
        let strat = assemble_from_toml(toml, &builtin_registry()).unwrap();
        let cfg = test_config("evals");
        let _ = std::fs::remove_file(&cfg.journal_path);
        let mut eng = LiveEngine::new(
            strat,
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "2026-07-26".into(),
        );
        for i in 0..30i64 {
            let price = 67000.0 + ((i % 3) as f64 - 1.0);
            eng.on_trade(&trade(i * 10_000, price)).await;
        }
        // 快照带最新评估
        let snap = eng.snapshot();
        let eval = snap.last_eval.expect("快照应带 last_eval");
        assert_eq!(eval["decision"], "none");
        assert!(eval["reason"].as_str().unwrap().contains("等待"));
        // journal 落盘含预热与逐桶评估。
        let text = std::fs::read_to_string(&cfg.journal_path).unwrap();
        let j: serde_json::Value = serde_json::from_str(&text).unwrap();
        let evals = j["evals"].as_array().unwrap();
        assert!(evals.len() >= 20, "evals={}", evals.len());
        assert_eq!(evals[0]["source"], "OrderFlowExhaustion");
        assert_eq!(evals[0]["note"]["decision"], "warmup");
        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    /// 重启续跑（P1-1 回归）：持仓/现金/fills/熔断计数从 journal 恢复；
    /// 恢复后首个行情节拍按 journal 止损价重挂保护性止损；
    /// 非组合模式恢复时丢弃持仓（交易所侧已由启动检查保证为空）。
    #[tokio::test]
    async fn restore_resumes_position_and_account() {
        let cfg = test_config("restore");
        let _ = std::fs::remove_file(&cfg.journal_path);

        // 第一段进程：注入两笔成交（开仓 + 部分止盈），制造非平凡账户状态
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            10_000.0,
            "2026-08-01".into(),
        );
        eng.account.apply_fill(FillRequest {
            ts: Timestamp::from_millis(1000),
            side: Side::Buy,
            price: Price::from_f64(60_000.0),
            qty: Qty::from_f64(0.2),
            fee: 1.0,
            is_maker: false,
            reason: "open".into(),
        });
        eng.account.position_mut().unwrap().stop_price = Some(Price::from_f64(59_000.0));
        eng.account.apply_fill(FillRequest {
            ts: Timestamp::from_millis(2000),
            side: Side::Sell,
            price: Price::from_f64(61_000.0),
            qty: Qty::from_f64(0.1),
            fee: 1.0,
            is_maker: false,
            reason: "tp_partial".into(),
        });
        eng.cb_day = 42;
        eng.cb_consec_losses = 2;
        eng.persist_journal();

        // 第二段进程：新引擎从 journal 恢复（组合模式，allow_position=true）
        let mut eng2 = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            10_000.0,
            "2026-08-01".into(),
        );
        assert!(eng2.try_restore(true));
        assert_eq!(eng2.account().fills().len(), 2);
        // cash = 10000 - 1(开仓费) + 100(止盈 0.1 × $1000) - 1(平仓费)
        assert!((eng2.account().cash() - 10_098.0).abs() < 1e-6);
        let pos = eng2.account().position().expect("持仓应恢复");
        assert_eq!(pos.side, Side::Buy);
        assert!((pos.qty.to_f64() - 0.1).abs() < 1e-9);
        assert!((pos.closed_frac - 0.5).abs() < 1e-9);
        assert_eq!(pos.stop_price, Some(Price::from_f64(59_000.0)));
        assert!(eng2.needs_stop_rearm);
        assert_eq!(eng2.cb_day, 42);
        assert_eq!(eng2.cb_consec_losses, 2);

        // 恢复后首个行情节拍：重挂保护性止损且不丢失止损价
        eng2.on_trade(&trade(3000, 60_500.0)).await;
        assert!(!eng2.needs_stop_rearm);
        assert_eq!(
            eng2.account().position().unwrap().stop_price,
            Some(Price::from_f64(59_000.0))
        );

        // 非组合模式恢复：持仓丢弃，现金/fills 保留（人工已平仓的场景）
        let mut eng3 = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            10_000.0,
            "2026-08-01".into(),
        );
        assert!(eng3.try_restore(false));
        assert!(eng3.account().position().is_none());
        assert_eq!(eng3.account().fills().len(), 2);
        assert!((eng3.account().cash() - 10_098.0).abs() < 1e-6);

        let _ = std::fs::remove_file(&cfg.journal_path);
    }

    #[test]
    fn confirmed_signal_records_all_shadow_horizons() {
        let cfg = test_config("shadow");
        let _ = std::fs::remove_dir_all(&cfg.research_log_dir);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "1970-01-01".into(),
        );
        let signal = Signal::new(
            SignalKind::Other,
            Timestamp::from_millis(1_000),
            "OrderFlowExhaustion",
            serde_json::json!({
                "stage": "confirmed", "event_id": 1, "side": "buy",
                "profile": "observation", "strength": "weak",
                "price": 100.0, "zone": 99.5, "stop_anchor": 98.0,
                "context_score": 1, "location_confirmed": false,
            }),
        );
        eng.track_new_shadow_signals(&[signal]);
        for (i, horizon) in SHADOW_HORIZONS_MIN.iter().enumerate() {
            eng.update_shadow_outcomes(
                Timestamp::from_millis(1_000 + horizon * 60_000),
                Price::from_f64(101.0 + i as f64),
            );
        }
        assert!(eng.shadow_signals.is_empty());
        let text = std::fs::read_to_string(cfg.research_log_dir.join("1970-01-01.jsonl"))
            .expect("shadow research log");
        assert_eq!(text.matches("\"event_type\":\"shadow_outcome\"").count(), 6);
        assert!(text.contains("\"estimated_net_return_pct\""));
        assert!(text.contains("\"profile\":\"observation\""));
        let _ = std::fs::remove_dir_all(&cfg.research_log_dir);
    }

    #[test]
    fn observation_signal_tracks_outcomes_without_counting_as_confirmed() {
        let cfg = test_config("observation-shadow");
        let _ = std::fs::remove_dir_all(&cfg.research_log_dir);
        let mut eng = LiveEngine::new(
            noop_strategy(),
            AnyBroker::dry(FeeModel::default()),
            cfg.clone(),
            100_000.0,
            "1970-01-01".into(),
        );
        let signal = Signal::new(
            SignalKind::Other,
            Timestamp::from_millis(1_000),
            "OrderFlowExhaustion",
            serde_json::json!({
                "stage":"observation", "event_id":1, "side":"buy",
                "profile":"cumulative_absorption", "strength":"watch",
                "price":100.0, "zone":100.0,
                "observation":{"delta_share":-0.25,"return_pct":-0.001,"efficiency":0.2}
            }),
        );
        eng.track_new_shadow_signals(&[signal]);
        assert_eq!(eng.confirmed_signals_run, 0);
        assert_eq!(eng.observation_signals_run, 1);
        assert_eq!(eng.shadow_signals.len(), 1);
        eng.update_shadow_outcomes(
            Timestamp::from_millis(1_000 + 5 * 60_000),
            Price::from_f64(101.0),
        );
        let text = std::fs::read_to_string(cfg.research_log_dir.join("1970-01-01.jsonl"))
            .expect("observation research log");
        assert!(text.contains("\"event_type\":\"observation_detected\""));
        assert!(text.contains("\"profile\":\"cumulative_absorption\""));
        assert!(text.contains("\"features\":{\"delta_share\":-0.25"));
        let _ = std::fs::remove_dir_all(&cfg.research_log_dir);
    }
}
