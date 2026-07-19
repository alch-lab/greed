# TRDR 量化交易系统 · 开发手册

> **文档性质**：总开发手册。汇集交易方法论、量化规则、系统架构与 PR 实施拆解，作为后续逐行开发的对照蓝本。
> **版本**：v1.0 ｜ 日期：2026-07-19 ｜ 语言/技术栈：Rust（workspace 多 crate）
> **配套文档**（已交付，随本手册引用）：
> - 《TRDR 订单流交易方法体系》— 交易方法论原始整理（4 期视频 + 图文）
> - 《TRDR 量化策略设计书 v1.1》— 规则形式化（本手册第 2 章的母本）
> - 《PR-1 项目骨架完整代码》— 已实测通过的工程骨架

---

## 如何使用本手册

- **看方法论** → 第 1 章（策略哲学与体系总览），理解为什么这么做。
- **写规则代码** → 第 2 章（量化规则全集），每条规则都有公式、默认值、参数名、出处。
- **搭工程** → 第 3 章（系统架构）+ 第 4 章（数据层）。
- **排工期、领任务** → 第 5 章（PR 拆解），每个 PR 有目标/范围/验收/测试/依赖。
- **改参数** → 第 6 章（参数总表），全部落 TOML，校准只改配置不改代码。
- **验收与风控** → 第 7、8 章。

**总原则（贯穿全手册）**：
1. 信号皆参数——定性描述全部落为可调参数，默认值锚定教程，最终值由回测校准。
2. 反转体系先判趋势——所有反转开仓必须先过“非单边”过滤。
3. 止损锚定结构而非点数——止损是“证明自己错了的位置”的函数。
4. 回测与实盘同代码路径——信号/策略只消费统一事件流，不感知数据来源。
5. 规则插件化——策略 = 插件组合装配；新规则 = 新插件 + 改配置，不改核心引擎。

---

# 第 1 章　交易策略体系总览

## 1.1 体系定位：盘面雷达 + 反转扳机

本体系是一套 **BTC 永续合约的反转交易系统**，由“观察”与“执行”两层构成：

| 层 | 工具 | 回答的问题 | 角色 |
|---|---|---|---|
| 观察（雷达） | TRDR（trdr.io） | 在价格到达关键位置**之前**：市场强弱、资金方向、关键区域在哪 | **在哪做** |
| 执行（扳机） | Exocharts / 力竭反转模型 | 在价格到达关键位置**之后**：能不能入场、精确入场点 | **怎么进** |

TRDR 把全网订单簿、主动买卖、Delta、CVD、OI、巨鲸行为聚合到同一图表，用来判断当前是多头主动、空头主动、被动拉盘、诱多诱空还是易反转状态。**它不直接给买卖点**——色带给“位置”，总Delta给“力度”，OI给“性质”，力竭反转给“时机”，四者共振才入场。

## 1.2 四大核心概念（数据底座）

| 概念 | 定义 | 体系中的角色 |
|---|---|---|
| 订单簿不平衡 OBI | 同一百分比区间内买卖挂单的悬殊比例 | 色带模型输入，标注支撑/压力反应区 |
| Delta | 单周期主动买量 − 主动卖量（含平仓） | 总Delta基本单元；力竭反转的扳机变量 |
| CVD | Delta 的逐周期累加曲线 | 验证盘面方向、检测背离与重置 |
| OI 持仓量 | 未平仓合约总量 | 与 Delta、价格组四象限，判行情性质（主动/被动） |

挂单、成交、持仓三维度交叉验证，过滤大部分假象。

## 1.3 方法一：色带模型（订单簿不平衡）

**原理**：价格沿阻力最小方向运动。比较同一百分比区间（0–1% / 0–2.5% / 0–5%）内买卖挂单的比值，失衡越极端色带等级越高。

| 等级 | 失衡强度 | 含义与用法 |
|---|---|---|
| 蓝 | 极端（≈5.5×） | 最强，牛市历史胜率 80–90%，一年约 4–5 次；损在蓝信号下方 |
| 红 | 明显挂单墙（≈3×） | 止盈/反手主要依据，低流动性时段指引性强 |
| 黄 | 中度（≈2×） | 观察，不急于行动 |
| 绿 | 轻度（≈1.5×） | 弱，仅冷清时段参考 |

**关键经验**：近色带区（0–1%/0–2.5%）才可交易，远色带区（5%）只记录不追；空洞区（现价与真实挂单墙之间缺乏挂单）禁止中途开仓；同一信号美盘易被吃、亚盘/周末含金量高。

## 1.4 方法二：总Delta模板（全网最大子弹理论）

滚动累加全网主动买卖差 `AccDelta`，打到历史极值意味着这一侧“子弹”将尽、进入易反转状态。

| 档位 | 状态 | 动作 |
|---|---|---|
| ±1B | WATCH | 允许日内反转信号 |
| ±2B | ENTRY_OK | 可开仓（需确认） |
| ±3B | HIGH_Q | 高质量反转，仓位×1.5 |
| ±3.5B+ | EXTREME | 极值区，仓位×2（仍需确认） |

阈值随市值/流动性漂移（如 12 月假期降至 1–1.2B）。**多空不对称**用于识别趋势：空头连续大值多头弱反 → 下跌趋势；反之为上涨；出现反向极端值常是转折点。

## 1.5 OI 四象限（行情性质裁判）

| OI | Delta | 价格 | 解读 | 交易含义 |
|---|---|---|---|---|
| ↑ | 正 | 涨 | 多头主动开仓 | 趋势上行，可顺势持仓 |
| ↑ | 负 | 跌 | 空头主动开仓 | 空头主导，勿轻言抄底 |
| ↓ | 正 | 涨 | 逼空（空头平仓） | 持续性差，高位色带区易回落，只拿 TP1 |
| ↓ | 负 | 跌 | 杀多（多头平仓） | 杀尽常出阶段底，等反转信号 |

**OI 重置日**（集体平仓、指标归零）信号降权为观望。

## 1.6 入场模型：力竭反转（Exhaustion Reversal）

**本质**：反转入场。原理＝**努力（主动量）很大，但结果（价格）推不动 → 努力被吸收 → 买卖互换反转**。教程口径反转概率 ≥80%（指出现反转，不含盈亏比）。

**K 线引擎**：价格驱动反转砖（Exocharts `Trend Reversal 200-124`）：顺向满 100 美元收新砖，逆向回撤 62 美元出反转砖（成影线）。每砖记录 duration / volume / Delta / Delta% / NetLong / NetShort / footprint。

**三条件**（以做多为例，做空镜像）：
1. **放量快速**（必要）：砖速率 ≫ 基线，且 duration 很短（快速插针）。
2. **力竭不推进**（确认）：小实体/长影线，或 P 型 footprint（量集中在砖末端但价格不新高）；**加分项**＝针尖有被套单（NetLong/NetShort 大），反转后平仓助推。
3. **Delta 反转**（扳机）：前砖 Delta% 与趋势同向，当前砖 Delta% 翻向且足够大。激进入场＝刚翻即进（损近）；保守入场＝等收线（损远 1–2 倍）。

**止损**：针尖下方 100–200 美元；有挂单墙则放墙外。**保本**：离开 300 美元推保本（教程统计 90% 不亏，代价约 1/3 被二探打掉）。**单边处理**：趋势市不做反转；例外只在“放大量”力竭处轻仓做 TP1(300–500)+保本。

## 1.7 协同 SOP 与失效场景

**标准作业流程**：环境过滤（事件/单边？→管住手）→ 色带定位（近区？空洞？）→ 挂单核对 → Delta 共振（到档位？）→ OI 性质（主动/被动？）→ 入场触发（力竭反转/网格）→ 风控止盈（损锚、TP、保本）。

**失效场景（必须承认并管住手）**：未知来源的不计价抛盘/买盘、宏观黑天鹅、新高/新低无人区。单边占约 20% 行情，是反转体系的坟场；单边里亏的小钱，在随后 80% 震荡里能赚回来——前提是单边时没把本金和心态亏掉。

---

# 第 2 章　量化规则全集（可计算化）

> 本章是开发的**规则母本**。每条规则给出：名称 / 公式 / 参数默认值 / 对应代码位置 / 出处。
> 参数名与 `config/strategy.toml` 一一对应。

## 2.1 可量化性审计（动手前的边界确认）

| 环节 | 可量化性 | 处理 |
|---|---|---|
| 色带 OBI | 完全可量化 | 多区间 OBI 公式，阈值参数化 |
| 总Delta阈值 | 完全可量化 | 滚动累加窗口 + 档位参数化 |
| OI 四象限 | 完全可量化 | ΔOI×Delta×收益符号判定 |
| 趋势识别 | 可量化 | 多空极值比 → 状态机 |
| 近/远色带、空洞区 | 可量化 | 挂单墙加权距离 + 区间密度 |
| 时段过滤 | 可量化 | UTC 时段映射 + 系数 |
| 宏观事件 | 半量化 | 人工事件日历 CSV，窗口内禁开仓 |
| 力竭反转 | 可量化 | 反转砖 + 三条件公式化；AGGR 听觉用大单流速率替代 |
| 盘面感觉 / spoofing 识别 | 放弃 | 不进策略，作为已知噪音 |

## 2.2 色带模块（OBI）

**定义**（时刻 t，中间价 m，区间档位 b∈{1%,2.5%,5%}）：
```
BidQty_b = Σ 买单名义量(价 ∈ [m·(1−b), m))
AskQty_b = Σ 卖单名义量(价 ∈ (m, m·(1+b)])
OBI_b    = BidQty_b / AskQty_b   （买方厚度倍数）
AOI_b    = AskQty_b / BidQty_b   （卖方厚度倍数）
```

**分级**（参数默认，待校准）：
| 等级 | 触发 | 语义 |
|---|---|---|
| 蓝 BLUE | `OBI_b ≥ θ_blue`(5.5) 且近区间同源 | 极端买盘墙 |
| 红 RED | `OBI_b ≥ θ_red`(3.0) 或 `AOI_b ≥ θ_red` | 明显挂单墙 |
| 黄 YELLOW | `≥ θ_yellow`(2.0) | 观察 |
| 绿 GREEN | `≥ θ_green`(1.5) | 弱，冷清时段才计入 |

**近/远色带区**：`d_wall = Σ(q_i·|p_i−m|)/Σ(q_i)/m`（挂单墙量加权距离，%）。
`d_wall ≤ d_near`(1.5%) → 近区可交易；否则远区只记录，等价格走近激活。

**空洞区**：`density_gap = Σ(区间挂单)/Σ(墙档±0.3%挂单) < ρ_gap`(0.25) → 禁中途开仓。

**挂单墙定位**：信号区间内局部峰值检测 → `Walls=[(price,side,qty,band)]`，供止损锚定与网格布点。

> 代码：`signals/src/obi.rs`（Phase 2，依赖订单簿历史，待采集）。

## 2.3 总Delta模块

**滚动累加**（30m 聚合 Delta 为单元，窗口 W=24h，校准 4–48h）：
```
AccDelta(t) = Σ_{i∈[t−W,t]} AggDelta_30m(i)
```
**档位**（USD，随市值漂移）：`t1=1B / t2=2B / t3=3B / t4=3.5B`
- `|AccDelta|≥t1` → WATCH；`≥t2` → ENTRY_OK；`≥t3` → HIGH_Q(仓位×1.5)；`≥t4` → EXTREME(×2)。

**趋势状态机**（回看 N=14 天，取多空侧日度极值）：
```
ratio = maxAcc_short / max(maxAcc_long, ε)
ratio > r_trend(2.0) → TREND_DOWN（禁做多反转）
ratio < 1/r_trend    → TREND_UP（禁做空反转）
其余                  → RANGE（正常）
```
**连续击穿**：同向 AccDelta 连续 K(3) 次破 t2 而价格未反转 r_rev(1.5%) → 判单边，熔断。

> 代码：`signals/src/agg_delta.rs`（PR-9）。

## 2.4 OI 四象限与重置

30m 柱 `ΔOI = OI(t)−OI(t−1)`（±0.5% 噪音带）：
| 条件 | 象限 | 动作 |
|---|---|---|
| ΔOI>0,Δ>0,r>0 | 多头开仓 | 反转多单可持至 TP2 |
| ΔOI>0,Δ<0,r<0 | 空头开仓 | 多单降级 |
| ΔOI<0,Δ>0,r>0 | 逼空 | 多单只拿 TP1，不隔夜 |
| ΔOI<0,Δ<0,r<0 | 杀多 | 空单只拿 TP1；杀尽+色带共振→反转候选 |

**重置检测**：`ΔOI < −oi_reset_pct`(3%)/30m 或 CVD 日内重置 → 当日观望。

> 代码：`signals/src/oi_regime.rs`（PR-9）。

## 2.5 力竭反转（入场扳机）

**砖引擎**：`tick=0.5`；同向 `t_ren_ticks=200`(100 美元)收新砖，逆向 `r_ren_ticks=124`(62 美元)出反转砖。每砖聚合 volume/delta/duration/footprint/net_long/net_short。

**三条件**（做多，做空镜像）：
```
条件1 放量快速(必要)：
  V_now  = brick.volume / brick.duration          # USD/s
  V_base = 过去 lookback_exh(2h) 剔除大砖的典型速率（按时段系数缩放）
  触发   V_now ≥ vol_mult_exh(3.0) × V_base  且 duration ≤ dur_max_ms(30s)

条件2 力竭不推进(确认)：
  |close−open|/range ≤ prog_ratio(0.4)   # 小实体/长影线
  或 P 型 footprint（量集中砖末端但不新高）
  加分：NetLong/NetShort ≥ trapped_min_btc(50)   # 针尖被套（非必要）

条件3 Delta反转(扳机)：
  前砖 delta% 与趋势同向，当前砖 delta% 翻向 且 |delta%| ≥ delta_flip_pct(8)
  激进：翻即进；保守：等收线（损扩大1–2倍）
```

**AGGR 替代**：`large_trade_rate = 过去 window_secs(10s) 内 ≥ size_large_usd($200k) 的笔数/秒`，速率 ≥ surge_mult(5)× 基线 → FlowSurge 预警。

**位置约束**：只在色带区/Delta 阈值区（Zone）内触发，区外信号忽略。

**止损/保本/止盈**：
```
止损：针尖下方 sl_min–sl_max(100–200) 美元；有墙放墙外；等确认则扩1–2倍
保本：离开 BE_trigger(300) 美元推保本（方案A默认；方案B等TP1再推）
止盈：RANGE→TP1=首反向色带(≥黄)平50%推保本，TP2=次强色带/AccDelta归零
      大周期共振(蓝带/EXTREME)→TP拉长至日内VWAP或反向色带
      单边/疑似单边→TP收紧300–500美元即TP1推保本
```

**单边例外开关** `switch_trend_exh`（默认关）：单边中只在“放大量”(vol_mult×1.5)力竭处轻仓做 TP1+保本。

> 代码：`signals/src/renko.rs`（PR-6）、`strategy/src/triggers.rs` 内 ExhaustionReversal（PR-8）、`signals/src/exhaustion.rs` + LargeTradeFlow。

## 2.6 时段与事件过滤

| 过滤项 | 规则（UTC） |
|---|---|
| 亚盘 00:00–07:00 | 信号系数 1.0（绿/黄计入） |
| 欧盘 07:00–13:00 | 系数 1.0 |
| 美盘 13:00–21:00 | 系数 0.8（红/蓝需同时 ENTRY_OK） |
| 周末 | 允许色带独立交易，仓位×0.5，只做近色带区 |
| 宏观事件 | 事件日历 CSV，前后 evt_buf(±2h) 禁开仓 |
| 熔断 | 当日止损满 2 次 → 当日停止开仓 |

> 代码：`strategy/src/filters.rs`（PR-10）。

## 2.7 交易执行规则

**开仓**：
| 模式 | 触发 | 仓位 | 止损锚 |
|---|---|---|---|
| A 力竭反转单 | 观察区内三条件齐备 + 环境过滤 + Delta≥WATCH | 基础×共振系数(1/1.5/2/3) | 针尖下 100–200 美元/墙外 |
| B 网格单 | 密集色带区(≥2相邻墙,间距≤0.5%)+RANGE | 拆 n_grid(5)份，每份0.2×基础 | 最外墙外 buf_wall(0.5%) |
| C 蓝带市价单(仅RANGE) | 蓝色+近区+AccDelta同向≥t1 | 基础×2 | 蓝区下沿外 buf_blue(0.3%) |

**持仓管理**：保本(BE_trigger) → TP1(反向色带≥黄平50%推保本) → TP2(次强色带/AccDelta归零/VWAP) → 降级(被动行情只TP1) → 时间止损(t_max=36h，力竭日内单收紧4h) → 反手(到反向色带+反向力竭信号)。

**仓位风控**：基础仓位 = 权益 × risk_pct(0.75%) / 止损距离；单笔风险 ≤1.5%；日回撤 3% 或连损 2 笔熔断；总杠杆 ≤3×；同向最多一笔 + 一组网格。

---

# 第 3 章　系统架构（Rust）

## 3.1 Workspace 结构

```
trader/
├── crates/
│   ├── core/        领域模型 + 事件流 + 插件契约
│   ├── data/        数据层：历史导入 + Parquet 数据湖 + 实时采集
│   ├── signals/     信号引擎（纯函数，可独立测试）
│   ├── strategy/    策略编排：插件装配 + 过滤 + 扳机 + 出场 + 风控
│   ├── backtest/    事件驱动回测引擎 + 绩效报告
│   ├── execution/   实盘执行（Phase 4+）
│   └── cli/         命令行：ingest / collect / backtest / validate
├── config/          TOML 参数（base.toml + strategy.toml）
└── data/            Parquet 分片（本地数据湖）
```

## 3.2 技术选型

| 关注点 | 选型 | 理由 |
|---|---|---|
| 异步运行时 | tokio | WS/IO 密集 |
| WebSocket | tokio-tungstenite | 三所 WS 流 |
| HTTP | reqwest(rustls) | REST 快照、OI 历史 |
| 序列化 | serde / serde_json | 交易所消息 |
| 存储 | parquet(arrow2/polars) 按天分片 | 列存、压缩、零拷贝回放 |
| 数值精度 | 定点 i64（satoshis 级） | 杜绝浮点误差 |
| 并发 | 事件驱动单线程循环 + crossbeam 通道 | 回测确定性；采集并行 |
| 日志 | tracing + tracing-subscriber(JSON) | 实盘可观测 |
| 配置 | config crate + TOML | 参数热加载 |
| 测试 | proptest + 黄金样本快照测试 | 信号回归 |

## 3.3 核心数据流（事件驱动）

```
交易所 WS / 历史Parquet
   │ data::ingest
   ▼ core::Event (Trade | BookSnapshot | OiTick | Timer)
signals::engine（纯函数：renko / obi / agg_delta / oi / exhaustion）
   ▼ Signal
strategy::fsm（FLAT/WATCHING/ENTERED_*/HALTED/DISABLED，风控前置）
   ▼ core::OrderIntent
   ┌────┴────┐
backtest   execution（实盘）
```

**关键**：`signals` 与 `strategy` 只依赖 `core` 的事件流，不关心数据来自历史还是实时——这是避免“回测好、实盘废”的根本手段。

## 3.4 插件化架构（策略 = 插件组合装配）

四大插件类别（`core::plugin` 已立契约）：

```rust
pub trait SignalPlugin: Send {   // 信号源：从事件流产出观察信号，不下单
    fn name(&self) -> &'static str;
    fn on_event(&mut self, ev: &Event, ctx: &Ctx) -> Vec<Signal>;
}
pub trait FilterPlugin: Send {   // 过滤器：一票否决或降权
    fn name(&self) -> &'static str;
    fn check(&self, intent: &OrderIntent, ctx: &Ctx) -> Verdict; // Allow/Scale(f64)/Veto(&str)
}
pub trait TriggerPlugin: Send {  // 扳机：信号+过滤满足后是否扣扳机
    fn name(&self) -> &'static str;
    fn should_fire(&self, signals: &[Signal], ctx: &Ctx) -> Option<OrderIntent>;
}
pub trait ExitPlugin: Send {     // 出场：保本/止盈/反手/时间止损
    fn name(&self) -> &'static str;
    fn manage(&self, pos: &Position, ctx: &Ctx) -> Vec<ExitAction>;
}
```

**内置插件清单**：
| 类别 | 插件 | 规则 |
|---|---|---|
| Signal | ObiZones(Phase2) / AggDeltaTier / OiQuadrant / LargeTradeFlow | §2.2–2.5 |
| Filter | TrendRegimeFilter / SessionFilter / EventCalendarFilter / CircuitBreaker / InZoneFilter(Phase2) | §2.3/2.6 |
| Trigger | ExhaustionReversal / BlueBandMarket / GridCluster | §2.5/2.7 |
| Exit | BreakevenAt300 / TieredTakeProfit / DegradeOnPassive / TimeStop / ReverseOnSignal | §2.7 |

**TOML 装配**（`config/strategy.toml`）：
```toml
[strategy]
signals = ["AggDeltaTier","OiQuadrant","LargeTradeFlow"]
filters = ["TrendRegimeFilter","SessionFilter","EventCalendarFilter","CircuitBreaker"]
trigger = "ExhaustionReversal"
exits   = ["BreakevenAt300","TieredTakeProfit","TimeStop"]
```

**插件组合测试（sweep）**：同一份数据批量跑不同插件/参数组合，输出绩效矩阵，定位每条规则的边际贡献；Discord/kiyotaka 新规则写成插件后与基线做受控 A/B 对比。

## 3.5 策略状态机与伪代码

```
状态：FLAT / WATCHING / ENTERED_A / ENTERED_GRID / HALTED(日) / DISABLED(单边)

每 30m 收盘 + 每事件驱动：
1. 更新 AccDelta、OI 象限、趋势状态机 → TREND_UP/DOWN 挂起反向反转
2. 事件窗口或熔断 → HALTED，仅管理持仓
3. 计算 OBI 信号 → Walls / 近远区 / 空洞区
4. RANGE 且非事件：
   a. 蓝带+近区+Delta共振 → 开仓C
   b. 近色带区+力竭反转 → 开仓A
   c. 密集色带区+无持仓 → 布网格B
5. 持仓管理：保本/TP1/TP2/降级/时间止损/反手
6. 记录全部信号与决策（供复盘校准）
```

---

# 第 4 章　数据层

## 4.1 数据源与能力边界

| 数据 | 用途 | 主源 | 历史可得性 |
|---|---|---|---|
| 逐笔成交(含 maker) | Delta/CVD/力竭反转/量价 | Binance aggTrades 官方 dump（现货+合约） | **全历史免费**；Bybit/OKX 近期 REST，长历史需自采 |
| 订单簿快照 | 色带 OBI | Binance `depth`≤1000档(实时)；Bybit≤1000档(WS)；OKX≤400档 | **无免费历史** → 自采集（默认）或 Tardis 付费 |
| 持仓量 OI | 四象限/重置 | Bybit/OKX 官方历史；Binance 近期 + Tardis | Bybit/OKX 有官方历史 |
| K 线 OHLCV | 结构/入场近似 | 各所公共 dump | 全历史免费 |

**结论**：总Delta、CVD、OI、K线、**力竭反转**今天就能免费回溯多年（力竭反转只依赖逐笔成交）；**色带是唯一硬约束**——需即日起挂采集器积累 2–3 个月，或付费 Tardis 加速。

## 4.2 采集规格（自采集方案）

```
orderbook_snapshots  每 5s：ts, exchange, symbol, bids[[p,q]..], asks[[p,q]..]
trades               全量：ts, exchange, symbol, price, qty, is_buyer_maker
oi                   每 60s：ts, exchange, symbol, oi_usd
klines               每根收盘：1m/5m/15m/30m/1h
```
- 合约统一 USDT 永续 BTCUSDT；另采 Binance 现货（教程强调现货买盘）。
- 订单簿只保留距中间价 ±6% 档位（覆盖 5% 色带 + 余量）。
- 按天分片 Parquet；WS 断线重连 + 序列号校验（Binance `pu/u`、Bybit `u=1` 重快照）。

## 4.3 跨所聚合规则

- **聚合Delta**：三所（合约+Binance现货）同 30m 柱 `Σ(taker_buy−taker_sell)`（USD）。Binance `isBuyerMaker=true` 记卖方主动。
- **聚合OI**：三所合约 OI(USD) 求和；Binance 缺历史段按 Bybit+OKX 乘校准系数 `k_oi`(默认2.0)。
- **聚合订单簿**：同快照时刻按价格合并求和（USD 名义）；各所时间差 ≤10s，超出则该时刻聚合簿标记无效。
- 默认等权求和；若校准发现某所代表性不足，引入权重向量 `w_e`。

---

# 第 5 章　PR 实施拆解（开发路线图）

## 5.0 依赖关系总览

```
PR-1 (骨架 ✅已交付)
  └─ PR-2 (core领域模型)
        ├─ PR-3 (数据层: aggTrades导入)
        │     └─ PR-5 (回测引擎骨架)
        │           └─ PR-7 (绩效报告)
        ├─ PR-4 (插件框架+配置装配)  ← 关键路径
        │     └─ PR-6 (renko砖引擎)
        │           └─ PR-8 (力竭反转扳机)
        │                 └─ PR-9 (AggDelta+OI信号)
        │                       └─ PR-10 (基础过滤+CLI端到端)
        └─ PR-11 (采集器daemon)  ← 并行，不阻塞回测
```
关键路径：`1→2→4→6→8→9→10`（力竭反转端到端）；PR-11 采集器与主线并行、尽早上线跑数据。

## 5.1 PR 明细

### PR-1：Workspace 骨架与工程基建　✅ 已交付并通过验证
- **范围**：workspace（7 crate 占位）、rust-toolchain、CI（fmt+clippy+test）、config 骨架、README、CLI 子命令骨架。
- **验收**：`cargo build` / `cargo test`(7 passed) / `cargo clippy -D warnings` / `cargo fmt --check` 全过。
- **产物**：见《PR-1 项目骨架完整代码》。

### PR-2：core 领域模型与事件流
- **范围**：`types.rs`（Symbol/Price/Qty/Side/Timestamp，**价格数量改定点 i64**）、`event.rs`（Trade/BookSnapshot/OiTick/Event）、`clock.rs`（EventClock/SystemClock）。
- **验收**：类型转换正确，定点算术无浮点；事件可序列化往返。
- **测试**：定点价格 proptest；事件 ts 提取。
- **依赖**：PR-1。规模 ~400 行。

### PR-3：数据层 — Binance aggTrades 历史导入器
- **范围**：`data/src/binance_dump.rs`（下载/解压官方月度日度 CSV）、`normalize.rs`（CSV→Trade，isBuyerMaker→主动方向）、`lake.rs`（按天分片 Parquet）、`loader.rs`（按时间范围流式读取）。
- **验收**：`cli ingest --month 2024-01` 落盘；loader 按时间升序回放，笔数与官方 CSV 一致。
- **测试**：样本 CSV 解析（含 maker 方向）；分片边界。
- **依赖**：PR-2。规模 ~600 行。

### PR-4：插件框架 + TOML 装配　★核心
- **范围**：`core/plugin.rs` 完善（Signal/OrderIntent/Ctx）、`strategy/registry.rs`（PluginRegistry）、`strategy/assemble.rs`（从 TOML 装配 Strategy）。
- **验收**：strategy.toml 能装配出策略对象；未知插件名报错清晰。
- **测试**：装配成功/失败；Verdict 三态语义。
- **依赖**：PR-2。规模 ~500 行。

### PR-5：回测引擎骨架（事件驱动）
- **范围**：`engine.rs`（多路事件按时间归并）、`broker.rs`（模拟撮合：市价吃 spread、限价按价位穿越）、`account.rs`（权益/保证金/仓位记账）、`fees.rs`（taker/maker + 滑点）。
- **验收**：空策略跑 1 个月 < 30s；记账守恒。
- **测试**：撮合规则、手续费。
- **依赖**：PR-3、PR-4。规模 ~700 行。

### PR-6：renko 反转砖引擎　★业务地基
- **范围**：`signals/renko.rs`：逐笔消费 Trade，按 100/62 美元出砖；聚合 volume/delta/duration/footprint/net；实现 SignalPlugin 输出 BrickClosed。
- **验收**：固定 trade 序列出砖与手工演算一致（用教程 100/62/62/100 例）；footprint 守恒。
- **测试**：砖生成黄金样本（含反转砖、影线）；footprint proptest。
- **依赖**：PR-4。规模 ~500 行。

### PR-7：绩效报告
- **范围**：`backtest/report.rs`：胜率/盈亏比/expectancy/最大回撤/Sharpe/Sortino、分时段统计、权益曲线；输出 markdown+JSON+PNG(plotters)。
- **验收**：手工构造成交序列，指标与独立演算一致。
- **测试**：指标计算（空/单笔/连败边界）。
- **依赖**：PR-5。规模 ~400 行。

### PR-8：力竭反转扳机　★业务核心
- **范围**：`signals/large_trade_flow.rs`（大单速率 SignalPlugin → FlowSurge）；`strategy/triggers/exhaustion.rs`（三条件 TriggerPlugin）；基线估计器（剔除大砖 + 时段系数）。
- **验收**：视频教科书案例（插70k/圆底/推土机末尾）做黄金样本，断言预期砖触发；横盘慢速不触发。
- **测试**：三条件组合开/关；时段系数影响。
- **依赖**：PR-6。规模 ~600 行。

### PR-9：AggDelta 阈值 + OI 象限信号
- **范围**：`signals/agg_delta.rs`（30m聚合+AccDelta+档位+趋势状态机）、`signals/oi_regime.rs`（四象限+重置）。OI 历史用 Bybit/OKX REST 落湖。
- **验收**：合成序列下档位切换与状态机迁移正确。
- **测试**：阈值穿越、趋势比临界、重置检测。
- **依赖**：PR-4。规模 ~500 行。

### PR-10：基础过滤插件 + CLI 端到端
- **范围**：`strategy/filters/`（TrendRegime/Session/EventCalendar/CircuitBreaker）；出场插件（BreakevenAt300/TimeStop/TieredTakeProfit）；`cli backtest` 全参数接入。
- **验收**：`cli backtest --strategy config/strategy.toml --from ... --to ...` 端到端跑通出报告。
- **测试**：每个过滤器独立用例；熔断状态机。
- **依赖**：PR-5、8、9、7。规模 ~600 行。

### PR-11：三所实时采集器 daemon（并行，不阻塞回测）
- **范围**：`data/live/`：三所 WS 订阅（trades+depth快照5s+OI 60s）、连接管理（断线重连+序列号gap检测+重快照）、写 Parquet、指标+日志。
- **验收**：连续 72h 无数据空洞；落盘 schema 与 lake 一致可读。
- **测试**：mock WS server 重放断线/gap。
- **依赖**：PR-2、PR-3。规模 ~800 行。**与 PR-4~10 并行评审。**

## 5.2 合并顺序建议

| 批次 | PR | 说明 |
|---|---|---|
| Batch 1 | PR-1 ✅, PR-2 | 地基 |
| Batch 2 | PR-3, PR-4, **PR-11**（并行启动采集） | 数据+插件框架+**采集器尽早上线** |
| Batch 3 | PR-5, PR-6 | 回测骨架+砖引擎 |
| Batch 4 | PR-7, PR-8, PR-9 | 报告+力竭反转+Delta/OI |
| Batch 5 | PR-10 | 端到端，出第一版回测曲线 |

> PR-11 采集器在 Batch 2 就上线跑（色带数据在积累），主线回测开发不受影响。PR-10 合并时即得「力竭反转 2023–2026 回测报告」，此时色带数据已积累数周。

---

# 第 6 章　参数总表（对应 `config/strategy.toml`）

> 全部参数进 TOML，改动走 git 历史；校准只改配置不改代码；样本外验证强制。

| 参数 | 默认 | 校准范围 | 来源 / 说明 |
|---|---|---|---|
| θ_blue / θ_red / θ_yellow / θ_green | 5.5 / 3.0 / 2.0 / 1.5 | 4–8 / 2–4 / 1.5–3 / 1.2–2 | 教程仅蓝带有锚点(≈5.5×)，其余工程估计**必须校准** |
| d_near（近色带阈值） | 1.5% | 0.8–2.5% | 教程“0–1%/0–2.5% 区间” |
| ρ_gap（空洞区） | 0.25 | 0.1–0.5 | 工程估计 |
| t1–t4（Delta 阈值） | 1/2/3/3.5（B USD） | 随市值季度重估 | 教程明确 |
| W（Delta 累加窗口） | 24h | 4–48h | 教程未明示，需校准 |
| r_trend（趋势比） | 2.0 | 1.5–3.0 | 教程定性 |
| **t_ren / r_ren（砖参数）** | 100 / 62 美元（200/124 tick） | BTC 永续固定 | 教程 Exocharts `Trend Reversal 200-124` |
| **vol_mult_exh（放量倍数）** | 3.0（×时段系数） | 2–5，按时段校准 | 教程“相对过去半小时到几小时放大量” |
| **dur_max_ms（快速上限）** | 30000（30s/砖） | 10–60s | 教程快慢对照 |
| **prog_ratio（不推进比）** | 0.4 | 0.3–0.6 | 工程估计 |
| **delta_flip_pct（Delta反转）** | 8% | 5–15% | 教程案例 1–37%，取保守下沿 |
| **trapped_min_btc（针尖被套）** | 50 | 20–100 | 教程案例 350/357/380，取宽松下沿（加分项） |
| **sl_min / sl_max（止损）** | 100 / 200 美元 | 固定 | 教程明确 |
| **BE_trigger（保本触发）** | 300 美元 | 200–500 | 教程“离开300点推保本，90%不亏；1/3二探打掉” |
| size_large_usd / window_secs / surge_mult | 200k / 10s / 5× | 校准 | AGGR 大单速率替代 |
| risk_pct / 日熔断 | 0.75% / 2次·3% | 0.5–1% | 教程“0.1 仓位、损两手停” |
| buf_tick / buf_wall / buf_blue | 0.15% / 0.5% / 0.3% | ±50% | 工程估计 |

---

# 第 7 章　回测与校准计划

1. **第一阶段（现在即可做，含入场模型）**：基于 Binance 公共数据 dump（aggTrades 全历史）回溯 2023–2026。**力竭反转可直接回测**（只依赖逐笔成交），同步验证总Delta档位反转统计、趋势状态机区分度、OI 重置日表现。
2. **第二阶段（采集期）**：启动三所订单簿快照采集；用 Tardis 免费样本校验 OBI 与图 1–6 已知色带日对齐。
3. **第三阶段**：数据满 2–3 个月后做色带+力竭反转+全套规则 walk-forward 回测（70/30 切分）。重点：**力竭反转反转概率是否复现 ≥80%**、蓝带胜率（牛市 80–90%）、红带止盈命中、网格组穿越最大回撤。
4. **第四阶段**：模拟盘前向 ≥1 个月，对比回测滑点与信号延迟（力竭反转对执行延迟敏感，需实测砖内入场 vs 砖确认入场滑点差）。
5. **过拟合控制**：默认值锚定教程、校准只做单调微调；任何参数调整需样本外验证；保留参数变更日志。

---

# 第 8 章　风控红线与已知局限

## 8.1 风控红线（硬编码于 strategy 层，不可被配置覆盖）

- 基础仓位 = 权益 × risk_pct / 止损距离（固定风险百分比）。
- 单笔最大风险 ≤ 1.5%（共振放大仍受限）；网格整组视作一笔。
- 日度回撤 3% 或连损 2 笔 → 熔断至次日 UTC 00:00。
- 总杠杆 ≤ 3×；同向最多一笔持仓 + 一组网格。
- 单边识别 → 挂起反向反转开仓（DISABLED）。

## 8.2 已知局限（诚实清单）

- **色带颜色是 TRDR 专有算法**，阈值与合成不可完全复制，只能逼近；蓝带 5.5× 是唯一锚点。
- **订单簿历史缺失**：免费世界无三所历史 L2，色带回测最早 2–3 个月后；Tardis 可买历史但成本高。
- **挂单可撤**：快照看不到撤单意图，假墙（spoofing）造成假信号，只能靠墙存续时长过滤（参数 `wall_min_age` 后续加）。
- **AGGR 听觉通道不可替代**：人耳对节奏的模式识别含主观成分，大单速率预警是近似。
- **砖参数依赖品种与周期**：200-124 是 BTC 永续调教值；vol_mult_exh 时段基线估计是校准重点。
- **宏观事件**依赖人工日历，无法全自动。
- **胜率口径**（蓝带 80–90%、力竭反转 ≥80%）来自主观复盘，样本小且含幸存者偏差，量化复现应以统计区间看待。

---

# 附录 A　文件与模块索引

| 规则 / 功能 | 代码位置 | 所属 PR |
|---|---|---|
| 定点类型 / 事件 / 时钟 | `core/src/types.rs` `event.rs` `clock.rs` | PR-2 |
| 插件 trait 契约 | `core/src/plugin.rs` | PR-4 |
| 历史数据导入 | `data/src/binance_dump.rs` `lake.rs` `loader.rs` | PR-3 |
| 实时采集 daemon | `data/src/live.rs` | PR-11 |
| renko 反转砖 | `signals/src/renko.rs` | PR-6 |
| 力竭反转三条件 | `signals/src/exhaustion.rs` + `strategy/src/triggers.rs` | PR-8 |
| 大单速率（AGGR 替代） | `signals/src/large_trade_flow.rs` | PR-8 |
| 色带 OBI | `signals/src/obi.rs` | Phase 2 |
| 总Delta阈值 / 趋势 | `signals/src/agg_delta.rs` | PR-9 |
| OI 四象限 | `signals/src/oi_regime.rs` | PR-9 |
| 过滤插件 | `strategy/src/filters.rs` | PR-10 |
| 出场插件 | `strategy/src/exits.rs` | PR-10 |
| 策略状态机 | `strategy/src/fsm.rs` | Phase 3 |
| 回测引擎 / 撮合 / 记账 | `backtest/src/engine.rs` `broker.rs` `account.rs` | PR-5 |
| 绩效报告 | `backtest/src/report.rs` | PR-7 |

# 附录 B　常用命令

```bash
cargo build --workspace                 # 编译
cargo test  --workspace                 # 测试
cargo clippy --workspace --all-targets -- -D warnings   # 静态检查
cargo fmt --all -- --check              # 格式检查

# 子命令（随 PR 启用）
cargo run -p cli -- ingest  --config config/base.toml    # PR-3 导入历史
cargo run -p cli -- collect --config config/base.toml    # PR-11 采集（常驻）
cargo run -p cli -- backtest --config config/base.toml   # PR-10 回测
# 插件/参数扫描（Phase 3）
# cli sweep --data 2023-2026 --grid 'trigger=[ExhaustionReversal,BlueBandMarket]' ...
```

---

*免责声明：本手册为方法论工程化实现文档，仅供学习研究，不构成投资建议。加密衍生品交易风险极高，回测与模拟验证通过前勿投入实盘资金。*
