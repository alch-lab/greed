# greed

BTCUSDT 永续合约波段/短线量化系统（Rust workspace）。订单流数据采集 → 本地数据湖 → 回测引擎 → 决策流水（Journal）→ 前端监控。

定稿策略：**5m/1h 双时间框架均值回归信号 + 金字塔加仓**，双期验证年化约 13-15%、回撤 ~1.5%（详见 `docs/STRATEGY_DOC.md` 第 9.6 节）。

## 目录结构

```
crates/        Rust workspace
  core/        基础类型与插件接口
  data/        数据湖（lake）、Binance 历史导入、实时采集 daemon
  signals/     renko 砖等信号研究工具
  strategy/    策略装配（信号/扳机/过滤器/出场插件 + TOML 注册表）
  backtest/    回测引擎（账户/撮合/手续费/统计/报告/决策流水）
  live/        模拟盘/实盘执行（行情 WS、签名 REST、经纪层、LiveEngine）
  cli/         greed 二进制入口
config/
  base.toml              采集配置 + 模拟盘账户 [account]
  strategy-final.toml    定稿策略（唯一生产配置）
  strategy-dual-tf.toml  v1 基线（金字塔加仓前的对照，仅研究用）
scripts/       月度批跑/采集/汇总脚本
docs/          策略文档与研究阶段总结
data/lake/     本地数据湖（trades / OI / 资金费率）
out/           回测输出（不入库）
```

## 构建与测试

```bash
cargo build --release   # 产出 target/release/greed
cargo test --workspace
```

## 数据采集

```bash
# 历史 aggTrades 导入数据湖
./target/release/greed ingest --market perp --from 2025-01-01 --to 2025-12-31 --lake data/lake

# 实时采集 daemon（读 config/base.toml [collector]）
./target/release/greed collect --config config/base.toml
./target/release/greed collect --config config/base.toml --dry-run   # 试运行不落盘
```

## 回测（最终配置）

```bash
./target/release/greed backtest \
  --market perp --from 2026-01-01 --to 2026-06-30 --lake data/lake \
  --strategy config/strategy-final.toml \
  --cash 100000 --risk-pct 0.015 --max-risk-pct 0.015 --trials 1 \
  --out out/final-2026h1 \
  --journal data/journal/2026h1.json    # 可选：导出决策流水供前端
```

批量跑整年：`scripts/run_year.sh strategy-final 2026 1 6`（4 路并行，月粒度）。

## 决策流水（Journal）→ 前端监控

`--journal <path>` 导出 JSON：`meta`（区间/初始资金/策略）、`intents`（每次下单的时间/方向/数量/限价/止损/止盈/**原因**）、`fills`（实际成交价/数量/费用/已实现盈亏）、`equity_curve`（逐日权益）。

前端仓库：[greed-web](../greed-web)（`/Users/wonder/Code/greed-web`），把导出的 journal JSON 放到其 `public/journal/` 即可在看板中查看下单明细、日盈亏曲线、月盈亏、账户余额。

## 模拟盘执行（greed trade）

在 <https://testnet.binancefuture.com> 申请币安合约 testnet 密钥，写入环境变量（不要写进配置文件）：

```bash
export BINANCE_API_KEY="..."
export BINANCE_API_SECRET="..."
```

```bash
# 干跑（不需要密钥）：真实行情 + 模拟撮合，验证全链路
./target/release/greed trade --strategy config/strategy-final.toml --dry-run \
  --journal data/journal/live.json --risk-pct 0.015 --max-risk-pct 0.015

# testnet 模拟盘：真实下单（[account] testnet = true）
./target/release/greed trade --strategy config/strategy-final.toml \
  --journal data/journal/live.json --risk-pct 0.015 --max-risk-pct 0.015
```

工作机制：

- **决策与回测同一代码路径**：信号 → 扳机 → 过滤器 → 仓位计算（equity × risk_pct / 止损距离）→ 出场插件，全部复用回测引擎逻辑。
- **启动安全检查**：必须空仓启动（不接管外部持仓）；自动清遗留挂单、设杠杆（默认 3x 逐仓）、读交易对精度约束（tick/step/最小名义价值）。
- **成交对账**：testnet 成交经 `userTrades` 每 2s 轮询确认，取真实成交价/手续费/maker 标记；每小时与钱包余额对账，漂移告警。
- **Journal 原子落盘**：每次意图/成交/权益采样后重写 journal JSON（tmp + rename），前端随时读到最新状态；契约与回测导出完全一致，greed-web 直接可用。
- **优雅退出**：SIGINT 只落盘退出，**不撤交易所挂单**（止损单是持仓保护）。
- 行情 WS 断线指数退避重连 + 90s 假死看门狗；`--ws-base` 可覆盖行情源（如 dry-run 用主网更稠密的行情）。

`config/base.toml` 的 `[account]` 段按 `testnet = true/false` 自动选择 testnet/实盘端点，密钥只从 `api_key_env` / `api_secret_env` 指定的环境变量读取。

> ⚠️ 上实盘（testnet = false）前，请先在 testnet 模拟盘充分运行验证。
