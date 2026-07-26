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

## 模拟盘 API key

在 <https://testnet.binancefuture.com> 申请币安合约 testnet 密钥，写入环境变量（不要写进配置文件）：

```bash
export BINANCE_API_KEY="..."
export BINANCE_API_SECRET="..."
```

`config/base.toml` 的 `[account]` 段按 `testnet = true/false` 自动选择 testnet/实盘端点，密钥只从 `api_key_env` / `api_secret_env` 指定的环境变量读取。

> 注意：当前只完成配置层。连接 testnet 实际下单的执行引擎尚未实现，落地时将复用同一 Journal 契约，前端无需改动。
