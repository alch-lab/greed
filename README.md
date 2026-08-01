# greed

BTCUSDT 永续订单流力竭交易系统（Rust）。生产路径只有一个模型：

`10 秒放量 → effort/result 背离 → 3/5 分钟 Delta 翻转 → context 分层 → 结构风控`

模型直接消费 Binance 主网逐笔成交，并轮询公共订单簿与 OI。模拟盘/实盘只使用一个
USDⓈ-M 合约账户；没有 EMA、资金费 carry、每日执行探针或旧 MR 旁路。

## 代码结构

```text
crates/core       事件、类型和插件接口
crates/data       Binance 导入、采集和本地数据湖
crates/signals    OrderFlowExhaustion 原始订单流信号
crates/strategy   OrderFlowEntry、账户保护和分批出场
crates/backtest   逐笔回测、撮合、账户和 Journal
crates/live       主网公共行情 + dry/testnet/live 执行
crates/cli        greed 命令行与 HTTP 控制面
config/base.toml              市场和账户配置
config/strategy-final.toml    唯一生产策略配置
```

详细规则见 [docs/ORDERFLOW_MODEL.md](docs/ORDERFLOW_MODEL.md)。

## 构建与验证

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo build --release
./target/release/greed validate \
  --strategy config/strategy-final.toml --market perp --symbol BTCUSDT --lake data/lake
```

## 回测

```bash
./target/release/greed backtest \
  --market perp --symbol BTCUSDT --from 2026-01-01 --to 2026-06-30 \
  --lake data/lake --strategy config/strategy-final.toml \
  --cash 100000 --risk-pct 0.002 --max-risk-pct 0.005 --trials 1 \
  --out out/orderflow-2026h1 --journal data/journal/orderflow-2026h1.json
```

回测按时间合并逐笔成交、OI、资金费事件；订单簿文件存在时也会送入同一个信号插件。
手续费、滑点、分批止盈和结构止损均走撮合/账户层，不在研究脚本里另算。

## 模拟盘与实盘

在 Binance Demo Trading 创建合约 API key，密钥只放环境变量：

```bash
export BINANCE_API_KEY='...'
export BINANCE_API_SECRET='...'

# 不下真实订单
./target/release/greed trade --config config/base.toml \
  --strategy config/strategy-final.toml --dry-run --risk-pct 0.002 --max-risk-pct 0.005

# Demo Futures 真实撮合
./target/release/greed trade --config config/base.toml \
  --strategy config/strategy-final.toml --risk-pct 0.002 --max-risk-pct 0.005
```

paper/dry 的信号来自主网公共行情，订单在 Demo Futures 执行。启动时会同步服务器时间、
设置逐仓和杠杆并清理遗留挂单。若交易所已有仓位，只有它与同一策略 Journal 中可恢复
仓位完全一致时才会接管；否则拒绝启动。进程退出保留保护性止损。

订单流基线只用真实逐笔成交，默认需要 120 个 10 秒桶（约 20 分钟）。不会用 K 线合成
Delta。公共 depth/OI 暂时不可用时，核心成交模型仍运行，前端相应字段显示为空。

## 控制面与前端

```bash
./target/release/greed serve --port 8088
cd ../greed-web
npm ci
npm run build
npm run dev
```

前端展示每个 10 秒桶的五步漏斗、五个质量层级的独立收益/胜率/费用、真实意图和成交、
持仓、权益与自动重启状态。设置 `GREED_WEB_PASSWORD` 可启用登录。

## 数据

```bash
./target/release/greed ingest --market perp --symbol BTCUSDT \
  --from 2026-01-01 --to 2026-06-30 --lake data/lake
./target/release/greed collect --config config/base.toml
```

原始逐笔、订单簿和 OI 数据保存在 `data/lake/`；Journal 在 `data/journal/`。密钥、
研究附件和大体量市场数据不得提交到 Git。
