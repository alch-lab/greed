# greed

可组合的加密货币策略研究与 Binance Demo 模拟交易系统。策略账户使用交易所模拟撮合，
不会向 Binance 主网提交订单；本地 broker 只保留给历史回测。

```text
greed-kernel    领域合同、Artifact、DAG
greed-strategy  原语、市场状态、策略组合、组合风控
greed-runtime   官方历史数据回测、公开实时行情、Binance Demo 执行、日志
```

## 验证与回测

```bash
cargo test --workspace
cargo build --release

./target/release/greed validate --config config/paper.toml
./target/release/greed backtest --config config/paper.toml \
  --train-from 2026-08-08 --split 2026-08-15 --to 2026-08-21
```

回测自动下载并缓存 Binance 官方归档，训练段只负责选参数，验证段不参与选择。

## 一周模拟盘

```bash
sudo install -m 600 /dev/null /etc/greed-paper.env
# 在 /etc/greed-paper.env 中填写 Binance Demo API key/secret：
# BINANCE_DEMO_API_KEY=...
# BINANCE_DEMO_API_SECRET=...

./target/release/greed once --config config/paper.toml
./target/release/greed paper --config config/paper.toml
./target/release/greed report --journal data/runtime/demo-events.jsonl
```

Demo 账户必须使用 One-way Mode，启动前不能留有不属于当前 runtime 的仓位。认证、账户同步或
保护单失败时，程序会停止新增订单；保护单提交失败时，已成交的入场会立即发送 reduce-only
市价平仓，不会退回本地模拟成交。

`paper` 启动后同时在 `127.0.0.1:8088` 提供只读监控接口：
`/api/health`、`/api/status`、`/api/events` 和 `/api/history`。监听地址可通过
`runtime.http_listen` 调整；生产环境应保持回环监听，由带访问控制的 Web 代理转发。

状态和历史记录交易所账户、订单、成交、拒单、保护退出、策略阶段、程序 commit/config
版本及 Binance 限流/重试/延迟遥测。

Major 使用高确认、低频参数；Altcoin 每 15 分钟从 Binance 可交易 USDT 永续中动态组合
高流动性与异动合约，最多对 30 个标的做完整评估。1h/4h 涨跌和量能只触发雷达；入场还需
5m 延续或回踩收复确认。大振幅高潮 K 线、方向影线和过度延伸会拒绝追入。BTC 与市场广度
只调整仓位，不再作为同向硬门。候选 10 分钟过期，最长持仓 3 小时。

详细边界、数据含义、部署步骤与实盘门槛见
[架构与模拟盘说明](docs/ARCHITECTURE.zh-CN.md)。
