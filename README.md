# greed

可组合的加密货币策略研究与模拟盘系统。目前只有公开行情、回测和本地 paper 撮合，
没有交易所鉴权或实盘下单代码。

```text
greed-kernel    领域合同、Artifact、DAG
greed-strategy  原语、市场状态、策略组合、组合风控
greed-runtime   官方历史数据回测、公开实时行情、paper broker、日志
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
./target/release/greed once --config config/paper.toml
./target/release/greed paper --config config/paper.toml
./target/release/greed report --journal data/runtime/paper-events.jsonl
```

`paper` 启动后同时在 `127.0.0.1:8088` 提供只读监控接口：
`/api/health`、`/api/status`、`/api/events` 和 `/api/history`。监听地址可通过
`runtime.http_listen` 调整；生产环境应保持回环监听，由带访问控制的 Web 代理转发。

状态和历史会分别记录 Major/Altcoin 两个 1500 USDT 虚拟账本、每套策略当前执行阶段与
blocker、recipe 归属、程序 commit/config 版本及 Binance 限流/重试/延迟遥测。`report`
会把这些事件汇总成逐日、分资金桶、分 recipe 的一周审计报告。

Major 使用高确认、低频参数；Altcoin 每 15 分钟从 Binance 可交易 USDT 永续中动态组合
高流动性与异动合约，最多对 30 个标的做完整评估。妖币动量以个币 1h/4h 加速和量能为主，
BTC 与市场广度只调整仓位，不再作为同向硬门。候选按 1 小时 cycle 去重、最长持仓 3 小时，
并由独立滚动 10 笔 PF 门控限制坏行情中的连续试错。所有等待条件和 PF 拒单均进入状态、
历史和周报。

详细边界、数据含义、部署步骤与实盘门槛见
[架构与模拟盘说明](docs/ARCHITECTURE.zh-CN.md)。
