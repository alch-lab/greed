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

详细边界、数据含义、部署步骤与实盘门槛见
[架构与模拟盘说明](docs/ARCHITECTURE.zh-CN.md)。
