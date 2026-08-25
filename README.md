# greed

一个只连接 Binance Futures Demo 远端撮合的中频策略运行时。没有主网下单入口，也没有本地
模拟成交器。

生产代码只有三层：

```text
greed-kernel    市场、账户、候选和仓位计划契约
greed-strategy  SFP反转、趋势延续、点火冲刺、统一仓位与风控
greed-runtime   Binance WebSocket、Demo 执行、日志和监控 API
```

三条 Demo 资金策略：

- `sfp_reversal`：1h刺破过去12天前高后重新收回，随后三小时内跌破拒绝K线低点才做空；
  0.5%权益风险，结构止损，2R全平，最多持仓3小时。该分支样本仍小，只用于Demo验证。

- `trend_continuation`：4h/12h 趋势一致，15m 回踩并重新延续后入场；1%权益风险，2R平40%，
  剩余仓位保本并跟踪趋势。
- `ignition_sprint`：5m价格和成交点火后，在3分钟内等待1m微回踩与主动成交重新同向；OFI保留
  为诊断特征，不作为未经历史验证的硬门；
  0.25%权益风险，post-only限价等待30秒，1 ATR止损、1.25R止盈、最多持仓10分钟。

三条策略共享同一动态币池、最多3个持仓、4倍总名义敞口、2.5%日损停止和10%峰值回撤停止。
同一币种同一帧只允许一个解释；SFP失败突破优先于趋势延续，禁止同时提交相反方向计划。

## 验证与运行

```bash
cargo test --workspace
cargo build --release
./target/release/greed validate --config config/demo.toml
```

配置 Demo API 后运行：

```bash
export BINANCE_DEMO_API_KEY='...'
export BINANCE_DEMO_API_SECRET='...'
./target/release/greed paper --config config/demo.toml
```

API 默认监听 `127.0.0.1:8088`：`/api/health`、`/api/status`、`/api/events`、
`/api/history`。运行细节见 [架构说明](docs/ARCHITECTURE.zh-CN.md)。
