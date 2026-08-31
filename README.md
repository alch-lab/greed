# greed

一个只连接 Binance Futures Demo 远端撮合的中频策略运行时。没有主网下单入口，也没有本地
模拟成交器。

生产代码只有三层：

```text
greed-kernel    市场、账户、候选和仓位计划契约
greed-strategy  SFP反转、趋势延续、日内扫单反转、统一仓位与风控
greed-runtime   Binance WebSocket、Demo 执行、日志和监控 API
```

当前三条 Demo 资金策略：

- `sfp_reversal`：1h刺破过去12天前高后重新收回，随后三小时内跌破拒绝K线低点才做空；
  0.25%权益风险，结构止损，2R全平，最多持仓3小时。该分支样本仍小，只用于Demo验证。

- `trend_continuation`：4h/12h 趋势一致，15m 回踩并重新延续后入场；0.6%权益风险，2R平40%，
  剩余仓位保本并跟踪趋势。
- `intraday_sweep_reversal`：仅在4h非强趋势中运行。15m扫过近2小时局部高低点并收回，要求
  影线、1.6倍成交量和至少30%方向主动流，随后由1m突破拒绝K触发；多空对称，0.25%权益风险，
  结构止损，2R全平，最多持仓2小时。

四条策略共享同一动态币池、最多3个持仓、4倍总名义敞口、2.5%日损停止和10%峰值回撤停止。
同一币种同一帧只允许一个解释。日内扫单只在 |4h|<4% 运行，趋势延续要求 |4h|>=6%，
避免同一行情的反向解释。

执行采用 maker-first：趋势单挂60秒GTX并每15秒向盘口调整，主单追价上限8bp；仍未成交时，
只有可执行价处于原信号20bp内才用原计划仓位的5%做市价探针。SFP挂60秒，未成交直接放弃。
正常止盈全部使用
reduce-only GTC限价：通常在目标位休眠并按maker成交，若挂出前价格已经穿过目标则允许立即成交，
避免post-only拒单把盈利竞态变成保护失败。初始止损、熔断和保护失败强平仍使用taker。
趋势挂单存活期间还会持续使用主网WebSocket复核原信号：价格穿透计划入场位30bp，或成交方向与
10秒价格响应同时反转时立即撤单；若撤单竞态中已经成交，系统用reduce-only市价单立即平掉该部分。
这条保护复用已有流数据，不增加REST轮询。

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

服务器首次启动或交易接口升级后，先停止运行时并执行零成交预检。脚本会验证普通订单 test
endpoint，以及创建、查询并撤销一张远离市价的 Demo 条件保护单；它拒绝在存在持仓或挂单时运行：

```bash
sudo systemctl stop greed-paper
sudo /opt/greed/deploy/preflight-binance-demo.sh /etc/greed-paper.env
```

API 默认监听 `127.0.0.1:8088`：`/api/health`、`/api/status`、`/api/events`、
`/api/history`。运行细节见 [架构说明](docs/ARCHITECTURE.zh-CN.md)。
