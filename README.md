# greed

一个只连接 Binance Futures Demo 远端撮合的中频策略运行时。没有主网下单入口，也没有本地
模拟成交器。

生产代码只有三层：

```text
greed-kernel    市场、账户、候选和仓位计划契约
greed-strategy  趋势延续、统一仓位与风控
greed-runtime   Binance WebSocket、Demo 执行、日志和监控 API
```

当前只有一条 Demo 资金策略：

- `trend_continuation`：4h/12h 趋势一致，15m 回踩并重新延续后入场；1.5%权益风险。第一段
  到达约+0.4%后保护利润；若保护退出后先回撤、再由完整5m结构确认原方向恢复，只允许一次
  独立第二段入场，随后使用更宽的利润保护与跟踪退出。

它使用动态币池、最多3个持仓、4倍总名义敞口、2.5%日损停止和10%峰值回撤停止。
已证伪的 SFP、日内扫单、早期启动和 BTC 结构区策略只保留在历史研究与成交归因中，
不再进入运行图或产生新订单。

执行采用 maker-first：趋势单最多挂90秒GTX并每15秒向盘口调整，主单追价上限8bp；没有成交
就放弃，成交不足计划数量80%的偶发小仓会立即平掉，不作为正式持仓继续管理。正常止盈全部使用
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
export GREED_WEB_PASSWORD='8-or-more-characters'
./target/release/greed paper --config config/demo.toml
```

`GREED_WEB_PASSWORD` enables the operator session in the dashboard. Authenticated
operators can pause new entries and submit reduce-only manual closes; guests can
only read status and history. Operator actions and their market context are kept
in the same durable history ledger as automatic trades. The password must contain
at least 8 characters.

服务器首次启动或交易接口升级后，先停止运行时并执行零成交预检。脚本会验证普通订单 test
endpoint，以及创建、查询并撤销一张远离市价的 Demo 条件保护单；它拒绝在存在持仓或挂单时运行：

```bash
sudo systemctl stop greed-paper
sudo /opt/greed/deploy/preflight-binance-demo.sh /etc/greed-paper.env
```

API 默认监听 `127.0.0.1:8088`。只读状态接口保持公开；暂停、恢复和手动平仓接口
必须使用登录后取得的进程级 Bearer token（服务重启后自动失效）。运行细节见
[架构说明](docs/ARCHITECTURE.zh-CN.md)。
