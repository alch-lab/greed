# greed

统一币池、趋势复利 lane、Binance Demo 真实模拟撮合。运行时不会向 Binance 主网提交订单，
也没有本地成交模拟器。

```text
greed-kernel    市场/账户契约、Artifact、策略 DAG
greed-strategy  趋势续航 lane、研究采样模块与统一组合风控
greed-runtime   Binance/Hyperliquid 公共数据、Binance Demo 执行、日志与监控 API
```

## 当前资金 lane

- `trend_continuation`：识别 4h/12h 同向趋势，等待 15m 回踩 EMA21 后重新站回 EMA8，
  再由主动成交、量能、价差和深度确认。多空完全对称，不追第一根脉冲。
- `liquidation_impulse`、`cross_venue_crowding`：保留代码和数据采样，但默认不启用资金执行；
  当前历史样本还不足以证明它们能改善组合。

模块使用同一个最多 30 个 USDT 永续合约的动态币池，不再区分 major/altcoin。
全市场发现与活跃合约行情以 WebSocket 为主；REST 只补首次入池的 K 线、低频 OI 和 Demo 账户/订单。

## 验证与运行

```bash
cargo test --workspace
cargo build --release
./target/release/greed validate --config config/demo.toml
./target/release/greed once --config config/demo.toml
```

模拟盘需要 Binance Futures Demo key：

```bash
export BINANCE_DEMO_API_KEY='...'
export BINANCE_DEMO_API_SECRET='...'
./target/release/greed paper --config config/demo.toml
```

Hyperliquid 仅通过公开 `info` API 提供跨场数据，不需要 API key。只有未来改为在 Hyperliquid
下单时才需要钱包/交易授权。

Demo 账户必须使用 One-way Mode，启动前不能留有不属于当前 runtime 的仓位。认证、账户同步或
保护单失败会停止新增订单；保护单提交失败时，已成交入场会立即发送 reduce-only 市价平仓。

监控 API 默认监听 `127.0.0.1:8088`：`/api/health`、`/api/status`、`/api/events`、
`/api/history`。详细设计与一周验证指标见 [架构说明](docs/ARCHITECTURE.zh-CN.md)。
