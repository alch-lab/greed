# 统一 alpha 架构与模拟盘验证

## 边界

- 仅 Binance Futures Demo 负责下单、撮合、仓位和保护单；代码没有本地成交模拟器。
- Binance 主网只提供公开行情。执行 URL 只允许 Demo/Testnet 域名。
- Hyperliquid 只调用公开 `https://api.hyperliquid.xyz/info`，不需要密钥，也不执行订单。
- Binance secret 只从 `BINANCE_DEMO_API_KEY`、`BINANCE_DEMO_API_SECRET` 环境变量读取。

## 数据路径

全市场 `!ticker@arr` WebSocket 每次更新都会进入雷达。运行时每 15 秒按 24 小时流动性与
1m/5m/15m 异动重排，保留种子标的并将完整订阅限制在 30 个合约。活跃合约订阅
15m/5m/1m K 线、mark price、aggTrade、forceOrder 和 20 档盘口。

首次入池分别用一次 REST 请求补齐 15m/5m 历史。OI 正常每 120 秒刷新；出现至少 5 万美元
爆仓流时，对相关币种临时提升为最快 15 秒一次。Hyperliquid 的 `metaAndAssetCtxs` 与
`predictedFundings` 每 60 秒各调用一次。日志记录每个 endpoint 的请求、失败、429、重试、
延迟和 Binance `x-mbx-used-weight-1m`。

## 信号与组合

当前只有经过 walk-forward 与锁定样本验证的 `trend_continuation` 可以使用资金，输出统一
`TradeCandidate` 后由组合层排序与下单：

1. 4h 绝对涨跌至少 6%，12h 同方向，4h 路径效率至少 45%；
2. 最近三个已完成 15m K 线触及 EMA21，最新 K 线重新站回 EMA8 并延续方向；
3. 最近 1h 成交额不低于 24h 小时均值的 65%，最新 K 线主动成交方向一致；
4. WebSocket 盘口新鲜、spread 与深度可执行，候选最多保留 5 分钟。

多空条件对称。`liquidation_impulse` 与 `cross_venue_crowding` 默认关闭，只继续采集所需数据，
不能在没有新的样本外证据时自动获得资金。

每条 lane 每帧最多 2 个候选。组合最多同时持有 3 个仓位，同一币种不重复开仓。

## 仓位和风控

策略资本基准为 2,000 USDT。每笔初始风险 1%（20 USDT），初始止损 1%，目标名义仓位约
2,000 USDT，并受单笔 1.5 倍权益上限限制。最多 3 个仓位、总名义敞口 4 倍权益；5 倍杠杆
只降低保证金占用，不改变止损风险。

达到 2R 平 40%，随后止损移动到包含 0.18% 成本缓冲的保本位，剩余 60% 使用 0.5%
单向 trailing，不设固定 4h/6h 强平。日亏 2.5% 或峰值回撤 10% 触发组合停止。每个
lane/方向独立统计最近 20 笔，至少 8 笔后 PF 低于 1 进入 6 小时冷却。

## 一周验证

旧的 15m 本地 K 线回测器已删除，因为它无法重建 aggTrade、forceOrder、跨场资金费和真实
Demo 撮合，结果会误导。模拟周应至少检查：

- 每条 lane 的观察数、候选、计划、成交、拒单与卡点；
- 方向拆分后的成交数、胜率、PF、净收益、手续费、MFE/MAE、持仓时间；
- signal-to-order 与 discovery latency；
- WebSocket 重连/消息空窗、Hyperliquid 配对覆盖、REST 权重和限流；
- 止损、分段止盈、保本移动、trailing 和最长持有退出是否都由交易所事件验证。

一周数据不足以证明未来盈利，只用于排除执行错误、明显负期望 lane 和参数过于惰性/过密。
