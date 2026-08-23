# Greed 架构与一周模拟盘说明

## 安全边界

`greed` 的 forward test 只连接 Binance Demo/Testnet，不允许配置主网交易域名。API key 和
secret 只从环境变量读取，不写入 TOML、状态或日志。历史回测使用仅由 `backtest` 命令调用
的 K 线成交仿真；`paper` 命令使用 Binance Demo 的账户、撮合、订单过滤器和保护单。
程序没有本地模拟成交模式，也没有主网 `live` 模式。

旧 runner、旧插件注册表和旧账户代码已经删除。

## crate 边界

- `greed-kernel`：无 I/O 领域合同、Artifact、DAG 拓扑和执行。
- `greed-strategy`：特征原语、状态分类器、recipe 和组合风控。
- `greed-runtime`：公开行情适配、Binance Demo 鉴权执行、隔离的历史回测仿真和 journal。

事件处理入口固定为：

```text
Binance WebSocket → Market Cache → MarketFrame → Primitive DAG → State DAG → Recipe → PositionPlanner
            → Binance Demo → Account/Order Reconcile → Journal / Status / Report
```

任何节点的数据不足必须输出 `Unknown` 或不产生候选，不允许把缺失值解释为零或通过。

## 当前原语

- 价格：多周期收益、路径效率、趋势/震荡状态。
- 订单流：Binance 现货与永续 taker CVD 分离、现货参与占比、perp-led 判定。
- 衍生品：OI 十分钟变化、价格/OI 象限、funding、basis。
- 流动性：spread、两侧深度、预估扫单滑点、山寨币成交额/OI/盘口资格。
- 外部资金：USDT/USD 校正后的 Coinbase true premium、手工确认的 ETF/CME 慢上下文。
- 结构：64 档蜡烛成交量近似 POC；其质量明确标为 `Partial`，不能冒充逐笔 POC。
- 订单墙：最大盘口档位、名义金额和跨轮询持续性；墙撤销后持久度归零。
- 横截面：合格山寨币池的中位收益、上涨/下跌参与率、相对强弱排名。
- 退出：交易所 STOP_MARKET、TAKE_PROFIT_MARKET 和 reduce-only 时间退出。
- 风控：北京时间风险日、日亏损熔断、峰值回撤熔断、已有仓位总敞口上限。

模拟资金为 3000 USDT，并硬分成主流币、山寨币各 1500 USDT 的独立资金桶。当前主流币
单笔名义仓位 600 USDT，早期脉冲单笔 240 USDT，异动探索仓 60 USDT；任何一类都不能借用
另一类未使用的预算，组合也不允许超过 3000 USDT 名义敞口。

尚未接入但合同已预留的数据包括逐笔成交 POC、强平 WebSocket、ETF 自动抓取、CME
日终数据和多交易所盘口。这些应在一周样本确认数据质量后分批加入。

## 当前 recipe

- `major_trend_pullback`：BTC/ETH 趋势、现货/永续 CVD、OI 新仓、流动性、Coinbase
  true premium、回踩和收回。
- `major_exhaustion_reversal`：大幅移动、跨市场 CVD 反转、去杠杆完成和流动性确认。
- `alt_outlier_momentum`：1h/4h 加速、量能和路径效率先触发雷达，再使用 5m K 线区分延续、
  回踩收复和高潮风险；只有至少一个市场上下文同向才允许 2% 观察仓。
- `alt_early_impulse`：15m 移动 0.6%～2.5%、5m 成交量至少 1.6 倍、突破结构和高潮过滤；
  BTC 趋势或山寨币广度至少一个同向时使用 8% 进攻仓，最长持仓 90 分钟。
- `alt_cross_section_momentum`：保留作研究分支，生产配置暂时关闭。
- `alt_shock_reversal`：一小时冲击、15m 强反转、量能异常和全市场方向锚。

当前职责刻意不对称：Major 使用 24 小时趋势窗口、更高趋势效率和回踩/订单流确认，作为
低频压仓石；Altcoin 是受控进攻桶。运行时由全市场 ticker WebSocket 持续发现异动，每分钟
按 15m、1h、24h 动量和流动性重排，最多保留 30 个进入 15m/5m K 线、盘口和 OI 评估。
K 线、mark price 和 20 档盘口全部使用推送缓存；新币首次入池才用 REST 补历史，OI 因没有
对应推送流而每 120 秒低频补充。妖币候选按
15 分钟 cycle 去重、7～10 分钟过期、最长持仓 2 小时；BTC/广度用于确认和缩仓。冲击反转
每帧记录距离移动、反转和量能门槛的差距，不再静默返回。

每个计划使用费用感知的三段退出：0.75R 平 25% 并把止损移动到入场价加 15 bps 缓冲，
1R 再平 30%，剩余 45% 在 2.5R 或移动保护下退出。交易所部分成交写
`exchange_partial_exit`，止损上移写 `exchange_protection_updated`；两者都包含实际数量和
累计交易所费用/净盈亏。PF 门控按 `recipe × side` 分开，3 笔后低于 1.0 暂停 12 小时。

所有 recipe 都生成带 blocker、证据 lineage 和过期时间的候选。只有 `Pass` 候选才会进入
Demo position planner；`Block/Unknown` 仍写 journal，用来研究漏斗和数据缺口。

Demo forward test 以交易所实际成交与费用为准。订单拒绝写入 `exchange_order_rejected`；
账户同步失败时整帧停止。入场成交后如果任一保护单失败，程序撤销残单并立即发送
reduce-only 市价平仓。每轮同步还会检查止损和止盈是否仍存在，缺失时强制退出。

## 一周运行

```bash
cargo build --release -p greed-runtime
./target/release/greed validate --config config/demo.toml
./target/release/greed once --config config/demo.toml
./target/release/greed paper --config config/demo.toml
```

服务器使用 `deploy/greed-paper.service`。`runtime.proxy` 只代理 REST；行情 WebSocket 需要
服务器能够直连 `wss://fstream.binance.com`。REST 会节流并轮换 Binance 官方备用域名。
WebSocket 自动应答 ping、指数退避重连，并在动态池变化时重建订阅；任一关键行情流超过
15 秒未更新，position planner 不会生成新订单。单个非关键接口失败会降级为 `Unknown`，
不会放宽策略门槛。

创建仅用于 Binance Demo 的环境文件，并将 Demo 账户切换为 One-way Mode：

```bash
install -m 600 /dev/null /etc/greed-paper.env
vi /etc/greed-paper.env
# BINANCE_DEMO_API_KEY=...
# BINANCE_DEMO_API_SECRET=...
```

首次安装 service 前先创建隔离用户和可写目录：

```bash
id greed >/dev/null 2>&1 || useradd --system --home /opt/greed --shell /sbin/nologin greed
mkdir -p /opt/greed/data/runtime
chown -R greed:greed /opt/greed/data/runtime
cp deploy/greed-paper.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now greed-paper
journalctl -u greed-paper -f
```

冷启动后 OI 变化会刻意保持约十分钟 `Unknown`，这是防止把单次 OI 快照误判成趋势，
不代表 runner 卡住。

持久文件：

- `data/runtime/demo-events.jsonl`：行情、全量 Artifact、交易所请求结果、成交和错误。
- `data/runtime/demo-history.jsonl`：供监控页读取的账户曲线和交易所活动。
- `data/runtime/binance-demo-state.json`：Demo 钱包基线、已见候选以及交易所仓位的策略归属。
  交易所持仓是事实来源；状态文件不存在但账户仍有仓位时，程序拒绝启动新增交易。
- `data/runtime/demo-status.json`：当前账户、持仓、候选、风控和所有原语状态。
- `data/runtime/slow-context.json`：确认后的 ETF/CME 日频输入，可参考示例文件。

一周后执行：

```bash
./target/release/greed report --journal data/runtime/demo-events.jsonl \
  > data/runtime/week-1-report.json
```

报告按 Major/Altcoin、recipe、方向和北京时间自然日拆分成交数、手续费、已实现净盈亏、胜率、
PF 和平均持仓时间；同时包含两套资金曲线的最大回撤/日亏损、候选漏斗及高频 blocker、
程序重启与 commit/config 版本、帧空窗、WebSocket 可用率/重连/消息空窗，以及 Binance
各接口请求、真实分钟权重、失败、429/418、重试、备用域名和延迟统计。山寨币两个 recipe
的每帧状态及原因也会汇总。因此只要保留
`demo-events.jsonl`，一周后可以定位“哪套策略、哪条
recipe、哪一天、卡在哪一关、当时接口是否异常”。

## 一周评审门槛

先评数据，再评收益：

1. WebSocket 行情可用率至少 99.9%，没有连续一分钟消息空窗；REST 帧成功率至少 99%。
2. BTC/ETH 现货、永续、OI、盘口和 Coinbase 数据完整率分别统计。
3. Block/Unknown 的主要原因可解释，不能出现缺数据却 Pass。
4. 每个 recipe 的多空候选数量、成交数量、费用后 PF 分开统计；Major/Altcoin 分账检查
   最大回撤。若需要 MAE/MFE，再由全量 candle 和成交事件进行离线重放，不能用当前价近似。
5. Demo 交易所状态跨重启连续，不能重复开同一个 candidate。
6. 高滑点、接口中断和同 K 止损/止盈冲突按保守路径重放。

一周结果只用于修复数据和缩小策略空间，不足以证明长期 alpha，也不应直接开启真钱。

## 当前回测结论

回测数据来自 Binance 官方 15m/5m 归档，费用和滑点均按单边 5 bps 计入。中频 balanced
配置在 2026-08-15～08-21 上涨验证段完成 44 笔，组合 +13.49 USDT、PF 1.45、最大回撤
0.28%；早期启动 +7.15。2026-07-30～08-07 震荡验证段完成 17 笔，组合 -6.68、最大回撤
0.25%；早期启动 +1.13，主要亏损来自一笔 Major 趋势交易和小仓异动探索。

两个短窗口不能证明稳定盈利。回测的用途是确定中频频率、排除明显有害分支和建立日志基线；
异动延续保留极小观察仓，横截面分支因震荡窗口明显为负继续关闭。
当前结论是：只允许 Binance Demo forward test，禁止主网下单；一周后依据交易所真实拒单、
成交、费用和短周期路径决定删除或保留各分支。
一周 Demo forward test 后只有同时满足以下条件才进入下一阶段评审：

1. 行情与 OI 关键数据完整率至少 99%，没有连续五分钟空窗。
2. 至少 20 个完整平仓样本，费用后 PF 至少 1.25，净收益为正。
3. 最大回撤不超过 2%，没有触发日亏损或峰值回撤熔断。
4. BTC/ETH 趋势、山寨币横截面分别核算；任一分支 PF 小于 1 时只能继续 shadow。
5. 状态跨重启连续，没有重复候选、重复开仓或丢失止损。

即使全部通过，也应先扩展样本或使用极小风险限额，而不能仅凭一个上涨周直接投入全部资金。
