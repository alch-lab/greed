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
MarketFrame → Primitive DAG → State DAG → Recipe → PositionPlanner
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
单笔名义仓位 600 USDT，妖币动量基础单笔 150 USDT；任何一类都不能借用另一类未使用的预算，
组合也不允许超过 3000 USDT 名义敞口。

尚未接入但合同已预留的数据包括逐笔成交 POC、强平 WebSocket、ETF 自动抓取、CME
日终数据和多交易所盘口。这些应在一周样本确认数据质量后分批加入。

## 当前 recipe

- `major_trend_pullback`：BTC/ETH 趋势、现货/永续 CVD、OI 新仓、流动性、Coinbase
  true premium、回踩和收回。
- `major_exhaustion_reversal`：大幅移动、跨市场 CVD 反转、去杠杆完成和流动性确认。
- `alt_outlier_momentum`：1h/4h 加速、量能和路径效率先触发雷达，再使用 5m K 线区分延续、
  回踩收复和高潮风险；只有延续或回踩收复产生候选。BTC 与市场广度只调整仓位。
- `alt_cross_section_momentum`：保留作研究分支，生产配置暂时关闭。
- `alt_shock_reversal`：一小时冲击、15m 强反转、量能异常和全市场方向锚。

当前职责刻意不对称：Major 使用 24 小时趋势窗口、更高趋势效率和回踩/订单流确认，作为
低频压仓石；Altcoin 是受控进攻桶。运行时每 15 分钟从全部可交易 USDT 永续中组合高流动性
合约和涨跌异动合约，最多保留 30 个进入 15m/5m K 线、盘口和 OI 评估。妖币候选按
15 分钟 cycle 去重、10 分钟过期、最长持仓 3 小时；BTC 同向使用标准仓位，中性或反向只缩仓。冲击反转
每帧记录距离移动、反转和量能门槛的差距，不再静默返回。

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

服务器使用 `deploy/greed-paper.service`。需要网络代理时只填写
`runtime.proxy`；程序会在每次请求前节流，并轮换 Binance 官方备用域名。单个非关键接口
失败会降级为 `Unknown`，不会放宽策略门槛。

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
- `data/runtime/status.json`：当前账户、持仓、候选、风控和所有原语状态。
- `data/runtime/slow-context.json`：确认后的 ETF/CME 日频输入，可参考示例文件。

一周后执行：

```bash
./target/release/greed report --journal data/runtime/demo-events.jsonl \
  > data/runtime/week-1-report.json
```

报告按 Major/Altcoin、recipe、方向和北京时间自然日拆分成交数、手续费、已实现净盈亏、胜率、
PF 和平均持仓时间；同时包含两套资金曲线的最大回撤/日亏损、候选漏斗及高频 blocker、
程序重启与 commit/config 版本、帧空窗，以及 Binance 各接口请求、失败、429/418、重试、
备用域名和延迟统计。山寨币两个 recipe 的每帧状态及原因也会汇总。因此只要保留
`demo-events.jsonl`，一周后可以定位“哪套策略、哪条
recipe、哪一天、卡在哪一关、当时接口是否异常”。

## 一周评审门槛

先评数据，再评收益：

1. 主行情轮询成功率至少 99%，没有连续五分钟空窗。
2. BTC/ETH 现货、永续、OI、盘口和 Coinbase 数据完整率分别统计。
3. Block/Unknown 的主要原因可解释，不能出现缺数据却 Pass。
4. 每个 recipe 的多空候选数量、成交数量、费用后 PF 分开统计；Major/Altcoin 分账检查
   最大回撤。若需要 MAE/MFE，再由全量 candle 和成交事件进行离线重放，不能用当前价近似。
5. Demo 交易所状态跨重启连续，不能重复开同一个 candidate。
6. 高滑点、接口中断和同 K 止损/止盈冲突按保守路径重放。

一周结果只用于修复数据和缩小策略空间，不足以证明长期 alpha，也不应直接开启真钱。

## 当前回测结论

回测数据来自 Binance 官方 15m/5m 归档，费用和滑点均按单边 5 bps 计入。默认 balanced
雷达在 2026-08-15～08-21 上涨验证段的组合收益为 +0.82 USDT，PF 1.02、最大回撤
0.80%；其中延续入场 +2.52，回踩收复 -3.41。2026-07-30～08-07 震荡验证段组合
-13.26，其中山寨币延续入场 -7.56。激进雷达在上涨周收益更高，但震荡段亏损也更大。

因此 5m 确认解决的是“不要在单根高潮 K 线末端盲目追入”，并没有证明策略已经稳定盈利。
Continuation 与 Pullback Reclaim 使用独立滚动 PF 门控，避免一个分支拖累后继续无条件试错。
当前结论是：只允许 Binance Demo forward test，禁止主网下单；一周后依据交易所真实拒单、
成交、费用和短周期路径决定删除或保留各分支。
一周 Demo forward test 后只有同时满足以下条件才进入下一阶段评审：

1. 行情与 OI 关键数据完整率至少 99%，没有连续五分钟空窗。
2. 至少 20 个完整平仓样本，费用后 PF 至少 1.25，净收益为正。
3. 最大回撤不超过 2%，没有触发日亏损或峰值回撤熔断。
4. BTC/ETH 趋势、山寨币横截面分别核算；任一分支 PF 小于 1 时只能继续 shadow。
5. 状态跨重启连续，没有重复候选、重复开仓或丢失止损。

即使全部通过，也应先扩展样本或使用极小风险限额，而不能仅凭一个上涨周直接投入全部资金。
