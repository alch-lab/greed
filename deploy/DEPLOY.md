# greed 公网部署指南

架构：

```
浏览器 ──HTTPS──> Caddy（443，自动 Let's Encrypt 证书）
                    ├─ /api/*  → 127.0.0.1:8088（greed serve，仅监听本机）
                    └─ 其他    → /opt/greed/web/dist（greed-web 静态构建）
greed serve ──> 币安 testnet/主网（交易）+ 本地 data/（journal、数据湖）
```

安全要点：控制面只绑 `127.0.0.1` 不直接暴露；HTTPS 由 Caddy 终结；
`GREED_WEB_PASSWORD` 必须设置（登录鉴权 + 按 IP 限流 + token 7 天过期）。

## 1. 服务器准备（Ubuntu/Debian 示例）

```bash
# 运行用户
sudo useradd -r -m -d /opt/greed greed

# Caddy（官方仓库）
sudo apt install -y caddy

# Rust 工具链（在服务器上编译；或在本地交叉编译 linux/amd64 后上传二进制）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 2. 部署后端

```bash
# 上传源码后（或 git clone）
cd /opt/greed
cargo build --release
cp target/release/greed /opt/greed/greed

# 配置
cp config/base.toml config/strategy-final.toml /opt/greed/config/   # 随源码已有
cp deploy/greed.env.example /opt/greed/greed.env
chmod 600 /opt/greed/greed.env
# 编辑 greed.env：GREED_WEB_PASSWORD、BINANCE_API_KEY/SECRET（testnet）

# systemd
sudo cp deploy/greed-serve.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now greed-serve
systemctl status greed-serve    # 应看到 控制面已启动 addr=127.0.0.1:8088
```

## 3. 部署前端

```bash
cd greed-web
npm install
npm run build                    # 产物在 dist/
sudo mkdir -p /opt/greed/web
sudo cp -r dist /opt/greed/web/
```

## 4. HTTPS 反代

```bash
# 编辑 deploy/Caddyfile：把 greed.example.com 换成你的域名
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile
sudo mkdir -p /var/log/caddy && sudo chown caddy:caddy /var/log/caddy
sudo systemctl reload caddy
# 首次访问自动签发证书（要求域名 A 记录已指向本机、80/443 放行）
```

验证：`curl https://<域名>/api/health` 应返回 `{"ok":true}`；
浏览器打开 `https://<域名>` 应看到登录页。

## 5. 数据说明

- **交易（模拟盘/实盘）不需要数据湖**——实时行情走 WebSocket，开箱即用。
- **回测页需要数据湖**（`data/lake/`，约 135 MB/天/交易对）：
  - 方式 A：从本机 rsync 已有的湖 `rsync -avz data/lake/ server:/opt/greed/data/lake/`
  - 方式 B：服务器上自行采集——`greed ingest` 回补历史 + `greed collect` 持续采集
    （可再加一个 `greed-collect.service`，照抄 serve 的 unit 改 ExecStart）

## 6. 日常运维

```bash
journalctl -u greed-serve -f          # 看日志（启动/下单/成交/对账都在这里）
sudo systemctl restart greed-serve    # 重启（交易任务不会自动恢复——
                                      # 到前端重新点“启动策略”；交易所挂单不受影响）
```

策略执行协程因网络或交易所 API 错误退出时，会在同一个 `greed serve` 进程内按
2 秒到 5 分钟的指数退避自动重启，前端会显示恢复状态与错误原因，无需人工重启。
只有整个 systemd 服务被重启时，才需要重新点击“启动策略”。

注意：**重启 serve 会停掉正在运行的交易策略**（优雅退出、不撤保护性止损单），
重启后需要在前端手动重新启动策略。
