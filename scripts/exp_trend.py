#!/usr/bin/env python3
"""趋势模块信号层原型实验（2026-07-28）。

设计目标：捕捉阴跌/单边行情（MR 结构性缺席的场景），与均值回归模块互补。
策略：1h 唐奇安突破（海龟 S1 变体），最小参数集：
  - 入场：1h 收盘突破前 N 根最高/最低 → 下一根开盘进场（taker）
  - 出场：反向 M=N/2 根通道突破离场，或 2×ATR(14,1h) 硬止损（盘中触及）
  - 费用：taker 4bps + 滑点 1bps（单边），与 MR 回测口径一致
  - 仓位：0.75% 风险/笔（止损距离定仓），名义本金 ≤ 3x 权益
  - 单仓位，不加仓

用法：
  python scripts/exp_trend.py fetch          # 拉取并缓存 1h K线到 data/exp/btc_1h.csv
  python scripts/exp_trend.py run [N ...]    # 对 N 列表跑双期回测（默认 20）
"""
import csv
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path("/Users/wonder/Code/greed")
CACHE = ROOT / "data/exp/btc_1h.csv"
SYM = "BTCUSDT"


def ms_of(s):
    import datetime
    return int(datetime.datetime.fromisoformat(s)
               .replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def fetch():
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    start, end = ms_of("2024-11-01"), ms_of("2026-07-01")  # 前置 2 个月做指标预热
    out, cur = [], start
    while cur < end:
        url = (f"https://fapi.binance.com/fapi/v1/klines?symbol={SYM}&interval=1h"
               f"&startTime={cur}&endTime={end}&limit=1500")
        with urllib.request.urlopen(url, timeout=20) as r:
            data = json.load(r)
        if not data:
            break
        out.extend(data)
        cur = data[-1][0] + 3600000
        time.sleep(0.12)
    with open(CACHE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["open_time", "open", "high", "low", "close", "volume"])
        for k in out:
            w.writerow([k[0], k[1], k[2], k[3], k[4], k[5]])
    print(f"fetched {len(out)} bars -> {CACHE}")


def load():
    bars = []
    with open(CACHE) as f:
        for r in csv.DictReader(f):
            bars.append((int(r["open_time"]), float(r["open"]), float(r["high"]),
                         float(r["low"]), float(r["close"])))
    bars.sort()
    return bars


def backtest(bars, n, frm, to, risk_pct=0.0075, equity0=100_000.0,
             fee=0.0004, slip=0.0001, max_lev=3.0):
    """返回 (trades, equity_curve)。trades: dict 列表。"""
    m = n // 2
    atr = None
    trs = []
    equity = equity0
    pos = None  # dict(dir, entry, stop, qty, entry_t)
    trades = []
    curve = []
    cost_side = fee + slip

    for i in range(len(bars)):
        t, o, h, l, c = bars[i]
        # ATR(14) Wilder
        if i > 0:
            pc = bars[i - 1][4]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            if i <= 14:
                trs.append(tr)
                if i == 14:
                    atr = sum(trs) / 14
            else:
                atr = (atr * 13 + tr) / 14

        in_range = ms_of(frm) <= t <= ms_of(to) + 86399999

        # 盘中止损（用当根 high/low，保守假设先触及止损）
        if pos is not None and atr is not None:
            stopped = (pos["dir"] == 1 and l <= pos["stop"]) or \
                      (pos["dir"] == -1 and h >= pos["stop"])
            if stopped:
                px = pos["stop"]
                pnl = pos["dir"] * (px - pos["entry"]) * pos["qty"]
                fee_out = px * pos["qty"] * cost_side
                equity += pnl - fee_out
                trades.append({**pos, "exit": px, "exit_t": t, "pnl": pnl - fee_out,
                               "reason": "stop"})
                pos = None

        # 通道出场 / 突破入场（收盘确认，下一根开盘成交）
        if i > n + 14 and atr is not None:
            hh = max(b[2] for b in bars[i - n - 1:i])
            ll = min(b[3] for b in bars[i - n - 1:i])
            hh_x = max(b[2] for b in bars[i - m - 1:i])
            ll_x = min(b[3] for b in bars[i - m - 1:i])
            nxt_o = bars[i + 1][1] if i + 1 < len(bars) else c

            if pos is not None:
                exit_sig = (pos["dir"] == 1 and c < ll_x) or \
                           (pos["dir"] == -1 and c > hh_x)
                if exit_sig:
                    px = nxt_o
                    pnl = pos["dir"] * (px - pos["entry"]) * pos["qty"]
                    fee_out = px * pos["qty"] * cost_side
                    equity += pnl - fee_out
                    trades.append({**pos, "exit": px, "exit_t": t, "pnl": pnl - fee_out,
                                   "reason": "channel"})
                    pos = None

            if pos is None and in_range:
                sig = 1 if c > hh else (-1 if c < ll else 0)
                if sig != 0:
                    dist = 2 * atr
                    qty_risk = equity * risk_pct / dist
                    qty_cap = equity * max_lev / nxt_o
                    qty = min(qty_risk, qty_cap)
                    entry = nxt_o * (1 + sig * slip)  # 滑点
                    fee_in = entry * qty * fee
                    equity -= fee_in
                    stop = entry - sig * dist
                    pos = {"dir": sig, "entry": entry, "stop": stop, "qty": qty,
                           "entry_t": t, "fee_in": fee_in}
        if in_range:
            upnl = 0.0
            if pos is not None:
                upnl = pos["dir"] * (c - pos["entry"]) * pos["qty"]
            curve.append((t, equity + upnl))

    # 期末强平
    if pos is not None:
        px = bars[-1][4]
        pnl = pos["dir"] * (px - pos["entry"]) * pos["qty"]
        fee_out = px * pos["qty"] * cost_side
        equity += pnl - fee_out
        trades.append({**pos, "exit": px, "exit_t": bars[-1][0],
                       "pnl": pnl - fee_out, "reason": "eod"})

    peak, maxdd = equity0, 0.0
    for _, e in curve:
        peak = max(peak, e)
        maxdd = max(maxdd, (peak - e) / peak)
    return trades, curve, equity, maxdd


def report(bars, n, periods):
    res = {}
    for label, (frm, to) in periods.items():
        trades, curve, eq, mdd = backtest(bars, n, frm, to)
        rng = [tr for tr in trades if ms_of(frm) <= tr["entry_t"] <= ms_of(to) + 86399999]
        wins = [tr for tr in rng if tr["pnl"] > 0]
        gp = sum(tr["pnl"] for tr in rng if tr["pnl"] > 0)
        gl = -sum(tr["pnl"] for tr in rng if tr["pnl"] < 0)
        net = sum(tr["pnl"] for tr in rng)
        pf = gp / gl if gl > 0 else None
        res[label] = {
            "n": len(rng), "winrate": len(wins) / len(rng) if rng else None,
            "net": net, "mdd": mdd, "pf": pf,
            "long": sum(1 for tr in rng if tr["dir"] == 1),
            "short": sum(1 for tr in rng if tr["dir"] == -1),
            "trades": rng,
        }
    return res


PERIODS = {"2025": ("2025-01-01", "2025-12-31"), "2026H1": ("2026-01-01", "2026-06-30")}

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "fetch":
        fetch()
        sys.exit(0)
    args = [a for a in sys.argv[1:] if a != "run"]
    ns = [int(x) for x in args] if args else [20]
    bars = load()
    print(f"bars: {len(bars)}")
    for n in ns:
        res = report(bars, n, PERIODS)
        for label, r in res.items():
            wr = f"{r['winrate']*100:.1f}%" if r["winrate"] is not None else "-"
            pf = f"{r['pf']:.2f}" if r["pf"] else "-"
            print(f"N={n:<3} {label:<6} 笔数={r['n']:>3} (多{r['long']}/空{r['short']}) "
                  f"胜率={wr:>6} 净=${r['net']:>8.0f} 回撤={r['mdd']*100:.2f}% PF={pf}")
