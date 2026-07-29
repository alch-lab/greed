#!/usr/bin/env python3
"""OI 四象限 / 资金费率信号层原型实验（2026-07-29）。

冲击 20% 的最后一条证据型路径（STRATEGY_DOC 9.10 节）。数据全部本地：
  - OI/多空比/taker 量比：data/lake/metrics/binance_futures/BTCUSDT/*.csv（5m）
  - 资金费率：data/lake/funding/binance_futures/BTCUSDT/funding.csv（8h）
  - 30m 价格：币安 API（缓存 data/exp/btc_30m.csv）

信号候选（复刻 OiQuadrant 插件口径：30m 柱 ΔOI% ±0.5% 噪音带）：
  fade 族（杀多/逼空=出清→反向）:
    long_kill(ΔOI<-0.5% 且 r<0) → 多；short_squeeze(ΔOI<-0.5% 且 r>0) → 空
  follow 族（增仓=趋势燃料→同向）:
    long_open(ΔOI>0.5% 且 r>0) → 多；short_open(ΔOI>0.5% 且 r<0) → 空
  funding 极端值 fade：|rate| ≥ f_thr → 结算后反向
出场：固定持有 H 根 30m 柱（或对称 TP/SL）。
成本：taker 4bps + 滑点 1bps 单边，固定名义 $100k/笔（信号层 edge 检验）。

用法：
  python scripts/exp_oi.py fetch   # 缓存 30m K线
  python scripts/exp_oi.py run     # 跑全部变体双期评估
"""
import csv
import datetime as dt
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path("/Users/wonder/Code/greed")
METRICS_DIR = ROOT / "data/lake/metrics/binance_futures/BTCUSDT"
FUNDING_CSV = ROOT / "data/lake/funding/binance_futures/BTCUSDT/funding.csv"
K30_CACHE = ROOT / "data/exp/btc_30m.csv"
BUCKET_MS = 30 * 60_000
NOTIONAL = 100_000.0
COST_SIDE = 0.0004 + 0.0001  # taker + 滑点（单边）


def ms_of(s):
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)


def fetch30m():
    K30_CACHE.parent.mkdir(parents=True, exist_ok=True)
    start, end = ms_of("2024-12-15"), ms_of("2026-07-01")
    out, cur = [], start
    while cur < end:
        url = (f"https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=30m"
               f"&startTime={cur}&endTime={end}&limit=1500")
        with urllib.request.urlopen(url, timeout=20) as r:
            data = json.load(r)
        if not data:
            break
        out.extend(data)
        cur = data[-1][0] + BUCKET_MS
        time.sleep(0.12)
    with open(K30_CACHE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["open_time", "open", "high", "low", "close"])
        for k in out:
            w.writerow([k[0], k[1], k[2], k[3], k[4]])
    print(f"fetched {len(out)} 30m bars")


def load_frame():
    """返回 30m 柱序列: (bucket_ms, open, high, low, close, oi_usd|None)"""
    oi = {}
    for p in sorted(METRICS_DIR.glob("*.csv")):
        with open(p) as f:
            for r in csv.DictReader(f):
                t = dt.datetime.strptime(r["create_time"], "%Y-%m-%d %H:%M:%S") \
                    .replace(tzinfo=dt.timezone.utc)
                ms = int(t.timestamp() * 1000)
                oi[ms] = float(r["sum_open_interest_value"])
    bars = []
    with open(K30_CACHE) as f:
        for r in csv.DictReader(f):
            bars.append((int(r["open_time"]), float(r["open"]), float(r["high"]),
                         float(r["low"]), float(r["close"])))
    bars.sort()
    oi_ts = sorted(oi)
    frame = []
    j = 0
    for (t, o, h, l, c) in bars:
        # 桶内最后一个 OI 读数（≤ t+30m）
        last = None
        while j < len(oi_ts) and oi_ts[j] <= t + BUCKET_MS - 1:
            last = oi[oi_ts[j]]
            j += 1
        frame.append((t, o, h, l, c, last))
    return frame


def load_funding():
    rows = []
    with open(FUNDING_CSV) as f:
        for r in csv.DictReader(f):
            rows.append((int(r["funding_time_ms"]), float(r["rate"])))
    rows.sort()
    return rows


def quadrant_signals(frame, noise=0.5, reset_pct=3.0):
    """复刻 OiQuadrant：返回 {bucket_ms: quadrant}"""
    sigs = {}
    reset_day = None
    prev = None
    for (t, o, h, l, c, oi) in frame:
        if prev is not None and oi is not None and prev[1] is not None and prev[1] > 0:
            doi = (oi - prev[1]) / prev[1] * 100
            ret = (c - prev[0]) / prev[0] * 100 if prev[0] > 0 else 0.0
            day = t // 86_400_000
            if doi < -reset_pct:
                reset_day = day
                sigs[t] = "reset"
            elif reset_day == day:
                pass
            elif abs(doi) >= noise and ret != 0:
                q = {(True, True): "long_open", (True, False): "short_open",
                     (False, True): "short_squeeze", (False, False): "long_kill"}[
                    (doi > 0, ret > 0)]
                sigs[t] = q
        prev = (c, oi)
    return sigs


def bt_horizon(frame, entries, hold_bars, frm, to):
    """固定持有 H 柱出场（下一根开盘进、H 柱后开盘出），单仓位。"""
    lo, hi = ms_of(frm), ms_of(to) + 86_399_999
    trades = []
    i = 0
    n = len(frame)
    while i < n:
        t = frame[i][0]
        if t in entries and lo <= t <= hi and i + 1 < n:
            d = entries[t]
            entry_i = i + 1
            exit_i = min(entry_i + hold_bars, n - 1)
            ep = frame[entry_i][1]
            xp = frame[exit_i][1]
            pnl = d * (xp - ep) / ep * NOTIONAL - NOTIONAL * COST_SIDE * 2
            trades.append({"t": t, "dir": d, "pnl": pnl})
            i = exit_i
        else:
            i += 1
    return trades


def bt_tpsl(frame, entries, tp, sl, frm, to, max_hold=48):
    """对称 TP/SL（盘中 high/low 判定，保守先止损），最长持有 max_hold 柱。"""
    lo, hi = ms_of(frm), ms_of(to) + 86_399_999
    trades = []
    i = 0
    n = len(frame)
    while i < n:
        t = frame[i][0]
        if t in entries and lo <= t <= hi and i + 1 < n:
            d = entries[t]
            ep = frame[i + 1][1]
            xp, xt = None, None
            for j in range(i + 1, min(i + 1 + max_hold, n)):
                _, o, h, l, c, _ = frame[j]
                hit_tp = h >= ep * (1 + d * tp)
                hit_sl = l <= ep * (1 - d * sl) if d == 1 else l <= ep * (1 - sl)
                if d == 1:
                    hit_sl = l <= ep * (1 - sl)
                    hit_tp = h >= ep * (1 + tp)
                else:
                    hit_sl = h >= ep * (1 + sl)
                    hit_tp = l <= ep * (1 - tp)
                if hit_sl:
                    xp, xt = ep * (1 - d * sl), frame[j][0]
                    break
                if hit_tp:
                    xp, xt = ep * (1 + d * tp), frame[j][0]
                    break
            if xp is None:
                j = min(i + 1 + max_hold, n - 1)
                xp, xt = frame[j][4], frame[j][0]
            pnl = d * (xp - ep) / ep * NOTIONAL - NOTIONAL * COST_SIDE * 2
            trades.append({"t": t, "dir": d, "pnl": pnl})
            i = j
        else:
            i += 1
    return trades


def stat(trades, label, extra=""):
    if not trades:
        print(f"  {label:<28} 0 笔 {extra}")
        return
    wins = [t for t in trades if t["pnl"] > 0]
    gp = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gl = -sum(t["pnl"] for t in trades if t["pnl"] < 0)
    net = sum(t["pnl"] for t in trades)
    pf = gp / gl if gl > 0 else None
    # 回撤（累计 pnl 曲线）
    cum, peak, mdd = 0.0, 0.0, 0.0
    for t in trades:
        cum += t["pnl"]
        peak = max(peak, cum)
        mdd = max(mdd, peak - cum)
    wr = len(wins) / len(trades) * 100
    avg = net / len(trades)
    print(f"  {label:<28} {len(trades):>3} 笔 胜率={wr:>5.1f}% 平均=${avg:>6.0f} "
          f"净=${net:>8.0f} PF={pf if pf else float('nan'):>5.2f} 最大连亏DD=${mdd:>7.0f} {extra}")


PERIODS = {"2025": ("2025-01-01", "2025-12-31"), "2026H1": ("2026-01-01", "2026-06-30")}


def main():
    frame = load_frame()
    qs = quadrant_signals(frame)
    funding = load_funding()
    print(f"30m bars={len(frame)} 象限信号={len(qs)} funding={len(funding)} 期")

    fade = {t: 1 if q == "long_kill" else -1 for t, q in qs.items()
            if q in ("long_kill", "short_squeeze")}
    follow = {t: 1 if q == "long_open" else -1 for t, q in qs.items()
              if q in ("long_open", "short_open")}
    fade_lk = {t: 1 for t, q in qs.items() if q == "long_kill"}
    fade_ss = {t: -1 for t, q in qs.items() if q == "short_squeeze"}

    for label, (frm, to) in PERIODS.items():
        print(f"\n== {label} ==")
        for name, ent in [("fade(杀多→多/逼空→空)", fade),
                          ("  仅 long_kill→多", fade_lk),
                          ("  仅 short_squeeze→空", fade_ss),
                          ("follow(增仓同向)", follow)]:
            for h in (4, 8, 16):
                stat(bt_horizon(frame, ent, h, frm, to), f"{name} H={h}")
            stat(bt_tpsl(frame, ent, 0.01, 0.01, frm, to), f"{name} TP/SL1%")

        # funding 极端 fade：结算时刻后第一根 30m 入场，持有到下次结算（16 柱）
        for f_thr in (0.0003, 0.0005):
            ent = {}
            fmap = {t: r for t, r in funding}
            for (t, o, h_, l, c, oi) in frame:
                # 找最近一次结算费率
                ft = t - (t % (8 * 3600_000))
                r = fmap.get(ft)
                if r is None:
                    continue
                if r >= f_thr:
                    ent[t] = -1  # 费率极高 → 空
                elif r <= -f_thr:
                    ent[t] = 1
            stat(bt_horizon(frame, ent, 16, frm, to), f"funding≥{f_thr*100:.2f}% fade H=16")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "fetch":
        fetch30m()
    else:
        main()
