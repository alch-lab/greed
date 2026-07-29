#!/usr/bin/env python3
"""V9 吸收（absorption）过滤器假设验证（2026-07-29）。

V9 唯一的新元素：极端偏离事件中，"高 delta 低位移"（吸收）比
"低 delta 高位移"（动能）更可能反转。若成立，可作为 MR 入场质量过滤器
（不是新策略——V9 框架本身 = 我们 MR 的工作机制）。

数据：币安 5m K线（含 taker_buy_volume → delta 代理），2025 + 2026H1，
缓存 data/exp/btc_5m.csv。taker 口径效率 eff = |close-open| / |delta_usd|。

用法：~/miniconda3/bin/python3 scripts/exp_v9.py
"""
import csv
import datetime as dt
import json
import time
import urllib.request
from pathlib import Path

import numpy as np

CACHE = Path("/Users/wonder/Code/greed/data/exp/btc_5m.csv")


def ms_of(s):
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)


def fetch():
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    end = ms_of("2026-07-01")
    cur = ms_of("2025-01-01")
    if CACHE.exists():
        with open(CACHE) as f:
            last = None
            for r in csv.DictReader(f):
                last = int(r["open_time"])
        if last:
            cur = last + 300000
            print(f"resume from {cur}")
    mode = "a" if CACHE.exists() else "w"
    with open(CACHE, mode, newline="") as f:
        w = csv.writer(f)
        if mode == "w":
            w.writerow(["open_time", "open", "high", "low", "close", "volume", "taker_buy"])
        n = 0
        while cur < end:
            url = (f"https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m"
                   f"&startTime={cur}&endTime={end}&limit=1500")
            data = None
            for attempt in range(8):
                try:
                    with urllib.request.urlopen(url, timeout=30) as r:
                        data = json.load(r)
                    break
                except Exception as e:
                    print(f"retry {attempt + 1}: {e}", flush=True)
                    time.sleep(3)
            if data is None:
                print(f"giving up at {cur}（已缓存可断点续跑）")
                return
            if not data:
                break
            for k in data:
                w.writerow([k[0], k[1], k[2], k[3], k[4], k[5], k[9]])
            f.flush()
            n += len(data)
            cur = data[-1][0] + 300000
            time.sleep(0.25)
        print(f"this run fetched {n} bars")


def load():
    rows = []
    with open(CACHE) as f:
        for r in csv.DictReader(f):
            rows.append((int(r["open_time"]), float(r["open"]), float(r["high"]),
                         float(r["low"]), float(r["close"]),
                         float(r["volume"]), float(r["taker_buy"])))
    rows.sort()
    return rows


def main():
    if not CACHE.exists():
        fetch()
    ks = load()
    print("bars:", len(ks))
    ema = None
    rows = []
    for (t, o, h, l, c, vol, tb) in ks:
        ema = c if ema is None else c * (2 / 21) + ema * (19 / 21)
        rows.append((t, o, c, vol * c, (2 * tb - vol) * c, ema))

    def test(frm, to, dev_thr):
        evs = []
        for i, (t, o, c, vu, du, e) in enumerate(rows):
            if not (ms_of(frm) <= t <= ms_of(to) + 86399999):
                continue
            dev = (c - e) / e
            if abs(dev) < dev_thr or i + 12 >= len(rows):
                continue
            d = 1 if dev < 0 else -1
            eff = abs(c - o) / max(abs(du), 1e-9)
            fwd = d * (rows[i + 12][2] - c) / c * 1e4
            evs.append((eff, fwd))
        if not evs:
            print(f"{frm}~{to} dev≥{dev_thr * 100:.1f}%: 无事件")
            return
        med = float(np.median([e[0] for e in evs]))
        lo = [e for e in evs if e[0] <= med]
        hi = [e for e in evs if e[0] > med]

        def s(xs, label):
            a = np.array([x[1] for x in xs])
            t = a.mean() / (a.std() / np.sqrt(len(a))) if len(a) > 1 and a.std() > 0 else 0
            print(f"  {label}: n={len(a):>4} 反转均值={a.mean():>+7.1f}bps "
                  f"中位={np.median(a):>+7.1f} 胜率={(a > 0).mean():.1%} t={t:>+5.2f}")

        print(f"{frm}~{to} |dev|≥{dev_thr * 100:.1f}% 事件 {len(evs)} 个（eff 中位={med:.4f}）")
        s(lo, "低效率组（吸收）")
        s(hi, "高效率组（动能）")

    test("2025-01-01", "2025-12-31", 0.010)
    test("2026-01-01", "2026-06-30", 0.010)
    print("\n== MR 实际出手区（|dev|≥1.5%）==")
    test("2025-01-01", "2025-12-31", 0.015)
    test("2026-01-01", "2026-06-30", 0.015)


if __name__ == "__main__":
    main()
