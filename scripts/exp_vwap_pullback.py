#!/usr/bin/env python3
"""VWAP 回归 / 趋势回踩信号族的双期证伪实验。

数据使用 Binance Futures 5m K 线缓存。所有入场均在信号确认后的下一根 open，
同一根同时触及止损与止盈时按止损优先；每次往返扣 10bps（taker+滑点保守口径）。
参数只做小型、预先声明的网格，并分别报告 2025 与 2026H1，避免合并期掩盖失效。
"""

import csv
import datetime as dt
from dataclasses import dataclass
from pathlib import Path


DATA = Path(__file__).resolve().parents[1] / "data/exp/btc_5m.csv"
ROUND_TRIP_COST = 0.0010


def ms(s: str) -> int:
    return int(dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)


@dataclass
class Bar:
    ts: int
    o: float
    h: float
    l: float
    c: float
    volume: float
    atr: float = 0.0
    vwap24: float = 0.0
    ema1h_fast: float = 0.0
    ema1h_slow: float = 0.0


def load() -> list[Bar]:
    bars = []
    with DATA.open() as f:
        for r in csv.DictReader(f):
            bars.append(Bar(int(r["open_time"]), float(r["open"]), float(r["high"]),
                            float(r["low"]), float(r["close"]), float(r["volume"])))
    bars.sort(key=lambda b: b.ts)

    # Wilder ATR(14), rolling 24h VWAP (288 x 5m), closed-hour EMA20/EMA50.
    atr = None
    pv = vol = 0.0
    window = []
    hour = None
    hour_close = None
    e20 = e50 = None
    prev = None
    for b in bars:
        if prev is not None:
            tr = max(b.h - b.l, abs(b.h - prev), abs(b.l - prev))
            atr = tr if atr is None else (atr * 13 + tr) / 14
        b.atr = atr or (b.h - b.l)
        prev = b.c

        typical = (b.h + b.l + b.c) / 3
        item = (typical * b.volume, b.volume)
        window.append(item); pv += item[0]; vol += item[1]
        if len(window) > 288:
            old = window.pop(0); pv -= old[0]; vol -= old[1]
        b.vwap24 = pv / vol if vol else b.c

        hidx = b.ts // 3_600_000
        if hour is None:
            hour, hour_close = hidx, b.c
        elif hidx != hour:
            e20 = hour_close if e20 is None else hour_close * 2 / 21 + e20 * 19 / 21
            e50 = hour_close if e50 is None else hour_close * 2 / 51 + e50 * 49 / 51
            hour, hour_close = hidx, b.c
        else:
            hour_close = b.c
        b.ema1h_fast = e20 or b.c
        b.ema1h_slow = e50 or b.c
    return bars


def simulate(bars, start, end, family, threshold, reclaim, stop_atr, max_bars):
    trades = []
    pos = None
    pending = None
    extreme = 0.0
    cooldown = 0
    locked = False
    for i, b in enumerate(bars):
        if not (start <= b.ts < end):
            continue
        if cooldown:
            cooldown -= 1
        if pending is not None:
            side, target, sig_ts = pending
            entry = b.o
            stop = entry - side * stop_atr * b.atr
            pos = [side, entry, stop, target, i + max_bars, sig_ts]
            pending = None

        if pos is not None:
            side, entry, stop, target, deadline, sig_ts = pos
            stop_hit = b.l <= stop if side == 1 else b.h >= stop
            target_hit = b.h >= target if side == 1 else b.l <= target
            if stop_hit:
                exit_px, why = stop, "stop"
            elif target_hit:
                exit_px, why = target, "target"
            elif i >= deadline:
                exit_px, why = b.c, "time"
            else:
                continue
            ret = side * (exit_px / entry - 1) - ROUND_TRIP_COST
            risk = abs(entry - stop) / entry
            trades.append((sig_ts, ret, ret / risk if risk else 0.0, why))
            pos = None; cooldown = 12; extreme = 0.0
            continue

        if cooldown or i + 1 >= len(bars) or b.atr <= 0:
            continue

        dev_atr = (b.c - b.vwap24) / b.atr
        trend = 1 if b.ema1h_fast > b.ema1h_slow else -1
        # 一次偏离只允许一笔；必须回到 VWAP 后才能重新武装，避免在同一条腿上刷单。
        if locked:
            if abs(dev_atr) < 0.10:
                locked = False
            else:
                continue
        if family == "vwap_reclaim":
            # 先偏离，再至少回收 reclaim*ATR；目标为信号时的滚动 VWAP。
            if dev_atr < 0:
                extreme = min(extreme, dev_atr)
                if extreme <= -threshold and dev_atr - extreme >= reclaim:
                    pending = (1, b.vwap24, b.ts); extreme = 0.0; locked = True
            elif dev_atr > 0:
                extreme = max(extreme, dev_atr)
                if extreme >= threshold and extreme - dev_atr >= reclaim:
                    pending = (-1, b.vwap24, b.ts); extreme = 0.0; locked = True
            else:
                extreme = 0.0
        else:
            # 1h EMA20/50 定方向；价格穿越 VWAP 后重新站回，作为趋势回踩确认。
            if trend == 1:
                extreme = min(extreme, dev_atr)
                if extreme <= -threshold and dev_atr >= reclaim:
                    pending = (1, max(b.vwap24 + b.atr, b.c + b.atr), b.ts); extreme = 0.0; locked = True
            else:
                extreme = max(extreme, dev_atr)
                if extreme >= threshold and dev_atr <= -reclaim:
                    pending = (-1, min(b.vwap24 - b.atr, b.c - b.atr), b.ts); extreme = 0.0; locked = True
    return trades


def metrics(xs):
    n = len(xs); rets = [x[1] for x in xs]
    gp = sum(x for x in rets if x > 0); gl = -sum(x for x in rets if x < 0)
    eq = peak = dd = 0.0
    for x in rets:
        eq += x; peak = max(peak, eq); dd = max(dd, peak - eq)
    return {"n": n, "net_pct": sum(rets) * 100, "pf": gp / gl if gl else 99.0,
            "win_pct": (sum(x > 0 for x in rets) / n * 100 if n else 0), "dd_pct": dd * 100}


def daily_trend(bars, fast, slow, long_short, start, end, leverage=0.5):
    daily = {}
    for b in bars:
        day = dt.datetime.fromtimestamp(b.ts / 1000, dt.timezone.utc).date()
        daily.setdefault(day, [b.ts, b.o, b.c])[2] = b.c
    ef = es = None
    enriched = []
    for day, (ts, op, close) in sorted(daily.items()):
        ef = close if ef is None else close * 2 / (fast + 1) + ef * (fast - 1) / (fast + 1)
        es = close if es is None else close * 2 / (slow + 1) + es * (slow - 1) / (slow + 1)
        enriched.append((day, close, ef, es))
    equity = peak = 1.0
    dd = 0.0; pos = 0; turns = 0; prev = None
    for day, close, ef, es in enriched:
        if not (start <= day < end):
            continue
        if prev is not None:
            equity *= 1 + pos * leverage * (close / prev - 1)
        want = 1 if ef > es else (-1 if long_short else 0)
        if want != pos:
            equity *= 1 - abs(want - pos) * leverage * 0.0007
            turns += abs(want - pos)
            pos = want
        peak = max(peak, equity); dd = max(dd, 1 - equity / peak); prev = close
    return {"net_pct": (equity - 1) * 100, "dd_pct": dd * 100, "turns": turns}


def main():
    bars = load()
    periods = [("2025", ms("2025-01-01"), ms("2026-01-01")),
               ("2026H1", ms("2026-01-01"), ms("2026-07-01"))]
    rows = []
    for family in ("vwap_reclaim", "trend_pullback"):
        for threshold in (1.0, 1.5, 2.0, 2.5):
            for reclaim in (0.25, 0.5, 0.75):
                for stop_atr in (1.0, 1.5, 2.0):
                    out = [metrics(simulate(bars, a, b, family, threshold, reclaim, stop_atr, 72))
                           for _, a, b in periods]
                    rows.append((family, threshold, reclaim, stop_atr, out))

    def acceptable(row):
        a, b = row[-1]
        return min(a["n"], b["n"]) >= 10 and min(a["net_pct"], b["net_pct"]) > 0 and min(a["pf"], b["pf"]) > 1.1

    good = [r for r in rows if acceptable(r)]
    good.sort(key=lambda r: min(r[-1][0]["net_pct"], r[-1][1]["net_pct"]), reverse=True)
    print(f"bars={len(bars)} grid={len(rows)} 双期通过={len(good)} cost={ROUND_TRIP_COST*1e4:.0f}bps")
    for family in ("vwap_reclaim", "trend_pullback"):
        print(f"\n== {family}: 按双期较差净收益排序（前 10）==")
        subset = [r for r in good if r[0] == family][:10]
        if not subset:
            print("无配置通过")
        for _, th, rec, sl, out in subset:
            s = " | ".join(f"{name} n={m['n']} net={m['net_pct']:+.2f}% PF={m['pf']:.2f} win={m['win_pct']:.1f}% DD={m['dd_pct']:.2f}%"
                           for (name, _, _), m in zip(periods, out))
            print(f"thr={th:.2f}ATR reclaim={rec:.2f}ATR stop={sl:.1f}ATR | {s}")
        if not subset:
            near = [r for r in rows if r[0] == family]
            near.sort(key=lambda r: min(r[-1][0]["net_pct"], r[-1][1]["net_pct"]), reverse=True)
            print("最接近通过的 5 组：")
            for _, th, rec, sl, out in near[:5]:
                s = " | ".join(f"{name} n={m['n']} net={m['net_pct']:+.2f}% PF={m['pf']:.2f} win={m['win_pct']:.1f}% DD={m['dd_pct']:.2f}%"
                               for (name, _, _), m in zip(periods, out))
                print(f"thr={th:.2f}ATR reclaim={rec:.2f}ATR stop={sl:.1f}ATR | {s}")

    print("\n日线 EMA 趋势需要 2025 之前的预热数据；请运行 exp_daily_trend.py，"
          "不要用本文件的 2025 起始 K 线评价慢速 EMA。")


if __name__ == "__main__":
    main()
