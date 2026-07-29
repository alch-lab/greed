#!/usr/bin/env python3
"""成交口径敏感性测试（2026-07-29）——评审 #1 信息泄漏的量化。

v3-1.py 的 trade_v7 假设在反转砖极值 ±tick 成交（理想口径，含未来信息）。
本脚本复用其信号与结构过滤（≥3连反转 + 顺势+位置严，预注册最终档），
对比三种成交口径：
  ideal     : 反转砖极值 ± tick（原口径，含未来信息 = 收益上界）
  close     : 信号砖收盘价成交（砖完成后立即市价单）
  next_open : 下一砖开盘价成交（最保守砖级模拟）

用法（需 pandas）：
  ~/miniconda3/bin/python3 scripts/exp_fill_model.py
"""
import importlib.util
import numpy as np

spec = importlib.util.spec_from_file_location(
    "v31", "/Users/wonder/Code/greed/scripts/v3-1.py")
v31 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v31)

SL_BUF = 300.0
TTL = 20
TICK = 0.5
FEE_BPS, SLIP_BPS = 4.0, 2.0


def trade(df, i0, fill):
    d = int(df.at[i0, 'dir'])
    hi, lo = df.at[i0, 'high'], df.at[i0, 'low']
    # limit5：砖完成后在极值挂限价单，等 5 砖内回踩成交（诚实版 ideal）
    start = i0 + 1
    if fill == 'limit5':
        px = lo + TICK if d == 1 else hi - TICK
        filled_at = None
        for j in range(i0 + 1, min(i0 + 6, len(df))):
            if (d == 1 and df.at[j, 'low'] <= px) or (d == -1 and df.at[j, 'high'] >= px):
                filled_at = j
                break
        if filled_at is None:
            return None
        entry = px
        start = filled_at
    elif d == 1:
        entry = {'ideal': lo + TICK, 'close': df.at[i0, 'close'],
                 'next_open': df.at[i0 + 1, 'open']}[fill]
    else:
        entry = {'ideal': hi - TICK, 'close': df.at[i0, 'close'],
                 'next_open': df.at[i0 + 1, 'open']}[fill]
    sl = lo - SL_BUF if d == 1 else hi + SL_BUF
    risk = abs(entry - sl)
    if risk <= 0 or i0 + 1 >= len(df):
        return None
    end = min(start + TTL, len(df) - 1)
    if end <= start:
        return None
    pos, realized, peak = 1.0, 0.0, entry
    tp1_done, trail_stop = False, sl
    for j in range(start, end + 1):
        bh, bl, bc = df.at[j, 'high'], df.at[j, 'low'], df.at[j, 'close']
        peak = max(peak, bh) if d == 1 else min(peak, bl)
        gain = (peak - entry) if d == 1 else (entry - peak)
        if gain / risk >= 1.5:
            ns = (peak - 1.5 * risk) if d == 1 else (peak + 1.5 * risk)
            trail_stop = max(trail_stop, ns) if d == 1 else min(trail_stop, ns)
        if d == 1 and bl <= trail_stop:
            realized += pos * ((trail_stop - entry) / risk)
            pos = 0
            break
        if d == -1 and bh >= trail_stop:
            realized += pos * ((entry - trail_stop) / risk)
            pos = 0
            break
        cg = (bc - entry) if d == 1 else (entry - bc)
        if (not tp1_done) and cg / risk >= 1.0:
            realized += 0.5 * (cg / risk)
            pos -= 0.5
            tp1_done = True
    if pos > 0:
        final = df.at[end, 'close']
        realized += pos * (((final - entry) if d == 1 else (entry - final)) / risk)
    cost = (FEE_BPS + SLIP_BPS) / 1e4 * entry / risk
    return realized - cost


def run_month(path):
    df = v31.load(path)
    df = v31.add_structure(df, trend_win=200, band_win=500)
    sig = v31.base_signals(df, 3)
    sel = [i for i in sig
           if int(df.at[i, 'dir']) == int(df.at[i, 'trend_dir'])
           and df.at[i, 'trend_dir'] != 0
           and ((df.at[i, 'dir'] == 1 and df.at[i, 'band_pos'] <= 0.382)
                or (df.at[i, 'dir'] == -1 and df.at[i, 'band_pos'] >= 0.618))]
    out = {}
    for fill in ('ideal', 'close', 'next_open', 'limit5'):
        rs = [trade(df, i, fill) for i in sel]
        rs = [x for x in rs if x is not None]
        if not rs:
            out[fill] = (0, 0, 0.0, 0.0)
            continue
        w = sum(1 for x in rs if x > 0)
        gp = sum(x for x in rs if x > 0)
        gl = -sum(x for x in rs if x < 0)
        out[fill] = (len(rs), w / len(rs), float(np.mean(rs)),
                     gp / gl if gl > 1e-9 else float('inf'))
    return out


MONTHS = [("2025-10", "/Users/wonder/Code/greed/out/renko-2025-10-100_62-bricks.csv")]
MONTHS += [(f"2026-{m:02d}",
            f"/Users/wonder/Code/greed/out/renko-2026-{m:02d}-100_62-bricks.csv")
           for m in range(1, 7)]

print(f"{'月份':<9}{'口径':<10}{'笔数':>5}{'胜率':>8}{'净meanR':>9}{'净PF':>7}")
tot = {f: [] for f in ('ideal', 'close', 'next_open', 'limit5')}
for label, path in MONTHS:
    r = run_month(path)
    for fill in ('ideal', 'close', 'next_open', 'limit5'):
        n, wr, mr, pf = r[fill]
        print(f"{label:<9}{fill:<10}{n:>5}{wr:>8.1%}{mr:>9.3f}{pf:>7.2f}")
    for fill in tot:
        pass

# 汇总（2026 H1 加权）
print("\n== 2026 H1 加权 ==")
agg = {f: [] for f in ('ideal', 'close', 'next_open', 'limit5')}
for label, path in MONTHS[1:]:
    r = run_month(path)
    for fill in agg:
        n, wr, mr, pf = r[fill]
        agg[fill].append((n, mr))
for fill, xs in agg.items():
    n = sum(x[0] for x in xs)
    mr = sum(x[0] * x[1] for x in xs) / n if n else 0
    print(f"{fill:<10} 总笔数={n:>4}  加权净meanR={mr:+.3f}")
