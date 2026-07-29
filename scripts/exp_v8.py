#!/usr/bin/env python3
"""V8 策略核心信号质量测试（2026-07-29）。

V8（修正版）的核心 alpha 声明：趋势环境中的订单流耗竭回调捕捉
（≥3连链 + 反转 + delta 背离 + 成交量高潮 + 下一砖确认）。

按证伪纪律，先测信号方向边际（诚实价格 = 确认砖 close），再谈出场：
  对每个信号计算其后 5/10/20 砖的前瞻收益（按信号方向签名），
  与全样本基线对比。均值若不超过费用线（~10-15bps），信号无 edge，
  任何出场设计都救不回来。

变体（层层加码，看订单流维度是否贡献边际）：
  S0 基线：所有 ≥3连链 + 反转 + 确认（无订单流条件）
  S1 ：S0 + delta 背离（链内价格推进但 delta 递减）
  S2 ：S1 + 成交量高潮（反转砖量 ≥ 链均 1.5×）
另外报告趋势过滤（EMA50>EMA200 4H 代理：砖级 200 砖净位移方向一致）。

用法：~/miniconda3/bin/python3 scripts/exp_v8.py
"""
import glob
import numpy as np
import pandas as pd

HORIZONS = (5, 10, 20)
FEE_LINE_BPS = 12.0  # taker 双边 + 滑点的量级


def load_all():
    files = sorted(glob.glob("/Users/wonder/Code/greed/out/renko-2025-*-100_62-bricks.csv")) + \
            sorted(glob.glob("/Users/wonder/Code/greed/out/renko-2026-*-100_62-bricks.csv"))
    frames = []
    for f in files:
        df = pd.read_csv(f)
        if df.empty:
            continue
        df["month"] = f.split("renko-")[1][:7]
        frames.append(df)
    return frames


def signals(df, min_chain=3, vol_mult=1.5):
    """返回 (idx, dir, s1_ok, s2_ok, trend_ok)。dir=1 多（下跌链后反转向上）。"""
    out = []
    closes = df["close"].values
    deltas = df["delta"].values
    vols = df["volume"].values
    rev = df["is_reversal"].values
    chain = df["chain_index"].values
    dirs = df["dir"].values
    n = len(df)
    for i in range(2, n - max(HORIZONS) - 2):
        if not rev[i] or chain[i - 1] < min_chain:
            continue
        d = int(dirs[i])
        # 确认：下一砖不破信号砖极值
        if d == 1 and df.at[i + 1, "low"] < df.at[i, "low"]:
            continue
        if d == -1 and df.at[i + 1, "high"] > df.at[i, "high"]:
            continue
        # 链内 delta 背离：价格朝链方向推进，但 delta 逐砖递减（取链末 3 砖）
        k = int(chain[i - 1])
        chain_d = deltas[max(0, i - k):i]
        s1 = False
        if len(chain_d) >= 3:
            # 下跌链（做多信号）：delta 应为负且递增收敛（卖方衰竭）
            if d == 1 and chain_d[-3] < chain_d[-2] < chain_d[-1]:
                s1 = True
            # 上涨链（做空信号）：delta 为正且递减（买方衰竭）
            if d == -1 and chain_d[-3] > chain_d[-2] > chain_d[-1]:
                s1 = True
        # 成交量高潮：反转砖量 ≥ 链均 vol_mult 倍
        cvol = vols[max(0, i - k):i]
        s2 = s1 and len(cvol) > 0 and vols[i] >= vol_mult * np.mean(cvol)
        # 趋势环境（砖级代理：200 砖净位移与信号方向一致）
        trend_ok = False
        if i >= 200:
            net = closes[i] - closes[i - 200]
            trend_ok = (d == 1 and net > 0) or (d == -1 and net < 0)
        out.append((i, d, s1, s2, trend_ok))
    return out


def fwd_returns(df, sigs):
    closes = df["close"].values
    res = {name: {h: [] for h in HORIZONS}
           for name in ("S0", "S1", "S2", "S1+趋势")}
    base = {h: [] for h in HORIZONS}
    n = len(df)
    # 基线：随机时点方向随机（用砖收益绝对分布的对照 = 0 漂移检验）
    for (i, d, s1, s2, trend_ok) in sigs:
        entry = closes[i + 1]  # 确认砖收盘 = 诚实可得价
        if entry <= 0:
            continue
        for h in HORIZONS:
            r = d * (closes[i + 1 + h] - entry) / entry * 1e4  # bps
            res["S0"][h].append(r)
            if s1:
                res["S1"][h].append(r)
                if trend_ok:
                    res["S1+趋势"][h].append(r)
            if s2:
                res["S2"][h].append(r)
    # 全时段漂移基线：所有砖的同跨度收益（带方向 = 0 期望的对照）
    step = max(1, n // 5000)
    for i in range(0, n - max(HORIZONS) - 1, step):
        for h in HORIZONS:
            base[h].append((closes[i + h] - closes[i]) / closes[i] * 1e4)
    return res, base


def stat(xs, h):
    if not xs:
        return "n=0"
    a = np.array(xs)
    t = a.mean() / (a.std() / np.sqrt(len(a))) if a.std() > 0 and len(a) > 1 else 0
    return f"n={len(a):>4} 均值={a.mean():>+7.1f}bps 中位={np.median(a):>+7.1f} t={t:>+5.2f}"


frames = load_all()
for df in frames:
    month = df["month"].iloc[0]
    sigs = signals(df)
    res, base = fwd_returns(df, sigs)
    print(f"\n===== {month}（信号 {len(sigs)} 个）=====")
    for name in ("S0", "S1", "S1+趋势", "S2"):
        line = "  " + name.ljust(7)
        for h in HORIZONS:
            line += f" | +{h}砖 {stat(res[name][h], h)}"
        print(line)
    line = "  基线(无方向)"
    for h in HORIZONS:
        b = np.array(base[h])
        line += f" | +{h}砖 均值={b.mean():>+6.1f} σ={b.std():>6.1f}    "
    print(line)
print(f"\n费用线参考：~{FEE_LINE_BPS}bps（taker 双边+滑点）；均值须显著超过此线才有戏")
