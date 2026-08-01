#!/usr/bin/env python3
"""跨所资金费套利空间实验（Hyperliquid vs 币安，2026-08-01）。

方法：
1. HL metaAndAssetCtxs → 全币种当前费率 + 持仓额（OI），剔除 BTC/稳定币，
   取 |当前费率| 最高的 ~20 个山寨币（OI > $3M 保证基本容量）。
2. 对候选币拉两边近 7 天资金费历史：
   - HL fundingHistory（每小时一条）
   - 币安 data.binance.vision 月度 fundingRate CSV（每 8h 一条）
3. 日均费率差 → 年化毛套利空间；扣双边进出成本（perp-perp 17bps /
   spot-perp 29bps 两档）算回本周期。
输出：data/funding_spread.json + 终端表格。
"""
import io
import json
import time
import urllib.request
import zipfile
from statistics import mean

NOW_MS = int(time.time() * 1000)
# 分析窗口 = 2026-06 整月（币安 dump 只发布已完结月份；30 天样本也更稳）
START_MS = 1780272000000          # 2026-06-01 00:00 UTC
END_MS = 1782950400000            # 2026-07-01 00:00 UTC
WINDOW_MS = END_MS - START_MS
UA = {"User-Agent": "greed-research/1.0", "Content-Type": "application/json"}


def post_json(url, payload, timeout=15):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "greed-research/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def hl_daily_avg(coin):
    # 分 5 天一段拉（防单段记录数上限）
    rates = []
    chunk = 5 * 86_400_000
    t = START_MS
    while t < END_MS:
        rows = post_json("https://api.hyperliquid.xyz/info",
                         {"type": "fundingHistory", "coin": coin, "startTime": t})
        rates.extend(float(r["fundingRate"]) for r in rows
                     if START_MS <= r["time"] < END_MS)
        got_max = max((r["time"] for r in rows), default=t)
        t = max(t + chunk, got_max + 1)
        time.sleep(0.15)
    if not rates:
        return None, 0
    # 小时费率 → 日均 = mean × 24
    return mean(rates) * 24, len(rates)


def binance_daily_avg(symbol):
    """月度 CSV：calc_time,funding_interval_hours,last_funding_rate → 日均 = sum(费率)/天数"""
    ym = "2026-06"
    url = (f"https://data.binance.vision/data/futures/um/monthly/fundingRate/"
           f"{symbol}/{symbol}-fundingRate-{ym}.zip")
    try:
        blob = get(url)
    except Exception:
        return None, 0
    zf = zipfile.ZipFile(io.BytesIO(blob))
    name = zf.namelist()[0]
    rows = []
    for line in zf.read(name).decode().splitlines()[1:]:
        parts = line.split(",")
        if len(parts) >= 3:
            rows.append((int(parts[0]), float(parts[2])))
    recent = [r for ts, r in rows if START_MS <= ts < END_MS]
    if recent:
        days = WINDOW_MS / 86_400_000
        return sum(recent) / days, len(recent)
    return None, 0


def main():
    meta, ctxs = post_json("https://api.hyperliquid.xyz/info", {"type": "metaAndAssetCtxs"})
    cands = []
    for i, a in enumerate(meta["universe"]):
        name = a["name"]
        if name in ("BTC", "USDT", "USDC", "USDH", "PURR") or name.startswith("@"):
            continue
        c = ctxs[i]
        oi_usd = float(c["openInterest"]) * float(c["markPx"])
        fr_now = float(c["funding"])
        if oi_usd >= 3_000_000:
            cands.append((name, abs(fr_now), fr_now, oi_usd))
    cands.sort(key=lambda x: -x[1])
    top = cands[:20]
    print(f"候选 {len(top)} 个（按 HL 当前|费率|排序，OI≥$3M）\n")

    results = []
    for name, _, fr_now, oi in top:
        sym = f"{name}USDT"
        hl_avg, hl_n = hl_daily_avg(name)
        bn_avg, bn_n = binance_daily_avg(sym)
        row = {"coin": name, "oi_usd": round(oi), "hl_now_pct_h": fr_now * 100,
               "hl_daily_avg": hl_avg, "hl_samples": hl_n,
               "bn_daily_avg": bn_avg, "bn_samples": bn_n}
        if hl_avg is not None and bn_avg is not None:
            # 套利：做空费率高的一方、做多低的一方（正 spread = 空 HL 多币安）
            spread_daily = hl_avg - bn_avg
            row["spread_daily_pct"] = spread_daily * 100
            row["spread_annual_pct"] = spread_daily * 365 * 100
            for label, cost_bps in (("perp_perp", 17), ("spot_perp", 29)):
                be = (cost_bps / 100) / (abs(spread_daily) * 100) if spread_daily else None
                row[f"breakeven_days_{label}"] = round(be, 1) if be else None
        results.append(row)
        time.sleep(0.3)  # 礼貌限速

    out = {"generated_at": NOW_MS, "window": "2026-06", "results": results}
    with open("/Users/wonder/Code/greed/data/funding_spread.json", "w") as f:
        json.dump(out, f, indent=1)

    hdr = f"{'币种':<8}{'HL日费率%':>10}{'币安日费率%':>11}{'差(bp/日)':>10}{'年化%':>8}{'回本(天)':>9}{'OI($M)':>9}"
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        if r.get("spread_daily_pct") is None:
            print(f"{r['coin']:<8}  数据缺失（HL样本{r['hl_samples']} 币安样本{r['bn_samples']}）")
            continue
        print(f"{r['coin']:<8}{r['hl_daily_avg']*100:>10.4f}{r['bn_daily_avg']*100:>11.4f}"
              f"{r['spread_daily_pct']*100:>10.2f}{r['spread_annual_pct']:>8.1f}"
              f"{r['breakeven_days_perp_perp'] or 0:>9.1f}{r['oi_usd']/1e6:>9.1f}")


if __name__ == "__main__":
    main()
