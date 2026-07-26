#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json, glob, sys

dir_path = sys.argv[1] if len(sys.argv) > 1 else "out/ema-scan/h1_20_50_1pct_1h"

months = []
for m in range(1, 13):
    mm = f"{m:02d}"
    files = glob.glob(f"{dir_path}/m{mm}.json")
    if not files:
        continue
    with open(files[0]) as f:
        d = json.load(f)
    o = d.get("overall", {})
    months.append({
        "month": f"2025-{mm}",
        "trades": o.get("n", 0),
        "win_rate": o.get("winrate", 0) * 100,
        "profit": o.get("gross_profit", 0) - o.get("gross_loss", 0) - o.get("total_fees", 0),
        "max_dd": d.get("max_drawdown_pct", 0) * 100,
        "sharpe": d.get("sharpe", 0),
        "expectancy": o.get("expectancy", 0),
        "pf": o.get("profit_factor", 0),
    })

if not months:
    print("No results found")
    sys.exit(1)

total_trades = sum(m["trades"] for m in months)
total_wins = sum(m["trades"] * m["win_rate"]/100 for m in months)
avg_wr = total_wins / total_trades * 100 if total_trades > 0 else 0
total_profit = sum(m["profit"] for m in months)

print("=" * 90)
print(f"{'月份':>10} {'交易':>6} {'胜率%':>7} {'盈亏$':>10} {'回撤%':>7} {'Sharpe':>7} {'PF':>6}")
print("-" * 90)
for m in months:
    print(f"{m['month']:>10} {m['trades']:>6} {m['win_rate']:>7.1f} {m['profit']:>10.2f} {m['max_dd']:>7.1f} {m['sharpe']:>7.2f} {m['pf']:>6.2f}")
print("-" * 90)
print(f"{'全年合计':>10} {total_trades:>6} {avg_wr:>7.1f} {total_profit:>10.2f}")
print(f"\n全年总盈亏: ${total_profit:.2f}  (本金 $100K/月)")
print(f"月度平均交易: {total_trades/len(months):.1f} 笔")
print(f"月度平均胜率: {avg_wr:.1f}%")
