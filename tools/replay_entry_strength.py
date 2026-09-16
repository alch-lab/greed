#!/usr/bin/env python3
"""Replay observed fills under the shared entry-strength risk budget.

This is a sizing counterfactual, not a signal rediscovery backtest: it keeps the
historical entries/exits and scales their realized net PnL by the risk budget
that the new causal, entry-time classifier would have assigned.
"""

import argparse
import datetime
import json
import math
from collections import Counter, defaultdict


def number(context, key):
    try:
        value = float(context.get(key))
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def score(entry):
    context = entry.get("signal_context") or {}
    side = 1.0 if entry.get("side") == "buy" else -1.0
    recipe = entry.get("recipe")
    checks = []

    def check(key, predicate, aligned=False):
        value = number(context, key)
        if value is not None:
            checks.append(predicate(value * side if aligned else value))

    if recipe == "fast_trend_activation":
        check("body_return_5m", lambda value: value >= 0.010, True)
        check("volume_ratio_5m", lambda value: value >= 3.0)
        check("directional_flow_5m", lambda value: value >= 0.30, True)
        check("market_breadth_1h", lambda value: value >= 0.60)
        check("market_return_1h", lambda value: value >= 0.0, True)
        check("prebreak_return_1h", lambda value: value <= 0.009, True)
        check("compression_ratio", lambda value: value <= 0.93)
        check("directional_extension", lambda value: value <= 0.0185)
        check("confirmation_flow_1m", lambda value: value >= 0.25, True)
        cutoffs = (0.68, 0.80)
    elif recipe == "liquidation_exhaustion_reversal":
        check("liquidation_depth_ratio_3s", lambda value: value >= 1.6)
        check("liquidation_aligned_return_bps_3s", lambda value: 14.0 <= value <= 27.5)
        check("liquidation_reversal_bps", lambda value: value <= 49.0)
        check("signal_age_ms", lambda value: value <= 5_000.0)
        check("live_1m_opposing_body_pct", lambda value: value <= 0.012)
        check("live_1m_opposing_flow", lambda value: value <= 0.117)
        cutoffs = (0.50, 0.70)
    elif recipe == "trend_continuation":
        check("trend_efficiency", lambda value: value >= 0.50)
        check("trend_extension_atr", lambda value: value <= 2.5)
        check("trend_reclaim_body_atr", lambda value: value >= 0.8)
        check("market_directional_breadth", lambda value: value >= 0.70)
        check("trend_age_bars", lambda value: value <= 1.0)
        cutoffs = (0.60, 0.80)
    else:
        return "unchanged", 1.0
    value = sum(checks) / len(checks) if checks else 0.5
    strength = "conviction" if value >= cutoffs[1] else "confirmed" if value >= cutoffs[0] else "probe"
    new_risk = {"probe": 0.00075, "confirmed": 0.0015, "conviction": 0.003}[strength]
    old_risk = number(context, "risk_per_trade_pct") or {
        "fast_trend_activation": 0.002,
        "liquidation_exhaustion_reversal": 0.0025,
        "trend_continuation": 0.015,
    }[recipe]
    return strength, new_risk / old_risk


def day(timestamp_ms):
    zone = datetime.timezone(datetime.timedelta(hours=8))
    return datetime.datetime.fromtimestamp(timestamp_ms / 1_000, zone).strftime("%m-%d")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("history", nargs="+", help="alpha-history JSONL files, oldest first")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    entries, exits = {}, {}
    for path in args.history:
        with open(path, encoding="utf-8") as source:
            for line in source:
                event = json.loads(line)
                payload = event.get("payload") or {}
                if payload.get("run_id") != args.run_id or not payload.get("candidate_id"):
                    continue
                if event.get("kind") == "exchange_entry":
                    entries[payload["candidate_id"]] = payload
                elif event.get("kind") == "exchange_exit":
                    exits[payload["candidate_id"]] = payload
    rows = []
    for candidate_id, entry in entries.items():
        if candidate_id not in exits:
            continue
        exit_event = exits[candidate_id]
        strength, multiplier = score(entry)
        pnl = float(exit_event.get("pnl_usd") or 0.0)
        rows.append((day(int(exit_event["ts_ms"])), entry["recipe"], strength, pnl, pnl * multiplier))
    windows = [
        ("calibration", {"09-09", "09-10"}),
        ("validation-1", {"09-11", "09-12"}),
        ("validation-2", {"09-13", "09-14"}),
        ("validation-3", {"09-15", "09-16"}),
        ("all", {row[0] for row in rows}),
    ]
    for label, days in windows:
        selected = [row for row in rows if row[0] in days]
        base = sum(row[3] for row in selected)
        replay = sum(row[4] for row in selected)
        print(f"{label:12} n={len(selected):3d} observed={base:9.2f} replay={replay:9.2f} delta={replay-base:9.2f}")
        by_recipe = defaultdict(list)
        for row in selected:
            by_recipe[row[1]].append(row)
        for recipe, recipe_rows in sorted(by_recipe.items()):
            profiles = Counter(row[2] for row in recipe_rows)
            print(f"  {recipe:36} {sum(row[3] for row in recipe_rows):8.2f} -> {sum(row[4] for row in recipe_rows):8.2f} {dict(profiles)}")


if __name__ == "__main__":
    main()
