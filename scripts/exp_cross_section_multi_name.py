#!/usr/bin/env python3
"""Causal N-name replay for the production 12h/3h cross-section rule.

Uses closed 15m bars, next-open entries, equal-weight legs, 5bp taker fees and
5bp adverse slippage on both sides.  The configured 4% stop and 5%/33% partial
take-profit followed by a 2% trailing stop are applied inside each 3h cohort.
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone

BAR_MS = 15 * 60_000
DAY_MS = 24 * 3_600_000
EXCLUDED = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT", "SUIUSDT",
}


def ts(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def leg_return(rows, entry_i: int, side: int, hold_bars=12, stop=0.04,
               activation=0.05, trail=0.02, slip_bps=5.0) -> float | None:
    if entry_i + hold_bars >= len(rows):
        return None
    slip = slip_bps / 10_000
    fee = 0.0005
    entry = float(rows[entry_i][1]) * (1 + side * slip)
    stop_price = entry * (1 - side * stop)
    extreme = entry
    remaining = 1.0
    result = -fee
    partial = False

    def exit_fraction(raw: float, fraction: float) -> None:
        nonlocal result, remaining
        fill = raw * (1 - side * slip)
        result += remaining * fraction * (side * (fill / entry - 1) - fee * fill / entry)
        remaining *= 1 - fraction

    for row in rows[entry_i:entry_i + hold_bars]:
        op, high, low = map(float, (row[1], row[2], row[3]))
        hit = low <= stop_price if side > 0 else high >= stop_price
        if hit:
            gapped = op < stop_price if side > 0 else op > stop_price
            exit_fraction(op if gapped else stop_price, 1.0)
            return result
        favorable = high / entry - 1 if side > 0 else entry / low - 1
        if not partial and favorable >= activation:
            exit_fraction(entry * (1 + side * activation), 0.33)
            partial = True
            stop_price = max(stop_price, entry) if side > 0 else min(stop_price, entry)
        if side > 0:
            extreme = max(extreme, high)
            if partial:
                stop_price = max(stop_price, extreme * (1 - trail))
        else:
            extreme = min(extreme, low)
            if partial:
                stop_price = min(stop_price, extreme * (1 + trail))
    exit_fraction(float(rows[entry_i + hold_bars][1]), 1.0)
    return result


def build_series(data: dict, names: int, slip_bps: float) -> list[tuple[int, float, int, float]]:
    lookups = {}
    rows_by_symbol = {}
    for symbol, rows in data.items():
        if symbol in EXCLUDED or not symbol.endswith("USDT") or len(rows) < 100:
            continue
        rows_by_symbol[symbol] = rows
        lookups[symbol] = {int(row[0]): i for i, row in enumerate(rows)}
    all_times = sorted({int(row[0]) for rows in rows_by_symbol.values() for row in rows})
    boundaries = [value for value in all_times if value % (3 * 3_600_000) == 0]
    output = []
    for boundary in boundaries:
        ranks = []
        signal_ts = boundary - BAR_MS
        back_ts = boundary - 12 * 3_600_000 - BAR_MS
        for symbol, lookup in lookups.items():
            signal_i, back_i = lookup.get(signal_ts), lookup.get(back_ts)
            if signal_i is None or back_i is None or signal_i < 95:
                continue
            rows = rows_by_symbol[symbol]
            back = float(rows[back_i][4])
            volume = sum(float(row[7]) for row in rows[signal_i - 95:signal_i + 1])
            value = float(rows[signal_i][4]) / back - 1 if back > 0 else 99.0
            if volume >= 10_000_000 and abs(value) <= 1.5:
                ranks.append((value, symbol, signal_i + 1))
        ranks.sort()
        if len(ranks) < names:
            continue
        median = ranks[len(ranks) // 2][0]
        selected = list(reversed(ranks[-names:])) if median >= 0.01 else ranks[:names]
        side = 1 if median >= 0.01 else -1
        if any((side > 0 and item[0] <= 0) or (side < 0 and item[0] >= 0) for item in selected):
            continue
        returns = [leg_return(rows_by_symbol[symbol], entry_i, side, slip_bps=slip_bps)
                   for _, symbol, entry_i in selected]
        if any(value is None for value in returns):
            continue
        excess = abs(selected[0][0] - median)
        output.append((boundary, sum(returns) / names, names, excess))
    return output


def evaluate(series, start: int, end: int, gross: float, dynamic=False,
             dynamic_tiers=(0.05, 0.40, 0.50)):
    equity = peak = 1000.0
    drawdown = 0.0
    baskets = legs = wins = 0
    fees_and_slippage_included = True
    daily = defaultdict(lambda: {"entries": 0, "pnl": 0.0})
    day_start = equity
    current_day = None
    history = []
    for signal_ts, basket_return, count, excess in series:
        past = history[-30:]
        gains = sum(max(value, 0) for value in past)
        losses = sum(max(-value, 0) for value in past)
        profit_factor = gains / losses if losses else 99.0
        gate = len(past) == 30 and sum(past) > 0 and profit_factor >= 1.30
        base_gross, active_gross, strong_gross = dynamic_tiers
        active_gross = (strong_gross if excess >= 0.12 else active_gross) if gate else base_gross
        history.append(basket_return)
        if not start <= signal_ts < end:
            continue
        day = signal_ts // DAY_MS
        if day != current_day:
            current_day, day_start = day, equity
        if equity < day_start * 0.96:
            continue
        before = equity
        equity *= max(0.01, 1 + basket_return * (active_gross if dynamic else gross))
        pnl = equity - before
        daily[day]["entries"] += count
        daily[day]["pnl"] += pnl
        baskets += 1
        legs += count
        wins += basket_return > 0
        peak = max(peak, equity)
        drawdown = max(drawdown, 1 - equity / peak)
    active_days = sum(item["entries"] > 0 for item in daily.values())
    return {
        "pnl": equity - 1000,
        "return_pct": (equity / 1000 - 1) * 100,
        "max_drawdown_pct": drawdown * 100,
        "baskets": baskets,
        "entries": legs,
        "entries_per_active_day": legs / active_days if active_days else 0,
        "basket_win_rate_pct": wins / baskets * 100 if baskets else 0,
        "costs_included": fees_and_slippage_included,
        "daily": [{"date": datetime.fromtimestamp(day * DAY_MS / 1000, timezone.utc).date().isoformat(), **value}
                  for day, value in sorted(daily.items()) if value["entries"]],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/private/tmp/binance-testnet-15m-20260515-0820.json.gz")
    parser.add_argument("--output", default="/private/tmp/cross-section-multi-name.json")
    args = parser.parse_args()
    with gzip.open(args.input, "rt") as handle:
        payload = json.load(handle)
    periods = {
        "full": (payload["start"] + DAY_MS, payload["end"]),
        "june": (ts("2026-06-01"), ts("2026-07-01")),
        "july": (ts("2026-07-01"), ts("2026-08-01")),
        "august": (ts("2026-08-01"), payload["end"]),
        "recent_10d": (max(payload["start"], payload["end"] - 10 * DAY_MS), payload["end"]),
    }
    report = {"input": args.input, "method": "12h/3h production direction; next-open; protected exits; 5bp fee + 5bp slippage each side", "variants": {}}
    for names in (1, 2, 3, 5, 6, 10):
        series = build_series(payload["data"], names, 5.0)
        report["variants"][str(names)] = {
            "fixed_total_gross_0.5x": {period: evaluate(series, *bounds, 0.5) for period, bounds in periods.items()},
            "fixed_per_name_gross_0.5x": {period: evaluate(series, *bounds, 0.5 * names) for period, bounds in periods.items()},
            "production_dynamic_gross": {period: evaluate(series, *bounds, 0.5, dynamic=True) for period, bounds in periods.items()},
        }
        if names == 5:
            report["five_name_sizing_grid"] = {
                label: {period: evaluate(series, *bounds, 0.5, dynamic=True, dynamic_tiers=tiers)
                        for period, bounds in periods.items()}
                for label, tiers in {
                    "production_0.05_0.40_0.50": (0.05, 0.40, 0.50),
                    "conservative_0.25_0.50_0.75": (0.25, 0.50, 0.75),
                    "base_0.15_active_0.75_strong_1.00": (0.15, 0.75, 1.00),
                    "base_0.15_active_1.00_strong_1.50": (0.15, 1.00, 1.50),
                    "base_0.20_active_0.75_strong_1.00": (0.20, 0.75, 1.00),
                    "base_0.20_active_1.00_strong_1.50": (0.20, 1.00, 1.50),
                    "base_0.25_active_0.75_strong_1.00": (0.25, 0.75, 1.00),
                    "base_0.25_active_1.00_strong_1.50": (0.25, 1.00, 1.50),
                    "base_0.30_active_0.75_strong_1.00": (0.30, 0.75, 1.00),
                    "balanced_0.50_1.00_1.50": (0.50, 1.00, 1.50),
                    "aggressive_0.75_1.25_2.00": (0.75, 1.25, 2.00),
                    "always_0.50": (0.50, 0.50, 0.50),
                    "always_1.00": (1.00, 1.00, 1.00),
                    "always_1.50": (1.50, 1.50, 1.50),
                }.items()
            }
    with open(args.output, "w") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
