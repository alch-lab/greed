#!/usr/bin/env python3
"""Causal replay for the production 12h/3h cross-section execution rules.

The replay isolates direction selection and exit/rotation mechanics.  It uses
closed 15 minute bars, next-open fills, 5 bps taker fees and 5 bps adverse
slippage on every fill.  A fixed 0.40x equity notional is used so variants are
comparable without letting the rolling quality gate hide execution effects.
"""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone


BAR_MS = 15 * 60_000
DAY_MS = 24 * 60 * 60_000
EXCLUDED = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT", "SUIUSDT",
}


@dataclass
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float


@dataclass
class Position:
    symbol: str
    side: int
    entry: float
    notional: float
    remaining: float = 1.0
    stop: float = 0.0
    extreme: float = 0.0
    partial: bool = False
    trade_pnl: float = 0.0


def select(ranks: list[tuple[float, str]], threshold: float, symmetric: bool):
    if not ranks:
        return None
    median = ranks[len(ranks) // 2][0]
    weakest, strongest = ranks[0], ranks[-1]
    if not symmetric:
        return (strongest[1], 1, strongest[0], median) if median >= threshold and strongest[0] > 0 else ((weakest[1], -1, weakest[0], median) if weakest[0] < 0 else None)
    long_valid, short_valid = strongest[0] > 0, weakest[0] < 0
    if median >= threshold and long_valid:
        chosen, side = strongest, 1
    elif median <= -threshold and short_valid:
        chosen, side = weakest, -1
    elif long_valid and (not short_valid or strongest[0] - median >= median - weakest[0]):
        chosen, side = strongest, 1
    elif short_valid:
        chosen, side = weakest, -1
    else:
        return None
    return chosen[1], side, chosen[0], median


def replay(data, *, symmetric, carry, protected, post_exit_rerank=False, post_exit_min_excess=0.0, spot_symbols=None, stop=0.04, activation=0.05, trail=0.02, gross=0.40, dynamic_gross=False, slippage_bps=5.0):
    bars = {}
    by_ts = {}
    for symbol, rows in data.items():
        if symbol in EXCLUDED or not symbol.endswith("USDT") or (spot_symbols is not None and symbol not in spot_symbols):
            continue
        parsed = [Bar(int(r[0]), *map(float, (r[1], r[2], r[3], r[4], r[7]))) for r in rows]
        bars[symbol] = parsed
        by_ts[symbol] = {bar.ts: bar for bar in parsed}
    start = max(min(items[0].ts for items in bars.values()) + DAY_MS, min(items[-1].ts for items in bars.values()) - 7 * DAY_MS)
    end = max(items[-1].ts for items in bars.values())
    first = ((start + 3 * 3_600_000 - 1) // (3 * 3_600_000)) * (3 * 3_600_000)
    boundaries = list(range(first, end, 3 * 3_600_000))
    equity = peak = 1000.0
    max_drawdown = 0.0
    position = None
    entries = wins = exits = 0
    fees = 0.0
    daily = defaultdict(lambda: {"entries": 0, "pnl": 0.0})
    current_day = None
    day_start = equity

    def fill_price(raw, side, entering):
        direction = side if entering else -side
        return raw * (1 + direction * slippage_bps / 10_000)

    def close(raw_price, ts, fraction=1.0):
        nonlocal equity, peak, max_drawdown, position, fees, wins, exits
        fill = fill_price(raw_price, position.side, False)
        amount = position.notional * position.remaining * fraction
        pnl = amount * position.side * (fill / position.entry - 1)
        fee = amount * fill / position.entry * 0.0005
        equity += pnl - fee
        position.trade_pnl += pnl - fee
        fees += fee
        daily[ts // DAY_MS]["pnl"] += pnl - fee
        position.remaining *= 1 - fraction
        if position.remaining < 1e-9:
            exits += 1
            wins += position.trade_pnl > 0
            position = None
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1 - equity / peak)

    def process_bar(bar):
        nonlocal position
        if position is None:
            return
        hit = bar.low <= position.stop if position.side > 0 else bar.high >= position.stop
        if hit:
            gapped = bar.open < position.stop if position.side > 0 else bar.open > position.stop
            close(bar.open if gapped else position.stop, bar.ts)
            return
        if not protected:
            return
        favorable = (bar.high / position.entry - 1) if position.side > 0 else (position.entry / bar.low - 1)
        if not position.partial and favorable >= activation:
            close(position.entry * (1 + position.side * activation), bar.ts, 0.33)
            if position is None:
                return
            position.partial = True
            position.stop = max(position.stop, position.entry) if position.side > 0 else min(position.stop, position.entry)
        if position.side > 0:
            position.extreme = max(position.extreme, bar.high)
            if position.partial:
                position.stop = max(position.stop, position.extreme * (1 - trail))
        else:
            position.extreme = min(position.extreme, bar.low)
            if position.partial:
                position.stop = min(position.stop, position.extreme * (1 + trail))

    def candidate_at(boundary, exclusions=frozenset()):
        ranks = []
        for symbol, lookup in by_ts.items():
            if symbol in exclusions:
                continue
            signal = lookup.get(boundary - BAR_MS)
            back = lookup.get(boundary - 12 * 3_600_000 - BAR_MS)
            if not signal or not back or back.close <= 0:
                continue
            volume = sum(lookup[t].quote_volume for t in range(boundary - DAY_MS, boundary, BAR_MS) if t in lookup)
            value = signal.close / back.close - 1
            if volume >= 10_000_000 and abs(value) <= 1.5:
                ranks.append((value, symbol))
        ranks.sort()
        return select(ranks, 0.01, symmetric)

    def modeled_return(candidate, boundary):
        if candidate is None:
            return None
        symbol, side, _, _ = candidate
        entry_bar = by_ts[symbol].get(boundary)
        exit_bar = by_ts[symbol].get(boundary + 3 * 3_600_000)
        if entry_bar is None or exit_bar is None:
            return None
        entry = entry_bar.open
        stop_price = entry * (1 - side * 0.04)
        exit_price = exit_bar.open
        for ts in range(boundary, boundary + 3 * 3_600_000, BAR_MS):
            bar = by_ts[symbol].get(ts)
            if bar is None:
                return None
            hit = bar.low <= stop_price if side > 0 else bar.high >= stop_price
            if hit:
                gapped = bar.open < stop_price if side > 0 else bar.open > stop_price
                exit_price = bar.open if gapped else stop_price
                break
        return side * (exit_price / entry - 1) - 0.002

    def configured_gross(candidate, boundary):
        if not dynamic_gross or candidate is None:
            return gross
        history = []
        for offset in range(60, 0, -1):
            past_boundary = boundary - offset * 3 * 3_600_000
            value = modeled_return(candidate_at(past_boundary), past_boundary)
            if value is not None:
                history.append(value)
        history = history[-30:]
        gains = sum(max(value, 0) for value in history)
        losses = sum(max(-value, 0) for value in history)
        pf = gains / losses if losses else 99.0
        open_gate = len(history) == 30 and sum(history) > 0 and pf >= 1.30
        excess = abs(candidate[2] - candidate[3])
        return 0.50 if open_gate and excess >= 0.12 else 0.40 if open_gate else 0.05

    def open_candidate(candidate, boundary):
        nonlocal equity, max_drawdown, position, fees, entries, current_day, day_start
        if candidate is None:
            return False
        day = boundary // DAY_MS
        if day != current_day:
            current_day, day_start = day, equity
        if equity < day_start * 0.96 or daily[day]["entries"] >= 10:
            return False
        symbol, side, _, _ = candidate
        bar = by_ts[symbol].get(boundary)
        if not bar:
            return False
        notional = equity * configured_gross(candidate, boundary)
        entry = fill_price(bar.open, side, True)
        fee = notional * 0.0005
        equity -= fee
        fees += fee
        daily[day]["pnl"] -= fee
        daily[day]["entries"] += 1
        entries += 1
        position = Position(symbol, side, entry, notional, stop=entry * (1 - side * stop), extreme=entry, trade_pnl=-fee)
        max_drawdown = max(max_drawdown, 1 - equity / peak)
        return True

    previous = first
    window_exclusions = set()
    for boundary in boundaries:
        for ts in range(previous, boundary, BAR_MS):
            if position is not None and (bar := by_ts[position.symbol].get(ts)):
                previous_symbol = position.symbol
                process_bar(bar)
                if position is None:
                    window_exclusions.add(previous_symbol)
                    if post_exit_rerank:
                        replacement = candidate_at(ts + BAR_MS, window_exclusions)
                        if replacement is not None and abs(replacement[2] - replacement[3]) >= post_exit_min_excess:
                            open_candidate(replacement, ts + BAR_MS)
        previous = boundary
        # The wall-clock boundary starts a fresh ranking window. Symbols exited
        # in the previous window may participate in the new independent signal.
        window_exclusions.clear()
        candidate = candidate_at(boundary)
        same = position is not None and candidate is not None and (position.symbol, position.side) == candidate[:2]
        if position is not None and not (carry and same):
            raw = by_ts[position.symbol].get(boundary)
            close(raw.open if raw else by_ts[position.symbol][boundary - BAR_MS].close, boundary)
        if position is None:
            open_candidate(candidate, boundary)
    if position is not None:
        last = bars[position.symbol][-1]
        close(last.close, last.ts)
    days = []
    for day in range(first // DAY_MS, end // DAY_MS + 1):
        item = daily[day]
        days.append({"date": datetime.fromtimestamp(day * DAY_MS / 1000, timezone.utc).strftime("%Y-%m-%d"), **item})
    return {
        "return_pct": (equity / 1000 - 1) * 100,
        "pnl": equity - 1000,
        "entries": entries,
        "exits": exits,
        "wins": wins,
        "win_rate_pct": wins / exits * 100 if exits else 0,
        "fees": fees,
        "max_drawdown_pct": max_drawdown * 100,
        "daily": days,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/private/tmp/binance-testnet-15m-20260810-17.json.gz")
    parser.add_argument("--spot-symbol-dir", default="/private/tmp/greed-altcoin-oi-full/spot-klines")
    args = parser.parse_args()
    with gzip.open(args.input, "rt") as handle:
        payload = json.load(handle)
    spot_dir = Path(args.spot_symbol_dir)
    spot_symbols = {path.name for path in spot_dir.iterdir() if path.is_dir()} if spot_dir.exists() else None
    variants = {
        "old_asymmetric_fixed_3h": dict(symmetric=False, carry=False, protected=False, stop=0.08),
        "old_direction_carry_protected": dict(symmetric=False, carry=True, protected=True),
        "post_exit_15m_rerank": dict(symmetric=False, carry=True, protected=True, post_exit_rerank=True, post_exit_min_excess=0.12),
        "symmetric_fixed_3h": dict(symmetric=True, carry=False, protected=False, stop=0.08),
        "symmetric_carry_protected": dict(symmetric=True, carry=True, protected=True),
    }
    result = {name: replay(payload["data"], spot_symbols=spot_symbols, **options) for name, options in variants.items()}
    grids = {}
    for label, symmetric in (("production_direction", False), ("symmetric_tail", True)):
        grid = []
        for stop in (0.03, 0.04, 0.05):
            for activation in (0.02, 0.03, 0.05, 0.075, 0.10):
                for trail in (0.005, 0.01, 0.02, 0.03):
                    if trail >= activation:
                        continue
                    value = replay(payload["data"], symmetric=symmetric, carry=True, protected=True, spot_symbols=spot_symbols, stop=stop, activation=activation, trail=trail)
                    grid.append({"stop": stop, "activation": activation, "trail": trail, **{k: value[k] for k in ("return_pct", "entries", "win_rate_pct", "fees")}})
        grid.sort(key=lambda item: item["return_pct"], reverse=True)
        grids[label] = {"top": grid[:10], "configured_rank": next(i + 1 for i, item in enumerate(grid) if (item["stop"], item["activation"], item["trail"]) == (0.04, 0.05, 0.02))}
    print(json.dumps({"source": {**{k: payload[k] for k in ("start", "end")}, "spot_symbols": len(spot_symbols) if spot_symbols is not None else None}, "variants": result, "grids": grids}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
