#!/usr/bin/env python3
"""Out-of-sample search for faster two-sided cross-sectional rotation.

This tests whether the robust 12h/3h ranking idea can be shortened enough to
enter the first part of a pump/dump without becoming fee-heavy noise. Signals
use closed 15m bars and next-open execution with taker fees and adverse slip.
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


BAR_MS = 15 * 60_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
PERIODS = {
    "train": ("2026-05-22", "2026-07-01"),
    "validation": ("2026-07-01", "2026-08-01"),
    "holdout": ("2026-08-01", "2026-08-15"),
    "recent": ("2026-08-15", "2026-08-21"),
}
EXCLUDED = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT", "SUIUSDT"}


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote: float


@dataclass
class Position:
    symbol: str
    side: int
    entry: float
    notional: float
    stop: float
    extreme: float
    partial: bool = False
    remaining: float = 1.0
    pnl: float = 0.0


def stamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def load(path: Path):
    with gzip.open(path, "rt") as handle:
        payload = json.load(handle)
    bars, lookup = {}, {}
    for symbol, rows in payload.get("data", payload).items():
        if symbol in EXCLUDED or not symbol.endswith("USDT"):
            continue
        parsed = [Bar(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[7])) for row in rows]
        if len(parsed) >= 8 * 96:
            bars[symbol] = parsed
            lookup[symbol] = {bar.ts: bar for bar in parsed}
    return bars, lookup


def replay(bars, lookup, start, end, *, lookback_hours, interval_minutes, stop, activation, trail, slip_bps=5.0):
    interval = interval_minutes * 60_000
    first = ((start + interval - 1) // interval) * interval
    equity = peak = day_start = 1000.0
    max_dd = fees = 0.0
    position = None
    entries = exits = wins = 0
    daily_entries = 0
    day = None
    trade_pnls = []

    def fill(raw, side, entering):
        return raw * (1 + (side if entering else -side) * slip_bps / 10_000)

    def close(raw, ts, fraction=1.0):
        nonlocal equity, peak, max_dd, fees, position, exits, wins
        price = fill(raw, position.side, False)
        amount = position.notional * position.remaining * fraction
        pnl = amount * position.side * (price / position.entry - 1)
        fee = amount * price / position.entry * 0.0005
        equity += pnl - fee
        fees += fee
        position.pnl += pnl - fee
        position.remaining *= 1 - fraction
        if position.remaining < 1e-9:
            exits += 1
            wins += position.pnl > 0
            trade_pnls.append(position.pnl)
            position = None
        peak = max(peak, equity)
        max_dd = max(max_dd, 1 - equity / peak)

    def process(bar):
        nonlocal position
        if position is None:
            return
        hit = bar.low <= position.stop if position.side > 0 else bar.high >= position.stop
        if hit:
            gapped = bar.open < position.stop if position.side > 0 else bar.open > position.stop
            close(bar.open if gapped else position.stop, bar.ts)
            return
        favorable = bar.high / position.entry - 1 if position.side > 0 else position.entry / bar.low - 1
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

    def select(boundary):
        ranks = []
        back_ts = boundary - lookback_hours * HOUR_MS - BAR_MS
        signal_ts = boundary - BAR_MS
        for symbol, values in lookup.items():
            signal, back = values.get(signal_ts), values.get(back_ts)
            if not signal or not back or back.close <= 0:
                continue
            recent = [values.get(t) for t in range(boundary - DAY_MS, boundary, BAR_MS)]
            # A partial 24h tape made earlier experiments look profitable by
            # ranking newly listed, paused or nearly untradeable contracts.
            # Production cannot assume a fill through a missing market.
            if sum(item is not None for item in recent) < 95:
                continue
            volume = sum(item.quote for item in recent if item is not None)
            ret = signal.close / back.close - 1
            if volume >= 10_000_000 and abs(ret) <= 1.0:
                ranks.append((ret, symbol))
        if len(ranks) < 20:
            return None
        ranks.sort()
        weakest, strongest = ranks[0], ranks[-1]
        median = ranks[len(ranks) // 2][0]
        long_excess = strongest[0] - median
        short_excess = median - weakest[0]
        chosen, side = (strongest, 1) if long_excess >= short_excess else (weakest, -1)
        # A minimum residual avoids rotating on a flat, noisy cross-section.
        if max(long_excess, short_excess) < 0.02:
            return None
        return chosen[1], side

    previous = first
    for boundary in range(first, end, interval):
        for ts in range(previous, boundary, BAR_MS):
            if position is not None and (bar := lookup[position.symbol].get(ts)):
                process(bar)
        previous = boundary
        candidate = select(boundary)
        same = position is not None and candidate == (position.symbol, position.side)
        if position is not None and not same:
            raw = lookup[position.symbol].get(boundary)
            prior = lookup[position.symbol].get(boundary - BAR_MS)
            close(raw.open if raw else prior.close if prior else position.entry, boundary)
        risk_day = (boundary + 8 * HOUR_MS) // DAY_MS
        if risk_day != day:
            day, day_start, daily_entries = risk_day, equity, 0
        if position is None and candidate and daily_entries < 10 and equity > day_start * 0.96:
            symbol, side = candidate
            bar = lookup[symbol].get(boundary)
            if not bar:
                continue
            notional = equity * 0.5
            entry = fill(bar.open, side, True)
            fee = notional * 0.0005
            equity -= fee
            fees += fee
            position = Position(symbol, side, entry, notional, entry * (1 - side * stop), entry, pnl=-fee)
            entries += 1
            daily_entries += 1
            max_dd = max(max_dd, 1 - equity / peak)
    if position is not None:
        close(bars[position.symbol][-1].close, end)
    gains = sum(value for value in trade_pnls if value > 0)
    losses = -sum(value for value in trade_pnls if value < 0)
    return {
        "return_pct": (equity / 1000 - 1) * 100,
        "max_drawdown_pct": max_dd * 100,
        "entries": entries,
        "trades_per_day": entries / max((end - start) / DAY_MS, 1),
        "win_rate_pct": wins / exits * 100 if exits else 0,
        "profit_factor": gains / losses if losses else None,
        "fees": fees,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("/private/tmp/binance-testnet-15m-20260515-0820.json.gz"))
    parser.add_argument("--output", type=Path, default=Path("/private/tmp/greed-altcoin-rotation-horizon.json"))
    args = parser.parse_args()
    bars, lookup = load(args.data)
    results = {}
    for lookback in (1, 2, 4, 8, 12, 24):
        for interval in (30, 60, 120, 180):
            for stop, activation, trail in ((0.02, 0.03, 0.01), (0.03, 0.05, 0.02), (0.04, 0.05, 0.02)):
                name = f"lb{lookback}h_i{interval}m_s{stop:.2f}_a{activation:.2f}_t{trail:.2f}"
                settings = dict(lookback_hours=lookback, interval_minutes=interval, stop=stop, activation=activation, trail=trail)
                results[name] = {"settings": settings, "periods": {period: replay(bars, lookup, stamp(bounds[0]), stamp(bounds[1]), **settings) for period, bounds in PERIODS.items()}}
    eligible = []
    for name, item in results.items():
        train, valid = item["periods"]["train"], item["periods"]["validation"]
        if train["return_pct"] > 0 and valid["return_pct"] > 0 and valid["entries"] >= 15:
            eligible.append((valid["return_pct"] - 0.5 * valid["max_drawdown_pct"], name))
    ranked = [name for _, name in sorted(eligible, reverse=True)]
    args.output.write_text(json.dumps({"periods": PERIODS, "ranked_without_holdout": ranked, "results": results}, indent=2) + "\n")
    print(json.dumps({"eligible": len(ranked), "top": [{"name": name, "periods": results[name]["periods"]} for name in ranked[:10]]}, indent=2))


if __name__ == "__main__":
    main()
