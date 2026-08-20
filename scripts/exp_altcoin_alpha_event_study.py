#!/usr/bin/env python3
"""Forward-return map for liquid altcoin impulse events.

The study is descriptive rather than a portfolio backtest. It asks whether an
observed one-hour move tends to continue or reverse after executable costs, and
segments the answer by direction, move size, volume, close location and trend
alignment. Events are non-overlapping per symbol and horizon.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_five_optimizations import load_symbol_bars, spot_history_symbols
from exp_altcoin_oi_launch import MAJORS, Bar


@dataclass
class Stats:
    count: int = 0
    total: float = 0.0
    wins: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    values: list[float] | None = None

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.wins += value > 0
        self.gross_profit += max(value, 0.0)
        self.gross_loss += max(-value, 0.0)
        if self.values is None:
            self.values = []
        self.values.append(value)

    def summary(self) -> dict[str, float | int | None]:
        ordered = sorted(self.values or [])
        median = ordered[len(ordered) // 2] if ordered else 0.0
        return {
            "events": self.count,
            "mean_net_pct": self.total / self.count * 100 if self.count else 0.0,
            "median_net_pct": median * 100,
            "win_rate_pct": self.wins / self.count * 100 if self.count else 0.0,
            "profit_factor": self.gross_profit / self.gross_loss if self.gross_loss > 0 else None,
        }


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


PERIODS = {
    "train": (timestamp("2026-01-15"), timestamp("2026-05-01")),
    "validation": (timestamp("2026-05-01"), timestamp("2026-07-01")),
    "july": (timestamp("2026-07-01"), timestamp("2026-08-01")),
    "aug": (timestamp("2026-08-01"), timestamp("2026-08-11")),
}


def period_for(ts: int) -> str | None:
    for name, (start, end) in PERIODS.items():
        if start <= ts < end:
            return name
    return None


def move_bin(value: float) -> str:
    value = abs(value)
    if value < 0.02:
        return "01_02"
    if value < 0.04:
        return "02_04"
    if value < 0.08:
        return "04_08"
    if value < 0.15:
        return "08_15"
    return "15_plus"


def volume_bin(value: float) -> str:
    if value < 1.5:
        return "lt_1_5"
    if value < 3.0:
        return "1_5_3"
    if value < 6.0:
        return "3_6"
    return "6_plus"


def location_bin(value: float) -> str:
    if value < 0.40:
        return "weak"
    if value < 0.65:
        return "middle"
    return "strong"


def forward_net(entry_raw: float, exit_raw: float, side: int, cost_bps: float) -> float:
    # Default 20 bp round trip = 5 bp slippage on each fill + 5 bp fee
    # on each fill. Half is embedded in prices and half deducted explicitly.
    slippage = cost_bps / 4 / 10_000
    entry = entry_raw * (1 + side * slippage)
    exit_price = exit_raw * (1 - side * slippage)
    return side * (exit_price / entry - 1.0) - cost_bps / 2 / 10_000


def study_symbol(symbol: str, bars: list[Bar], output: dict[tuple[object, ...], Stats], cost_bps: float) -> int:
    quote_prefix = [0.0]
    for bar in bars:
        quote_prefix.append(quote_prefix[-1] + bar.quote_volume)
    last_event: dict[int, int] = {4: -10**9, 8: -10**9, 16: -10**9}
    events = 0
    for index in range(14 * 96, len(bars) - 17):
        ts = bars[index].ts
        period = period_for(ts)
        if period is None:
            continue
        close = bars[index].close
        r15 = close / bars[index - 1].close - 1.0
        r1 = close / bars[index - 4].close - 1.0
        r4 = close / bars[index - 16].close - 1.0
        if abs(r1) < 0.01:
            continue
        q24 = quote_prefix[index + 1] - quote_prefix[index - 95]
        if q24 < 10_000_000:
            continue
        q7d = quote_prefix[index] - quote_prefix[index - 7 * 96]
        q1 = quote_prefix[index + 1] - quote_prefix[index - 3]
        vol = q1 / max(q7d / (7 * 24), 1.0)
        side = 1 if r1 > 0 else -1
        span = bars[index].high - bars[index].low
        long_location = (close - bars[index].low) / span if span > 0 else 0.5
        directional_location = long_location if side > 0 else 1 - long_location
        direction = "up" if side > 0 else "down"
        rbin = move_bin(r1)
        vbin = volume_bin(vol)
        lbin = location_bin(directional_location)
        trend = "aligned" if side * r4 > 0 else "opposed"
        current = "aligned" if side * r15 > 0 else "reversing"
        for horizon in (4, 8, 16):
            if index - last_event[horizon] < horizon:
                continue
            last_event[horizon] = index
            entry = bars[index + 1].open
            exit_price = bars[index + horizon].close
            for mode, trade_side in (("continue", side), ("fade", -side)):
                value = forward_net(entry, exit_price, trade_side, cost_bps)
                keys = [
                    ("base", period, mode, horizon, direction, rbin),
                    ("volume", period, mode, horizon, direction, rbin, vbin),
                    ("location", period, mode, horizon, direction, rbin, lbin),
                    ("structure", period, mode, horizon, direction, rbin, vbin, lbin, trend, current),
                ]
                for key in keys:
                    output[key].add(value)
            events += 1
    return events


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-alpha-event-study.json")
    parser.add_argument("--round-trip-cost-bps", type=float, default=20.0)
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted({path.parent.name for path in (cache / "klines").glob("*/*.zip")})
    allowed = spot_history_symbols()
    symbols = [symbol for symbol in symbols if symbol in allowed and symbol not in MAJORS]
    aggregates: dict[tuple[object, ...], Stats] = defaultdict(Stats)
    event_count = 0
    used = 0
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        if len(bars) < 15 * 96:
            continue
        used += 1
        event_count += study_symbol(symbol, bars, aggregates, args.round_trip_cost_bps)
        if completed % 50 == 0:
            print(f"symbols {completed}/{len(symbols)} events={event_count}", flush=True)
    rows: list[dict[str, object]] = []
    for key, stats in aggregates.items():
        row = {"key": list(key), **stats.summary()}
        rows.append(row)
    rows.sort(key=lambda row: (str(row["key"]), -int(row["events"])))
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Binance Vision USD-M 15m klines",
        "symbols": used,
        "raw_nonoverlap_events": event_count,
        "round_trip_cost_bps": args.round_trip_cost_bps,
        "periods": {name: [start, end] for name, (start, end) in PERIODS.items()},
        "rows": rows,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
