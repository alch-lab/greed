#!/usr/bin/env python3
"""Causal side-specific online gate for the altcoin impulse model.

Every de-duplicated breakout signal is evaluated as a shadow trade.  Only
shadow outcomes completed before a new signal are admitted to the rolling
window, so the gate never sees future prices.  Actual portfolio signals are
enabled independently for longs and shorts when their recent shadow mean is
positive and profit factor clears the configured threshold.
"""

from __future__ import annotations

import argparse
import heapq
import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_entry_exit_v2 import (
    EntryProfile,
    ExitProfile,
    enrich_point_in_time_spot,
    filter_signals,
    simulate,
    spot_paths,
    timestamp,
)
from exp_altcoin_five_optimizations import load_symbol_bars, make_breakout_signals
from exp_altcoin_oi_launch import BAR_MS, MAJORS, Bar, Signal, load_bars


@dataclass(frozen=True)
class ShadowOutcome:
    exit_ts: int
    return_fraction: float


def isolated_outcome(
    signal: Signal,
    bars: list[Bar],
    profile: ExitProfile,
    slippage_bps: float = 5.0,
) -> ShadowOutcome | None:
    index = int(signal.features["signal_index"]) + 1
    if index >= len(bars) or bars[index].ts != signal.execute_ts:
        return None
    fee = 5.0 / 10_000.0
    slip = slippage_bps / 10_000.0
    entry = bars[index].open * (1.0 + signal.side * slip)
    stop = entry * (1.0 - signal.side * profile.stop)
    extreme = entry
    exit_raw = bars[min(index + profile.max_hold_bars, len(bars) - 1)].open
    exit_ts = bars[min(index + profile.max_hold_bars, len(bars) - 1)].ts
    for cursor in range(index, min(index + profile.max_hold_bars, len(bars))):
        bar = bars[cursor]
        gap = bar.open <= stop if signal.side > 0 else bar.open >= stop
        hit = bar.low <= stop if signal.side > 0 else bar.high >= stop
        if gap or hit:
            exit_raw = bar.open if gap else stop
            exit_ts = bar.ts
            break
        if signal.side > 0:
            extreme = max(extreme, bar.high)
            if extreme / entry - 1.0 >= profile.trail_activation:
                stop = max(stop, extreme * (1.0 - profile.trail_distance))
        else:
            extreme = min(extreme, bar.low)
            if 1.0 - extreme / entry >= profile.trail_activation:
                stop = min(stop, extreme * (1.0 + profile.trail_distance))
    exit_price = exit_raw * (1.0 - signal.side * slip)
    gross = signal.side * (exit_price / entry - 1.0)
    net = gross - fee - fee * exit_price / entry
    return ShadowOutcome(exit_ts, net)


def gate_metrics(values: deque[float], minimum_pf: float) -> tuple[bool, float, float]:
    if not values:
        return False, 0.0, 0.0
    mean = sum(values) / len(values)
    profit = sum(max(value, 0.0) for value in values)
    loss = sum(max(-value, 0.0) for value in values)
    profit_factor = profit / loss if loss > 0.0 else (99.0 if profit > 0.0 else 0.0)
    return mean > 0.0 and profit_factor >= minimum_pf, mean, profit_factor


def online_filter(
    signals: list[Signal],
    bars_by_symbol: dict[str, list[Bar]],
    exit_profile: ExitProfile,
    window: int,
    minimum_pf: float,
    *,
    shadow_cooldown_hours: int = 6,
) -> tuple[list[Signal], dict[str, object]]:
    histories = {1: deque(maxlen=window), -1: deque(maxlen=window)}
    pending: list[tuple[int, int, int, float]] = []
    last_shadow: dict[tuple[str, int], int] = {}
    output: list[Signal] = []
    serial = 0
    decisions = {1: 0, -1: 0}
    enabled = {1: 0, -1: 0}
    ordered = sorted(signals, key=lambda signal: (signal.execute_ts, -signal.score))
    for signal in ordered:
        while pending and pending[0][0] <= signal.execute_ts:
            _, _, side, value = heapq.heappop(pending)
            histories[side].append(value)
        values = histories[signal.side]
        gate_on, _, _ = gate_metrics(values, minimum_pf)
        gate_on = gate_on and len(values) == window
        decisions[signal.side] += 1
        if gate_on:
            output.append(signal)
            enabled[signal.side] += 1

        key = (signal.symbol, signal.side)
        cooldown_ms = shadow_cooldown_hours * 3_600_000
        if signal.execute_ts - last_shadow.get(key, -10**18) < cooldown_ms:
            continue
        outcome = isolated_outcome(signal, bars_by_symbol[signal.symbol], exit_profile)
        if outcome is None:
            continue
        last_shadow[key] = signal.execute_ts
        serial += 1
        heapq.heappush(
            pending,
            (outcome.exit_ts, serial, signal.side, outcome.return_fraction),
        )

    long_on, long_mean, long_pf = gate_metrics(histories[1], minimum_pf)
    short_on, short_mean, short_pf = gate_metrics(histories[-1], minimum_pf)
    return output, {
        "long": {
            "samples": len(histories[1]),
            "enabled": long_on and len(histories[1]) == window,
            "mean": long_mean,
            "profit_factor": long_pf,
            "kept_signals": enabled[1],
            "total_signals": decisions[1],
        },
        "short": {
            "samples": len(histories[-1]),
            "enabled": short_on and len(histories[-1]) == window,
            "mean": short_mean,
            "profit_factor": short_pf,
            "kept_signals": enabled[-1],
            "total_signals": decisions[-1],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-online-impulse-gate.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted(
        {
            path.name
            for path in (cache / "spot-klines").iterdir()
            if path.is_dir() and path.name not in MAJORS
        }
        & {
            path.name
            for path in (cache / "klines").iterdir()
            if path.is_dir() and path.name not in MAJORS
        }
    )
    bars_by_symbol: dict[str, list[Bar]] = {}
    base_signals: list[Signal] = []
    for completed, symbol in enumerate(symbols, 1):
        futures_bars = load_symbol_bars(cache, symbol)
        if len(futures_bars) < 15 * 96:
            continue
        spot_bars = load_bars(spot_paths(cache, symbol))
        enriched = enrich_point_in_time_spot(
            make_breakout_signals(symbol, futures_bars),
            spot_bars,
        )
        if enriched:
            bars_by_symbol[symbol] = futures_bars
            base_signals.extend(enriched)
        if completed % 100 == 0:
            print(f"features {completed}/{len(symbols)} signals={len(base_signals)}", flush=True)

    entries = [
        EntryProfile("baseline"),
        EntryProfile("long_cap_08", long_max_1h=0.08),
        EntryProfile("long_cap_10", long_max_1h=0.10),
        EntryProfile("long_cap_10_gap_05", long_max_1h=0.10, max_spot_perp_gap=0.05),
    ]
    exits = [
        ExitProfile("S5_A2_T1_H6", 0.05, 0.02, 0.01, 24),
        ExitProfile("S5_A1.5_T0.75_H6", 0.05, 0.015, 0.0075, 24),
        ExitProfile("S5_A2.5_T1_H6", 0.05, 0.025, 0.01, 24),
    ]
    periods = {
        "train": (timestamp("2026-01-15"), timestamp("2026-05-01")),
        "validation": (timestamp("2026-05-01"), timestamp("2026-07-01")),
        "july_test": (timestamp("2026-07-01"), timestamp("2026-08-01")),
        "aug_holdout": (timestamp("2026-08-01"), timestamp("2026-08-11")),
        "full": (timestamp("2026-01-15"), timestamp("2026-08-11")),
    }
    rows: list[dict[str, object]] = []
    for entry in entries:
        entry_signals = filter_signals(base_signals, entry)
        for exit_profile in exits:
            for window in (10, 20, 40, 60):
                for minimum_pf in (1.0, 1.1, 1.2, 1.3):
                    gated, final_gate = online_filter(
                        entry_signals,
                        bars_by_symbol,
                        exit_profile,
                        window,
                        minimum_pf,
                    )
                    results = {
                        name: simulate(
                            bars_by_symbol,
                            gated,
                            *bounds,
                            exit_profile,
                            high_water_limit=0.08,
                        )
                        for name, bounds in periods.items()
                    }
                    rows.append(
                        {
                            "key": f"{entry.name}__{exit_profile.name}__W{window}_PF{minimum_pf:.1f}",
                            "entry": entry.name,
                            "exit": exit_profile.name,
                            "window": window,
                            "minimum_pf": minimum_pf,
                            "signals": len(gated),
                            "final_gate": final_gate,
                            "results": results,
                        }
                    )
            print(f"grid {entry.name} {exit_profile.name} complete", flush=True)

    eligible = [
        row
        for row in rows
        if float(row["results"]["train"]["return_pct"]) > 0.0
        and float(row["results"]["validation"]["return_pct"]) > 0.0
        and int(row["results"]["train"]["trades"]) >= 20
        and int(row["results"]["validation"]["trades"]) >= 10
    ]
    eligible.sort(
        key=lambda row: (
            float(row["results"]["validation"]["profit_factor"] or 0.0),
            -float(row["results"]["validation"]["max_drawdown_pct"]),
        ),
        reverse=True,
    )
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "symbols": len(bars_by_symbol),
            "signals": len(base_signals),
            "range": ["2026-01-15", "2026-08-11"],
            "selection": "train Jan-Apr + validation May-Jun only",
        },
        "grid_size": len(rows),
        "eligible": len(eligible),
        "top": eligible[:30],
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"eligible={len(eligible)} report={args.output}", flush=True)


if __name__ == "__main__":
    main()
