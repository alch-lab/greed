#!/usr/bin/env python3
"""Walk-forward study for trend entries missed by the first maker order.

The existing 0.30 ATR / one-minute maker attempt remains the baseline.  This
study compares three treatments for an unfilled setup: chase in the original
direction, reverse immediately, or wait for a bounded pullback plus a fresh
one-minute directional/flow reclaim.  Recovery parameters are selected on
train+validation only; locked and recent windows remain untouched.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import oi_taker_continuation_backtest as minute
import trend_execution_walkforward as execution
import unified_compound_research as base


@dataclass(frozen=True)
class RecoveryConfig:
    min_escape_bps: float
    min_pullback_bps: float
    max_extension_bps: float
    min_flow: float
    wait_minutes: int


@dataclass(frozen=True)
class ConfirmedChaseConfig:
    max_extension_bps: float
    min_flow: float
    min_close_location: float


EXIT = execution.ExitConfig(.0125, 2.0, .4, .5, .005)
ENTRY = execution.EntryConfig(.30, 1)


def explicit_outcome(setup, values, fill, entry, side, end_ms, *, entry_fee):
    synthetic = replace(setup, side=side)
    risk = entry * EXIT.stop_pct
    stop = entry - side * risk
    target = entry + side * risk * EXIT.target_r
    remaining = 1.0
    realized = -entry_fee
    partial = shielded = False
    extreme = entry
    mfe_r = 0.0
    exit_index = fill
    exit_price = entry
    reason = "window_mark"
    for index in range(fill + 1, len(values)):
        bar = values[index]
        if bar.ts >= end_ms:
            exit_index = max(fill, index - 1)
            exit_price = values[exit_index].close
            break
        exit_index = index
        favorable = bar.high - entry if side > 0 else entry - bar.low
        mfe_r = max(mfe_r, favorable / max(risk, 1e-12))
        stop_hit = bar.low <= stop if side > 0 else bar.high >= stop
        target_hit = not partial and (bar.high >= target if side > 0 else bar.low <= target)
        if stop_hit:
            exit_price = stop * (1.0 - side * .0003)
            reason = "initial_stop" if not (partial or shielded) else (
                "trailing_stop" if partial else "cost_shield"
            )
            break
        if target_hit:
            fraction = min(EXIT.take_fraction, remaining)
            realized += fraction * side * (target / entry - 1.0) - fraction * .0005
            remaining -= fraction
            partial = True
            lock = entry * (1.0 + side * (entry_fee + .0005 + .0002))
            stop = max(stop, lock) if side > 0 else min(stop, lock)
        shield = entry + side * risk * EXIT.shield_r
        shield_hit = bar.high >= shield if side > 0 else bar.low <= shield
        if not partial and not shielded and shield_hit:
            lock = entry * (1.0 + side * (entry_fee + .0005 + .0002))
            stop = max(stop, lock) if side > 0 else min(stop, lock)
            shielded = True
        extreme = max(extreme, bar.high) if side > 0 else min(extreme, bar.low)
        if partial:
            trailing = extreme * (1.0 - side * EXIT.trail_pct)
            stop = max(stop, trailing) if side > 0 else min(stop, trailing)
    else:
        exit_price = values[-1].close
    realized += remaining * side * (exit_price / entry - 1.0) - remaining * .0005
    realized_r = realized / EXIT.stop_pct
    return execution.Outcome(
        values[fill].ts, values[exit_index].ts, setup.symbol, side,
        synthetic.score, entry, realized, reason, mfe_r, mfe_r - realized_r,
    )


def primary_or_miss(setup, values, index_by_ts, end_ms):
    start = index_by_ts.get(setup.ts)
    if start is None or start >= len(values) or values[start].ts >= end_ms:
        return None, None
    limit = setup.close - setup.side * setup.atr * ENTRY.pullback_atr
    crossed = values[start].low <= limit if setup.side > 0 else values[start].high >= limit
    if crossed:
        return execution.replay_one(setup, values, index_by_ts, ENTRY, EXIT, end_ms), None
    return None, start


def recovery_fill(setup, values, start, cfg):
    sign = setup.side
    extreme = setup.close
    escaped = False
    stop = min(len(values), start + 1 + cfg.wait_minutes)
    for index in range(start + 1, stop):
        bar = values[index]
        extreme = max(extreme, bar.high) if sign > 0 else min(extreme, bar.low)
        escape_bps = sign * (extreme / setup.close - 1.0) * 10_000.0
        escaped = escaped or escape_bps >= cfg.min_escape_bps
        pullback_bps = sign * (extreme / bar.low - 1.0) * 10_000.0 if sign > 0 \
            else sign * (extreme / bar.high - 1.0) * 10_000.0
        extension_bps = sign * (bar.close / setup.close - 1.0) * 10_000.0
        location = (bar.close - bar.low) / max(bar.high - bar.low, 1e-12)
        flow = (2.0 * bar.taker_buy_quote / max(bar.quote_volume, 1.0) - 1.0)
        directional = bar.close > bar.open and location >= .60 if sign > 0 \
            else bar.close < bar.open and location <= .40
        if (escaped and pullback_bps >= cfg.min_pullback_bps
                and -5.0 <= extension_bps <= cfg.max_extension_bps
                and directional and sign * flow >= cfg.min_flow):
            return index
    return None


def confirmed_chase_fill(setup, values, start, cfg):
    bar = values[start]
    sign = setup.side
    extension_bps = sign * (bar.close / setup.close - 1.0) * 10_000.0
    flow = 2.0 * bar.taker_buy_quote / max(bar.quote_volume, 1.0) - 1.0
    location = (bar.close - bar.low) / max(bar.high - bar.low, 1e-12)
    directional = bar.close > bar.open and location >= cfg.min_close_location if sign > 0 \
        else bar.close < bar.open and location <= 1.0 - cfg.min_close_location
    return start if (0.0 <= extension_bps <= cfg.max_extension_bps
                     and sign * flow >= cfg.min_flow and directional) else None


def outcomes(setups, minutes_by_symbol, indexes_by_symbol, end_ms, mode, recovery=None,
             confirmed_chase=None):
    rows = []
    missed = recovered = 0
    for setup in setups:
        values = minutes_by_symbol[setup.symbol]
        indexes = indexes_by_symbol[setup.symbol]
        primary, start = primary_or_miss(setup, values, indexes, end_ms)
        if primary is not None:
            rows.append(primary)
            continue
        if start is None:
            continue
        missed += 1
        if mode == "baseline":
            continue
        if mode == "chase":
            rows.append(explicit_outcome(
                setup, values, start, values[start].close, setup.side, end_ms,
                entry_fee=.0005,
            ))
        elif mode == "reverse":
            rows.append(explicit_outcome(
                setup, values, start, values[start].close, -setup.side, end_ms,
                entry_fee=.0005,
            ))
        elif mode == "recovery":
            fill = recovery_fill(setup, values, start, recovery)
            if fill is not None:
                recovered += 1
                rows.append(explicit_outcome(
                    setup, values, fill, values[fill].close, setup.side, end_ms,
                    entry_fee=.0002,
                ))
        elif mode == "confirmed_chase":
            fill = confirmed_chase_fill(setup, values, start, confirmed_chase)
            if fill is not None:
                recovered += 1
                rows.append(explicit_outcome(
                    setup, values, fill, values[fill].close, setup.side, end_ms,
                    entry_fee=.0005,
                ))
    return sorted(rows, key=lambda row: (row.entry_ts, -row.score, row.symbol)), {
        "maker_misses": missed, "recovered": recovered,
    }


def evaluate(setups, minutes, indexes, mode, recovery=None, confirmed_chase=None):
    result = {}
    for name, period in execution.PERIODS.items():
        if name not in setups:
            continue
        rows, counts = outcomes(setups[name], minutes, indexes, period[1], mode, recovery,
                                confirmed_chase)
        summary, trades = execution.portfolio(rows, *period, EXIT)
        result[name] = {"summary": summary, **counts, "trades": trades}
    return result


def selection_score(result):
    train = result["train"]["summary"]
    validation = result["validation"]["summary"]
    return min(train["return_pct"], validation["return_pct"]) \
        - .35 * max(train["max_dd_pct"], validation["max_dd_pct"])


def compact(result):
    return {name: {"summary": value["summary"], "maker_misses": value["maker_misses"],
                   "recovered": value["recovered"]}
            for name, value in result.items()}


def main():
    root = Path("data/alpha-backtest/cache")
    historical = {symbol: base.load_bars(root, symbol) for symbol in base.SYMBOLS}
    minutes = {symbol: minute.load_minutes(root, symbol) for symbol in base.SYMBOLS}
    setups = {name: execution.make_setups(historical, base.SYMBOLS, *period)
              for name, period in execution.PERIODS.items() if name != "recent"}

    recent_symbols = sorted(path.name for path in (root / "klines").iterdir()
                            if path.is_dir() and any(path.glob("*-2026-08-26.zip")))
    recent_bars = {symbol: historical.get(symbol) or base.load_bars(root, symbol)
                   for symbol in recent_symbols}
    setups["recent"] = execution.make_setups(
        recent_bars, recent_symbols, *execution.PERIODS["recent"]
    )
    for symbol in recent_symbols:
        minutes.setdefault(symbol, minute.load_minutes(root, symbol))
    indexes = {symbol: {bar.ts: index for index, bar in enumerate(values)}
               for symbol, values in minutes.items()}

    baseline = evaluate(setups, minutes, indexes, "baseline")
    chase = evaluate(setups, minutes, indexes, "chase")
    reverse = evaluate(setups, minutes, indexes, "reverse")
    grid = []
    for escape in (8.0, 12.0, 20.0):
        for pullback in (5.0, 10.0, 15.0):
            for extension in (20.0, 30.0, 50.0):
                for flow in (0.0, .10, .20):
                    for wait in (3, 5, 8):
                        cfg = RecoveryConfig(escape, pullback, extension, flow, wait)
                        result = evaluate(setups, minutes, indexes, "recovery", cfg)
                        viable = (result["train"]["recovered"] >= 8
                                  and result["validation"]["recovered"] >= 2)
                        grid.append((viable, selection_score(result), cfg, result))
    viable = [row for row in grid if row[0]]
    selected = max(viable or grid, key=lambda row: row[1])
    _, selected_score, selected_cfg, selected_result = selected
    chase_grid = []
    for extension in (6.0, 8.0, 12.0, 20.0, 30.0):
        for flow in (0.0, .10, .20, .30):
            for location in (.55, .60, .65, .70):
                cfg = ConfirmedChaseConfig(extension, flow, location)
                result = evaluate(setups, minutes, indexes, "confirmed_chase",
                                  confirmed_chase=cfg)
                viable_row = (result["train"]["recovered"] >= 5
                              and result["validation"]["recovered"] >= 1)
                chase_grid.append((viable_row, selection_score(result), cfg, result))
    viable_chase = [row for row in chase_grid if row[0]]
    selected_chase = max(viable_chase or chase_grid, key=lambda row: row[1])
    _, chase_score, chase_cfg, chase_result = selected_chase
    report = {
        "strategy": "maker_timeout_pullback_reentry_v1",
        "selection": "recovery selected on train+validation only; locked/recent untouched",
        "cost_model": {"maker_bps": 2, "taker_bps": 5, "exit_bps": 5,
                       "stop_slippage_bps": 3},
        "baseline": compact(baseline),
        "direct_chase": compact(chase),
        "immediate_reverse": compact(reverse),
        "selected_confirmed_chase": {"score": chase_score, "config": asdict(chase_cfg),
                                     **compact(chase_result)},
        "selected_recovery": {"score": selected_score, "config": asdict(selected_cfg),
                              **compact(selected_result)},
        "top_recovery_grid": [
            {"viable": viable_row, "score": score, "config": asdict(cfg),
             "train": compact(result)["train"],
             "validation": compact(result)["validation"]}
            for viable_row, score, cfg, result in sorted(
                grid, key=lambda row: row[1], reverse=True
            )[:20]
        ],
        "top_confirmed_chase_grid": [
            {"viable": viable_row, "score": score, "config": asdict(cfg),
             "train": compact(result)["train"],
             "validation": compact(result)["validation"]}
            for viable_row, score, cfg, result in sorted(
                chase_grid, key=lambda row: row[1], reverse=True
            )[:20]
        ],
    }
    path = Path("data/alpha-backtest/trend-maker-reentry-walkforward.json")
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items()
                      if not key.startswith("top_")}, indent=2))


if __name__ == "__main__":
    main()
