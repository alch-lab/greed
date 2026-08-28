#!/usr/bin/env python3
"""Walk-forward test for a confirmed, bounded taker fallback after maker timeout."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import oi_taker_continuation_backtest as minute
import trend_execution_walkforward as execution
import trend_maker_reentry_research as maker
import unified_compound_research as base


@dataclass(frozen=True)
class FallbackConfig:
    max_adverse_bps: float
    min_flow: float
    min_close_location: float
    require_confirmation: bool
    size_multiplier: float


ENTRY = execution.EntryConfig(.30, 1)
EXIT = execution.ExitConfig(.0125, 2.0, .4, .5, .005)
FALLBACK_SLIPPAGE_BPS = 3.0


def outcome_for_setup(setup, values, indexes, end_ms, cfg):
    start = indexes.get(setup.ts)
    if start is None or start >= len(values) or values[start].ts >= end_ms:
        return None, "unavailable"
    bar = values[start]
    limit = setup.close - setup.side * setup.atr * ENTRY.pullback_atr
    maker_fill = bar.low <= limit if setup.side > 0 else bar.high >= limit
    if maker_fill:
        return execution.replay_one(setup, values, indexes, ENTRY, EXIT, end_ms), "maker"

    location = (bar.close - bar.low) / max(bar.high - bar.low, 1e-12)
    flow = 2.0 * bar.taker_buy_quote / max(bar.quote_volume, 1.0) - 1.0
    directional = bar.close > bar.open and location >= cfg.min_close_location \
        if setup.side > 0 else bar.close < bar.open and location <= 1.0 - cfg.min_close_location
    executable = bar.close * (1.0 + setup.side * FALLBACK_SLIPPAGE_BPS / 10_000.0)
    adverse_bps = setup.side * (executable / setup.close - 1.0) * 10_000.0
    confirmed = (not cfg.require_confirmation) or (
        directional and setup.side * flow >= cfg.min_flow
    )
    if not confirmed or adverse_bps > cfg.max_adverse_bps:
        return None, "missed"
    outcome = maker.explicit_outcome(
        setup, values, start, executable, setup.side, end_ms, entry_fee=.0005
    )
    return replace(
        outcome,
        return_on_notional=outcome.return_on_notional * cfg.size_multiplier,
        mfe_r=outcome.mfe_r * cfg.size_multiplier,
        giveback_r=outcome.giveback_r * cfg.size_multiplier,
    ), "fallback"


def evaluate(setups, minutes, indexes, cfg):
    output = {}
    for name, rows in setups.items():
        outcomes = []
        fallback_ids = set()
        counts = {"maker": 0, "fallback": 0, "missed": 0, "unavailable": 0}
        for setup in rows:
            outcome, kind = outcome_for_setup(
                setup, minutes[setup.symbol], indexes[setup.symbol],
                execution.PERIODS[name][1], cfg,
            )
            counts[kind] += 1
            if outcome is not None:
                outcomes.append(outcome)
                if kind == "fallback":
                    fallback_ids.add((outcome.entry_ts, outcome.symbol))
        outcomes.sort(key=lambda row: (row.entry_ts, -row.score, row.symbol))
        summary, trades = execution.portfolio(
            outcomes, *execution.PERIODS[name], EXIT
        )
        fallback_trades = sum(
            (trade["entry_ts"], trade["symbol"]) in fallback_ids for trade in trades
        )
        output[name] = {"summary": summary, "signals": counts,
                        "portfolio_fallback_trades": fallback_trades}
    return output


def development_score(result):
    train = result["train"]["summary"]
    validation = result["validation"]["summary"]
    return min(train["return_pct"], validation["return_pct"]) \
        - .35 * max(train["max_dd_pct"], validation["max_dd_pct"])


def compact(result):
    return {name: value for name, value in result.items()}


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

    baseline_cfg = FallbackConfig(-10_000.0, 1.0, 1.0, True, 0.0)
    baseline = evaluate(setups, minutes, indexes, baseline_cfg)
    grid = []
    configs = [
        FallbackConfig(adverse, 0.0, 0.0, False, size)
        for adverse in (-8.0, -4.0, 0.0, 4.0, 6.0, 8.0, 12.0, 20.0, 30.0, 50.0, 10_000.0)
        for size in (.05, .10, .25, .50)
    ] + [
        FallbackConfig(adverse, flow, location, True, size)
        for adverse in (0.0, 4.0, 6.0, 8.0, 12.0)
        for flow in (-.10, 0.0, .10, .20)
        for location in (.55, .60, .65, .70)
        for size in (.25, .50, 1.0)
    ]
    for cfg in configs:
        result = evaluate(setups, minutes, indexes, cfg)
        # The user-facing objective is a meaningfully more active sleeve, not
        # a parameter that happens to recover one or two orders. Apply that
        # requirement on development data before opening locked/recent results.
        viable = (result["train"]["summary"]["trades"] >= 35
                  and result["validation"]["summary"]["trades"] >= 7
                  and result["train"]["summary"]["return_pct"] > 0
                  and result["validation"]["summary"]["return_pct"] > 0)
        grid.append((viable, development_score(result), cfg, result))
    viable = [row for row in grid if row[0]]
    selected = max(viable or grid, key=lambda row: row[1])
    viable_row, score, cfg, result = selected
    report = {
        "strategy": "confirmed_bounded_taker_fallback_v1",
        "selection": "parameters selected on train+validation; locked/recent untouched",
        "cost_model": {"maker_entry_bps": 2, "fallback_fee_bps": 5,
                       "fallback_slippage_bps": FALLBACK_SLIPPAGE_BPS,
                       "exit_bps": 5, "stop_slippage_bps": 3},
        "baseline": compact(baseline),
        "selected": {"viable": viable_row, "score": score, "config": asdict(cfg),
                     **compact(result)},
        "top_grid": [{"viable": ok, "score": value, "config": asdict(config),
                      "train": rows["train"], "validation": rows["validation"],
                      "locked_test": rows["locked_test"], "recent": rows["recent"]}
                     for ok, value, config, rows in sorted(
                         grid, key=lambda row: row[1], reverse=True
                     )[:30]],
    }
    baseline_locked = baseline["locked_test"]["summary"]
    selected_locked = result["locked_test"]["summary"]
    report["locked_comparison"] = {
        "return_delta_pct_points": 100 * (
            selected_locked["return_pct"] - baseline_locked["return_pct"]
        ),
        "max_dd_delta_pct_points": 100 * (
            selected_locked["max_dd_pct"] - baseline_locked["max_dd_pct"]
        ),
        "trade_delta": selected_locked["trades"] - baseline_locked["trades"],
    }
    report["decision"] = (
        "deploy_small_bounded_fallback"
        if viable_row
        and selected_locked["return_pct"] >= baseline_locked["return_pct"]
        and result["recent"]["summary"]["return_pct"] > 0
        else "reject_fallback_family"
    )
    path = Path("data/alpha-backtest/trend-bounded-fallback-walkforward.json")
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "top_grid"}, indent=2))


if __name__ == "__main__":
    main()
