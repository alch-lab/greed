#!/usr/bin/env python3
"""Walk-forward research for direction concentration and soft PF sizing."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

import oi_taker_continuation_backtest as minute
import trend_execution_walkforward as execution
import unified_compound_research as base


@dataclass(frozen=True)
class Control:
    max_same_side: int
    second_same_side_multiplier: float
    soft_pf_enabled: bool
    soft_pf_multiplier: float = .5
    soft_pf_trades: int = 4
    soft_pf_floor: float = .8


def profit_factor(values):
    gains = sum(max(0.0, value) for value in values)
    losses = -sum(min(0.0, value) for value in values)
    return gains / losses if losses else None


def simulate(outcomes, start_ms, end_ms, cfg):
    equity = peak = 2000.0
    max_dd = 0.0
    active = []
    trades = []
    last_exit = defaultdict(lambda: -10**18)
    loss_until = defaultdict(lambda: -10**18)
    side_history = defaultdict(list)
    day_start = {}

    def settle(until):
        nonlocal equity, peak, max_dd, active
        keep = []
        for outcome, notional, multiplier in sorted(active, key=lambda row: row[0].exit_ts):
            if outcome.exit_ts > until:
                keep.append((outcome, notional, multiplier))
                continue
            pnl = notional * outcome.return_on_notional
            equity += pnl
            trades.append({**asdict(outcome), "notional": notional, "pnl": pnl,
                           "size_multiplier": multiplier})
            last_exit[outcome.symbol] = outcome.exit_ts
            side_history[outcome.side].append(outcome.return_on_notional / .0125)
            if pnl < 0:
                loss_until[outcome.symbol] = outcome.exit_ts + 180 * 60_000
            peak = max(peak, equity)
            max_dd = max(max_dd, 1 - equity / peak)
        active = keep

    for outcome in outcomes:
        if not start_ms <= outcome.entry_ts < end_ms:
            continue
        settle(outcome.entry_ts)
        day = outcome.entry_ts // base.DAY_MS
        day_start.setdefault(day, equity)
        if equity < day_start[day] * .975 or equity < peak * .90:
            continue
        same_side = sum(row.side == outcome.side for row, _, _ in active)
        if (len(active) >= 3 or same_side >= cfg.max_same_side
                or any(row.symbol == outcome.symbol for row, _, _ in active)
                or outcome.entry_ts - last_exit[outcome.symbol] < 60 * 60_000
                or outcome.entry_ts < loss_until[outcome.symbol]):
            continue
        multiplier = cfg.second_same_side_multiplier if same_side else 1.0
        recent = side_history[outcome.side][-cfg.soft_pf_trades:]
        recent_pf = profit_factor(recent)
        if (cfg.soft_pf_enabled and len(recent) >= cfg.soft_pf_trades
                and recent_pf is not None and recent_pf < cfg.soft_pf_floor):
            multiplier *= cfg.soft_pf_multiplier
        gross = sum(notional for _, notional, _ in active)
        notional = min(equity * .01 / .0125, equity * 1.5, equity * 4 - gross) * multiplier
        if notional >= 100:
            active.append((outcome, notional, multiplier))
    settle(end_ms)
    for outcome, notional, multiplier in active:
        pnl = notional * outcome.return_on_notional
        equity += pnl
        trades.append({**asdict(outcome), "notional": notional, "pnl": pnl,
                       "size_multiplier": multiplier})
    gains = sum(max(0.0, row["pnl"]) for row in trades)
    losses = -sum(min(0.0, row["pnl"]) for row in trades)
    days = (end_ms - start_ms) / base.DAY_MS
    return {
        "start": 2000.0, "end": equity, "return_pct": equity / 2000.0 - 1,
        "max_dd_pct": max_dd, "trades": len(trades), "trades_per_day": len(trades) / days,
        "win_rate": sum(row["pnl"] > 0 for row in trades) / len(trades) if trades else None,
        "pf": gains / losses if losses else None,
        "average_pnl_usd": sum(row["pnl"] for row in trades) / len(trades) if trades else None,
        "reduced_size_trades": sum(row["size_multiplier"] < .999 for row in trades),
    }


def score(result):
    return min(result["train"]["return_pct"], result["validation"]["return_pct"]) \
        - .35 * max(result["train"]["max_dd_pct"], result["validation"]["max_dd_pct"])


def main():
    root = Path("data/alpha-backtest/cache")
    bars = {symbol: base.load_bars(root, symbol) for symbol in base.SYMBOLS}
    minutes = {symbol: minute.load_minutes(root, symbol) for symbol in base.SYMBOLS}
    setups = {name: execution.make_setups(bars, base.SYMBOLS, *period)
              for name, period in execution.PERIODS.items() if name != "recent"}
    outcomes = {name: execution.make_outcomes(rows, minutes, execution.EntryConfig(.3, 1),
                                               execution.ExitConfig(.0125, 2., .4, .5, .005),
                                               execution.PERIODS[name][1])
                for name, rows in setups.items()}
    configs = [
        Control(max_side, second, soft)
        for max_side in (1, 2, 3)
        for second in (.5, .7, 1.0)
        for soft in (False, True)
        if max_side > 1 or second == 1.0
    ]
    rows = []
    for cfg in configs:
        result = {name: simulate(outcomes[name], *execution.PERIODS[name], cfg)
                  for name in ("train", "validation")}
        result["locked_test"] = simulate(
            outcomes["locked_test"], *execution.PERIODS["locked_test"], cfg
        )
        rows.append((score(result), cfg, result))
    viable = [row for row in rows if row[2]["train"]["return_pct"] > 0
              and row[2]["validation"]["return_pct"] > 0]
    selected_score, selected_cfg, development = max(viable or rows, key=lambda row: row[0])
    baseline_cfg = Control(3, 1.0, False)
    baseline = {name: simulate(outcomes[name], *execution.PERIODS[name], baseline_cfg)
                for name in outcomes}
    locked_baseline = baseline["locked_test"]
    locked_selected = development["locked_test"]
    report = {
        "strategy": "trend_portfolio_controls_v1",
        "selection": "train+validation only; locked test untouched",
        "periods": {name: {"start_ms": bounds[0], "end_ms": bounds[1]}
                    for name, bounds in execution.PERIODS.items() if name != "recent"},
        "baseline": baseline,
        "selected": {"score": selected_score, "config": asdict(selected_cfg),
                     **development},
        "locked_comparison": {
            "return_delta_pct_points": 100 * (
                locked_selected["return_pct"] - locked_baseline["return_pct"]
            ),
            "max_dd_delta_pct_points": 100 * (
                locked_selected["max_dd_pct"] - locked_baseline["max_dd_pct"]
            ),
            "trade_delta": locked_selected["trades"] - locked_baseline["trades"],
        },
        "decision": "reject_controls_locked_test_return_and_trade_count_worse",
        "grid": [{"score": value, "config": asdict(cfg), **result}
                 for value, cfg, result in sorted(rows, key=lambda row: row[0], reverse=True)],
    }
    path = Path("data/alpha-backtest/trend-portfolio-controls-walkforward.json")
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"baseline": baseline, "selected": report["selected"]}, indent=2))


if __name__ == "__main__":
    main()
