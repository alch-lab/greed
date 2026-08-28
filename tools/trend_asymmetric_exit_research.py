#!/usr/bin/env python3
"""Walk-forward study for trend loss asymmetry and pre-TP profit retention.

The signal detector and passive entry stay fixed.  Exit parameters and dollar
risk are selected on train+validation only; the locked window is opened once
after selection.  One-minute candles resolve ambiguous stop/target ordering
against the strategy and include maker entry, taker exit and stop slippage.
"""

from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

import oi_taker_continuation_backtest as minute
import trend_execution_walkforward as execution
import unified_compound_research as base


@dataclass(frozen=True)
class ExitPolicy:
    stop_pct: float
    target_r: float
    take_fraction: float
    shield_r: float
    trail_activation_r: float
    trail_pct: float
    early_minutes: int
    early_adverse_r: float
    early_max_mfe_r: float


@dataclass(frozen=True)
class RiskPolicy:
    risk_per_trade_pct: float


ENTRY = execution.EntryConfig(.30, 1)
BASELINE_EXIT = ExitPolicy(.0125, 2.0, .40, .50, .80, .005, 0, 0.0, 0.0)
BASELINE_RISK = RiskPolicy(.010)


def replay_one(setup, values, index_by_ts, policy, end_ms, *,
               entry_fee=.0002, exit_fee=.0005, stop_slip=.0003):
    start = index_by_ts.get(setup.ts)
    if start is None:
        return None
    limit = setup.close - setup.side * setup.atr * ENTRY.pullback_atr
    crossed = values[start].low <= limit if setup.side > 0 else values[start].high >= limit
    if not crossed or values[start].ts >= end_ms:
        return None
    entry = limit
    risk = entry * policy.stop_pct
    stop = entry - setup.side * risk
    target = entry + setup.side * risk * policy.target_r
    remaining = 1.0
    realized = -entry_fee
    partial = shielded = False
    extreme = entry
    mfe_r = 0.0
    exit_index = start
    exit_price = entry
    reason = "window_mark"
    for index in range(start + 1, len(values)):
        bar = values[index]
        if bar.ts >= end_ms:
            exit_index = max(start, index - 1)
            exit_price = values[exit_index].close
            break
        exit_index = index
        favorable = bar.high - entry if setup.side > 0 else entry - bar.low
        mfe_r = max(mfe_r, favorable / max(risk, 1e-12))
        stop_hit = bar.low <= stop if setup.side > 0 else bar.high >= stop
        target_hit = not partial and (bar.high >= target if setup.side > 0 else bar.low <= target)
        if stop_hit:
            exit_price = stop * (1.0 - setup.side * stop_slip)
            reason = "initial_stop" if not (partial or shielded) else (
                "trailing_stop" if partial else "protected_stop"
            )
            break
        if target_hit:
            fraction = min(policy.take_fraction, remaining)
            realized += fraction * setup.side * (target / entry - 1.0) - fraction * exit_fee
            remaining -= fraction
            partial = True
            lock = entry * (1.0 + setup.side * (entry_fee + exit_fee + .0002))
            stop = max(stop, lock) if setup.side > 0 else min(stop, lock)
        shield_price = entry + setup.side * risk * policy.shield_r
        shield_hit = bar.high >= shield_price if setup.side > 0 else bar.low <= shield_price
        if not shielded and shield_hit:
            lock = entry * (1.0 + setup.side * (entry_fee + exit_fee + .0002))
            stop = max(stop, lock) if setup.side > 0 else min(stop, lock)
            shielded = True
        extreme = max(extreme, bar.high) if setup.side > 0 else min(extreme, bar.low)
        if mfe_r >= policy.trail_activation_r:
            trailing = extreme * (1.0 - setup.side * policy.trail_pct)
            stop = max(stop, trailing) if setup.side > 0 else min(stop, trailing)
        if policy.early_minutes > 0 and index - start >= policy.early_minutes:
            close_r = setup.side * (bar.close / entry - 1.0) / policy.stop_pct
            if mfe_r < policy.early_max_mfe_r and close_r <= -policy.early_adverse_r:
                exit_price = bar.close
                reason = "early_failure"
                break
    else:
        exit_price = values[-1].close
    realized += remaining * setup.side * (exit_price / entry - 1.0) - remaining * exit_fee
    realized_r = realized / policy.stop_pct
    return execution.Outcome(
        values[start].ts, values[exit_index].ts, setup.symbol, setup.side,
        setup.score, entry, realized, reason, mfe_r, mfe_r - realized_r,
    )


def outcomes(setups, minutes_by_symbol, indexes, policy, end_ms, **costs):
    rows = []
    for setup in setups:
        row = replay_one(setup, minutes_by_symbol[setup.symbol], indexes[setup.symbol],
                         policy, end_ms, **costs)
        if row is not None:
            rows.append(row)
    return sorted(rows, key=lambda row: (row.entry_ts, -row.score, row.symbol))


def simulate(rows, start_ms, end_ms, exit_policy, risk_policy):
    equity = peak = 2000.0
    max_dd = 0.0
    active = []
    trades = []
    last_exit = defaultdict(lambda: -10**18)
    loss_until = defaultdict(lambda: -10**18)
    day_start = {}

    def settle(until):
        nonlocal equity, peak, max_dd, active
        keep = []
        for outcome, notional in sorted(active, key=lambda item: item[0].exit_ts):
            if outcome.exit_ts > until:
                keep.append((outcome, notional))
                continue
            pnl = notional * outcome.return_on_notional
            equity += pnl
            trades.append({**asdict(outcome), "notional": notional, "pnl": pnl})
            last_exit[outcome.symbol] = outcome.exit_ts
            if pnl < 0:
                loss_until[outcome.symbol] = outcome.exit_ts + 180 * 60_000
            peak = max(peak, equity)
            max_dd = max(max_dd, 1.0 - equity / peak)
        active = keep

    for outcome in rows:
        if not start_ms <= outcome.entry_ts < end_ms:
            continue
        settle(outcome.entry_ts)
        day = outcome.entry_ts // base.DAY_MS
        day_start.setdefault(day, equity)
        if equity < day_start[day] * .975 or equity < peak * .90:
            continue
        if (len(active) >= 3
                or any(item.symbol == outcome.symbol for item, _ in active)
                or outcome.entry_ts - last_exit[outcome.symbol] < 60 * 60_000
                or outcome.entry_ts < loss_until[outcome.symbol]):
            continue
        gross = sum(notional for _, notional in active)
        notional = min(
            equity * risk_policy.risk_per_trade_pct / exit_policy.stop_pct,
            equity * 1.5,
            equity * 4.0 - gross,
        )
        if notional >= 100:
            active.append((outcome, notional))
    settle(end_ms)
    for outcome, notional in active:
        pnl = notional * outcome.return_on_notional
        equity += pnl
        trades.append({**asdict(outcome), "notional": notional, "pnl": pnl})
    gains = sum(max(0.0, row["pnl"]) for row in trades)
    losses = -sum(min(0.0, row["pnl"]) for row in trades)
    pnl_values = [row["pnl"] for row in trades]
    days = (end_ms - start_ms) / base.DAY_MS
    return {
        "start": 2000.0,
        "end": equity,
        "return_pct": equity / 2000.0 - 1.0,
        "max_dd_pct": max_dd,
        "trades": len(trades),
        "trades_per_day": len(trades) / days,
        "win_rate": sum(value > 0 for value in pnl_values) / len(pnl_values) if pnl_values else None,
        "pf": gains / losses if losses else None,
        "average_pnl_usd": statistics.mean(pnl_values) if pnl_values else None,
        "average_win_usd": statistics.mean(value for value in pnl_values if value > 0) if gains else None,
        "average_loss_usd": statistics.mean(value for value in pnl_values if value < 0) if losses else None,
        "worst_trade_usd": min(pnl_values, default=None),
        "average_giveback_r": statistics.mean(row["giveback_r"] for row in trades) if trades else None,
        "exit_reasons": dict(Counter(row["reason"] for row in trades)),
    }, trades


def development_score(result):
    train = result["train"]
    validation = result["validation"]
    return min(train["return_pct"], validation["return_pct"]) \
        - .50 * max(train["max_dd_pct"], validation["max_dd_pct"])


def evaluate(period_rows, exit_policy, risk_policy):
    return {
        name: simulate(period_rows[name], *execution.PERIODS[name], exit_policy, risk_policy)[0]
        for name in ("train", "validation")
    }


def exit_grid():
    early = ((0, 0.0, 0.0), (2, .35, .15), (3, .35, .15),
             (3, .50, .20), (5, .50, .20))
    for target in (1.0, 1.25, 1.5, 2.0):
        for fraction in (.40, .50, .60):
            for shield in (.40, .50):
                for activation in (.60, .80, 1.00):
                    for trail in (.003, .004, .005):
                        for minutes, adverse, max_mfe in early:
                            if shield >= target:
                                continue
                            yield ExitPolicy(.0125, target, fraction, shield, activation, trail,
                                             minutes, adverse, max_mfe)


def main():
    root = Path("data/alpha-backtest/cache")
    bars = {symbol: base.load_bars(root, symbol) for symbol in base.SYMBOLS}
    minutes = {symbol: minute.load_minutes(root, symbol) for symbol in base.SYMBOLS}
    indexes = {symbol: {bar.ts: index for index, bar in enumerate(values)}
               for symbol, values in minutes.items()}
    setups = {name: execution.make_setups(bars, base.SYMBOLS, *period)
              for name, period in execution.PERIODS.items()}

    baseline_rows = {name: outcomes(setups[name], minutes, indexes, BASELINE_EXIT,
                                    execution.PERIODS[name][1]) for name in setups}
    baseline = {name: simulate(baseline_rows[name], *execution.PERIODS[name],
                               BASELINE_EXIT, BASELINE_RISK)[0] for name in setups}

    grid = []
    for policy in exit_grid():
        rows = {name: outcomes(setups[name], minutes, indexes, policy,
                               execution.PERIODS[name][1])
                for name in ("train", "validation")}
        for risk in (RiskPolicy(.006), RiskPolicy(.007), RiskPolicy(.008), RiskPolicy(.010)):
            result = {name: simulate(rows[name], *execution.PERIODS[name], policy, risk)[0]
                      for name in rows}
            viable = (result["train"]["trades"] >= 15
                      and result["validation"]["trades"] >= 5
                      and result["train"]["return_pct"] > 0
                      and result["validation"]["return_pct"] > 0
                      and (result["train"]["pf"] or 0) > 1
                      and (result["validation"]["pf"] or 0) > 1)
            grid.append((viable, development_score(result), policy, risk, result))
    viable = [row for row in grid if row[0]]
    _, score, selected_exit, selected_risk, development = max(
        viable or grid, key=lambda row: row[1]
    )
    selected_rows = {name: outcomes(setups[name], minutes, indexes, selected_exit,
                                    execution.PERIODS[name][1]) for name in setups}
    locked, locked_trades = simulate(selected_rows["locked_test"],
                                     *execution.PERIODS["locked_test"],
                                     selected_exit, selected_risk)
    stress_rows = outcomes(setups["locked_test"], minutes, indexes, selected_exit,
                           execution.PERIODS["locked_test"][1],
                           entry_fee=.0005, exit_fee=.0010, stop_slip=.0005)
    locked_stress = simulate(stress_rows, *execution.PERIODS["locked_test"],
                             selected_exit, selected_risk)[0]
    recent = simulate(selected_rows["recent"], *execution.PERIODS["recent"],
                      selected_exit, selected_risk)[0]
    ranked = sorted(viable or grid, key=lambda row: row[1], reverse=True)
    family = []
    seen = set()
    for _, value, policy, risk, _ in ranked:
        key = (policy, risk)
        if key in seen:
            continue
        seen.add(key)
        rows = outcomes(setups["locked_test"], minutes, indexes, policy,
                        execution.PERIODS["locked_test"][1])
        result = simulate(rows, *execution.PERIODS["locked_test"], policy, risk)[0]
        family.append({"development_score": value, "exit": asdict(policy),
                       "risk": asdict(risk), "locked_test": result})
        if len(family) == 20:
            break
    report = {
        "strategy": "trend_asymmetric_exit_v1",
        "selection": "exit and risk selected on train+validation only; locked test untouched",
        "cost_model": {"maker_entry_bps": 2, "taker_exit_bps": 5,
                       "stop_slippage_bps": 3},
        "baseline": baseline,
        "selected": {"score": score, "exit": asdict(selected_exit),
                     "risk": asdict(selected_risk), **development,
                     "locked_test": locked,
                     "recent_2026_08_24_to_26": recent,
                     "locked_stress_5bps_entry_10bps_exit": locked_stress},
        "locked_comparison": {
            "net_usd_delta": locked["end"] - baseline["locked_test"]["end"],
            "max_dd_pct_point_delta": 100 * (
                locked["max_dd_pct"] - baseline["locked_test"]["max_dd_pct"]),
            "worst_trade_usd_delta": (
                locked["worst_trade_usd"] - baseline["locked_test"]["worst_trade_usd"]),
        },
        "grid_size": len(grid),
        "viable": len(viable),
        "top_family_locked_robustness": {
            "count": len(family),
            "profitable_fraction": sum(
                row["locked_test"]["return_pct"] > 0 for row in family
            ) / len(family),
            "median_return_pct": statistics.median(
                row["locked_test"]["return_pct"] for row in family
            ),
            "members": family,
        },
        "top_development": [
            {"score": value, "exit": asdict(policy), "risk": asdict(risk), **result}
            for _, value, policy, risk, result in ranked[:20]
        ],
        "locked_test_trades": locked_trades,
    }
    path = Path("data/alpha-backtest/trend-asymmetric-exit-walkforward.json")
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items()
                      if key not in ("top_development", "locked_test_trades",
                                     "top_family_locked_robustness")}, indent=2))


if __name__ == "__main__":
    main()
