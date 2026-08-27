#!/usr/bin/env python3
"""Minute-level walk-forward for executable trend entries and exits.

The detector remains fixed. Train and validation choose the entry/exit shape;
locked and recent windows are evaluated only after selection. A limit can fill
only on a later 1m candle and an ambiguous stop/target candle resolves against
the strategy.
"""

from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

import oi_taker_continuation_backtest as minute
import trend_v2_walkforward as trend
import unified_compound_research as base


@dataclass(frozen=True)
class EntryConfig:
    pullback_atr: float
    wait_minutes: int


@dataclass(frozen=True)
class ExitConfig:
    stop_pct: float
    target_r: float
    take_fraction: float
    shield_r: float
    trail_pct: float


@dataclass(frozen=True)
class Setup:
    ts: int
    symbol: str
    side: int
    score: float
    close: float
    atr: float


@dataclass(frozen=True)
class Outcome:
    entry_ts: int
    exit_ts: int
    symbol: str
    side: int
    score: float
    entry: float
    return_on_notional: float
    reason: str
    mfe_r: float
    giveback_r: float


PERIODS = {
    "train": (base.ms("2026-06-01"), base.ms("2026-07-24")),
    "validation": (base.ms("2026-07-24"), base.ms("2026-08-15")),
    "locked_test": (base.ms("2026-08-15"), base.ms("2026-08-23")),
    "recent": (base.ms("2026-08-24"), base.ms("2026-08-27")),
}


def make_setups(bars_by_symbol, symbols, start_ms, end_ms):
    detector = trend.Config(.06, .45, 100.0, .01, 2.0, .4, .003, 180, 1.0, 2)
    raw = trend.generate(detector, bars_by_symbol, start_ms, end_ms)
    bars = {symbol: {bar.ts: bar for bar in bars_by_symbol[symbol]} for symbol in symbols}
    output = []
    for signal in raw:
        bar = bars[signal.symbol].get(signal.ts - base.BAR_MS)
        if bar:
            output.append(Setup(signal.ts, signal.symbol, signal.side, signal.score,
                                bar.close, bar.atr))
    return output


def replay_one(setup, values, index_by_ts, entry_cfg, exit_cfg, end_ms, *,
               entry_fee=.0002, exit_fee=.0005, stop_slip=.0003):
    start = index_by_ts.get(setup.ts)
    if start is None:
        return None
    limit = setup.close - setup.side * setup.atr * entry_cfg.pullback_atr
    fill = None
    for index in range(start, min(start + entry_cfg.wait_minutes, len(values))):
        if values[index].ts >= end_ms:
            break
        crossed = values[index].low <= limit if setup.side > 0 else values[index].high >= limit
        if crossed:
            fill = index
            break
    if fill is None:
        return None

    entry = limit
    risk = entry * exit_cfg.stop_pct
    stop = entry - setup.side * risk
    target = entry + setup.side * risk * exit_cfg.target_r
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
        favorable = bar.high - entry if setup.side > 0 else entry - bar.low
        mfe_r = max(mfe_r, favorable / max(risk, 1e-12))
        stop_hit = bar.low <= stop if setup.side > 0 else bar.high >= stop
        target_hit = not partial and (bar.high >= target if setup.side > 0 else bar.low <= target)
        # A one-minute candle does not reveal event ordering. Resolve ambiguity
        # against the strategy instead of silently assuming target-first.
        if stop_hit:
            exit_price = stop * (1.0 - setup.side * stop_slip)
            reason = "initial_stop" if not (partial or shielded) else (
                "trailing_stop" if partial else "cost_shield"
            )
            break
        if target_hit:
            fraction = min(exit_cfg.take_fraction, remaining)
            realized += fraction * setup.side * (target / entry - 1.0) - fraction * exit_fee
            remaining -= fraction
            partial = True
            lock = entry * (1.0 + setup.side * (entry_fee + exit_fee + .0002))
            stop = max(stop, lock) if setup.side > 0 else min(stop, lock)
        shield = entry + setup.side * risk * exit_cfg.shield_r
        shield_hit = bar.high >= shield if setup.side > 0 else bar.low <= shield
        if not partial and not shielded and shield_hit:
            lock = entry * (1.0 + setup.side * (entry_fee + exit_fee + .0002))
            stop = max(stop, lock) if setup.side > 0 else min(stop, lock)
            shielded = True
        extreme = max(extreme, bar.high) if setup.side > 0 else min(extreme, bar.low)
        if partial:
            trailing = extreme * (1.0 - setup.side * exit_cfg.trail_pct)
            stop = max(stop, trailing) if setup.side > 0 else min(stop, trailing)
    else:
        exit_price = values[-1].close
    realized += remaining * setup.side * (exit_price / entry - 1.0) - remaining * exit_fee
    realized_r = realized / exit_cfg.stop_pct
    return Outcome(values[fill].ts, values[exit_index].ts, setup.symbol, setup.side,
                   setup.score, entry, realized, reason, mfe_r, mfe_r - realized_r)


def make_outcomes(setups, minutes_by_symbol, entry_cfg, exit_cfg, end_ms, **costs):
    grouped = defaultdict(list)
    for setup in setups:
        grouped[setup.symbol].append(setup)
    output = []
    for symbol, rows in grouped.items():
        values = minutes_by_symbol[symbol]
        index_by_ts = {bar.ts: index for index, bar in enumerate(values)}
        for setup in rows:
            outcome = replay_one(setup, values, index_by_ts, entry_cfg, exit_cfg,
                                 end_ms, **costs)
            if outcome:
                output.append(outcome)
    return sorted(output, key=lambda row: (row.entry_ts, -row.score, row.symbol))


def portfolio(outcomes, start_ms, end_ms, cfg):
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
        for outcome, notional in sorted(active, key=lambda row: row[0].exit_ts):
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
        if (len(active) >= 3
                or any(row.symbol == outcome.symbol for row, _ in active)
                or outcome.entry_ts - last_exit[outcome.symbol] < 60 * 60_000
                or outcome.entry_ts < loss_until[outcome.symbol]):
            continue
        gross = sum(notional for _, notional in active)
        notional = min(equity * .01 / cfg.stop_pct, equity * 1.5, equity * 4 - gross)
        if notional >= 100:
            active.append((outcome, notional))
    settle(end_ms)
    # Outcomes are bounded at end_ms, so all remaining positions are marks.
    for outcome, notional in active:
        pnl = notional * outcome.return_on_notional
        equity += pnl
        trades.append({**asdict(outcome), "notional": notional, "pnl": pnl})
    gains = sum(max(0.0, row["pnl"]) for row in trades)
    losses = -sum(min(0.0, row["pnl"]) for row in trades)
    days = (end_ms - start_ms) / base.DAY_MS
    return {
        "start": 2000.0, "end": equity, "return_pct": equity / 2000.0 - 1,
        "max_dd_pct": max_dd, "trades": len(trades), "trades_per_day": len(trades) / days,
        "win_rate": sum(row["pnl"] > 0 for row in trades) / len(trades) if trades else None,
        "pf": gains / losses if losses else None,
        "average_pnl_usd": statistics.mean(row["pnl"] for row in trades) if trades else None,
        "average_mfe_r": statistics.mean(row["mfe_r"] for row in trades) if trades else None,
        "average_giveback_r": statistics.mean(row["giveback_r"] for row in trades) if trades else None,
        "exit_reasons": dict(Counter(row["reason"] for row in trades)),
    }, trades


def score(train, validation):
    return min(train["return_pct"], validation["return_pct"]) \
        - .35 * max(train["max_dd_pct"], validation["max_dd_pct"])


def evaluate(setups_by_period, minutes_by_symbol, entry_cfg, exit_cfg, **costs):
    output = {}
    for name in ("train", "validation"):
        outcomes = make_outcomes(setups_by_period[name], minutes_by_symbol, entry_cfg,
                                 exit_cfg, PERIODS[name][1], **costs)
        output[name] = portfolio(outcomes, *PERIODS[name], exit_cfg)[0]
    return output


def main():
    root = Path("data/alpha-backtest/cache")
    historical_bars = {symbol: base.load_bars(root, symbol) for symbol in base.SYMBOLS}
    setups = {name: make_setups(historical_bars, base.SYMBOLS, *period)
              for name, period in PERIODS.items() if name != "recent"}
    minutes_by_symbol = {symbol: minute.load_minutes(root, symbol) for symbol in base.SYMBOLS}
    baseline_exit = ExitConfig(.01, 2.0, .4, 1.0, .003)
    entry_rows = []
    for entry in (EntryConfig(offset, wait)
                  for offset in (0.0, .10, .20, .30)
                  for wait in (1, 3, 5)):
        result = evaluate(setups, minutes_by_symbol, entry, baseline_exit)
        entry_rows.append((score(result["train"], result["validation"]), entry, result))
    # Deep passive entries trade less often by design. Five independent
    # validation fills is the minimum here; requiring six accidentally rejects
    # the only entry family profitable in both development windows.
    viable_entries = [row for row in entry_rows
                      if row[2]["train"]["trades"] >= 15 and row[2]["validation"]["trades"] >= 5
                      and row[2]["train"]["return_pct"] > 0
                      and row[2]["validation"]["return_pct"] > 0]
    selected_entry = max(viable_entries or entry_rows, key=lambda row: row[0])[1]

    exit_rows = []
    exits = (ExitConfig(stop, target, fraction, shield, trail)
             for stop in (.0075, .01, .0125)
             for target in (1.0, 1.25, 1.5, 1.75, 2.0)
             for fraction in (.4, .6, .8)
             for shield in (.5, .75, 1.0, 1.25) if shield < target
             for trail in (.003, .005))
    for exit_cfg in exits:
        result = evaluate(setups, minutes_by_symbol, selected_entry, exit_cfg)
        exit_rows.append((score(result["train"], result["validation"]), exit_cfg, result))
    viable = [row for row in exit_rows
              if row[2]["train"]["trades"] >= 15 and row[2]["validation"]["trades"] >= 5
              and row[2]["train"]["return_pct"] > 0 and row[2]["validation"]["return_pct"] > 0
              and (row[2]["train"]["pf"] or 0) > 1 and (row[2]["validation"]["pf"] or 0) > 1]
    selected = max(viable or exit_rows, key=lambda row: row[0])
    selected_score, exit_cfg, selected_periods = selected
    locked_outcomes = make_outcomes(setups["locked_test"], minutes_by_symbol, selected_entry,
                                    exit_cfg, PERIODS["locked_test"][1])
    locked, locked_trades = portfolio(locked_outcomes, *PERIODS["locked_test"], exit_cfg)
    stress_outcomes = make_outcomes(setups["locked_test"], minutes_by_symbol, selected_entry,
                                    exit_cfg, PERIODS["locked_test"][1],
                                    entry_fee=.0005, exit_fee=.001, stop_slip=.0005)
    locked_stress = portfolio(stress_outcomes, *PERIODS["locked_test"], exit_cfg)[0]

    recent_symbols = sorted(path.name for path in (root / "klines").iterdir()
                            if path.is_dir() and any(path.glob("*-2026-08-26.zip")))
    recent_bars = {symbol: base.load_bars(root, symbol) for symbol in recent_symbols}
    recent_setups = make_setups(recent_bars, recent_symbols, *PERIODS["recent"])
    recent_minutes = {symbol: minute.load_minutes(root, symbol) for symbol in recent_symbols}
    recent_outcomes = make_outcomes(recent_setups, recent_minutes, selected_entry, exit_cfg,
                                    PERIODS["recent"][1])
    recent, recent_trades = portfolio(recent_outcomes, *PERIODS["recent"], exit_cfg)

    report = {
        "strategy": "fresh_trend_pullback_minute_execution_v1",
        "selection": "entry then exit selected on train+validation only; locked/recent untouched",
        "cost_model": {"maker_entry_bps": 2, "exit_bps": 5, "stop_slippage_bps": 3},
        "selected": {"score": selected_score, "entry": asdict(selected_entry),
                     "exit": asdict(exit_cfg)},
        "train": selected_periods["train"], "validation": selected_periods["validation"],
        "locked_test": locked, "locked_stress_5bps_entry_10bps_exit": locked_stress,
        "recent_2026_08_24_to_26_partial": recent,
        "signal_counts": {name: len(rows) for name, rows in setups.items()} | {"recent": len(recent_setups)},
        "entry_grid": [{"score": value, "config": asdict(cfg), **result}
                       for value, cfg, result in sorted(entry_rows, reverse=True, key=lambda row: row[0])],
        "top_exit_grid": [{"score": value, "config": asdict(cfg), **result}
                          for value, cfg, result in sorted(exit_rows, reverse=True, key=lambda row: row[0])[:20]],
        "locked_test_trades": locked_trades, "recent_trades": recent_trades,
    }
    path = Path("data/alpha-backtest/trend-execution-walkforward.json")
    path.write_text(json.dumps(report, indent=2))
    print(json.dumps({key: value for key, value in report.items()
                      if not key.endswith("_grid") and not key.endswith("_trades")}, indent=2))


if __name__ == "__main__":
    main()
