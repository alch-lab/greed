#!/usr/bin/env python3
"""Walk-forward research for a mechanical swing-failure-pattern lane.

Signals use completed 15m/1h bars.  A bar must sweep a mature structural
extreme, close back inside it, and leave a directional rejection wick.  Entry
requires a later break of the rejection bar; stop/target collisions are
resolved in the adverse direction.  The locked test is not consulted during
selection.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from price_action_backtest import CORE_SYMBOLS, FiveBar, MINUTE_MS, load_minutes


DAY_MS = 24 * 60 * MINUTE_MS


@dataclass(frozen=True, slots=True)
class Config:
    bar_minutes: int
    lookback_bars: int
    min_sweep_atr: float
    max_sweep_atr: float
    min_volume_ratio: float
    target_r: float
    max_hold_minutes: int


@dataclass(slots=True)
class Trade:
    symbol: str
    side: int
    signal_ms: int
    entry_ms: int
    exit_ms: int
    entry: float
    stop: float
    exit: float
    stop_fraction: float
    gross_return: float
    reason: str
    sweep_atr: float
    volume_ratio: float


def iso_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def aggregate(minutes, bar_minutes: int) -> list[FiveBar]:
    interval_ms = bar_minutes * MINUTE_MS
    output = []
    for offset in range(0, len(minutes), bar_minutes):
        group = minutes[offset:offset + bar_minutes]
        if len(group) != bar_minutes:
            continue
        bucket = group[0].ts - group[0].ts % interval_ms
        if any(value.ts != bucket + index * MINUTE_MS for index, value in enumerate(group)):
            continue
        output.append(FiveBar(
            bucket, group[0].open, max(value.high for value in group),
            min(value.low for value in group), group[-1].close,
            sum(value.quote for value in group),
        ))
    ema = None
    atr = None
    previous_close = None
    for value in output:
        ema = value.close if ema is None else (2 / 21) * value.close + (19 / 21) * ema
        previous = value.close if previous_close is None else previous_close
        true_range = max(
            value.high - value.low, abs(value.high - previous), abs(value.low - previous)
        )
        atr = true_range if atr is None else (2 / 15) * true_range + (13 / 15) * atr
        value.ema20 = ema
        value.atr = atr
        previous_close = value.close
    return output


def candidates(symbol: str, minutes, bars, cfg: Config) -> list[tuple]:
    output = []
    last_signal = {1: -10_000, -1: -10_000}
    warmup = max(cfg.lookback_bars, 48)
    for index in range(warmup, len(bars)):
        bar = bars[index]
        history = bars[index - cfg.lookback_bars:index]
        baseline = statistics.median(value.quote for value in bars[index - 36:index])
        volume_ratio = bar.quote / max(baseline, 1.0)
        if volume_ratio < cfg.min_volume_ratio:
            continue
        prior_high = max(value.high for value in history)
        prior_low = min(value.low for value in history)
        span = max(bar.high - bar.low, 1e-12)
        close_location = (bar.close - bar.low) / span
        high_index = max(range(index - cfg.lookback_bars, index), key=lambda value: bars[value].high)
        low_index = min(range(index - cfg.lookback_bars, index), key=lambda value: bars[value].low)
        for side, level, extreme, level_index in (
            (1, prior_low, bar.low, low_index), (-1, prior_high, bar.high, high_index)
        ):
            if index - last_signal[side] < 12:
                continue
            if index - level_index < 6:
                continue
            sweep = side * (level - extreme)
            reclaimed = bar.close > level if side > 0 else bar.close < level
            rejection = close_location >= 0.60 if side > 0 else close_location <= 0.40
            directional = bar.close > bar.open if side > 0 else bar.close < bar.open
            sweep_atr = sweep / max(bar.atr, 1e-12)
            if not (
                cfg.min_sweep_atr <= sweep_atr <= cfg.max_sweep_atr
                and reclaimed
                and rejection
                and directional
            ):
                continue
            bar_ms = cfg.bar_minutes * MINUTE_MS
            signal_close = bar.ts + bar_ms
            minute_index = max(0, math.ceil((signal_close - minutes[0].ts) / MINUTE_MS))
            if minute_index >= len(minutes):
                continue
            stop = extreme - 0.10 * bar.atr if side > 0 else extreme + 0.10 * bar.atr
            trigger = bar.high if side > 0 else bar.low
            fill = None
            for candidate_index in range(
                minute_index,
                min(len(minutes), minute_index + 3 * cfg.bar_minutes),
            ):
                minute = minutes[candidate_index]
                invalid = minute.low <= stop if side > 0 else minute.high >= stop
                hit = minute.high >= trigger if side > 0 else minute.low <= trigger
                if invalid:
                    break
                if hit:
                    entry = max(trigger, minute.open) if side > 0 else min(trigger, minute.open)
                    fill = (candidate_index, entry)
                    break
            if fill is None:
                continue
            minute_index, entry = fill
            risk = side * (entry - stop)
            stop_fraction = risk / max(entry, 1e-12)
            if risk <= 0 or not 0.003 <= stop_fraction <= 0.03:
                continue
            output.append((side, signal_close, minute_index, entry, stop, sweep_atr, volume_ratio))
            last_signal[side] = index
    return output


def replay(symbol: str, minutes, signal, cfg: Config) -> Trade:
    side, signal_ms, entry_index, entry, stop, sweep_atr, volume_ratio = signal
    risk = side * (entry - stop)
    target = entry + side * cfg.target_r * risk
    exit_price = minutes[entry_index].close
    exit_ms = minutes[entry_index].ts + MINUTE_MS
    reason = "time"
    end = min(len(minutes), entry_index + cfg.max_hold_minutes)
    for value in minutes[entry_index:end]:
        stop_hit = value.low <= stop if side > 0 else value.high >= stop
        target_hit = value.high >= target if side > 0 else value.low <= target
        if stop_hit:
            exit_price = min(stop, value.open) if side > 0 else max(stop, value.open)
            exit_ms = value.ts + MINUTE_MS
            reason = "stop"
            break
        if target_hit:
            exit_price = target
            exit_ms = value.ts + MINUTE_MS
            reason = "target"
            break
        exit_price = value.close
        exit_ms = value.ts + MINUTE_MS
    return Trade(
        symbol, side, signal_ms, minutes[entry_index].ts, exit_ms, entry, stop,
        exit_price, risk / entry, side * (exit_price / entry - 1), reason,
        sweep_atr, volume_ratio,
    )


def portfolio(rows: list[Trade], start_ms: int, end_ms: int, cost_bps: float = 16.0) -> dict:
    candidates_in_period = sorted(
        (value for value in rows if start_ms <= value.entry_ms < end_ms),
        key=lambda value: (value.entry_ms, -value.sweep_atr, value.symbol),
    )
    equity = 2_000.0
    peak = equity
    max_drawdown = 0.0
    active = []
    completed = []
    skipped = Counter()
    cost = cost_bps / 10_000

    def settle(until_ms: int) -> None:
        nonlocal equity, peak, max_drawdown, active
        closing = sorted((value for value in active if value["trade"].exit_ms <= until_ms), key=lambda value: value["trade"].exit_ms)
        for position in closing:
            trade = position["trade"]
            pnl = position["notional"] * (trade.gross_return - cost)
            equity += pnl
            completed.append({**asdict(trade), "notional_usd": position["notional"], "pnl_usd": pnl})
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / max(peak, 1e-12))
        active = [value for value in active if value["trade"].exit_ms > until_ms]

    for trade in candidates_in_period:
        settle(trade.entry_ms)
        if any(value["trade"].symbol == trade.symbol for value in active):
            skipped["symbol_overlap"] += 1
            continue
        if len(active) >= 3:
            skipped["max_positions"] += 1
            continue
        risk_usd = equity * 0.005
        desired = risk_usd / trade.stop_fraction
        gross = sum(value["notional"] for value in active)
        notional = min(desired, equity * 1.5, max(0.0, equity * 4 - gross))
        if notional * trade.stop_fraction < risk_usd * 0.5:
            skipped["capacity"] += 1
            continue
        active.append({"trade": trade, "notional": notional})
    settle(10**30)
    gains = sum(max(0.0, value["pnl_usd"]) for value in completed)
    losses = -sum(min(0.0, value["pnl_usd"]) for value in completed)
    days = max(1.0, (end_ms - start_ms) / DAY_MS)
    by_side = {}
    for side, label in ((1, "buy"), (-1, "sell")):
        selected = [value for value in completed if value["side"] == side]
        gp = sum(max(0.0, value["pnl_usd"]) for value in selected)
        gl = -sum(min(0.0, value["pnl_usd"]) for value in selected)
        by_side[label] = {
            "trades": len(selected), "pnl_usd": sum(value["pnl_usd"] for value in selected),
            "profit_factor": gp / gl if gl else None,
        }
    return {
        "start_ms": start_ms, "end_ms": end_ms, "start_equity_usd": 2_000.0,
        "end_equity_usd": equity, "pnl_usd": equity - 2_000.0,
        "return_pct": equity / 2_000.0 - 1, "max_drawdown_pct": max_drawdown,
        "trades": len(completed), "trades_per_day": len(completed) / days,
        "win_rate": sum(value["pnl_usd"] > 0 for value in completed) / len(completed) if completed else None,
        "profit_factor": gains / losses if losses else None,
        "average_pnl_usd": statistics.mean(value["pnl_usd"] for value in completed) if completed else None,
        "round_trip_cost_bps": cost_bps, "by_side": by_side, "skipped": dict(skipped),
    }


def main() -> None:
    root = Path("data/alpha-backtest/cache")
    output = Path("data/alpha-backtest/sfp-liquidity-sweep-walkforward.json")
    symbols = sorted(path.name for path in (root / "klines").iterdir() if any(path.glob("*.zip")))
    market = {}
    for symbol in symbols:
        minutes = load_minutes(root, symbol)
        if minutes:
            market[symbol] = (
                minutes,
                {bar_minutes: aggregate(minutes, bar_minutes) for bar_minutes in (15, 60)},
            )

    train = (iso_ms("2026-06-01"), iso_ms("2026-07-24"))
    validation = (iso_ms("2026-07-24"), iso_ms("2026-08-15"))
    locked = (iso_ms("2026-08-15"), iso_ms("2026-08-24"))
    configs = [
        Config(bar_minutes, lookback, minimum, maximum, volume, target, hold)
        for bar_minutes in (15, 60)
        for lookback in (48, 96, 288)
        for minimum in (0.05, 0.10)
        for maximum in (0.75, 1.25)
        for volume in (1.0, 1.5)
        for target in (1.25, 1.5, 2.0)
        for hold in (180, 360)
    ]
    signal_cache = {}
    for cfg in configs:
        signal_key = (
            cfg.bar_minutes, cfg.lookback_bars, cfg.min_sweep_atr, cfg.max_sweep_atr,
            cfg.min_volume_ratio,
        )
        if signal_key in signal_cache:
            continue
        by_symbol = {}
        for symbol, (minutes, bars_by_interval) in market.items():
            by_symbol[symbol] = candidates(
                symbol, minutes, bars_by_interval[cfg.bar_minutes], cfg
            )
        signal_cache[signal_key] = by_symbol

    evaluated = []
    for cfg in configs:
        signal_key = (
            cfg.bar_minutes, cfg.lookback_bars, cfg.min_sweep_atr, cfg.max_sweep_atr,
            cfg.min_volume_ratio,
        )
        rows = []
        for symbol, signals in signal_cache[signal_key].items():
            minutes = market[symbol][0]
            rows.extend(replay(symbol, minutes, signal, cfg) for signal in signals)
        for universe_mode in ("all", "core_10"):
            universe_rows = rows if universe_mode == "all" else [
                value for value in rows if value.symbol in CORE_SYMBOLS
            ]
            for side_mode in ("both", "buy", "sell"):
                selected_rows = universe_rows if side_mode == "both" else [
                    value for value in universe_rows
                    if value.side == (1 if side_mode == "buy" else -1)
                ]
                train_result = portfolio(selected_rows, *train)
                validation_result = portfolio(selected_rows, *validation)
                eligible = (
                    train_result["trades"] >= 12 and validation_result["trades"] >= 5
                    and train_result["return_pct"] > 0 and validation_result["return_pct"] > 0
                    and (train_result["profit_factor"] or 0) > 1.05
                    and (validation_result["profit_factor"] or 0) > 1.05
                )
                score = min(
                    train_result["profit_factor"] or 0,
                    validation_result["profit_factor"] or 0,
                )
                evaluated.append((
                    eligible, score, validation_result["return_pct"], cfg,
                    universe_mode, side_mode, selected_rows, train_result,
                    validation_result,
                ))
    eligible_rows = [value for value in evaluated if value[0]]
    pool = eligible_rows or evaluated
    pool.sort(key=lambda value: (value[0], value[1], value[2]), reverse=True)
    selected = pool[0]
    (
        _, _, _, cfg, universe_mode, side_mode, rows, train_result,
        validation_result,
    ) = selected
    locked_result = portfolio(rows, *locked)
    stress_result = portfolio(rows, *locked, cost_bps=30.0)
    leaderboard = sorted(
        evaluated,
        key=lambda value: (value[0], value[1], value[2]),
        reverse=True,
    )[:10]
    report = {
        "strategy": "sfp_liquidity_sweep_v1",
        "status": "provisional_demo" if eligible_rows else "development_failed",
        "selection": "train and validation only; locked test untouched",
        "universe_symbols": len(market), "grid": len(evaluated), "eligible": len(eligible_rows),
        "selected": {**asdict(cfg), "universe_mode": universe_mode, "side_mode": side_mode},
        "train": train_result, "validation": validation_result,
        "locked_test": locked_result, "locked_test_30bps": stress_result,
        "leaderboard": [
            {
                "eligible": value[0],
                "score": value[1],
                "config": {
                    **asdict(value[3]),
                    "universe_mode": value[4],
                    "side_mode": value[5],
                },
                "train": {
                    key: value[7][key]
                    for key in (
                        "trades", "trades_per_day", "pnl_usd",
                        "profit_factor", "max_drawdown_pct",
                    )
                },
                "validation": {
                    key: value[8][key]
                    for key in (
                        "trades", "trades_per_day", "pnl_usd",
                        "profit_factor", "max_drawdown_pct",
                    )
                },
            }
            for value in leaderboard
        ],
        "assumptions": {
            "signal": "completed 15m/1h sweep of a mature rolling high/low and directional close back inside",
            "entry": "break of the rejection bar within three bars, replayed on 1m data",
            "intrabar_priority": "stop before target",
            "portfolio": "0.5% risk, 1.5x single, 4x gross, max 3 positions",
            "cost": "16 bps round trip; 30 bps stress",
        },
    }
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
