#!/usr/bin/env python3
"""Walk-forward bracket/exit search for the production altcoin impulse signal.

Entry signals are unchanged and causal (closed 15 minute bar, next bar open).
The experiment isolates whether the production 5% stop and delayed trailing
exit are the source of instability.  Candidate parameters are selected using
Jan-Apr training and May-Jun validation only; July and August are reported
after selection.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from exp_altcoin_entry_exit_v2 import (
    enrich_point_in_time_spot,
    load_symbol_bars,
    spot_paths,
    timestamp,
)
from exp_altcoin_five_optimizations import make_breakout_signals
from exp_altcoin_oi_launch import BAR_MS, DAY_MS, MAJORS, Bar, Signal, load_bars


@dataclass(frozen=True)
class Exit:
    name: str
    stop: float
    target: float | None
    activation: float | None
    trail: float | None
    hold_bars: int
    # (favourable excursion, locked return from entry).  Locks are evaluated
    # after the bar closes and only affect later bars, matching the conservative
    # unknown-intrabar ordering used by the rest of this replay.
    locks: tuple[tuple[float, float], ...] = ()
    partial_target: float | None = None
    partial_fraction: float = 0.0
    remainder_lock: float = 0.0
    timed_lock_after_bars: int | None = None
    timed_lock_activation: float = 0.0
    timed_lock_return: float = 0.0
    recovery_adverse: float | None = None
    recovery_activation: float = 0.0
    recovery_lock_return: float = 0.0
    invalidation_buffer: float | None = None
    invalidation_long_only: bool = False
    unproven_stop: float | None = None
    proof_activation: float = 0.0
    initial_fraction: float = 1.0
    add_trigger: float | None = None
    post_add_lock: float = 0.0
    staged_long_only: bool = False


@dataclass
class Position:
    signal: Signal
    qty: float
    original_qty: float
    entry: float
    first_entry: float
    entry_fee: float
    index: int
    entry_ts: int
    stop: float
    target: float | None
    extreme: float
    adverse_extreme: float
    mark: float
    partial_taken: bool = False
    realized_gross: float = 0.0
    realized_fees: float = 0.0
    planned_notional: float = 0.0
    added: bool = False


def simulate(
    bars_by_symbol: dict[str, list[Bar]],
    signals: list[Signal],
    start_ts: int,
    end_ts: int,
    profile: Exit,
    *,
    risk: float = 0.03,
    slippage_bps: float = 5.0,
    high_water_limit: float | None = 0.08,
    max_daily_entries: int = 6,
    position_scale: Callable[[Signal], float] | None = None,
    cooldown_hours: int = 8,
) -> dict[str, object]:
    signal_map: dict[int, list[Signal]] = defaultdict(list)
    for signal in signals:
        if start_ts <= signal.execute_ts < end_ts:
            signal_map[signal.execute_ts].append(signal)
    for values in signal_map.values():
        values.sort(key=lambda item: item.score, reverse=True)

    cash = 1_000.0
    fee = 0.0005
    slip = slippage_bps / 10_000.0
    positions: dict[str, Position] = {}
    last_exit: dict[str, int] = {}
    trades: list[dict[str, object]] = []
    curve: list[float] = []
    day_id: int | None = None
    day_start = cash
    day_peak = cash
    daily_entries = 0
    latched = False

    def equity() -> float:
        return cash + sum(
            pos.signal.side * pos.qty * (pos.mark - pos.entry)
            for pos in positions.values()
        )

    def close(symbol: str, ts: int, raw_price: float, reason: str) -> None:
        nonlocal cash
        pos = positions.pop(symbol)
        price = raw_price * (1.0 - pos.signal.side * slip)
        gross = pos.signal.side * pos.qty * (price - pos.entry)
        exit_fee = pos.qty * price * fee
        cash += gross - exit_fee
        pnl = pos.realized_gross + gross - pos.entry_fee - pos.realized_fees - exit_fee
        trades.append(
            {
                "symbol": symbol,
                "side": "long" if pos.signal.side > 0 else "short",
                "entry_ts": pos.entry_ts,
                "exit_ts": ts,
                "pnl": pnl,
                "reason": reason,
            }
        )
        last_exit[symbol] = ts

    def take_partial(pos: Position, raw_price: float) -> None:
        nonlocal cash
        if pos.partial_taken or profile.partial_target is None:
            return
        partial_qty = min(pos.qty, pos.original_qty * profile.partial_fraction)
        if partial_qty <= 0.0:
            return
        price = raw_price * (1.0 - pos.signal.side * slip)
        gross = pos.signal.side * partial_qty * (price - pos.entry)
        exit_fee = partial_qty * price * fee
        cash += gross - exit_fee
        pos.realized_gross += gross
        pos.realized_fees += exit_fee
        pos.qty -= partial_qty
        pos.partial_taken = True
        locked = pos.entry * (1.0 + pos.signal.side * profile.remainder_lock)
        pos.stop = max(pos.stop, locked) if pos.signal.side > 0 else min(pos.stop, locked)

    for ts in range((start_ts // BAR_MS) * BAR_MS, end_ts, BAR_MS):
        for symbol in list(positions):
            pos = positions[symbol]
            index = pos.index + (ts - pos.entry_ts) // BAR_MS
            bars = bars_by_symbol[symbol]
            if index < 0 or index >= len(bars) or bars[index].ts != ts:
                continue
            bar = bars[index]
            pos.mark = bar.open
            proved = pos.signal.side * (pos.extreme / pos.entry - 1.0) >= profile.proof_activation
            effective_stop = pos.stop
            if profile.unproven_stop is not None and not proved:
                early = pos.entry * (1.0 - pos.signal.side * profile.unproven_stop)
                effective_stop = max(effective_stop, early) if pos.signal.side > 0 else min(effective_stop, early)
            stop_gap = bar.open <= effective_stop if pos.signal.side > 0 else bar.open >= effective_stop
            stop_hit = bar.low <= effective_stop if pos.signal.side > 0 else bar.high >= effective_stop
            target_gap = (
                pos.target is not None
                and (bar.open >= pos.target if pos.signal.side > 0 else bar.open <= pos.target)
            )
            target_hit = (
                pos.target is not None
                and (bar.high >= pos.target if pos.signal.side > 0 else bar.low <= pos.target)
            )
            timed = index - pos.index >= profile.hold_bars
            partial_price = (
                pos.entry * (1.0 + pos.signal.side * profile.partial_target)
                if profile.partial_target is not None
                else None
            )
            partial_gap = (
                not pos.partial_taken
                and partial_price is not None
                and (bar.open >= partial_price if pos.signal.side > 0 else bar.open <= partial_price)
            )
            partial_hit = (
                not pos.partial_taken
                and partial_price is not None
                and (bar.high >= partial_price if pos.signal.side > 0 else bar.low <= partial_price)
            )
            # If both boundaries occur in one 15m bar, choose the stop.  This is
            # deliberately conservative because the intrabar path is unknown.
            if stop_gap or stop_hit:
                close(symbol, ts, bar.open if stop_gap else effective_stop, "unproven_stop" if effective_stop != pos.stop else "stop")
            elif target_gap or target_hit:
                close(symbol, ts, bar.open if target_gap else float(pos.target), "target")
            elif timed:
                close(symbol, ts, bar.open, "time")
            elif partial_gap or partial_hit:
                take_partial(pos, bar.open if partial_gap else float(partial_price))

            if symbol not in positions:
                continue
            pos = positions[symbol]
            if (
                not pos.added
                and profile.add_trigger is not None
                and (not profile.staged_long_only or pos.signal.side > 0)
            ):
                add_price = pos.first_entry * (
                    1.0 + pos.signal.side * profile.add_trigger
                )
                add_hit = (
                    bar.high >= add_price
                    if pos.signal.side > 0
                    else bar.low <= add_price
                )
                if add_hit:
                    remaining = max(0.0, pos.planned_notional - pos.original_qty * pos.first_entry)
                    if remaining >= 50.0:
                        fill = add_price * (1.0 + pos.signal.side * slip)
                        add_qty = remaining / fill
                        add_fee = remaining * fee
                        cash -= add_fee
                        total_qty = pos.qty + add_qty
                        pos.entry = (pos.qty * pos.entry + add_qty * fill) / total_qty
                        pos.qty = total_qty
                        pos.original_qty = total_qty
                        pos.entry_fee += add_fee
                        pos.added = True
                        locked = pos.first_entry * (
                            1.0 + pos.signal.side * profile.post_add_lock
                        )
                        pos.stop = max(pos.stop, locked) if pos.signal.side > 0 else min(pos.stop, locked)

        current = equity()
        current_day = ts // DAY_MS
        if current_day != day_id:
            day_id = current_day
            day_start = current
            day_peak = current
            daily_entries = 0
            latched = False
        day_peak = max(day_peak, current)
        if high_water_limit is not None and current < day_peak * (1.0 - high_water_limit):
            latched = True
        gross = sum(pos.qty * pos.mark for pos in positions.values())
        capacity = max(0.0, 4.0 * current - gross)
        for signal in signal_map.get(ts, []):
            if (
                daily_entries >= max_daily_entries
                or len(positions) >= 2
                or current < day_start * 0.96
                or latched
                or signal.symbol in positions
                or ts - last_exit.get(signal.symbol, -10**18)
                < cooldown_hours * 60 * 60 * 1_000
                or capacity < 50.0
            ):
                continue
            index = int(signal.features["signal_index"]) + 1
            bars = bars_by_symbol[signal.symbol]
            if index >= len(bars) or bars[index].ts != ts:
                continue
            bar = bars[index]
            scale = max(0.0, min(1.0, position_scale(signal))) if position_scale else 1.0
            planned_notional = min(current * risk / profile.stop, 2.0 * current, capacity) * scale
            staged = profile.add_trigger is not None and (
                not profile.staged_long_only or signal.side > 0
            )
            notional = planned_notional * profile.initial_fraction if staged else planned_notional
            if notional < 50.0:
                continue
            entry = bar.open * (1.0 + signal.side * slip)
            qty = notional / entry
            entry_fee = notional * fee
            cash -= entry_fee
            positions[signal.symbol] = Position(
                signal=signal,
                qty=qty,
                original_qty=qty,
                entry=entry,
                first_entry=entry,
                entry_fee=entry_fee,
                index=index,
                entry_ts=ts,
                stop=entry * (1.0 - signal.side * profile.stop),
                target=(entry * (1.0 + signal.side * profile.target) if profile.target else None),
                extreme=entry,
                adverse_extreme=entry,
                mark=bar.open,
                planned_notional=planned_notional,
                added=not staged,
            )
            daily_entries += 1
            capacity -= notional

        for symbol in list(positions):
            pos = positions[symbol]
            index = pos.index + (ts - pos.entry_ts) // BAR_MS
            bars = bars_by_symbol[symbol]
            if index < 0 or index >= len(bars) or bars[index].ts != ts:
                continue
            bar = bars[index]
            if pos.entry_ts == ts:
                initial_hit = bar.low <= pos.stop if pos.signal.side > 0 else bar.high >= pos.stop
                initial_effective_stop = pos.stop
                if profile.unproven_stop is not None:
                    early = pos.entry * (1.0 - pos.signal.side * profile.unproven_stop)
                    initial_effective_stop = max(pos.stop, early) if pos.signal.side > 0 else min(pos.stop, early)
                    initial_hit = bar.low <= initial_effective_stop if pos.signal.side > 0 else bar.high >= initial_effective_stop
                target_hit = (
                    pos.target is not None
                    and (bar.high >= pos.target if pos.signal.side > 0 else bar.low <= pos.target)
                )
                if initial_hit:
                    close(symbol, ts, initial_effective_stop, "unproven_stop" if initial_effective_stop != pos.stop else "stop")
                    continue
                if target_hit:
                    close(symbol, ts, float(pos.target), "target")
                    continue
                if profile.partial_target is not None:
                    partial_price = pos.entry * (
                        1.0 + pos.signal.side * profile.partial_target
                    )
                    partial_hit = (
                        bar.high >= partial_price
                        if pos.signal.side > 0
                        else bar.low <= partial_price
                    )
                    if partial_hit:
                        take_partial(pos, partial_price)
            pos.mark = bar.close
            if profile.invalidation_buffer is not None and (
                not profile.invalidation_long_only or pos.signal.side > 0
            ):
                breakout = float(pos.signal.features["breakout_level"])
                invalidated = (
                    bar.close < breakout * (1.0 - profile.invalidation_buffer)
                    if pos.signal.side > 0
                    else bar.close > breakout * (1.0 + profile.invalidation_buffer)
                )
                if invalidated:
                    close(symbol, ts, bar.close, "breakout_invalidated")
                    continue
            pos.adverse_extreme = (
                min(pos.adverse_extreme, bar.low)
                if pos.signal.side > 0
                else max(pos.adverse_extreme, bar.high)
            )
            adverse_excursion = -pos.signal.side * (
                pos.adverse_extreme / pos.entry - 1.0
            )
            current_return = pos.signal.side * (bar.close / pos.entry - 1.0)
            if (
                profile.recovery_adverse is not None
                and adverse_excursion >= profile.recovery_adverse
                and current_return >= profile.recovery_activation
            ):
                locked = pos.entry * (
                    1.0 + pos.signal.side * profile.recovery_lock_return
                )
                pos.stop = (
                    max(pos.stop, locked)
                    if pos.signal.side > 0
                    else min(pos.stop, locked)
                )
            if profile.timed_lock_after_bars is not None:
                age_bars = index - pos.index
                current_return = pos.signal.side * (bar.close / pos.entry - 1.0)
                if (
                    age_bars >= profile.timed_lock_after_bars
                    and current_return >= profile.timed_lock_activation
                ):
                    locked = pos.entry * (
                        1.0 + pos.signal.side * profile.timed_lock_return
                    )
                    pos.stop = (
                        max(pos.stop, locked)
                        if pos.signal.side > 0
                        else min(pos.stop, locked)
                    )
            if pos.signal.side > 0:
                pos.extreme = max(pos.extreme, bar.high)
                excursion = pos.extreme / pos.entry - 1.0
                for activation, locked_return in profile.locks:
                    if excursion >= activation:
                        pos.stop = max(pos.stop, pos.entry * (1.0 + locked_return))
                if (
                    profile.activation is not None
                    and profile.trail is not None
                    and excursion >= profile.activation
                ):
                    pos.stop = max(pos.stop, pos.extreme * (1.0 - profile.trail))
            else:
                pos.extreme = min(pos.extreme, bar.low)
                excursion = 1.0 - pos.extreme / pos.entry
                for activation, locked_return in profile.locks:
                    if excursion >= activation:
                        pos.stop = min(pos.stop, pos.entry * (1.0 - locked_return))
                if (
                    profile.activation is not None
                    and profile.trail is not None
                    and excursion >= profile.activation
                ):
                    pos.stop = min(pos.stop, pos.extreme * (1.0 + profile.trail))
        end_equity = equity()
        day_peak = max(day_peak, end_equity)
        curve.append(end_equity)

    for symbol in list(positions):
        close(symbol, end_ts, positions[symbol].mark, "end")
    curve.append(cash)
    peak = 1_000.0
    drawdown = 0.0
    for value in curve:
        peak = max(peak, value)
        drawdown = max(drawdown, 1.0 - value / peak)
    wins = [trade for trade in trades if float(trade["pnl"]) > 0.0]
    losses = [trade for trade in trades if float(trade["pnl"]) < 0.0]
    gp = sum(float(trade["pnl"]) for trade in wins)
    gl = -sum(float(trade["pnl"]) for trade in losses)
    return {
        "return_pct": (cash / 1_000.0 - 1.0) * 100.0,
        "max_drawdown_pct": drawdown * 100.0,
        "trades": len(trades),
        "active_days": len({int(trade["entry_ts"]) // DAY_MS for trade in trades}),
        "win_rate_pct": 100.0 * len(wins) / len(trades) if trades else 0.0,
        "profit_factor": gp / gl if gl else None,
        "expectancy_usdt": sum(float(item["pnl"]) for item in trades) / len(trades) if trades else 0.0,
        "average_win_usdt": gp / len(wins) if wins else 0.0,
        "average_loss_usdt": -gl / len(losses) if losses else 0.0,
        "long_pnl": sum(float(item["pnl"]) for item in trades if item["side"] == "long"),
        "short_pnl": sum(float(item["pnl"]) for item in trades if item["side"] == "short"),
        "targets": sum(item["reason"] == "target" for item in trades),
        "stops": sum(item["reason"] == "stop" for item in trades),
        "time_exits": sum(item["reason"] == "time" for item in trades),
        "invalidation_exits": sum(item["reason"] == "breakout_invalidated" for item in trades),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-bracket-exit-v3.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted(
        {path.name for path in (cache / "spot-klines").iterdir() if path.is_dir()}
        & {path.name for path in (cache / "klines").iterdir() if path.is_dir()}
        - MAJORS
    )
    bars_by_symbol: dict[str, list[Bar]] = {}
    signals: list[Signal] = []
    for count, symbol in enumerate(symbols, 1):
        futures = load_symbol_bars(cache, symbol)
        spot = load_bars(spot_paths(cache, symbol))
        if len(futures) < 15 * 96 or not spot:
            continue
        enriched = enrich_point_in_time_spot(make_breakout_signals(symbol, futures), spot)
        if enriched:
            bars_by_symbol[symbol] = futures
            signals.extend(enriched)
        if count % 100 == 0:
            print(f"features {count}/{len(symbols)} signals={len(signals)}", flush=True)

    profiles: list[Exit] = []
    for stop in (0.015, 0.02, 0.03, 0.04, 0.05):
        for target in (0.01, 0.015, 0.02, 0.03, 0.04, 0.06):
            for hold in (4, 8, 12, 24):
                profiles.append(
                    Exit(
                        f"bracket_S{stop*100:g}_TP{target*100:g}_H{hold/4:g}",
                        stop,
                        target,
                        None,
                        None,
                        hold,
                    )
                )
    for stop in (0.02, 0.03, 0.04, 0.05):
        for activation, trail in ((0.01, 0.005), (0.015, 0.0075), (0.02, 0.01)):
            for hold in (4, 8, 12, 24):
                profiles.append(
                    Exit(
                        f"trail_S{stop*100:g}_A{activation*100:g}_T{trail*100:g}_H{hold/4:g}",
                        stop,
                        None,
                        activation,
                        trail,
                        hold,
                    )
                )

    periods = {
        "train": (timestamp("2026-01-15"), timestamp("2026-05-01")),
        "validation": (timestamp("2026-05-01"), timestamp("2026-07-01")),
        "july_test": (timestamp("2026-07-01"), timestamp("2026-08-01")),
        "aug_holdout": (timestamp("2026-08-01"), timestamp("2026-08-11")),
        "full": (timestamp("2026-01-15"), timestamp("2026-08-11")),
    }
    development: list[dict[str, object]] = []
    for index, profile in enumerate(profiles, 1):
        train = simulate(bars_by_symbol, signals, *periods["train"], profile)
        validation = simulate(bars_by_symbol, signals, *periods["validation"], profile)
        development.append({"profile": profile.name, "train": train, "validation": validation})
        if index % 40 == 0:
            print(f"grid {index}/{len(profiles)}", flush=True)
    eligible = [
        row for row in development
        if float(row["train"]["return_pct"]) > 0.0
        and float(row["validation"]["return_pct"]) > 0.0
        and int(row["train"]["trades"]) >= 30
        and int(row["validation"]["trades"]) >= 20
    ]
    eligible.sort(
        key=lambda row: (
            float(row["validation"]["profit_factor"] or 0.0),
            -float(row["validation"]["max_drawdown_pct"]),
        ),
        reverse=True,
    )
    if not eligible:
        development.sort(
            key=lambda row: min(
                float(row["train"]["return_pct"]),
                float(row["validation"]["return_pct"]),
            ),
            reverse=True,
        )
    selected = (eligible or development)[:20]
    by_name = {profile.name: profile for profile in profiles}
    finalists: list[dict[str, object]] = []
    for row in selected:
        profile = by_name[str(row["profile"])]
        final = dict(row)
        for period in ("july_test", "aug_holdout", "full"):
            final[period] = simulate(bars_by_symbol, signals, *periods[period], profile)
        final["full_slippage_15bps"] = simulate(
            bars_by_symbol, signals, *periods["full"], profile, slippage_bps=15.0
        )
        final["full_risk_10pct"] = simulate(
            bars_by_symbol, signals, *periods["full"], profile, risk=0.10
        )
        finalists.append(final)
        print(
            profile.name,
            f"train={row['train']['return_pct']:.2f}",
            f"validation={row['validation']['return_pct']:.2f}",
            f"july={final['july_test']['return_pct']:.2f}",
            f"aug={final['aug_holdout']['return_pct']:.2f}",
            flush=True,
        )
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "causal closed-bar signal, next-open fill, conservative stop-first intrabar ordering",
        "data": {"symbols": len(bars_by_symbol), "signals": len(signals)},
        "grid_size": len(profiles),
        "eligible_development_candidates": len(eligible),
        "selection": "train Jan-Apr + validation May-Jun; July/Aug untouched",
        "finalists": finalists,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
