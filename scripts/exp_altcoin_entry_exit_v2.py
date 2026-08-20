#!/usr/bin/env python3
"""Focused walk-forward experiment for the production altcoin impulse sleeve.

The script reuses the exact causal breakout feature builder from the earlier
ablation study, then tests side-specific entry caps, spot/perpetual agreement,
fixed stop/trailing exits and a latched intraday high-water gate.  Parameters
are ranked only on Jan-Apr train and May-Jun validation. July and August are
reported afterwards and are never used to rank candidates.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_five_optimizations import load_symbol_bars, make_breakout_signals
from exp_altcoin_oi_launch import BAR_MS, DAY_MS, MAJORS, Bar, Signal, load_bars


@dataclass(frozen=True)
class EntryProfile:
    name: str
    long_max_1h: float = 0.45
    max_spot_perp_gap: float | None = None
    long_max_volume_ratio: float | None = None


@dataclass(frozen=True)
class ExitProfile:
    name: str
    stop: float
    trail_activation: float
    trail_distance: float
    max_hold_bars: int


@dataclass
class Position:
    signal: Signal
    qty: float
    original_qty: float
    entry_price: float
    entry_fee: float
    entry_index: int
    entry_ts: int
    extreme: float
    stop: float
    mark: float


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def spot_paths(cache: Path, symbol: str) -> list[Path]:
    paths = sorted((cache / "spot-klines" / symbol).glob("*.zip"))
    paths += sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
    return paths


def enrich_point_in_time_spot(
    signals: list[Signal],
    spot_bars: list[Bar],
) -> list[Signal]:
    # Binance spot archives switched from millisecond to microsecond timestamps;
    # USD-M archives remain milliseconds. Normalize before point-in-time joins.
    closes = {
        (bar.ts // 1_000 if bar.ts > 10**15 else bar.ts): bar.close
        for bar in spot_bars
    }
    output: list[Signal] = []
    for signal in signals:
        formation_ts = signal.execute_ts - BAR_MS
        current = closes.get(formation_ts)
        previous = closes.get(formation_ts - 4 * BAR_MS)
        if current is None or previous is None or previous <= 0.0:
            continue
        features = dict(signal.features)
        features["spot_return_1h"] = current / previous - 1.0
        features["spot_perp_gap_1h"] = abs(
            float(features["return_1h"]) - float(features["spot_return_1h"])
        )
        output.append(
            Signal(
                signal.symbol,
                signal.execute_ts,
                signal.side,
                signal.score,
                signal.kind,
                features,
            )
        )
    return output


def filter_signals(signals: list[Signal], profile: EntryProfile) -> list[Signal]:
    output: list[Signal] = []
    for signal in signals:
        if signal.side > 0:
            if float(signal.features["return_1h"]) > profile.long_max_1h:
                continue
            if (
                profile.long_max_volume_ratio is not None
                and float(signal.features["volume_ratio"]) > profile.long_max_volume_ratio
            ):
                continue
        if (
            profile.max_spot_perp_gap is not None
            and float(signal.features["spot_perp_gap_1h"]) > profile.max_spot_perp_gap
        ):
            continue
        output.append(signal)
    return output


def simulate(
    bars_by_symbol: dict[str, list[Bar]],
    signals: list[Signal],
    start_ts: int,
    end_ts: int,
    exit_profile: ExitProfile,
    *,
    risk_per_trade: float = 0.03,
    high_water_limit: float | None = None,
    slippage_bps: float = 5.0,
    keep_trades: bool = False,
    max_daily_entries: int = 6,
    max_positions: int = 2,
    overextension_threshold: float | None = None,
    latch_overextension_after_loss: bool = False,
) -> dict[str, object]:
    signal_map: dict[int, list[Signal]] = defaultdict(list)
    for signal in signals:
        if start_ts <= signal.execute_ts < end_ts:
            signal_map[signal.execute_ts].append(signal)
    for values in signal_map.values():
        values.sort(key=lambda item: item.score, reverse=True)

    cash = 1_000.0
    fee_rate = 0.0005
    slippage = slippage_bps / 10_000.0
    positions: dict[str, Position] = {}
    last_exit: dict[str, int] = {}
    trades: list[dict[str, object]] = []
    curve: list[float] = []
    current_day: int | None = None
    day_start_equity = cash
    day_peak = cash
    high_water_blocked = False
    daily_entries = 0
    overextension_blocked = False

    def equity() -> float:
        return cash + sum(
            position.signal.side
            * position.qty
            * (position.mark - position.entry_price)
            for position in positions.values()
        )

    def close_position(symbol: str, ts: int, raw_price: float, reason: str) -> None:
        nonlocal cash, overextension_blocked
        position = positions.pop(symbol)
        exit_price = raw_price * (1.0 - position.signal.side * slippage)
        gross = position.signal.side * position.qty * (exit_price - position.entry_price)
        exit_fee = position.qty * exit_price * fee_rate
        cash += gross - exit_fee
        pnl = gross - position.entry_fee - exit_fee
        if (
            latch_overextension_after_loss
            and overextension_threshold is not None
            and position.signal.side > 0
            and float(position.signal.features["return_1h"]) >= overextension_threshold
            and pnl < 0.0
        ):
            overextension_blocked = True
        trades.append(
            {
                "symbol": symbol,
                "side": "long" if position.signal.side > 0 else "short",
                "entry_ts": position.entry_ts,
                "exit_ts": ts,
                "pnl": pnl,
                "return_on_notional": pnl / (position.original_qty * position.entry_price),
                "reason": reason,
                "return_1h": float(position.signal.features["return_1h"]),
                "spot_return_1h": float(position.signal.features["spot_return_1h"]),
                "volume_ratio": float(position.signal.features["volume_ratio"]),
                "features": dict(position.signal.features),
            }
        )
        last_exit[symbol] = ts

    for ts in range((start_ts // BAR_MS) * BAR_MS, end_ts, BAR_MS):
        # Existing positions are evaluated before new signals at this timestamp.
        for symbol in list(positions):
            position = positions[symbol]
            bars = bars_by_symbol[symbol]
            index = position.entry_index + (ts - position.entry_ts) // BAR_MS
            if index < 0 or index >= len(bars) or bars[index].ts != ts:
                continue
            bar = bars[index]
            position.mark = bar.open
            gap = bar.open <= position.stop if position.signal.side > 0 else bar.open >= position.stop
            stopped = bar.low <= position.stop if position.signal.side > 0 else bar.high >= position.stop
            timed = index - position.entry_index >= exit_profile.max_hold_bars
            if gap or stopped or timed:
                close_position(
                    symbol,
                    ts,
                    bar.open if gap or timed else position.stop,
                    "stop" if gap or stopped else "time",
                )

        current_equity = equity()
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start_equity = current_equity
            day_peak = current_equity
            high_water_blocked = False
            daily_entries = 0
            overextension_blocked = False
        day_peak = max(day_peak, current_equity)
        if (
            high_water_limit is not None
            and current_equity < day_peak * (1.0 - high_water_limit)
        ):
            high_water_blocked = True

        gross = sum(position.qty * position.mark for position in positions.values())
        capacity = max(0.0, 4.0 * current_equity - gross)
        entries_allowed = (
            daily_entries < max_daily_entries
            and current_equity >= day_start_equity * 0.96
            and not high_water_blocked
        )
        for signal in signal_map.get(ts, []):
            if (
                not entries_allowed
                or daily_entries >= max_daily_entries
                or len(positions) >= max_positions
                or capacity < 50.0
                or signal.symbol in positions
                or (
                    overextension_blocked
                    and overextension_threshold is not None
                    and signal.side > 0
                    and float(signal.features["return_1h"]) >= overextension_threshold
                )
                or ts - last_exit.get(signal.symbol, -10**18) < 8 * 60 * 60 * 1_000
            ):
                continue
            index = int(signal.features["signal_index"]) + 1
            bars = bars_by_symbol[signal.symbol]
            if index >= len(bars) or bars[index].ts != ts:
                continue
            bar = bars[index]
            notional = min(
                current_equity * risk_per_trade / exit_profile.stop,
                2.0 * current_equity,
                capacity,
            )
            if notional < 50.0:
                continue
            entry = bar.open * (1.0 + signal.side * slippage)
            quantity = notional / entry
            entry_fee = notional * fee_rate
            cash -= entry_fee
            positions[signal.symbol] = Position(
                signal,
                quantity,
                quantity,
                entry,
                entry_fee,
                index,
                ts,
                entry,
                entry * (1.0 - signal.side * exit_profile.stop),
                bar.open,
            )
            daily_entries += 1
            capacity -= notional

        # Only information inside the current bar is now applied to future stops.
        for symbol in list(positions):
            position = positions[symbol]
            bars = bars_by_symbol[symbol]
            index = position.entry_index + (ts - position.entry_ts) // BAR_MS
            if index < 0 or index >= len(bars) or bars[index].ts != ts:
                continue
            bar = bars[index]
            # An entry is filled at this bar's open. If both the initial stop
            # and a favorable excursion occur inside the bar, count the stop
            # first because the intrabar path is unknowable.
            if position.entry_ts == ts:
                entry_stop_hit = (
                    bar.low <= position.stop
                    if position.signal.side > 0
                    else bar.high >= position.stop
                )
                if entry_stop_hit:
                    close_position(symbol, ts, position.stop, "stop")
                    continue
            position.mark = bar.close
            if position.signal.side > 0:
                position.extreme = max(position.extreme, bar.high)
                excursion = position.extreme / position.entry_price - 1.0
                if excursion >= exit_profile.trail_activation:
                    position.stop = max(
                        position.stop,
                        position.extreme * (1.0 - exit_profile.trail_distance),
                    )
            else:
                position.extreme = min(position.extreme, bar.low)
                excursion = 1.0 - position.extreme / position.entry_price
                if excursion >= exit_profile.trail_activation:
                    position.stop = min(
                        position.stop,
                        position.extreme * (1.0 + exit_profile.trail_distance),
                    )
        end_equity = equity()
        day_peak = max(day_peak, end_equity)
        curve.append(end_equity)

    for symbol in list(positions):
        close_position(symbol, end_ts, positions[symbol].mark, "end")
    curve.append(cash)
    peak = 1_000.0
    max_drawdown = 0.0
    for value in curve:
        peak = max(peak, value)
        max_drawdown = max(max_drawdown, 1.0 - value / peak)
    wins = [trade for trade in trades if float(trade["pnl"]) > 0.0]
    losses = [trade for trade in trades if float(trade["pnl"]) < 0.0]
    gross_profit = sum(float(trade["pnl"]) for trade in wins)
    gross_loss = -sum(float(trade["pnl"]) for trade in losses)
    longs = [trade for trade in trades if trade["side"] == "long"]
    shorts = [trade for trade in trades if trade["side"] == "short"]
    active_days = len({int(trade["entry_ts"]) // DAY_MS for trade in trades})
    calendar_days = max(1, math.ceil((end_ts - start_ts) / DAY_MS))
    result: dict[str, object] = {
        "return_pct": (cash / 1_000.0 - 1.0) * 100.0,
        "final_equity": cash,
        "max_drawdown_pct": max_drawdown * 100.0,
        "trades": len(trades),
        "active_days": active_days,
        "trades_per_calendar_day": len(trades) / calendar_days,
        "win_rate_pct": len(wins) / len(trades) * 100.0 if trades else 0.0,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0.0 else None,
        "expectancy_usdt": sum(float(trade["pnl"]) for trade in trades) / len(trades)
        if trades
        else 0.0,
        "longs": len(longs),
        "long_pnl": sum(float(trade["pnl"]) for trade in longs),
        "shorts": len(shorts),
        "short_pnl": sum(float(trade["pnl"]) for trade in shorts),
        "stops": sum(trade["reason"] == "stop" for trade in trades),
        "time_exits": sum(trade["reason"] == "time" for trade in trades),
    }
    if keep_trades:
        result["trade_list"] = trades
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-entry-exit-v2.json")
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
        if not spot_bars:
            continue
        enriched = enrich_point_in_time_spot(
            make_breakout_signals(symbol, futures_bars),
            spot_bars,
        )
        if enriched:
            bars_by_symbol[symbol] = futures_bars
            base_signals.extend(enriched)
        if completed % 50 == 0:
            print(
                f"features {completed}/{len(symbols)} symbols={len(bars_by_symbol)} signals={len(base_signals)}",
                flush=True,
            )

    entries = [
        EntryProfile("baseline"),
        EntryProfile("long_cap_08", long_max_1h=0.08),
        EntryProfile("long_cap_10", long_max_1h=0.10),
        EntryProfile("long_cap_12", long_max_1h=0.12),
        EntryProfile("long_cap_15", long_max_1h=0.15),
        EntryProfile("spot_gap_03", max_spot_perp_gap=0.03),
        EntryProfile("spot_gap_05", max_spot_perp_gap=0.05),
        EntryProfile("long_cap_08_gap_05", long_max_1h=0.08, max_spot_perp_gap=0.05),
        EntryProfile("long_cap_10_gap_05", long_max_1h=0.10, max_spot_perp_gap=0.05),
        EntryProfile("long_volume_cap_50", long_max_volume_ratio=50.0),
        EntryProfile("long_cap_10_volume_50", long_max_1h=0.10, long_max_volume_ratio=50.0),
    ]
    exits = [
        ExitProfile("S5_A2_T1_H6", 0.05, 0.02, 0.01, 24),
        ExitProfile("S4_A2_T1_H6", 0.04, 0.02, 0.01, 24),
        ExitProfile("S4_A1.5_T0.75_H6", 0.04, 0.015, 0.0075, 24),
        ExitProfile("S5_A1.5_T0.75_H6", 0.05, 0.015, 0.0075, 24),
        ExitProfile("S5_A2_T0.75_H6", 0.05, 0.02, 0.0075, 24),
        ExitProfile("S5_A2.5_T1_H6", 0.05, 0.025, 0.01, 24),
        ExitProfile("S5_A2_T1_H3", 0.05, 0.02, 0.01, 12),
    ]
    gates: list[tuple[str, float | None]] = [
        ("no_hwm", None),
        ("hwm_06", 0.06),
        ("hwm_08", 0.08),
        ("hwm_10", 0.10),
    ]
    periods = {
        "train": (timestamp("2026-01-15"), timestamp("2026-05-01")),
        "validation": (timestamp("2026-05-01"), timestamp("2026-07-01")),
        "july_test": (timestamp("2026-07-01"), timestamp("2026-08-01")),
        "aug_holdout": (timestamp("2026-08-01"), timestamp("2026-08-11")),
        "full": (timestamp("2026-01-15"), timestamp("2026-08-11")),
    }
    filtered = {entry.name: filter_signals(base_signals, entry) for entry in entries}
    rows: list[dict[str, object]] = []
    for entry in entries:
        for exit_profile in exits:
            for gate_name, gate_value in gates:
                key = f"{entry.name}__{exit_profile.name}__{gate_name}"
                train = simulate(
                    bars_by_symbol,
                    filtered[entry.name],
                    *periods["train"],
                    exit_profile,
                    high_water_limit=gate_value,
                )
                validation = simulate(
                    bars_by_symbol,
                    filtered[entry.name],
                    *periods["validation"],
                    exit_profile,
                    high_water_limit=gate_value,
                )
                rows.append(
                    {
                        "key": key,
                        "entry": entry.name,
                        "exit": exit_profile.name,
                        "gate": gate_name,
                        "signal_count": len(filtered[entry.name]),
                        "train": train,
                        "validation": validation,
                    }
                )
        print(f"grid entry={entry.name} complete", flush=True)

    # Ranking has no access to July/August. Prefer candidates positive in both
    # development segments, then validation PF, drawdown and sample size.
    eligible = [
        row
        for row in rows
        if float(row["train"]["return_pct"]) > 0.0
        and float(row["validation"]["return_pct"]) > 0.0
        and int(row["train"]["trades"]) >= 30
        and int(row["validation"]["trades"]) >= 20
    ]
    eligible.sort(
        key=lambda row: (
            float(row["validation"]["profit_factor"] or 0.0),
            -float(row["validation"]["max_drawdown_pct"]),
            int(row["validation"]["trades"]),
        ),
        reverse=True,
    )
    selected = eligible[:20]
    if not selected:
        rows.sort(
            key=lambda row: (
                min(float(row["train"]["return_pct"]), float(row["validation"]["return_pct"])),
                float(row["validation"]["profit_factor"] or 0.0),
            ),
            reverse=True,
        )
        selected = rows[:20]

    by_entry = {item.name: item for item in entries}
    by_exit = {item.name: item for item in exits}
    by_gate = dict(gates)
    finalists: list[dict[str, object]] = []
    for row in selected:
        signals = filtered[str(row["entry"])]
        exit_profile = by_exit[str(row["exit"])]
        gate_value = by_gate[str(row["gate"])]
        enriched = dict(row)
        for period in ("july_test", "aug_holdout", "full"):
            enriched[period] = simulate(
                bars_by_symbol,
                signals,
                *periods[period],
                exit_profile,
                high_water_limit=gate_value,
            )
        enriched["full_slippage_15bps"] = simulate(
            bars_by_symbol,
            signals,
            *periods["full"],
            exit_profile,
            high_water_limit=gate_value,
            slippage_bps=15.0,
        )
        enriched["full_risk_10pct"] = simulate(
            bars_by_symbol,
            signals,
            *periods["full"],
            exit_profile,
            high_water_limit=gate_value,
            risk_per_trade=0.10,
        )
        finalists.append(enriched)
        print(
            f"final {row['key']} train={row['train']['return_pct']:.2f} "
            f"val={row['validation']['return_pct']:.2f} "
            f"july={enriched['july_test']['return_pct']:.2f} "
            f"aug={enriched['aug_holdout']['return_pct']:.2f}",
            flush=True,
        )

    baseline_entry = by_entry["baseline"]
    baseline_exit = by_exit["S5_A2_T1_H6"]
    baselines: dict[str, object] = {}
    for risk in (0.03, 0.10):
        baselines[f"risk_{int(risk*100)}pct"] = {
            period: simulate(
                bars_by_symbol,
                filtered[baseline_entry.name],
                *bounds,
                baseline_exit,
                risk_per_trade=risk,
            )
            for period, bounds in periods.items()
        }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "range": ["2026-01-15", "2026-08-11"],
            "symbols": len(bars_by_symbol),
            "point_in_time_spot": True,
            "signals": len(base_signals),
            "selection": "rank train Jan-Apr + validation May-Jun; July/Aug untouched",
        },
        "costs": {"fee_bps_each_side": 5, "slippage_bps_each_side": 5},
        "baselines": baselines,
        "grid_size": len(rows),
        "eligible_development_candidates": len(eligible),
        "finalists": finalists,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
