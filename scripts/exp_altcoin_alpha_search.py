#!/usr/bin/env python3
"""Causal search for altcoin entry alpha on Binance historical data.

This script intentionally starts from independent entry families rather than
adding filters to the old 24-hour breakout. Signals are formed on a closed
15-minute candle and executed at the next candle open. The portfolio compounds
from $1,000 and includes fees, slippage, position limits and a daily loss gate.

The first stage is price/volume only. It is designed to identify entry families
that survive validation before spending OI data and complexity on them.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_five_optimizations import load_symbol_bars, spot_history_symbols
from exp_altcoin_oi_launch import BAR_MS, DAY_MS, MAJORS, Bar, Signal, rolling_prior_extreme


@dataclass(frozen=True)
class EntryProfile:
    name: str
    family: str
    level: int


@dataclass(frozen=True)
class ExitProfile:
    name: str
    stop: float
    target: float
    max_bars: int
    trail_activation: float = math.inf
    trail_distance: float = math.inf


@dataclass
class Position:
    symbol: str
    side: int
    qty: float
    entry_ts: int
    entry_index: int
    entry_price: float
    entry_fee: float
    stop: float
    target: float
    extreme: float
    kind: str


ENTRY_PROFILES = [
    EntryProfile(f"{family}_{level}", family, level)
    for family in (
        "acceleration",
        "first_expansion",
        "pullback_resume",
        "shock_reversal",
        "pump_continuation",
        "blowoff_fade",
        "dump_rebound",
    )
    for level in (1, 2, 3)
]

EXIT_PROFILES = [
    ExitProfile("scalp_2_3_1h", 0.02, 0.03, 4),
    ExitProfile("fast_3_5_2h", 0.03, 0.05, 8),
    ExitProfile("wide_4_8_4h", 0.04, 0.08, 16),
    ExitProfile("trail_3_3_6h", 0.03, math.inf, 24, 0.03, 0.015),
    ExitProfile("time_5_2h", 0.05, math.inf, 8),
    ExitProfile("time_8_2h", 0.08, math.inf, 8),
    ExitProfile("time_5_4h", 0.05, math.inf, 16),
    ExitProfile("time_8_4h", 0.08, math.inf, 16),
]


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july_test": ("2026-07-01", "2026-08-01"),
    "aug_holdout": ("2026-08-01", "2026-08-11"),
    "full": ("2026-01-15", "2026-08-11"),
}


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def prefix(values: list[float]) -> list[float]:
    output = [0.0]
    for value in values:
        output.append(output[-1] + value)
    return output


def total(values: list[float], start: int, end: int) -> float:
    return values[end] - values[max(start, 0)]


def directional_close_location(bar: Bar, side: int) -> float:
    span = bar.high - bar.low
    location = (bar.close - bar.low) / span if span > 0 else 0.5
    return location if side > 0 else 1.0 - location


def efficiency(bars: list[Bar], start: int, end: int) -> float:
    path = [bar.close for bar in bars[start : end + 1]]
    travelled = sum(abs(right - left) for left, right in zip(path, path[1:]))
    return abs(path[-1] - path[0]) / max(travelled, 1e-12)


def accepts(profile: EntryProfile, values: dict[str, float | int]) -> tuple[bool, int, float]:
    level = profile.level
    r15 = float(values["r15"])
    r1 = float(values["r1"])
    r4 = float(values["r4"])
    r24 = float(values["r24"])
    vol_bar = float(values["vol_bar"])
    vol_hour = float(values["vol_hour"])
    eff = float(values["efficiency"])
    close_loc_long = float(values["close_loc_long"])
    candle_range = float(values["candle_range"])
    atr_compression = float(values["atr_compression"])
    side = 1 if r1 >= 0 else -1

    if profile.family == "acceleration":
        thresholds = {
            1: (0.012, 0.070, 0.018, 0.18, 0.002, 1.7, 0.45, 0.58),
            2: (0.018, 0.075, 0.028, 0.20, 0.004, 2.3, 0.55, 0.64),
            3: (0.028, 0.085, 0.045, 0.22, 0.006, 3.2, 0.65, 0.70),
        }
        min_r1, max_r1, min_r4, max_r4, min_r15, min_vol, min_eff, min_loc = thresholds[level]
        ok = (
            min_r1 <= side * r1 <= max_r1
            and min_r4 <= side * r4 <= max_r4
            and min_r15 <= side * r15 <= 0.04
            and side * r24 <= 0.35
            and vol_hour >= min_vol
            and eff >= min_eff
            and (close_loc_long if side > 0 else 1.0 - close_loc_long) >= min_loc
            and candle_range <= 0.10
        )
        score = side * r1 * math.log1p(vol_hour) * (0.5 + eff)
        return ok, side, score

    if profile.family == "first_expansion":
        thresholds = {
            1: (0.85, 1.8, 0.003, 0.55),
            2: (0.68, 2.5, 0.005, 0.63),
            3: (0.52, 3.5, 0.007, 0.70),
        }
        max_compression, min_vol, min_r15, min_loc = thresholds[level]
        side = int(values["breakout6_side"])
        ok = (
            side != 0
            and min_r15 <= side * r15 <= 0.04
            and -0.03 <= side * r4 <= 0.12
            and atr_compression <= max_compression
            and vol_bar >= min_vol
            and directional_close_location_proxy(close_loc_long, side) >= min_loc
            and candle_range <= 0.08
        )
        score = side * r15 * math.log1p(vol_bar) / max(atr_compression, 0.15)
        return ok, side, score

    if profile.family == "pullback_resume":
        thresholds = {
            1: (0.030, 0.008, 0.055, 1.15, 0.56),
            2: (0.045, 0.012, 0.050, 1.45, 0.63),
            3: (0.065, 0.016, 0.045, 1.90, 0.70),
        }
        min_trend, min_pullback, max_pullback, min_vol, min_loc = thresholds[level]
        side = 1 if r4 >= 0 else -1
        pullback = float(values["pullback_depth_long"] if side > 0 else values["pullback_depth_short"])
        ok = (
            min_trend <= side * r4 <= 0.25
            and 0.04 <= side * r24 <= 0.45
            and min_pullback <= pullback <= max_pullback
            and side * r15 >= 0.003
            and vol_bar >= min_vol
            and directional_close_location_proxy(close_loc_long, side) >= min_loc
            and candle_range <= 0.08
        )
        score = side * r4 * math.log1p(vol_bar) / max(pullback, 0.005)
        return ok, side, score

    if profile.family == "shock_reversal":
        thresholds = {
            1: (0.030, 0.004, 1.8, 0.60),
            2: (0.045, 0.006, 2.5, 0.68),
            3: (0.065, 0.009, 3.5, 0.75),
        }
        min_shock, min_reversal, min_vol, min_loc = thresholds[level]
        shock = float(values["prior_hour_return"])
        side = -1 if shock > 0 else 1
        swept = bool(values["swept_high"] if side < 0 else values["swept_low"])
        ok = (
            min_shock <= abs(shock) <= 0.22
            and side * r15 >= min_reversal
            and swept
            and vol_bar >= min_vol
            and directional_close_location_proxy(close_loc_long, side) >= min_loc
            and candle_range <= 0.15
        )
        score = abs(shock) * abs(r15) * math.log1p(vol_bar)
        return ok, side, score

    if profile.family == "pump_continuation":
        thresholds = {
            1: (0.06, 0.15, 0.60),
            2: (0.08, 0.15, 0.65),
            3: (0.10, 0.18, 0.70),
        }
        min_move, max_move, min_loc = thresholds[level]
        side = 1
        ok = (
            min_move <= r1 < max_move
            and close_loc_long >= min_loc
            and r15 > -0.01
            and candle_range <= 0.20
        )
        return ok, side, r1 * (0.5 + close_loc_long)

    if profile.family == "blowoff_fade":
        thresholds = {1: 0.12, 2: 0.15, 3: 0.20}
        side = -1
        ok = thresholds[level] <= r1 <= 1.50 and candle_range <= 0.50
        return ok, side, r1 * math.log1p(max(vol_hour, 0.0))

    if profile.family == "dump_rebound":
        thresholds = {
            1: (0.03, 0.08, 0.30, 0.70),
            2: (0.04, 0.08, 0.35, 0.60),
            3: (0.05, 0.10, 0.38, 0.58),
        }
        min_move, max_move, min_long_loc, max_long_loc = thresholds[level]
        side = 1
        ok = (
            min_move <= -r1 < max_move
            and min_long_loc <= close_loc_long < max_long_loc
            and candle_range <= 0.20
        )
        return ok, side, -r1 * (0.5 + close_loc_long)

    raise ValueError(profile.family)


def directional_close_location_proxy(long_location: float, side: int) -> float:
    return long_location if side > 0 else 1.0 - long_location


def build_signals(symbol: str, bars: list[Bar]) -> dict[str, list[Signal]]:
    output: dict[str, list[Signal]] = defaultdict(list)
    if len(bars) < 14 * 96 + 2:
        return output
    quote = [bar.quote_volume for bar in bars]
    quote_prefix = prefix(quote)
    tr = [0.0]
    for index in range(1, len(bars)):
        previous = bars[index - 1].close
        tr.append(max(bars[index].high - bars[index].low, abs(bars[index].high - previous), abs(bars[index].low - previous)))
    tr_prefix = prefix(tr)
    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    prior_high_6h = rolling_prior_extreme(highs, 24, True)
    prior_low_6h = rolling_prior_extreme(lows, 24, False)
    prior_high_2h = rolling_prior_extreme(highs, 8, True)
    prior_low_2h = rolling_prior_extreme(lows, 8, False)
    warmup = 14 * 96
    for index in range(warmup, len(bars) - 1):
        if bars[index].ts - bars[index - 96].ts > 97 * BAR_MS:
            continue
        close = bars[index].close
        r15 = close / bars[index - 1].close - 1.0
        r1 = close / bars[index - 4].close - 1.0
        r4 = close / bars[index - 16].close - 1.0
        r24 = close / bars[index - 96].close - 1.0
        q24 = total(quote_prefix, index - 95, index + 1)
        if q24 < 5_000_000.0:
            continue
        q7d = total(quote_prefix, index - 7 * 96, index)
        vol_bar = quote[index] / max(q7d / (7 * 96), 1.0)
        q1 = total(quote_prefix, index - 3, index + 1)
        vol_hour = q1 / max(q7d / (7 * 24), 1.0)
        atr6 = total(tr_prefix, index - 23, index + 1) / 24
        atr7 = total(tr_prefix, index - 7 * 96, index) / (7 * 96)
        spread = bars[index].high - bars[index].low
        close_loc_long = (close - bars[index].low) / spread if spread > 0 else 0.5
        breakout6_side = 1 if close > prior_high_6h[index] else -1 if close < prior_low_6h[index] else 0
        prior_hour_return = bars[index - 1].close / bars[index - 5].close - 1.0
        pullback_depth_long = (prior_high_6h[index] - bars[index - 1].close) / max(prior_high_6h[index], 1e-12)
        pullback_depth_short = (bars[index - 1].close - prior_low_6h[index]) / max(prior_low_6h[index], 1e-12)
        values: dict[str, float | int | bool] = {
            "r15": r15,
            "r1": r1,
            "r4": r4,
            "r24": r24,
            "vol_bar": vol_bar,
            "vol_hour": vol_hour,
            "volume_24h": q24,
            "efficiency": efficiency(bars, index - 4, index),
            "close_loc_long": close_loc_long,
            "candle_range": spread / max(close, 1e-12),
            "atr": atr6,
            "atr_fraction": atr6 / max(close, 1e-12),
            "atr_compression": atr6 / max(atr7, 1e-12),
            "breakout6_side": breakout6_side,
            "prior_hour_return": prior_hour_return,
            "pullback_depth_long": pullback_depth_long,
            "pullback_depth_short": pullback_depth_short,
            "swept_high": bars[index].high >= prior_high_2h[index],
            "swept_low": bars[index].low <= prior_low_2h[index],
            "signal_index": index,
            "signal_ts": bars[index].ts + BAR_MS - 1,
        }
        for profile in ENTRY_PROFILES:
            accepted, side, score = accepts(profile, values)
            if accepted:
                output[profile.name].append(
                    Signal(symbol, bars[index + 1].ts, side, score, profile.family, dict(values))
                )
    return output


def simulate(
    bars_by_symbol: dict[str, list[Bar]],
    signals: list[Signal],
    exit_profile: ExitProfile,
    start_ts: int,
    end_ts: int,
    slippage_bps: float = 5.0,
    risk_per_trade: float = 0.04,
) -> dict[str, object]:
    fee = 5.0 / 10_000.0
    slippage = slippage_bps / 10_000.0
    selected = {signal.symbol for signal in signals if start_ts <= signal.execute_ts < end_ts}
    lookups = {
        symbol: {bar.ts: (index, bar) for index, bar in enumerate(bars_by_symbol[symbol])}
        for symbol in selected
    }
    signal_map: dict[int, list[Signal]] = defaultdict(list)
    for signal in signals:
        if start_ts <= signal.execute_ts < end_ts:
            signal_map[signal.execute_ts].append(signal)
    cash = 1_000.0
    positions: dict[str, Position] = {}
    last_prices: dict[str, float] = {}
    last_exit: dict[str, int] = {}
    trades: list[dict[str, object]] = []
    curve = [cash]
    current_day = -1
    day_start_equity = cash
    daily_entries = 0

    def equity() -> float:
        return cash + sum(
            position.side * position.qty * (last_prices.get(symbol, position.entry_price) - position.entry_price)
            for symbol, position in positions.items()
        )

    def close_position(symbol: str, ts: int, raw_price: float, reason: str) -> None:
        nonlocal cash
        position = positions.pop(symbol)
        price = raw_price * (1.0 - position.side * slippage)
        gross = position.side * position.qty * (price - position.entry_price)
        exit_fee = position.qty * price * fee
        pnl = gross - position.entry_fee - exit_fee
        cash += gross - exit_fee
        trades.append(
            {
                "symbol": symbol,
                "side": "long" if position.side > 0 else "short",
                "entry_ts": position.entry_ts,
                "exit_ts": ts,
                "entry_price": position.entry_price,
                "exit_price": price,
                "pnl": pnl,
                "return_on_notional": pnl / (position.qty * position.entry_price),
                "reason": reason,
                "kind": position.kind,
            }
        )
        last_exit[symbol] = ts

    for ts in range((start_ts // BAR_MS) * BAR_MS, end_ts, BAR_MS):
        for symbol in list(positions):
            lookup = lookups.get(symbol, {}).get(ts)
            if lookup is None:
                continue
            index, bar = lookup
            position = positions[symbol]
            last_prices[symbol] = bar.open
            gap_stop = bar.open <= position.stop if position.side > 0 else bar.open >= position.stop
            stop_hit = bar.low <= position.stop if position.side > 0 else bar.high >= position.stop
            target_hit = bar.high >= position.target if position.side > 0 else bar.low <= position.target
            timed = index - position.entry_index >= exit_profile.max_bars
            if gap_stop or stop_hit:
                close_position(symbol, ts, bar.open if gap_stop else position.stop, "stop")
                continue
            if target_hit:
                close_position(symbol, ts, position.target, "target")
                continue
            if timed:
                close_position(symbol, ts, bar.open, "time")
                continue

        current_equity = equity()
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start_equity = current_equity
            daily_entries = 0
        gross = sum(position.qty * last_prices.get(symbol, position.entry_price) for symbol, position in positions.items())
        capacity = max(0.0, 4.0 * current_equity - gross)
        for signal in sorted(signal_map.get(ts, []), key=lambda item: item.score, reverse=True):
            if (
                daily_entries >= 8
                or current_equity < day_start_equity * 0.92
                or len(positions) >= 2
                or signal.symbol in positions
                or capacity < 50.0
            ):
                continue
            if ts - last_exit.get(signal.symbol, -10**18) < 4 * 60 * 60 * 1_000:
                continue
            lookup = lookups.get(signal.symbol, {}).get(ts)
            if lookup is None:
                continue
            index, bar = lookup
            notional = min(current_equity * risk_per_trade / exit_profile.stop, 2.0 * current_equity, capacity)
            entry = bar.open * (1.0 + signal.side * slippage)
            qty = notional / entry
            entry_fee = notional * fee
            cash -= entry_fee
            positions[signal.symbol] = Position(
                signal.symbol,
                signal.side,
                qty,
                ts,
                index,
                entry,
                entry_fee,
                entry * (1.0 - signal.side * exit_profile.stop),
                entry * (1.0 + signal.side * exit_profile.target),
                entry,
                signal.kind,
            )
            daily_entries += 1
            capacity -= notional

        for symbol, position in positions.items():
            lookup = lookups.get(symbol, {}).get(ts)
            if lookup is None:
                continue
            _, bar = lookup
            last_prices[symbol] = bar.close
            if position.side > 0:
                position.extreme = max(position.extreme, bar.high)
                if position.extreme / position.entry_price - 1.0 >= exit_profile.trail_activation:
                    position.stop = max(position.stop, position.extreme * (1.0 - exit_profile.trail_distance))
            else:
                position.extreme = min(position.extreme, bar.low)
                if 1.0 - position.extreme / position.entry_price >= exit_profile.trail_activation:
                    position.stop = min(position.stop, position.extreme * (1.0 + exit_profile.trail_distance))
        curve.append(equity())

    for symbol in list(positions):
        close_position(symbol, end_ts, last_prices.get(symbol, positions[symbol].entry_price), "end")
    peak = 1_000.0
    drawdown = 0.0
    for value in curve + [cash]:
        peak = max(peak, value)
        drawdown = max(drawdown, 1.0 - value / peak)
    wins = [float(trade["pnl"]) for trade in trades if float(trade["pnl"]) > 0]
    losses = [-float(trade["pnl"]) for trade in trades if float(trade["pnl"]) < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    ordered_wins = sorted(wins, reverse=True)
    return {
        "return_pct": (cash / 1_000.0 - 1.0) * 100.0,
        "final_equity": cash,
        "max_drawdown_pct": drawdown * 100.0,
        "trades": len(trades),
        "win_rate_pct": len(wins) / len(trades) * 100.0 if trades else 0.0,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
        "expectancy_usd": sum(float(trade["pnl"]) for trade in trades) / len(trades) if trades else 0.0,
        "top_two_winners_share": sum(ordered_wins[:2]) / gross_profit if gross_profit > 0 else 0.0,
        "longs": sum(trade["side"] == "long" for trade in trades),
        "shorts": sum(trade["side"] == "short" for trade in trades),
        "targets": sum(trade["reason"] == "target" for trade in trades),
        "stops": sum(trade["reason"] == "stop" for trade in trades),
        "times": sum(trade["reason"] == "time" for trade in trades),
        "best": sorted(trades, key=lambda item: float(item["pnl"]), reverse=True)[:5],
        "worst": sorted(trades, key=lambda item: float(item["pnl"]))[:5],
    }


def objective(result: dict[str, object]) -> float:
    trades = int(result["trades"])
    pf = float(result["profit_factor"] or 0.0)
    if trades < 25 or pf <= 0:
        return -1_000.0
    return float(result["return_pct"]) - 0.50 * float(result["max_drawdown_pct"]) + 12.0 * (pf - 1.0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-alpha-search.json")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--phase-only", action="store_true")
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted({path.parent.name for path in (cache / "klines").glob("*/*.zip")})
    allowed = spot_history_symbols()
    symbols = [symbol for symbol in symbols if symbol in allowed and symbol not in MAJORS]
    bars_by_symbol: dict[str, list[Bar]] = {}
    signals_by_profile: dict[str, list[Signal]] = defaultdict(list)
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        if len(bars) < 15 * 96:
            continue
        bars_by_symbol[symbol] = bars
        generated = build_signals(symbol, bars)
        for name, signals in generated.items():
            signals_by_profile[name].extend(signals)
        if completed % 50 == 0:
            print(f"features {completed}/{len(symbols)} signals={sum(map(len, signals_by_profile.values()))}", flush=True)

    active_entries = (
        [entry for entry in ENTRY_PROFILES if entry.family in {"pump_continuation", "blowoff_fade", "dump_rebound"}]
        if args.phase_only
        else ENTRY_PROFILES
    )
    results: dict[str, object] = {}
    train_start, train_end = map(timestamp, PERIODS["train"])
    for entry in active_entries:
        signals = signals_by_profile[entry.name]
        for exit_profile in EXIT_PROFILES:
            name = f"{entry.name}__{exit_profile.name}"
            train = simulate(
                bars_by_symbol,
                signals,
                exit_profile,
                train_start,
                train_end,
                args.slippage_bps,
            )
            results[name] = {
                "entry": entry.__dict__,
                "exit": exit_profile.__dict__,
                "signal_count": len(signals),
                "train": train,
                "objective": objective(train),
            }
            print(name, round(float(train["return_pct"]), 2), train["trades"], train["profit_factor"], flush=True)

    ranked = sorted(results, key=lambda name: float(results[name]["objective"]), reverse=True)
    selected: list[str] = []
    selected_families: set[str] = set()
    for name in ranked:
        family = str(results[name]["entry"]["family"])
        train = results[name]["train"]
        if family in selected_families:
            continue
        if int(train["trades"]) < 25:
            continue
        selected.append(name)
        selected_families.add(family)

    for name in selected:
        entry_name = name.split("__", 1)[0]
        entry = next(profile for profile in ENTRY_PROFILES if profile.name == entry_name)
        exit_profile = next(profile for profile in EXIT_PROFILES if name.endswith(profile.name))
        signals = signals_by_profile[entry.name]
        periods: dict[str, object] = {}
        for period, (start, end) in PERIODS.items():
            periods[period] = simulate(
                bars_by_symbol,
                signals,
                exit_profile,
                timestamp(start),
                timestamp(end),
                args.slippage_bps,
            )
        stress = simulate(
            bars_by_symbol,
            signals,
            exit_profile,
            timestamp(PERIODS["full"][0]),
            timestamp(PERIODS["full"][1]),
            15.0,
        )
        results[name]["periods"] = periods
        results[name]["stress_15bps"] = stress

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "source": "Binance Vision USD-M 15m klines",
            "symbols": len(bars_by_symbol),
            "universe": "USD-M symbols with Binance Vision spot USDT history directory, excluding majors",
            "periods": PERIODS,
        },
        "assumptions": {
            "causal": "closed 15m signal; next 15m open fill",
            "fee_each_side_bps": 5,
            "slippage_each_side_bps": args.slippage_bps,
            "stress_slippage_each_side_bps": 15,
            "initial_equity": 1_000,
            "risk_per_trade_pct": 4,
            "max_notional_each_position_equity": 2,
            "max_positions": 2,
            "daily_entries": 8,
            "daily_loss_gate_pct": 8,
            "same_symbol_cooldown_hours": 4,
            "intrabar": "stop before target",
        },
        "selected_one_per_family": selected,
        "results": results,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
