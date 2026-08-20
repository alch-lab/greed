#!/usr/bin/env python3
"""Reproduce and stress-test a diversified daily crypto trend program.

The experiment follows the public design in Concretum Research's
"Catching Crypto Trends": Donchian signals, an equal-weighted horizon ensemble,
90-day volatility scaling, a monthly trailing-volume universe and a 20% no-trade
band for volatility-only resizing.  Binance funding and executable costs are
added because the public paper uses aggregated spot data rather than perps.

This module is offline research code.  It never submits orders.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import statistics
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


DAY_MS = 86_400_000
YEAR_DAYS = 365.25


@dataclass(frozen=True)
class Day:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float
    funding_rate: float


@dataclass(frozen=True)
class Model:
    name: str
    horizons: tuple[int, ...]
    allow_short: bool
    short_multiplier: float = 1.0
    target_asset_vol: float = 0.25
    vol_window: int = 90
    asset_leverage_cap: float = 2.0
    gross_cap: float = 1.0
    top_n: int = 10
    minimum_daily_volume: float = 1_000_000.0
    minimum_daily_move: float = 0.005
    resize_threshold: float = 0.20
    fee_bps: float = 5.0
    slippage_bps: float = 2.0


@dataclass
class Result:
    model: Model
    initial_cash: float
    final_equity: float
    curve: list[tuple[int, float]]
    turnover: float
    costs: float
    funding_pnl: float
    trade_events: int
    max_gross: float


def archive_rows(path: Path) -> list[list[str]]:
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise ValueError(f"unexpected archive members in {path}")
        with archive.open(names[0]) as raw:
            return list(csv.reader(io.TextIOWrapper(raw)))


def month_key(path: Path, marker: str) -> str:
    return path.stem.split(marker, 1)[1]


def load_daily(root: Path, symbol: str) -> list[Day]:
    """Load only months having both kline and funding archives."""
    kline_paths = {
        month_key(path, "-4h-"): path
        for path in (root / symbol / "klines").glob("*.zip")
    }
    funding_paths = {
        month_key(path, "-fundingRate-"): path
        for path in (root / symbol / "funding").glob("*.zip")
    }
    bars_by_day: dict[int, list[tuple[float, float, float, float, float]]] = defaultdict(list)
    funding_by_day: dict[int, float] = defaultdict(float)
    for month in sorted(kline_paths.keys() & funding_paths.keys()):
        rows = archive_rows(kline_paths[month])
        if rows and rows[0][0] == "open_time":
            rows = rows[1:]
        for row in rows:
            day_ts = int(row[0]) // DAY_MS * DAY_MS
            bars_by_day[day_ts].append(
                (float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[7]))
            )
        rows = archive_rows(funding_paths[month])
        if rows and rows[0][0] == "calc_time":
            rows = rows[1:]
        for row in rows:
            day_ts = int(row[0]) // DAY_MS * DAY_MS
            funding_by_day[day_ts] += float(row[2])
    result: list[Day] = []
    for ts in sorted(bars_by_day):
        bars = bars_by_day[ts]
        result.append(
            Day(
                ts=ts,
                open=bars[0][0],
                high=max(item[1] for item in bars),
                low=min(item[2] for item in bars),
                close=bars[-1][3],
                quote_volume=sum(item[4] for item in bars),
                funding_rate=funding_by_day.get(ts, 0.0),
            )
        )
    return result


def rolling_volatility(days: list[Day], window: int) -> list[float | None]:
    returns = [0.0]
    for previous, current in zip(days, days[1:]):
        returns.append(current.close / previous.close - 1.0)
    result: list[float | None] = [None] * len(days)
    for index in range(window, len(days)):
        sample = returns[index - window + 1 : index + 1]
        vol = statistics.stdev(sample) * math.sqrt(365.0)
        result[index] = vol if vol > 0.0 else None
    return result


def donchian_states(days: list[Day], horizon: int, allow_short: bool) -> list[int]:
    """Signal observed at each close; target becomes executable next day."""
    result = [0] * len(days)
    state = 0
    stop: float | None = None
    closes = [day.close for day in days]
    for index in range(horizon - 1, len(days)):
        sample = closes[index - horizon + 1 : index + 1]
        upper = max(sample)
        lower = min(sample)
        midpoint = 0.5 * (upper + lower)
        close = closes[index]
        if state > 0:
            stop = midpoint if stop is None else max(stop, midpoint)
            if close <= stop:
                state = -1 if allow_short and close <= lower else 0
                stop = midpoint if state else None
        elif state < 0:
            stop = midpoint if stop is None else min(stop, midpoint)
            if close >= stop:
                state = 1 if close >= upper else 0
                stop = midpoint if state else None
        elif close >= upper:
            state = 1
            stop = midpoint
        elif allow_short and close <= lower:
            state = -1
            stop = midpoint
        result[index] = state
    return result


def ensemble_inputs(days: list[Day], model: Model) -> tuple[dict[int, float], dict[int, float]]:
    states = [donchian_states(days, horizon, model.allow_short) for horizon in model.horizons]
    volatility = rolling_volatility(days, model.vol_window)
    signal_at_open: dict[int, float] = {}
    vol_at_open: dict[int, float] = {}
    for execute_index in range(1, len(days)):
        signal_index = execute_index - 1
        signal = sum(items[signal_index] for items in states) / len(states)
        if signal < 0.0:
            signal *= model.short_multiplier
        vol = volatility[signal_index]
        if vol is not None:
            signal_at_open[days[execute_index].ts] = signal
            vol_at_open[days[execute_index].ts] = vol
    return signal_at_open, vol_at_open


def monthly_universe(
    all_days: dict[str, list[Day]], model: Model, timeline: list[int]
) -> dict[int, set[str]]:
    by_symbol = {symbol: {day.ts: day for day in days} for symbol, days in all_days.items()}
    selected: set[str] = set()
    result: dict[int, set[str]] = {}
    prior_month: tuple[int, int] | None = None
    for ts in timeline:
        stamp = datetime.fromtimestamp(ts / 1_000, tz=timezone.utc)
        month = (stamp.year, stamp.month)
        if month != prior_month:
            ranked: list[tuple[float, str]] = []
            begin = ts - 30 * DAY_MS
            for symbol, lookup in by_symbol.items():
                sample = [lookup[item] for item in sorted(lookup) if begin <= item < ts]
                if len(sample) < 20:
                    continue
                volumes = [day.quote_volume for day in sample]
                moves = [
                    abs(current.close / previous.close - 1.0)
                    for previous, current in zip(sample, sample[1:])
                ]
                median_volume = statistics.median(volumes)
                median_move = statistics.median(moves) if moves else 0.0
                if median_volume >= model.minimum_daily_volume and median_move >= model.minimum_daily_move:
                    ranked.append((median_volume, symbol))
            ranked.sort(reverse=True)
            selected = {symbol for _, symbol in ranked[: model.top_n]}
            prior_month = month
        result[ts] = set(selected)
    return result


def simulate(
    all_days: dict[str, list[Day]],
    model: Model,
    start_ts: int,
    end_ts: int,
    initial_cash: float = 100_000.0,
) -> Result:
    lookup = {symbol: {day.ts: day for day in days} for symbol, days in all_days.items()}
    last_ts = {symbol: days[-1].ts for symbol, days in all_days.items()}
    timeline = sorted(
        {
            day.ts
            for days in all_days.values()
            for day in days
            if start_ts <= day.ts <= end_ts
        }
    )
    signals: dict[str, dict[int, float]] = {}
    volatilities: dict[str, dict[int, float]] = {}
    for symbol, days in all_days.items():
        signals[symbol], volatilities[symbol] = ensemble_inputs(days, model)
    universe = monthly_universe(all_days, model, timeline)
    equity = initial_cash
    quantities: dict[str, float] = defaultdict(float)
    previous_close: dict[str, float] = {}
    prior_signal: dict[str, float] = defaultdict(float)
    curve: list[tuple[int, float]] = []
    turnover = costs = funding_pnl = 0.0
    trade_events = 0
    max_gross = 0.0
    one_way_cost = (model.fee_bps + model.slippage_bps) / 10_000.0

    for ts in timeline:
        current = {symbol: days[ts] for symbol, days in lookup.items() if ts in days}
        # Mark carried positions through the overnight gap.
        for symbol, qty in list(quantities.items()):
            if qty and symbol in current and symbol in previous_close:
                equity += qty * (current[symbol].open - previous_close[symbol])

        eligible = universe.get(ts, set())
        target_weights: dict[str, float] = {}
        divisor = max(1, len(eligible))
        for symbol in eligible:
            if symbol not in current:
                continue
            signal = signals[symbol].get(ts, 0.0)
            vol = volatilities[symbol].get(ts)
            if vol is None or vol <= 0.0:
                continue
            exposure = min(model.target_asset_vol / vol, model.asset_leverage_cap)
            target_weights[symbol] = signal * exposure / divisor
        gross_target = sum(abs(weight) for weight in target_weights.values())
        if gross_target > model.gross_cap:
            scale = model.gross_cap / gross_target
            target_weights = {symbol: weight * scale for symbol, weight in target_weights.items()}

        tradable = set(quantities) | set(target_weights)
        for symbol in sorted(tradable):
            if symbol not in current:
                continue
            price = current[symbol].open
            current_notional = quantities[symbol] * price
            target_notional = equity * target_weights.get(symbol, 0.0)
            signal = signals[symbol].get(ts, 0.0) if symbol in eligible else 0.0
            signal_changed = abs(signal - prior_signal[symbol]) > 1e-12
            allocation_change = abs(target_notional - current_notional)
            relative_change = allocation_change / max(abs(current_notional), 1.0)
            should_trade = signal_changed or relative_change > model.resize_threshold
            if should_trade and allocation_change > 1.0:
                equity -= allocation_change * one_way_cost
                turnover += allocation_change
                costs += allocation_change * one_way_cost
                quantities[symbol] = target_notional / price
                trade_events += 1
            prior_signal[symbol] = signal

        day_funding = 0.0
        for symbol, qty in quantities.items():
            if qty and symbol in current:
                day_funding -= qty * current[symbol].close * current[symbol].funding_rate
        funding_pnl += day_funding
        equity += day_funding
        for symbol, qty in list(quantities.items()):
            if qty and symbol in current:
                equity += qty * (current[symbol].close - current[symbol].open)
        for symbol, day in current.items():
            previous_close[symbol] = day.close

        # Realistically flatten a contract on its final available day.
        for symbol, qty in list(quantities.items()):
            if qty and symbol in current and ts == last_ts[symbol]:
                exit_notional = abs(qty) * current[symbol].close
                equity -= exit_notional * one_way_cost
                turnover += exit_notional
                costs += exit_notional * one_way_cost
                quantities[symbol] = 0.0
                prior_signal[symbol] = 0.0
                trade_events += 1
        gross = sum(
            abs(qty) * current[symbol].close
            for symbol, qty in quantities.items()
            if qty and symbol in current
        )
        max_gross = max(max_gross, gross / equity if equity > 0.0 else math.inf)
        curve.append((ts + DAY_MS - 1, equity))

    return Result(model, initial_cash, equity, curve, turnover, costs, funding_pnl, trade_events, max_gross)


def summarize(result: Result, start_ts: int, end_ts: int) -> dict[str, float | int | str]:
    total_return = result.final_equity / result.initial_cash - 1.0
    years = max((end_ts - start_ts) / (YEAR_DAYS * DAY_MS), 1.0 / YEAR_DAYS)
    cagr = (result.final_equity / result.initial_cash) ** (1.0 / years) - 1.0
    peak = 0.0
    max_drawdown = 0.0
    returns: list[float] = []
    previous = None
    for _, equity in result.curve:
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1.0 - equity / peak)
        if previous and previous > 0.0:
            returns.append(equity / previous - 1.0)
        previous = equity
    sharpe = 0.0
    if len(returns) > 2 and statistics.stdev(returns) > 0.0:
        sharpe = statistics.mean(returns) / statistics.stdev(returns) * math.sqrt(365.0)
    return {
        "name": result.model.name,
        "return_pct": total_return * 100.0,
        "cagr_pct": cagr * 100.0,
        "max_drawdown_pct": max_drawdown * 100.0,
        "sharpe": sharpe,
        "turnover_multiple": result.turnover / result.initial_cash,
        "costs": result.costs,
        "funding_pnl": result.funding_pnl,
        "trade_events": result.trade_events,
        "max_gross": result.max_gross,
    }


def timestamp(value: str) -> int:
    return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1_000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--symbols", required=True)
    args = parser.parse_args()
    symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]
    all_days = {symbol: load_daily(args.data, symbol) for symbol in symbols}
    all_days = {symbol: days for symbol, days in all_days.items() if len(days) >= 180}
    models = [
        Model("single_5d_long", (5,), False),
        Model("single_10d_long", (10,), False),
        Model("single_20d_long", (20,), False),
        Model("single_30d_long", (30,), False),
        Model("combo_fast_long", (5, 10, 20, 30), False),
        Model("combo_all_long", (5, 10, 20, 30, 60, 90), False),
        Model("combo_all_ls", (5, 10, 20, 30, 60, 90), True),
        Model("combo_all_70_30", (5, 10, 20, 30, 60, 90), True, short_multiplier=3 / 7),
    ]
    periods = {
        "year_2021": (timestamp("2021-01-01"), timestamp("2021-12-31") + DAY_MS - 1),
        "year_2022": (timestamp("2022-01-01"), timestamp("2022-12-31") + DAY_MS - 1),
        "year_2023": (timestamp("2023-01-01"), timestamp("2023-12-31") + DAY_MS - 1),
        "year_2024": (timestamp("2024-01-01"), timestamp("2024-12-31") + DAY_MS - 1),
        "year_2025": (timestamp("2025-01-01"), timestamp("2025-12-31") + DAY_MS - 1),
        "oos_2026": (timestamp("2026-01-01"), timestamp("2026-07-31") + DAY_MS - 1),
        "full": (timestamp("2021-01-01"), timestamp("2026-07-31") + DAY_MS - 1),
    }
    report: dict[str, object] = {
        "symbols": sorted(all_days),
        "periods": {},
        "assumptions": {
            "source": "Binance Vision USD-M 4h klines and realized funding",
            "dynamic_universe": "monthly top 10 by prior 30-day median quote volume",
            "fee_bps_one_way": 5.0,
            "slippage_bps_one_way": 2.0,
            "target_asset_vol": 0.25,
            "vol_window_days": 90,
            "resize_threshold": 0.20,
        },
    }
    for period_name, (start, end) in periods.items():
        report["periods"][period_name] = [
            summarize(simulate(all_days, model, start, end), start, end)
            for model in models
        ]
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.out:
        args.out.write_text(text + "\n")


if __name__ == "__main__":
    main()
