#!/usr/bin/env python3
"""Backtest a causal altcoin OI/positioning launch model on Binance Vision data.

The experiment deliberately separates data acquisition from the production
runner.  Signals are formed at a 15 minute close and executed at the next open.
It compares the current price/volume breakout with the fixed thresholds shown
in the referenced OI post and a size-normalised variant suitable for small caps.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import http.client
import io
import json
import math
import os
import re
import ssl
import statistics
import time
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree


VISION = "https://data.binance.vision/data/futures/um"
S3_LIST = "https://data.binance.vision.s3.amazonaws.com/?list-type=2&delimiter=%2F&prefix=data%2Ffutures%2Fum%2Fdaily%2Fmetrics%2F"
BAR_MS = 15 * 60 * 1_000
DAY_MS = 24 * 60 * 60 * 1_000
MAJORS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float


@dataclass(frozen=True)
class Metric:
    ts: int
    oi_value: float
    top_account_lsr: float
    top_position_lsr: float
    global_lsr: float
    taker_lsr: float


@dataclass(frozen=True)
class Signal:
    symbol: str
    execute_ts: int
    side: int
    score: float
    kind: str
    features: dict[str, float | int | bool]


@dataclass(frozen=True)
class Profile:
    name: str
    kind: str
    min_return_1h: float = 0.01
    max_return_1h: float = 1_000_000.0
    min_return_4h: float = 0.0
    min_volume_ratio: float = 1.5
    min_oi_change_5m: float = 0.01
    min_oi_delta_usd: float = 500_000.0
    min_divergence: float = 1.10
    max_taker_lsr: float = 1_000_000.0
    min_setup_count: int = 1
    long_only: bool = False
    score_mode: str = "momentum"
    risk_per_trade: float = 0.10
    max_positions: int = 2
    stop_fraction: float = 0.05
    trail_activation: float = 0.02
    trail_fraction: float = 0.01
    max_holding_bars: int = 24


@dataclass
class Position:
    symbol: str
    side: int
    qty: float
    entry_ts: int
    entry_index: int
    entry_price: float
    entry_fee: float
    extreme: float
    stop: float
    kind: str


def fetch(url: str, destination: Path | None = None, attempts: int = 4) -> bytes:
    context = ssl._create_unverified_context() if "s3.amazonaws.com" in url else None
    error: Exception | None = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "greed-research/1.0"})
            with urllib.request.urlopen(request, timeout=30, context=context) as response:
                data = response.read()
            if destination is not None:
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_suffix(destination.suffix + ".tmp")
                temporary.write_bytes(data)
                temporary.replace(destination)
            return data
        except (OSError, urllib.error.URLError, urllib.error.HTTPError, http.client.IncompleteRead) as caught:
            error = caught
            if isinstance(caught, urllib.error.HTTPError) and caught.code == 404:
                break
            time.sleep(0.35 * 2**attempt)
    raise RuntimeError(f"download failed: {url}: {error}")


def is_zip(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as archive:
            return bool(archive.namelist())
    except (OSError, zipfile.BadZipFile):
        return False


def download(url: str, path: Path) -> Path | None:
    if path.exists() and is_zip(path):
        return path
    try:
        fetch(url, path)
    except RuntimeError:
        path.unlink(missing_ok=True)
        return None
    if not is_zip(path):
        path.unlink(missing_ok=True)
        return None
    return path


def list_symbols(limit: int | None = None) -> list[str]:
    root = ElementTree.fromstring(fetch(S3_LIST))
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    symbols: list[str] = []
    for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace):
        match = re.search(r"/([^/]+)/$", node.text or "")
        if match and re.fullmatch(r"[A-Z0-9]+USDT", match.group(1)):
            symbol = match.group(1)
            if symbol not in MAJORS:
                symbols.append(symbol)
    symbols.sort()
    return symbols[:limit] if limit else symbols


def month_range(start_month: str, end_month: str) -> list[str]:
    current = datetime.strptime(start_month, "%Y-%m").date().replace(day=1)
    end = datetime.strptime(end_month, "%Y-%m").date().replace(day=1)
    values: list[str] = []
    while current <= end:
        values.append(current.strftime("%Y-%m"))
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return values


def read_zip_rows(path: Path) -> list[list[str]]:
    with zipfile.ZipFile(path) as archive:
        name = archive.namelist()[0]
        text = archive.read(name).decode("utf-8")
    return list(csv.reader(io.StringIO(text)))


def load_bars(paths: list[Path]) -> list[Bar]:
    result: dict[int, Bar] = {}
    for path in paths:
        rows = read_zip_rows(path)
        for row in rows[1:] if rows and not rows[0][0].isdigit() else rows:
            try:
                ts = int(row[0])
                result[ts] = Bar(ts, float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[7]))
            except (IndexError, ValueError):
                continue
    return [result[key] for key in sorted(result)]


def load_metrics(paths: list[Path]) -> list[Metric]:
    result: dict[int, Metric] = {}
    for path in paths:
        rows = read_zip_rows(path)
        for row in rows[1:]:
            try:
                ts = int(datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1_000)
                result[ts] = Metric(ts, float(row[3]), float(row[4]), float(row[5]), float(row[6]), float(row[7]))
            except (IndexError, ValueError):
                continue
    return [result[key] for key in sorted(result)]


def load_funding(paths: list[Path]) -> tuple[list[int], list[float]]:
    values: dict[int, float] = {}
    for path in paths:
        rows = read_zip_rows(path)
        for row in rows[1:] if rows and not rows[0][0].isdigit() else rows:
            try:
                values[int(row[0])] = float(row[2])
            except (IndexError, ValueError):
                continue
    times = sorted(values)
    return times, [values[item] for item in times]


def latest(times: list[int], values: list[float], ts: int, default: float = 0.0) -> float:
    index = bisect.bisect_right(times, ts) - 1
    return values[index] if index >= 0 else default


def rolling_prior_extreme(values: list[float], window: int, maximum: bool) -> list[float]:
    """Extreme of the preceding window, excluding the current item, in O(n)."""
    result = [math.nan] * len(values)
    queue: deque[int] = deque()
    for index, value in enumerate(values):
        while queue and queue[0] < index - window:
            queue.popleft()
        if queue:
            result[index] = values[queue[0]]
        while queue and ((values[queue[-1]] <= value) if maximum else (values[queue[-1]] >= value)):
            queue.pop()
        queue.append(index)
    return result


def bar_features(bars: list[Bar]) -> dict[int, dict[str, float | bool | int]]:
    features: dict[int, dict[str, float | bool | int]] = {}
    hourly = [0.0] * len(bars)
    rolling_24h = [0.0] * len(bars)
    volume_prefix = [0.0]
    for bar in bars:
        volume_prefix.append(volume_prefix[-1] + bar.quote_volume)
    for i in range(len(bars)):
        hourly[i] = volume_prefix[i + 1] - volume_prefix[max(0, i - 3)]
        rolling_24h[i] = volume_prefix[i + 1] - volume_prefix[max(0, i - 95)]
    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    prior_high_24h = rolling_prior_extreme(highs, 96, True)
    prior_low_24h = rolling_prior_extreme(lows, 96, False)
    prior_high_14d = rolling_prior_extreme(highs, 14 * 96, True)
    prior_low_14d = rolling_prior_extreme(lows, 14 * 96, False)
    prior_max_volume_14d = rolling_prior_extreme(rolling_24h, 14 * 96, True)
    warmup = 14 * 96
    for i in range(warmup, len(bars) - 1):
        close = bars[i].close
        return_1h = close / bars[i - 4].close - 1.0
        return_4h = close / bars[i - 16].close - 1.0
        prior_high = prior_high_24h[i]
        prior_low = prior_low_24h[i]
        side = 1 if close > prior_high else -1 if close < prior_low else 0
        if abs(return_1h) < 0.01 or rolling_24h[i] < 2_000_000.0:
            continue
        median_volume = statistics.median(hourly[i - 7 * 96 : i])
        volume_ratio = hourly[i] / max(median_volume, 1.0)
        if volume_ratio < 1.5:
            continue
        path = [item.close for item in bars[i - 4 : i + 1]]
        traveled = sum(abs(right - left) for left, right in zip(path, path[1:]))
        efficiency = abs(path[-1] - path[0]) / max(traveled, 1e-12)
        spread = bars[i].high - bars[i].low
        close_location = (close - bars[i].low) / spread if spread > 0 else 0.5
        high_14d = prior_high_14d[i]
        low_14d = prior_low_14d[i]
        range_position = (close - low_14d) / max(high_14d - low_14d, 1e-12)
        max_volume_14d = prior_max_volume_14d[i]
        volume_compression = rolling_24h[i] / max(max_volume_14d, 1.0)
        range_24h = (max(prior_high, bars[i].high) - min(prior_low, bars[i].low)) / close
        range_14d = (high_14d - low_14d) / close
        range_compression = range_24h / max(range_14d, 1e-12)
        features[bars[i + 1].ts] = {
            "signal_ts": bars[i].ts + BAR_MS - 1,
            "return_1h": return_1h,
            "return_4h": return_4h,
            "breakout_side": side,
            "volume_ratio": volume_ratio,
            "volume_24h": rolling_24h[i],
            "efficiency": efficiency,
            "close_location": close_location,
            "range_position": range_position,
            "volume_compression": volume_compression,
            "range_compression": range_compression,
        }
    return features


def required_metric_dates(features: dict[int, dict[str, float | bool | int]]) -> set[date]:
    dates: set[date] = set()
    for values in features.values():
        side = 1 if float(values["return_1h"]) > 0 else -1
        close_location = float(values["close_location"])
        early_price_possible = (
            float(values["volume_24h"]) >= 5_000_000.0
            and side * float(values["return_4h"]) >= 0.0
            and float(values["efficiency"]) >= 0.25
            and ((side > 0 and close_location >= 0.55) or (side < 0 and close_location <= 0.45))
        )
        breakout_side = int(values["breakout_side"])
        baseline_possible = (
            breakout_side != 0
            and float(values["volume_ratio"]) >= 3.0
            and float(values["efficiency"]) >= 0.35
            and ((breakout_side > 0 and close_location >= 0.60) or (breakout_side < 0 and close_location <= 0.40))
            and abs(float(values["return_1h"])) >= 0.04
            and abs(float(values["return_4h"])) >= 0.06
        )
        if not (early_price_possible or baseline_possible):
            continue
        current = datetime.fromtimestamp(int(values["signal_ts"]) / 1_000, timezone.utc).date()
        dates.add(current)
    return dates


def metric_at(metrics: list[Metric], metric_times: list[int], ts: int) -> Metric | None:
    index = bisect.bisect_right(metric_times, ts) - 1
    return metrics[index] if index >= 0 else None


def make_signals(
    symbol: str,
    features: dict[int, dict[str, float | bool | int]],
    metrics: list[Metric],
    funding: tuple[list[int], list[float]],
    profile: Profile,
) -> list[Signal]:
    metric_times = [item.ts for item in metrics]
    funding_times, funding_values = funding
    output: list[Signal] = []
    for execute_ts, raw in features.items():
        return_1h = float(raw["return_1h"])
        return_4h = float(raw["return_4h"])
        volume_ratio = float(raw["volume_ratio"])
        volume_24h = float(raw["volume_24h"])
        efficiency = float(raw["efficiency"])
        close_location = float(raw["close_location"])
        breakout_side = int(raw["breakout_side"])
        baseline = (
            breakout_side != 0
            and volume_24h >= 5_000_000.0
            and volume_ratio >= 3.0
            and efficiency >= 0.35
            and ((breakout_side > 0 and close_location >= 0.60) or (breakout_side < 0 and close_location <= 0.40))
            and ((breakout_side > 0 and 0.04 <= return_1h <= 0.45 and 0.06 <= return_4h <= 1.20)
                 or (breakout_side < 0 and -0.45 <= return_1h <= -0.04 and -1.20 <= return_4h <= -0.06))
        )
        if profile.kind == "baseline":
            if baseline:
                output.append(Signal(symbol, execute_ts, breakout_side, abs(return_1h) * math.log1p(volume_ratio), "breakout", dict(raw)))
            continue

        signal_ts = int(raw["signal_ts"])
        current = metric_at(metrics, metric_times, signal_ts)
        previous = metric_at(metrics, metric_times, signal_ts - 5 * 60 * 1_000)
        if current is None or previous is None or previous.oi_value <= 0:
            continue
        oi_change = current.oi_value / previous.oi_value - 1.0
        oi_delta = current.oi_value - previous.oi_value
        divergence = current.top_position_lsr / max(current.global_lsr, 1e-9)
        rate = latest(funding_times, funding_values, signal_ts)
        side = 1 if return_1h > 0 else -1
        directional_return = side * return_1h
        directional_return_4h = side * return_4h
        directional_structure = (
            current.top_position_lsr >= 1.0
            and divergence >= profile.min_divergence
            and 0.80 <= current.taker_lsr <= profile.max_taker_lsr
            if side > 0
            else current.top_position_lsr <= 1.0 and divergence <= 1.0 / profile.min_divergence and current.taker_lsr <= 1.25
        )
        setup_count = sum(
            [
                float(raw["volume_compression"]) <= 0.50,
                float(raw["range_compression"]) <= 0.40,
                (float(raw["range_position"]) <= 0.60 if side > 0 else float(raw["range_position"]) >= 0.40),
                abs(rate) <= 0.0001,
            ]
        )
        early = (
            volume_24h >= 5_000_000.0
            and directional_return >= profile.min_return_1h
            and directional_return <= profile.max_return_1h
            and directional_return_4h >= profile.min_return_4h
            and volume_ratio >= profile.min_volume_ratio
            and efficiency >= 0.25
            and ((side > 0 and close_location >= 0.55) or (side < 0 and close_location <= 0.45))
            and oi_change >= profile.min_oi_change_5m
            and oi_delta >= profile.min_oi_delta_usd
            and directional_structure
            and setup_count >= profile.min_setup_count
            and ((side > 0 and rate <= 0.002) or (side < 0 and rate >= -0.002))
            and (side > 0 or not profile.long_only)
        )
        accepted = early if profile.kind in {"strict", "normalised"} else early or (baseline and directional_structure)
        if not accepted:
            continue
        score = (
            directional_return
            * math.log1p(volume_ratio)
            * math.log1p(max(oi_delta, 0.0) / 100_000.0)
            * max(divergence if side > 0 else 1.0 / max(divergence, 1e-9), 1.0)
        )
        if profile.score_mode == "absorption":
            score = (
                setup_count
                * max(divergence, 1.0)
                * max(1.4 - current.taker_lsr, 0.1)
                / (1.0 + 10.0 * directional_return)
            )
        enriched = dict(raw)
        enriched.update(
            {
                "oi_change_5m": oi_change,
                "oi_delta_usd": oi_delta,
                "top_position_lsr": current.top_position_lsr,
                "global_lsr": current.global_lsr,
                "divergence": divergence,
                "taker_lsr": current.taker_lsr,
                "funding_rate": rate,
                "setup_count": setup_count,
            }
        )
        output.append(Signal(symbol, execute_ts, side, score, "early_oi" if early else "structured_breakout", enriched))
    return output


def simulate(
    bars: dict[str, list[Bar]],
    signals: list[Signal],
    profile: Profile,
    start_ts: int,
    end_ts: int,
    initial_cash: float = 1_000.0,
) -> dict[str, object]:
    selected_symbols = {item.symbol for item in signals if start_ts <= item.execute_ts < end_ts}
    lookups = {symbol: {bar.ts: (index, bar) for index, bar in enumerate(bars[symbol])} for symbol in selected_symbols}
    signal_map: dict[int, list[Signal]] = defaultdict(list)
    for signal in signals:
        if start_ts <= signal.execute_ts < end_ts:
            signal_map[signal.execute_ts].append(signal)
    timestamps = list(range((start_ts // BAR_MS) * BAR_MS, end_ts, BAR_MS))
    cash = initial_cash
    positions: dict[str, Position] = {}
    last_prices: dict[str, float] = {}
    last_exit_ts: dict[str, int] = {}
    trades: list[dict[str, object]] = []
    curve: list[float] = []
    current_day: int | None = None
    day_start_equity = initial_cash
    daily_entries = 0
    fee_rate = 0.0005
    slippage = 0.0005

    def equity() -> float:
        return cash + sum(
            position.side * position.qty * (last_prices.get(symbol, position.entry_price) - position.entry_price)
            for symbol, position in positions.items()
        )

    def close(symbol: str, ts: int, raw_price: float, reason: str) -> None:
        nonlocal cash
        position = positions.pop(symbol)
        price = raw_price * (1.0 - position.side * slippage)
        gross = position.side * position.qty * (price - position.entry_price)
        exit_fee = position.qty * price * fee_rate
        cash += gross - exit_fee
        net = gross - position.entry_fee - exit_fee
        trades.append(
            {
                "symbol": symbol,
                "side": "long" if position.side > 0 else "short",
                "entry_ts": position.entry_ts,
                "exit_ts": ts,
                "pnl": net,
                "return_on_notional": net / (position.qty * position.entry_price),
                "reason": reason,
                "kind": position.kind,
            }
        )
        last_exit_ts[symbol] = ts

    for ts in timestamps:
        for symbol in list(positions):
            lookup = lookups.get(symbol, {}).get(ts)
            if lookup is None:
                continue
            index, bar = lookup
            last_prices[symbol] = bar.open
            position = positions[symbol]
            gap = bar.open <= position.stop if position.side > 0 else bar.open >= position.stop
            intrabar = bar.low <= position.stop if position.side > 0 else bar.high >= position.stop
            timed = index - position.entry_index >= profile.max_holding_bars
            if gap or intrabar or timed:
                close(symbol, ts, bar.open if gap or timed else position.stop, "stop" if gap or intrabar else "time")

        current_equity = equity()
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start_equity = current_equity
            daily_entries = 0
        gross_notional = sum(position.qty * last_prices.get(symbol, position.entry_price) for symbol, position in positions.items())
        capacity = max(0.0, 4.0 * current_equity - gross_notional)
        entries_allowed = daily_entries < 6 and current_equity >= day_start_equity * 0.96
        candidates = sorted(signal_map.get(ts, []), key=lambda item: item.score, reverse=True)
        for signal in candidates:
            if not entries_allowed or len(positions) >= profile.max_positions or capacity < 50 or signal.symbol in positions:
                continue
            if ts - last_exit_ts.get(signal.symbol, -10**18) < 8 * 60 * 60 * 1_000:
                continue
            lookup = lookups.get(signal.symbol, {}).get(ts)
            if lookup is None:
                continue
            index, bar = lookup
            notional = min(current_equity * profile.risk_per_trade / profile.stop_fraction, 2.0 * current_equity, capacity)
            price = bar.open * (1.0 + signal.side * slippage)
            qty = notional / price
            fee = notional * fee_rate
            cash -= fee
            positions[signal.symbol] = Position(
                signal.symbol,
                signal.side,
                qty,
                ts,
                index,
                price,
                fee,
                price,
                price * (1.0 - signal.side * profile.stop_fraction),
                signal.kind,
            )
            capacity -= notional
            daily_entries += 1

        for symbol, position in list(positions.items()):
            lookup = lookups.get(symbol, {}).get(ts)
            if lookup is None:
                continue
            _, bar = lookup
            last_prices[symbol] = bar.close
            if position.side > 0:
                position.extreme = max(position.extreme, bar.high)
                if position.extreme / position.entry_price - 1 >= profile.trail_activation:
                    position.stop = max(position.stop, position.extreme * (1.0 - profile.trail_fraction))
            else:
                position.extreme = min(position.extreme, bar.low)
                if 1.0 - position.extreme / position.entry_price >= profile.trail_activation:
                    position.stop = min(position.stop, position.extreme * (1.0 + profile.trail_fraction))
        curve.append(equity())

    for symbol in list(positions):
        close(symbol, end_ts, last_prices.get(symbol, positions[symbol].entry_price), "end")
    peak = initial_cash
    max_drawdown = 0.0
    for value in curve + [cash]:
        peak = max(peak, value)
        max_drawdown = max(max_drawdown, 1.0 - value / peak)
    wins = [item for item in trades if float(item["pnl"]) > 0]
    losses = [item for item in trades if float(item["pnl"]) < 0]
    return {
        "return_pct": (cash / initial_cash - 1.0) * 100,
        "final_equity": cash,
        "max_drawdown_pct": max_drawdown * 100,
        "trades": len(trades),
        "win_rate_pct": len(wins) / len(trades) * 100 if trades else 0.0,
        "profit_factor": sum(float(item["pnl"]) for item in wins) / max(-sum(float(item["pnl"]) for item in losses), 1e-12),
        "longs": sum(item["side"] == "long" for item in trades),
        "shorts": sum(item["side"] == "short" for item in trades),
        "early_oi_trades": sum(item["kind"] == "early_oi" for item in trades),
        "best": sorted(trades, key=lambda item: float(item["pnl"]), reverse=True)[:8],
        "worst": sorted(trades, key=lambda item: float(item["pnl"]))[:8],
    }


def event_study(bars: dict[str, list[Bar]], signals: list[Signal], profile: Profile) -> list[dict[str, object]]:
    """Evaluate every accepted signal independently with the profile's exit rules."""
    lookups = {symbol: {bar.ts: index for index, bar in enumerate(values)} for symbol, values in bars.items() if any(s.symbol == symbol for s in signals)}
    output: list[dict[str, object]] = []
    fee_rate = 0.0005
    slippage = 0.0005
    for signal in signals:
        index = lookups.get(signal.symbol, {}).get(signal.execute_ts)
        if index is None:
            continue
        values = bars[signal.symbol]
        entry = values[index].open * (1.0 + signal.side * slippage)
        stop = entry * (1.0 - signal.side * profile.stop_fraction)
        extreme = entry
        exit_price = values[min(index + profile.max_holding_bars, len(values) - 1)].open
        exit_reason = "time"
        exit_ts = values[min(index + profile.max_holding_bars, len(values) - 1)].ts
        for bar in values[index : min(index + profile.max_holding_bars + 1, len(values))]:
            gap = bar.open <= stop if signal.side > 0 else bar.open >= stop
            intrabar = bar.low <= stop if signal.side > 0 else bar.high >= stop
            if gap or intrabar:
                exit_price = bar.open if gap else stop
                exit_reason = "stop"
                exit_ts = bar.ts
                break
            if signal.side > 0:
                extreme = max(extreme, bar.high)
                if extreme / entry - 1 >= profile.trail_activation:
                    stop = max(stop, extreme * (1.0 - profile.trail_fraction))
            else:
                extreme = min(extreme, bar.low)
                if 1.0 - extreme / entry >= profile.trail_activation:
                    stop = min(stop, extreme * (1.0 + profile.trail_fraction))
        executed_exit = exit_price * (1.0 - signal.side * slippage)
        net_return = signal.side * (executed_exit / entry - 1.0) - fee_rate - fee_rate * executed_exit / entry
        row: dict[str, object] = {
            "symbol": signal.symbol,
            "execute_ts": signal.execute_ts,
            "month": datetime.fromtimestamp(signal.execute_ts / 1_000, timezone.utc).strftime("%Y-%m"),
            "side": signal.side,
            "kind": signal.kind,
            "net_return": net_return,
            "exit_reason": exit_reason,
            "exit_ts": exit_ts,
        }
        row.update(signal.features)
        output.append(row)
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-month", default="2026-05")
    parser.add_argument("--end-month", default="2026-07")
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-cache")
    parser.add_argument("--workers", type=int, default=48)
    parser.add_argument("--limit-symbols", type=int)
    parser.add_argument("--output", default="/tmp/greed-altcoin-oi-report.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    months = month_range(args.start_month, args.end_month)
    symbols = list_symbols(args.limit_symbols)
    print(f"universe={len(symbols)} months={months}", flush=True)

    jobs: list[tuple[str, str, str, Path]] = []
    for symbol in symbols:
        for month in months:
            path = cache / "klines" / symbol / f"{symbol}-15m-{month}.zip"
            url = f"{VISION}/monthly/klines/{symbol}/15m/{symbol}-15m-{month}.zip"
            jobs.append((symbol, month, url, path))
    completed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(download, url, path): (symbol, month) for symbol, month, url, path in jobs}
        for future in as_completed(futures):
            future.result()
            completed += 1
            if completed % 250 == 0:
                print(f"kline downloads {completed}/{len(jobs)}", flush=True)

    all_bars: dict[str, list[Bar]] = {}
    all_features: dict[str, dict[int, dict[str, float | bool | int]]] = {}
    for symbol in symbols:
        paths = [cache / "klines" / symbol / f"{symbol}-15m-{month}.zip" for month in months]
        values = load_bars([path for path in paths if path.exists()])
        if len(values) < 15 * 96:
            continue
        features = bar_features(values)
        if features:
            all_bars[symbol] = values
            all_features[symbol] = features
    print(f"bar_symbols={len(all_bars)} relaxed_event_bars={sum(map(len, all_features.values()))}", flush=True)

    metric_jobs: list[tuple[str, date, str, Path]] = []
    for symbol, features in all_features.items():
        for day in required_metric_dates(features):
            stamp = day.isoformat()
            path = cache / "metrics" / symbol / f"{symbol}-metrics-{stamp}.zip"
            url = f"{VISION}/daily/metrics/{symbol}/{symbol}-metrics-{stamp}.zip"
            metric_jobs.append((symbol, day, url, path))
    completed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(download, url, path): (symbol, day) for symbol, day, url, path in metric_jobs}
        for future in as_completed(futures):
            future.result()
            completed += 1
            if completed % 500 == 0:
                print(f"metric downloads {completed}/{len(metric_jobs)}", flush=True)

    funding_jobs: list[tuple[str, str, Path]] = []
    for symbol in all_bars:
        for month in months:
            path = cache / "funding" / symbol / f"{symbol}-fundingRate-{month}.zip"
            url = f"{VISION}/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{month}.zip"
            funding_jobs.append((url, str(path), path))
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(download, url, path) for url, _, path in funding_jobs]
        for future in as_completed(futures):
            future.result()

    profiles = [
        Profile("current-breakout", "baseline"),
        Profile("post-fixed-3m", "strict", min_return_1h=0.01, min_volume_ratio=1.5, min_oi_change_5m=0.03, min_oi_delta_usd=3_000_000, min_divergence=1.15, min_setup_count=1),
        Profile("normalised-oi", "normalised", min_return_1h=0.01, min_volume_ratio=1.5, min_oi_change_5m=0.01, min_oi_delta_usd=500_000, min_divergence=1.10, min_setup_count=1),
        Profile("hybrid-current-exit", "hybrid", min_return_1h=0.01, min_volume_ratio=1.5, min_oi_change_5m=0.01, min_oi_delta_usd=500_000, min_divergence=1.10, min_setup_count=1),
        Profile("hybrid-runner-exit", "hybrid", min_return_1h=0.01, min_volume_ratio=1.5, min_oi_change_5m=0.01, min_oi_delta_usd=500_000, min_divergence=1.10, min_setup_count=1, trail_activation=0.03, trail_fraction=0.015, max_holding_bars=96),
        Profile("validated-oi-r10", "normalised", min_return_1h=0.01, max_return_1h=0.20, min_volume_ratio=3.0, min_oi_change_5m=0.05, min_oi_delta_usd=1_000_000, min_divergence=1.50, min_setup_count=1, risk_per_trade=0.10),
        Profile("validated-oi-r3", "normalised", min_return_1h=0.01, max_return_1h=0.20, min_volume_ratio=3.0, min_oi_change_5m=0.05, min_oi_delta_usd=1_000_000, min_divergence=1.50, min_setup_count=1, risk_per_trade=0.03),
        Profile("oi-absorption-r10", "normalised", min_return_1h=0.01, max_return_1h=0.20, min_volume_ratio=5.0, min_oi_change_5m=0.01, min_oi_delta_usd=500_000, min_divergence=1.30, max_taker_lsr=1.20, min_setup_count=2, long_only=True, risk_per_trade=0.10),
        Profile("oi-absorption-r3", "normalised", min_return_1h=0.01, max_return_1h=0.20, min_volume_ratio=5.0, min_oi_change_5m=0.01, min_oi_delta_usd=500_000, min_divergence=1.30, max_taker_lsr=1.20, min_setup_count=2, long_only=True, risk_per_trade=0.03),
        Profile("oi-absorption-quality-r3", "normalised", min_return_1h=0.01, max_return_1h=0.20, min_volume_ratio=5.0, min_oi_change_5m=0.01, min_oi_delta_usd=500_000, min_divergence=1.30, max_taker_lsr=1.20, min_setup_count=2, long_only=True, score_mode="absorption", risk_per_trade=0.03, max_positions=2),
        Profile("oi-absorption-quality-one-r3", "normalised", min_return_1h=0.01, max_return_1h=0.20, min_volume_ratio=5.0, min_oi_change_5m=0.01, min_oi_delta_usd=500_000, min_divergence=1.30, max_taker_lsr=1.20, min_setup_count=2, long_only=True, score_mode="absorption", risk_per_trade=0.03, max_positions=1),
    ]
    signals_by_profile: dict[str, list[Signal]] = {profile.name: [] for profile in profiles}
    for symbol, features in all_features.items():
        metric_paths = sorted((cache / "metrics" / symbol).glob("*.zip"))
        funding_paths = sorted((cache / "funding" / symbol).glob("*.zip"))
        metrics = load_metrics(metric_paths)
        funding = load_funding(funding_paths)
        for profile in profiles:
            signals_by_profile[profile.name].extend(make_signals(symbol, features, metrics, funding, profile))

    periods = {
        "may_late": ("2026-05-15", "2026-06-01"),
        "june": ("2026-06-01", "2026-07-01"),
        "july": ("2026-07-01", "2026-08-01"),
        "full": ("2026-05-15", "2026-08-01"),
    }
    results: dict[str, object] = {}
    for profile in profiles:
        profile_results: dict[str, object] = {}
        for period, (start, end) in periods.items():
            start_ts = int(datetime.fromisoformat(start).replace(tzinfo=timezone.utc).timestamp() * 1_000)
            end_ts = int(datetime.fromisoformat(end).replace(tzinfo=timezone.utc).timestamp() * 1_000)
            profile_results[period] = simulate(all_bars, signals_by_profile[profile.name], profile, start_ts, end_ts)
        results[profile.name] = {
            "signals": len(signals_by_profile[profile.name]),
            "periods": profile_results,
        }
        print(profile.name, json.dumps(profile_results["full"], ensure_ascii=False)[:500], flush=True)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "months": months,
        "universe_source": "all Binance Vision USD-M metrics symbol directories; majors excluded",
        "bar_symbols": len(all_bars),
        "profiles": {profile.name: profile.__dict__ for profile in profiles},
        "results": results,
        "event_study": {
            profile.name: event_study(all_bars, signals_by_profile[profile.name], profile)
            for profile in profiles
            if profile.name in {"post-fixed-3m", "normalised-oi", "validated-oi-r10", "validated-oi-r3", "oi-absorption-r10", "oi-absorption-r3"}
        },
        "assumptions": {
            "signal_execution": "15m close signal, next 15m open execution",
            "fee_each_side_bps": 5,
            "slippage_each_side_bps": 5,
            "initial_equity": 1000,
            "risk_per_trade": 0.10,
            "max_positions": 2,
            "max_gross_equity": 4,
            "daily_entry_limit": 6,
            "daily_loss_gate": 0.04,
            "intrabar_ordering": "stop checked before favourable extreme; conservative",
            "smart_money_limitation": "public top-trader aggregates proxy proprietary MM cluster PnL",
        },
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
