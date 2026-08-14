#!/usr/bin/env python3
"""Causal ablation test for five altcoin impulse improvements.

The experiment uses Binance Vision 15 minute futures klines, 5 minute futures
metrics and realised funding.  Every signal is formed after a closed bar and is
filled at the following bar open.  The five changes are tested independently
before testing their combination:

1. cross-sectional market regime;
2. breakout retest/reclaim;
3. OI/positioning quality;
4. observable liquidity proxy;
5. ATR stop, partial take profit and ATR trail.

Historical order-book snapshots are not available in this dataset.  The
liquidity experiment therefore uses actual quote turnover and a slippage stress
test; it must not be described as a depth replay.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import re
import statistics
import urllib.parse
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree

from exp_altcoin_oi_launch import (
    BAR_MS,
    DAY_MS,
    MAJORS,
    VISION,
    Bar,
    Metric,
    Signal,
    download,
    fetch,
    latest,
    list_symbols,
    load_bars,
    load_funding,
    load_metrics,
    month_range,
    rolling_prior_extreme,
)


@dataclass(frozen=True)
class Regime:
    breadth: float
    median_return_1h: float
    dispersion_iqr: float
    dispersion_p90_14d: float
    sample_size: int


@dataclass
class Position:
    symbol: str
    side: int
    qty: float
    original_qty: float
    entry_ts: int
    entry_index: int
    entry_price: float
    entry_fee: float
    extreme: float
    stop: float
    stop_distance: float
    atr: float
    partial_taken: bool
    partial_gross_after_fee: float
    kind: str


def dates_between(start: str, end: str) -> list[date]:
    current = date.fromisoformat(start)
    finish = date.fromisoformat(end)
    output: list[date] = []
    while current <= finish:
        output.append(current)
        current += timedelta(days=1)
    return output


def spot_history_symbols() -> set[str]:
    prefix = "data/spot/daily/klines/"
    base = (
        "https://data.binance.vision.s3.amazonaws.com/"
        f"?list-type=2&delimiter=%2F&prefix={urllib.parse.quote(prefix, safe='')}"
    )
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    output: set[str] = set()
    token: str | None = None
    while True:
        url = base + (f"&continuation-token={urllib.parse.quote(token, safe='')}" if token else "")
        root = ElementTree.fromstring(fetch(url))
        for node in root.findall("s3:CommonPrefixes/s3:Prefix", namespace):
            match = re.search(r"/([^/]+)/$", node.text or "")
            if match and re.fullmatch(r"[A-Z0-9]+USDT", match.group(1)):
                output.add(match.group(1))
        truncated = root.findtext("s3:IsTruncated", default="false", namespaces=namespace) == "true"
        token = root.findtext("s3:NextContinuationToken", default="", namespaces=namespace)
        if not truncated or not token:
            break
    return output


def download_daily_klines(cache: Path, symbols: list[str], days: list[date], workers: int) -> None:
    jobs: list[tuple[str, Path]] = []
    for symbol in symbols:
        for day in days:
            stamp = day.isoformat()
            path = cache / "klines-daily" / symbol / f"{symbol}-15m-{stamp}.zip"
            url = f"{VISION}/daily/klines/{symbol}/15m/{symbol}-15m-{stamp}.zip"
            jobs.append((url, path))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(download, url, path) for url, path in jobs]
        for completed, future in enumerate(as_completed(futures), 1):
            future.result()
            if completed % 1000 == 0:
                print(f"daily klines {completed}/{len(jobs)}", flush=True)


def download_monthly_history(
    cache: Path,
    symbols: list[str],
    start_month: str,
    end_month: str,
    workers: int,
) -> None:
    jobs: list[tuple[str, Path]] = []
    for symbol in symbols:
        for month in month_range(start_month, end_month):
            kline_path = cache / "klines" / symbol / f"{symbol}-15m-{month}.zip"
            kline_url = f"{VISION}/monthly/klines/{symbol}/15m/{symbol}-15m-{month}.zip"
            funding_path = cache / "funding" / symbol / f"{symbol}-fundingRate-{month}.zip"
            funding_url = f"{VISION}/monthly/fundingRate/{symbol}/{symbol}-fundingRate-{month}.zip"
            jobs.extend([(kline_url, kline_path), (funding_url, funding_path)])
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(download, url, path) for url, path in jobs]
        for completed, future in enumerate(as_completed(futures), 1):
            future.result()
            if completed % 1000 == 0:
                print(f"monthly klines/funding {completed}/{len(jobs)}", flush=True)


def load_symbol_bars(cache: Path, symbol: str) -> list[Bar]:
    monthly = sorted((cache / "klines" / symbol).glob("*.zip"))
    daily = sorted((cache / "klines-daily" / symbol).glob("*.zip"))
    return load_bars(monthly + daily)


def true_range(bars: list[Bar], index: int) -> float:
    previous = bars[index - 1].close
    return max(
        bars[index].high - bars[index].low,
        abs(bars[index].high - previous),
        abs(bars[index].low - previous),
    )


def make_breakout_signals(symbol: str, bars: list[Bar]) -> list[Signal]:
    if len(bars) < 14 * 96 + 20:
        return []
    highs = [bar.high for bar in bars]
    lows = [bar.low for bar in bars]
    prior_high = rolling_prior_extreme(highs, 96, True)
    prior_low = rolling_prior_extreme(lows, 96, False)
    volume_prefix = [0.0]
    for bar in bars:
        volume_prefix.append(volume_prefix[-1] + bar.quote_volume)
    hourly = [
        volume_prefix[index + 1] - volume_prefix[max(0, index - 3)]
        for index in range(len(bars))
    ]
    tr_values = [math.nan] + [true_range(bars, index) for index in range(1, len(bars))]
    output: list[Signal] = []
    for index in range(14 * 96, len(bars) - 1):
        if bars[index].ts - bars[index - 96].ts > 97 * BAR_MS:
            continue
        close = bars[index].close
        side = 1 if close > prior_high[index] else -1 if close < prior_low[index] else 0
        if side == 0:
            continue
        return_1h = close / bars[index - 4].close - 1.0
        return_4h = close / bars[index - 16].close - 1.0
        directional_1h = side * return_1h
        directional_4h = side * return_4h
        if not (0.04 <= directional_1h <= 0.45 and 0.06 <= directional_4h <= 1.20):
            continue
        volume_24h = volume_prefix[index + 1] - volume_prefix[index - 95]
        if volume_24h < 5_000_000.0:
            continue
        median_hourly = statistics.median(hourly[index - 7 * 96 : index])
        volume_ratio = hourly[index] / max(median_hourly, 1.0)
        if volume_ratio < 3.0:
            continue
        path = [item.close for item in bars[index - 4 : index + 1]]
        traveled = sum(abs(right - left) for left, right in zip(path, path[1:]))
        efficiency = abs(path[-1] - path[0]) / max(traveled, 1e-12)
        if efficiency < 0.35:
            continue
        spread = bars[index].high - bars[index].low
        close_location = (close - bars[index].low) / spread if spread > 0 else 0.5
        if (side > 0 and close_location < 0.60) or (side < 0 and close_location > 0.40):
            continue
        atr = statistics.mean(tr_values[index - 13 : index + 1])
        breakout_level = prior_high[index] if side > 0 else prior_low[index]
        candle_range = spread / max(close, 1e-12)
        return_15m = close / bars[index - 1].close - 1.0
        previous_45m_return = bars[index - 1].close / bars[index - 4].close - 1.0
        previous_1h_return = bars[index - 4].close / bars[index - 8].close - 1.0
        volume_concentration = bars[index].quote_volume / max(hourly[index], 1.0)
        directional_wick = (
            (bars[index].high - close) / max(spread, 1e-12)
            if side > 0
            else (close - bars[index].low) / max(spread, 1e-12)
        )
        body_fraction = abs(close - bars[index].open) / max(spread, 1e-12)
        breakout_atr = side * (close - breakout_level) / max(atr, 1e-12)
        features: dict[str, float | int | bool] = {
            "signal_ts": bars[index].ts + BAR_MS - 1,
            "signal_index": index,
            "breakout_level": breakout_level,
            "return_1h": return_1h,
            "return_4h": return_4h,
            "volume_ratio": volume_ratio,
            "volume_24h": volume_24h,
            "hour_volume": hourly[index],
            "efficiency": efficiency,
            "close_location": close_location,
            "atr": atr,
            "atr_fraction": atr / max(close, 1e-12),
            "candle_range": candle_range,
            "return_15m": return_15m,
            "previous_45m_return": previous_45m_return,
            "previous_1h_return": previous_1h_return,
            "late_move_share": side * return_15m / max(directional_1h, 1e-12),
            "volume_concentration": volume_concentration,
            "directional_wick": directional_wick,
            "body_fraction": body_fraction,
            "breakout_atr": breakout_atr,
        }
        score = directional_1h * math.log1p(volume_ratio) * math.log1p(volume_24h / 1_000_000.0)
        output.append(Signal(symbol, bars[index + 1].ts, side, score, "breakout", features))
    return output


def make_retest_signals(base: list[Signal], bars_by_symbol: dict[str, list[Bar]]) -> list[Signal]:
    output: dict[tuple[str, int, int], Signal] = {}
    for signal in base:
        bars = bars_by_symbol[signal.symbol]
        event_index = int(signal.features["signal_index"])
        level = float(signal.features["breakout_level"])
        for index in range(event_index + 1, min(event_index + 5, len(bars) - 1)):
            bar = bars[index]
            spread = bar.high - bar.low
            close_location = (bar.close - bar.low) / spread if spread > 0 else 0.5
            if signal.side > 0:
                invalid = bar.close < level * 0.985
                touched = bar.low <= level * 1.005
                reclaimed = bar.close >= level and close_location >= 0.55
            else:
                invalid = bar.close > level * 1.015
                touched = bar.high >= level * 0.995
                reclaimed = bar.close <= level and close_location <= 0.45
            if invalid:
                break
            if not (touched and reclaimed):
                continue
            enriched = dict(signal.features)
            enriched.update(
                {
                    "impulse_signal_ts": signal.features["signal_ts"],
                    "signal_ts": bar.ts + BAR_MS - 1,
                    "confirmation_index": index,
                    "retest_delay_bars": index - event_index,
                    "confirmation_close_location": close_location,
                }
            )
            candidate = Signal(
                signal.symbol,
                bars[index + 1].ts,
                signal.side,
                signal.score / (1.0 + 0.1 * (index - event_index)),
                "retest_reclaim",
                enriched,
            )
            output[(candidate.symbol, candidate.execute_ts, candidate.side)] = candidate
            break
    return list(output.values())


def compute_regimes(bars_by_symbol: dict[str, list[Bar]]) -> dict[int, Regime]:
    buckets: dict[int, list[float]] = defaultdict(list)
    for bars in bars_by_symbol.values():
        prefix = [0.0]
        for bar in bars:
            prefix.append(prefix[-1] + bar.quote_volume)
        for index in range(96, len(bars) - 1):
            if bars[index].ts - bars[index - 96].ts > 97 * BAR_MS:
                continue
            volume_24h = prefix[index + 1] - prefix[index - 95]
            if volume_24h < 5_000_000.0:
                continue
            buckets[bars[index + 1].ts].append(bars[index].close / bars[index - 4].close - 1.0)
    raw: dict[int, tuple[float, float, float, int]] = {}
    for ts, values in buckets.items():
        if len(values) < 30:
            continue
        ordered = sorted(values)
        lower = ordered[len(ordered) // 4]
        upper = ordered[(3 * len(ordered)) // 4]
        raw[ts] = (
            sum(value > 0 for value in values) / len(values),
            statistics.median(values),
            upper - lower,
            len(values),
        )
    output: dict[int, Regime] = {}
    history: deque[tuple[int, float]] = deque()
    for ts in sorted(raw):
        while history and history[0][0] < ts - 14 * DAY_MS:
            history.popleft()
        historical = sorted(value for _, value in history)
        p90 = historical[min(len(historical) - 1, int(0.90 * len(historical)))] if historical else math.inf
        breadth, median_return, dispersion, sample_size = raw[ts]
        output[ts] = Regime(breadth, median_return, dispersion, p90, sample_size)
        history.append((ts, dispersion))
    return output


def apply_regime(signals: list[Signal], regimes: dict[int, Regime]) -> list[Signal]:
    output: list[Signal] = []
    for signal in signals:
        regime = regimes.get(signal.execute_ts)
        if regime is None:
            continue
        directional = (
            regime.breadth >= 0.55 and regime.median_return_1h > 0
            if signal.side > 0
            else regime.breadth <= 0.45 and regime.median_return_1h < 0
        )
        if not directional or regime.dispersion_iqr > regime.dispersion_p90_14d:
            continue
        enriched = dict(signal.features)
        enriched.update(
            {
                "market_breadth": regime.breadth,
                "market_median_return_1h": regime.median_return_1h,
                "market_dispersion_iqr": regime.dispersion_iqr,
                "market_dispersion_p90_14d": regime.dispersion_p90_14d,
            }
        )
        output.append(Signal(signal.symbol, signal.execute_ts, signal.side, signal.score, signal.kind, enriched))
    return output


def metric_at(metrics: list[Metric], times: list[int], ts: int) -> Metric | None:
    index = bisect.bisect_right(times, ts) - 1
    return metrics[index] if index >= 0 else None


def apply_oi_quality(
    signals: list[Signal],
    metrics_by_symbol: dict[str, list[Metric]],
    funding_by_symbol: dict[str, tuple[list[int], list[float]]],
) -> list[Signal]:
    output: list[Signal] = []
    metric_times = {symbol: [item.ts for item in values] for symbol, values in metrics_by_symbol.items()}
    for signal in signals:
        metrics = metrics_by_symbol.get(signal.symbol, [])
        times = metric_times.get(signal.symbol, [])
        signal_ts = int(signal.features["signal_ts"])
        current = metric_at(metrics, times, signal_ts)
        previous = metric_at(metrics, times, signal_ts - 5 * 60 * 1_000)
        if current is None or previous is None or previous.oi_value <= 0:
            continue
        oi_change = current.oi_value / previous.oi_value - 1.0
        oi_delta = current.oi_value - previous.oi_value
        divergence = current.top_position_lsr / max(current.global_lsr, 1e-9)
        funding_times, funding_values = funding_by_symbol.get(signal.symbol, ([], []))
        funding_rate = latest(funding_times, funding_values, signal_ts)
        directional_structure = (
            divergence >= 1.10 and current.taker_lsr >= 1.0 and funding_rate <= 0.001
            if signal.side > 0
            else divergence <= 1.0 / 1.10 and current.taker_lsr <= 1.0 and funding_rate >= -0.001
        )
        if oi_change < 0.01 or oi_delta < 500_000.0 or not directional_structure:
            continue
        enriched = dict(signal.features)
        enriched.update(
            {
                "oi_change_5m": oi_change,
                "oi_delta_usd": oi_delta,
                "top_position_lsr": current.top_position_lsr,
                "global_lsr": current.global_lsr,
                "position_divergence": divergence,
                "taker_lsr": current.taker_lsr,
                "funding_rate": funding_rate,
            }
        )
        score = signal.score * math.log1p(oi_delta / 100_000.0) * max(
            divergence if signal.side > 0 else 1.0 / max(divergence, 1e-9), 1.0
        )
        output.append(Signal(signal.symbol, signal.execute_ts, signal.side, score, signal.kind, enriched))
    return output


def apply_liquidity(signals: list[Signal]) -> list[Signal]:
    return [
        signal
        for signal in signals
        if float(signal.features["volume_24h"]) >= 20_000_000.0
        and float(signal.features["hour_volume"]) >= 1_000_000.0
        and float(signal.features["candle_range"]) <= 0.15
    ]


def simulate(
    bars_by_symbol: dict[str, list[Bar]],
    signals: list[Signal],
    start_ts: int,
    end_ts: int,
    *,
    exit_mode: str,
    risk_per_trade: float = 0.03,
    slippage_bps: float = 5.0,
) -> dict[str, object]:
    selected = {signal.symbol for signal in signals if start_ts <= signal.execute_ts < end_ts}
    lookups = {
        symbol: {bar.ts: (index, bar) for index, bar in enumerate(bars_by_symbol[symbol])}
        for symbol in selected
    }
    signal_map: dict[int, list[Signal]] = defaultdict(list)
    for signal in signals:
        if start_ts <= signal.execute_ts < end_ts:
            signal_map[signal.execute_ts].append(signal)
    fee_rate = 0.0005
    slippage = slippage_bps / 10_000.0
    cash = 1_000.0
    positions: dict[str, Position] = {}
    last_prices: dict[str, float] = {}
    last_exit: dict[str, int] = {}
    trades: list[dict[str, object]] = []
    curve: list[float] = []
    current_day: int | None = None
    day_start_equity = cash
    daily_entries = 0

    def equity() -> float:
        return cash + sum(
            position.side * position.qty * (last_prices.get(symbol, position.entry_price) - position.entry_price)
            for symbol, position in positions.items()
        )

    def close_quantity(position: Position, raw_price: float, quantity: float) -> tuple[float, float]:
        nonlocal cash
        price = raw_price * (1.0 - position.side * slippage)
        gross = position.side * quantity * (price - position.entry_price)
        fee = quantity * price * fee_rate
        cash += gross - fee
        return gross - fee, price

    def close_position(symbol: str, ts: int, raw_price: float, reason: str) -> None:
        position = positions.pop(symbol)
        final_net, price = close_quantity(position, raw_price, position.qty)
        net = position.partial_gross_after_fee + final_net - position.entry_fee
        trades.append(
            {
                "symbol": symbol,
                "side": "long" if position.side > 0 else "short",
                "entry_ts": position.entry_ts,
                "exit_ts": ts,
                "pnl": net,
                "return_on_notional": net / (position.original_qty * position.entry_price),
                "reason": reason,
                "partial_taken": position.partial_taken,
                "kind": position.kind,
                "exit_price": price,
            }
        )
        last_exit[symbol] = ts

    timestamps = range((start_ts // BAR_MS) * BAR_MS, end_ts, BAR_MS)
    for ts in timestamps:
        for symbol in list(positions):
            lookup = lookups.get(symbol, {}).get(ts)
            if lookup is None:
                continue
            index, bar = lookup
            position = positions[symbol]
            last_prices[symbol] = bar.open
            gap = bar.open <= position.stop if position.side > 0 else bar.open >= position.stop
            stopped = bar.low <= position.stop if position.side > 0 else bar.high >= position.stop
            timed = index - position.entry_index >= 24
            if gap or stopped or timed:
                close_position(symbol, ts, bar.open if gap or timed else position.stop, "stop" if gap or stopped else "time")
                continue
            if exit_mode == "atr_partial" and not position.partial_taken:
                target = position.entry_price + position.side * position.stop_distance
                target_hit = bar.high >= target if position.side > 0 else bar.low <= target
                if target_hit:
                    partial_qty = position.qty * 0.5
                    partial_net, _ = close_quantity(position, target, partial_qty)
                    position.partial_gross_after_fee += partial_net
                    position.qty -= partial_qty
                    position.partial_taken = True
                    position.stop = position.entry_price * (1.0 + position.side * 0.0011)

        current_equity = equity()
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start_equity = current_equity
            daily_entries = 0
        gross = sum(position.qty * last_prices.get(symbol, position.entry_price) for symbol, position in positions.items())
        capacity = max(0.0, 4.0 * current_equity - gross)
        entries_allowed = daily_entries < 6 and current_equity >= day_start_equity * 0.96
        for signal in sorted(signal_map.get(ts, []), key=lambda item: item.score, reverse=True):
            if (
                not entries_allowed
                or daily_entries >= 6
                or len(positions) >= 2
                or capacity < 50.0
                or signal.symbol in positions
            ):
                continue
            if ts - last_exit.get(signal.symbol, -10**18) < 8 * 60 * 60 * 1_000:
                continue
            lookup = lookups.get(signal.symbol, {}).get(ts)
            if lookup is None:
                continue
            index, bar = lookup
            atr = float(signal.features.get("atr", bar.open * 0.03))
            stop_fraction = 0.05 if exit_mode == "fixed" else min(max(1.5 * atr / bar.open, 0.02), 0.08)
            notional = min(current_equity * risk_per_trade / stop_fraction, 2.0 * current_equity, capacity)
            entry = bar.open * (1.0 + signal.side * slippage)
            qty = notional / entry
            entry_fee = notional * fee_rate
            cash -= entry_fee
            stop_distance = entry * stop_fraction
            positions[signal.symbol] = Position(
                signal.symbol,
                signal.side,
                qty,
                qty,
                ts,
                index,
                entry,
                entry_fee,
                entry,
                entry - signal.side * stop_distance,
                stop_distance,
                atr,
                False,
                0.0,
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
                if exit_mode == "fixed" and position.extreme / position.entry_price - 1.0 >= 0.02:
                    position.stop = max(position.stop, position.extreme * 0.99)
                elif exit_mode == "atr_partial" and position.partial_taken:
                    position.stop = max(position.stop, position.extreme - position.atr)
            else:
                position.extreme = min(position.extreme, bar.low)
                if exit_mode == "fixed" and 1.0 - position.extreme / position.entry_price >= 0.02:
                    position.stop = min(position.stop, position.extreme * 1.01)
                elif exit_mode == "atr_partial" and position.partial_taken:
                    position.stop = min(position.stop, position.extreme + position.atr)
        curve.append(equity())

    for symbol in list(positions):
        close_position(symbol, end_ts, last_prices.get(symbol, positions[symbol].entry_price), "end")
    peak = 1_000.0
    drawdown = 0.0
    for value in curve + [cash]:
        peak = max(peak, value)
        drawdown = max(drawdown, 1.0 - value / peak)
    wins = [trade for trade in trades if float(trade["pnl"]) > 0]
    losses = [trade for trade in trades if float(trade["pnl"]) < 0]
    gross_profit = sum(float(trade["pnl"]) for trade in wins)
    gross_loss = -sum(float(trade["pnl"]) for trade in losses)
    ordered_wins = sorted((float(trade["pnl"]) for trade in wins), reverse=True)
    return {
        "return_pct": (cash / 1_000.0 - 1.0) * 100.0,
        "final_equity": cash,
        "max_drawdown_pct": drawdown * 100.0,
        "trades": len(trades),
        "win_rate_pct": len(wins) / len(trades) * 100.0 if trades else 0.0,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else None,
        "expectancy_usd": sum(float(trade["pnl"]) for trade in trades) / len(trades) if trades else 0.0,
        "top_two_winners_share_of_gross_profit": sum(ordered_wins[:2]) / gross_profit if gross_profit > 0 else 0.0,
        "longs": sum(trade["side"] == "long" for trade in trades),
        "shorts": sum(trade["side"] == "short" for trade in trades),
        "partial_take_profits": sum(bool(trade["partial_taken"]) for trade in trades),
        "time_exits": sum(trade["reason"] == "time" for trade in trades),
        "best": sorted(trades, key=lambda item: float(item["pnl"]), reverse=True)[:5],
        "worst": sorted(trades, key=lambda item: float(item["pnl"]))[:5],
    }


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--workers", type=int, default=48)
    parser.add_argument("--augment-start", default="2026-08-01")
    parser.add_argument("--augment-end", default="2026-08-10")
    parser.add_argument("--history-start-month", default="2026-01")
    parser.add_argument("--history-end-month", default="2026-04")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--output", default="/tmp/greed-altcoin-five-optimizations.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted({path.parent.name for path in (cache / "klines").glob("*/*.zip")})
    if not symbols:
        symbols = list_symbols()
    spot_symbols = spot_history_symbols()
    symbols = [symbol for symbol in symbols if symbol in spot_symbols and symbol not in MAJORS]
    days = dates_between(args.augment_start, args.augment_end)
    if not args.skip_download:
        download_monthly_history(
            cache,
            symbols,
            args.history_start_month,
            args.history_end_month,
            args.workers,
        )
        download_daily_klines(cache, symbols, days, args.workers)

    bars_by_symbol: dict[str, list[Bar]] = {}
    base: list[Signal] = []
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        if len(bars) < 15 * 96:
            continue
        bars_by_symbol[symbol] = bars
        base.extend(make_breakout_signals(symbol, bars))
        if completed % 100 == 0:
            print(f"features {completed}/{len(symbols)} signals={len(base)}", flush=True)
    print(f"bar_symbols={len(bars_by_symbol)} base_signals={len(base)}", flush=True)

    retest = make_retest_signals(base, bars_by_symbol)
    regimes = compute_regimes(bars_by_symbol)

    candidate_dates: dict[str, set[date]] = defaultdict(set)
    for signal in base + retest:
        signal_date = datetime.fromtimestamp(int(signal.features["signal_ts"]) / 1_000, timezone.utc).date()
        candidate_dates[signal.symbol].add(signal_date)
    if not args.skip_download:
        jobs: list[tuple[str, Path]] = []
        for symbol, symbol_days in candidate_dates.items():
            for day in symbol_days:
                stamp = day.isoformat()
                metric_path = cache / "metrics" / symbol / f"{symbol}-metrics-{stamp}.zip"
                metric_url = f"{VISION}/daily/metrics/{symbol}/{symbol}-metrics-{stamp}.zip"
                funding_path = cache / "funding" / symbol / f"{symbol}-fundingRate-{stamp}.zip"
                funding_url = f"{VISION}/daily/fundingRate/{symbol}/{symbol}-fundingRate-{stamp}.zip"
                jobs.extend([(metric_url, metric_path), (funding_url, funding_path)])
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(download, url, path) for url, path in jobs]
            for completed, future in enumerate(as_completed(futures), 1):
                future.result()
                if completed % 1000 == 0:
                    print(f"metrics/funding {completed}/{len(jobs)}", flush=True)

    metrics_by_symbol: dict[str, list[Metric]] = {}
    funding_by_symbol: dict[str, tuple[list[int], list[float]]] = {}
    for symbol in bars_by_symbol:
        metrics_by_symbol[symbol] = load_metrics(sorted((cache / "metrics" / symbol).glob("*.zip")))
        funding_by_symbol[symbol] = load_funding(sorted((cache / "funding" / symbol).glob("*.zip")))

    regime_base = apply_regime(base, regimes)
    oi_base = apply_oi_quality(base, metrics_by_symbol, funding_by_symbol)
    liquidity_base = apply_liquidity(base)
    regime_retest = apply_regime(retest, regimes)
    variants: dict[str, tuple[list[Signal], str]] = {
        "A_baseline": (base, "fixed"),
        "B_market_regime": (regime_base, "fixed"),
        "C_retest_reclaim": (retest, "fixed"),
        "D_oi_quality": (oi_base, "fixed"),
        "E_liquidity_proxy": (liquidity_base, "fixed"),
        "F_atr_partial_exit": (base, "atr_partial"),
    }
    combined_signals = apply_liquidity(
        apply_oi_quality(regime_retest, metrics_by_symbol, funding_by_symbol)
    )
    variants["G_all_combined"] = (combined_signals, "atr_partial")
    variants["H_all_filters_fixed_exit"] = (combined_signals, "fixed")
    variants["I_regime_oi_fixed"] = (
        apply_oi_quality(regime_base, metrics_by_symbol, funding_by_symbol),
        "fixed",
    )
    variants["J_retest_oi_fixed"] = (
        apply_oi_quality(retest, metrics_by_symbol, funding_by_symbol),
        "fixed",
    )
    variants["K_oi_liquidity_fixed"] = (apply_liquidity(oi_base), "fixed")
    variants["L_regime_retest_fixed"] = (regime_retest, "fixed")
    retest_oi = apply_oi_quality(retest, metrics_by_symbol, funding_by_symbol)
    variants["M_retest_oi_atr"] = (retest_oi, "atr_partial")
    variants["N_retest_oi_long_fixed"] = (
        [signal for signal in retest_oi if signal.side > 0],
        "fixed",
    )
    variants["O_retest_oi_regime_fixed"] = (apply_regime(retest_oi, regimes), "fixed")
    variants["P_retest_oi_liquidity_fixed"] = (apply_liquidity(retest_oi), "fixed")

    periods = {
        "jan_late": ("2026-01-15", "2026-02-01"),
        "february": ("2026-02-01", "2026-03-01"),
        "march": ("2026-03-01", "2026-04-01"),
        "april": ("2026-04-01", "2026-05-01"),
        "may_late": ("2026-05-15", "2026-06-01"),
        "june": ("2026-06-01", "2026-07-01"),
        "july": ("2026-07-01", "2026-08-01"),
        "aug_holdout": ("2026-08-01", "2026-08-11"),
        "full": ("2026-01-15", "2026-08-11"),
    }
    results: dict[str, object] = {}
    for name, (signals, exit_mode) in variants.items():
        period_results = {
            period: simulate(
                bars_by_symbol,
                signals,
                timestamp(start),
                timestamp(end),
                exit_mode=exit_mode,
            )
            for period, (start, end) in periods.items()
        }
        stress = simulate(
            bars_by_symbol,
            signals,
            timestamp(periods["full"][0]),
            timestamp(periods["full"][1]),
            exit_mode=exit_mode,
            slippage_bps=15.0,
        )
        results[name] = {
            "signal_count": len(signals),
            "signal_count_by_period": {
                period: sum(timestamp(start) <= signal.execute_ts < timestamp(end) for signal in signals)
                for period, (start, end) in periods.items()
            },
            "exit_mode": exit_mode,
            "periods": period_results,
            "full_slippage_15bps": stress,
        }
        print(name, json.dumps(period_results["full"], ensure_ascii=False)[:500], flush=True)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": {
            "source": "Binance Vision USD-M 15m klines, 5m metrics, realised funding",
            "universe": "USD-M symbols with a Binance Vision spot USDT history directory; removes non-crypto TradFi perps but does not reconstruct exact intramonth spot-listing status",
            "symbols_with_signals": len(bars_by_symbol),
            "range": ["2026-01-01", "2026-08-10"],
            "holdout": ["2026-08-01", "2026-08-10"],
        },
        "assumptions": {
            "causal_execution": "closed 15m signal/confirmation; next 15m open fill",
            "fee_each_side_bps": 5,
            "base_slippage_each_side_bps": 5,
            "stress_slippage_each_side_bps": 15,
            "risk_per_trade_pct": 3,
            "max_positions": 2,
            "max_gross_equity": 4,
            "daily_entries": 6,
            "daily_loss_gate_pct": 4,
            "liquidity_limitation": "quote-turnover proxy; no historical L2 depth replay",
            "intrabar_ordering": "stop before target/trailing update (conservative)",
        },
        "variant_definitions": {
            "A_baseline": "current 24h breakout and price/volume thresholds; fixed 5% stop, +2%/1% trail",
            "B_market_regime": "A plus directional breadth/median and trailing 14d dispersion p90 veto",
            "C_retest_reclaim": "A event, then 1-4 bars to touch and reclaim breakout level",
            "D_oi_quality": "A plus OI +1%/+500k, directional top-position divergence/taker flow, funding veto",
            "E_liquidity_proxy": "A plus 24h $20m, 1h $1m and signal candle range <=15%",
            "F_atr_partial_exit": "A signals; 1.5 ATR stop (2%-8%), half at 1R, breakeven then 1 ATR trail",
            "G_all_combined": "B+C+D+E signals with F exit",
            "H_all_filters_fixed_exit": "B+C+D+E signals with current fixed exit",
            "I_regime_oi_fixed": "B+D signals with current fixed exit",
            "J_retest_oi_fixed": "C+D signals with current fixed exit",
            "K_oi_liquidity_fixed": "D+E signals with current fixed exit",
            "L_regime_retest_fixed": "B+C signals with current fixed exit",
            "M_retest_oi_atr": "C+D signals with ATR/partial exit",
            "N_retest_oi_long_fixed": "C+D long-only signals with current fixed exit",
            "O_retest_oi_regime_fixed": "B+C+D signals with current fixed exit",
            "P_retest_oi_liquidity_fixed": "C+D+E signals with current fixed exit",
        },
        "results": results,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}", flush=True)


if __name__ == "__main__":
    main()
