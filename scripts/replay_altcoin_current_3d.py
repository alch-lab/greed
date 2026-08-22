#!/usr/bin/env python3
"""Causal three-day replay of the current altcoin impulse strategy.

Historical Binance klines can reproduce closed-bar signals and price-path
management.  They cannot reproduce the execution endpoint's historical L2
book or the demo exchange's symbol availability, so those gates are reported
as an explicit limitation rather than silently approximated.
"""

from __future__ import annotations

import argparse
import copy
import json
import math
import gzip
import pickle
import statistics
import time
import tomllib
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


FAPI = "https://fapi.binance.com"
SPOT_DATA = "https://data-api.binance.vision"
MINUTE_MS = 60_000
BAR_MS = 15 * MINUTE_MS
DAY_MS = 86_400_000
BEIJING_OFFSET_MS = 8 * 60 * 60 * 1000


def risk_day(ts_ms: int) -> int:
    return (ts_ms + BEIJING_OFFSET_MS) // DAY_MS


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float


@dataclass
class Signal:
    symbol: str
    signal_ms: int
    side: int
    price: float
    score: float
    return_1h: float
    return_4h: float
    volume_ratio: float
    phase: str
    breakout: float
    trigger: str = "direct_breakout"
    risk_scale: float = 1.0


@dataclass
class Pending:
    signal: Signal
    expires_ms: int
    last_checked_ms: int
    retest_seen: bool = False


@dataclass
class Position:
    signal: Signal
    entry_ms: int
    entry: float
    qty: float
    entry_fee: float
    initial_notional: float
    entry_notional: float
    stop: float
    extreme: float
    adverse: float
    mark: float
    partial_pnl: float = 0.0
    profit_trimmed: bool = False
    loss_trimmed: bool = False
    protection: str = "initial_stop"


def get_json(path: str, params: dict[str, object] | None = None) -> object:
    return get_url_json(FAPI, path, params)


def get_url_json(base: str, path: str, params: dict[str, object] | None = None) -> object:
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    request = urllib.request.Request(
        f"{base}{path}{query}", headers={"User-Agent": "greed-research/1.0"}
    )
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                return json.load(response)
        except Exception as error:
            last_error = error
            # Binance applies request-weight throttles to bulk historical
            # klines. Back off much more aggressively on 429 so a validation
            # run does not fail after downloading most of the universe.
            if isinstance(error, urllib.error.HTTPError) and error.code == 429:
                retry_after = error.headers.get("Retry-After")
                time.sleep(float(retry_after) if retry_after else 5.0 * (attempt + 1))
            else:
                time.sleep(0.4 * (2**attempt))
    assert last_error is not None
    raise last_error


def parse_rows(rows: object, end_ms: int) -> list[Bar]:
    return [
        Bar(int(row[0]), *(float(row[i]) for i in range(1, 5)), float(row[7]))
        for row in rows
        if int(row[6]) < end_ms
    ]


def fetch_15m(symbol: str, fetch_start: int, end_ms: int) -> tuple[str, list[Bar]]:
    output: list[Bar] = []
    cursor = fetch_start
    while cursor < end_ms:
        rows = get_json(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": "15m", "startTime": cursor, "endTime": end_ms, "limit": 1000},
        )
        parsed = parse_rows(rows, end_ms)
        if not parsed:
            break
        output.extend(parsed)
        cursor = parsed[-1].ts + BAR_MS
    return symbol, output


def fetch_1m(symbol: str, start_ms: int, end_ms: int) -> tuple[str, list[Bar]]:
    output: list[Bar] = []
    cursor = start_ms
    while cursor < end_ms:
        rows = get_json(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": "1m", "startTime": cursor, "endTime": end_ms, "limit": 1500},
        )
        parsed = parse_rows(rows, end_ms)
        if not parsed:
            break
        output.extend(parsed)
        cursor = parsed[-1].ts + MINUTE_MS
    return symbol, output


def universe(now_ms: int) -> list[str]:
    majors = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}
    spot = {
        item["symbol"]
        for item in get_url_json(SPOT_DATA, "/api/v3/exchangeInfo")["symbols"]
        if item.get("status") == "TRADING"
        and item.get("quoteAsset") == "USDT"
        and item.get("isSpotTradingAllowed") is True
    }
    futures = {
        item["symbol"]
        for item in get_json("/fapi/v1/exchangeInfo")["symbols"]
        if item.get("status") == "TRADING"
        and item.get("contractType") == "PERPETUAL"
        and item.get("quoteAsset") == "USDT"
        and item.get("underlyingType") == "COIN"
        and int(item.get("onboardDate", 10**30)) <= now_ms - 7 * DAY_MS
    }
    return sorted((spot & futures) - majors)


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def evaluate(symbol: str, bars: list[Bar], i: int, cfg: dict[str, object]) -> Signal | None:
    if i < 7 * 96 + 16:
        return None
    close = bars[i].close
    prior = bars[i - 96 : i]
    prior_high = max(item.high for item in prior)
    prior_low = min(item.low for item in prior)
    side = 1 if close > prior_high else -1 if close < prior_low else 0
    if side == 0:
        return None
    return_1h = close / bars[i - 4].close - 1.0
    return_4h = close / bars[i - 16].close - 1.0
    if not float(cfg["min_return_1h"]) <= side * return_1h <= float(cfg["max_return_1h"]):
        return None
    if not float(cfg["min_return_4h"]) <= side * return_4h <= float(cfg["max_return_4h"]):
        return None

    def hour_volume(end: int) -> float:
        return sum(item.quote_volume for item in bars[end - 3 : end + 1])

    volume_ratio = hour_volume(i) / max(
        median([hour_volume(end) for end in range(i - 7 * 96, i) if end >= 3]), 1.0
    )
    volume_24h = sum(item.quote_volume for item in bars[i - 95 : i + 1])
    path = bars[i - 4 : i + 1]
    travelled = sum(abs(right.close - left.close) for left, right in zip(path, path[1:]))
    efficiency = abs(path[-1].close - path[0].close) / max(travelled, 1e-12)
    candle_range = bars[i].high - bars[i].low
    location = (close - bars[i].low) / candle_range if candle_range else 0.5
    if (
        volume_ratio < float(cfg["min_volume_ratio"])
        or volume_24h < float(cfg["min_24h_volume_usd"])
        or efficiency < float(cfg["min_efficiency"])
    ):
        return None
    min_location = float(cfg["min_close_location"])
    if (side > 0 and location < min_location) or (side < 0 and location > 1.0 - min_location):
        return None
    phase = (
        "overextended_long"
        if side > 0
        and (
            return_1h >= float(cfg["overextension_long_return_1h"])
            or return_4h >= float(cfg["overextension_long_return_4h"])
        )
        else "standard_impulse"
    )
    return Signal(
        symbol=symbol,
        signal_ms=bars[i].ts + BAR_MS - 1,
        side=side,
        price=close,
        score=abs(return_1h) * math.log1p(volume_ratio) * math.log1p(volume_24h / 1_000_000),
        return_1h=return_1h,
        return_4h=return_4h,
        volume_ratio=volume_ratio,
        phase=phase,
        breakout=prior_high if side > 0 else prior_low,
    )


def build_signal_batches(
    bars_by_symbol: dict[str, list[Bar]], start_ms: int, end_ms: int, cfg: dict[str, object]
) -> dict[int, list[Signal]]:
    indexes = {symbol: {bar.ts: i for i, bar in enumerate(bars)} for symbol, bars in bars_by_symbol.items()}
    output: dict[int, list[Signal]] = defaultdict(list)
    for execute_ms in range((start_ms // BAR_MS + 1) * BAR_MS, end_ms, BAR_MS):
        open_ms = execute_ms - BAR_MS
        ranked: list[tuple[float, str, int]] = []
        for symbol, bars in bars_by_symbol.items():
            i = indexes[symbol].get(open_ms)
            if i is None or i < 96:
                continue
            volume = sum(item.quote_volume for item in bars[i - 95 : i + 1])
            change = abs(bars[i].close / bars[i - 96].close - 1.0)
            if volume >= float(cfg["min_24h_volume_usd"]) and change >= 0.04:
                ranked.append((volume * (1.0 + change), symbol, i))
        ranked.sort(reverse=True)
        for _, symbol, i in ranked[: int(cfg["scan_limit"])]:
            signal = evaluate(symbol, bars_by_symbol[symbol], i, cfg)
            if signal:
                output[execute_ms].append(signal)
        output[execute_ms].sort(key=lambda item: (-item.score, item.symbol))
    return output


def pending_decision(pending: Pending, bar: Bar, cfg: dict[str, object]) -> str:
    side = pending.signal.side
    breakout = pending.signal.breakout
    touch_pct = float(cfg["retest_touch_pct"])
    invalidation_pct = float(cfg["retest_invalidation_pct"])
    reclaim_pct = float(cfg["reclaim_pct"])
    touch = bar.low <= breakout * (1.0 + touch_pct) if side > 0 else bar.high >= breakout * (1.0 - touch_pct)
    holds = bar.low >= breakout * (1.0 - invalidation_pct) if side > 0 else bar.high <= breakout * (1.0 + invalidation_pct)
    if not holds:
        return "invalid"
    # Match production: the first bar that touches the breakout only records
    # the retest.  A later closed bar must provide the directional reclaim.
    if not pending.retest_seen:
        return "retest" if touch else "waiting"
    reclaimed = (
        bar.close >= breakout * (1.0 + reclaim_pct) and bar.close > bar.open
        if side > 0
        else bar.close <= breakout * (1.0 - reclaim_pct) and bar.close < bar.open
    )
    if reclaimed:
        return "confirmed"
    return "retest" if touch else "waiting"


def replay(
    batches: dict[int, list[Signal]],
    bars_15m: dict[str, list[Bar]],
    minute_bars: dict[str, list[Bar]],
    start_ms: int,
    end_ms: int,
    cfg: dict[str, object],
) -> dict[str, object]:
    lookup = {symbol: {bar.ts: bar for bar in bars} for symbol, bars in minute_bars.items()}
    lookup_15m = {symbol: {bar.ts: bar for bar in bars} for symbol, bars in bars_15m.items()}
    cash = 1000.0
    fee_rate = 0.0005
    slip = float(cfg["dry_slippage_bps"]) / 10_000.0
    positions: dict[str, Position] = {}
    pending: dict[str, Pending] = {}
    seen: dict[str, int] = {}
    cooldown: dict[str, int] = {}
    recent_exits: list[tuple[int, str]] = []
    trades: list[dict[str, object]] = []
    partials: list[dict[str, object]] = []
    daily_entries = 0
    day = risk_day(start_ms)
    day_start_equity = cash
    daily_loss_blocked = False
    overextended_loss_block = False
    peak = cash
    max_drawdown = 0.0
    window_closing_equity: dict[int, float] = {}

    def equity() -> float:
        return cash + sum(p.signal.side * p.qty * (p.mark - p.entry) for p in positions.values())

    def partial(symbol: str, ts: int, raw: float, fraction: float, reason: str) -> None:
        nonlocal cash
        p = positions[symbol]
        qty = p.qty * fraction
        price = raw * (1.0 - p.signal.side * slip)
        entry_fee = p.entry_fee * qty / p.qty
        exit_fee = qty * price * fee_rate
        pnl = p.signal.side * qty * (price - p.entry) - entry_fee - exit_fee
        cash += p.signal.side * qty * (price - p.entry) - exit_fee
        p.qty -= qty
        p.entry_fee -= entry_fee
        p.initial_notional = p.qty * p.entry
        p.partial_pnl += pnl
        partials.append({"ts_ms": ts, "symbol": symbol, "reason": reason, "pnl": pnl})

    def close(symbol: str, ts: int, raw: float, reason: str) -> None:
        nonlocal cash, overextended_loss_block
        p = positions.pop(symbol)
        price = raw * (1.0 - p.signal.side * slip)
        fee = p.qty * price * fee_rate
        final_pnl = p.signal.side * p.qty * (price - p.entry) - p.entry_fee - fee
        cash += p.signal.side * p.qty * (price - p.entry) - fee
        trade_pnl = p.partial_pnl + final_pnl
        if p.signal.phase == "overextended_long" and trade_pnl < 0:
            overextended_loss_block = True
        cooldown[symbol] = ts + 4 * 60 * MINUTE_MS
        recent_exits.append((ts, symbol))
        trades.append(
            {
                "symbol": symbol,
                "side": "long" if p.signal.side > 0 else "short",
                "phase": p.signal.phase,
                "trigger": p.signal.trigger,
                "entry_ms": p.entry_ms,
                "exit_ms": ts,
                "notional": p.entry_notional,
                "pnl": trade_pnl,
                "reason": reason,
            }
        )

    first_bar = (start_ms // BAR_MS + 1) * BAR_MS
    for ts in range((start_ms // MINUTE_MS) * MINUTE_MS, end_ms, MINUTE_MS):
        current_day = risk_day(ts)
        if current_day != day:
            day = current_day
            daily_entries = 0
            day_start_equity = equity()
            daily_loss_blocked = False
            overextended_loss_block = False

        # Exchange stop is live between polling ticks, so previously installed stops win intrabar.
        for symbol in list(positions):
            p = positions[symbol]
            bar = lookup.get(symbol, {}).get(ts)
            if not bar:
                continue
            p.mark = bar.open
            stop_hit = bar.low <= p.stop if p.signal.side > 0 else bar.high >= p.stop
            if stop_hit:
                close(symbol, ts, p.stop, p.protection)
                continue
            if ts - p.entry_ms >= int(cfg["max_hold_hours"]) * 60 * MINUTE_MS:
                close(symbol, ts, bar.open, "time")
                continue
            p.mark = bar.close
            p.extreme = max(p.extreme, bar.high) if p.signal.side > 0 else min(p.extreme, bar.low)
            p.adverse = min(p.adverse, bar.low) if p.signal.side > 0 else max(p.adverse, bar.high)
            mfe = p.signal.side * (p.extreme / p.entry - 1.0)
            activation = float(cfg["trail_activation_pct"])
            if not p.profit_trimmed and mfe >= activation:
                partial(
                    symbol,
                    ts,
                    p.entry * (1.0 + p.signal.side * activation),
                    float(cfg["partial_take_profit_fraction"]),
                    "partial_take_profit",
                )
                p = positions[symbol]
                p.profit_trimmed = True
                p.stop = p.entry
                p.protection = "partial_take_profit_break_even"
            if mfe >= activation:
                trailing = p.extreme * (1.0 - p.signal.side * float(cfg["trail_pct"]))
                p.stop = max(p.stop, trailing) if p.signal.side > 0 else min(p.stop, trailing)
                p.protection = "trailing_take_profit"
                # High/low ordering is unknown. Counting the retrace in the same minute is conservative.
                retraced = bar.low <= p.stop if p.signal.side > 0 else bar.high >= p.stop
                if retraced:
                    close(symbol, ts, p.stop, "trailing_take_profit")

        # Direct mode enters immediately. Confirmation mode advances only on newly closed 15m bars.
        if ts >= first_bar and ts % BAR_MS == 0:
            executable: list[Signal] = []
            if not bool(cfg["direct_entry_enabled"]):
                for symbol in list(pending):
                    item = pending[symbol]
                    bar = lookup_15m.get(symbol, {}).get(ts - BAR_MS)
                    if not bar or bar.ts + BAR_MS - 1 <= item.last_checked_ms:
                        continue
                    item.last_checked_ms = bar.ts + BAR_MS - 1
                    if item.last_checked_ms > item.expires_ms:
                        del pending[symbol]
                        continue
                    decision = pending_decision(item, bar, cfg)
                    if decision == "invalid":
                        del pending[symbol]
                    elif decision == "retest":
                        item.retest_seen = True
                    elif decision == "confirmed":
                        signal = copy.copy(item.signal)
                        signal.signal_ms = item.last_checked_ms
                        signal.price = bar.close
                        signal.trigger = "retest_reclaim_confirmed"
                        executable.append(signal)
                        del pending[symbol]

            fresh = [
                signal
                for signal in batches.get(ts, [])
                if signal.symbol not in positions
                and signal.symbol not in pending
                and seen.get(signal.symbol) != signal.signal_ms
            ]
            for signal in fresh:
                seen[signal.symbol] = signal.signal_ms
                if bool(cfg["direct_entry_enabled"]):
                    executable.append(signal)
                else:
                    pending[signal.symbol] = Pending(
                        signal,
                        signal.signal_ms + int(cfg["confirmation_window_bars"]) * BAR_MS,
                        signal.signal_ms,
                    )
            executable.sort(key=lambda item: (-item.score, item.symbol))
        else:
            executable = []

        if equity() < day_start_equity * (1.0 - float(cfg["daily_loss_limit"])):
            daily_loss_blocked = True
        if executable and len(positions) < int(cfg["max_positions"]) and not daily_loss_blocked:
            for signal in executable:
                if len(positions) >= int(cfg["max_positions"]) or daily_entries >= int(cfg["max_daily_entries"]):
                    break
                if signal.symbol in positions or cooldown.get(signal.symbol, 0) > ts:
                    continue
                if signal.phase == "overextended_long":
                    if not bool(cfg.get("overextension_long_enabled", True)):
                        continue
                    if overextended_loss_block or any(p.signal.phase == "overextended_long" for p in positions.values()):
                        continue
                    lookback = int(cfg["overextension_reentry_lookback_hours"])
                    if any(sym == signal.symbol and exit_ts >= ts - lookback * 60 * MINUTE_MS for exit_ts, sym in recent_exits):
                        continue
                bar = lookup.get(signal.symbol, {}).get(ts)
                if not bar:
                    continue
                entry = bar.open * (1.0 + signal.side * slip)
                if signal.side * (entry / signal.price - 1.0) > float(cfg["max_entry_slippage_pct"]):
                    continue
                current_equity = equity()
                gross = sum(p.initial_notional for p in positions.values())
                risk_distance = float(cfg["stop_pct"]) + float(cfg["risk_execution_buffer_pct"])
                notional = min(
                    current_equity * float(cfg["risk_per_trade"]) * signal.risk_scale / risk_distance,
                    max(0.0, current_equity * float(cfg["max_gross_multiple"]) - gross),
                )
                if notional < 20:
                    continue
                fee = notional * fee_rate
                qty = notional / entry
                cash -= fee
                positions[signal.symbol] = Position(
                    signal,
                    ts,
                    entry,
                    qty,
                    fee,
                    notional,
                    notional,
                    entry * (1.0 - signal.side * float(cfg["stop_pct"])),
                    entry,
                    entry,
                    entry,
                )
                daily_entries += 1

        # The exchange protection is installed immediately after the fill. If
        # the entry minute crosses it, count the stop instead of waiting for the
        # following polling minute.
        for symbol in [name for name, p in positions.items() if p.entry_ms == ts]:
            p = positions.get(symbol)
            bar = lookup.get(symbol, {}).get(ts)
            if not p or not bar:
                continue
            stop_hit = bar.low <= p.stop if p.signal.side > 0 else bar.high >= p.stop
            if stop_hit:
                close(symbol, ts, p.stop, "initial_stop")
            else:
                p.mark = bar.close

        value = equity()
        peak = max(peak, value)
        max_drawdown = max(max_drawdown, 1 - value / peak)
        # Preserve one close for every requested day. The original three-day
        # helper clamped longer replays into bucket 3, leaving the total intact
        # but making seven-day attribution misleading.
        window_closing_equity[(ts - start_ms) // DAY_MS] = value

    for symbol in list(positions):
        p = positions[symbol]
        close(symbol, end_ms, p.mark, "end_mark")
    pnl = sum(float(t["pnl"]) for t in trades)
    wins = sum(float(t["pnl"]) > 0 for t in trades)
    reasons: dict[str, int] = defaultdict(int)
    for trade in trades:
        reasons[str(trade["reason"])] += 1
    previous = 1000.0
    daily_results: list[dict[str, object]] = []
    for index, ending in sorted(window_closing_equity.items()):
        daily_results.append(
            {
                "window": index + 1,
                "period": [iso(start_ms + index * DAY_MS), iso(min(start_ms + (index + 1) * DAY_MS, end_ms))],
                "ending_equity": ending,
                "pnl": ending - previous,
            }
        )
        previous = ending
    return {
        "ending_equity": cash,
        "net_pnl": pnl,
        "return_pct": (cash / 1000 - 1) * 100,
        "entries": len(trades),
        "wins": wins,
        "win_rate_pct": wins / len(trades) * 100 if trades else 0,
        "max_drawdown_pct": max_drawdown * 100,
        "partial_exits": len(partials),
        "exit_reasons": reasons,
        "daily_results": daily_results,
        "trades": trades,
        "partials": partials,
    }


def iso(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone(timedelta(hours=8))).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--end-ms", type=int)
    parser.add_argument("--strategy", default="config/strategy-altcoin-impulse.toml")
    parser.add_argument("--output", default="out/altcoin-current-3d.json")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--data-cache",
        help="optional gzip pickle containing this exact window's fetched 15m/1m bars",
    )
    parser.add_argument("--compare", action="store_true")
    parser.add_argument(
        "--exit-grid",
        action="store_true",
        help="compare stop/partial/trailing exits while keeping the entry model fixed",
    )
    args = parser.parse_args()
    with Path(args.strategy).open("rb") as handle:
        cfg = tomllib.load(handle)["altcoin_impulse"]
    now_ms = int(get_json("/fapi/v1/time")["serverTime"])
    end_ms = min(args.end_ms or now_ms, now_ms) // MINUTE_MS * MINUTE_MS
    start_ms = end_ms - args.days * DAY_MS
    fetch_start = start_ms - 8 * DAY_MS
    cache_path = Path(args.data_cache) if args.data_cache else None
    cached = None
    if cache_path and cache_path.exists():
        with gzip.open(cache_path, "rb") as handle:
            cached = pickle.load(handle)
        if cached.get("start_ms") != start_ms or cached.get("end_ms") != end_ms:
            raise ValueError("data cache window does not match --days/--end-ms")
    if cached:
        symbols = cached["symbols"]
        bars_15m = cached["bars_15m"]
    else:
        symbols = universe(end_ms)
        bars_15m: dict[str, list[Bar]] = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [pool.submit(fetch_15m, symbol, fetch_start, end_ms) for symbol in symbols]
            for count, job in enumerate(as_completed(jobs), 1):
                symbol, bars = job.result()
                bars_15m[symbol] = bars
                if count % 100 == 0:
                    print(f"15m {count}/{len(symbols)}", flush=True)
    configs = {"current": cfg}
    if args.exit_grid:
        for stop_pct in (0.010, 0.012, 0.015):
            for activation_pct in (0.010, 0.012, 0.015):
                for trail_pct in (0.005, 0.0075):
                    for partial_fraction in (0.33, 0.50):
                        name = (
                            f"s{stop_pct * 100:.1f}_a{activation_pct * 100:.1f}_"
                            f"t{trail_pct * 100:.2f}_p{partial_fraction * 100:.0f}"
                        )
                        configs[name] = {
                            **cfg,
                            "stop_pct": stop_pct,
                            "trail_activation_pct": activation_pct,
                            "trail_pct": trail_pct,
                            "partial_take_profit_fraction": partial_fraction,
                        }
    if args.compare:
        confirm = {**cfg, "direct_entry_enabled": False}
        quality = {
            **confirm,
            "min_volume_ratio": 4.0,
            "min_efficiency": 0.45,
            "min_close_location": 0.70,
            "max_daily_entries": 6,
        }
        patient_exit = {
            **confirm,
            "trail_activation_pct": 0.018,
            "trail_pct": 0.0075,
            "partial_take_profit_fraction": 0.40,
        }
        quality_patient = {
            **quality,
            "trail_activation_pct": 0.018,
            "trail_pct": 0.0075,
            "partial_take_profit_fraction": 0.40,
        }
        direct_quality_patient = {**quality_patient, "direct_entry_enabled": True}
        quality_no_overextension = {**quality, "overextension_long_enabled": False}
        configs.update(
            {
                "confirmation_only": confirm,
                "confirmation_quality": quality,
                "confirmation_patient_exit": patient_exit,
                "confirmation_quality_patient_exit": quality_patient,
                "direct_quality_patient_exit": direct_quality_patient,
                "confirmation_quality_no_overextension": quality_no_overextension,
            }
        )
    if args.exit_grid:
        # Exit-only variants share precisely the same signal stream. Reusing it both
        # speeds up the search and prevents accidental entry-model drift.
        shared_batches = build_signal_batches(bars_15m, start_ms, end_ms, cfg)
        batches_by_variant = {name: shared_batches for name in configs}
    else:
        batches_by_variant = {
            name: build_signal_batches(bars_15m, start_ms, end_ms, variant_cfg)
            for name, variant_cfg in configs.items()
        }
    signal_symbols = sorted(
        {
            signal.symbol
            for batches in batches_by_variant.values()
            for values in batches.values()
            for signal in values
        }
    )
    minute_bars: dict[str, list[Bar]] = cached.get("minute_bars", {}) if cached else {}
    missing_minutes = [symbol for symbol in signal_symbols if symbol not in minute_bars]
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = [
            pool.submit(fetch_1m, symbol, start_ms - BAR_MS, end_ms)
            for symbol in missing_minutes
        ]
        for job in as_completed(jobs):
            symbol, bars = job.result()
            minute_bars[symbol] = bars
    if cache_path and not cached:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(cache_path, "wb") as handle:
            pickle.dump(
                {
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "symbols": symbols,
                    "bars_15m": bars_15m,
                    "minute_bars": minute_bars,
                },
                handle,
                protocol=pickle.HIGHEST_PROTOCOL,
            )
    variants = {
        name: replay(batches_by_variant[name], bars_15m, minute_bars, start_ms, end_ms, variant_cfg)
        for name, variant_cfg in configs.items()
    }
    current = variants["current"]
    batches = batches_by_variant["current"]
    report = {
        "period": [iso(start_ms), iso(end_ms)],
        "universe": len(symbols),
        "raw_signals": sum(len(v) for v in batches.values()),
        "signal_symbols": len(signal_symbols),
        "assumptions": {
            "capital": 1000,
            "fees_and_slippage": "5bp fee + 5bp slippage per side",
            "position": (
                f"{float(cfg['risk_per_trade'])*100:.1f}% equity risk budget / "
                f"{(float(cfg['stop_pct'])+float(cfg['risk_execution_buffer_pct']))*100:.1f}% "
                f"risk distance; max gross {float(cfg['max_gross_multiple']):.1f}x; "
                f"max {int(cfg['max_positions'])}"
            ),
            "limits": (
                f"{int(cfg['max_daily_entries'])} entries per Asia/Shanghai calendar day; "
                f"{int(cfg['cooldown_hours'])}h per-symbol cooldown; "
                f"latched {float(cfg['daily_loss_limit'])*100:.1f}% daily loss halt"
            ),
            "entry": (
                "eligible closed 15m 24h breakout enters directly"
                if bool(cfg["direct_entry_enabled"])
                else "breakout, later retest bar, then a separate directional reclaim bar"
            ),
            "exit": (
                f"{float(cfg['stop_pct'])*100:.1f}% hard stop; "
                f"+{float(cfg['trail_activation_pct'])*100:.1f}% take "
                f"{float(cfg['partial_take_profit_fraction'])*100:.0f}%; "
                f"{float(cfg['trail_pct'])*100:.1f}% trailing; "
                f"{int(cfg['max_hold_hours'])}h max"
            ),
            "historical_l2": "UNAVAILABLE: spread/depth/impact/recent-trade gate not applied",
            "intrabar": "1m conservative stop-first; same-minute trailing retrace counted",
        },
        "current": current,
    }
    if args.compare or args.exit_grid:
        report["comparison"] = {
            name: {
                "parameters": {
                    "direct_entry_enabled": variant_cfg["direct_entry_enabled"],
                    "min_volume_ratio": variant_cfg["min_volume_ratio"],
                    "min_efficiency": variant_cfg["min_efficiency"],
                    "min_close_location": variant_cfg["min_close_location"],
                    "max_daily_entries": variant_cfg["max_daily_entries"],
                    "stop_pct": variant_cfg["stop_pct"],
                    "trail_activation_pct": variant_cfg["trail_activation_pct"],
                    "trail_pct": variant_cfg["trail_pct"],
                    "partial_take_profit_fraction": variant_cfg["partial_take_profit_fraction"],
                    "overextension_long_enabled": variant_cfg.get("overextension_long_enabled", True),
                },
                "result": variants[name],
            }
            for name, variant_cfg in configs.items()
        }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    if args.quiet:
        print(
            f"wrote={output} pnl={current['net_pnl']:.2f} "
            f"trades={current['entries']} windows={len(current['daily_results'])}"
        )
    else:
        print("REPORT_JSON")
        print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
