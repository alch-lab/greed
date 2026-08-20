#!/usr/bin/env python3
"""Replay the deployed altcoin impulse rules over the current CST day.

Signals use closed 15m bars and the production top-60 rolling-24h shortlist.
Positions are replayed on 1m bars with conservative stop-first intrabar
ordering.  This is a research replay, not an exchange fill reconstruction.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


FAPI = "https://fapi.binance.com"
SPOT = "https://api.binance.com"
SPOT_DATA = "https://data-api.binance.vision"
MINUTE_MS = 60_000
BAR_MS = 15 * MINUTE_MS
DAY_MS = 86_400_000
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
class Signal:
    symbol: str
    signal_ms: int
    execute_ms: int
    side: int
    price: float
    score: float
    return_1h: float
    return_4h: float
    volume_ratio: float


@dataclass
class Position:
    signal: Signal
    entry_ms: int
    entry: float
    qty: float
    entry_fee: float
    initial_notional: float
    stop: float
    extreme: float
    mark: float


def get_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": "greed-research/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def parse_bars(rows: object, now_ms: int) -> list[Bar]:
    output: list[Bar] = []
    for row in rows:
        if int(row[6]) >= now_ms:
            continue
        output.append(
            Bar(
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(row[7]),
            )
        )
    return output


def fetch_bars(
    symbol: str,
    interval: str,
    limit: int,
    now_ms: int,
    start_ms: int | None = None,
    spot_proxy: bool = False,
) -> tuple[str, list[Bar]]:
    params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
    if start_ms is not None:
        params["startTime"] = str(start_ms)
    base = SPOT_DATA if spot_proxy else FAPI
    path = "/api/v3/klines" if spot_proxy else "/fapi/v1/klines"
    url = f"{base}{path}?{urllib.parse.urlencode(params)}"
    return symbol, parse_bars(get_json(url), now_ms)


def current_universe() -> list[str]:
    futures = {
        item["symbol"]
        for item in get_json(f"{FAPI}/fapi/v1/exchangeInfo")["symbols"]
        if item.get("status") == "TRADING"
        and item.get("contractType") == "PERPETUAL"
        and item.get("quoteAsset") == "USDT"
    }
    spot = {
        item["symbol"]
        for item in get_json(f"{SPOT}/api/v3/exchangeInfo")["symbols"]
        if item.get("status") == "TRADING"
        and item.get("quoteAsset") == "USDT"
        and item.get("isSpotTradingAllowed") is True
    }
    return sorted((futures & spot) - MAJORS)


def evaluate(symbol: str, bars: list[Bar], index: int) -> Signal | None:
    if index < 7 * 96 + 16:
        return None
    close = bars[index].close
    prior = bars[index - 96 : index]
    prior_high = max(item.high for item in prior)
    prior_low = min(item.low for item in prior)
    side = 1 if close > prior_high else -1 if close < prior_low else 0
    if side == 0:
        return None
    return_1h = close / bars[index - 4].close - 1.0
    return_4h = close / bars[index - 16].close - 1.0
    directional_1h = side * return_1h
    directional_4h = side * return_4h
    if not 0.04 <= directional_1h <= 0.45 or not 0.06 <= directional_4h <= 1.20:
        return None

    def hour_volume(end: int) -> float:
        return sum(item.quote_volume for item in bars[end - 3 : end + 1])

    current_hour = hour_volume(index)
    historical = [hour_volume(end) for end in range(index - 7 * 96, index) if end >= 3]
    volume_ratio = current_hour / max(statistics.median(historical), 1.0)
    if volume_ratio < 3.0:
        return None
    volume_24h = sum(item.quote_volume for item in bars[index - 95 : index + 1])
    if volume_24h < 5_000_000:
        return None
    path = bars[index - 4 : index + 1]
    travelled = sum(abs(right.close - left.close) for left, right in zip(path, path[1:]))
    efficiency = abs(path[-1].close - path[0].close) / max(travelled, 1e-12)
    if efficiency < 0.35:
        return None
    spread = bars[index].high - bars[index].low
    close_location = (close - bars[index].low) / spread if spread > 0 else 0.5
    if side > 0 and close_location < 0.60:
        return None
    if side < 0 and close_location > 0.40:
        return None
    score = abs(return_1h) * math.log1p(volume_ratio) * math.log1p(volume_24h / 1_000_000)
    return Signal(
        symbol,
        bars[index].ts + BAR_MS - 1,
        bars[index].ts + BAR_MS,
        side,
        close,
        score,
        return_1h,
        return_4h,
        volume_ratio,
    )


def build_signals(bars_by_symbol: dict[str, list[Bar]], start_ms: int, end_ms: int) -> list[Signal]:
    by_timestamp: dict[int, list[tuple[str, int, float, float]]] = defaultdict(list)
    index_by_symbol: dict[str, dict[int, int]] = {}
    for symbol, bars in bars_by_symbol.items():
        lookup = {bar.ts: index for index, bar in enumerate(bars)}
        index_by_symbol[symbol] = lookup
        for index in range(96, len(bars)):
            execute_ms = bars[index].ts + BAR_MS
            if not start_ms <= execute_ms < end_ms:
                continue
            volume_24h = sum(item.quote_volume for item in bars[index - 95 : index + 1])
            change = abs(bars[index].close / bars[index - 96].close - 1.0)
            if volume_24h >= 5_000_000 and change >= 0.04:
                by_timestamp[execute_ms].append(
                    (symbol, index, volume_24h * (1.0 + change), change)
                )
    output: list[Signal] = []
    for execute_ms, rows in by_timestamp.items():
        rows.sort(key=lambda item: item[2], reverse=True)
        for symbol, index, _, _ in rows[:60]:
            signal = evaluate(symbol, bars_by_symbol[symbol], index)
            if signal is not None:
                output.append(signal)
    return sorted(output, key=lambda item: (item.execute_ms, -item.score, item.symbol))


def load_journal_signals(path: str) -> tuple[list[Signal], int, int, list[int]]:
    scans: list[dict[str, object]] = []
    bonus_times: list[int] = []
    for line in Path(path).read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("event") == "scan":
            scans.append(event)
        elif event.get("event") == "daily_entry_limit_increased":
            bonus_times.append(int(event["ts_ms"]))
    if not scans:
        raise ValueError("journal has no scan events")
    output: list[Signal] = []
    seen: set[tuple[int, str, int]] = set()
    for scan in scans:
        execute_ms = int(scan["ts_ms"]) // MINUTE_MS * MINUTE_MS
        for candidate in scan.get("candidates", []):
            if candidate.get("blockers") or int(candidate.get("side", 0)) == 0:
                continue
            key = (execute_ms, str(candidate["symbol"]), int(candidate["signal_ms"]))
            if key in seen:
                continue
            seen.add(key)
            output.append(
                Signal(
                    str(candidate["symbol"]),
                    int(candidate["signal_ms"]),
                    execute_ms,
                    int(candidate["side"]),
                    float(candidate["price"]),
                    float(candidate["score"]),
                    float(candidate["return_1h"]),
                    float(candidate["return_4h"]),
                    float(candidate["volume_ratio"]),
                )
            )
    start_ms = int(scans[0]["ts_ms"]) // MINUTE_MS * MINUTE_MS
    end_ms = (int(scans[-1]["ts_ms"]) // MINUTE_MS + 1) * MINUTE_MS
    return sorted(output, key=lambda item: (item.execute_ms, -item.score, item.symbol)), start_ms, end_ms, bonus_times


def replay(
    signals: list[Signal],
    minute_bars: dict[str, list[Bar]],
    start_ms: int,
    end_ms: int,
    bonus_times: list[int],
) -> dict[str, object]:
    signals_at: dict[int, list[Signal]] = defaultdict(list)
    for signal in signals:
        signals_at[signal.execute_ms].append(signal)
    minute_lookup = {
        symbol: {bar.ts: bar for bar in bars}
        for symbol, bars in minute_bars.items()
    }
    cash = 1_000.0
    fee_rate = 0.0005
    slippage = 0.0005
    positions: dict[str, Position] = {}
    cooldown_until: dict[str, int] = {}
    trades: list[dict[str, object]] = []
    seen_signal: dict[str, int] = {}
    daily_entries = 0
    daily_bonus = 0
    day = start_ms // DAY_MS
    day_start_equity = cash
    bonus_cursor = 0
    peak = cash
    max_drawdown = 0.0

    def equity() -> float:
        return cash + sum(
            pos.signal.side * pos.qty * (pos.mark - pos.entry)
            for pos in positions.values()
        )

    def close(symbol: str, ts: int, raw_price: float, reason: str) -> None:
        nonlocal cash
        pos = positions.pop(symbol)
        exit_price = raw_price * (1.0 - pos.signal.side * slippage)
        gross = pos.signal.side * pos.qty * (exit_price - pos.entry)
        exit_fee = pos.qty * exit_price * fee_rate
        cash += gross - exit_fee
        pnl = gross - pos.entry_fee - exit_fee
        trades.append(
            {
                "symbol": symbol,
                "side": "long" if pos.signal.side > 0 else "short",
                "entry_ms": pos.entry_ms,
                "exit_ms": ts,
                "entry": pos.entry,
                "exit": exit_price,
                "pnl": pnl,
                "reason": reason,
                "return_1h_pct": pos.signal.return_1h * 100,
                "volume_ratio": pos.signal.volume_ratio,
            }
        )
        cooldown_until[symbol] = ts + 8 * 60 * MINUTE_MS

    for ts in range(start_ms, end_ms, MINUTE_MS):
        current_day = ts // DAY_MS
        if current_day != day:
            day = current_day
            day_start_equity = equity()
            daily_entries = 0
            daily_bonus = 0
        while bonus_cursor < len(bonus_times) and bonus_times[bonus_cursor] <= ts:
            daily_bonus = min(4, daily_bonus + 2)
            bonus_cursor += 1

        for symbol in list(positions):
            pos = positions[symbol]
            bar = minute_lookup.get(symbol, {}).get(ts)
            if bar is None:
                continue
            pos.mark = bar.open
            gap = bar.open <= pos.stop if pos.signal.side > 0 else bar.open >= pos.stop
            hit = bar.low <= pos.stop if pos.signal.side > 0 else bar.high >= pos.stop
            timed = ts - pos.entry_ms >= 6 * 60 * MINUTE_MS
            if gap or hit:
                close(symbol, ts, bar.open if gap else pos.stop, "trailing" if pos.stop * pos.signal.side > pos.entry * pos.signal.side else "initial_stop")
                continue
            if timed:
                close(symbol, ts, bar.open, "time")
                continue
            pos.mark = bar.close
            if pos.signal.side > 0:
                pos.extreme = max(pos.extreme, bar.high)
                excursion = pos.extreme / pos.entry - 1.0
                if excursion >= 0.02:
                    pos.stop = max(pos.stop, pos.extreme * 0.99)
            else:
                pos.extreme = min(pos.extreme, bar.low)
                excursion = 1.0 - pos.extreme / pos.entry
                if excursion >= 0.02:
                    pos.stop = min(pos.stop, pos.extreme * 1.01)

        current_equity = equity()
        effective_limit = 6 + daily_bonus
        if daily_entries < effective_limit and current_equity >= day_start_equity * 0.96:
            gross = sum(pos.initial_notional for pos in positions.values())
            capacity = max(0.0, current_equity * 4.0 - gross)
            for signal in signals_at.get(ts, []):
                # At the next minute open the signal is still inside the 120s window.
                if (
                    len(positions) >= 2
                    or daily_entries >= effective_limit
                    or ts - signal.signal_ms > 120_000
                    or signal.symbol in positions
                    or cooldown_until.get(signal.symbol, 0) > ts
                    or seen_signal.get(signal.symbol) == signal.signal_ms
                    or capacity < 20.0
                ):
                    continue
                bar = minute_lookup.get(signal.symbol, {}).get(ts)
                if bar is None:
                    continue
                seen_signal[signal.symbol] = signal.signal_ms
                entry = bar.open * (1.0 + signal.side * slippage)
                # IOC protection refuses entries more than 1.5% beyond signal close.
                if signal.side * (entry / signal.price - 1.0) > 0.015:
                    continue
                notional = min(current_equity * 0.10 / 0.05, capacity)
                if notional < 20.0:
                    continue
                qty = notional / entry
                entry_fee = notional * fee_rate
                cash -= entry_fee
                positions[signal.symbol] = Position(
                    signal,
                    ts,
                    entry,
                    qty,
                    entry_fee,
                    notional,
                    entry * (1.0 - signal.side * 0.05),
                    entry,
                    bar.open,
                )
                daily_entries += 1
                capacity -= notional

        # New orders are submitted together at the scan boundary.  Apply the
        # entry minute only after the whole batch is formed so an intraminute
        # stop cannot create fictional capacity for another same-scan order.
        for symbol in [name for name, pos in positions.items() if pos.entry_ms == ts]:
            pos = positions.get(symbol)
            if pos is None:
                continue
            bar = minute_lookup.get(symbol, {}).get(ts)
            if bar is None:
                continue
            initial_hit = bar.low <= pos.stop if pos.signal.side > 0 else bar.high >= pos.stop
            if initial_hit:
                close(symbol, ts, pos.stop, "initial_stop")
                continue
            pos.mark = bar.close
            if pos.signal.side > 0:
                pos.extreme = max(pos.extreme, bar.high)
                if pos.extreme / pos.entry - 1.0 >= 0.02:
                    pos.stop = max(pos.stop, pos.extreme * 0.99)
            else:
                pos.extreme = min(pos.extreme, bar.low)
                if 1.0 - pos.extreme / pos.entry >= 0.02:
                    pos.stop = min(pos.stop, pos.extreme * 1.01)

        value = equity()
        peak = max(peak, value)
        max_drawdown = max(max_drawdown, 1.0 - value / peak)

    open_positions = []
    for symbol, pos in positions.items():
        mark = pos.mark
        unrealized = pos.signal.side * pos.qty * (mark - pos.entry)
        open_positions.append(
            {
                "symbol": symbol,
                "side": "long" if pos.signal.side > 0 else "short",
                "entry_ms": pos.entry_ms,
                "entry": pos.entry,
                "mark": mark,
                "unrealized_pnl": unrealized,
                "stop": pos.stop,
            }
        )
    realized = sum(float(item["pnl"]) for item in trades)
    unrealized = sum(float(item["unrealized_pnl"]) for item in open_positions)
    wins = sum(float(item["pnl"]) > 0 for item in trades)
    return {
        "equity": cash + unrealized,
        "return_pct": (cash + unrealized) / 1_000.0 * 100 - 100,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "max_drawdown_pct": max_drawdown * 100,
        "entries": len(trades) + len(open_positions),
        "closed": len(trades),
        "wins": wins,
        "win_rate_pct": wins / len(trades) * 100 if trades else 0.0,
        "fees_included": True,
        "trades": trades,
        "open_positions": open_positions,
    }


def iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000, timezone(timedelta(hours=8))).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--end-ms", type=int)
    parser.add_argument("--journal")
    parser.add_argument(
        "--spot-proxy",
        action="store_true",
        help="Use Binance's public spot data endpoint when futures REST is unavailable.",
    )
    args = parser.parse_args()
    time_base = SPOT_DATA if args.spot_proxy else FAPI
    time_path = "/api/v3/time" if args.spot_proxy else "/fapi/v1/time"
    now_ms = int(get_json(f"{time_base}{time_path}")["serverTime"])
    end_ms = min(args.end_ms or now_ms, now_ms)
    cst = timezone(timedelta(hours=8))
    current = datetime.fromtimestamp(end_ms / 1_000, cst)
    if args.journal:
        signals, start_ms, journal_end_minute, actual_clicks = load_journal_signals(args.journal)
        # Continue managing positions through the requested/current minute even
        # though signal discovery stops at the final exported scan.
        end_closed_minute = (end_ms // MINUTE_MS) * MINUTE_MS
        # Journal replay already contains the exact production candidates, so
        # it does not need a live futures universe lookup.
        symbols = sorted({item.symbol for item in signals})
    else:
        start = current.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ms = int(start.timestamp() * 1_000)
        end_closed_minute = (end_ms // MINUTE_MS) * MINUTE_MS
        symbols = current_universe()
        bars_15m: dict[str, list[Bar]] = {}
        with ThreadPoolExecutor(max_workers=24) as pool:
            futures = [pool.submit(fetch_bars, symbol, "15m", 1_000, end_ms) for symbol in symbols]
            for count, future in enumerate(as_completed(futures), 1):
                symbol, bars = future.result()
                bars_15m[symbol] = bars
                if count % 100 == 0:
                    print(f"15m {count}/{len(symbols)}", flush=True)
        signals = build_signals(bars_15m, start_ms, end_closed_minute)
        actual_clicks = [
            int(datetime(2026, 8, 11, 0, 15, tzinfo=cst).timestamp() * 1_000),
            int(datetime(2026, 8, 11, 13, 58, tzinfo=cst).timestamp() * 1_000),
            int(datetime(2026, 8, 11, 18, 42, tzinfo=cst).timestamp() * 1_000),
        ]
    signal_symbols = sorted({item.symbol for item in signals})
    minute_bars: dict[str, list[Bar]] = {}
    with ThreadPoolExecutor(max_workers=24) as pool:
        futures = [
            pool.submit(fetch_bars, symbol, "1m", 1_500, end_ms, start_ms, args.spot_proxy)
            for symbol in signal_symbols
        ]
        for future in as_completed(futures):
            symbol, bars = future.result()
            minute_bars[symbol] = bars
    report = {
        "generated_at": iso(now_ms),
        "period": [iso(start_ms), iso(end_closed_minute)],
        "universe": len(symbols),
        "signals": len(signals),
        "signal_symbols": len(signal_symbols),
        "assumptions": {
            "signal": "production top-60 shortlist and closed 15m rules",
            "execution": "next 1m open; 5bp slippage and 5bp fee each side",
            "intrabar": "1m stop-first conservative ordering",
            "risk": "10% equity risk, 5% stop, max 2 positions, max gross 4x",
        },
        "default_6_entries": replay(signals, minute_bars, start_ms, end_closed_minute, []),
        "same_manual_clicks_capped_at_10": replay(
            signals,
            minute_bars,
            start_ms,
            end_closed_minute,
            [item for item in actual_clicks if start_ms <= item < end_closed_minute],
        ),
    }
    print("REPORT_JSON")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
