#!/usr/bin/env python3
"""Research a fast altcoin impulse sleeve on public Binance USD-M data.

This is deliberately isolated from the live engine.  It selects currently liquid
USDT perpetuals, forms causal 15-minute breakout/volume signals, executes on the
next open, and models small-cap slippage, fees, stops and portfolio constraints.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone


BASE = "https://fapi.binance.com"
BAR_MS = 15 * 60 * 1_000
DAY_MS = 86_400_000


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float


@dataclass(frozen=True)
class Model:
    name: str
    return_1h: float
    return_4h: float
    volume_ratio: float
    stop_fraction: float = 0.08
    trail_fraction: float = 0.10
    trail_activation: float = 0.12
    risk_per_trade: float = 0.03
    max_symbol_weight: float = 0.75
    max_positions: int = 3
    max_gross: float = 1.5
    fee_bps: float = 5.0
    slippage_bps: float = 5.0
    minimum_24h_volume: float = 5_000_000.0
    cooldown_bars: int = 16
    max_holding_bars: int = 3 * 96
    maximum_return_1h: float = 0.20
    maximum_return_4h: float = 0.45
    minimum_efficiency: float = 0.40
    minimum_close_location: float = 0.70
    maximum_daily_entries: int = 4
    daily_loss_limit: float = 0.04


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


@dataclass(frozen=True)
class Trade:
    symbol: str
    side: int
    entry_ts: int
    exit_ts: int
    entry_price: float
    exit_price: float
    net_pnl: float
    return_fraction: float
    reason: str


def request(path: str, params: dict[str, object] | None = None) -> object:
    query = urllib.parse.urlencode(params or {})
    url = (path if path.startswith("http") else BASE + path) + ("?" + query if query else "")
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                return json.load(response)
        except Exception:
            if attempt == 4:
                raise
            time.sleep(0.5 * 2**attempt)
    raise AssertionError("unreachable")


def fetch_bars(symbol: str, start_ts: int, end_ts: int) -> list[Bar]:
    rows: list[list[object]] = []
    cursor = start_ts
    while cursor < end_ts:
        batch = request(
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": "15m",
                "startTime": cursor,
                "endTime": end_ts,
                "limit": 1500,
            },
        )
        assert isinstance(batch, list)
        if not batch:
            break
        rows.extend(batch)
        next_cursor = int(batch[-1][0]) + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor
    deduped = {int(row[0]): row for row in rows}
    return [
        Bar(
            ts=ts,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            quote_volume=float(row[7]),
        )
        for ts, row in sorted(deduped.items())
    ]


def liquid_symbols(minimum_current_volume: float, require_spot: bool = False) -> list[str]:
    exchange = request("/fapi/v1/exchangeInfo")
    tickers = request("/fapi/v1/ticker/24hr")
    assert isinstance(exchange, dict) and isinstance(tickers, list)
    active = {
        item["symbol"]
        for item in exchange["symbols"]
        if item["status"] == "TRADING"
        and item["contractType"] == "PERPETUAL"
        and item["quoteAsset"] == "USDT"
    }
    spot_active: set[str] | None = None
    if require_spot:
        spot_exchange = request("https://api.binance.com/api/v3/exchangeInfo")
        assert isinstance(spot_exchange, dict)
        spot_active = {
            item["symbol"]
            for item in spot_exchange["symbols"]
            if item["status"] == "TRADING"
            and item["quoteAsset"] == "USDT"
            and item.get("isSpotTradingAllowed", False)
        }
    majors = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"}
    return sorted(
        item["symbol"]
        for item in tickers
        if item["symbol"] in active
        and (spot_active is None or item["symbol"] in spot_active)
        and item["symbol"] not in majors
        and float(item["quoteVolume"]) >= minimum_current_volume
    )


def signal_inputs(bars: list[Bar]) -> dict[int, tuple[object, ...]]:
    """Map execution timestamp to (direction, score), using the prior close only."""
    result: dict[int, tuple[int, float]] = {}
    one_hour_volume = [0.0] * len(bars)
    for index in range(3, len(bars)):
        one_hour_volume[index] = sum(bar.quote_volume for bar in bars[index - 3 : index + 1])
    for execute_index in range(7 * 96 + 1, len(bars)):
        index = execute_index - 1
        prior_volumes = one_hour_volume[index - 7 * 96 : index]
        median_volume = statistics.median(prior_volumes)
        if median_volume <= 0.0:
            continue
        close = bars[index].close
        return_1h = close / bars[index - 4].close - 1.0
        return_4h = close / bars[index - 16].close - 1.0
        volume_ratio = one_hour_volume[index] / median_volume
        prior_high = max(bar.high for bar in bars[index - 96 : index])
        prior_low = min(bar.low for bar in bars[index - 96 : index])
        volume_24h = sum(bar.quote_volume for bar in bars[index - 95 : index + 1])
        path = [bars[item].close for item in range(index - 4, index + 1)]
        traveled = sum(abs(current - previous) for previous, current in zip(path, path[1:]))
        efficiency = abs(path[-1] - path[0]) / traveled if traveled > 0.0 else 0.0
        spread = bars[index].high - bars[index].low
        close_location = (close - bars[index].low) / spread if spread > 0.0 else 0.5
        direction = 1 if close > prior_high else -1 if close < prior_low else 0
        if direction:
            score = direction * return_1h * math.log1p(volume_ratio) * math.log1p(volume_24h / 1e6)
            result[bars[execute_index].ts] = (
                direction,
                score,
                return_1h,
                return_4h,
                volume_ratio,
                volume_24h,
                efficiency,
                close_location,
            )
    return result


def execution_price(price: float, side: int, bps: float) -> float:
    return price * (1.0 + side * bps / 10_000.0)


def simulate(
    all_bars: dict[str, list[Bar]],
    model: Model,
    initial_cash: float = 1_000.0,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> dict[str, object]:
    lookups = {symbol: {bar.ts: (i, bar) for i, bar in enumerate(bars)} for symbol, bars in all_bars.items()}
    signals = {symbol: signal_inputs(bars) for symbol, bars in all_bars.items()}
    timeline = sorted(
        {
            bar.ts
            for bars in all_bars.values()
            for bar in bars
            if (start_ts is None or bar.ts >= start_ts)
            and (end_ts is None or bar.ts <= end_ts)
        }
    )
    cash = initial_cash
    positions: dict[str, Position] = {}
    last_exit_index: dict[str, int] = {}
    last_prices: dict[str, float] = {}
    trades: list[Trade] = []
    curve: list[tuple[int, float]] = []
    current_day: int | None = None
    day_start_equity = initial_cash
    daily_entries = 0

    def equity(prices: dict[str, float]) -> float:
        return cash + sum(
            position.side * position.qty * (prices.get(symbol, position.entry_price) - position.entry_price)
            for symbol, position in positions.items()
        )

    def close(symbol: str, ts: int, raw_price: float, reason: str) -> None:
        nonlocal cash
        position = positions.pop(symbol)
        price = execution_price(raw_price, -position.side, model.slippage_bps)
        price_pnl = position.side * position.qty * (price - position.entry_price)
        exit_fee = position.qty * price * model.fee_bps / 10_000.0
        cash += price_pnl - exit_fee
        notional = position.qty * position.entry_price
        trades.append(
            Trade(
                symbol=symbol,
                side=position.side,
                entry_ts=position.entry_ts,
                exit_ts=ts,
                entry_price=position.entry_price,
                exit_price=price,
                net_pnl=price_pnl - position.entry_fee - exit_fee,
                return_fraction=(price_pnl - position.entry_fee - exit_fee) / notional,
                reason=reason,
            )
        )

    for ts in timeline:
        current = {symbol: lookup[ts] for symbol, lookup in lookups.items() if ts in lookup}
        for symbol, (index, bar) in current.items():
            last_prices[symbol] = bar.open
            position = positions.get(symbol)
            if not position:
                continue
            gap_hit = bar.open <= position.stop if position.side > 0 else bar.open >= position.stop
            intrabar_hit = bar.low <= position.stop if position.side > 0 else bar.high >= position.stop
            timed_out = index - position.entry_index >= model.max_holding_bars
            if gap_hit or intrabar_hit or timed_out:
                raw = bar.open if gap_hit else position.stop if intrabar_hit else bar.open
                close(symbol, ts, raw, "stop" if gap_hit or intrabar_hit else "time")
                last_exit_index[symbol] = index

        prices = {symbol: bar.open for symbol, (_, bar) in current.items()}
        prices.update(last_prices)
        current_equity = equity(prices)
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start_equity = current_equity
            daily_entries = 0
        entries_allowed = (
            daily_entries < model.maximum_daily_entries
            and current_equity >= day_start_equity * (1.0 - model.daily_loss_limit)
        )
        gross = sum(position.qty * prices[position.symbol] for position in positions.values())
        capacity = max(0.0, model.max_gross * current_equity - gross)
        candidates: list[tuple[float, str, int, Bar]] = []
        for symbol, (index, bar) in current.items():
            values = signals[symbol].get(ts)
            if not values or symbol in positions:
                continue
            (
                direction,
                score,
                return_1h,
                return_4h,
                volume_ratio,
                volume_24h,
                efficiency,
                close_location,
            ) = values
            if index - last_exit_index.get(symbol, -10_000) < model.cooldown_bars:
                continue
            valid_return = (
                model.return_1h <= return_1h <= model.maximum_return_1h
                and model.return_4h <= return_4h <= model.maximum_return_4h
                if direction > 0
                else -model.maximum_return_1h <= return_1h <= -model.return_1h
                and -model.maximum_return_4h <= return_4h <= -model.return_4h
            )
            close_is_strong = (
                close_location >= model.minimum_close_location
                if direction > 0
                else close_location <= 1.0 - model.minimum_close_location
            )
            if (
                entries_allowed
                and valid_return
                and efficiency >= model.minimum_efficiency
                and close_is_strong
                and volume_ratio >= model.volume_ratio
                and volume_24h >= model.minimum_24h_volume
            ):
                candidates.append((abs(score), symbol, direction, bar))
        candidates.sort(reverse=True)
        for _, symbol, direction, bar in candidates:
            if len(positions) >= model.max_positions or capacity < 50.0:
                break
            notional = min(
                current_equity * model.risk_per_trade / model.stop_fraction,
                current_equity * model.max_symbol_weight,
                capacity,
            )
            if notional < 50.0:
                continue
            price = execution_price(bar.open, direction, model.slippage_bps)
            qty = notional / price
            fee = notional * model.fee_bps / 10_000.0
            cash -= fee
            stop = price * (1.0 - direction * model.stop_fraction)
            positions[symbol] = Position(symbol, direction, qty, ts, current[symbol][0], price, fee, price, stop)
            capacity -= notional
            daily_entries += 1

        for symbol, (index, bar) in current.items():
            last_prices[symbol] = bar.close
            position = positions.get(symbol)
            if not position:
                continue
            if position.side > 0:
                position.extreme = max(position.extreme, bar.high)
                if position.extreme / position.entry_price - 1.0 >= model.trail_activation:
                    position.stop = max(position.stop, position.extreme * (1.0 - model.trail_fraction))
            else:
                position.extreme = min(position.extreme, bar.low)
                if 1.0 - position.extreme / position.entry_price >= model.trail_activation:
                    position.stop = min(position.stop, position.extreme * (1.0 + model.trail_fraction))
        curve.append((ts, equity(last_prices)))

    if timeline:
        for symbol in list(positions):
            close(symbol, timeline[-1], last_prices[symbol], "end")
        curve.append((timeline[-1], cash))

    peak = initial_cash
    drawdown = 0.0
    for _, value in curve:
        peak = max(peak, value)
        drawdown = max(drawdown, 1.0 - value / peak)
    wins = [trade for trade in trades if trade.net_pnl > 0.0]
    losses = [trade for trade in trades if trade.net_pnl < 0.0]
    tut = [trade for trade in trades if trade.symbol == "TUTUSDT"]
    entries_by_day: dict[str, int] = {}
    for trade in trades:
        day = datetime.fromtimestamp(trade.entry_ts / 1_000, timezone.utc).date().isoformat()
        entries_by_day[day] = entries_by_day.get(day, 0) + 1
    calendar_days = max(1, math.ceil((timeline[-1] - timeline[0] + BAR_MS) / DAY_MS)) if timeline else 1
    return {
        "model": model.name,
        "return_pct": (cash / initial_cash - 1.0) * 100.0,
        "final_equity": cash,
        "max_drawdown_pct": drawdown * 100.0,
        "trades": len(trades),
        "win_rate_pct": len(wins) / len(trades) * 100.0 if trades else 0.0,
        "profit_factor": sum(t.net_pnl for t in wins) / -sum(t.net_pnl for t in losses) if losses else math.inf,
        "longs": sum(t.side > 0 for t in trades),
        "shorts": sum(t.side < 0 for t in trades),
        "active_days": len(entries_by_day),
        "calendar_days": calendar_days,
        "days_without_entries": calendar_days - len(entries_by_day),
        "average_trades_per_calendar_day": len(trades) / calendar_days,
        "entries_by_day": entries_by_day,
        "best": [(t.symbol, round(t.net_pnl, 2), round(t.return_fraction * 100.0, 2)) for t in sorted(trades, key=lambda x: x.net_pnl, reverse=True)[:10]],
        "worst": [(t.symbol, round(t.net_pnl, 2), round(t.return_fraction * 100.0, 2)) for t in sorted(trades, key=lambda x: x.net_pnl)[:10]],
        "tut": [
            {
                "side": "long" if t.side > 0 else "short",
                "entry": datetime.fromtimestamp(t.entry_ts / 1_000, timezone.utc).isoformat(),
                "exit": datetime.fromtimestamp(t.exit_ts / 1_000, timezone.utc).isoformat(),
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "return_pct": t.return_fraction * 100.0,
                "pnl": t.net_pnl,
                "reason": t.reason,
            }
            for t in tut
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--current-volume", type=float, default=10_000_000.0)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--require-spot", action="store_true")
    args = parser.parse_args()
    now = int(request("/fapi/v1/time")["serverTime"])
    symbols = liquid_symbols(args.current_volume, args.require_spot)
    bars: dict[str, list[Bar]] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        tasks = {pool.submit(fetch_bars, symbol, now - args.days * DAY_MS, now): symbol for symbol in symbols}
        for task in as_completed(tasks):
            symbol = tasks[task]
            try:
                values = task.result()
                if len(values) >= 8 * 96:
                    bars[symbol] = values
            except Exception as error:
                print(f"warning: {symbol}: {error}", flush=True)
    models = [
        Model(
            "fast-impulse-R2",
            0.04,
            0.06,
            3.0,
            stop_fraction=0.05,
            trail_fraction=0.03,
            trail_activation=0.05,
            risk_per_trade=0.10,
            max_symbol_weight=2.0,
            max_positions=2,
            max_gross=4.0,
            cooldown_bars=32,
            max_holding_bars=24,
            maximum_return_1h=0.45,
            maximum_return_4h=1.20,
            minimum_efficiency=0.35,
            minimum_close_location=0.60,
            maximum_daily_entries=6,
        ),
        Model("balanced-R1.5", 0.04, 0.08, 4.0, risk_per_trade=0.015, max_positions=2, max_gross=1.0, cooldown_bars=96),
        Model("balanced-R2", 0.04, 0.08, 4.0, risk_per_trade=0.02, max_positions=2, max_gross=1.25, cooldown_bars=96),
        Model("strict-R2", 0.06, 0.10, 5.0, risk_per_trade=0.02, max_positions=2, max_gross=1.25, cooldown_bars=96),
    ]
    periods = {
        "week_minus_3": (now - 21 * DAY_MS, now - 14 * DAY_MS),
        "week_minus_2": (now - 14 * DAY_MS, now - 7 * DAY_MS),
        "latest_week": (now - 7 * DAY_MS, now),
        "full": (now - args.days * DAY_MS, now),
    }
    report = {
        "generated_at": datetime.fromtimestamp(now / 1_000, timezone.utc).isoformat(),
        "selection_warning": (
            "current active-contract universe; research sample is not fully survivorship-free"
            if args.current_volume <= 0
            else "symbols selected by current 24h volume; research sample is not survivorship-free"
        ),
        "requires_current_spot_listing": args.require_spot,
        "symbols": len(bars),
        "days": args.days,
        "results": {
            model.name: {
                period: simulate(bars, model, start_ts=start, end_ts=end)
                for period, (start, end) in periods.items()
                if start >= now - args.days * DAY_MS
            }
            for model in models
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
