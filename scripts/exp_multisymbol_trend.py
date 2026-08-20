#!/usr/bin/env python3
"""Walk-forward experiment for a non-BTC multi-symbol time-series trend sleeve.

The script reads Binance Vision monthly USD-M 4h klines and funding-rate files.
Signals are formed at a bar close and executed at the next bar open.  It models
fees, slippage, actual historical funding, portfolio concentration and ATR risk.

This is research code only; it does not share code with the live trading engine.
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


BAR_MS = 4 * 60 * 60 * 1_000
YEAR_MS = 365.25 * 24 * 60 * 60 * 1_000


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
    entry_side: int = 0
    exit_long: bool = False
    exit_short: bool = False
    score: float = 0.0
    atr: float = 0.0


@dataclass
class Position:
    symbol: str
    side: int
    qty: float
    entry_ts: int
    entry_price: float
    entry_fee: float
    atr: float
    extreme: float
    funding_pnl: float = 0.0


@dataclass
class Trade:
    symbol: str
    side: int
    entry_ts: int
    exit_ts: int
    entry_price: float
    exit_price: float
    price_pnl: float
    funding_pnl: float
    fees: float
    net_pnl: float
    reason: str


@dataclass(frozen=True)
class Config:
    entry_window: int
    exit_window: int
    atr_window: int = 20
    stop_atr: float = 3.0
    risk_per_trade: float = 0.005
    max_symbol_weight: float = 0.20
    max_positions: int = 5
    gross_limit: float = 1.0
    fee_bps: float = 5.0
    slippage_bps: float = 2.0
    allow_short: bool = True
    regime_ema: int = 0
    long_breadth_min: float = 0.0
    short_breadth_max: float = 1.0
    funding_filter_days: int = 0
    max_long_daily_funding: float = math.inf
    min_short_daily_funding: float = -math.inf
    universe_top_n: int = 0
    minimum_daily_volume: float = 1_000_000.0
    minimum_daily_move: float = 0.005

    @property
    def name(self) -> str:
        suffix = "LS" if self.allow_short else "L"
        breadth = f"-B{self.regime_ema}" if self.regime_ema else ""
        carry = f"-F{self.funding_filter_days}" if self.funding_filter_days else ""
        universe = f"-U{self.universe_top_n}" if self.universe_top_n else ""
        return f"D{self.entry_window}/{self.exit_window}-{suffix}{breadth}{carry}{universe}"


@dataclass
class Result:
    config: Config
    start_ts: int
    end_ts: int
    initial_cash: float
    final_equity: float
    trades: list[Trade]
    equity_curve: list[tuple[int, float]]
    funding_pnl: float
    fees: float
    max_gross: float
    by_symbol: dict[str, float] = field(default_factory=dict)


def read_zip_rows(path: Path) -> list[list[str]]:
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise ValueError(f"unexpected archive members: {path}")
        with archive.open(names[0]) as raw:
            return list(csv.reader(io.TextIOWrapper(raw)))


def load_symbol(root: Path, symbol: str) -> tuple[list[Bar], dict[int, float]]:
    bars: list[Bar] = []
    funding: dict[int, float] = {}
    kline_paths = {
        path.stem.split("-4h-", 1)[1]: path
        for path in (root / symbol / "klines").glob("*.zip")
    }
    funding_paths = {
        path.stem.split("-fundingRate-", 1)[1]: path
        for path in (root / symbol / "funding").glob("*.zip")
    }
    paired_months = sorted(kline_paths.keys() & funding_paths.keys())
    for month in paired_months:
        path = kline_paths[month]
        rows = read_zip_rows(path)
        if rows and rows[0][0] == "open_time":
            rows = rows[1:]
        for row in rows:
            bars.append(
                Bar(
                    ts=int(row[0]),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    quote_volume=float(row[7]),
                )
            )
    for month in paired_months:
        path = funding_paths[month]
        rows = read_zip_rows(path)
        if rows and rows[0][0] == "calc_time":
            rows = rows[1:]
        for row in rows:
            funding[int(row[0])] = float(row[2])
    bars.sort(key=lambda item: item.ts)
    deduped = {bar.ts: bar for bar in bars}
    return [deduped[ts] for ts in sorted(deduped)], funding


def true_ranges(bars: list[Bar]) -> list[float]:
    result: list[float] = []
    previous = bars[0].close
    for bar in bars:
        result.append(max(bar.high - bar.low, abs(bar.high - previous), abs(bar.low - previous)))
        previous = bar.close
    return result


def build_signals(bars: list[Bar], config: Config) -> list[Signal]:
    """Return the signal executed at each bar open, using only earlier bars."""
    signals = [Signal() for _ in bars]
    ranges = true_ranges(bars)
    warmup = max(config.entry_window, config.exit_window, config.atr_window) + 1
    for execute_index in range(warmup, len(bars)):
        signal_index = execute_index - 1
        current = bars[signal_index]
        entry_begin = signal_index - config.entry_window
        exit_begin = signal_index - config.exit_window
        prior_high = max(bar.high for bar in bars[entry_begin:signal_index])
        prior_low = min(bar.low for bar in bars[entry_begin:signal_index])
        exit_low = min(bar.low for bar in bars[exit_begin:signal_index])
        exit_high = max(bar.high for bar in bars[exit_begin:signal_index])
        atr = sum(ranges[signal_index - config.atr_window + 1 : signal_index + 1]) / config.atr_window
        if atr <= 0.0:
            continue
        entry_side = 0
        score = 0.0
        if current.close > prior_high:
            entry_side = 1
            score = (current.close - prior_high) / atr
        elif config.allow_short and current.close < prior_low:
            entry_side = -1
            score = (prior_low - current.close) / atr
        signals[execute_index] = Signal(
            entry_side=entry_side,
            exit_long=current.close < exit_low,
            exit_short=current.close > exit_high,
            score=score,
            atr=atr,
        )
    return signals


def build_breadth(all_bars: dict[str, list[Bar]], ema_window: int) -> dict[int, float]:
    """Fraction of the available universe above its causal EMA at each next open."""
    if ema_window <= 0:
        return {}
    votes: dict[int, list[bool]] = defaultdict(list)
    alpha = 2.0 / (ema_window + 1.0)
    for bars in all_bars.values():
        ema = bars[0].close
        for index, bar in enumerate(bars):
            ema = alpha * bar.close + (1.0 - alpha) * ema
            execute_index = index + 1
            if index >= ema_window and execute_index < len(bars):
                votes[bars[execute_index].ts].append(bar.close > ema)
    return {
        ts: sum(items) / len(items)
        for ts, items in votes.items()
        if len(items) >= max(5, len(all_bars) // 2)
    }


def build_funding_filter(
    all_bars: dict[str, list[Bar]],
    all_funding: dict[str, dict[int, float]],
    lookback_days: int,
) -> dict[str, dict[int, float]]:
    if lookback_days <= 0:
        return {}
    window_ms = lookback_days * 86_400_000
    result: dict[str, dict[int, float]] = {}
    for symbol, bars in all_bars.items():
        events = sorted(all_funding[symbol].items())
        left = right = 0
        rolling_sum = 0.0
        values: dict[int, float] = {}
        for bar in bars:
            while right < len(events) and events[right][0] < bar.ts:
                rolling_sum += events[right][1]
                right += 1
            while left < right and events[left][0] < bar.ts - window_ms:
                rolling_sum -= events[left][1]
                left += 1
            values[bar.ts] = rolling_sum / lookback_days
        result[symbol] = values
    return result


def build_monthly_universe(
    all_bars: dict[str, list[Bar]], config: Config, timeline: list[int]
) -> dict[int, set[str]]:
    """Select liquid contracts monthly using only the preceding 30 calendar days."""
    if config.universe_top_n <= 0:
        return {}
    daily: dict[str, dict[int, tuple[float, float]]] = {}
    for symbol, bars in all_bars.items():
        grouped: dict[int, list[Bar]] = defaultdict(list)
        for bar in bars:
            grouped[bar.ts // 86_400_000 * 86_400_000].append(bar)
        previous_close: float | None = None
        observations: dict[int, tuple[float, float]] = {}
        for day_ts in sorted(grouped):
            items = grouped[day_ts]
            close = items[-1].close
            move = abs(close / previous_close - 1.0) if previous_close else 0.0
            observations[day_ts] = (sum(item.quote_volume for item in items), move)
            previous_close = close
        daily[symbol] = observations

    selected: set[str] = set()
    prior_month: tuple[int, int] | None = None
    result: dict[int, set[str]] = {}
    for ts in timeline:
        stamp = datetime.fromtimestamp(ts / 1_000, tz=timezone.utc)
        month = (stamp.year, stamp.month)
        if month != prior_month:
            begin = ts - 30 * 86_400_000
            ranked: list[tuple[float, str]] = []
            for symbol, observations in daily.items():
                sample = [value for day, value in observations.items() if begin <= day < ts]
                if len(sample) < 20:
                    continue
                volume = statistics.median(value[0] for value in sample)
                move = statistics.median(value[1] for value in sample)
                if volume >= config.minimum_daily_volume and move >= config.minimum_daily_move:
                    ranked.append((volume, symbol))
            ranked.sort(reverse=True)
            selected = {symbol for _, symbol in ranked[: config.universe_top_n]}
            prior_month = month
        result[ts] = set(selected)
    return result


def marked_equity(cash: float, positions: dict[str, Position], prices: dict[str, float]) -> float:
    unrealized = 0.0
    for symbol, position in positions.items():
        price = prices.get(symbol, position.entry_price)
        unrealized += position.side * position.qty * (price - position.entry_price)
    return cash + unrealized


def execution_price(raw_price: float, order_side: int, slippage_bps: float) -> float:
    return raw_price * (1.0 + order_side * slippage_bps / 10_000.0)


def close_position(
    position: Position,
    ts: int,
    raw_price: float,
    reason: str,
    config: Config,
) -> tuple[Trade, float]:
    exit_price = execution_price(raw_price, -position.side, config.slippage_bps)
    price_pnl = position.side * position.qty * (exit_price - position.entry_price)
    exit_fee = position.qty * exit_price * config.fee_bps / 10_000.0
    fees = position.entry_fee + exit_fee
    trade = Trade(
        symbol=position.symbol,
        side=position.side,
        entry_ts=position.entry_ts,
        exit_ts=ts,
        entry_price=position.entry_price,
        exit_price=exit_price,
        price_pnl=price_pnl,
        funding_pnl=position.funding_pnl,
        fees=fees,
        net_pnl=price_pnl + position.funding_pnl - fees,
        reason=reason,
    )
    # Entry fee and funding were already booked to cash while the trade was open.
    cash_delta = price_pnl - exit_fee
    return trade, cash_delta


def simulate(
    all_bars: dict[str, list[Bar]],
    all_funding: dict[str, dict[int, float]],
    config: Config,
    start_ts: int,
    end_ts: int,
    initial_cash: float = 100_000.0,
) -> Result:
    indexes = {symbol: {bar.ts: i for i, bar in enumerate(bars)} for symbol, bars in all_bars.items()}
    signals = {symbol: build_signals(bars, config) for symbol, bars in all_bars.items()}
    breadth = build_breadth(all_bars, config.regime_ema)
    daily_funding = build_funding_filter(
        all_bars, all_funding, config.funding_filter_days
    )
    timeline = sorted(
        {
            bar.ts
            for bars in all_bars.values()
            for bar in bars
            if start_ts <= bar.ts <= end_ts
        }
    )
    universe = build_monthly_universe(all_bars, config, timeline)
    symbol_last_ts = {symbol: bars[-1].ts for symbol, bars in all_bars.items()}
    cash = initial_cash
    positions: dict[str, Position] = {}
    last_prices: dict[str, float] = {}
    trades: list[Trade] = []
    curve: list[tuple[int, float]] = []
    total_funding = 0.0
    total_fees = 0.0
    max_gross = 0.0

    for ts in timeline:
        current: dict[str, tuple[int, Bar, Signal]] = {}
        for symbol, bars in all_bars.items():
            index = indexes[symbol].get(ts)
            if index is not None:
                current[symbol] = (index, bars[index], signals[symbol][index])
                last_prices[symbol] = bars[index].open

        # Funding at the timestamp applies to positions carried into this bar.
        for symbol, position in list(positions.items()):
            rate = all_funding[symbol].get(ts)
            if rate is None:
                continue
            mark = current[symbol][1].open if symbol in current else last_prices[symbol]
            payment = -position.side * position.qty * mark * rate
            position.funding_pnl += payment
            total_funding += payment
            cash += payment

        # Exits always have priority over new entries.
        for symbol, position in list(positions.items()):
            if symbol not in current:
                continue
            _, bar, signal = current[symbol]
            channel_exit = signal.exit_long if position.side > 0 else signal.exit_short
            reverse = signal.entry_side == -position.side
            universe_exit = (
                config.universe_top_n > 0 and symbol not in universe.get(ts, set())
            )
            trailing_exit = (
                bar.open <= position.extreme - config.stop_atr * signal.atr
                if position.side > 0
                else bar.open >= position.extreme + config.stop_atr * signal.atr
            ) if signal.atr > 0.0 else False
            if channel_exit or reverse or trailing_exit or universe_exit:
                reason = (
                    "universe"
                    if universe_exit
                    else "reverse"
                    if reverse
                    else "atr_stop"
                    if trailing_exit
                    else "channel"
                )
                trade, delta = close_position(position, ts, bar.open, reason, config)
                cash += delta
                total_fees += trade.fees - position.entry_fee
                trades.append(trade)
                del positions[symbol]

        prices_at_open = {
            symbol: (current[symbol][1].open if symbol in current else price)
            for symbol, price in last_prices.items()
        }
        equity = marked_equity(cash, positions, prices_at_open)
        gross = sum(abs(position.qty) * prices_at_open[symbol] for symbol, position in positions.items())
        capacity = max(0.0, equity * config.gross_limit - gross)
        slots = max(0, config.max_positions - len(positions))
        candidates = [
            (signal.score, symbol, bar, signal)
            for symbol, (_, bar, signal) in current.items()
            if symbol not in positions
            and signal.entry_side != 0
            and signal.atr > 0.0
            and (
                config.universe_top_n <= 0
                or symbol in universe.get(ts, set())
            )
            and (
                not config.regime_ema
                or (
                    signal.entry_side > 0
                    and breadth.get(ts, 0.5) >= config.long_breadth_min
                )
                or (
                    signal.entry_side < 0
                    and breadth.get(ts, 0.5) <= config.short_breadth_max
                )
            )
            and (
                not config.funding_filter_days
                or (
                    signal.entry_side > 0
                    and daily_funding[symbol].get(ts, 0.0)
                    <= config.max_long_daily_funding
                )
                or (
                    signal.entry_side < 0
                    and daily_funding[symbol].get(ts, 0.0)
                    >= config.min_short_daily_funding
                )
            )
        ]
        candidates.sort(reverse=True)
        for _, symbol, bar, signal in candidates:
            if slots <= 0 or capacity <= 0.0:
                break
            stop_fraction = config.stop_atr * signal.atr / bar.open
            if stop_fraction <= 0.0:
                continue
            risk_notional = equity * config.risk_per_trade / stop_fraction
            notional = min(equity * config.max_symbol_weight, risk_notional, capacity)
            if notional < 100.0:
                continue
            entry_price = execution_price(bar.open, signal.entry_side, config.slippage_bps)
            qty = notional / entry_price
            entry_fee = notional * config.fee_bps / 10_000.0
            cash -= entry_fee
            total_fees += entry_fee
            positions[symbol] = Position(
                symbol=symbol,
                side=signal.entry_side,
                qty=qty,
                entry_ts=ts,
                entry_price=entry_price,
                entry_fee=entry_fee,
                atr=signal.atr,
                extreme=bar.open,
            )
            capacity -= notional
            slots -= 1

        # Mark at close and update the trailing extreme without looking forward.
        close_prices = dict(last_prices)
        for symbol, (_, bar, _) in current.items():
            close_prices[symbol] = bar.close
            last_prices[symbol] = bar.close
        for symbol, position in list(positions.items()):
            close = close_prices.get(symbol, position.entry_price)
            position.extreme = max(position.extreme, close) if position.side > 0 else min(position.extreme, close)
            if ts == symbol_last_ts[symbol] and ts < end_ts:
                trade, delta = close_position(position, ts + BAR_MS - 1, close, "data_end", config)
                cash += delta
                total_fees += trade.fees - position.entry_fee
                trades.append(trade)
                del positions[symbol]
        equity = marked_equity(cash, positions, close_prices)
        gross = sum(abs(position.qty) * close_prices[symbol] for symbol, position in positions.items())
        max_gross = max(max_gross, gross / equity if equity > 0.0 else math.inf)
        curve.append((ts + BAR_MS - 1, equity))

    if timeline:
        final_ts = timeline[-1] + BAR_MS - 1
        for symbol, position in list(positions.items()):
            raw = last_prices[symbol]
            trade, delta = close_position(position, final_ts, raw, "end", config)
            cash += delta
            total_fees += trade.fees - position.entry_fee
            trades.append(trade)
            del positions[symbol]
        curve.append((final_ts, cash))

    by_symbol: dict[str, float] = defaultdict(float)
    for trade in trades:
        by_symbol[trade.symbol] += trade.net_pnl
    return Result(
        config=config,
        start_ts=start_ts,
        end_ts=end_ts,
        initial_cash=initial_cash,
        final_equity=cash,
        trades=trades,
        equity_curve=curve,
        funding_pnl=total_funding,
        fees=total_fees,
        max_gross=max_gross,
        by_symbol=dict(by_symbol),
    )


def summarize(result: Result) -> dict[str, object]:
    total_return = result.final_equity / result.initial_cash - 1.0
    years = max((result.end_ts - result.start_ts) / YEAR_MS, 1.0 / 365.25)
    cagr = (result.final_equity / result.initial_cash) ** (1.0 / years) - 1.0
    peak = 0.0
    max_drawdown = 0.0
    daily_last: dict[str, float] = {}
    for ts, equity in result.equity_curve:
        peak = max(peak, equity)
        if peak > 0.0:
            max_drawdown = max(max_drawdown, 1.0 - equity / peak)
        day = datetime.fromtimestamp(ts / 1_000, tz=timezone.utc).strftime("%Y-%m-%d")
        daily_last[day] = equity
    daily_returns: list[float] = []
    values = [daily_last[day] for day in sorted(daily_last)]
    for previous, current in zip(values, values[1:]):
        if previous > 0.0:
            daily_returns.append(current / previous - 1.0)
    sharpe = 0.0
    if len(daily_returns) > 2 and statistics.stdev(daily_returns) > 0.0:
        sharpe = statistics.mean(daily_returns) / statistics.stdev(daily_returns) * math.sqrt(365.0)
    wins = [trade for trade in result.trades if trade.net_pnl > 0.0]
    losses = [trade for trade in result.trades if trade.net_pnl < 0.0]
    gross_profit = sum(trade.net_pnl for trade in wins)
    gross_loss = -sum(trade.net_pnl for trade in losses)
    holding_days = [max(0.0, (trade.exit_ts - trade.entry_ts) / 86_400_000.0) for trade in result.trades]
    return {
        "name": result.config.name,
        "return_pct": total_return * 100.0,
        "cagr_pct": cagr * 100.0,
        "max_drawdown_pct": max_drawdown * 100.0,
        "sharpe": sharpe,
        "trades": len(result.trades),
        "win_rate_pct": len(wins) / len(result.trades) * 100.0 if result.trades else 0.0,
        "profit_factor": gross_profit / gross_loss if gross_loss > 0.0 else math.inf,
        "funding_pnl": result.funding_pnl,
        "fees": result.fees,
        "average_holding_days": statistics.mean(holding_days) if holding_days else 0.0,
        "long_trades": sum(trade.side > 0 for trade in result.trades),
        "short_trades": sum(trade.side < 0 for trade in result.trades),
        "max_gross_leverage": result.max_gross,
        "best_symbols": sorted(result.by_symbol.items(), key=lambda item: item[1], reverse=True)[:5],
        "worst_symbols": sorted(result.by_symbol.items(), key=lambda item: item[1])[:5],
    }


def timestamp(value: str) -> int:
    return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1_000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument(
        "--symbols",
        default="ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,AVAXUSDT,LTCUSDT,DOTUSDT,SUIUSDT,NEARUSDT,AAVEUSDT,UNIUSDT",
    )
    args = parser.parse_args()
    symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]
    all_bars: dict[str, list[Bar]] = {}
    all_funding: dict[str, dict[int, float]] = {}
    for symbol in symbols:
        bars, funding = load_symbol(args.data, symbol)
        if bars:
            all_bars[symbol] = bars
            all_funding[symbol] = funding
    configs = [
        Config(entry_window=entry, exit_window=entry // 2)
        for entry in (20, 40, 80, 120, 240, 300)
    ]
    periods = {
        "year_2021": (timestamp("2021-01-01"), timestamp("2021-12-31") + 86_399_999),
        "year_2022": (timestamp("2022-01-01"), timestamp("2022-12-31") + 86_399_999),
        "year_2023": (timestamp("2023-01-01"), timestamp("2023-12-31") + 86_399_999),
        "train_2024": (timestamp("2024-01-01"), timestamp("2024-12-31") + 86_399_999),
        "validate_2025": (timestamp("2025-01-01"), timestamp("2025-12-31") + 86_399_999),
        "oos_2026": (timestamp("2026-01-01"), timestamp("2026-07-31") + 86_399_999),
        "full": (timestamp("2021-01-01"), timestamp("2026-07-31") + 86_399_999),
    }
    report: dict[str, object] = {
        "symbols": sorted(all_bars),
        "assumptions": {
            "bar_interval": "4h",
            "fee_bps_one_way": configs[0].fee_bps,
            "slippage_bps_one_way": configs[0].slippage_bps,
            "actual_historical_funding": True,
            "signal_execution": "close signal, next open execution",
            "max_positions": configs[0].max_positions,
            "max_gross_leverage": configs[0].gross_limit,
            "risk_per_trade_pct": configs[0].risk_per_trade * 100.0,
        },
        "periods": {},
    }
    for period_name, (start, end) in periods.items():
        report["periods"][period_name] = [
            summarize(simulate(all_bars, all_funding, config, start, end)) for config in configs
        ]

    # A conservative cost-stress run for the most stable validation candidate.
    stress = Config(entry_window=40, exit_window=20, fee_bps=5.0, slippage_bps=7.0)
    report["cost_stress_D40_20"] = {
        period_name: summarize(simulate(all_bars, all_funding, stress, start, end))
        for period_name, (start, end) in periods.items()
        if period_name != "train_2024"
    }
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.out:
        args.out.write_text(text + "\n")


if __name__ == "__main__":
    main()
