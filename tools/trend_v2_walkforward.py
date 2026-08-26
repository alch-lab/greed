#!/usr/bin/env python3
"""Walk-forward research for trend pullback risk management.

Selection uses train and validation only. The locked test is evaluated once
after selection. Entries occur at the next 15m open and costs are charged on
both sides; the stress replay uses 15 bps per side.
"""

from __future__ import annotations

import itertools
import json
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

import unified_compound_research as base


@dataclass(frozen=True)
class Config:
    min_return_4h: float
    min_efficiency: float
    max_extension_atr: float
    stop_fraction: float
    target_r: float
    take_fraction: float
    trail_fraction: float
    loss_cooldown_minutes: int
    profit_shield_r: float = 0.0
    max_trend_age_bars: int = 2


@dataclass(frozen=True)
class Signal:
    ts: int
    symbol: str
    side: int
    score: float
    stop_fraction: float


@dataclass
class Position:
    signal: Signal
    entry_ts: int
    entry: float
    qty: float
    original_qty: float
    stop: float
    extreme: float
    partial: bool = False
    shielded: bool = False
    realized: float = 0.0
    entry_fee: float = 0.0


def generate(cfg: Config, bars_by_symbol, start_ms: int, end_ms: int):
    signals = []
    for symbol, bars in bars_by_symbol.items():
        prefix = [0.0]
        for bar in bars:
            prefix.append(prefix[-1] + bar.quote)
        for i in range(7 * 96, len(bars) - 1):
            bar = bars[i]
            if not start_ms <= bar.ts < end_ms or bar.atr <= 0:
                continue
            return_4h = bar.close / bars[i - 16].close - 1.0
            return_12h = bar.close / bars[i - 48].close - 1.0
            side = 1 if return_4h > 0 else -1
            trend_age_bars = 0
            for cursor in range(i, 15, -1):
                historical_return_4h = (
                    bars[cursor].close / bars[cursor - 16].close - 1.0
                )
                if side * historical_return_4h < cfg.min_return_4h:
                    break
                trend_age_bars += 1
            path = sum(
                abs(bars[j].close / bars[j - 1].close - 1.0)
                for j in range(i - 15, i + 1)
            )
            efficiency = abs(return_4h) / max(path, 1e-12)
            aligned = (
                return_4h >= cfg.min_return_4h
                and return_12h > 0
                and bar.ema21 > bar.ema36
            ) if side > 0 else (
                return_4h <= -cfg.min_return_4h
                and return_12h < 0
                and bar.ema21 < bar.ema36
            )
            if not aligned or efficiency < cfg.min_efficiency:
                continue
            touched = any(value.low <= bars[i - 1].ema21 for value in bars[i - 3:i]) \
                if side > 0 else any(value.high >= bars[i - 1].ema21 for value in bars[i - 3:i])
            reclaimed = bar.close > bar.ema8 and bar.close > bars[i - 1].close \
                if side > 0 else bar.close < bar.ema8 and bar.close < bars[i - 1].close
            extension_atr = side * (bar.close - bar.ema21) / bar.atr
            hour_volume = prefix[i + 1] - prefix[i - 3]
            baseline = (prefix[i + 1] - prefix[i + 1 - 7 * 96]) / (7 * 24)
            volume_ratio = hour_volume / max(baseline, 1.0)
            if (
                not touched
                or not reclaimed
                or trend_age_bars > cfg.max_trend_age_bars
                or extension_atr > cfg.max_extension_atr
                or side * bar.imbalance < 0.0
                or volume_ratio < 0.65
            ):
                continue
            stop_fraction = cfg.stop_fraction
            score = abs(return_4h) * efficiency * volume_ratio / (1.0 + extension_atr)
            signals.append(Signal(bar.ts + base.BAR_MS, symbol, side, score, stop_fraction))
    return sorted(signals, key=lambda value: (value.ts, -value.score, value.symbol))


def summarize(trades, start_equity, equity, max_dd, start_ms, end_ms):
    gains = sum(max(0.0, row["pnl"]) for row in trades)
    losses = -sum(min(0.0, row["pnl"]) for row in trades)
    days = (end_ms - start_ms) / base.DAY_MS
    symbols = defaultdict(float)
    for row in trades:
        symbols[row["symbol"]] += row["pnl"]
    return {
        "start": start_equity,
        "end": equity,
        "return_pct": equity / start_equity - 1.0,
        "max_dd_pct": max_dd,
        "trades": len(trades),
        "trades_per_day": len(trades) / days,
        "win_rate": sum(row["pnl"] > 0 for row in trades) / len(trades) if trades else None,
        "pf": gains / losses if losses else None,
        "average_pnl_usd": sum(row["pnl"] for row in trades) / len(trades) if trades else None,
        "profitable_symbols": sum(value > 0 for value in symbols.values()),
        "traded_symbols": len(symbols),
        "top_symbol_pnl_share": max(symbols.values(), default=0.0) / max(gains, 1e-12),
    }


def simulate(cfg: Config, bars_by_symbol, signals, start_ms, end_ms, *,
             per_side_cost=0.0008, risk_per_trade=0.010):
    indexes = {
        symbol: {bar.ts: index for index, bar in enumerate(bars)}
        for symbol, bars in bars_by_symbol.items()
    }
    grouped = defaultdict(list)
    for signal in signals:
        if start_ms <= signal.ts < end_ms:
            grouped[signal.ts].append(signal)
    equity = start_equity = peak = 2000.0
    max_dd = 0.0
    positions = {}
    trades = []
    last_exit = defaultdict(lambda: -10**18)
    loss_cooldown_until = defaultdict(lambda: -10**18)
    day_start = {}
    for ts in range(start_ms - start_ms % base.BAR_MS, end_ms, base.BAR_MS):
        for symbol in list(positions):
            position = positions[symbol]
            index = indexes[symbol].get(ts)
            if index is None:
                continue
            bar = bars_by_symbol[symbol][index]
            position.extreme = max(position.extreme, bar.high) if position.signal.side > 0 \
                else min(position.extreme, bar.low)
            stop_hit = bar.low <= position.stop if position.signal.side > 0 else bar.high >= position.stop
            target = position.entry * (
                1.0 + position.signal.side * position.signal.stop_fraction * cfg.target_r
            )
            target_hit = not position.partial and (
                bar.high >= target if position.signal.side > 0 else bar.low <= target
            )
            invalid = (bar.ema8 < bar.ema21 and bar.imbalance < 0) if position.signal.side > 0 \
                else (bar.ema8 > bar.ema21 and bar.imbalance > 0)
            reason = None
            exit_price = bar.close
            if stop_hit:
                reason = "stop_or_trail"
                exit_price = position.stop * (1.0 - position.signal.side * max(0.0, per_side_cost - 0.0005))
            elif target_hit:
                quantity = position.original_qty * cfg.take_fraction
                position.realized += (
                    position.signal.side * quantity * (target - position.entry)
                    - quantity * target * per_side_cost
                )
                position.qty -= quantity
                position.partial = True
                cost_lock = position.entry * (
                    1.0 + position.signal.side * (2.0 * per_side_cost + 0.0002)
                )
                position.stop = max(position.stop, cost_lock) if position.signal.side > 0 \
                    else min(position.stop, cost_lock)
            elif invalid:
                reason = "trend_invalid"
            # Arm the pre-TP cost shield only after this whole bar has closed.
            # This avoids assuming a favorable intrabar ordering when one
            # candle crosses both the activation and the stop.
            shield = position.entry * (
                1.0
                + position.signal.side
                * position.signal.stop_fraction
                * cfg.profit_shield_r
            )
            shield_hit = (
                bar.high >= shield if position.signal.side > 0 else bar.low <= shield
            )
            if (
                cfg.profit_shield_r > 0.0
                and not position.shielded
                and not position.partial
                and reason is None
                and shield_hit
            ):
                cost_lock = position.entry * (
                    1.0 + position.signal.side * (2.0 * per_side_cost + 0.0002)
                )
                position.stop = max(position.stop, cost_lock) if position.signal.side > 0 \
                    else min(position.stop, cost_lock)
                position.shielded = True
            if position.partial and reason is None:
                trail = position.extreme * (
                    1.0 - position.signal.side * cfg.trail_fraction
                )
                position.stop = max(position.stop, trail) if position.signal.side > 0 \
                    else min(position.stop, trail)
            if reason is None:
                continue
            pnl = (
                position.realized
                + position.signal.side * position.qty * (exit_price - position.entry)
                - position.qty * exit_price * per_side_cost
                - position.entry_fee
            )
            equity += pnl
            trades.append({
                "symbol": symbol, "side": position.signal.side,
                "entry_ts": position.entry_ts, "exit_ts": ts,
                "pnl": pnl, "reason": reason,
            })
            last_exit[symbol] = ts
            if pnl < 0:
                loss_cooldown_until[symbol] = ts + cfg.loss_cooldown_minutes * 60_000
            del positions[symbol]
        peak = max(peak, equity)
        max_dd = max(max_dd, 1.0 - equity / peak)
        day = datetime.fromtimestamp(ts / 1000, timezone.utc).date().isoformat()
        day_start.setdefault(day, equity)
        if equity < day_start[day] * 0.975 or equity < peak * 0.90:
            continue
        gross = sum(value.entry * value.qty for value in positions.values())
        for signal in grouped.get(ts, []):
            if (
                len(positions) >= 3
                or signal.symbol in positions
                or ts - last_exit[signal.symbol] < 60 * 60_000
                or ts < loss_cooldown_until[signal.symbol]
            ):
                continue
            index = indexes[signal.symbol].get(ts)
            if index is None:
                continue
            notional = min(
                equity * risk_per_trade / signal.stop_fraction,
                equity * 1.50,
                max(0.0, equity * 4.0 - gross),
            )
            if notional < 100:
                continue
            bar = bars_by_symbol[signal.symbol][index]
            entry = bar.open * (1.0 + signal.side * max(0.0, per_side_cost - 0.0005))
            quantity = notional / entry
            positions[signal.symbol] = Position(
                signal, ts, entry, quantity, quantity,
                entry * (1.0 - signal.side * signal.stop_fraction),
                entry, entry_fee=notional * per_side_cost,
            )
            gross += notional
    for symbol, position in list(positions.items()):
        eligible = [bar for bar in bars_by_symbol[symbol] if bar.ts < end_ms]
        if not eligible:
            continue
        price = eligible[-1].close * (1.0 - position.signal.side * max(0.0, per_side_cost - 0.0005))
        pnl = (
            position.realized
            + position.signal.side * position.qty * (price - position.entry)
            - position.qty * price * per_side_cost
            - position.entry_fee
        )
        equity += pnl
        trades.append({
            "symbol": symbol, "side": position.signal.side,
            "entry_ts": position.entry_ts, "exit_ts": end_ms - 1,
            "pnl": pnl, "reason": "window_mark",
        })
    return trades, summarize(trades, start_equity, equity, max_dd, start_ms, end_ms)


def configs():
    for values in itertools.product(
        (0.03, 0.06), (0.45, 0.60), (5.0, 100.0), (0.0075, 0.0100),
        (1.25, 1.75, 2.0), (0.40, 0.60), (0.003, 0.005),
        (180, 360), (0.0, 1.0, 1.25, 1.5),
    ):
        yield Config(*values)


def main():
    root = Path("data/alpha-backtest/cache")
    bars = {symbol: base.load_bars(root, symbol) for symbol in base.SYMBOLS}
    periods = {
        "train": (base.ms("2026-06-01"), base.ms("2026-07-24")),
        "validation": (base.ms("2026-07-24"), base.ms("2026-08-15")),
        "locked_test": (base.ms("2026-08-15"), base.ms("2026-08-23")),
    }
    rows = []
    signal_cache = {}
    for cfg in configs():
        key = (
            cfg.min_return_4h, cfg.min_efficiency,
            cfg.max_extension_atr, cfg.stop_fraction,
        )
        signals = signal_cache.get(key)
        if signals is None:
            signals = generate(
                cfg, bars, periods["train"][0], periods["locked_test"][1]
            )
            signal_cache[key] = signals
        _, train = simulate(cfg, bars, signals, *periods["train"])
        _, validation = simulate(cfg, bars, signals, *periods["validation"])
        score = min(train["return_pct"], validation["return_pct"]) \
            - 0.5 * max(train["max_dd_pct"], validation["max_dd_pct"])
        rows.append((score, cfg, signals, train, validation))
    rows.sort(key=lambda value: value[0], reverse=True)
    eligible = [row for row in rows if
                row[3]["trades"] >= 20 and row[4]["trades"] >= 6
                and row[3]["return_pct"] > 0 and row[4]["return_pct"] > 0
                and (row[3]["pf"] or 0) > 1 and (row[4]["pf"] or 0) > 1]
    selected = eligible[0] if eligible else rows[0]
    score, cfg, signals, train, validation = selected
    test_trades, locked = simulate(cfg, bars, signals, *periods["locked_test"])
    _, stress = simulate(
        cfg, bars, signals, *periods["locked_test"], per_side_cost=0.0015
    )
    baseline_cfg = replace(cfg, profit_shield_r=0.0)
    _, locked_baseline = simulate(
        baseline_cfg, bars, signals, *periods["locked_test"]
    )
    recent_symbols = sorted(
        path.name for path in (root / "klines").iterdir()
        if path.is_dir()
        and any(path.glob("*-2026-08-2[45].zip"))
        and any(path.glob("*-2026-08-26.zip"))
    )
    recent_bars = {symbol: base.load_bars(root, symbol) for symbol in recent_symbols}
    recent_period = (base.ms("2026-08-24"), base.ms("2026-08-27"))
    recent_signals = generate(cfg, recent_bars, *recent_period)
    recent_trades, recent = simulate(cfg, recent_bars, recent_signals, *recent_period)
    _, recent_stress = simulate(
        cfg, recent_bars, recent_signals, *recent_period, per_side_cost=0.0015
    )
    _, recent_baseline = simulate(
        baseline_cfg, recent_bars, recent_signals, *recent_period
    )
    family = []
    for _, candidate_cfg, candidate_signals, _, _ in eligible:
        _, value = simulate(candidate_cfg, bars, candidate_signals, *periods["locked_test"])
        family.append(value["return_pct"])
    report = {
        "strategy": "fresh_trend_pullback_v4",
        "status": "research_candidate" if eligible else "development_failed",
        "selection": "train and validation only; locked test untouched",
        "grid": len(rows), "eligible": len(eligible),
        "selected": {"score": score, "config": asdict(cfg)},
        "train": train, "validation": validation,
        "locked_test": locked, "locked_test_15bps_per_side": stress,
        "locked_test_without_profit_shield": locked_baseline,
        "recent_2026_08_24_to_26_partial": recent,
        "recent_15bps_per_side": recent_stress,
        "recent_without_profit_shield": recent_baseline,
        "recent_symbols": recent_symbols,
        "eligible_family_locked_test": {
            "count": len(family),
            "profitable_fraction": sum(value > 0 for value in family) / len(family) if family else None,
            "median_return_pct": statistics.median(family) if family else None,
            "min_return_pct": min(family) if family else None,
            "max_return_pct": max(family) if family else None,
        },
        "locked_test_trades": test_trades,
        "recent_trades": recent_trades,
    }
    output = Path("data/alpha-backtest/trend-v2-walkforward.json")
    output.write_text(json.dumps(report, indent=2))
    print(json.dumps({key: value for key, value in report.items() if key != "locked_test_trades"}, indent=2))


if __name__ == "__main__":
    main()
