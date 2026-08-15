#!/usr/bin/env python3
"""Research adaptive risk and exits on the confirmed-impulse signal stream.

This deliberately keeps entries unchanged.  It compares causal position sizing
and protection rules over adjacent windows, using only information available at
each entry.  Historical L2 is unavailable, so a cost stress test is included.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import importlib.util
import json
import math
import pickle
import tomllib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPLAY_PATH = ROOT / "scripts/replay_altcoin_current_3d.py"
spec = importlib.util.spec_from_file_location("rp", REPLAY_PATH)
assert spec and spec.loader
rp = importlib.util.module_from_spec(spec)
import sys
sys.modules["rp"] = rp
spec.loader.exec_module(rp)


@dataclass(frozen=True)
class Variant:
    name: str
    stop_mode: str = "fixed"
    stop_atr_multiple: float = 1.0
    exit_mode: str = "fixed"
    activation_atr_multiple: float = 1.0
    trail_atr_multiple: float = 0.35
    long_scale: float = 1.0
    short_scale: float = 1.0
    dynamic_mode: str = "fixed"
    slippage_bps: float = 5.0
    entry_guard_pct: float | None = None
    volatility_target_pct: float | None = None


@dataclass
class ResearchPosition:
    signal: object
    entry_ms: int
    entry: float
    qty: float
    entry_fee: float
    entry_notional: float
    stop: float
    stop_pct: float
    activation_pct: float
    trail_pct: float
    extreme: float
    adverse: float
    mark: float
    atr_pct: float
    partial_pnl: float = 0.0
    profit_trimmed: bool = False
    protection: str = "initial_stop"


def load_cache(path: Path):
    with gzip.open(path, "rb") as handle:
        return pickle.load(handle)


def atr_pct(bars: list[object], i: int, lookback: int = 16) -> float:
    values: list[float] = []
    for j in range(max(1, i - lookback + 1), i + 1):
        previous = bars[j - 1].close
        true_range = max(
            bars[j].high - bars[j].low,
            abs(bars[j].high - previous),
            abs(bars[j].low - previous),
        )
        values.append(true_range / max(previous, 1e-12))
    return sum(values) / len(values) if values else 0.01


def replay_variant(batches, bars_15m, minute_bars, start_ms, end_ms, cfg, variant: Variant):
    minute = {symbol: {bar.ts: bar for bar in bars} for symbol, bars in minute_bars.items()}
    fifteen = {symbol: {bar.ts: bar for bar in bars} for symbol, bars in bars_15m.items()}
    fifteen_index = {symbol: {bar.ts: i for i, bar in enumerate(bars)} for symbol, bars in bars_15m.items()}
    cash = 1000.0
    fee_rate = 0.0005
    slip = variant.slippage_bps / 10_000.0
    positions: dict[str, ResearchPosition] = {}
    pending = {}
    seen: dict[str, int] = {}
    cooldown: dict[str, int] = {}
    completed_pnls: list[float] = []
    trades: list[dict[str, object]] = []
    daily_entries = 0
    day = rp.risk_day(start_ms)
    day_start_equity = cash
    daily_loss_blocked = False
    peak = cash
    max_drawdown = 0.0

    def equity() -> float:
        return cash + sum(p.signal.side * p.qty * (p.mark - p.entry) for p in positions.values())

    def partial(symbol: str, ts: int, raw: float, fraction: float) -> None:
        nonlocal cash
        p = positions[symbol]
        quantity = p.qty * fraction
        price = raw * (1.0 - p.signal.side * slip)
        entry_fee = p.entry_fee * quantity / p.qty
        exit_fee = quantity * price * fee_rate
        pnl = p.signal.side * quantity * (price - p.entry) - entry_fee - exit_fee
        cash += p.signal.side * quantity * (price - p.entry) - exit_fee
        p.qty -= quantity
        p.entry_fee -= entry_fee
        p.partial_pnl += pnl

    def close(symbol: str, ts: int, raw: float, reason: str) -> None:
        nonlocal cash
        p = positions.pop(symbol)
        price = raw * (1.0 - p.signal.side * slip)
        fee = p.qty * price * fee_rate
        final = p.signal.side * p.qty * (price - p.entry) - p.entry_fee - fee
        cash += p.signal.side * p.qty * (price - p.entry) - fee
        pnl = p.partial_pnl + final
        completed_pnls.append(pnl)
        cooldown[symbol] = ts + int(cfg["cooldown_hours"]) * 60 * rp.MINUTE_MS
        mfe = p.signal.side * (p.extreme / p.entry - 1.0)
        mae = -p.signal.side * (p.adverse / p.entry - 1.0)
        trades.append({
            "symbol": symbol,
            "side": "long" if p.signal.side > 0 else "short",
            "entry_ms": p.entry_ms,
            "exit_ms": ts,
            "notional": p.entry_notional,
            "pnl": pnl,
            "reason": reason,
            "atr_pct": p.atr_pct,
            "stop_pct": p.stop_pct,
            "activation_pct": p.activation_pct,
            "trail_pct": p.trail_pct,
            "mfe_pct": mfe,
            "mae_pct": mae,
        })

    first_bar = (start_ms // rp.BAR_MS + 1) * rp.BAR_MS
    for ts in range((start_ms // rp.MINUTE_MS) * rp.MINUTE_MS, end_ms, rp.MINUTE_MS):
        current_day = rp.risk_day(ts)
        if current_day != day:
            day = current_day
            daily_entries = 0
            day_start_equity = equity()
            daily_loss_blocked = False

        for symbol in list(positions):
            p = positions[symbol]
            bar = minute.get(symbol, {}).get(ts)
            if not bar:
                continue
            p.mark = bar.open
            stop_hit = bar.low <= p.stop if p.signal.side > 0 else bar.high >= p.stop
            if stop_hit:
                close(symbol, ts, p.stop, p.protection)
                continue
            if ts - p.entry_ms >= int(cfg["max_hold_hours"]) * 60 * rp.MINUTE_MS:
                close(symbol, ts, bar.open, "time")
                continue
            p.mark = bar.close
            p.extreme = max(p.extreme, bar.high) if p.signal.side > 0 else min(p.extreme, bar.low)
            p.adverse = min(p.adverse, bar.low) if p.signal.side > 0 else max(p.adverse, bar.high)
            mfe = p.signal.side * (p.extreme / p.entry - 1.0)
            if not p.profit_trimmed and mfe >= p.activation_pct:
                partial(symbol, ts, p.entry * (1.0 + p.signal.side * p.activation_pct), float(cfg["partial_take_profit_fraction"]))
                p = positions[symbol]
                p.profit_trimmed = True
                p.stop = p.entry
                p.protection = "partial_take_profit_break_even"
            if mfe >= p.activation_pct:
                trailing = p.extreme * (1.0 - p.signal.side * p.trail_pct)
                p.stop = max(p.stop, trailing) if p.signal.side > 0 else min(p.stop, trailing)
                p.protection = "trailing_take_profit"
                if (bar.low <= p.stop if p.signal.side > 0 else bar.high >= p.stop):
                    close(symbol, ts, p.stop, "trailing_take_profit")

        if ts >= first_bar and ts % rp.BAR_MS == 0:
            executable = []
            for symbol in list(pending):
                item = pending[symbol]
                bar = fifteen.get(symbol, {}).get(ts - rp.BAR_MS)
                if not bar or bar.ts + rp.BAR_MS - 1 <= item.last_checked_ms:
                    continue
                item.last_checked_ms = bar.ts + rp.BAR_MS - 1
                if item.last_checked_ms > item.expires_ms:
                    del pending[symbol]
                    continue
                decision = rp.pending_decision(item, bar, cfg)
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
            for signal in batches.get(ts, []):
                if signal.symbol in positions or signal.symbol in pending or seen.get(signal.symbol) == signal.signal_ms:
                    continue
                seen[signal.symbol] = signal.signal_ms
                pending[signal.symbol] = rp.Pending(signal, signal.signal_ms + int(cfg["confirmation_window_bars"]) * rp.BAR_MS, signal.signal_ms)
            executable.sort(key=lambda item: (-item.score, item.symbol))
        else:
            executable = []

        if equity() < day_start_equity * (1.0 - float(cfg["daily_loss_limit"])):
            daily_loss_blocked = True
        for signal in executable:
            if daily_loss_blocked or len(positions) >= int(cfg["max_positions"]) or daily_entries >= int(cfg["max_daily_entries"]):
                break
            if signal.symbol in positions or cooldown.get(signal.symbol, 0) > ts:
                continue
            if signal.phase == "overextended_long" and not bool(cfg.get("overextension_long_enabled", True)):
                continue
            bar = minute.get(signal.symbol, {}).get(ts)
            index = fifteen_index.get(signal.symbol, {}).get(ts - rp.BAR_MS)
            if not bar or index is None:
                continue
            entry = bar.open * (1.0 + signal.side * slip)
            entry_guard = variant.entry_guard_pct or float(cfg["max_entry_slippage_pct"])
            if signal.side * (entry / signal.price - 1.0) > entry_guard:
                continue
            volatility = atr_pct(bars_15m[signal.symbol], index)
            stop_pct = float(cfg["stop_pct"])
            activation_pct = float(cfg["trail_activation_pct"])
            trail_pct = float(cfg["trail_pct"])
            if variant.stop_mode == "atr":
                stop_pct = min(0.015, max(0.0075, volatility * variant.stop_atr_multiple))
            if variant.exit_mode == "atr":
                activation_pct = min(0.0225, max(0.012, volatility * variant.activation_atr_multiple))
                trail_pct = min(0.008, max(0.004, volatility * variant.trail_atr_multiple))
            direction_scale = variant.long_scale if signal.side > 0 else variant.short_scale
            if variant.volatility_target_pct is not None:
                direction_scale *= min(1.0, max(0.60, variant.volatility_target_pct / max(volatility, 1e-12)))
            gross_multiple = float(cfg["max_gross_multiple"])
            if variant.dynamic_mode == "last3":
                if len(completed_pnls) < 3:
                    gross_multiple = 2.0
                elif sum(completed_pnls[-3:]) > 0:
                    gross_multiple = 2.5
                else:
                    gross_multiple = 1.5
            current_equity = equity()
            gross = sum(p.entry_notional for p in positions.values())
            risk_distance = stop_pct + float(cfg["risk_execution_buffer_pct"])
            notional = min(
                current_equity * float(cfg["risk_per_trade"]) * direction_scale / risk_distance,
                max(0.0, current_equity * gross_multiple - gross),
            )
            if notional < 20:
                continue
            fee = notional * fee_rate
            quantity = notional / entry
            cash -= fee
            positions[signal.symbol] = ResearchPosition(
                signal, ts, entry, quantity, fee, notional,
                entry * (1.0 - signal.side * stop_pct), stop_pct,
                activation_pct, trail_pct, entry, entry, entry, volatility,
            )
            daily_entries += 1

        for symbol in [name for name, p in positions.items() if p.entry_ms == ts]:
            p = positions.get(symbol)
            bar = minute.get(symbol, {}).get(ts)
            if not p or not bar:
                continue
            if bar.low <= p.stop if p.signal.side > 0 else bar.high >= p.stop:
                close(symbol, ts, p.stop, "initial_stop")
            else:
                p.mark = bar.close

        value = equity()
        peak = max(peak, value)
        max_drawdown = max(max_drawdown, 1.0 - value / peak)

    for symbol in list(positions):
        close(symbol, end_ms, positions[symbol].mark, "end_mark")
    wins = sum(t["pnl"] > 0 for t in trades)
    return {
        "ending_equity": cash,
        "return_pct": (cash / 1000.0 - 1.0) * 100.0,
        "entries": len(trades),
        "wins": wins,
        "win_rate_pct": wins / len(trades) * 100.0 if trades else 0.0,
        "max_drawdown_pct": max_drawdown * 100.0,
        "profit_factor": sum(max(t["pnl"], 0) for t in trades) / max(-sum(min(t["pnl"], 0) for t in trades), 1e-12),
        "trades": trades,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", type=Path, default=Path("/private/tmp/altcoin-fast-grid-bars.pkl.gz"))
    parser.add_argument("--output", type=Path, default=Path("/private/tmp/altcoin-adaptive-risk.json"))
    parser.add_argument("--window-days", type=int, default=3, choices=(1, 2, 3))
    args = parser.parse_args()
    with (ROOT / "config/strategy-altcoin-impulse.toml").open("rb") as handle:
        cfg = tomllib.load(handle)["altcoin_impulse"]
    _, spot_symbols, _, all_bars = load_cache(args.cache)
    bars_15m = {symbol: all_bars[symbol] for symbol in spot_symbols if symbol in all_bars}
    windows = [
        (1786077480000, 1786336680000),  # Aug 8 12:38 -> Aug 11 12:38
        (1786336680000, 1786595880000),  # Aug 11 12:38 -> Aug 14 12:38
    ]
    # Use the actual ISO periods from the validated reports if timestamps above drift.
    reports = [Path("/private/tmp/altcoin-entry-grid-prior.json"), Path("/private/tmp/altcoin-entry-grid-latest.json")]
    windows = []
    from datetime import datetime
    for report in reports:
        period = json.loads(report.read_text())["period"]
        windows.append(tuple(int(datetime.fromisoformat(value).timestamp() * 1000) for value in period))
    start_ms = windows[0][0]
    end_ms = windows[-1][1]
    if args.window_days != 3:
        step = args.window_days * rp.DAY_MS
        windows = [(cursor, min(cursor + step, end_ms)) for cursor in range(start_ms, end_ms, step)]
    batches = rp.build_signal_batches(bars_15m, start_ms, end_ms, cfg)
    symbols = sorted({signal.symbol for values in batches.values() for signal in values})
    minute_cache = Path("/private/tmp/altcoin-adaptive-minute.pkl.gz")
    if minute_cache.exists():
        with gzip.open(minute_cache, "rb") as handle:
            cached_period, minute_bars = pickle.load(handle)
        if cached_period != (start_ms, end_ms) or not set(symbols) <= set(minute_bars):
            minute_bars = {}
    else:
        minute_bars = {}
    if not minute_bars:
        with ThreadPoolExecutor(max_workers=4) as pool:
            jobs = [pool.submit(rp.fetch_1m, symbol, start_ms - rp.BAR_MS, end_ms) for symbol in symbols]
            for job in as_completed(jobs):
                symbol, bars = job.result()
                minute_bars[symbol] = bars
        with gzip.open(minute_cache, "wb") as handle:
            pickle.dump(((start_ms, end_ms), minute_bars), handle)
    variants = [
        Variant("baseline"),
        Variant("atr_stop_075", stop_mode="atr", stop_atr_multiple=0.75),
        Variant("atr_stop_100", stop_mode="atr", stop_atr_multiple=1.00),
        Variant("atr_stop_125", stop_mode="atr", stop_atr_multiple=1.25),
        Variant("atr_exit_075", exit_mode="atr", activation_atr_multiple=0.75, trail_atr_multiple=0.30),
        Variant("atr_exit_100", exit_mode="atr", activation_atr_multiple=1.00, trail_atr_multiple=0.35),
        Variant("atr_stop_exit", stop_mode="atr", stop_atr_multiple=1.0, exit_mode="atr", activation_atr_multiple=1.0, trail_atr_multiple=0.35),
        Variant("long_060", long_scale=0.60),
        Variant("long_065", long_scale=0.65),
        Variant("long_070", long_scale=0.70),
        Variant("long_075", long_scale=0.75),
        Variant("long_080", long_scale=0.80),
        Variant("long_085", long_scale=0.85),
        Variant("long_090", long_scale=0.90),
        Variant("long_095", long_scale=0.95),
        Variant("short_075", short_scale=0.75),
        Variant("all_070", long_scale=0.70, short_scale=0.70),
        Variant("all_075", long_scale=0.75, short_scale=0.75),
        Variant("all_080", long_scale=0.80, short_scale=0.80),
        Variant("dynamic_last3", dynamic_mode="last3"),
        Variant("vol_size_150", volatility_target_pct=0.015),
        Variant("vol_size_200", volatility_target_pct=0.020),
        Variant("vol_size_250", volatility_target_pct=0.025),
        Variant("cost_075bps", slippage_bps=7.5),
        Variant("cost_10bps", slippage_bps=10.0),
        Variant("cost_15bps", slippage_bps=15.0),
        Variant("long_075_cost075", long_scale=0.75, slippage_bps=7.5),
        Variant("long_070_cost10", long_scale=0.70, slippage_bps=10.0),
        Variant("long_075_cost10", long_scale=0.75, slippage_bps=10.0),
        Variant("long_080_cost10", long_scale=0.80, slippage_bps=10.0),
        Variant("long_075_cost15", long_scale=0.75, slippage_bps=15.0),
        Variant("guard_050", entry_guard_pct=0.005),
        Variant("guard_075", entry_guard_pct=0.0075),
        Variant("guard_100", entry_guard_pct=0.010),
        Variant("long_075_guard_050", long_scale=0.75, entry_guard_pct=0.005),
        Variant("long_075_guard_075", long_scale=0.75, entry_guard_pct=0.0075),
        Variant("long_075_guard_100", long_scale=0.75, entry_guard_pct=0.010),
    ]
    output = {"windows": [[rp.iso(a), rp.iso(b)] for a, b in windows], "variants": {}}
    for variant in variants:
        results = [replay_variant(batches, bars_15m, minute_bars, a, b, cfg, variant) for a, b in windows]
        compound = (math.prod(1 + result["return_pct"] / 100 for result in results) - 1) * 100
        output["variants"][variant.name] = {
            "settings": variant.__dict__,
            "windows": results,
            "compound_return_pct": compound,
            "worst_window_pct": min(result["return_pct"] for result in results),
            "worst_drawdown_pct": max(result["max_drawdown_pct"] for result in results),
            "entries": sum(result["entries"] for result in results),
        }
        print(variant.name, round(compound, 3), round(output["variants"][variant.name]["worst_window_pct"], 3), round(output["variants"][variant.name]["worst_drawdown_pct"], 3), output["variants"][variant.name]["entries"], flush=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
