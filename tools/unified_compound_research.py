#!/usr/bin/env python3
"""Walk-forward replay for a trend + tactical compound portfolio.

This research uses the local Binance 15m futures archive and 5m derivatives
metrics. The strategy has no fixed time exit: trend invalidation, structural
failure, targets, break-even and trailing stops decide when a position closes.
"""

from __future__ import annotations

import csv
import io
import itertools
import json
import math
import statistics
import zipfile
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

BAR_MS = 900_000
DAY_MS = 86_400_000
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "LINKUSDT", "AVAXUSDT", "SUIUSDT", "LTCUSDT", "BCHUSDT", "TRXUSDT", "DOTUSDT", "ATOMUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT", "FILUSDT", "AAVEUSDT", "UNIUSDT", "ETCUSDT", "WIFUSDT", "PEPEUSDT", "1000SHIBUSDT", "1000BONKUSDT", "FETUSDT", "RENDERUSDT", "TAOUSDT", "SEIUSDT", "TIAUSDT", "JUPUSDT", "ENAUSDT", "ONDOUSDT", "WLDUSDT", "ORDIUSDT", "PENDLEUSDT", "CRVUSDT", "MKRUSDT", "LDOUSDT", "GALAUSDT", "SANDUSDT", "MANAUSDT", "YBUSDT",
]


@dataclass
class Bar:
    ts: int; open: float; high: float; low: float; close: float; quote: float; buy_quote: float
    ema8: float = 0; ema21: float = 0; ema36: float = 0; atr: float = 0

    @property
    def imbalance(self):
        return 2 * self.buy_quote / max(self.quote, 1.0) - 1


@dataclass(frozen=True)
class Config:
    trend4: float
    efficiency: float
    ignition1: float
    volume_ratio: float
    flow: float
    stop: float
    tp_r: float
    trail: float


@dataclass
class Signal:
    ts: int; symbol: str; side: int; lane: str; score: float; anchor: float


@dataclass
class Position:
    signal: Signal; entry_ts: int; entry: float; qty: float; original_qty: float
    stop: float; extreme: float; partial: bool = False; realized: float = 0.0; fees: float = 0.0


def read_zip(path: Path):
    with zipfile.ZipFile(path) as archive:
        with archive.open(archive.namelist()[0]) as raw:
            yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"))


def load_bars(root: Path, symbol: str):
    minutes = []
    for path in sorted((root / "klines" / symbol).glob("*.zip")):
        for row in read_zip(path):
            try:
                minutes.append(Bar(int(row["open_time"]), float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]), float(row["quote_volume"]), float(row["taker_buy_quote_volume"])))
            except (KeyError, ValueError):
                pass
    minutes.sort(key=lambda value: value.ts)
    grouped = defaultdict(list)
    for bar in minutes:
        grouped[bar.ts - bar.ts % BAR_MS].append(bar)
    out = []
    for ts, values in sorted(grouped.items()):
        if len(values) != 15:
            continue
        out.append(Bar(ts, values[0].open, max(x.high for x in values), min(x.low for x in values), values[-1].close, sum(x.quote for x in values), sum(x.buy_quote for x in values)))
    out.sort(key=lambda value: value.ts)
    for period, field in ((8, "ema8"), (21, "ema21"), (36, "ema36")):
        ema = None
        alpha = 2 / (period + 1)
        for bar in out:
            ema = bar.close if ema is None else alpha * bar.close + (1 - alpha) * ema
            setattr(bar, field, ema)
    trs = []
    for index, bar in enumerate(out):
        previous = out[index - 1].close if index else bar.close
        trs.append(max(bar.high - bar.low, abs(bar.high - previous), abs(bar.low - previous)))
        bar.atr = sum(trs[max(0, index - 19):index + 1]) / min(index + 1, 20)
    return out


def load_oi(root: Path, symbol: str):
    values = {}
    for path in sorted((root / "metrics" / symbol).glob("*.zip")):
        for row in read_zip(path):
            try:
                ts = int(datetime.strptime(row["create_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
                values[ts] = float(row["sum_open_interest_value"])
            except (KeyError, ValueError):
                pass
    return values


def at_oi(values, ts):
    return values.get(ts - ts % 300_000)


def generate(cfg: Config, bars_by_symbol, oi_by_symbol, start_ms, end_ms):
    signals = []
    for symbol, bars in bars_by_symbol.items():
        oi = oi_by_symbol[symbol]
        quote_prefix = [0.0]
        for value in bars:
            quote_prefix.append(quote_prefix[-1] + value.quote)
        pending = None
        for i in range(7 * 96, len(bars) - 1):
            b = bars[i]
            if not start_ms <= b.ts < end_ms:
                continue
            ret1 = b.close / bars[i - 4].close - 1
            ret4 = b.close / bars[i - 16].close - 1
            ret12 = b.close / bars[i - 48].close - 1
            path = sum(abs(bars[j].close / bars[j - 1].close - 1) for j in range(i - 15, i + 1))
            efficiency = abs(ret4) / max(path, 1e-9)
            hour_volume = quote_prefix[i + 1] - quote_prefix[i - 3]
            baseline_hour_volume = (quote_prefix[i + 1] - quote_prefix[i + 1 - 7 * 96]) / (7 * 24)
            volume_ratio = hour_volume / max(baseline_hour_volume, 1.0)
            now_oi, old_oi = at_oi(oi, b.ts + BAR_MS - 1), at_oi(oi, b.ts - 4 * BAR_MS)
            oi_change = now_oi / old_oi - 1 if now_oi and old_oi else 0.0
            # Fast lane is a state machine: detect the impulse immediately,
            # but execute only after a later retest and an independent reclaim.
            if pending is not None:
                signal, expires, retest_seen = pending
                invalid = b.low < signal.anchor * 0.985 if signal.side > 0 else b.high > signal.anchor * 1.015
                touched = b.low <= signal.anchor * 1.010 if signal.side > 0 else b.high >= signal.anchor * 0.990
                reclaimed = b.close >= signal.anchor * 1.002 and b.close > b.open if signal.side > 0 else b.close <= signal.anchor * 0.998 and b.close < b.open
                if invalid or i > expires:
                    pending = None
                elif retest_seen and reclaimed and signal.side * b.imbalance >= 0:
                    signal.ts = b.ts + BAR_MS
                    signal.score *= 1 + abs(b.imbalance)
                    signals.append(signal)
                    pending = None
                    continue
                elif touched:
                    pending = (signal, expires, True)
            side = 1 if ret1 >= cfg.ignition1 else -1 if ret1 <= -cfg.ignition1 else 0
            prior_high = max(value.high for value in bars[i - 96:i])
            prior_low = min(value.low for value in bars[i - 96:i])
            breakout = b.close > prior_high if side > 0 else b.close < prior_low if side < 0 else False
            aligned = side * ret4 > 0 and side * ret12 >= -0.002
            if pending is None and side and breakout and aligned and volume_ratio >= cfg.volume_ratio and side * b.imbalance >= cfg.flow and oi_change >= -0.003:
                anchor = prior_high if side > 0 else prior_low
                pending = (Signal(b.ts + BAR_MS, symbol, side, "ignition", abs(ret1) * volume_ratio * (1 + max(oi_change, 0)), anchor), i + 4, False)
            # Trend lane: enter a resumed pullback, not an extended candle.
            side = 1 if ret4 >= cfg.trend4 and ret12 > 0 and b.ema21 > b.ema36 else -1 if ret4 <= -cfg.trend4 and ret12 < 0 and b.ema21 < b.ema36 else 0
            if not side or efficiency < cfg.efficiency:
                continue
            pullback_low = min(value.low for value in bars[i - 3:i])
            pullback_high = max(value.high for value in bars[i - 3:i])
            touched = pullback_low <= bars[i - 1].ema21 if side > 0 else pullback_high >= bars[i - 1].ema21
            reclaimed = b.close > b.ema8 and b.close > bars[i - 1].close if side > 0 else b.close < b.ema8 and b.close < bars[i - 1].close
            if touched and reclaimed and side * b.imbalance >= cfg.flow / 2 and volume_ratio >= 0.65:
                signals.append(Signal(b.ts + BAR_MS, symbol, side, "trend", abs(ret4) * efficiency * volume_ratio, b.ema21))
    signals.sort(key=lambda value: (value.ts, -value.score))
    return signals


def summarize(trades, start_equity, end_equity, max_dd, start_ms, end_ms):
    gains = sum(max(0.0, value["pnl"]) for value in trades)
    losses = -sum(min(0.0, value["pnl"]) for value in trades)
    days = (end_ms - start_ms) / DAY_MS
    by_lane = {}
    for lane in ("ignition", "trend"):
        rows = [value for value in trades if value["lane"] == lane]
        gp = sum(max(0, value["pnl"]) for value in rows); gl = -sum(min(0, value["pnl"]) for value in rows)
        by_lane[lane] = {"trades": len(rows), "pnl": sum(value["pnl"] for value in rows), "pf": gp / gl if gl else None}
    daily = defaultdict(float)
    for value in trades:
        daily[datetime.fromtimestamp(value["exit_ts"] / 1000, timezone.utc).date().isoformat()] += value["pnl"]
    return {"start": start_equity, "end": end_equity, "return_pct": end_equity / start_equity - 1, "max_dd_pct": max_dd, "trades": len(trades), "trades_per_day": len(trades) / days, "win_rate": sum(value["pnl"] > 0 for value in trades) / len(trades) if trades else None, "pf": gains / losses if losses else None, "by_lane": by_lane, "daily_pnl": dict(daily)}


def simulate(cfg, bars_by_symbol, signals, start_ms, end_ms, per_side_cost=0.0008):
    indexes = {symbol: {bar.ts: i for i, bar in enumerate(bars)} for symbol, bars in bars_by_symbol.items()}
    bar_map = {bar.ts: bar for bars in bars_by_symbol.values() for bar in bars}
    grouped = defaultdict(list)
    for signal in signals:
        if start_ms <= signal.ts < end_ms:
            grouped[signal.ts].append(signal)
    equity = start_equity = peak = 2000.0
    max_dd = 0.0
    positions = {}
    trades = []
    last_exit = defaultdict(lambda: -10**18)
    day_start = {}
    for ts in range(start_ms - start_ms % BAR_MS, end_ms, BAR_MS):
        # Manage existing positions first. No fixed holding deadline exists.
        for symbol in list(positions):
            pos = positions[symbol]; idx = indexes[symbol].get(ts)
            if idx is None:
                continue
            b = bars_by_symbol[symbol][idx]
            pos.extreme = max(pos.extreme, b.high) if pos.signal.side > 0 else min(pos.extreme, b.low)
            stop_hit = b.low <= pos.stop if pos.signal.side > 0 else b.high >= pos.stop
            tp = pos.entry * (1 + pos.signal.side * cfg.stop * cfg.tp_r)
            tp_hit = not pos.partial and (b.high >= tp if pos.signal.side > 0 else b.low <= tp)
            ema_invalid = (b.ema8 < b.ema21 and b.imbalance < 0) if pos.signal.side > 0 else (b.ema8 > b.ema21 and b.imbalance > 0)
            structural_fail = (b.close < pos.signal.anchor if pos.signal.side > 0 else b.close > pos.signal.anchor) and pos.signal.lane == "ignition"
            reason = None; exit_price = b.close
            if stop_hit:
                reason = "stop_or_trail"; exit_price = pos.stop * (1 - pos.signal.side * (per_side_cost - 0.0005))
            elif tp_hit:
                fraction = 0.60 if pos.signal.lane == "ignition" else 0.40
                qty = pos.original_qty * fraction
                gross = pos.signal.side * qty * (tp - pos.entry)
                fee = qty * tp * per_side_cost
                pos.realized += gross - fee; pos.qty -= qty; pos.partial = True
                cost_lock = pos.entry * (1 + pos.signal.side * (2 * per_side_cost + 0.0002))
                pos.stop = max(pos.stop, cost_lock) if pos.signal.side > 0 else min(pos.stop, cost_lock)
            elif ema_invalid or structural_fail:
                reason = "trend_invalid" if ema_invalid else "structure_invalid"
            if pos.partial and reason is None:
                trail_price = pos.extreme * (1 - pos.signal.side * cfg.trail)
                pos.stop = max(pos.stop, trail_price) if pos.signal.side > 0 else min(pos.stop, trail_price)
            if reason:
                gross = pos.signal.side * pos.qty * (exit_price - pos.entry)
                fee = pos.qty * exit_price * per_side_cost
                pnl = pos.realized + gross - fee - pos.fees
                equity += pnl
                trades.append({"symbol": symbol, "lane": pos.signal.lane, "side": pos.signal.side, "entry_ts": pos.entry_ts, "exit_ts": ts, "pnl": pnl, "reason": reason})
                last_exit[symbol] = ts; del positions[symbol]
        peak = max(peak, equity); max_dd = max(max_dd, 1 - equity / peak)
        day = datetime.fromtimestamp(ts / 1000, timezone.utc).date().isoformat()
        day_start.setdefault(day, equity)
        if equity < day_start[day] * 0.975 or equity < peak * 0.90:
            continue
        gross_open = sum(pos.entry * pos.qty for pos in positions.values())
        for signal in grouped.get(ts, []):
            if len(positions) >= 3 or signal.symbol in positions or ts - last_exit[signal.symbol] < 60 * 60_000:
                continue
            idx = indexes[signal.symbol].get(ts)
            if idx is None:
                continue
            b = bars_by_symbol[signal.symbol][idx]
            risk = 0.01 if signal.lane == "trend" else 0.0075
            notional = min(equity * risk / cfg.stop, equity * 1.50, max(0, equity * 4 - gross_open))
            if notional < 100:
                continue
            entry = b.open * (1 + signal.side * (per_side_cost - 0.0005))
            qty = notional / entry; fee = notional * per_side_cost
            positions[signal.symbol] = Position(signal, ts, entry, qty, qty, entry * (1 - signal.side * cfg.stop), entry, fees=fee)
            gross_open += notional
    # Mark remaining positions out at the boundary, without claiming future data.
    for symbol in list(positions):
        pos = positions.pop(symbol); bars = bars_by_symbol[symbol]; eligible = [b for b in bars if b.ts < end_ms]
        if not eligible: continue
        price = eligible[-1].close * (1 - pos.signal.side * (per_side_cost - 0.0005))
        gross = pos.signal.side * pos.qty * (price - pos.entry); fee = pos.qty * price * per_side_cost
        pnl = pos.realized + gross - fee - pos.fees; equity += pnl
        trades.append({"symbol": symbol, "lane": pos.signal.lane, "side": pos.signal.side, "entry_ts": pos.entry_ts, "exit_ts": end_ms - 1, "pnl": pnl, "reason": "window_mark"})
    return trades, summarize(trades, start_equity, equity, max_dd, start_ms, end_ms)


def configs():
    for values in itertools.product((0.030, 0.060), (0.45, 0.60), (0.040, 0.060), (4.0, 6.0), (0.00, 0.10), (0.010, 0.015), (1.5, 2.0), (0.0050, 0.0075)):
        yield Config(*values)


def ms(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def main():
    root = Path("data/alpha-backtest/cache")
    bars = {symbol: load_bars(root, symbol) for symbol in SYMBOLS}
    oi = {symbol: load_oi(root, symbol) for symbol in SYMBOLS}
    periods = {"train": (ms("2026-06-01"), ms("2026-07-24")), "validation": (ms("2026-07-24"), ms("2026-08-15")), "test": (ms("2026-08-15"), ms("2026-08-23"))}
    rows = []
    all_rows = []
    signal_cache = {}
    for cfg in configs():
        signal_key = (cfg.trend4, cfg.efficiency, cfg.ignition1, cfg.volume_ratio, cfg.flow)
        signals = signal_cache.get(signal_key)
        if signals is None:
            signals = generate(cfg, bars, oi, periods["train"][0], periods["test"][1])
            signal_cache[signal_key] = signals
        _, train = simulate(cfg, bars, signals, *periods["train"])
        _, validation = simulate(cfg, bars, signals, *periods["validation"])
        score = min(train["return_pct"], validation["return_pct"]) - 0.5 * max(train["max_dd_pct"], validation["max_dd_pct"])
        all_rows.append((score, cfg, signals, train, validation))
        if train["trades"] < 20 or validation["trades"] < 8 or (train["pf"] or 0) < 1.0 or (validation["pf"] or 0) < 1.0:
            continue
        rows.append((score, cfg, signals, train, validation))
    rows.sort(key=lambda value: value[0], reverse=True)
    all_rows.sort(key=lambda value: value[0], reverse=True)
    report = {
        "status": "no_candidate" if not rows else "research_candidate", "grid": 2 ** 8,
        "eligible": len(rows), "periods": {k: v for k, v in periods.items()},
        "closest": [{"config": asdict(cfg), "train": train, "validation": validation} for _, cfg, _, train, validation in all_rows[:5]],
    }
    if rows:
        _, cfg, signals, train, validation = rows[0]
        trades, test = simulate(cfg, bars, signals, *periods["test"])
        _, stress = simulate(cfg, bars, signals, *periods["test"], per_side_cost=0.0015)
        lanes = {}
        for lane in ("trend", "ignition"):
            lane_signals = [value for value in signals if value.lane == lane]
            lanes[lane] = {}
            for name, bounds in periods.items():
                _, lane_stats = simulate(cfg, bars, lane_signals, *bounds)
                lanes[lane][name] = lane_stats
            _, lane_stress = simulate(cfg, bars, lane_signals, *periods["test"], per_side_cost=0.0015)
            lanes[lane]["test_15bps_per_side"] = lane_stress
        family_tests = []
        for _, candidate_cfg, candidate_signals, _, _ in rows:
            _, candidate_test = simulate(candidate_cfg, bars, candidate_signals, *periods["test"])
            family_tests.append(candidate_test["return_pct"])
        report.update({
            "config": asdict(cfg), "train": train, "validation": validation,
            "locked_test": test, "locked_test_15bps_per_side": stress,
            "lane_ablation": lanes,
            "eligible_family_locked_test": {
                "count": len(family_tests),
                "profitable_count": sum(value > 0 for value in family_tests),
                "profitable_fraction": sum(value > 0 for value in family_tests) / len(family_tests),
                "median_return_pct": statistics.median(family_tests),
                "min_return_pct": min(family_tests),
                "max_return_pct": max(family_tests),
            },
            "test_trades": trades,
        })
    output = Path("data/alpha-backtest/unified-compound-walkforward.json")
    output.write_text(json.dumps(report, indent=2))
    print(json.dumps({k: v for k, v in report.items() if k != "test_trades"}, indent=2))


if __name__ == "__main__":
    main()
