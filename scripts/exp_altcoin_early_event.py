#!/usr/bin/env python3
"""Causal study of early, market-relative altcoin events.

Signals use only closed 15 minute bars and fill at the following open.  The
study separates the first market-relative expansion from its next-bar
confirmation and rejection, so a late 1h/4h threshold cannot leak into the
definition of an "early" entry.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


BAR_MS = 15 * 60_000
DAY_MS = 86_400_000
BJ_OFFSET_MS = 8 * 3_600_000
PERIODS = {
    "train": ("2026-05-22", "2026-07-01"),
    "validation": ("2026-07-01", "2026-08-01"),
    "holdout": ("2026-08-01", "2026-08-15"),
    "recent": ("2026-08-15", "2026-08-21"),
}


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote: float


@dataclass(frozen=True)
class Feature:
    symbol: str
    index: int
    ts: int
    r15: float
    r1: float
    prior_r1: float
    r4: float
    r24: float
    volume_ratio: float
    close_location: float
    range_pct: float


@dataclass(frozen=True)
class Signal:
    symbol: str
    index: int
    execute_ts: int
    side: int
    score: float
    family: str


@dataclass(frozen=True)
class Exit:
    stop: float
    target: float
    bars: int

    @property
    def name(self) -> str:
        return f"s{self.stop:.3f}_t{self.target:.3f}_h{self.bars}"


def stamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def load(path: Path) -> dict[str, list[Bar]]:
    with gzip.open(path, "rt") as handle:
        payload = json.load(handle)
    result: dict[str, list[Bar]] = {}
    for symbol, rows in payload.get("data", payload).items():
        bars = [
            Bar(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[7]))
            for row in rows
        ]
        if len(bars) >= 8 * 96:
            result[symbol] = bars
    return result


def features(all_bars: dict[str, list[Bar]]) -> dict[int, list[Feature]]:
    grouped: dict[int, list[Feature]] = defaultdict(list)
    for symbol, bars in all_bars.items():
        quote_prefix = [0.0]
        for bar in bars:
            quote_prefix.append(quote_prefix[-1] + bar.quote)
        for i in range(7 * 96, len(bars) - 1):
            bar = bars[i]
            if bars[i - 96].close <= 0 or bar.open <= 0:
                continue
            prior_avg = (quote_prefix[i] - quote_prefix[i - 96]) / 96
            span = bar.high - bar.low
            grouped[bar.ts].append(
                Feature(
                    symbol=symbol,
                    index=i,
                    ts=bar.ts,
                    r15=bar.close / bar.open - 1,
                    r1=bar.close / bars[i - 4].close - 1,
                    prior_r1=bars[i - 4].close / bars[i - 8].close - 1,
                    r4=bar.close / bars[i - 16].close - 1,
                    r24=bar.close / bars[i - 96].close - 1,
                    volume_ratio=bar.quote / max(prior_avg, 1e-12),
                    close_location=(bar.close - bar.low) / span if span > 0 else 0.5,
                    range_pct=span / bar.open,
                )
            )
    return grouped


def build_signals(grouped: dict[int, list[Feature]], family: str, level: int) -> list[Signal]:
    # level controls strictness without changing the conceptual alpha.
    thresholds = {
        1: (0.0075, 0.014, 1.8, 0.60),
        2: (0.0100, 0.020, 2.5, 0.67),
        3: (0.0150, 0.028, 3.5, 0.73),
    }
    min_r15, min_r1, min_volume, min_location = thresholds[level]
    output: list[Signal] = []
    prior_ignitions: dict[str, tuple[int, int, float]] = {}
    for ts, rows in sorted(grouped.items()):
        if len(rows) < 20:
            continue
        market_r15 = statistics.median(item.r15 for item in rows)
        market_r1 = statistics.median(item.r1 for item in rows)
        candidates: list[Signal] = []
        for item in rows:
            residual15 = item.r15 - market_r15
            residual1 = item.r1 - market_r1
            side = 1 if residual15 >= 0 else -1
            directional_location = item.close_location if side > 0 else 1 - item.close_location
            first = (
                min_r15 <= side * residual15 <= 0.040
                and min_r1 <= side * residual1 <= 0.075
                and side * (item.prior_r1 - market_r1) <= 0.012
                and side * item.r4 <= 0.12
                and side * item.r24 <= 0.20
                and item.volume_ratio >= min_volume
                and directional_location >= min_location
                and item.range_pct <= 0.08
            )
            previous = prior_ignitions.get(item.symbol)
            if family == "ignition" and first:
                score = side * residual15 * math.log1p(item.volume_ratio) * directional_location
                candidates.append(Signal(item.symbol, item.index, ts + BAR_MS, side, score, family))
            elif previous and previous[0] == ts - BAR_MS:
                previous_side, previous_score = previous[1], previous[2]
                continuation = previous_side * residual15
                if family == "confirm" and 0.002 <= continuation <= 0.025 and directional_location >= 0.58:
                    candidates.append(Signal(item.symbol, item.index, ts + BAR_MS, previous_side, previous_score + continuation, family))
                if family == "reject" and continuation <= -0.004:
                    reverse = -previous_side
                    reverse_location = item.close_location if reverse > 0 else 1 - item.close_location
                    if reverse_location >= 0.60:
                        candidates.append(Signal(item.symbol, item.index, ts + BAR_MS, reverse, previous_score + abs(continuation), family))
            if first:
                prior_ignitions[item.symbol] = (
                    ts,
                    side,
                    side * residual15 * math.log1p(item.volume_ratio) * directional_location,
                )
        # Only the strongest cross-sectional event at a timestamp is actionable.
        if candidates:
            output.append(max(candidates, key=lambda value: value.score))
    return output


def outcome(signal: Signal, bars: list[Bar], cfg: Exit, slip_bps: float) -> tuple[int, float, str] | None:
    i = signal.index + 1
    if i >= len(bars) or bars[i].ts != signal.execute_ts:
        return None
    fee = 5 / 10_000
    slip = slip_bps / 10_000
    entry = bars[i].open * (1 + signal.side * slip)
    stop = entry * (1 - signal.side * cfg.stop)
    target = entry * (1 + signal.side * cfg.target)
    end = min(i + cfg.bars, len(bars) - 1)
    raw_exit, exit_ts, reason = bars[end].open, bars[end].ts, "time"
    for cursor in range(i, end):
        bar = bars[cursor]
        stop_hit = bar.low <= stop if signal.side > 0 else bar.high >= stop
        target_hit = bar.high >= target if signal.side > 0 else bar.low <= target
        if stop_hit:  # conservative same-bar ambiguity
            raw_exit, exit_ts, reason = stop, bar.ts, "stop"
            break
        if target_hit:
            raw_exit, exit_ts, reason = target, bar.ts, "target"
            break
    exit_price = raw_exit * (1 - signal.side * slip)
    net = signal.side * (exit_price / entry - 1) - fee - fee * exit_price / entry
    return exit_ts, net, reason


def simulate(signals: list[Signal], all_bars: dict[str, list[Bar]], cfg: Exit, start: int, end: int, slip_bps: float = 5.0) -> dict[str, object]:
    candidates = []
    for signal in signals:
        if not start <= signal.execute_ts < end:
            continue
        result = outcome(signal, all_bars[signal.symbol], cfg, slip_bps)
        if result and result[0] < end:
            candidates.append((signal, *result))
    equity = peak = day_start = 1000.0
    active: list[tuple[int, str, float]] = []
    cooldown: dict[str, int] = {}
    day = None
    daily = 0
    trades = []
    max_dd = 0.0
    for signal, exit_ts, net, reason in sorted(candidates, key=lambda value: (value[0].execute_ts, -value[0].score)):
        still_active = []
        for pending_exit, symbol, pnl in active:
            if pending_exit <= signal.execute_ts:
                equity += pnl
                cooldown[symbol] = pending_exit
                peak = max(peak, equity)
                max_dd = max(max_dd, 1 - equity / peak)
            else:
                still_active.append((pending_exit, symbol, pnl))
        active = still_active
        risk_day = (signal.execute_ts + BJ_OFFSET_MS) // DAY_MS
        if risk_day != day:
            day, day_start, daily = risk_day, equity, 0
        if daily >= 10 or equity <= day_start * 0.96 or len(active) >= 2:
            continue
        if signal.execute_ts - cooldown.get(signal.symbol, -10**18) < 4 * 3_600_000:
            continue
        notional = equity * 0.50
        pnl = notional * net
        active.append((exit_ts, signal.symbol, pnl))
        trades.append({"symbol": signal.symbol, "side": signal.side, "entry_ts": signal.execute_ts, "exit_ts": exit_ts, "pnl": pnl, "net_return": net, "reason": reason})
        daily += 1
    for exit_ts, symbol, pnl in sorted(active):
        del exit_ts, symbol
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, 1 - equity / peak)
    values = [item["pnl"] for item in trades]
    wins = [value for value in values if value > 0]
    losses = [-value for value in values if value < 0]
    days = (end - start) / DAY_MS
    return {
        "return_pct": (equity / 1000 - 1) * 100,
        "max_drawdown_pct": max_dd * 100,
        "trades": len(trades),
        "trades_per_day": len(trades) / max(days, 1),
        "win_rate_pct": len(wins) / len(values) * 100 if values else 0,
        "profit_factor": sum(wins) / sum(losses) if losses else None,
        "trades_detail": trades,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("/private/tmp/binance-testnet-15m-20260515-0820.json.gz"))
    parser.add_argument("--output", type=Path, default=Path("/private/tmp/greed-altcoin-early-event.json"))
    args = parser.parse_args()
    all_bars = load(args.data)
    grouped = features(all_bars)
    exits = [Exit(stop, target, bars) for stop in (0.01, 0.015, 0.02) for target in (0.015, 0.02, 0.03) for bars in (2, 4, 8)]
    results = {}
    for family in ("ignition", "confirm", "reject"):
        for level in (1, 2, 3):
            signals = build_signals(grouped, family, level)
            for cfg in exits:
                key = f"{family}_{level}__{cfg.name}"
                results[key] = {
                    "family": family,
                    "level": level,
                    "exit": cfg.__dict__,
                    "periods": {
                        name: simulate(signals, all_bars, cfg, stamp(bounds[0]), stamp(bounds[1]))
                        for name, bounds in PERIODS.items()
                    },
                }
    # Rank without peeking at holdout/recent.
    eligible = []
    for name, result in results.items():
        train = result["periods"]["train"]
        valid = result["periods"]["validation"]
        if train["trades"] >= 25 and valid["trades"] >= 15 and train["return_pct"] > 0 and valid["return_pct"] > 0:
            score = valid["return_pct"] - 0.5 * valid["max_drawdown_pct"]
            eligible.append((score, name))
    ranked = [name for _, name in sorted(eligible, reverse=True)]
    output = {"periods": PERIODS, "symbols": len(all_bars), "ranked_without_holdout": ranked, "results": results}
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    summary = []
    for name in ranked[:20]:
        item = results[name]
        summary.append({"name": name, **{period: {key: item["periods"][period][key] for key in ("return_pct", "max_drawdown_pct", "trades", "trades_per_day", "win_rate_pct", "profit_factor")} for period in PERIODS}})
    print(json.dumps({"symbols": len(all_bars), "eligible": len(ranked), "top": summary}, indent=2))


if __name__ == "__main__":
    main()
