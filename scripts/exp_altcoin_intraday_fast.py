#!/usr/bin/env python3
"""Causal search for a fast long/short altcoin sleeve.

The search deliberately separates model selection from evaluation:

* train: select a small exit shortlist inside each entry family;
* validation: choose one candidate per family;
* holdout/recent: report only, never tune;

Every signal is formed on a closed 15 minute candle and filled at the next
open.  Stops win same-candle ambiguity, and fees plus adverse slippage are
charged on both sides.
"""

from __future__ import annotations

import argparse
import gzip
import heapq
import json
import math
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_alpha_search import ENTRY_PROFILES, build_signals
from exp_altcoin_oi_launch import BAR_MS, Bar, Signal


DAY_MS = 86_400_000
BEIJING_OFFSET_MS = 8 * 3_600_000
PERIODS = {
    "train": ("2026-06-01", "2026-07-10"),
    "validation": ("2026-07-10", "2026-08-01"),
    "holdout": ("2026-08-01", "2026-08-15"),
    "recent": ("2026-08-15", "2026-08-20"),
}


@dataclass(frozen=True)
class ExitConfig:
    stop: float
    target: float
    max_bars: int
    trail_activation: float = math.inf
    trail_distance: float = math.inf

    @property
    def name(self) -> str:
        target = "none" if math.isinf(self.target) else f"{self.target:.3f}"
        trail = "none" if math.isinf(self.trail_activation) else f"{self.trail_activation:.3f}_{self.trail_distance:.3f}"
        return f"s{self.stop:.3f}_t{target}_h{self.max_bars}_tr{trail}"


@dataclass(frozen=True)
class Outcome:
    signal: Signal
    exit_ts: int
    net_return: float
    reason: str


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def load_bars(path: Path) -> dict[str, list[Bar]]:
    with gzip.open(path, "rt") as handle:
        payload = json.load(handle)
    source = payload["data"] if "data" in payload else payload
    output = {}
    for symbol, rows in source.items():
        bars = [Bar(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[7])) for row in rows]
        if len(bars) >= 20 * 96:
            output[symbol] = bars
    return output


def make_exits() -> list[ExitConfig]:
    values: list[ExitConfig] = []
    for stop in (0.010, 0.015, 0.020, 0.030):
        for target in (0.015, 0.020, 0.030, 0.040):
            for max_bars in (4, 8, 12):
                values.append(ExitConfig(stop, target, max_bars))
        for activation, distance in ((0.015, 0.005), (0.020, 0.005), (0.020, 0.010), (0.030, 0.010)):
            for max_bars in (8, 12):
                values.append(ExitConfig(stop, math.inf, max_bars, activation, distance))
    return values


def outcome(signal: Signal, bars: list[Bar], cfg: ExitConfig, slippage_bps: float) -> Outcome | None:
    index = int(signal.features.get("signal_index", -2)) + 1
    if index >= len(bars) or bars[index].ts != signal.execute_ts:
        return None
    fee = 5.0 / 10_000
    slip = slippage_bps / 10_000
    entry = bars[index].open * (1 + signal.side * slip)
    stop = entry * (1 - signal.side * cfg.stop)
    extreme = entry
    end = min(index + cfg.max_bars, len(bars) - 1)
    raw_exit, exit_ts, reason = bars[end].open, bars[end].ts, "time"
    for cursor in range(index, end):
        bar = bars[cursor]
        gap = bar.open <= stop if signal.side > 0 else bar.open >= stop
        hit_stop = bar.low <= stop if signal.side > 0 else bar.high >= stop
        hit_target = (
            not math.isinf(cfg.target)
            and (bar.high >= entry * (1 + cfg.target) if signal.side > 0 else bar.low <= entry * (1 - cfg.target))
        )
        if gap or hit_stop:
            raw_exit, exit_ts, reason = (bar.open if gap else stop), bar.ts, "stop"
            break
        if hit_target:
            raw_exit = entry * (1 + signal.side * cfg.target)
            exit_ts, reason = bar.ts, "target"
            break
        if signal.side > 0:
            extreme = max(extreme, bar.high)
            if extreme / entry - 1 >= cfg.trail_activation:
                stop = max(stop, extreme * (1 - cfg.trail_distance))
        else:
            extreme = min(extreme, bar.low)
            if 1 - extreme / entry >= cfg.trail_activation:
                stop = min(stop, extreme * (1 + cfg.trail_distance))
    exit_price = raw_exit * (1 - signal.side * slip)
    net = signal.side * (exit_price / entry - 1) - fee - fee * exit_price / entry
    return Outcome(signal, exit_ts, net, reason)


def simulate(
    outcomes: list[Outcome],
    start: int,
    end: int,
    gross_per_position: float,
    stress_label: str = "standard",
) -> dict[str, object]:
    equity = peak = 1_000.0
    max_drawdown = 0.0
    pending: list[tuple[int, int, str, float, float, Outcome]] = []
    active_symbols: set[str] = set()
    last_exit: dict[str, int] = {}
    trades: list[dict[str, object]] = []
    day = None
    day_start = equity
    daily_entries = 0
    serial = 0

    def settle(until: int) -> None:
        nonlocal equity, peak, max_drawdown
        while pending and pending[0][0] <= until:
            _, _, symbol, notional, pnl, item = heapq.heappop(pending)
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            active_symbols.discard(symbol)
            last_exit[symbol] = item.exit_ts
            trades.append({"symbol": symbol, "side": item.signal.side, "entry_ts": item.signal.execute_ts, "exit_ts": item.exit_ts, "pnl": pnl, "return": item.net_return, "reason": item.reason, "notional": notional})

    grouped: dict[int, list[Outcome]] = defaultdict(list)
    for item in outcomes:
        if start <= item.signal.execute_ts < end and item.exit_ts <= end:
            grouped[item.signal.execute_ts].append(item)
    blocked_daily = blocked_capacity = blocked_cooldown = 0
    for entry_ts, candidates in sorted(grouped.items()):
        settle(entry_ts)
        risk_day = (entry_ts + BEIJING_OFFSET_MS) // DAY_MS
        if risk_day != day:
            day, day_start, daily_entries = risk_day, equity, 0
        for item in sorted(candidates, key=lambda value: value.signal.score, reverse=True):
            if daily_entries >= 10 or equity <= day_start * 0.96:
                blocked_daily += 1
                continue
            if len(pending) >= 2 or item.signal.symbol in active_symbols:
                blocked_capacity += 1
                continue
            if entry_ts - last_exit.get(item.signal.symbol, -10**18) < 60 * 60_000:
                blocked_cooldown += 1
                continue
            signal_scale = float(item.signal.features.get("research_risk_scale", 1.0))
            notional = equity * gross_per_position * signal_scale
            pnl = notional * item.net_return
            serial += 1
            heapq.heappush(pending, (item.exit_ts, serial, item.signal.symbol, notional, pnl, item))
            active_symbols.add(item.signal.symbol)
            daily_entries += 1
    settle(10**30)
    pnl_values = [float(item["pnl"]) for item in trades]
    wins = [value for value in pnl_values if value > 0]
    losses = [-value for value in pnl_values if value < 0]
    days = max((end - start) / DAY_MS, 1)
    daily_counts: dict[str, int] = defaultdict(int)
    for item in trades:
        key = datetime.fromtimestamp((int(item["entry_ts"]) + BEIJING_OFFSET_MS) / 1_000, timezone.utc).date().isoformat()
        daily_counts[key] += 1
    return {
        "label": stress_label,
        "return_pct": (equity / 1_000 - 1) * 100,
        "final_equity": equity,
        "max_drawdown_pct": max_drawdown * 100,
        "trades": len(trades),
        "trades_per_day": len(trades) / days,
        "win_rate_pct": len(wins) / len(trades) * 100 if trades else 0,
        "profit_factor": sum(wins) / sum(losses) if losses else None,
        "expectancy_usd": sum(pnl_values) / len(trades) if trades else 0,
        "longs": sum(item["side"] > 0 for item in trades),
        "shorts": sum(item["side"] < 0 for item in trades),
        "stops": sum(item["reason"] == "stop" for item in trades),
        "targets": sum(item["reason"] == "target" for item in trades),
        "times": sum(item["reason"] == "time" for item in trades),
        "daily_counts": dict(sorted(daily_counts.items())),
        "blocked_daily": blocked_daily,
        "blocked_capacity": blocked_capacity,
        "blocked_cooldown": blocked_cooldown,
        "best": sorted(trades, key=lambda item: float(item["pnl"]), reverse=True)[:5],
        "worst": sorted(trades, key=lambda item: float(item["pnl"]))[:5],
    }


def causal_gate(
    outcomes: list[Outcome],
    window: int,
    min_profit_factor: float,
    side_specific: bool,
) -> list[Outcome]:
    """Enable signals only from previously completed shadow outcomes."""
    histories: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=window))
    pending: list[tuple[int, int, int, float]] = []
    selected: list[Outcome] = []
    serial = 0
    grouped: dict[int, list[Outcome]] = defaultdict(list)
    for item in outcomes:
        grouped[item.signal.execute_ts].append(item)
    for entry_ts, items in sorted(grouped.items()):
        while pending and pending[0][0] < entry_ts:
            _, _, side, value = heapq.heappop(pending)
            histories[side if side_specific else 0].append(value)
        for item in items:
            history = histories[item.signal.side if side_specific else 0]
            gains = sum(max(value, 0.0) for value in history)
            losses = sum(max(-value, 0.0) for value in history)
            profit_factor = gains / losses if losses else 99.0
            if len(history) == window and sum(history) > 0 and profit_factor >= min_profit_factor:
                selected.append(item)
        # Same-timestamp candidates cannot inform one another.
        for item in items:
            serial += 1
            heapq.heappush(pending, (item.exit_ts, serial, item.signal.side, item.net_return))
    return selected


def score(result: dict[str, object]) -> float:
    trades = int(result["trades"])
    pf = float(result["profit_factor"] or 0)
    frequency = float(result["trades_per_day"])
    if trades < 30 or not 2 <= frequency <= 12 or pf < 1:
        return -10_000
    return float(result["return_pct"]) - 0.6 * float(result["max_drawdown_pct"]) + 8 * (pf - 1) - 2 * abs(frequency - 6)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    bars_by_symbol = load_bars(Path(args.data))
    signals_by_profile: dict[str, list[Signal]] = defaultdict(list)
    for completed, (symbol, bars) in enumerate(sorted(bars_by_symbol.items()), 1):
        for name, signals in build_signals(symbol, bars).items():
            signals_by_profile[name].extend(signals)
        if completed % 25 == 0:
            print(f"features {completed}/{len(bars_by_symbol)} signals={sum(map(len, signals_by_profile.values()))}", flush=True)

    exits = make_exits()
    results: dict[str, object] = {}
    adaptive_results: dict[str, object] = {}
    winners: list[str] = []
    for entry in ENTRY_PROFILES:
        signals = signals_by_profile[entry.name]
        train_rows = []
        for cfg in exits:
            standard = [value for signal in signals if (value := outcome(signal, bars_by_symbol[signal.symbol], cfg, 5.0))]
            train = simulate(standard, *map(timestamp, PERIODS["train"]), 1.0)
            train_rows.append((score(train), cfg, standard, train))
        train_rows.sort(key=lambda row: row[0], reverse=True)
        validation_rows = []
        for _, cfg, standard, train in train_rows[:5]:
            validation = simulate(standard, *map(timestamp, PERIODS["validation"]), 1.0)
            validation_rows.append((score(validation), cfg, standard, train, validation))
        if not validation_rows:
            continue
        validation_rows.sort(key=lambda row: row[0], reverse=True)
        _, cfg, standard, train, validation = validation_rows[0]
        name = f"{entry.name}__{cfg.name}"
        stress = [value for signal in signals if (value := outcome(signal, bars_by_symbol[signal.symbol], cfg, 15.0))]
        periods = {period: simulate(standard, *map(timestamp, bounds), 1.0) for period, bounds in PERIODS.items()}
        stress_periods = {period: simulate(stress, *map(timestamp, bounds), 1.0, "15bps_slippage") for period, bounds in PERIODS.items()}
        results[name] = {"entry": asdict(entry), "exit": asdict(cfg), "train": train, "validation": validation, "periods": periods, "stress": stress_periods}
        if score(validation) > -10_000:
            winners.append(name)
        print(name, "train", round(float(train["return_pct"]), 2), "validation", round(float(validation["return_pct"]), 2), "holdout", round(float(periods["holdout"]["return_pct"]), 2), flush=True)

        # A fixed entry rule can be profitable only in certain micro-regimes.
        # Search a deliberately small, causal family of completed-outcome gates
        # using the train shortlist, then choose once on validation.
        adaptive_grid = []
        for _, candidate_cfg, candidate_standard, _ in train_rows[:8]:
            for window in (20, 40, 80):
                for minimum_pf in (1.0, 1.1, 1.2):
                    for side_specific in (False, True):
                        gated = causal_gate(candidate_standard, window, minimum_pf, side_specific)
                        gated_train = simulate(gated, *map(timestamp, PERIODS["train"]), 1.0)
                        adaptive_grid.append((score(gated_train), candidate_cfg, window, minimum_pf, side_specific, gated, gated_train))
        adaptive_grid.sort(key=lambda row: row[0], reverse=True)
        adaptive_validation = []
        for _, candidate_cfg, window, minimum_pf, side_specific, gated, gated_train in adaptive_grid[:8]:
            gated_validation = simulate(gated, *map(timestamp, PERIODS["validation"]), 1.0)
            adaptive_validation.append((score(gated_validation), candidate_cfg, window, minimum_pf, side_specific, gated, gated_train, gated_validation))
        adaptive_validation.sort(key=lambda row: row[0], reverse=True)
        if adaptive_validation:
            _, candidate_cfg, window, minimum_pf, side_specific, gated, gated_train, gated_validation = adaptive_validation[0]
            gated_periods = {period: simulate(gated, *map(timestamp, bounds), 1.0) for period, bounds in PERIODS.items()}
            candidate_stress = [value for signal in signals if (value := outcome(signal, bars_by_symbol[signal.symbol], candidate_cfg, 15.0))]
            gated_stress = causal_gate(candidate_stress, window, minimum_pf, side_specific)
            gated_stress_periods = {period: simulate(gated_stress, *map(timestamp, bounds), 1.0, "15bps_slippage") for period, bounds in PERIODS.items()}
            adaptive_name = f"{entry.name}__{candidate_cfg.name}__w{window}_pf{minimum_pf:.1f}_{'side' if side_specific else 'pooled'}"
            adaptive_results[adaptive_name] = {
                "entry": asdict(entry),
                "exit": asdict(candidate_cfg),
                "gate": {"window": window, "minimum_profit_factor": minimum_pf, "side_specific": side_specific, "positive_sum_required": True},
                "train": gated_train,
                "validation": gated_validation,
                "periods": gated_periods,
                "stress": gated_stress_periods,
            }
            print("adaptive", adaptive_name, "train", round(float(gated_train["return_pct"]), 2), "validation", round(float(gated_validation["return_pct"]), 2), "holdout", round(float(gated_periods["holdout"]["return_pct"]), 2), "recent", round(float(gated_periods["recent"]["return_pct"]), 2), flush=True)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data": str(args.data),
        "symbols": len(bars_by_symbol),
        "periods": PERIODS,
        "assumptions": {"fee_each_side_bps": 5, "slippage_each_side_bps": 5, "stress_slippage_each_side_bps": 15, "gross_per_position": 1.0, "max_positions": 2, "max_daily_entries": 10, "beijing_daily_loss_gate": 0.04, "same_symbol_cooldown_hours": 1, "intrabar": "stop_before_target"},
        "validated_candidates": winners,
        "results": results,
        "adaptive_results": adaptive_results,
    }
    with open(args.output, "w") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    print(f"report={args.output} validated={len(winners)}")


if __name__ == "__main__":
    main()
