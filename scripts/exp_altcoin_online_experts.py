#!/usr/bin/env python3
"""Causal online switching between cross-sectional momentum and reversal."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_high_turnover_cross_section import (
    Config, PERIODS, Snapshot, basket_series, build_snapshots, timestamp,
)
from exp_altcoin_oi_launch import DAY_MS, MAJORS, load_bars, load_funding
from exp_altcoin_five_optimizations import load_symbol_bars


def score(history: deque[float], method: str) -> float:
    if not history:
        return -1e9
    if method == "sum":
        return sum(history)
    gains = sum(max(value, 0.0) for value in history)
    losses = sum(max(-value, 0.0) for value in history)
    return gains / losses if losses else 99.0


def simulate(
    momentum: list[tuple[int, float, int]],
    reversal: list[tuple[int, float, int]],
    start: int,
    end: int,
    gross: float,
    window: int,
    method: str,
    positive_only: bool,
) -> dict[str, float | int | None]:
    momentum_by_ts = {ts: (value, count) for ts, value, count in momentum}
    reversal_by_ts = {ts: (value, count) for ts, value, count in reversal}
    timestamps = sorted(momentum_by_ts.keys() & reversal_by_ts.keys())
    histories = {"momentum": deque(maxlen=window), "reversal": deque(maxlen=window)}
    equity = peak = 1_000.0
    max_drawdown = 0.0
    day_start = equity
    current_day = None
    daily_latched = False
    values: list[float] = []
    trades = warmup = risk_blocked = 0
    chosen = defaultdict(int)
    for exit_ts in timestamps:
        entry_ts = exit_ts - 4 * 3_600_000
        if not start <= entry_ts < end:
            continue
        day = entry_ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start = equity
            daily_latched = False
        if equity < day_start * 0.96:
            daily_latched = True
        ready = all(len(history) == window for history in histories.values())
        scores = {name: score(history, method) for name, history in histories.items()}
        selected = max(scores, key=scores.get)
        threshold = 0.0 if method == "sum" else 1.0
        enabled = ready and (not positive_only or scores[selected] > threshold)
        if not ready:
            warmup += 1
        elif daily_latched:
            risk_blocked += 1
        elif enabled:
            raw, count = (momentum_by_ts if selected == "momentum" else reversal_by_ts)[exit_ts]
            value = raw * gross
            equity *= max(0.01, 1 + value)
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            values.append(value)
            trades += count
            chosen[selected] += 1
        histories["momentum"].append(momentum_by_ts[exit_ts][0])
        histories["reversal"].append(reversal_by_ts[exit_ts][0])
    wins = [value for value in values if value > 0]
    losses = [-value for value in values if value < 0]
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "baskets": len(values),
        "trades": trades,
        "trades_per_day": trades / days,
        "hours_per_trade": days * 24 / trades if trades else None,
        "win_rate_pct": len(wins) / len(values) * 100 if values else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses and sum(losses) else None,
        "chosen": dict(chosen),
        "warmup_baskets": warmup,
        "risk_blocked_baskets": risk_blocked,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/private/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-online-experts.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    formations = (24, 72, 168)
    symbols = sorted(
        {path.name for path in (cache / "klines").iterdir() if path.is_dir()}
        & {path.name for root in (cache / "spot-klines", cache / "spot-klines-daily") if root.exists() for path in root.iterdir() if path.is_dir()}
        - MAJORS
    )
    bars_by_symbol = {}
    funding_by_symbol = {}
    snapshots: dict[int, dict[int, list[Snapshot]]] = {hours: defaultdict(list) for hours in formations}
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        spot_paths = sorted((cache / "spot-klines" / symbol).glob("*.zip")) + sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
        spot_days = {(bar.ts // 1_000 if bar.ts > 10**15 else bar.ts) // DAY_MS for bar in load_bars(spot_paths)}
        if len(bars) < 8 * 96 or not spot_days:
            continue
        bars_by_symbol[symbol] = bars
        funding_by_symbol[symbol] = load_funding(sorted((cache / "funding" / symbol).glob("*.zip")))
        for hours, values in build_snapshots(symbol, bars, formations).items():
            for item in values:
                if item.ts // DAY_MS in spot_days:
                    snapshots[hours][item.ts].append(item)
        if completed % 100 == 0:
            print(f"loaded {completed}/{len(symbols)}", flush=True)

    train = tuple(map(timestamp, PERIODS["train"]))
    validation = tuple(map(timestamp, PERIODS["validation"]))
    development = []
    for formation in formations:
        for names in (1, 2, 3):
            for liquidity in ("mid", "liquid", "all"):
                for stop in (0.04, 0.08, 0.12):
                    base = dict(formation_hours=formation, hold_hours=4, names=names, liquidity=liquidity, stop=stop)
                    momentum_config = Config(direction="neutral_momentum", **base)
                    reversal_config = Config(direction="neutral_reversal", **base)
                    momentum = basket_series(momentum_config, snapshots, bars_by_symbol, funding_by_symbol, 5.0)
                    reversal = basket_series(reversal_config, snapshots, bars_by_symbol, funding_by_symbol, 5.0)
                    for gross in (0.5, 0.75, 1.0):
                        for window in (5, 10, 20, 40):
                            for method in ("sum", "pf"):
                                for positive_only in (False, True):
                                    a = simulate(momentum, reversal, *train, gross, window, method, positive_only)
                                    b = simulate(momentum, reversal, *validation, gross, window, method, positive_only)
                                    if a["return_pct"] > 0 and b["return_pct"] > 0 and (a["profit_factor"] or 0) > 1.03 and (b["profit_factor"] or 0) > 1.03:
                                        development.append((base, gross, window, method, positive_only, momentum, reversal, a, b))
    development.sort(key=lambda row: (min(row[7]["return_pct"], row[8]["return_pct"]), min(row[7]["profit_factor"] or 0, row[8]["profit_factor"] or 0)), reverse=True)
    finalists = []
    for base, gross, window, method, positive_only, momentum, reversal, train_result, validation_result in development[:80]:
        momentum_stress = basket_series(Config(direction="neutral_momentum", **base), snapshots, bars_by_symbol, funding_by_symbol, 15.0)
        reversal_stress = basket_series(Config(direction="neutral_reversal", **base), snapshots, bars_by_symbol, funding_by_symbol, 15.0)
        item = {"config": {**base, "gross": gross, "window": window, "method": method, "positive_only": positive_only}, "train": train_result, "validation": validation_result}
        for name, bounds in PERIODS.items():
            if name in ("train", "validation"):
                continue
            period = tuple(map(timestamp, bounds))
            item[name] = simulate(momentum, reversal, *period, gross, window, method, positive_only)
            item[f"{name}_stress_15bps"] = simulate(momentum_stress, reversal_stress, *period, gross, window, method, positive_only)
        finalists.append(item)
    report = {"generated_at": datetime.now(timezone.utc).isoformat(), "eligible_train_validation": len(development), "finalists": finalists}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"eligible={len(development)} report={args.output}")


if __name__ == "__main__":
    main()
