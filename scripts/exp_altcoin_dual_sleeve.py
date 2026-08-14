#!/usr/bin/env python3
"""Final causal replay for the dual market-neutral aggressive sleeve."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_five_optimizations import load_symbol_bars
from exp_altcoin_high_turnover_cross_section import Config, Snapshot, basket_series, build_snapshots, timestamp
from exp_altcoin_oi_launch import DAY_MS, MAJORS, load_bars, load_funding


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july_holdout": ("2026-07-01", "2026-08-01"),
    "aug_holdout": ("2026-08-01", "2026-08-11"),
}

MONTHS = {
    "jan_partial": ("2026-01-15", "2026-02-01"),
    "feb": ("2026-02-01", "2026-03-01"),
    "mar": ("2026-03-01", "2026-04-01"),
    "apr": ("2026-04-01", "2026-05-01"),
    "may": ("2026-05-01", "2026-06-01"),
    "jun": ("2026-06-01", "2026-07-01"),
    "jul": ("2026-07-01", "2026-08-01"),
    "aug_partial": ("2026-08-01", "2026-08-11"),
}


def gate_enabled(history: deque[float]) -> bool:
    if len(history) < history.maxlen:
        return False
    gains = sum(max(value, 0.0) for value in history)
    losses = sum(max(-value, 0.0) for value in history)
    profit_factor = gains / losses if losses else 99.0
    return sum(history) > 0 and profit_factor >= 1.0


def simulate(
    momentum: list[tuple[int, float, int]],
    reversal: list[tuple[int, float, int]],
    start: int,
    end: int,
) -> dict[str, object]:
    momentum_by_ts = {ts: (value, count) for ts, value, count in momentum}
    reversal_by_ts = {ts: (value, count) for ts, value, count in reversal}
    timestamps = sorted(momentum_by_ts.keys() & reversal_by_ts.keys())
    histories = {"momentum": deque(maxlen=10), "reversal": deque(maxlen=10)}
    equity = peak = 1_000.0
    max_drawdown = 0.0
    current_day = None
    day_start = equity
    daily_latched = False
    returns = []
    legs = baskets = blocked = 0
    enabled_counts = defaultdict(int)
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
        enabled = {name: gate_enabled(history) for name, history in histories.items()}
        value = 0.0
        count = 0
        if not daily_latched:
            if enabled["momentum"]:
                raw, raw_count = momentum_by_ts[exit_ts]
                value += 0.50 * raw
                count += raw_count
                enabled_counts["momentum"] += 1
            if enabled["reversal"]:
                raw, raw_count = reversal_by_ts[exit_ts]
                value += 0.50 * raw
                count += raw_count
                enabled_counts["reversal"] += 1
        elif any(enabled.values()):
            blocked += 1
        if count:
            equity *= max(0.01, 1 + value)
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            returns.append(value)
            legs += count
            baskets += 1
        histories["momentum"].append(momentum_by_ts[exit_ts][0])
        histories["reversal"].append(reversal_by_ts[exit_ts][0])
    wins = [value for value in returns if value > 0]
    losses = [-value for value in returns if value < 0]
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "ending_equity": equity,
        "max_drawdown_pct": max_drawdown * 100,
        "baskets": baskets,
        "legs": legs,
        "legs_per_day": legs / days,
        "hours_per_leg": days * 24 / legs if legs else None,
        "win_rate_pct": len(wins) / len(returns) * 100 if returns else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses and sum(losses) else None,
        "enabled_baskets": dict(enabled_counts),
        "daily_blocked_baskets": blocked,
    }


def simulate_single(series: list[tuple[int, float, int]], start: int, end: int, gross: float) -> dict[str, object]:
    history: deque[float] = deque(maxlen=10)
    equity = peak = day_start = 1_000.0
    current_day = None
    max_drawdown = 0.0
    baskets = legs = wins = 0
    gross_profit = gross_loss = 0.0
    for exit_ts, raw, count in series:
        entry_ts = exit_ts - 4 * 3_600_000
        if not start <= entry_ts < end:
            continue
        day = entry_ts // DAY_MS
        if day != current_day:
            current_day, day_start = day, equity
        is_enabled = gate_enabled(history)
        if is_enabled and equity >= day_start * 0.96:
            value = gross * raw
            equity *= max(0.01, 1 + value)
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            baskets += 1
            legs += count
            wins += value > 0
            gross_profit += max(value, 0.0)
            gross_loss += max(-value, 0.0)
        history.append(raw)
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "baskets": baskets,
        "legs": legs,
        "legs_per_day": legs / days,
        "basket_win_rate_pct": wins / baskets * 100 if baskets else 0.0,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/private/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-dual-sleeve.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted(
        {path.name for path in (cache / "klines").iterdir() if path.is_dir()}
        & {path.name for root in (cache / "spot-klines", cache / "spot-klines-daily") if root.exists() for path in root.iterdir() if path.is_dir()}
        - MAJORS
    )
    bars_by_symbol = {}
    funding_by_symbol = {}
    snapshots: dict[int, dict[int, list[Snapshot]]] = {hours: defaultdict(list) for hours in (6, 72)}
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        spot_paths = sorted((cache / "spot-klines" / symbol).glob("*.zip")) + sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
        spot_days = {(bar.ts // 1_000 if bar.ts > 10**15 else bar.ts) // DAY_MS for bar in load_bars(spot_paths)}
        if len(bars) < 8 * 96 or not spot_days:
            continue
        bars_by_symbol[symbol] = bars
        funding_by_symbol[symbol] = load_funding(sorted((cache / "funding" / symbol).glob("*.zip")))
        for hours, values in build_snapshots(symbol, bars, (6, 72)).items():
            for item in values:
                if item.ts // DAY_MS in spot_days:
                    snapshots[hours][item.ts].append(item)
        if completed % 100 == 0:
            print(f"loaded {completed}/{len(symbols)}", flush=True)

    momentum_config = Config(72, 4, "neutral_momentum", 1, "liquid", 0.08)
    reversal_config = Config(6, 4, "neutral_reversal", 2, "liquid", 0.04)
    results = {}
    for slippage in (5.0, 15.0):
        momentum = basket_series(momentum_config, snapshots, bars_by_symbol, funding_by_symbol, slippage)
        reversal = basket_series(reversal_config, snapshots, bars_by_symbol, funding_by_symbol, slippage)
        label = "standard_5bps" if slippage == 5.0 else "stress_15bps"
        results[label] = {
            "periods": {name: simulate(momentum, reversal, *map(timestamp, bounds)) for name, bounds in PERIODS.items()},
            "months": {name: simulate(momentum, reversal, *map(timestamp, bounds)) for name, bounds in MONTHS.items()},
            "reversal_only": {
                f"gross_{gross:g}": {name: simulate_single(reversal, *map(timestamp, bounds), gross) for name, bounds in PERIODS.items()}
                for gross in (0.75, 1.0, 1.25, 1.5)
            },
        }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "strategy": {
            "rebalance_hours": 4,
            "momentum": {**momentum_config.__dict__, "gross": 0.50, "gate": [10, 1.0]},
            "reversal": {**reversal_config.__dict__, "gross": 0.50, "gate": [10, 1.0]},
            "daily_loss_gate": 0.04,
            "total_max_gross": 1.0,
        },
        "results": results,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
