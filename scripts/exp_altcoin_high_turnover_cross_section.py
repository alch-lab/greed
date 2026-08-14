#!/usr/bin/env python3
"""Walk-forward search for a genuinely high-turnover altcoin sleeve.

Signals are formed from a point-in-time cross section on closed 15 minute bars,
filled at the next bar open, and charged taker fees plus adverse slippage on
both entry and exit.  Train/validation select the model; July and August are
untouched holdouts.  This is a research executable, not production trading.
"""

from __future__ import annotations

import argparse
import bisect
import heapq
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_five_optimizations import load_symbol_bars
from exp_altcoin_oi_launch import BAR_MS, DAY_MS, MAJORS, Bar, load_bars, load_funding


@dataclass(frozen=True)
class Snapshot:
    symbol: str
    ts: int
    index: int
    trailing_return: float
    volume_24h: float


@dataclass(frozen=True)
class Config:
    formation_hours: int
    hold_hours: int
    direction: str
    names: int
    liquidity: str
    stop: float


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july_holdout": ("2026-07-01", "2026-08-01"),
    "aug_holdout": ("2026-08-01", "2026-08-11"),
}


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def liquidity_ok(volume: float, tier: str) -> bool:
    if tier == "mid":
        return 10_000_000 <= volume < 150_000_000
    if tier == "liquid":
        return volume >= 50_000_000
    if tier == "all":
        return volume >= 10_000_000
    raise ValueError(tier)


def build_snapshots(symbol: str, bars: list[Bar], formations: tuple[int, ...]) -> dict[int, list[Snapshot]]:
    volume_prefix = [0.0]
    for bar in bars:
        volume_prefix.append(volume_prefix[-1] + bar.quote_volume)
    output = {hours: [] for hours in formations}
    warmup = max(max(formations) * 4, 96)
    for index in range(warmup, len(bars) - 17):
        bar = bars[index]
        dt = datetime.fromtimestamp(bar.ts / 1_000, timezone.utc)
        if dt.minute != 45:
            continue
        entry_index = index + 1
        volume = volume_prefix[index + 1] - volume_prefix[index - 95]
        for hours in formations:
            back = hours * 4
            if bar.ts - bars[index - back].ts > (back + 1) * BAR_MS:
                continue
            output[hours].append(
                Snapshot(symbol, bars[entry_index].ts, entry_index, bar.close / bars[index - back].close - 1.0, volume)
            )
    return output


def leg_return(
    snapshot: Snapshot,
    bars: list[Bar],
    side: int,
    hold_hours: int,
    stop: float,
    funding: tuple[list[int], list[float]] | None,
    slippage_bps: float,
) -> float:
    fee = 0.0005
    slip = slippage_bps / 10_000
    entry = bars[snapshot.index].open * (1 + side * slip)
    stop_price = entry * (1 - side * stop)
    final_index = min(snapshot.index + hold_hours * 4, len(bars) - 1)
    raw_exit = bars[final_index].open
    exit_ts = bars[final_index].ts
    for bar in bars[snapshot.index:final_index]:
        hit = bar.low <= stop_price if side > 0 else bar.high >= stop_price
        if hit:
            gapped = bar.open < stop_price if side > 0 else bar.open > stop_price
            raw_exit = bar.open if gapped else stop_price
            exit_ts = bar.ts
            break
    exit_price = raw_exit * (1 - side * slip)
    funding_return = 0.0
    if funding:
        times, rates = funding
        left = bisect.bisect_right(times, bars[snapshot.index].ts)
        right = bisect.bisect_right(times, exit_ts)
        funding_return = -side * sum(rates[left:right])
    return side * (exit_price / entry - 1.0) - fee - fee * exit_price / entry + funding_return


def basket_series(
    config: Config,
    snapshots: dict[int, dict[int, list[Snapshot]]],
    bars: dict[str, list[Bar]],
    funding: dict[str, tuple[list[int], list[float]]],
    slippage_bps: float,
    staggered: bool = False,
    phase_hour: int = 0,
) -> list[tuple[int, float, int]]:
    output = []
    for ts, values in sorted(snapshots[config.formation_hours].items()):
        hour = datetime.fromtimestamp(ts / 1_000, timezone.utc).hour
        if not staggered and (hour - phase_hour) % config.hold_hours:
            continue
        universe = [item for item in values if liquidity_ok(item.volume_24h, config.liquidity) and abs(item.trailing_return) <= 1.5]
        if len(universe) < config.names:
            continue
        ordered = sorted(universe, key=lambda item: item.trailing_return)
        if config.direction == "short_winners":
            selected = [(item, -1) for item in ordered[-config.names:] if item.trailing_return > 0]
        elif config.direction == "long_winners":
            selected = [(item, 1) for item in reversed(ordered[-config.names:]) if item.trailing_return > 0]
        elif config.direction == "long_losers":
            selected = [(item, 1) for item in ordered[:config.names] if item.trailing_return < 0]
        elif config.direction == "short_losers":
            selected = [(item, -1) for item in ordered[:config.names] if item.trailing_return < 0]
        elif config.direction == "neutral_reversal":
            selected = (
                [(item, -1) for item in ordered[-config.names:] if item.trailing_return > 0]
                + [(item, 1) for item in ordered[:config.names] if item.trailing_return < 0]
            )
        elif config.direction == "neutral_momentum":
            selected = (
                [(item, 1) for item in reversed(ordered[-config.names:]) if item.trailing_return > 0]
                + [(item, -1) for item in ordered[:config.names] if item.trailing_return < 0]
            )
        elif config.direction.startswith("market_momentum_"):
            threshold = float(config.direction.rsplit("_", 1)[1])
            median_return = ordered[len(ordered) // 2].trailing_return
            selected = (
                [(item, 1) for item in reversed(ordered[-config.names:]) if item.trailing_return > 0]
                if median_return >= threshold
                else [(item, -1) for item in ordered[:config.names] if item.trailing_return < 0]
            )
        elif config.direction.startswith("market_reversal_"):
            threshold = float(config.direction.rsplit("_", 1)[1])
            median_return = ordered[len(ordered) // 2].trailing_return
            selected = (
                [(item, -1) for item in ordered[-config.names:] if item.trailing_return > 0]
                if median_return >= threshold
                else [(item, 1) for item in ordered[:config.names] if item.trailing_return < 0]
            )
        else:
            raise ValueError(config.direction)
        expected_names = config.names * 2 if config.direction.startswith("neutral_") else config.names
        if len(selected) != expected_names:
            continue
        value = sum(
            leg_return(item, bars[item.symbol], side, config.hold_hours, config.stop, funding.get(item.symbol), slippage_bps)
            for item, side in selected
        ) / len(selected)
        # Portfolio PnL becomes available only when the cohort has finished.
        # Recording it at signal time lets a daily gate see four hours into the
        # future when cohorts overlap, which can manufacture apparent alpha.
        output.append((ts + config.hold_hours * 3_600_000, value, len(selected)))
    return output


def evaluate(series: list[tuple[int, float, int]], start: int, end: int, gross: float, daily_gate: float = 0.04) -> dict[str, float | int | None]:
    equity = peak = 1_000.0
    max_drawdown = 0.0
    values: list[float] = []
    trades = blocked = 0
    current_day = None
    day_start = equity
    for ts, raw_value, count in series:
        if not start <= ts < end:
            continue
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start = equity
        if equity < day_start * (1 - daily_gate):
            blocked += 1
            continue
        value = raw_value * gross
        equity *= max(0.01, 1 + value)
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1 - equity / peak)
        values.append(value)
        trades += count
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
        "blocked_baskets": blocked,
    }


def evaluate_overlapping(
    series: list[tuple[int, float, int]],
    start: int,
    end: int,
    total_gross: float,
    hold_hours: int,
    daily_gate: float = 0.04,
) -> dict[str, float | int | None]:
    """Causal portfolio accounting for overlapping hourly cohorts.

    Each cohort locks its dollar notional from equity known at entry.  Later
    losses can block only future cohorts; positions already opened still settle.
    """
    cohort_gross = total_gross / hold_hours
    equity = peak = 1_000.0
    max_drawdown = 0.0
    outcomes: list[float] = []
    pending: list[tuple[int, float, float, int]] = []
    trades = blocked = 0
    current_day = None
    day_start = equity

    def realize(until_ms: int) -> None:
        nonlocal equity, peak, max_drawdown
        while pending and pending[0][0] <= until_ms:
            _, pnl, return_on_equity, _ = heapq.heappop(pending)
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            outcomes.append(return_on_equity)

    for exit_ts, raw_return, count in series:
        entry_ts = exit_ts - hold_hours * 3_600_000
        if entry_ts < start or entry_ts >= end:
            continue
        realize(entry_ts)
        day = entry_ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start = equity
        if equity < day_start * (1 - daily_gate):
            blocked += 1
            continue
        return_on_equity = raw_return * cohort_gross
        heapq.heappush(pending, (exit_ts, equity * return_on_equity, return_on_equity, count))
        trades += count
    realize(10**30)
    wins = [value for value in outcomes if value > 0]
    losses = [-value for value in outcomes if value < 0]
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "baskets": len(outcomes),
        "trades": trades,
        "trades_per_day": trades / days,
        "hours_per_trade": days * 24 / trades if trades else None,
        "win_rate_pct": len(wins) / len(outcomes) * 100 if outcomes else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses and sum(losses) else None,
        "blocked_baskets": blocked,
    }


def evaluate_overlapping_gated(
    series: list[tuple[int, float, int]],
    start: int,
    end: int,
    total_gross: float,
    hold_hours: int,
    gate: tuple[int, float],
    cohort_count: int | None = None,
    daily_gate: float = 0.04,
) -> dict[str, float | int | None]:
    """Causal overlapping portfolio plus an always-settled shadow PF gate."""
    cohort_gross = total_gross / (cohort_count or hold_hours)
    equity = peak = 1_000.0
    max_drawdown = 0.0
    actual_pending: list[tuple[int, float, float, int]] = []
    shadow_pending: list[tuple[int, float]] = []
    history: deque[float] = deque(maxlen=gate[0])
    outcomes: list[float] = []
    trades = daily_blocked = gate_blocked = 0
    current_day = None
    day_start = equity

    def settle(until_ms: int) -> None:
        nonlocal equity, peak, max_drawdown
        while shadow_pending and shadow_pending[0][0] <= until_ms:
            _, raw_return = heapq.heappop(shadow_pending)
            history.append(raw_return)
        while actual_pending and actual_pending[0][0] <= until_ms:
            _, pnl, return_on_equity, _ = heapq.heappop(actual_pending)
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            outcomes.append(return_on_equity)

    for exit_ts, raw_return, count in series:
        entry_ts = exit_ts - hold_hours * 3_600_000
        if not start <= entry_ts < end:
            continue
        settle(entry_ts)
        day = entry_ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start = equity
        gp = sum(max(value, 0.0) for value in history)
        gl = sum(max(-value, 0.0) for value in history)
        pf = gp / gl if gl else 99.0
        enabled = len(history) == gate[0] and sum(history) > 0 and pf >= gate[1]
        heapq.heappush(shadow_pending, (exit_ts, raw_return))
        if not enabled:
            gate_blocked += 1
            continue
        if equity < day_start * (1 - daily_gate):
            daily_blocked += 1
            continue
        return_on_equity = raw_return * cohort_gross
        heapq.heappush(actual_pending, (exit_ts, equity * return_on_equity, return_on_equity, count))
        trades += count
    settle(10**30)
    wins = [value for value in outcomes if value > 0]
    losses = [-value for value in outcomes if value < 0]
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "baskets": len(outcomes),
        "trades": trades,
        "trades_per_day": trades / days,
        "hours_per_trade": days * 24 / trades if trades else None,
        "win_rate_pct": len(wins) / len(outcomes) * 100 if outcomes else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses and sum(losses) else None,
        "blocked_baskets": daily_blocked,
        "gate_blocked_baskets": gate_blocked,
    }


def evaluate_gated(
    series: list[tuple[int, float, int]],
    start: int,
    end: int,
    gross: float,
    hold_hours: int,
    gate: tuple[int, float] | None,
    daily_gate: float = 0.04,
) -> dict[str, float | int | None]:
    """Non-overlapping basket replay with an always-updating shadow PF gate."""
    history: deque[float] = deque(maxlen=gate[0] if gate else 1)
    equity = peak = 1_000.0
    max_drawdown = 0.0
    values: list[float] = []
    trades = blocked = gate_blocked = 0
    current_day = None
    day_start = equity
    for exit_ts, raw_value, count in series:
        entry_ts = exit_ts - hold_hours * 3_600_000
        if not start <= entry_ts < end:
            continue
        gp = sum(max(value, 0.0) for value in history)
        gl = sum(max(-value, 0.0) for value in history)
        pf = gp / gl if gl else 99.0
        enabled = gate is None or (len(history) == gate[0] and sum(history) > 0 and pf >= gate[1])
        day = entry_ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start = equity
        risk_enabled = equity >= day_start * (1 - daily_gate)
        if enabled and risk_enabled:
            value = raw_value * gross
            equity *= max(0.01, 1 + value)
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            values.append(value)
            trades += count
        elif enabled:
            blocked += 1
        else:
            gate_blocked += 1
        history.append(raw_value)
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
        "blocked_baskets": blocked,
        "gate_blocked_baskets": gate_blocked,
        "gate_ready": len(history) == (gate[0] if gate else len(history)),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/private/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-high-turnover.json")
    parser.add_argument("--pairs-only", action="store_true")
    parser.add_argument("--staggered-pairs-only", action="store_true")
    parser.add_argument("--phase-pairs-only", action="store_true")
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted(
        {path.name for path in (cache / "klines").iterdir() if path.is_dir()}
        & {path.name for root in (cache / "spot-klines", cache / "spot-klines-daily") if root.exists() for path in root.iterdir() if path.is_dir()}
        - MAJORS
    )
    formations = (6, 12, 24, 72, 168) if args.staggered_pairs_only or args.phase_pairs_only else (6, 12, 24, 72)
    bars_by_symbol: dict[str, list[Bar]] = {}
    funding_by_symbol: dict[str, tuple[list[int], list[float]]] = {}
    snapshots: dict[int, dict[int, list[Snapshot]]] = {hours: defaultdict(list) for hours in formations}
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        spot_paths = sorted((cache / "spot-klines" / symbol).glob("*.zip")) + sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
        spot_days = {(bar.ts // 1_000 if bar.ts > 10**15 else bar.ts) // DAY_MS for bar in load_bars(spot_paths)}
        if len(bars) < 8 * 96 or not spot_days:
            continue
        bars_by_symbol[symbol] = bars
        funding_times, funding_rates = load_funding(sorted((cache / "funding" / symbol).glob("*.zip")))
        funding_by_symbol[symbol] = (funding_times, funding_rates)
        for hours, values in build_snapshots(symbol, bars, formations).items():
            for item in values:
                if item.ts // DAY_MS in spot_days:
                    snapshots[hours][item.ts].append(item)
        if completed % 100 == 0:
            print(f"loaded {completed}/{len(symbols)}", flush=True)

    directions = (
        ()
        if args.staggered_pairs_only or args.phase_pairs_only
        else
        ("neutral_reversal", "neutral_momentum")
        if args.pairs_only
        else (
            "short_winners", "long_winners", "long_losers", "short_losers",
            "neutral_reversal", "neutral_momentum",
            "market_momentum_0.0", "market_momentum_0.005", "market_momentum_0.01",
            "market_reversal_0.0", "market_reversal_0.005", "market_reversal_0.01",
        )
    )
    configs = [
        Config(formation, hold, direction, names, liquidity, stop)
        for formation in formations
        for hold in (1, 2, 4)
        for direction in directions
        for names in (1, 2, 3)
        for liquidity in ("mid", "liquid", "all")
        for stop in (0.04, 0.08)
    ]
    train = tuple(map(timestamp, PERIODS["train"]))
    validation = tuple(map(timestamp, PERIODS["validation"]))
    development = []
    gates: tuple[tuple[int, float] | None, ...] = (
        None, (10, 1.0), (10, 1.1), (10, 1.2), (20, 1.0), (20, 1.1),
        (20, 1.2), (20, 1.4), (40, 1.0), (40, 1.1), (40, 1.2),
    )
    series_cache = {}
    for index, config in enumerate(configs, 1):
        series = basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 5.0)
        series_cache[config] = series
        for gross in (0.5, 0.75, 1.0):
            for gate in gates:
                a = evaluate_gated(series, *train, gross, config.hold_hours, gate)
                b = evaluate_gated(series, *validation, gross, config.hold_hours, gate)
                if a["trades"] >= 100 and b["trades"] >= 60:
                    development.append((config, gross, gate, a, b))
        if index % 100 == 0:
            print(f"grid {index}/{len(configs)}", flush=True)
    eligible = [row for row in development if row[3]["return_pct"] > 0 and row[4]["return_pct"] > 0 and (row[3]["profit_factor"] or 0) > 1.03 and (row[4]["profit_factor"] or 0) > 1.03]
    eligible.sort(key=lambda row: (min(row[3]["return_pct"], row[4]["return_pct"]), min(row[3]["profit_factor"] or 0, row[4]["profit_factor"] or 0)), reverse=True)
    finalists = []
    for config, gross, gate, train_result, validation_result in eligible[:300]:
        standard = series_cache[config]
        stress = basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 15.0)
        item = {"config": config.__dict__, "gross": gross, "gate": gate, "train": train_result, "validation": validation_result}
        for name, bounds in PERIODS.items():
            if name in ("train", "validation"):
                continue
            period = tuple(map(timestamp, bounds))
            item[name] = evaluate_gated(standard, *period, gross, config.hold_hours, gate)
            item[f"{name}_stress_15bps"] = evaluate_gated(stress, *period, gross, config.hold_hours, gate)
        finalists.append(item)
    # A four-hour signal need not imply four hours of silence.  Four independent
    # hourly cohorts can each hold for four hours while sharing the same gross
    # exposure.  This tests one actual entry per hour without multiplying risk.
    staggered_development = []
    staggered_configs = (
        [
            Config(formation, 4, "neutral_momentum", names, liquidity, stop)
            for formation in (24, 72, 168)
            for names in (1, 2, 3)
            for liquidity in ("mid", "liquid", "all")
            for stop in (0.04, 0.08, 0.12)
        ]
        if args.staggered_pairs_only
        else [
            Config(formation, 4, f"market_momentum_{threshold}", names, liquidity, stop)
            for formation in (12, 24, 72)
            for threshold in (0.0, 0.005, 0.01, 0.02)
            for names in (1, 2)
            for liquidity in ("liquid", "all")
            for stop in (0.04, 0.08)
        ]
    )
    staggered_gates = ((10, 1.0), (10, 1.1), (10, 1.2), (20, 1.0), (20, 1.1), (20, 1.2))
    for config in ([] if args.phase_pairs_only or (args.pairs_only and not args.staggered_pairs_only) else staggered_configs):
        series = basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 5.0, staggered=True)
        for gross in (0.5, 0.75, 1.0):
            for gate in staggered_gates:
                a = evaluate_overlapping_gated(series, *train, gross, config.hold_hours, gate)
                b = evaluate_overlapping_gated(series, *validation, gross, config.hold_hours, gate)
                if a["return_pct"] > 0 and b["return_pct"] > 0 and (a["profit_factor"] or 0) > 1.02 and (b["profit_factor"] or 0) > 1.02:
                    staggered_development.append((config, gross, gate, series, a, b))
    staggered_development.sort(key=lambda row: (min(row[4]["return_pct"], row[5]["return_pct"]), min(row[4]["profit_factor"] or 0, row[5]["profit_factor"] or 0)), reverse=True)
    staggered_finalists = []
    for config, gross, gate, series, train_result, validation_result in staggered_development[:60]:
        stress = basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 15.0, staggered=True)
        item = {"config": config.__dict__, "gross": gross, "gate": gate, "cohorts": config.hold_hours, "gross_per_cohort": gross / config.hold_hours, "train": train_result, "validation": validation_result}
        for name, bounds in PERIODS.items():
            if name in ("train", "validation"):
                continue
            period = tuple(map(timestamp, bounds))
            item[name] = evaluate_overlapping_gated(series, *period, gross, config.hold_hours, gate)
            item[f"{name}_stress_15bps"] = evaluate_overlapping_gated(stress, *period, gross, config.hold_hours, gate)
        staggered_finalists.append(item)
    phase_pair_development = []
    phase_pairs = ((0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3))
    if args.phase_pairs_only:
        phase_configs = [
            Config(formation, 4, "neutral_momentum", names, liquidity, stop)
            for formation in (24, 72, 168)
            for names in (1, 2)
            for liquidity in ("liquid", "all")
            for stop in (0.08, 0.12)
        ]
        for config in phase_configs:
            for phases in phase_pairs:
                series = sorted(
                    basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 5.0, phase_hour=phases[0])
                    + basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 5.0, phase_hour=phases[1])
                )
                for gross in (0.5, 0.75, 1.0):
                    for gate in staggered_gates:
                        a = evaluate_overlapping_gated(series, *train, gross, config.hold_hours, gate, len(phases))
                        b = evaluate_overlapping_gated(series, *validation, gross, config.hold_hours, gate, len(phases))
                        if a["return_pct"] > 0 and b["return_pct"] > 0 and (a["profit_factor"] or 0) > 1.02 and (b["profit_factor"] or 0) > 1.02:
                            phase_pair_development.append((config, phases, gross, gate, series, a, b))
    phase_pair_development.sort(key=lambda row: (min(row[5]["return_pct"], row[6]["return_pct"]), min(row[5]["profit_factor"] or 0, row[6]["profit_factor"] or 0)), reverse=True)
    phase_pair_finalists = []
    for config, phases, gross, gate, series, train_result, validation_result in phase_pair_development[:60]:
        stress = sorted(
            basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 15.0, phase_hour=phases[0])
            + basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, 15.0, phase_hour=phases[1])
        )
        item = {"config": config.__dict__, "phases": phases, "gross": gross, "gate": gate, "train": train_result, "validation": validation_result}
        for name, bounds in PERIODS.items():
            if name in ("train", "validation"):
                continue
            period = tuple(map(timestamp, bounds))
            item[name] = evaluate_overlapping_gated(series, *period, gross, config.hold_hours, gate, len(phases))
            item[f"{name}_stress_15bps"] = evaluate_overlapping_gated(stress, *period, gross, config.hold_hours, gate, len(phases))
        phase_pair_finalists.append(item)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "hourly cross-sectional rank; next-open fill; 5bps fee + 5bps slippage each side; 15bps slippage stress",
        "symbols": len(bars_by_symbol),
        "grid": len(configs) * 3,
        "eligible_train_validation": len(eligible),
        "finalists": finalists,
        "staggered_eligible_train_validation": len(staggered_development),
        "staggered_finalists": staggered_finalists,
        "phase_pair_eligible_train_validation": len(phase_pair_development),
        "phase_pair_finalists": phase_pair_finalists,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"eligible={len(eligible)} report={args.output}")


if __name__ == "__main__":
    main()
