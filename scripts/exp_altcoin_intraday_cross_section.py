#!/usr/bin/env python3
"""Exploratory walk-forward intraday cross-sectional altcoin study.

This deliberately studies a different alpha family from the production
single-name impulse trigger.  Every 4/8/12/24 hours it ranks a point-in-time
spot-tradable, medium-liquidity universe by trailing return, trades the tails
at the next 15-minute open, and exits at the next rebalance.  Candidate
parameters are selected on Jan-Apr plus May-Jun; July and August are printed
only after that selection.
"""

from __future__ import annotations

import argparse
import bisect
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
    rebalance_hours: int
    direction: str
    names: int
    liquidity: str
    stop: float


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july": ("2026-07-01", "2026-08-01"),
    "aug": ("2026-08-01", "2026-08-11"),
}


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def build_snapshots(symbol: str, bars: list[Bar], formation_hours: tuple[int, ...]) -> dict[int, list[Snapshot]]:
    volume_prefix = [0.0]
    for bar in bars:
        volume_prefix.append(volume_prefix[-1] + bar.quote_volume)
    output = {hours: [] for hours in formation_hours}
    for index in range(14 * 96, len(bars) - 98):
        dt = datetime.fromtimestamp(bars[index].ts / 1_000, timezone.utc)
        if dt.minute != 0 or dt.hour % 4:
            continue
        volume = volume_prefix[index + 1] - volume_prefix[index - 95]
        for hours in formation_hours:
            back = hours * 4
            if index < back or bars[index].ts - bars[index - back].ts > (back + 1) * BAR_MS:
                continue
            output[hours].append(
                Snapshot(
                    symbol,
                    bars[index + 1].ts,
                    index + 1,
                    bars[index].close / bars[index - back].close - 1.0,
                    volume,
                )
            )
    return output


def liquid(volume: float, tier: str) -> bool:
    if tier == "mid":
        return 10_000_000 <= volume < 150_000_000
    if tier == "all":
        return 10_000_000 <= volume
    if tier == "liquid":
        return 50_000_000 <= volume
    raise ValueError(tier)


def trade_return(
    snapshot: Snapshot,
    bars: list[Bar],
    side: int,
    hold_bars: int,
    stop: float,
    funding: tuple[list[int], list[float]] | None,
    slippage_bps: float,
) -> float:
    fee = 0.0005
    slip = slippage_bps / 10_000
    entry = bars[snapshot.index].open * (1 + side * slip)
    stop_price = entry * (1 - side * stop)
    raw_exit = bars[snapshot.index + hold_bars].open
    exit_ts = bars[snapshot.index + hold_bars].ts
    for bar in bars[snapshot.index : snapshot.index + hold_bars]:
        gap = bar.open <= stop_price if side > 0 else bar.open >= stop_price
        hit = bar.low <= stop_price if side > 0 else bar.high >= stop_price
        if gap or hit:
            raw_exit = bar.open if gap else stop_price
            exit_ts = bar.ts
            break
    exit_price = raw_exit * (1 - side * slip)
    funding_return = 0.0
    if funding is not None:
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
) -> list[tuple[int, float, int]]:
    output: list[tuple[int, float, int]] = []
    hold_bars = config.rebalance_hours * 4
    for ts, values in sorted(snapshots[config.formation_hours].items()):
        hour = datetime.fromtimestamp((ts - BAR_MS) / 1_000, timezone.utc).hour
        if hour % config.rebalance_hours:
            continue
        universe = [
            item for item in values
            if liquid(item.volume_24h, config.liquidity) and abs(item.trailing_return) <= 1.5
        ]
        if len(universe) < config.names:
            continue
        ordered = sorted(universe, key=lambda item: item.trailing_return)
        chosen: list[tuple[Snapshot, int]] = []
        if config.direction in {"short", "long_short"}:
            chosen += [(item, -1) for item in ordered[-config.names:] if item.trailing_return > 0]
        if config.direction in {"long", "long_short"}:
            chosen += [(item, 1) for item in ordered[:config.names] if item.trailing_return < 0]
        if not chosen:
            continue
        # One times equity gross, equally weighted.  This keeps comparisons
        # about alpha rather than leverage.
        basket = sum(
            trade_return(
                item,
                bars[item.symbol],
                side,
                hold_bars,
                config.stop,
                funding.get(item.symbol),
                slippage_bps,
            )
            for item, side in chosen
        ) / len(chosen)
        output.append((ts, basket, len(chosen)))
    return output


def evaluate(
    series: list[tuple[int, float, int]],
    start: int,
    end: int,
    gate: tuple[int, float] | None,
    scale: float = 1.0,
    gate_source: list[tuple[int, float, int]] | None = None,
    daily_loss_gate: float | None = None,
) -> dict[str, float | int | None]:
    history: deque[float] = deque(maxlen=gate[0] if gate else 1)
    source_values = {ts: value for ts, value, _ in (gate_source or series)}
    equity = peak = 1_000.0
    drawdown = 0.0
    values: list[float] = []
    trades = 0
    current_day: int | None = None
    day_start_equity = equity
    blocked_baskets = 0
    for ts, value, count in series:
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start_equity = equity
        gp = sum(max(item, 0.0) for item in history)
        gl = sum(max(-item, 0.0) for item in history)
        pf = gp / gl if gl else 99.0
        enabled = gate is None or (len(history) == gate[0] and pf >= gate[1] and sum(history) > 0)
        risk_enabled = daily_loss_gate is None or equity >= day_start_equity * (1.0 - daily_loss_gate)
        if start <= ts < end and enabled and risk_enabled:
            value *= scale
            equity *= max(0.01, 1 + value)
            peak = max(peak, equity)
            drawdown = max(drawdown, 1 - equity / peak)
            values.append(value)
            trades += count
        elif start <= ts < end and enabled and not risk_enabled:
            blocked_baskets += 1
        history.append(source_values.get(ts, value))
    wins = [item for item in values if item > 0]
    losses = [-item for item in values if item < 0]
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": drawdown * 100,
        "baskets": len(values),
        "trades": trades,
        "win_rate_pct": len(wins) / len(values) * 100 if values else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses and sum(losses) else None,
        "daily_gate_blocked_baskets": blocked_baskets,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-intraday-cross-section.json")
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    args = parser.parse_args()
    cache = Path(args.cache)
    symbols = sorted(
        {path.name for path in (cache / "klines").iterdir() if path.is_dir()}
        & {
            path.name
            for root in (cache / "spot-klines", cache / "spot-klines-daily")
            if root.exists()
            for path in root.iterdir()
            if path.is_dir()
        }
        - MAJORS
    )
    bars_by_symbol: dict[str, list[Bar]] = {}
    funding_by_symbol: dict[str, tuple[list[int], list[float]]] = {}
    snapshots: dict[int, dict[int, list[Snapshot]]] = {
        hours: defaultdict(list) for hours in (24, 72, 168)
    }
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        spot_paths = sorted((cache / "spot-klines" / symbol).glob("*.zip"))
        spot_paths += sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
        spot_days = {
            (bar.ts // 1_000 if bar.ts > 10**15 else bar.ts) // DAY_MS
            for bar in load_bars(spot_paths)
        }
        if len(bars) < 15 * 96 or not spot_days:
            continue
        bars_by_symbol[symbol] = bars
        funding_paths = sorted((cache / "funding" / symbol).glob("*.zip"))
        funding_times, funding_rates = load_funding(funding_paths)
        merged = dict(zip(funding_times, funding_rates))
        api_path = cache / "funding-api" / f"{symbol}-2026-08.json"
        if api_path.exists():
            try:
                api_rows = json.loads(api_path.read_text())
            except (OSError, json.JSONDecodeError):
                api_rows = []
            for row in api_rows if isinstance(api_rows, list) else []:
                try:
                    merged[int(row["fundingTime"])] = float(row["fundingRate"])
                except (KeyError, TypeError, ValueError):
                    continue
        funding_times = sorted(merged)
        funding_by_symbol[symbol] = (funding_times, [merged[ts] for ts in funding_times])
        made = build_snapshots(symbol, bars, (24, 72, 168))
        for hours, values in made.items():
            for item in values:
                if item.ts // DAY_MS in spot_days:
                    snapshots[hours][item.ts].append(item)
        if completed % 50 == 0:
            print(f"snapshots {completed}/{len(symbols)}", flush=True)

    configs = [
        Config(formation, rebalance, direction, names, liquidity, stop)
        for formation in (24, 72, 168)
        for rebalance in (4, 8, 12, 24)
        for direction in ("short", "long", "long_short")
        for names in (3, 5)
        for liquidity in ("mid", "all", "liquid")
        for stop in (0.06, 0.08, 0.12)
    ]
    gates: list[tuple[int, float] | None] = [
        None,
        (10, 1.1),
        (10, 1.2),
        (10, 1.4),
        (10, 1.6),
        (20, 1.1),
        (20, 1.2),
        (20, 1.4),
        (20, 1.6),
        (40, 1.2),
        (40, 1.4),
    ]
    train = tuple(map(timestamp, PERIODS["train"]))
    validation = tuple(map(timestamp, PERIODS["validation"]))
    development: list[dict[str, object]] = []
    series_cache: dict[Config, list[tuple[int, float, int]]] = {}
    for index, config in enumerate(configs, 1):
        series = basket_series(config, snapshots, bars_by_symbol, funding_by_symbol, args.slippage_bps)
        series_cache[config] = series
        for gate in gates:
            a = evaluate(series, *train, gate)
            b = evaluate(series, *validation, gate)
            if a["baskets"] >= 30 and b["baskets"] >= 20:
                development.append({"config": config, "gate": gate, "train": a, "validation": b})
        if index % 100 == 0:
            print(f"grid {index}/{len(configs)}", flush=True)
    eligible = [
        row for row in development
        if row["train"]["return_pct"] > 0 and row["validation"]["return_pct"] > 0
    ]
    eligible.sort(
        key=lambda row: (
            min(row["train"]["return_pct"], row["validation"]["return_pct"]),
            row["validation"]["profit_factor"] or 0.0,
        ),
        reverse=True,
    )
    finalists: list[dict[str, object]] = []
    for row in eligible[:30]:
        config = row["config"]
        gate = row["gate"]
        item = {"config": config.__dict__, "gate": gate, "train": row["train"], "validation": row["validation"]}
        for period in ("july", "aug"):
            item[period] = evaluate(
                series_cache[config],
                *tuple(map(timestamp, PERIODS[period])),
                gate,
            )
        finalists.append(item)
    reference_config = Config(24, 4, "short", 3, "mid", 0.08)
    reference_gate = (20, 1.4)
    reference_series = series_cache[reference_config]
    reference_stress = basket_series(
        reference_config,
        snapshots,
        bars_by_symbol,
        funding_by_symbol,
        15.0,
    )
    reference_no_funding = basket_series(
        reference_config,
        snapshots,
        bars_by_symbol,
        {},
        5.0,
    )
    monthly_bounds = [
        ("jan_partial", "2026-01-15", "2026-02-01"),
        ("feb", "2026-02-01", "2026-03-01"),
        ("mar", "2026-03-01", "2026-04-01"),
        ("apr", "2026-04-01", "2026-05-01"),
        ("may", "2026-05-01", "2026-06-01"),
        ("jun", "2026-06-01", "2026-07-01"),
        ("jul", "2026-07-01", "2026-08-01"),
        ("aug_partial", "2026-08-01", "2026-08-11"),
    ]
    reference = {
        "config": reference_config.__dict__,
        "gate": reference_gate,
        "months": {
            name: evaluate(reference_series, timestamp(start), timestamp(end), reference_gate)
            for name, start, end in monthly_bounds
        },
        "leverage": {
            f"gross_{scale:.2f}x": {
                period: evaluate(
                    reference_series,
                    *tuple(map(timestamp, bounds)),
                    reference_gate,
                    scale,
                )
                for period, bounds in PERIODS.items()
            }
            for scale in (0.5, 0.75, 1.0, 1.5)
        },
        "stress_15bps": {
            period: evaluate(
                reference_stress,
                *tuple(map(timestamp, bounds)),
                reference_gate,
                gate_source=reference_series,
            )
            for period, bounds in PERIODS.items()
        },
        "without_funding": {
            period: evaluate(
                reference_no_funding,
                *tuple(map(timestamp, bounds)),
                reference_gate,
                gate_source=reference_series,
            )
            for period, bounds in PERIODS.items()
        },
        "daily_gate_4pct": {
            f"gross_{scale:.2f}x": {
                period: evaluate(
                    reference_series,
                    *tuple(map(timestamp, bounds)),
                    reference_gate,
                    scale,
                    daily_loss_gate=0.04,
                )
                for period, bounds in PERIODS.items()
            }
            for scale in (0.5, 0.75, 1.0, 1.5, 2.0)
        },
    }
    robust_config = Config(168, 12, "short", 3, "mid", 0.12)
    robust_gate = (10, 1.2)
    robust_series = series_cache[robust_config]
    robust_stress = basket_series(
        robust_config,
        snapshots,
        bars_by_symbol,
        funding_by_symbol,
        15.0,
    )
    robust_reference = {
        "config": robust_config.__dict__,
        "gate": robust_gate,
        "months": {
            name: evaluate(robust_series, timestamp(start), timestamp(end), robust_gate)
            for name, start, end in monthly_bounds
        },
        "leverage": {
            f"gross_{scale:.2f}x": {
                period: evaluate(
                    robust_series,
                    *tuple(map(timestamp, bounds)),
                    robust_gate,
                    scale,
                )
                for period, bounds in PERIODS.items()
            }
            for scale in (0.5, 0.75, 1.0, 1.5)
        },
        "stress_15bps": {
            period: evaluate(
                robust_stress,
                *tuple(map(timestamp, bounds)),
                robust_gate,
                gate_source=robust_series,
            )
            for period, bounds in PERIODS.items()
        },
    }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": len(bars_by_symbol),
        "grid": len(configs) * len(gates),
        "eligible_train_validation": len(eligible),
        "finalists": finalists,
        "reference_candidate": reference,
        "robust_reference_candidate": robust_reference,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"eligible={len(eligible)} report={args.output}")


if __name__ == "__main__":
    main()
