#!/usr/bin/env python3
"""Point-in-time cross-sectional altcoin reversal experiment.

At 00:15 UTC each day the model ranks the trailing return known at the close of
the 00:00 candle, enters at the next 15-minute open, and exits one day later.
The universe is rebuilt from trailing quote turnover on every rebalance.
"""

from __future__ import annotations

import argparse
import bisect
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_five_optimizations import load_symbol_bars, spot_history_symbols
from exp_altcoin_oi_launch import BAR_MS, MAJORS, Bar


@dataclass(frozen=True)
class Snapshot:
    symbol: str
    signal_ts: int
    index: int
    trailing_return: float
    volume_24h: float
    volatility_24h: float


@dataclass(frozen=True)
class Config:
    name: str
    formation_days: int
    names_per_leg: int
    direction: str
    liquidity: str
    stop: float
    gross: float = 2.0


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july_test": ("2026-07-01", "2026-08-01"),
    "aug_holdout": ("2026-08-01", "2026-08-11"),
    "full": ("2026-01-15", "2026-08-11"),
}


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def build_snapshots(symbol: str, bars: list[Bar], formation_days: int) -> list[Snapshot]:
    window = formation_days * 96
    warmup = max(14 * 96, window)
    volume_prefix = [0.0]
    for bar in bars:
        volume_prefix.append(volume_prefix[-1] + bar.quote_volume)
    output: list[Snapshot] = []
    for index in range(warmup, len(bars) - 98):
        bar = bars[index]
        dt = datetime.fromtimestamp(bar.ts / 1_000, timezone.utc)
        if dt.hour != 0 or dt.minute != 0:
            continue
        if bars[index].ts - bars[index - window].ts > (window + 1) * BAR_MS:
            continue
        volume_24h = volume_prefix[index + 1] - volume_prefix[index - 95]
        closes = [item.close for item in bars[index - 95 : index + 1]]
        returns = [right / left - 1 for left, right in zip(closes, closes[1:]) if left > 0]
        volatility = (sum(value * value for value in returns) / max(len(returns), 1)) ** 0.5
        output.append(
            Snapshot(
                symbol,
                bars[index + 1].ts,
                index + 1,
                bars[index].close / bars[index - window].close - 1,
                volume_24h,
                volatility,
            )
        )
    return output


def liquid(snapshot: Snapshot, tier: str) -> bool:
    volume = snapshot.volume_24h
    if tier == "small":
        return 5_000_000 <= volume < 50_000_000
    if tier == "mid":
        return 10_000_000 <= volume < 150_000_000
    if tier == "liquid":
        return volume >= 50_000_000
    if tier == "all":
        return volume >= 5_000_000
    raise ValueError(tier)


def trade_return(
    snapshot: Snapshot,
    bars: list[Bar],
    side: int,
    stop_fraction: float,
    slippage_bps: float,
    funding: tuple[list[int], list[float]] | None = None,
) -> tuple[float, str]:
    fee = 5.0 / 10_000
    slip = slippage_bps / 10_000
    index = snapshot.index
    entry = bars[index].open * (1 + side * slip)
    stop = entry * (1 - side * stop_fraction)
    raw_exit = bars[index + 96].open
    exit_ts = bars[index + 96].ts
    reason = "time"
    for cursor in range(index, index + 96):
        bar = bars[cursor]
        gap = bar.open <= stop if side > 0 else bar.open >= stop
        hit = bar.low <= stop if side > 0 else bar.high >= stop
        if gap or hit:
            raw_exit = bar.open if gap else stop
            exit_ts = bar.ts
            reason = "stop"
            break
    exit_price = raw_exit * (1 - side * slip)
    funding_return = 0.0
    if funding is not None:
        funding_times, funding_rates = funding
        left = bisect.bisect_right(funding_times, bars[index].ts)
        right = bisect.bisect_right(funding_times, exit_ts)
        funding_return = -side * sum(funding_rates[left:right])
    value = side * (exit_price / entry - 1) - fee - fee * exit_price / entry + funding_return
    return value, reason


def simulate(
    config: Config,
    snapshots_by_formation: dict[int, dict[int, list[Snapshot]]],
    bars_by_symbol: dict[str, list[Bar]],
    start: int,
    end: int,
    slippage_bps: float = 5.0,
) -> dict[str, object]:
    equity = 1_000.0
    peak = equity
    drawdown = 0.0
    returns: list[float] = []
    trades = 0
    stopped = 0
    active_days = 0
    equity_curve: list[float] = []
    for ts in sorted(snapshots_by_formation[config.formation_days]):
        if not start <= ts < end:
            continue
        universe = [
            item
            for item in snapshots_by_formation[config.formation_days][ts]
            if liquid(item, config.liquidity) and abs(item.trailing_return) <= 1.50
        ]
        if len(universe) < 2 * config.names_per_leg:
            continue
        ordered = sorted(universe, key=lambda item: item.trailing_return)
        chosen: list[tuple[Snapshot, int]] = []
        if config.direction in {"long", "long_short"}:
            chosen.extend((item, 1) for item in ordered[: config.names_per_leg] if item.trailing_return < 0)
        if config.direction in {"short", "long_short"}:
            chosen.extend((item, -1) for item in ordered[-config.names_per_leg :] if item.trailing_return > 0)
        if not chosen:
            continue
        active_days += 1
        weight = config.gross / len(chosen)
        day_return = 0.0
        for snapshot, side in chosen:
            value, reason = trade_return(snapshot, bars_by_symbol[snapshot.symbol], side, config.stop, slippage_bps)
            day_return += weight * value
            returns.append(weight * value)
            trades += 1
            stopped += reason == "stop"
        # A bad daily basket cannot make equity negative in this accounting.
        equity *= max(0.01, 1 + day_return)
        peak = max(peak, equity)
        drawdown = max(drawdown, 1 - equity / peak)
        equity_curve.append(equity)
    wins = [value for value in returns if value > 0]
    losses = [-value for value in returns if value < 0]
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "final_equity": equity,
        "max_drawdown_pct": drawdown * 100,
        "trades": trades,
        "active_days": active_days,
        "trades_per_active_day": trades / active_days if active_days else 0.0,
        "win_rate_pct": len(wins) / trades * 100 if trades else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses and sum(losses) > 0 else None,
        "stops": stopped,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-daily-reversal.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    allowed = spot_history_symbols()
    symbols = sorted({path.parent.name for path in (cache / "klines").glob("*/*.zip")})
    symbols = [symbol for symbol in symbols if symbol in allowed and symbol not in MAJORS]
    bars_by_symbol: dict[str, list[Bar]] = {}
    snapshots: dict[int, dict[int, list[Snapshot]]] = {
        formation: {} for formation in (1, 3, 7)
    }
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        if len(bars) < 15 * 96:
            continue
        bars_by_symbol[symbol] = bars
        for formation in snapshots:
            for item in build_snapshots(symbol, bars, formation):
                snapshots[formation].setdefault(item.signal_ts, []).append(item)
        if completed % 50 == 0:
            print(f"snapshots {completed}/{len(symbols)}", flush=True)
    configs = [
        Config(
            f"R{formation}_{direction}_N{names}_{liquidity}_S{int(stop * 100)}",
            formation,
            names,
            direction,
            liquidity,
            stop,
        )
        for formation in (1, 3, 7)
        for direction in ("long", "short", "long_short")
        for names in (1, 3, 5)
        for liquidity in ("small", "mid", "liquid", "all")
        for stop in (0.08, 0.12, 0.20)
    ]
    results: dict[str, object] = {}
    for config in configs:
        periods = {
            period: simulate(config, snapshots, bars_by_symbol, timestamp(start), timestamp(end), 5.0)
            for period, (start, end) in PERIODS.items()
        }
        results[config.name] = {"config": config.__dict__, "periods": periods}
    ranked = sorted(
        results,
        key=lambda name: (
            float(results[name]["periods"]["train"]["return_pct"])
            - 0.5 * float(results[name]["periods"]["train"]["max_drawdown_pct"])
        ),
        reverse=True,
    )
    shortlisted = ranked[:30]
    for name in shortlisted:
        config_values = results[name]["config"]
        config = Config(**config_values)
        results[name]["stress_15bps"] = simulate(
            config,
            snapshots,
            bars_by_symbol,
            timestamp(PERIODS["full"][0]),
            timestamp(PERIODS["full"][1]),
            15.0,
        )
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Binance Vision USD-M 15m klines",
        "method": "daily point-in-time cross-sectional reversal/momentum rank; next-open execution",
        "symbols": len(bars_by_symbol),
        "assumptions": {
            "initial_equity": 1_000,
            "gross_leverage": 2,
            "fee_each_side_bps": 5,
            "slippage_each_side_bps": 5,
            "stress_slippage_each_side_bps": 15,
            "holding_hours": 24,
            "intrabar": "catastrophe stop checked before time exit",
        },
        "train_ranked_top30": shortlisted,
        "results": results,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
