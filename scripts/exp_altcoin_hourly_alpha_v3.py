#!/usr/bin/env python3
"""Search hourly altcoin alpha families on the recent broad-universe cache.

This is deliberately separate from the production breakout state machine.  It
tests fresh acceleration, first-pullback continuation and exhaustion reversal
with causal hourly decisions, next-bar fills, fees/slippage, conservative
intrabar stop ordering, overlapping positions and a daily loss gate.
"""

from __future__ import annotations

import argparse
import gzip
import importlib.util
import itertools
import json
import math
import pickle
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


BAR_MS = 15 * 60_000
DAY_MS = 86_400_000
BEIJING_OFFSET_MS = 8 * 3_600_000
MAJORS = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "TRXUSDT", "LINKUSDT", "BCHUSDT", "LTCUSDT",
}


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float


@dataclass(frozen=True)
class Feature:
    symbol: str
    ts: int
    index: int
    close: float
    r15: float
    r30: float
    r1: float
    r2: float
    r4: float
    r8: float
    r24: float
    volume_ratio: float
    volume_24h: float
    efficiency: float
    location: float
    draw_from_high_1h: float
    bounce_from_low_1h: float
    prior_r1: float
    market_r1: float = 0.0
    market_r4: float = 0.0


@dataclass(frozen=True)
class Config:
    family: str
    hold_bars: int
    stop: float
    take: float
    min_volume_24h: float
    min_volume_ratio: float
    threshold_a: float
    threshold_b: float
    threshold_c: float
    trail: float = 0.0


def load_cache(path: Path, pairs_only: bool) -> dict[str, list[Bar]]:
    source = Path(__file__).with_name("replay_altcoin_current_3d.py")
    spec = importlib.util.spec_from_file_location("rp", source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    with gzip.open(path, "rb") as handle:
        _, spot_symbols, _, raw = pickle.load(handle)
    allowed = set(spot_symbols) if pairs_only else set(raw)
    output: dict[str, list[Bar]] = {}
    for symbol in sorted((set(raw) & allowed) - MAJORS):
        values = [Bar(x.ts, x.open, x.high, x.low, x.close, x.quote_volume) for x in raw[symbol]]
        values.sort(key=lambda x: x.ts)
        if len(values) >= 8 * 96:
            output[symbol] = values
    return output


def load_history(path: Path) -> dict[str, list[Bar]]:
    source = Path(__file__).with_name("exp_altcoin_oi_launch.py")
    spec = importlib.util.spec_from_file_location("oi_hourly_v3", source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    output: dict[str, list[Bar]] = {}
    for directory in sorted((path / "klines").iterdir()):
        if not directory.is_dir() or directory.name in MAJORS:
            continue
        values = module.load_bars(sorted(directory.glob("*.zip")))
        if len(values) >= 8 * 96:
            output[directory.name] = [
                Bar(x.ts, x.open, x.high, x.low, x.close, x.quote_volume) for x in values
            ]
    return output


def feature_rows(bars_by_symbol: dict[str, list[Bar]]) -> dict[int, list[Feature]]:
    rows: dict[int, list[Feature]] = defaultdict(list)
    for symbol, bars in bars_by_symbol.items():
        prefix = [0.0]
        hourly_volumes: list[float] = []
        for bar in bars:
            prefix.append(prefix[-1] + bar.quote_volume)
        # Two fully closed days are enough for the hourly volume baseline in
        # this recent-cache experiment.  The longer historical validation run
        # uses a much larger warm-up separately.
        for i in range(2 * 96, len(bars) - 9):
            # One decision per hour, at the close of xx:45.  Entry is the next
            # bar open, so no unfinished candle enters the feature vector.
            if datetime.fromtimestamp(bars[i].ts / 1000, timezone.utc).minute != 45:
                continue
            required = (1, 2, 4, 8, 16, 32, 96)
            if any(bars[i].ts - bars[i - back].ts > (back + 1) * BAR_MS for back in required):
                continue
            current_hour_volume = prefix[i + 1] - prefix[i - 3]
            historical = []
            for end in range(i - 2 * 96, i, 4):
                if end >= 3:
                    historical.append(prefix[end + 1] - prefix[end - 3])
            median_volume = statistics.median(historical) if historical else 0.0
            volume_ratio = current_hour_volume / max(median_volume, 1.0)
            volume_24h = prefix[i + 1] - prefix[i - 95]
            path = bars[i - 4 : i + 1]
            travelled = sum(abs(b.close - a.close) for a, b in zip(path, path[1:]))
            move = bars[i].close - bars[i - 4].close
            candle_range = bars[i].high - bars[i].low
            rows[bars[i + 1].ts].append(Feature(
                symbol=symbol,
                ts=bars[i + 1].ts,
                index=i + 1,
                close=bars[i].close,
                r15=bars[i].close / bars[i - 1].close - 1,
                r30=bars[i].close / bars[i - 2].close - 1,
                r1=bars[i].close / bars[i - 4].close - 1,
                r2=bars[i].close / bars[i - 8].close - 1,
                r4=bars[i].close / bars[i - 16].close - 1,
                r8=bars[i].close / bars[i - 32].close - 1,
                r24=bars[i].close / bars[i - 96].close - 1,
                volume_ratio=volume_ratio,
                volume_24h=volume_24h,
                efficiency=abs(move) / max(travelled, 1e-12),
                location=(bars[i].close - bars[i].low) / candle_range if candle_range else 0.5,
                draw_from_high_1h=bars[i].close / max(x.high for x in bars[i - 3 : i + 1]) - 1,
                bounce_from_low_1h=bars[i].close / min(x.low for x in bars[i - 3 : i + 1]) - 1,
                prior_r1=bars[i - 4].close / bars[i - 8].close - 1,
            ))
    for ts, values in rows.items():
        liquid = [x for x in values if x.volume_24h >= 5_000_000]
        market_r1 = statistics.median(x.r1 for x in liquid) if liquid else 0.0
        market_r4 = statistics.median(x.r4 for x in liquid) if liquid else 0.0
        rows[ts] = [Feature(**{**x.__dict__, "market_r1": market_r1, "market_r4": market_r4}) for x in values]
    return rows


def select(config: Config, values: list[Feature]) -> tuple[Feature, int, float] | None:
    candidates: list[tuple[float, Feature, int]] = []
    for x in values:
        if x.volume_24h < config.min_volume_24h or x.volume_ratio < config.min_volume_ratio:
            continue
        if config.family == "fresh_acceleration":
            # First strong hour in the direction of the 4h move.  Exclude
            # already parabolic 24h paths and broad-market beta moves.
            for side in (1, -1):
                if (
                    side * x.r1 >= config.threshold_a
                    and side * x.r4 >= config.threshold_b
                    and side * x.prior_r1 < config.threshold_a
                    and side * x.r24 <= config.threshold_c
                    and side * (x.r1 - x.market_r1) >= config.threshold_a * 0.65
                    and x.efficiency >= 0.45
                    and (x.location >= 0.62 if side > 0 else x.location <= 0.38)
                ):
                    score = side * (x.r1 - x.market_r1) * math.log1p(x.volume_ratio)
                    candidates.append((score, x, side))
        elif config.family == "first_pullback":
            # A prior impulse, a shallow counter-move, then a directional
            # 15-minute recovery.  This is the missing HEMI-style second leg.
            for side in (1, -1):
                pullback = -side * x.r1
                recovery = side * x.r15
                if (
                    side * x.r4 >= config.threshold_a
                    and 0 <= pullback <= config.threshold_b
                    and recovery >= config.threshold_c
                    and side * x.r24 <= 0.80
                    and side * (x.r4 - x.market_r4) >= config.threshold_a * 0.6
                    and (x.location >= 0.58 if side > 0 else x.location <= 0.42)
                ):
                    score = side * (x.r4 - x.market_r4) * (1 + recovery) * math.log1p(x.volume_ratio)
                    candidates.append((score, x, side))
        elif config.family == "exhaustion_reversal":
            # Fade only after an extreme prior hour and a closed rejection
            # candle; never fade merely because the mover is large.
            for move_side in (1, -1):
                side = -move_side
                rejection = side * x.r15
                if (
                    move_side * x.prior_r1 >= config.threshold_a
                    and rejection >= config.threshold_b
                    and move_side * x.r4 >= config.threshold_c
                    and (x.location >= 0.58 if side > 0 else x.location <= 0.42)
                ):
                    score = move_side * x.prior_r1 * math.log1p(x.volume_ratio)
                    candidates.append((score, x, side))
    if not candidates:
        return None
    score, feature, side = max(candidates, key=lambda row: (row[0], row[1].volume_24h))
    return feature, side, score


def trade_return(
    feature: Feature, side: int, bars: list[Bar], config: Config, slippage_bps: float,
) -> tuple[float, str, int]:
    slip = slippage_bps / 10_000
    fee = 0.0005
    entry = bars[feature.index].open * (1 + side * slip)
    stop = entry * (1 - side * config.stop)
    take = entry * (1 + side * config.take)
    extreme = entry
    trail_active = False
    raw_exit = bars[min(feature.index + config.hold_bars, len(bars) - 1)].open
    reason = "time"
    exit_index = min(feature.index + config.hold_bars, len(bars) - 1)
    for i, bar in enumerate(bars[feature.index : exit_index], feature.index):
        if trail_active and config.trail > 0:
            trailing = extreme * (1 - side * config.trail)
            trailing_hit = bar.low <= trailing if side > 0 else bar.high >= trailing
            if trailing_hit:
                gapped = bar.open < trailing if side > 0 else bar.open > trailing
                raw_exit, reason, exit_index = (bar.open if gapped else trailing), "trailing", i
                break
        stop_hit = bar.low <= stop if side > 0 else bar.high >= stop
        take_hit = bar.high >= take if side > 0 else bar.low <= take
        # Conservative ordering when both levels occur in the same 15m bar.
        if stop_hit:
            raw_exit, reason, exit_index = stop, "stop", i
            break
        if take_hit and config.trail <= 0:
            raw_exit, reason, exit_index = take, "take", i
            break
        extreme = max(extreme, bar.high) if side > 0 else min(extreme, bar.low)
        if side * (extreme / entry - 1) >= config.take:
            # The high/low ordering inside a 15m bar is unknown.  Activate the
            # trailing stop only from the next bar to avoid assuming that the
            # favourable extreme occurred before the retracement.
            trail_active = True
    exit_price = raw_exit * (1 - side * slip)
    value = side * (exit_price / entry - 1) - fee - fee * exit_price / entry
    return value, reason, bars[exit_index].ts


def series_for(config: Config, rows: dict[int, list[Feature]], bars: dict[str, list[Bar]], slippage_bps: float):
    output = []
    for ts, values in sorted(rows.items()):
        chosen = select(config, values)
        if not chosen:
            continue
        feature, side, score = chosen
        value, reason, exit_ts = trade_return(feature, side, bars[feature.symbol], config, slippage_bps)
        output.append({
            "entry_ts": ts, "exit_ts": exit_ts, "symbol": feature.symbol,
            "side": side, "return": value, "reason": reason, "score": score,
        })
    return output


def evaluate(series, start: int, end: int, gross: float = 1.0) -> dict[str, object]:
    equity = peak = 1_000.0
    pending: list[dict[str, object]] = []
    current_day = None
    day_start = equity
    blocked = 0
    outcomes = []

    def settle(until: int) -> None:
        nonlocal equity, peak
        ready = [x for x in pending if int(x["exit_ts"]) <= until]
        pending[:] = [x for x in pending if int(x["exit_ts"]) > until]
        for trade in sorted(ready, key=lambda x: int(x["exit_ts"])):
            pnl = float(trade["locked_notional"]) * float(trade["return"])
            equity += pnl
            peak = max(peak, equity)
            outcomes.append({**trade, "pnl": pnl, "equity": equity})

    for trade in series:
        entry_ts = int(trade["entry_ts"])
        if not start <= entry_ts < end:
            continue
        settle(entry_ts)
        day = (entry_ts + BEIJING_OFFSET_MS) // DAY_MS
        if day != current_day:
            current_day, day_start = day, equity
        if equity < day_start * 0.96 or len(pending) >= 2:
            blocked += 1
            continue
        pending.append({**trade, "locked_notional": equity * gross})
    settle(10**30)
    pnls = [float(x["pnl"]) for x in outcomes]
    gains = sum(x for x in pnls if x > 0)
    losses = -sum(x for x in pnls if x < 0)
    running_peak = 1_000.0
    max_dd = 0.0
    for x in outcomes:
        running_peak = max(running_peak, float(x["equity"]))
        max_dd = max(max_dd, 1 - float(x["equity"]) / running_peak)
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": max_dd * 100,
        "trades": len(outcomes),
        "trades_per_day": len(outcomes) / days,
        "win_rate_pct": 100 * sum(x > 0 for x in pnls) / len(pnls) if pnls else 0,
        "profit_factor": gains / losses if losses else None,
        "blocked": blocked,
        "ending_equity": equity,
        "trades_detail": outcomes,
    }


def configs() -> list[Config]:
    output = []
    common = itertools.product((4, 8), (0.01, 0.015, 0.02), (0.015, 0.025, 0.04), (5e6, 15e6, 50e6), (1.0, 1.5, 2.5))
    for hold, stop, take, volume, ratio in common:
        for a, b, c in itertools.product((0.02, 0.03, 0.04, 0.06), (0.02, 0.04, 0.06), (0.15, 0.30, 0.60)):
            output.append(Config("fresh_acceleration", hold, stop, take, volume, ratio, a, b, c))
        for a, b, c in itertools.product((0.04, 0.06, 0.10), (0.01, 0.02, 0.03), (0.002, 0.004, 0.007)):
            output.append(Config("first_pullback", hold, stop, take, volume, ratio, a, b, c))
        for a, b, c in itertools.product((0.04, 0.06, 0.10), (0.003, 0.006, 0.01), (0.04, 0.08, 0.12)):
            output.append(Config("exhaustion_reversal", hold, stop, take, volume, ratio, a, b, c))
    return output


def focused_configs() -> list[Config]:
    """Long-history confirmation grid around the broad recent-search frontier."""
    output = []
    for hold, stop, take, volume, ratio in itertools.product(
        (4, 8), (0.01, 0.015), (0.025, 0.04), (5e6, 15e6), (1.0, 1.5, 2.5)
    ):
        for a, b, c in itertools.product((0.02, 0.03, 0.04), (0.04, 0.06), (0.15, 0.30)):
            output.append(Config("fresh_acceleration", hold, stop, take, volume, ratio, a, b, c))
        for a, b, c in itertools.product((0.04, 0.06, 0.10), (0.01, 0.02, 0.03), (0.002, 0.004)):
            output.append(Config("first_pullback", hold, stop, take, volume, ratio, a, b, c))
        for a, b, c in itertools.product((0.04, 0.06, 0.10), (0.003, 0.006), (0.04, 0.08, 0.12)):
            output.append(Config("exhaustion_reversal", hold, stop, take, volume, ratio, a, b, c))
    return output


def convex_configs() -> list[Config]:
    output = []
    for hold, stop, activation, trail, volume, ratio, a, b, c in itertools.product(
        (8, 16), (0.01, 0.015), (0.02, 0.04), (0.01, 0.02),
        (5e6, 15e6), (1.5, 2.5), (0.02, 0.03, 0.04), (0.04, 0.06), (0.15, 0.30)
    ):
        output.append(Config(
            "fresh_acceleration", hold, stop, activation, volume, ratio, a, b, c, trail
        ))
    return output


def stamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def compact(result: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in result.items() if key != "trades_detail"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/private/tmp/altcoin-fast-grid-bars.pkl.gz")
    parser.add_argument("--history")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-hourly-alpha-v3.json")
    parser.add_argument("--all-futures", action="store_true")
    parser.add_argument("--train-start", default="2026-08-02")
    parser.add_argument("--train-end", default="2026-08-08")
    parser.add_argument("--validation-end", default="2026-08-11")
    parser.add_argument("--holdout-end", default="2026-08-14T06:00:00")
    parser.add_argument("--focused", action="store_true")
    parser.add_argument("--configs-json", action="append", default=[])
    parser.add_argument("--convex", action="store_true")
    args = parser.parse_args()
    bars = load_history(Path(args.history)) if args.history else load_cache(Path(args.cache), pairs_only=not args.all_futures)
    rows = feature_rows(bars)
    bounds = {
        "train": (stamp(args.train_start), stamp(args.train_end)),
        "validation": (stamp(args.train_end), stamp(args.validation_end)),
        "holdout": (stamp(args.validation_end), stamp(args.holdout_end)),
    }
    finalists = []
    if args.configs_json:
        unique = {}
        for source in args.configs_json:
            for item in json.loads(Path(source).read_text()).get("finalists", []):
                config = Config(**item["config"])
                unique[tuple(config.__dict__.values())] = config
        all_configs = list(unique.values())
    else:
        all_configs = convex_configs() if args.convex else focused_configs() if args.focused else configs()
    for number, config in enumerate(all_configs, 1):
        series = series_for(config, rows, bars, 5.0)
        train = evaluate(series, *bounds["train"])
        validation = evaluate(series, *bounds["validation"])
        if (
            float(train["trades_per_day"]) >= 6
            and float(validation["trades_per_day"]) >= 6
            and float(train["return_pct"]) > 0
            and float(validation["return_pct"]) > 0
            and float(train["profit_factor"] or 0) >= 1.03
            and float(validation["profit_factor"] or 0) >= 1.03
        ):
            holdout = evaluate(series, *bounds["holdout"])
            stress = evaluate(series_for(config, rows, bars, 15.0), *bounds["holdout"])
            finalists.append({
                "config": config.__dict__, "train": compact(train),
                "validation": compact(validation), "holdout": compact(holdout),
                "holdout_15bps": compact(stress),
                "holdout_trades": holdout["trades_detail"],
            })
        if number % 1000 == 0:
            print(f"grid {number}/{len(all_configs)} finalists={len(finalists)}", flush=True)
    finalists.sort(key=lambda x: (
        min(float(x["validation"]["return_pct"]), float(x["holdout"]["return_pct"])),
        float(x["holdout"]["profit_factor"] or 0),
    ), reverse=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": len(bars),
        "hourly_snapshots": len(rows),
        "grid": len(all_configs),
        "costs": "5bps taker fee + 5bps adverse slippage each side; 15bps holdout stress",
        "selection": "train+validation positive, PF>=1.03, >=6 trades/day; holdout untouched",
        "finalists": finalists[:100],
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps({**report, "finalists": finalists[:5]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
