#!/usr/bin/env python3
"""Walk-forward test of post-funding crowd unwind trades.

Extreme funding is observed at settlement.  The strategy enters only at the
next 15-minute open and takes the other side of the crowded position.  This
avoids look-ahead and does not assume collection of the funding payment.
"""

from __future__ import annotations

import argparse
import bisect
import importlib.util
import itertools
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


BAR_MS = 15 * 60_000
DAY_MS = 86_400_000
BEIJING_OFFSET_MS = 8 * 3_600_000


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(frozen=True)
class Event:
    symbol: str
    entry_ts: int
    index: int
    funding: float
    r1: float
    r4: float
    volume_24h: float


@dataclass(frozen=True)
class Config:
    funding_threshold: float
    move_threshold: float
    alignment: str
    hold_bars: int
    stop: float
    take: float
    names: int
    min_volume_24h: float


def build_events(root: Path, bars_by_symbol, funding_loader) -> dict[int, list[Event]]:
    output = defaultdict(list)
    for symbol, bars in bars_by_symbol.items():
        times = [x.ts for x in bars]
        prefix = [0.0]
        for bar in bars:
            prefix.append(prefix[-1] + bar.quote_volume)
        funding_times, funding_values = funding_loader(sorted((root / "funding" / symbol).glob("*.zip")))
        for funding_ts, funding in zip(funding_times, funding_values):
            if abs(funding) < 0.0003:
                continue
            index = bisect.bisect_right(times, funding_ts)
            if index < 96 or index + 17 >= len(bars):
                continue
            entry_ts = times[index]
            output[entry_ts].append(Event(
                symbol, entry_ts, index, funding,
                bars[index - 1].close / bars[index - 5].close - 1,
                bars[index - 1].close / bars[index - 17].close - 1,
                prefix[index] - prefix[index - 96],
            ))
    return output


def leg(event: Event, bars, config: Config, slippage_bps: float):
    side = -1 if event.funding > 0 else 1
    slip = slippage_bps / 10_000
    fee = 0.0005
    entry = bars[event.index].open * (1 + side * slip)
    stop = entry * (1 - side * config.stop)
    take = entry * (1 + side * config.take)
    final = min(event.index + config.hold_bars, len(bars) - 1)
    raw_exit = bars[final].open
    exit_ts = bars[final].ts
    reason = "time"
    for i, bar in enumerate(bars[event.index:final], event.index):
        stop_hit = bar.low <= stop if side > 0 else bar.high >= stop
        take_hit = bar.high >= take if side > 0 else bar.low <= take
        if stop_hit:
            raw_exit, exit_ts, reason = stop, bar.ts, "stop"
            break
        if take_hit:
            raw_exit, exit_ts, reason = take, bar.ts, "take"
            break
    exit_price = raw_exit * (1 - side * slip)
    value = side * (exit_price / entry - 1) - fee - fee * exit_price / entry
    return value, exit_ts, reason, side


def build_series(config: Config, events, bars_by_symbol, slippage_bps: float):
    output = []
    for entry_ts, batch in sorted(events.items()):
        candidates = []
        for event in batch:
            if abs(event.funding) < config.funding_threshold or event.volume_24h < config.min_volume_24h:
                continue
            crowd_side = 1 if event.funding > 0 else -1
            directional_move = crowd_side * event.r1
            if config.alignment == "aligned" and directional_move < config.move_threshold:
                continue
            if config.alignment == "rejection" and directional_move > -config.move_threshold:
                continue
            if config.alignment == "any" and abs(event.r1) < config.move_threshold:
                continue
            score = abs(event.funding) * (1 + max(directional_move, 0))
            candidates.append((score, event))
        for score, event in sorted(candidates, reverse=True, key=lambda x: x[0])[:config.names]:
            value, exit_ts, reason, side = leg(event, bars_by_symbol[event.symbol], config, slippage_bps)
            output.append({
                "entry_ts": entry_ts, "exit_ts": exit_ts, "symbol": event.symbol,
                "side": side, "funding": event.funding, "r1": event.r1,
                "return": value, "reason": reason, "score": score,
            })
    return sorted(output, key=lambda x: (x["entry_ts"], -x["score"]))


def evaluate(series, start: int, end: int):
    equity = 1_000.0
    peak = equity
    pending = []
    outcomes = []
    day = None
    day_start = equity
    blocked = 0

    def settle(until):
        nonlocal equity, peak
        ready = sorted((x for x in pending if x["exit_ts"] <= until), key=lambda x: x["exit_ts"])
        pending[:] = [x for x in pending if x["exit_ts"] > until]
        for trade in ready:
            pnl = trade["notional"] * trade["return"]
            equity += pnl
            peak = max(peak, equity)
            outcomes.append({**trade, "pnl": pnl, "equity": equity})

    for trade in series:
        if not start <= trade["entry_ts"] < end:
            continue
        settle(trade["entry_ts"])
        current_day = (trade["entry_ts"] + BEIJING_OFFSET_MS) // DAY_MS
        if current_day != day:
            day, day_start = current_day, equity
        if len(pending) >= 2 or equity < day_start * 0.96:
            blocked += 1
            continue
        pending.append({**trade, "notional": equity})
    settle(10**30)
    pnls = [x["pnl"] for x in outcomes]
    gains = sum(x for x in pnls if x > 0)
    losses = -sum(x for x in pnls if x < 0)
    running_peak = 1_000.0
    max_dd = 0.0
    for trade in outcomes:
        running_peak = max(running_peak, trade["equity"])
        max_dd = max(max_dd, 1 - trade["equity"] / running_peak)
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": max_dd * 100,
        "trades": len(outcomes), "trades_per_day": len(outcomes) / days,
        "win_rate_pct": 100 * sum(x > 0 for x in pnls) / len(pnls) if pnls else 0,
        "profit_factor": gains / losses if losses else None,
        "blocked": blocked, "ending_equity": equity, "trades_detail": outcomes,
    }


def configs():
    return [Config(*values) for values in itertools.product(
        (0.0003, 0.0005, 0.001, 0.002, 0.003, 0.005),
        (0.0, 0.01, 0.02, 0.04),
        ("aligned", "rejection", "any"),
        (4, 8, 16), (0.01, 0.015, 0.02), (0.015, 0.025, 0.04),
        (1, 2), (5e6, 15e6, 50e6),
    )]


def stamp(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def compact(value):
    return {k: v for k, v in value.items() if k != "trades_detail"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/private/tmp/greed-altcoin-hourly-history")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-funding-unwind.json")
    args = parser.parse_args()
    root = Path(args.root)
    v3 = load_module("hourly_v3_funding", Path(__file__).with_name("exp_altcoin_hourly_alpha_v3.py"))
    oi = load_module("oi_funding_unwind", Path(__file__).with_name("exp_altcoin_oi_launch.py"))
    bars = v3.load_history(root)
    events = build_events(root, bars, oi.load_funding)
    periods = {
        "train": (stamp("2026-02-01"), stamp("2026-05-01")),
        "validation": (stamp("2026-05-01"), stamp("2026-07-01")),
        "holdout": (stamp("2026-07-01"), stamp("2026-08-01")),
    }
    finalists = []
    grid = configs()
    for i, config in enumerate(grid, 1):
        series = build_series(config, events, bars, 5.0)
        train = evaluate(series, *periods["train"])
        validation = evaluate(series, *periods["validation"])
        if (
            train["trades_per_day"] >= 6 and validation["trades_per_day"] >= 6
            and train["return_pct"] > 0 and validation["return_pct"] > 0
            and (train["profit_factor"] or 0) >= 1.03 and (validation["profit_factor"] or 0) >= 1.03
        ):
            holdout = evaluate(series, *periods["holdout"])
            stress = evaluate(build_series(config, events, bars, 15.0), *periods["holdout"])
            finalists.append({
                "config": config.__dict__, "train": compact(train), "validation": compact(validation),
                "holdout": compact(holdout), "holdout_15bps": compact(stress),
                "holdout_trades": holdout["trades_detail"],
            })
        if i % 1000 == 0:
            print(f"grid {i}/{len(grid)} finalists={len(finalists)}", flush=True)
    finalists.sort(key=lambda x: (min(x["validation"]["return_pct"], x["holdout"]["return_pct"]), x["holdout"]["profit_factor"] or 0), reverse=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "universe": len(bars),
        "event_times": len(events), "grid": len(grid),
        "method": "post-settlement contrarian; next 15m open; fee 5bps + adverse slippage 5bps each side",
        "finalists": finalists[:100],
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps({**report, "finalists": finalists[:5]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
