#!/usr/bin/env python3
"""Event-level aggTrade microstructure validation on a stratified sample."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import statistics
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class TickEvent:
    period: str
    symbol: str
    entry_ts: int
    side: int
    outcome: float
    delta_10s: float
    delta_30s: float
    delta_60s: float
    delta_300s: float
    large_delta_60s: float
    volume_acceleration: float
    trade_acceleration: float
    persistence_60s: float
    burst_share_60s: float
    price_return_60s: float


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


def period(ts):
    day = datetime.fromtimestamp(ts / 1000, timezone.utc).date().isoformat()
    return "train" if day < "2026-05-01" else "validation" if day < "2026-07-01" else "holdout"


def ratio(values, side):
    total = sum(x[1] for x in values)
    return side * sum(x[2] for x in values) / total if total else 0.0


def load_tick_features(path: Path, entry_ts: int, side: int):
    start = entry_ts - 300_000
    ticks = []
    with zipfile.ZipFile(path) as archive:
        rows = csv.reader(line.decode() for line in archive.read(archive.namelist()[0]).splitlines())
        for row in rows:
            if not row or not row[0].isdigit():
                continue
            ts = int(row[5])
            if ts < start:
                continue
            if ts >= entry_ts:
                break
            price = float(row[1])
            notional = price * float(row[2])
            signed = -notional if row[6].lower() == "true" else notional
            ticks.append((ts, notional, signed, price))
    if len(ticks) < 20:
        return None
    windows = {seconds: [x for x in ticks if x[0] >= entry_ts - seconds * 1000] for seconds in (10, 30, 60, 300)}
    if not windows[60]:
        return None
    notionals = sorted(x[1] for x in windows[60])
    q90 = notionals[max(0, math.ceil(len(notionals) * 0.9) - 1)]
    large = [x for x in windows[60] if x[1] >= q90]
    recent_volume = sum(x[1] for x in windows[60])
    prior = [x for x in ticks if entry_ts - 300_000 <= x[0] < entry_ts - 60_000]
    prior_per_min = sum(x[1] for x in prior) / 4
    buckets = defaultdict(float)
    for ts, _, signed, _ in windows[60]:
        buckets[(ts - (entry_ts - 60_000)) // 10_000] += signed
    persistence = sum(side * value > 0 for value in buckets.values()) / 6
    seconds = defaultdict(float)
    for ts, notional, _, _ in windows[60]:
        seconds[ts // 1000] += notional
    return {
        "delta_10s": ratio(windows[10], side),
        "delta_30s": ratio(windows[30], side),
        "delta_60s": ratio(windows[60], side),
        "delta_300s": ratio(windows[300], side),
        "large_delta_60s": ratio(large, side),
        "volume_acceleration": recent_volume / max(prior_per_min, 1.0),
        "trade_acceleration": len(windows[60]) / max(len(prior) / 4, 1.0),
        "persistence_60s": persistence,
        "burst_share_60s": max(seconds.values(), default=0) / max(recent_volume, 1.0),
        "price_return_60s": side * (windows[60][-1][3] / windows[60][0][3] - 1),
    }


def stats(values):
    gains = sum(max(x, 0) for x in values)
    losses = sum(max(-x, 0) for x in values)
    return {
        "events": len(values), "mean_net_pct": statistics.mean(values) * 100 if values else 0,
        "win_rate_pct": 100 * sum(x > 0 for x in values) / len(values) if values else 0,
        "profit_factor": gains / losses if losses else None,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", default="/private/tmp/greed-altcoin-tick-events/sample.json")
    parser.add_argument("--ticks", default="/private/tmp/greed-altcoin-tick-events/aggTrades")
    parser.add_argument("--history", default="/private/tmp/greed-altcoin-hourly-history")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-tick-microstructure.json")
    args = parser.parse_args()
    root = Path(args.history)
    micro = module("tick_micro_loader", Path(__file__).with_name("exp_altcoin_microstructure_delta.py"))
    v3 = module("tick_v3", Path(__file__).with_name("exp_altcoin_hourly_alpha_v3.py"))
    sample = json.loads(Path(args.sample).read_text())
    by_symbol = defaultdict(list)
    for item in sample:
        by_symbol[item["symbol"]].append(item)
    output = []
    exit_cfg = v3.Config("fresh_acceleration", 16, 0.015, 0.04, 0, 0, 0, 0, 0, 0.01)
    for number, (symbol, items) in enumerate(sorted(by_symbol.items()), 1):
        raw = micro.load_symbol(root / "klines" / symbol)
        plain = [v3.Bar(x.ts, x.open, x.high, x.low, x.close, x.quote_volume) for x in raw]
        indexes = {bar.ts: i for i, bar in enumerate(raw)}
        for item in items:
            entry_ts = int(item["entry_ts"])
            day = datetime.fromtimestamp(entry_ts / 1000, timezone.utc).date().isoformat()
            tick_path = Path(args.ticks) / symbol / f"{symbol}-aggTrades-{day}.zip"
            index = indexes.get(entry_ts)
            if index is None or not tick_path.exists():
                continue
            features = load_tick_features(tick_path, entry_ts, int(item["side"]))
            if not features:
                continue
            feature = v3.Feature(symbol=symbol,ts=entry_ts,index=index,close=raw[index-1].close,r15=0,r30=0,r1=item["r1"],r2=0,r4=item["r4"],r8=0,r24=0,volume_ratio=item["volume_ratio"],volume_24h=0,efficiency=0,location=0,draw_from_high_1h=0,bounce_from_low_1h=0,prior_r1=0)
            value, _, _ = v3.trade_return(feature, int(item["side"]), plain, exit_cfg, 5.0)
            output.append(TickEvent(period(entry_ts),symbol,entry_ts,int(item["side"]),value,**features))
        if number % 50 == 0:
            print(f"symbols {number}/{len(by_symbol)} events={len(output)}",flush=True)
    profiles = {
        "baseline": lambda x: True,
        "aligned_10s": lambda x: x.delta_10s >= 0.05,
        "aligned_30s": lambda x: x.delta_30s >= 0.05,
        "aligned_60s": lambda x: x.delta_60s >= 0.05,
        "persistent": lambda x: x.delta_30s >= 0.05 and x.persistence_60s >= 2/3,
        "large_trade_aligned": lambda x: x.large_delta_60s >= 0.10,
        "flow_acceleration": lambda x: x.delta_30s >= 0.05 and x.volume_acceleration >= 1.5,
        "late_flip": lambda x: x.delta_300s <= -0.05 and x.delta_10s >= 0.05,
        "price_absorption": lambda x: x.delta_60s <= -0.05 and x.price_return_60s >= 0,
        "not_exhausted": lambda x: x.delta_60s >= 0 and x.delta_10s >= 0 and x.burst_share_60s <= 0.35,
    }
    results = {}
    for name, predicate in profiles.items():
        results[name] = {p: stats([x.outcome for x in output if x.period == p and predicate(x)]) for p in ("train","validation","holdout")}
    robust = [name for name, periods in results.items() if all(periods[p]["events"] >= 30 and periods[p]["mean_net_pct"] > 0 and (periods[p]["profit_factor"] or 0) > 1 for p in periods)]
    report={"generated_at":datetime.now(timezone.utc).isoformat(),"sample_requested":len(sample),"sample_parsed":len(output),"method":"pre-entry Binance aggTrades, exact 10s/30s/60s/5m windows, stratified fixed-seed sample","results":results,"robust_profiles":robust}
    Path(args.output).write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
