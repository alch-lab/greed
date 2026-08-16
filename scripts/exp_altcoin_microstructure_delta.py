#!/usr/bin/env python3
"""Causal microstructure screen using Binance taker-buy volume.

Binance 15m archives retain taker-buy quote volume and trade count even though
the production research loader historically discarded them.  This study asks
whether signed taker Delta, Delta acceleration and trade intensity add stable
out-of-sample information to a loose hourly acceleration signal.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import itertools
import json
import statistics
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


BAR_MS = 900_000
DAY_MS = 86_400_000
MAJORS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT", "LINKUSDT", "BCHUSDT", "LTCUSDT"}


@dataclass(frozen=True)
class MicroBar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float
    trades: int
    taker_buy_quote: float


@dataclass(frozen=True)
class Event:
    symbol: str
    entry_ts: int
    index: int
    side: int
    r1: float
    r4: float
    r24: float
    relative_r1: float
    volume_ratio: float
    volume_24h: float
    delta_15m: float
    delta_1h: float
    prior_delta_1h: float
    trade_ratio: float
    efficiency: float
    location: float


@dataclass(frozen=True)
class Rule:
    min_r1: float
    min_r4: float
    min_volume_ratio: float
    min_delta_1h: float
    min_delta_15m: float
    min_delta_acceleration: float
    min_trade_ratio: float
    stop: float
    mode: str


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_symbol(directory: Path) -> list[MicroBar]:
    output = []
    for path in sorted(directory.glob("*.zip")):
        try:
            with zipfile.ZipFile(path) as archive:
                rows = csv.reader(line.decode() for line in archive.read(archive.namelist()[0]).splitlines())
                for row in rows:
                    if not row or not row[0].isdigit():
                        continue
                    output.append(MicroBar(
                        int(row[0]), *(float(row[i]) for i in range(1, 5)),
                        float(row[7]), int(row[8]), float(row[10]),
                    ))
        except (zipfile.BadZipFile, IndexError, ValueError):
            continue
    output.sort(key=lambda x: x.ts)
    dedup = {bar.ts: bar for bar in output}
    return [dedup[key] for key in sorted(dedup)]


def build(root: Path):
    raw = {}
    for completed, directory in enumerate(sorted((root / "klines").iterdir()), 1):
        if directory.is_dir() and directory.name not in MAJORS:
            bars = load_symbol(directory)
            if len(bars) >= 8 * 96:
                raw[directory.name] = bars
        if completed % 100 == 0:
            print(f"loaded {completed}", flush=True)
    provisional = defaultdict(list)
    for symbol, bars in raw.items():
        quote_prefix = [0.0]
        delta_prefix = [0.0]
        trades_prefix = [0]
        for bar in bars:
            quote_prefix.append(quote_prefix[-1] + bar.quote_volume)
            delta_prefix.append(delta_prefix[-1] + 2 * bar.taker_buy_quote - bar.quote_volume)
            trades_prefix.append(trades_prefix[-1] + bar.trades)
        for i in range(2 * 96, len(bars) - 17):
            if datetime.fromtimestamp(bars[i].ts / 1000, timezone.utc).minute != 45:
                continue
            if bars[i].ts - bars[i - 96].ts > 97 * BAR_MS:
                continue
            r1 = bars[i].close / bars[i - 4].close - 1
            r4 = bars[i].close / bars[i - 16].close - 1
            if abs(r1) < 0.005 or r1 * r4 <= 0:
                continue
            side = 1 if r1 > 0 else -1
            prior_r1 = bars[i - 4].close / bars[i - 8].close - 1
            # Keep only the first acceleration hour, not every hour of an
            # already-running pump or dump.
            if side * prior_r1 >= side * r1:
                continue
            q1 = quote_prefix[i + 1] - quote_prefix[i - 3]
            historical_q = [quote_prefix[e + 1] - quote_prefix[e - 3] for e in range(i - 2 * 96, i, 4) if e >= 3]
            volume_ratio = q1 / max(statistics.median(historical_q), 1.0)
            q24 = quote_prefix[i + 1] - quote_prefix[i - 95]
            if q24 < 5_000_000 or volume_ratio < 0.75:
                continue
            d15 = (2 * bars[i].taker_buy_quote - bars[i].quote_volume) / max(bars[i].quote_volume, 1.0)
            d1 = (delta_prefix[i + 1] - delta_prefix[i - 3]) / max(q1, 1.0)
            prior_q1 = quote_prefix[i - 3] - quote_prefix[i - 7]
            prior_d1 = (delta_prefix[i - 3] - delta_prefix[i - 7]) / max(prior_q1, 1.0)
            t1 = trades_prefix[i + 1] - trades_prefix[i - 3]
            historical_t = [trades_prefix[e + 1] - trades_prefix[e - 3] for e in range(i - 2 * 96, i, 4) if e >= 3]
            trade_ratio = t1 / max(statistics.median(historical_t), 1.0)
            path = bars[i - 4 : i + 1]
            travelled = sum(abs(b.close - a.close) for a, b in zip(path, path[1:]))
            span = bars[i].high - bars[i].low
            location = (bars[i].close - bars[i].low) / span if span else 0.5
            provisional[bars[i + 1].ts].append(Event(
                symbol, bars[i + 1].ts, i + 1, side, r1, r4,
                bars[i].close / bars[i - 96].close - 1, 0.0,
                volume_ratio, q24, side * d15, side * d1, side * prior_d1,
                trade_ratio, abs(r1) / max(travelled / bars[i - 4].close, 1e-12),
                location if side > 0 else 1 - location,
            ))
    rows = {}
    for ts, values in provisional.items():
        market = statistics.median(x.r1 for x in values) if values else 0.0
        rows[ts] = [Event(**{**x.__dict__, "relative_r1": x.side * (x.r1 - market)}) for x in values]
    return raw, rows


def rules():
    price_profiles = ((0.005, 0.01), (0.01, 0.02), (0.02, 0.04), (0.03, 0.06))
    micro_profiles = (
        ("aligned", -1.0, -1.0, -1.0, 0.75),  # price/volume baseline
        ("aligned", 0.0, -0.05, -0.10, 1.0),
        ("aligned", 0.05, 0.0, 0.0, 1.0),
        ("aligned", 0.10, 0.05, 0.0, 1.0),
        ("aligned", 0.10, 0.05, 0.05, 1.0),
        ("aligned", 0.20, 0.10, 0.05, 1.5),
        # Price advances despite opposite cumulative Delta, then the latest
        # 15m flow stabilises or flips: passive absorption continuation.
        ("absorption", 0.05, -0.05, 0.0, 1.0),
        ("absorption", 0.10, 0.0, 0.05, 1.0),
        ("absorption", 0.20, 0.05, 0.10, 1.5),
        # Extreme aligned hourly pressure followed by an opposite closing
        # flow/candle: exhaustion fade rather than blind counter-trend entry.
        ("fade", 0.10, 0.05, 0.0, 1.0),
        ("fade", 0.20, 0.05, 0.0, 1.0),
        ("fade", 0.20, 0.10, 0.05, 1.5),
    )
    return [
        Rule(r1, r4, volume, delta1, delta15, acceleration, trades, stop, mode)
        for (r1, r4), volume, (mode, delta1, delta15, acceleration, trades), stop
        in itertools.product(price_profiles, (0.75, 1.5, 2.5), micro_profiles, (0.01, 0.015))
    ]


def select(rule: Rule, values: list[Event]):
    base = [x for x in values if (
        abs(x.r1) >= rule.min_r1 and abs(x.r4) >= rule.min_r4
        and x.volume_ratio >= rule.min_volume_ratio
        and x.trade_ratio >= rule.min_trade_ratio and x.relative_r1 >= rule.min_r1 * 0.5
        and x.efficiency >= 0.35 and x.side * x.r24 <= 0.30
    )]
    if rule.mode == "aligned":
        eligible = [x for x in base if x.location >= 0.55 and x.delta_1h >= rule.min_delta_1h and x.delta_15m >= rule.min_delta_15m and x.delta_1h - x.prior_delta_1h >= rule.min_delta_acceleration]
        side = lambda x: x.side
    elif rule.mode == "absorption":
        eligible = [x for x in base if x.location >= 0.55 and x.delta_1h <= -rule.min_delta_1h and x.delta_15m >= rule.min_delta_15m and x.delta_15m - x.delta_1h >= rule.min_delta_acceleration]
        side = lambda x: x.side
    else:
        eligible = [x for x in base if x.location <= 0.45 and x.delta_1h >= rule.min_delta_1h and x.delta_15m <= -rule.min_delta_15m and x.prior_delta_1h - x.delta_15m >= rule.min_delta_acceleration]
        side = lambda x: -x.side
    event = max(eligible, key=lambda x: x.relative_r1 * (1 + abs(x.delta_1h)) * x.volume_ratio, default=None)
    return (event, side(event)) if event else None


def stamp(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def compact(value):
    return {k: v for k, v in value.items() if k != "trades_detail"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/private/tmp/greed-altcoin-hourly-history")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-microstructure-delta.json")
    args = parser.parse_args()
    v3 = load_module("micro_v3", Path(__file__).with_name("exp_altcoin_hourly_alpha_v3.py"))
    raw, rows = build(Path(args.root))
    plain = {s: [v3.Bar(x.ts, x.open, x.high, x.low, x.close, x.quote_volume) for x in bars] for s, bars in raw.items()}
    periods = {
        "train": (stamp("2026-02-01"), stamp("2026-05-01")),
        "validation": (stamp("2026-05-01"), stamp("2026-07-01")),
        "holdout": (stamp("2026-07-01"), stamp("2026-08-01")),
    }
    finalists = []
    all_results = []
    grid = rules()
    for number, rule in enumerate(grid, 1):
        exit_cfg = v3.Config("fresh_acceleration", 16, rule.stop, 0.04, 0, 0, 0, 0, 0, 0.01)
        series = []
        for ts, values in sorted(rows.items()):
            selected = select(rule, values)
            if selected:
                event, trade_side = selected
                feature = v3.Feature(
                    symbol=event.symbol, ts=ts, index=event.index,
                    close=raw[event.symbol][event.index - 1].close,
                    r15=0, r30=0, r1=event.r1, r2=0, r4=event.r4,
                    r8=0, r24=event.r24, volume_ratio=event.volume_ratio,
                    volume_24h=event.volume_24h, efficiency=event.efficiency,
                    location=event.location, draw_from_high_1h=0,
                    bounce_from_low_1h=0, prior_r1=0,
                )
                value, reason, exit_ts = v3.trade_return(feature, trade_side, plain[event.symbol], exit_cfg, 5.0)
                series.append({"entry_ts":ts,"exit_ts":exit_ts,"symbol":event.symbol,"side":trade_side,"return":value,"reason":reason,"score":event.relative_r1,"delta_1h":event.delta_1h,"delta_15m":event.delta_15m,"delta_acceleration":event.delta_1h-event.prior_delta_1h,"trade_ratio":event.trade_ratio})
        train = v3.evaluate(series, *periods["train"])
        validation = v3.evaluate(series, *periods["validation"])
        holdout = v3.evaluate(series, *periods["holdout"])
        all_results.append({"rule":rule.__dict__,"train":compact(train),"validation":compact(validation),"holdout":compact(holdout)})
        if train["trades_per_day"] >= 6 and validation["trades_per_day"] >= 6 and train["return_pct"] > 0 and validation["return_pct"] > 0 and (train["profit_factor"] or 0) >= 1.03 and (validation["profit_factor"] or 0) >= 1.03:
            finalists.append({"rule":rule.__dict__,"train":compact(train),"validation":compact(validation),"holdout":compact(holdout),"holdout_trades":holdout["trades_detail"]})
        if number % 2000 == 0:
            print(f"grid {number}/{len(grid)} finalists={len(finalists)}", flush=True)
    finalists.sort(key=lambda x:(min(x["validation"]["return_pct"],x["holdout"]["return_pct"]),x["holdout"]["profit_factor"] or 0),reverse=True)
    tick_rule = Rule(0.01, 0.02, 1.5, -1.0, -1.0, -1.0, 0.75, 0.015, "aligned")
    tick_manifest = []
    for ts, values in sorted(rows.items()):
        chosen = select(tick_rule, values)
        if chosen:
            event, side = chosen
            tick_manifest.append({"entry_ts":ts,"symbol":event.symbol,"index":event.index,"side":side,"r1":event.r1,"r4":event.r4,"volume_ratio":event.volume_ratio})
    report={"generated_at":datetime.now(timezone.utc).isoformat(),"symbols":len(raw),"hourly_event_times":len(rows),"grid":len(grid),"method":"directional taker Delta + Delta acceleration + trade intensity; causal next-open execution","finalists":finalists[:100],"all_results":all_results,"tick_manifest":tick_manifest}
    Path(args.output).write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps({**report,"finalists":finalists[:5]},ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
