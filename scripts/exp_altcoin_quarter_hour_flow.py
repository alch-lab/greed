#!/usr/bin/env python3
"""Test quarter-hour taker-flow imbalance across Binance altcoin futures.

The source paper measures activity close to clock marks. Binance historical
klines expose taker-buy quote volume for the whole 15m bar, so this is a coarse
first-stage test. A positive result would justify downloading exact aggTrades;
a negative result rejects the cheap proxy without pretending it is tick data.
"""

from __future__ import annotations

import argparse
import gzip
import heapq
import json
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


BAR_MS = 900_000
DAY_MS = 86_400_000
PERIODS = {
    "train": ("2026-05-22", "2026-07-01"),
    "validation": ("2026-07-01", "2026-08-01"),
    "holdout": ("2026-08-01", "2026-08-15"),
    "recent": ("2026-08-15", "2026-08-21"),
}
EXCLUDED = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT", "LINKUSDT", "AVAXUSDT", "SUIUSDT"}


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote: float
    delta: float


@dataclass(frozen=True)
class Signal:
    symbol: str
    index: int
    ts: int
    side: int
    score: float


@dataclass(frozen=True)
class FlowEvent:
    symbol: str
    index: int
    ts: int
    delta: float
    volume_ratio: float
    r15: float
    prior_delta: float


def stamp(value):
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def load(path):
    with gzip.open(path, "rt") as handle:
        payload = json.load(handle)
    result = {}
    for symbol, rows in payload["data"].items():
        if symbol in EXCLUDED or not symbol.endswith("USDT") or not symbol.isascii():
            continue
        bars = []
        for row in rows:
            quote, taker = float(row[7]), float(row[10])
            bars.append(Bar(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), quote, 2 * taker / quote - 1 if quote else 0.0))
        if len(bars) >= 8 * 96:
            result[symbol] = bars
    return result


def flow_features(all_bars):
    by_ts = defaultdict(list)
    for symbol, bars in all_bars.items():
        prefix = [0.0]
        for bar in bars:
            prefix.append(prefix[-1] + bar.quote)
        continuous = 1
        for i in range(7 * 96, len(bars) - 1):
            continuous = continuous + 1 if bars[i].ts - bars[i - 1].ts == BAR_MS else 1
            if continuous < 96:
                continue
            volume_24h = prefix[i + 1] - prefix[i + 1 - 96]
            average = (prefix[i] - prefix[i - 96]) / 96
            ratio = bars[i].quote / max(average, 1.0)
            if volume_24h < 10_000_000:
                continue
            r15 = bars[i].close / bars[i].open - 1
            by_ts[bars[i].ts].append(FlowEvent(symbol, i, bars[i].ts + BAR_MS, bars[i].delta, ratio, r15, bars[i - 1].delta))
    return by_ts


def signals(by_ts, profile, threshold, min_volume_ratio):
    output = []
    for _, rows in sorted(by_ts.items()):
        candidates = []
        for item in rows:
            if item.volume_ratio < min_volume_ratio or abs(item.delta) < threshold:
                continue
            flow_side = 1 if item.delta > 0 else -1
            if profile == "flow" :
                side = flow_side
            elif profile == "aligned":
                if flow_side * item.r15 <= 0:
                    continue
                side = flow_side
            elif profile == "persistent":
                if flow_side * item.prior_delta <= 0.05:
                    continue
                side = flow_side
            elif profile == "absorption":
                if flow_side * item.r15 >= -0.002:
                    continue
                side = -flow_side
            else:
                raise ValueError(profile)
            score = abs(item.delta) * item.volume_ratio * (1 + min(abs(item.r15) * 20, 1))
            candidates.append(Signal(item.symbol, item.index, item.ts, side, score))
        if candidates:
            output.append(max(candidates, key=lambda item: item.score))
    return output


def outcome(signal, bars, stop, hold_bars, slip_bps):
    i = signal.index + 1
    if i >= len(bars) or bars[i].ts != signal.ts:
        return None
    fee, slip = 0.0005, slip_bps / 10_000
    entry = bars[i].open * (1 + signal.side * slip)
    stop_price = entry * (1 - signal.side * stop)
    end = min(i + hold_bars, len(bars) - 1)
    raw, exit_ts, reason = bars[end].open, bars[end].ts, "time"
    for cursor in range(i, end):
        bar = bars[cursor]
        hit = bar.low <= stop_price if signal.side > 0 else bar.high >= stop_price
        if hit:
            gap = bar.open < stop_price if signal.side > 0 else bar.open > stop_price
            raw, exit_ts, reason = (bar.open if gap else stop_price), bar.ts, "stop"
            break
    exit_price = raw * (1 - signal.side * slip)
    net = signal.side * (exit_price / entry - 1) - fee - fee * exit_price / entry
    return exit_ts, net, reason


def simulate(signal_list, all_bars, start, end, stop, hold_bars, slip_bps=5.0):
    events = []
    for signal in signal_list:
        if start <= signal.ts < end and (value := outcome(signal, all_bars[signal.symbol], stop, hold_bars, slip_bps)) and value[0] < end:
            events.append((signal, *value))
    equity = peak = day_start = 1000.0
    max_dd = 0.0
    active = []
    cooldown = {}
    serial = entries = wins = 0
    day = None
    daily = 0
    values = []
    for signal, exit_ts, net, reason in sorted(events, key=lambda value: (value[0].ts, -value[0].score)):
        while active and active[0][0] <= signal.ts:
            done, _, symbol, pnl = heapq.heappop(active)
            equity += pnl
            cooldown[symbol] = done
            peak = max(peak, equity)
            max_dd = max(max_dd, 1 - equity / peak)
            values.append(pnl)
            wins += pnl > 0
        risk_day = (signal.ts + 8 * 3_600_000) // DAY_MS
        if risk_day != day:
            day, day_start, daily = risk_day, equity, 0
        if daily >= 10 or equity <= day_start * 0.96 or len(active) >= 2 or signal.ts - cooldown.get(signal.symbol, -10**18) < 4 * 3_600_000:
            continue
        pnl = equity * 0.5 * net
        serial += 1
        heapq.heappush(active, (exit_ts, serial, signal.symbol, pnl))
        entries += 1
        daily += 1
    while active:
        _, _, _, pnl = heapq.heappop(active)
        equity += pnl
        values.append(pnl)
        wins += pnl > 0
        peak = max(peak, equity)
        max_dd = max(max_dd, 1 - equity / peak)
    gains, losses = sum(max(x, 0) for x in values), sum(max(-x, 0) for x in values)
    return {"return_pct": (equity / 1000 - 1) * 100, "max_drawdown_pct": max_dd * 100, "trades": entries, "trades_per_day": entries / max((end-start)/DAY_MS, 1), "win_rate_pct": wins / len(values) * 100 if values else 0, "profit_factor": gains / losses if losses else None}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("/private/tmp/binance-testnet-15m-20260515-0820.json.gz"))
    parser.add_argument("--output", type=Path, default=Path("/private/tmp/greed-altcoin-quarter-hour-flow.json"))
    args = parser.parse_args()
    all_bars = load(args.data)
    features = flow_features(all_bars)
    results = {}
    for profile in ("flow", "aligned", "persistent", "absorption"):
        for threshold in (0.10, 0.20, 0.35):
            for volume in (1.0, 2.0, 4.0):
                items = signals(features, profile, threshold, volume)
                for stop in (0.01, 0.02, 0.03):
                    for hold in (16, 32, 48):
                        name = f"{profile}_d{threshold:.2f}_v{volume:.0f}_s{stop:.2f}_h{hold}"
                        results[name] = {"profile": profile, "delta": threshold, "volume": volume, "stop": stop, "hold_bars": hold, "periods": {period: simulate(items, all_bars, stamp(bounds[0]), stamp(bounds[1]), stop, hold) for period, bounds in PERIODS.items()}}
    eligible=[]
    for name,item in results.items():
        a,b=item["periods"]["train"],item["periods"]["validation"]
        if a["return_pct"]>0 and b["return_pct"]>0 and a["trades"]>=25 and b["trades"]>=15:
            eligible.append((b["return_pct"]-.5*b["max_drawdown_pct"],name))
    ranked=[name for _,name in sorted(eligible,reverse=True)]
    args.output.write_text(json.dumps({"periods":PERIODS,"ranked_without_holdout":ranked,"results":results},indent=2)+"\n")
    compact=lambda periods:{p:{k:v[k] for k in ("return_pct","max_drawdown_pct","trades","trades_per_day","win_rate_pct","profit_factor")} for p,v in periods.items()}
    print(json.dumps({"eligible":len(ranked),"top":[{"name":name,"periods":compact(results[name]["periods"])} for name in ranked[:15]]},indent=2))


if __name__ == "__main__":
    main()
