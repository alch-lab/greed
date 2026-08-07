#!/usr/bin/env python3
"""No-look-ahead replay for the V12 single-account MR + strong-trend portfolio.

It reconstructs decisions from production eval/research logs.  Prices are 10-second
observations, so fills are conservative at configured limit/stop/target prices and do
not claim queue-position or intrabar precision.
"""

from __future__ import annotations

import argparse
import bisect
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def trend_stats(times, prices, ts, window_ms=7_200_000):
    right = bisect.bisect_right(times, ts)
    left = bisect.bisect_left(times, ts - window_ms, 0, right)
    sample = prices[left:right]
    if len(sample) < 2:
        return 0.0, 0.0
    path = sum(abs(b - a) for a, b in zip(sample, sample[1:]))
    return sample[-1] / sample[0] - 1.0, abs(sample[-1] - sample[0]) / path if path else 0.0


def mr_events(run_id, research_rows, times, prices):
    events = []
    for row in research_rows:
        if row.get("run_id") != run_id or row.get("event_type") != "signal_confirmed":
            continue
        signal = (row.get("data") or {}).get("signal") or {}
        if signal.get("stage") != "confirmed" or not str(signal.get("model", "")).startswith("orderflow_exhaustion_v"):
            continue
        side = signal.get("side")
        trdr = signal.get("trdr") or {}
        reasons = set(signal.get("context_reasons") or [])
        location = bool(signal.get("location_confirmed")) or "sweep_or_vwap" in reasons
        absorption = "absorption" in reasons
        score = sum(
            [
                location,
                absorption,
                "trdr_delta" in reasons,
                "footprint_cluster" in reasons,
                "oi" in reasons,
                "spot_perp_confluence" in reasons,
                "liquidation" in reasons,
            ]
        )
        ts = int(row["ts_ms"])
        slow_return, slow_efficiency = trend_stats(times, prices, ts)
        if slow_return >= 0.005 and slow_efficiency >= 0.10:
            regime = "trend_up"
        elif slow_return <= -0.005 and slow_efficiency >= 0.10:
            regime = "trend_down"
        else:
            regime = trdr.get("regime", "range")
        countertrend = (side == "sell" and regime == "trend_up") or (side == "buy" and regime == "trend_down")
        aligned = (side == "buy" and regime == "trend_up") or (side == "sell" and regime == "trend_down")
        required = 4 if aligned else 5
        structural = abs(float(signal["stop_anchor"]) - float(signal.get("zone") or signal["price"])) / float(signal.get("zone") or signal["price"])
        eligible = (
            not countertrend
            and score >= required
            and absorption
            and (regime != "range" or location)
            and trdr.get("source_coverage_complete") is True
            and trdr.get("footprint_matches") is True
            and structural <= 0.0025
        )
        if eligible:
            events.append({"ts": ts, "strategy": "mr", "side": side, "entry": float(signal.get("zone") or signal["price"])})
    return events


def trend_events(times, prices, flows):
    events = []
    started = times[0]
    fired = None
    neutral_since = None
    flow_times = sorted(flows)
    flow_index = 0
    flow = {}
    for ts, price in zip(times, prices):
        while flow_index < len(flow_times) and flow_times[flow_index] <= ts:
            flow = flows[flow_times[flow_index]]
            flow_index += 1
        slow_return, slow_efficiency = trend_stats(times, prices, ts)
        side = "buy" if slow_return >= 0.008 and slow_efficiency >= 0.15 else "sell" if slow_return <= -0.008 and slow_efficiency >= 0.15 else None
        if side is None:
            neutral_since = ts if neutral_since is None else neutral_since
            if ts - neutral_since >= 900_000:
                fired = None
            continue
        neutral_since = None
        if fired is not None and fired != side:
            fired = None
        if fired == side or ts - started < 7_200_000:
            continue
        sign = 1 if side == "buy" else -1
        oi = flow.get("oi_quadrant")
        oi_ready = oi in ({"new_longs", "short_covering"} if sign > 0 else {"new_shorts", "long_liquidation"})
        ready = (
            flow.get("source_coverage_complete") is True
            and flow.get("direction") == side
            and int(flow.get("tier") or 0) >= 1
            and float(flow.get("spot_delta_usd") or 0) * sign > 0
            and float(flow.get("perp_delta_usd") or 0) * sign > 0
            and oi_ready
        )
        if ready:
            events.append({"ts": ts, "strategy": "trend", "side": side, "entry": price})
            fired = side
    return events


def exit_trade(event, entry_ts, entry, qty, times, prices):
    sign = 1 if event["side"] == "buy" else -1
    start = bisect.bisect_right(times, entry_ts)
    stop = entry * (1 - sign * 0.0025)
    remaining = qty
    gross = 0.0
    exit_fees = 0.0
    stage = 0
    exit_ts, exit_price, reason = entry_ts, entry, "time"
    max_hold = 7_200_000 if event["strategy"] == "trend" else 1_800_000
    end = bisect.bisect_right(times, entry_ts + max_hold)
    peak = entry
    for index in range(start, end):
        ts, price = times[index], prices[index]
        exit_ts, exit_price = ts, price
        stopped = price <= stop if sign > 0 else price >= stop
        if stopped:
            gross += sign * (stop - entry) * remaining
            exit_fees += stop * remaining * 0.0004
            remaining = 0.0
            exit_price, reason = stop, "stop" if stage == 0 else "protected_stop"
            break
        if event["strategy"] == "trend":
            peak = max(peak, price) if sign > 0 else min(peak, price)
            favorable_pct = sign * (price / entry - 1.0)
            if favorable_pct >= 0.0025:
                candidate = peak * (1 - sign * 0.0025)
                stop = max(stop, candidate) if sign > 0 else min(stop, candidate)
            continue
        tp1 = entry * (1 + sign * 0.0020)
        tp2 = entry * (1 + sign * 0.0035)
        if stage == 0 and ((sign > 0 and price >= tp1) or (sign < 0 and price <= tp1)):
            close_qty = qty * 0.50
            gross += sign * (tp1 - entry) * close_qty
            exit_fees += tp1 * close_qty * 0.0004
            remaining -= close_qty
            stop = entry
            stage = 1
        if stage == 1 and ((sign > 0 and price >= tp2) or (sign < 0 and price <= tp2)):
            close_qty = qty * 0.30
            gross += sign * (tp2 - entry) * close_qty
            exit_fees += tp2 * close_qty * 0.0004
            remaining -= close_qty
            stage = 2
            peak = price
        if stage == 2:
            peak = max(peak, price) if sign > 0 else min(peak, price)
            candidate = peak * (1 - sign * 0.0015)
            stop = max(stop, candidate) if sign > 0 else min(stop, candidate)
    if remaining > 0:
        gross += sign * (exit_price - entry) * remaining
        exit_fees += exit_price * remaining * 0.0004
    entry_fee = entry * qty * (0.0004 if event["strategy"] == "trend" else 0.0002)
    return exit_ts, exit_price, gross, entry_fee + exit_fees, reason, stage


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-dir", required=True, type=Path)
    parser.add_argument("--research-dir", required=True, type=Path)
    parser.add_argument("--cash", type=float, default=5_000.0)
    args = parser.parse_args()
    research = [row for path in sorted(args.research_dir.glob("*.jsonl")) for row in read_jsonl(path)]
    equity = args.cash
    peak_equity = equity
    max_drawdown = 0.0
    trades = []
    daily = defaultdict(float)
    for eval_path in sorted(args.eval_dir.glob("*.jsonl")):
        run_id = eval_path.stem
        observed, flows = {}, {}
        for row in read_jsonl(eval_path):
            note = row.get("note") or {}
            if row.get("source") == "OrderFlowExhaustion" and isinstance(note.get("price"), (int, float)):
                observed[int(row["ts_ms"])] = float(note["price"])
            elif row.get("source") == "TrdrMarketMap" and isinstance(note.get("flow"), dict):
                flows[int(row["ts_ms"])] = note["flow"]
        times = sorted(observed)
        if len(times) < 5_000:
            continue
        prices = [observed[ts] for ts in times]
        events = mr_events(run_id, research, times, prices) + trend_events(times, prices, flows)
        # Strong trend wins an exact-timestamp tie.  Once an order/position exists, later
        # events are skipped: this mirrors the engine's single pending-entry/position lock.
        events.sort(key=lambda item: (item["ts"], 0 if item["strategy"] == "trend" else 1))
        available = times[0]
        seen_ts = set()
        for event in events:
            if event["ts"] < available or (event["ts"], event["strategy"]) in seen_ts:
                continue
            seen_ts.add((event["ts"], event["strategy"]))
            if event["strategy"] == "trend":
                index = bisect.bisect_left(times, event["ts"])
                if index >= len(times):
                    continue
                entry_ts, entry = times[index], prices[index]
            else:
                begin = bisect.bisect_left(times, event["ts"])
                end = bisect.bisect_right(times, event["ts"] + 600_000)
                fill = next((i for i in range(begin, end) if prices[i] <= event["entry"] if event["side"] == "buy"), None) if event["side"] == "buy" else next((i for i in range(begin, end) if prices[i] >= event["entry"]), None)
                if fill is None:
                    available = event["ts"] + 600_000
                    continue
                entry_ts, entry = times[fill], event["entry"]
            stop_distance = entry * 0.0025
            qty = int(min(equity * 0.0025 / stop_distance, equity * 3 / entry) * 10_000) / 10_000
            exit_ts, exit_price, gross, fees, reason, stage = exit_trade(event, entry_ts, entry, qty, times, prices)
            net = gross - fees
            equity += net
            peak_equity = max(peak_equity, equity)
            max_drawdown = max(max_drawdown, (peak_equity - equity) / peak_equity)
            available = exit_ts
            day = datetime.fromtimestamp(entry_ts / 1000).astimezone().strftime("%F")
            daily[day] += net
            trades.append({**event, "entry_ts": entry_ts, "entry": entry, "exit_ts": exit_ts, "exit": exit_price, "qty": qty, "gross": gross, "fees": fees, "net": net, "reason": reason, "stage": stage})
    for trade in trades:
        stamp = datetime.fromtimestamp(trade["entry_ts"] / 1000).astimezone().strftime("%F %T")
        print(f"{stamp} {trade['strategy']:5} {trade['side']:4} {trade['qty']:.4f} BTC ${trade['entry']*trade['qty']:.0f}  {trade['entry']:.1f}->{trade['exit']:.1f} {trade['reason']:14} net={trade['net']:+.2f}")
    by_strategy = {}
    for strategy in ("mr", "trend"):
        subset = [trade for trade in trades if trade["strategy"] == strategy]
        by_strategy[strategy] = {"trades": len(subset), "wins": sum(trade["net"] > 0 for trade in subset), "net_pnl": round(sum(trade["net"] for trade in subset), 4)}
    print(json.dumps({"trades": len(trades), "wins": sum(trade["net"] > 0 for trade in trades), "win_rate": sum(trade["net"] > 0 for trade in trades) / len(trades) if trades else 0, "net_pnl_usdt": round(equity - args.cash, 4), "return_pct": round((equity / args.cash - 1) * 100, 4), "max_drawdown_pct": round(max_drawdown * 100, 4), "by_strategy": by_strategy, "daily_pnl": {key: round(value, 4) for key, value in daily.items()}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
