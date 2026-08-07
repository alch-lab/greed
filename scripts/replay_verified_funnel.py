#!/usr/bin/env python3
"""Replay the V11 verified funnel from production research logs without look-ahead.

The diagnostic eval stream contains one observed BTC price per 10-second bucket and the
research stream contains every confirmed setup.  This replay deliberately uses only
prices available at each decision timestamp.  Intrabar OHLC and queue position are not
available, so limit fills and exits are conservatively evaluated on observed bucket
prices at the configured limit/stop/target price.
"""

from __future__ import annotations

import argparse
import bisect
import json
from datetime import datetime
from pathlib import Path


def rows(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def trend_at(times, prices, ts, window_ms=7_200_000):
    right = bisect.bisect_right(times, ts)
    left = bisect.bisect_left(times, ts - window_ms, 0, right)
    sample = prices[left:right]
    if len(sample) < 2 or sample[0] <= 0:
        return 0.0, 0.0
    ret = sample[-1] / sample[0] - 1.0
    path = sum(abs(b - a) for a, b in zip(sample, sample[1:]))
    efficiency = abs(sample[-1] - sample[0]) / path if path else 0.0
    return ret, efficiency


def at_or_after(times, ts):
    return bisect.bisect_left(times, ts)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval", required=True, type=Path)
    parser.add_argument("--research", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cash", type=float, default=5_000.0)
    parser.add_argument("--risk-pct", type=float, default=0.002)
    parser.add_argument("--risk-scale", type=float, default=1.0)
    args = parser.parse_args()

    observed = {}
    for row in rows(args.eval):
        if row.get("run_id") != args.run_id or row.get("source") != "OrderFlowExhaustion":
            continue
        note = row.get("note") or {}
        price = note.get("price")
        if isinstance(price, (int, float)) and price > 0:
            observed[int(row["ts_ms"])] = float(price)
    times = sorted(observed)
    prices = [observed[ts] for ts in times]

    signals = []
    for row in rows(args.research):
        if row.get("run_id") != args.run_id or row.get("event_type") != "signal_confirmed":
            continue
        signal = (row.get("data") or {}).get("signal") or {}
        if signal.get("stage") == "confirmed":
            signals.append((int(row["ts_ms"]), signal))
    signals.sort(key=lambda item: item[0])

    candidates = []
    rejected = {}
    for ts, signal in signals:
        side = signal.get("side")
        trdr = signal.get("trdr") or {}
        reasons = set(signal.get("context_reasons") or [])
        location = bool(signal.get("location_confirmed")) or "sweep_or_vwap" in reasons
        absorption = "absorption" in reasons
        evidence = {
            "location": location,
            "absorption": absorption,
            "delta": "trdr_delta" in reasons,
            "footprint": "footprint_cluster" in reasons,
            "oi": "oi" in reasons,
            "confluence": "spot_perp_confluence" in reasons,
            "liquidation": "liquidation" in reasons,
        }
        score = sum(evidence.values())
        slow_ret, slow_eff = trend_at(times, prices, ts)
        if slow_ret >= 0.005 and slow_eff >= 0.10:
            regime = "trend_up"
        elif slow_ret <= -0.005 and slow_eff >= 0.10:
            regime = "trend_down"
        else:
            regime = trdr.get("regime", "range")
        countertrend = (side == "sell" and regime == "trend_up") or (
            side == "buy" and regime == "trend_down"
        )
        aligned = (side == "buy" and regime == "trend_up") or (
            side == "sell" and regime == "trend_down"
        )
        required_score = 4 if aligned else 5
        blockers = []
        if countertrend:
            blockers.append("countertrend")
        if score < required_score:
            blockers.append("context_score")
        if not absorption:
            blockers.append("absorption")
        if regime == "range" and not location:
            blockers.append("range_location")
        if not trdr.get("source_coverage_complete", False):
            blockers.append("source_coverage")
        if not trdr.get("footprint_matches", False):
            blockers.append("footprint")
        if blockers:
            for blocker in blockers:
                rejected[blocker] = rejected.get(blocker, 0) + 1
            continue
        candidates.append(
            {
                "ts": ts,
                "side": side,
                "limit": float(signal.get("zone") or signal["price"]),
                "stop_anchor": float(signal["stop_anchor"]),
                "score": score,
                "regime": regime,
                "slow_ret": slow_ret,
                "slow_eff": slow_eff,
            }
        )

    equity = args.cash
    available_after = 0
    trades = []
    for candidate in candidates:
        if candidate["ts"] < available_after:
            continue
        begin = at_or_after(times, candidate["ts"])
        end = bisect.bisect_right(times, candidate["ts"] + 600_000)
        fill_index = None
        for index in range(begin, end):
            touched = prices[index] <= candidate["limit"] if candidate["side"] == "buy" else prices[index] >= candidate["limit"]
            if touched:
                fill_index = index
                break
        if fill_index is None:
            continue
        entry_ts = times[fill_index]
        entry = candidate["limit"]
        structural_pct = abs(entry - candidate["stop_anchor"]) / entry
        if structural_pct > 0.010:
            continue
        if candidate["side"] == "buy":
            stop = min(candidate["stop_anchor"], entry * 0.995)
            tp1 = entry + 1.5 * (entry - stop)
            tp2 = entry + 3.0 * (entry - stop)
        else:
            stop = max(candidate["stop_anchor"], entry * 1.005)
            tp1 = entry - 1.5 * (stop - entry)
            tp2 = entry - 3.0 * (stop - entry)
        risk_usd = equity * args.risk_pct * args.risk_scale
        qty = int((risk_usd / abs(entry - stop)) * 10_000) / 10_000
        qty = min(qty, int((equity * 3.0 / entry) * 10_000) / 10_000)
        if qty <= 0:
            continue

        remaining = qty
        closed_fraction = 0.0
        active_stop = stop
        realized = 0.0
        exit_fee = 0.0
        exit_reason = "time"
        exit_ts = entry_ts
        exit_price = entry
        last_index = min(len(times), bisect.bisect_right(times, entry_ts + 1_800_000))
        for index in range(fill_index + 1, last_index):
            px = prices[index]
            exit_ts, exit_price = times[index], px
            stopped = px <= active_stop if candidate["side"] == "buy" else px >= active_stop
            if stopped:
                px = active_stop
                direction = 1 if candidate["side"] == "buy" else -1
                realized += direction * (px - entry) * remaining
                exit_fee += px * remaining * 0.0004
                remaining = 0.0
                exit_price = px
                exit_reason = "stop" if closed_fraction == 0 else "breakeven"
                break
            hit_tp1 = px >= tp1 if candidate["side"] == "buy" else px <= tp1
            if closed_fraction < 0.49 and hit_tp1:
                close_qty = qty * 0.50
                direction = 1 if candidate["side"] == "buy" else -1
                realized += direction * (tp1 - entry) * close_qty
                exit_fee += tp1 * close_qty * 0.0004
                remaining -= close_qty
                closed_fraction = 0.50
                active_stop = entry
            hit_tp2 = px >= tp2 if candidate["side"] == "buy" else px <= tp2
            if closed_fraction >= 0.49 and closed_fraction < 0.74 and hit_tp2:
                close_qty = qty * 0.25
                direction = 1 if candidate["side"] == "buy" else -1
                realized += direction * (tp2 - entry) * close_qty
                exit_fee += tp2 * close_qty * 0.0004
                remaining -= close_qty
                closed_fraction = 0.75
            if closed_fraction >= 0.74:
                trail = px * (0.994 if candidate["side"] == "buy" else 1.006)
                active_stop = max(active_stop, trail) if candidate["side"] == "buy" else min(active_stop, trail)
        if remaining > 0:
            direction = 1 if candidate["side"] == "buy" else -1
            realized += direction * (exit_price - entry) * remaining
            exit_fee += exit_price * remaining * 0.0004
        entry_fee = entry * qty * 0.0002
        net = realized - entry_fee - exit_fee
        equity += net
        available_after = exit_ts
        trades.append({**candidate, "entry_ts": entry_ts, "entry": entry, "exit_ts": exit_ts, "exit": exit_price, "qty": qty, "reason": exit_reason, "gross": realized, "fees": entry_fee + exit_fee, "net": net})

    def stamp(ts):
        return datetime.fromtimestamp(ts / 1000).astimezone().strftime("%F %T")

    for trade in trades:
        structural_pct = abs(trade["entry"] - trade["stop_anchor"]) / trade["entry"] * 100
        print(f"{stamp(trade['entry_ts'])} {trade['side']:4} {trade['qty']:.4f} BTC  {trade['entry']:.1f} -> {trade['exit']:.1f}  {trade['reason']:9} gross={trade['gross']:+.4f} fee={trade['fees']:.4f} net={trade['net']:+.4f}  {trade['regime']} ctx={trade['score']} structural={structural_pct:.3f}%")
    total = equity - args.cash
    wins = sum(trade["net"] > 0 for trade in trades)
    print(json.dumps({"signals": len(signals), "eligible_candidates": len(candidates), "filled_trades": len(trades), "wins": wins, "win_rate": wins / len(trades) if trades else 0.0, "net_pnl_usdt": round(total, 6), "return_pct": round(total / args.cash * 100, 6), "ending_equity": round(equity, 6), "rejected_gate_counts": rejected}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
