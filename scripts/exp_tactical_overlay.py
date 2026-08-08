#!/usr/bin/env python3
"""Research a flow-filtered range-reversion / trend-pullback overlay.

The experiment consumes the same 10-second evaluation stream as the live engine.
Signals use only information available at their timestamp.  The portfolio remains
single-position, charges production fees, and gives the existing MR/strong-trend
signals priority over a later tactical candidate.
"""

from __future__ import annotations

import bisect
import itertools
import json
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from replay_hybrid_v12 import exit_trade, trend_stats
from replay_hybrid_v13 import ReplayConfig, build_mr_events, build_trend_events, read_jsonl


@dataclass
class TacticalConfig:
    trend_return_pct: float = 0.004
    trend_efficiency: float = 0.10
    trend_pullback_pct: float = 0.0015
    trend_reclaim_pct: float = 0.0008
    range_reclaim_pct: float = 0.0008
    range_edge_pct: float = 0.0003
    min_range_width_pct: float = 0.003
    max_range_width_pct: float = 0.010
    cooldown_ms: int = 30 * 60_000


def flow_supports(flow: dict, side: str) -> bool:
    sign = 1.0 if side == "buy" else -1.0
    return (
        flow.get("source_coverage_complete") is True
        and flow.get("direction") == side
        and int(flow.get("tier") or 0) >= 1
        and float(flow.get("spot_delta_usd") or 0.0) * sign > 0.0
        and float(flow.get("perp_delta_usd") or 0.0) * sign > 0.0
    )


def tactical_events(times, prices, flows, cfg: TacticalConfig):
    events = []
    flow_times = sorted(flows)
    flow_index = 0
    flow = {}
    window_30m: deque[tuple[int, float]] = deque()
    window_60m: deque[tuple[int, float]] = deque()
    trend_episode = None
    trend_extreme = None
    range_setup = None
    cooldown_until = {"range": 0, "pullback": 0}

    for ts, price in zip(times, prices):
        while flow_index < len(flow_times) and flow_times[flow_index] <= ts:
            flow = flows[flow_times[flow_index]]
            flow_index += 1
        while window_30m and window_30m[0][0] < ts - 1_800_000:
            window_30m.popleft()
        while window_60m and window_60m[0][0] < ts - 3_600_000:
            window_60m.popleft()
        prior_30_high = max((item[1] for item in window_30m), default=price)
        prior_30_low = min((item[1] for item in window_30m), default=price)
        prior_60_high = max((item[1] for item in window_60m), default=price)
        prior_60_low = min((item[1] for item in window_60m), default=price)
        slow_return, slow_efficiency = trend_stats(times, prices, ts)
        trend_side = (
            "buy"
            if slow_return >= cfg.trend_return_pct and slow_efficiency >= cfg.trend_efficiency
            else "sell"
            if slow_return <= -cfg.trend_return_pct and slow_efficiency >= cfg.trend_efficiency
            else None
        )

        if trend_side != trend_episode:
            trend_episode = trend_side
            trend_extreme = None
        if trend_side == "buy":
            trend_extreme = price if trend_extreme is None else min(trend_extreme, price)
            pulled = price <= prior_30_high * (1.0 - cfg.trend_pullback_pct)
            if pulled:
                trend_extreme = min(trend_extreme, price)
            reclaimed = trend_extreme is not None and price >= trend_extreme * (1.0 + cfg.trend_reclaim_pct)
            if ts >= cooldown_until["pullback"] and pulled and reclaimed and flow_supports(flow, "buy"):
                events.append({"ts": ts, "strategy": "trend", "sleeve": "pullback", "side": "buy", "entry": price})
                cooldown_until["pullback"] = ts + cfg.cooldown_ms
                trend_extreme = price
        elif trend_side == "sell":
            trend_extreme = price if trend_extreme is None else max(trend_extreme, price)
            pulled = price >= prior_30_low * (1.0 + cfg.trend_pullback_pct)
            if pulled:
                trend_extreme = max(trend_extreme, price)
            reclaimed = trend_extreme is not None and price <= trend_extreme * (1.0 - cfg.trend_reclaim_pct)
            if ts >= cooldown_until["pullback"] and pulled and reclaimed and flow_supports(flow, "sell"):
                events.append({"ts": ts, "strategy": "trend", "sleeve": "pullback", "side": "sell", "entry": price})
                cooldown_until["pullback"] = ts + cfg.cooldown_ms
                trend_extreme = price

        is_range = trend_side is None
        width = prior_60_high / prior_60_low - 1.0 if prior_60_low > 0.0 else 0.0
        if is_range and cfg.min_range_width_pct <= width <= cfg.max_range_width_pct:
            if range_setup is None:
                # First observe aggressive flow pressing into the edge.  Entry is
                # allowed only after that pressure flips, avoiding blind catches.
                if price <= prior_60_low * (1.0 + cfg.range_edge_pct) and flow_supports(flow, "sell"):
                    range_setup = {"side": "buy", "extreme": price, "expires": ts + 10 * 60_000}
                elif price >= prior_60_high * (1.0 - cfg.range_edge_pct) and flow_supports(flow, "buy"):
                    range_setup = {"side": "sell", "extreme": price, "expires": ts + 10 * 60_000}
            else:
                side = range_setup["side"]
                range_setup["extreme"] = (
                    min(range_setup["extreme"], price)
                    if side == "buy"
                    else max(range_setup["extreme"], price)
                )
                reclaimed = (
                    price >= range_setup["extreme"] * (1.0 + cfg.range_reclaim_pct)
                    if side == "buy"
                    else price <= range_setup["extreme"] * (1.0 - cfg.range_reclaim_pct)
                )
                if ts > range_setup["expires"]:
                    range_setup = None
                elif reclaimed and flow_supports(flow, side):
                    if ts >= cooldown_until["range"]:
                        events.append({"ts": ts, "strategy": "mr", "sleeve": "range", "side": side, "entry": price})
                        cooldown_until["range"] = ts + cfg.cooldown_ms
                    range_setup = None
        else:
            range_setup = None

        window_30m.append((ts, price))
        window_60m.append((ts, price))
    return events


def replay(eval_dir: Path, cash: float, tactical: TacticalConfig):
    equity = peak = cash
    max_drawdown = 0.0
    trades = []
    daily = defaultdict(float)
    for eval_path in sorted(eval_dir.glob("*.jsonl")):
        rows = list(read_jsonl(eval_path))
        times, prices, base = build_mr_events(rows, ReplayConfig())
        if len(times) < 5_000:
            continue
        flows = {
            int(row["ts_ms"]): (row.get("note") or {})["flow"]
            for row in rows
            if row.get("source") == "TrdrMarketMap"
            and isinstance((row.get("note") or {}).get("flow"), dict)
        }
        base += build_trend_events(times, prices, flows, ReplayConfig())
        overlay = tactical_events(times, prices, flows, tactical)
        events = base + overlay
        events.sort(key=lambda item: (item["ts"], 0 if "sleeve" not in item else 1))
        available = times[0]
        for event in events:
            if event["ts"] < available:
                continue
            index = bisect.bisect_left(times, event["ts"])
            if index >= len(times):
                continue
            if event["strategy"] == "trend" or "sleeve" in event:
                entry_ts, entry = times[index], prices[index]
            else:
                begin = index
                end = bisect.bisect_right(times, event["ts"] + 600_000)
                fill = (
                    next((i for i in range(begin, end) if prices[i] <= event["entry"]), None)
                    if event["side"] == "buy"
                    else next((i for i in range(begin, end) if prices[i] >= event["entry"]), None)
                )
                if fill is None:
                    available = event["ts"] + 600_000
                    continue
                entry_ts, entry = times[fill], event["entry"]
            qty = int(min(equity / entry, equity * 3 / entry) * 10_000) / 10_000
            exit_ts, exit_price, gross, fees, reason, stage = exit_trade(
                event, entry_ts, entry, qty, times, prices
            )
            net = gross - fees
            equity += net
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
            available = exit_ts
            sleeve = event.get("sleeve") or event["strategy"]
            day = datetime.fromtimestamp(entry_ts / 1000).astimezone().strftime("%F")
            daily[day] += net
            trades.append({**event, "sleeve": sleeve, "entry_ts": entry_ts, "entry": entry, "exit": exit_price, "net": net})
    by_sleeve = {}
    for sleeve in ("mr", "trend", "pullback", "range"):
        subset = [trade for trade in trades if trade["sleeve"] == sleeve]
        by_sleeve[sleeve] = (len(subset), sum(item["net"] > 0 for item in subset), sum(item["net"] for item in subset))
    return {
        "trades": len(trades),
        "wins": sum(item["net"] > 0 for item in trades),
        "net": equity - cash,
        "max_dd": max_drawdown * 100.0,
        "by_sleeve": by_sleeve,
        "daily": dict(daily),
        "trade_rows": trades,
    }


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-dir", required=True, type=Path)
    parser.add_argument("--cash", default=5_000.0, type=float)
    parser.add_argument("--grid", action="store_true")
    parser.add_argument("--trend-return-pct", default=0.004, type=float)
    parser.add_argument("--trend-pullback-pct", default=0.0015, type=float)
    parser.add_argument("--trend-reclaim-pct", default=0.0008, type=float)
    parser.add_argument("--disable-range", action="store_true")
    args = parser.parse_args()
    configs = [TacticalConfig(
        trend_return_pct=args.trend_return_pct,
        trend_pullback_pct=args.trend_pullback_pct,
        trend_reclaim_pct=args.trend_reclaim_pct,
        max_range_width_pct=0.0 if args.disable_range else 0.010,
    )]
    if args.grid:
        configs = [
            TacticalConfig(trend_return_pct=trend_ret, trend_pullback_pct=pull, trend_reclaim_pct=reclaim, range_reclaim_pct=0.0008)
            for trend_ret, pull, reclaim in itertools.product((0.003, 0.004, 0.005), (0.0010, 0.0015, 0.0020), (0.0006, 0.0008, 0.0010))
        ]
    ranked = [(replay(args.eval_dir, args.cash, config), config) for config in configs]
    ranked.sort(key=lambda item: (min(item[0]["daily"].values(), default=-999.0), item[0]["net"]), reverse=True)
    for result, config in ranked[:10]:
        print(config)
        print(json.dumps({key: value for key, value in result.items() if key != "trade_rows"}, ensure_ascii=False, indent=2, default=list))


if __name__ == "__main__":
    main()
