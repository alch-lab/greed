#!/usr/bin/env python3
"""Replay the V13 event-fusion MR + trend portfolio without look-ahead.

Unlike the V12 replay, this script reconstructs the order-flow state machine from
the 10-second eval stream.  That lets it test two production fixes that are not
present in old ``signal_confirmed`` rows:

* context may arrive after the volume event while its confirmation window is open;
* cooldown is direction-scoped and an exceptional/new-price event may re-arm it.

The replay still uses observed 10-second prices, conservative passive fills and
the production fee/exit assumptions.  It cannot model intrabucket queue priority.
"""

from __future__ import annotations

import argparse
import bisect
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from replay_hybrid_v12 import exit_trade, trend_stats


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def opposite(side: str) -> str:
    return "sell" if side == "buy" else "buy"


@dataclass
class ReplayConfig:
    classic_volume_ratio: float = 1.60
    strong_volume_ratio: float = 2.20
    min_delta_share: float = 0.15
    strong_delta_share: float = 0.35
    confirm_delta_share: float = 0.08
    absorption_efficiency: float = 0.45 * 0.65
    strong_confirm_ms: int = 180_000
    weak_confirm_ms: int = 300_000
    cooldown_ms: int = 600_000
    rearm_volume_ratio: float = 50.0
    rearm_price_pct: float = 0.005
    min_context_score: int = 5
    min_zone_persistence_ms: int = 30_000
    max_zone_distance_pct: float = 0.004
    min_stacked_imbalance: int = 2
    min_liquidation_usd: float = 1_000_000.0
    min_vwap_deviation_pct: float = 0.003
    trend_return_pct: float = 0.008
    trend_efficiency: float = 0.15
    trend_warmup_ms: int = 7_200_000


def market_context(note: dict, side: str, cfg: ReplayConfig) -> dict:
    zone = note.get("zone") or {}
    flow = note.get("flow") or {}
    pressure = opposite(side)
    distance = zone.get("distance_pct")
    zone_matches = (
        zone.get("active") is True
        and zone.get("side") == side
        and int(zone.get("grade_rank") or 0) >= 1
        and isinstance(distance, (int, float))
        and abs(float(distance)) <= cfg.max_zone_distance_pct
        and int(zone.get("persistence_ms") or 0) >= cfg.min_zone_persistence_ms
        and zone.get("spot_perp_confluence") is True
    )
    delta_matches = (
        int(flow.get("tier") or 0) >= 1 and flow.get("direction") == pressure
    )
    footprint_matches = (
        flow.get("footprint_direction") == pressure
        and int(flow.get("stacked_imbalance") or 0) >= cfg.min_stacked_imbalance
    )
    liquidation = float(
        (flow.get("long_liquidation_usd") if side == "buy" else flow.get("short_liquidation_usd"))
        or 0.0
    ) >= cfg.min_liquidation_usd
    center = None
    if isinstance(zone.get("zone_low"), (int, float)) and isinstance(
        zone.get("zone_high"), (int, float)
    ):
        center = (float(zone["zone_low"]) + float(zone["zone_high"])) / 2.0
    return {
        "zone": zone_matches,
        "delta": delta_matches,
        "footprint": footprint_matches,
        "coverage": flow.get("source_coverage_complete") is True,
        "oi": (flow.get("oi_quadrant") or "neutral") != "neutral",
        "confluence": zone_matches,
        "liquidation": liquidation,
        "zone_center": center,
        "zone_grade": zone.get("grade") if zone_matches else None,
    }


def context_score(setup: dict) -> int:
    return sum(
        bool(setup[key])
        for key in (
            "location",
            "absorption",
            "delta",
            "footprint",
            "oi",
            "confluence",
            "liquidation",
        )
    )


def fuse_context(setup: dict, market: dict, ts: int) -> None:
    for key in ("delta", "footprint", "coverage", "oi", "liquidation"):
        setup[key] = setup[key] or market[key]
    if market["zone"]:
        setup["zone"] = True
        setup["location"] = True
        setup["confluence"] = True
        setup["context_fused"] = setup["context_fused"] or ts > setup["event_ts"]
        setup["zone_grade"] = market["zone_grade"]
        if market["zone_center"] is not None:
            setup["entry"] = market["zone_center"]
    setup["score"] = context_score(setup)


def build_mr_events(eval_rows: list[dict], cfg: ReplayConfig):
    orderflow = {
        int(row["ts_ms"]): row.get("note") or {}
        for row in eval_rows
        if row.get("source") == "OrderFlowExhaustion"
        and isinstance((row.get("note") or {}).get("price"), (int, float))
    }
    market_maps = {
        int(row["ts_ms"]): row.get("note") or {}
        for row in eval_rows
        if row.get("source") == "TrdrMarketMap"
    }
    times = sorted(orderflow)
    map_times = sorted(market_maps)
    map_index = 0
    latest_map: dict = {}
    setup = None
    cooldown_until = {"buy": 0, "sell": 0}
    cooldown_price = {"buy": None, "sell": None}
    events = []
    previous_price = None

    for ts in times:
        note = orderflow[ts]
        price = float(note["price"])
        while map_index < len(map_times) and map_times[map_index] <= ts:
            latest_map = market_maps[map_times[map_index]]
            map_index += 1

        if setup is not None:
            fuse_context(setup, market_context(latest_map, setup["side"], cfg), ts)
            if ts > setup["expires_at"]:
                setup = None
            else:
                delta_share = float(note.get("delta_share") or 0.0)
                reverse_delta = (
                    delta_share >= cfg.confirm_delta_share
                    if setup["side"] == "buy"
                    else delta_share <= -cfg.confirm_delta_share
                )
                reverse_price = False
                if previous_price is not None:
                    reverse_price = (
                        price > previous_price and price > setup["event_price"]
                        if setup["side"] == "buy"
                        else price < previous_price and price < setup["event_price"]
                    )
                if reverse_delta and reverse_price:
                    structural_pct = abs(setup["entry"] - setup["stop_anchor"]) / setup["entry"]
                    eligible = (
                        setup["score"] >= cfg.min_context_score
                        and setup["absorption"]
                        and setup["location"]
                        and setup["coverage"]
                        and setup["footprint"]
                        and structural_pct <= 0.0025 + 1e-9
                    )
                    if eligible:
                        events.append(
                            {
                                "ts": ts,
                                "strategy": "mr",
                                "side": setup["side"],
                                "entry": setup["entry"],
                                "context_score": setup["score"],
                                "context_fused": setup["context_fused"],
                                "zone_grade": setup["zone_grade"],
                            }
                        )
                    cooldown_until[setup["side"]] = ts + cfg.cooldown_ms
                    cooldown_price[setup["side"]] = setup["event_price"]
                    setup = None

        delta_share = float(note.get("delta_share") or 0.0)
        volume_ratio = float(note.get("volume_ratio") or 0.0)
        pressure = (
            "buy"
            if delta_share >= cfg.min_delta_share
            else "sell"
            if delta_share <= -cfg.min_delta_share
            else None
        )
        volume_event = (
            volume_ratio >= cfg.classic_volume_ratio
            and pressure is not None
            and (note.get("funnel") or {}).get("stalled") is True
        )
        if volume_event:
            side = opposite(pressure)
            price_anchor = cooldown_price[side]
            new_price_region = price_anchor is not None and abs(price / price_anchor - 1.0) >= cfg.rearm_price_pct
            exceptional = volume_ratio >= cfg.rearm_volume_ratio or new_price_region
            pending_replace = setup is not None and exceptional and (
                setup["side"] != side or volume_ratio >= cfg.rearm_volume_ratio
            )
            can_arm = setup is None or pending_replace
            if can_arm and (ts >= cooldown_until[side] or exceptional):
                market = market_context(latest_map, side, cfg)
                vwap_deviation = float(note.get("vwap_deviation_pct") or 0.0)
                local_location = (
                    bool(note.get("swept_low"))
                    or vwap_deviation <= -cfg.min_vwap_deviation_pct
                    if side == "buy"
                    else bool(note.get("swept_high"))
                    or vwap_deviation >= cfg.min_vwap_deviation_pct
                )
                absorption = float(note.get("efficiency") or 0.0) <= cfg.absorption_efficiency
                strong = (
                    volume_ratio >= cfg.strong_volume_ratio
                    or abs(delta_share) >= cfg.strong_delta_share
                )
                entry = market["zone_center"] if market["zone"] else price
                stop_anchor = entry * (0.9975 if side == "buy" else 1.0025)
                setup = {
                    "event_ts": ts,
                    "event_price": price,
                    "entry": entry,
                    "stop_anchor": stop_anchor,
                    "side": side,
                    "expires_at": ts
                    + (cfg.strong_confirm_ms if strong else cfg.weak_confirm_ms),
                    "location": market["zone"] or local_location,
                    "absorption": absorption,
                    "zone": market["zone"],
                    "delta": market["delta"],
                    "footprint": market["footprint"],
                    "coverage": market["coverage"],
                    "oi": market["oi"]
                    or abs(float(note.get("oi_change_pct") or 0.0)) >= 0.0005,
                    "confluence": market["confluence"],
                    "liquidation": market["liquidation"],
                    "zone_grade": market["zone_grade"],
                    "context_fused": False,
                }
                setup["score"] = context_score(setup)
        previous_price = price
    return times, [float(orderflow[ts]["price"]) for ts in times], events


def build_trend_events(times, prices, flows, cfg: ReplayConfig):
    events = []
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
        side = (
            "buy"
            if slow_return >= cfg.trend_return_pct
            and slow_efficiency >= cfg.trend_efficiency
            else "sell"
            if slow_return <= -cfg.trend_return_pct
            and slow_efficiency >= cfg.trend_efficiency
            else None
        )
        if side is None:
            neutral_since = ts if neutral_since is None else neutral_since
            if ts - neutral_since >= 900_000:
                fired = None
            continue
        neutral_since = None
        if fired is not None and fired != side:
            fired = None
        if fired == side or ts - times[0] < cfg.trend_warmup_ms:
            continue
        sign = 1 if side == "buy" else -1
        oi = flow.get("oi_quadrant")
        oi_ready = oi in (
            {"new_longs", "short_covering", "short_cover"}
            if sign > 0
            else {"new_shorts", "long_liquidation"}
        )
        ready = (
            flow.get("source_coverage_complete") is True
            and flow.get("direction") == side
            and int(flow.get("tier") or 0) >= 1
            and float(flow.get("spot_delta_usd") or 0.0) * sign > 0.0
            and float(flow.get("perp_delta_usd") or 0.0) * sign > 0.0
            and oi_ready
        )
        if ready:
            events.append({"ts": ts, "strategy": "trend", "side": side, "entry": price})
            fired = side
    return events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-dir", required=True, type=Path)
    parser.add_argument("--cash", type=float, default=5_000.0)
    parser.add_argument("--rearm-volume-ratio", type=float, default=50.0)
    parser.add_argument("--rearm-price-pct", type=float, default=0.005)
    parser.add_argument("--trend-return-pct", type=float, default=0.008)
    parser.add_argument("--trend-efficiency", type=float, default=0.15)
    parser.add_argument("--trend-warmup-ms", type=int, default=7_200_000)
    args = parser.parse_args()
    cfg = ReplayConfig(
        rearm_volume_ratio=args.rearm_volume_ratio,
        rearm_price_pct=args.rearm_price_pct,
        trend_return_pct=args.trend_return_pct,
        trend_efficiency=args.trend_efficiency,
        trend_warmup_ms=args.trend_warmup_ms,
    )

    equity = args.cash
    peak_equity = equity
    max_drawdown = 0.0
    trades = []
    daily = defaultdict(float)
    for eval_path in sorted(args.eval_dir.glob("*.jsonl")):
        eval_rows = list(read_jsonl(eval_path))
        times, prices, events = build_mr_events(eval_rows, cfg)
        if len(times) < 5_000:
            continue
        flows = {
            int(row["ts_ms"]): (row.get("note") or {})["flow"]
            for row in eval_rows
            if row.get("source") == "TrdrMarketMap"
            and isinstance((row.get("note") or {}).get("flow"), dict)
        }
        events += build_trend_events(times, prices, flows, cfg)
        events.sort(key=lambda item: (item["ts"], 0 if item["strategy"] == "trend" else 1))
        available = times[0]
        for event in events:
            if event["ts"] < available:
                continue
            if event["strategy"] == "trend":
                index = bisect.bisect_left(times, event["ts"])
                if index >= len(times):
                    continue
                entry_ts, entry = times[index], prices[index]
            else:
                begin = bisect.bisect_left(times, event["ts"])
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
            qty = int(min(equity * 0.0025 / (entry * 0.0025), equity * 3 / entry) * 10_000) / 10_000
            exit_ts, exit_price, gross, fees, reason, stage = exit_trade(
                event, entry_ts, entry, qty, times, prices
            )
            net = gross - fees
            equity += net
            peak_equity = max(peak_equity, equity)
            max_drawdown = max(max_drawdown, (peak_equity - equity) / peak_equity)
            available = exit_ts
            day = datetime.fromtimestamp(entry_ts / 1000).astimezone().strftime("%F")
            daily[day] += net
            trades.append(
                {
                    **event,
                    "entry_ts": entry_ts,
                    "entry": entry,
                    "exit_ts": exit_ts,
                    "exit": exit_price,
                    "qty": qty,
                    "gross": gross,
                    "fees": fees,
                    "net": net,
                    "reason": reason,
                    "stage": stage,
                }
            )

    for trade in trades:
        stamp = datetime.fromtimestamp(trade["entry_ts"] / 1000).astimezone().strftime("%F %T")
        fusion = " fused" if trade.get("context_fused") else ""
        print(
            f"{stamp} {trade['strategy']:5} {trade['side']:4} "
            f"{trade['qty']:.4f} BTC ${trade['entry'] * trade['qty']:.0f} "
            f"{trade['entry']:.1f}->{trade['exit']:.1f} {trade['reason']:14} "
            f"net={trade['net']:+.2f}{fusion}"
        )
    by_strategy = {}
    for strategy in ("mr", "trend"):
        subset = [trade for trade in trades if trade["strategy"] == strategy]
        by_strategy[strategy] = {
            "trades": len(subset),
            "wins": sum(trade["net"] > 0 for trade in subset),
            "net_pnl": round(sum(trade["net"] for trade in subset), 4),
        }
    print(
        json.dumps(
            {
                "trades": len(trades),
                "wins": sum(trade["net"] > 0 for trade in trades),
                "win_rate": sum(trade["net"] > 0 for trade in trades) / len(trades)
                if trades
                else 0.0,
                "net_pnl_usdt": round(equity - args.cash, 4),
                "return_pct": round((equity / args.cash - 1.0) * 100.0, 4),
                "max_drawdown_pct": round(max_drawdown * 100.0, 4),
                "by_strategy": by_strategy,
                "daily_pnl": {key: round(value, 4) for key, value in daily.items()},
                "parameters": {
                    "rearm_volume_ratio": cfg.rearm_volume_ratio,
                    "rearm_price_pct": cfg.rearm_price_pct,
                    "trend_return_pct": cfg.trend_return_pct,
                    "trend_efficiency": cfg.trend_efficiency,
                    "trend_warmup_ms": cfg.trend_warmup_ms,
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
