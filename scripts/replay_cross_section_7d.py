#!/usr/bin/env python3
"""Causal seven-day replay of the deployed 12h/3h cross-section sleeve.

Loads the market cache produced by replay_altcoin_current_3d.py.  The rolling
30-basket gate is updated only after each shadow basket has completed.  Actual
positions are modeled as independent 3h cohorts, which conservatively charges
round-trip costs on every rebalance instead of assuming free same-symbol carry.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import pickle
import tomllib
from collections import deque
from pathlib import Path

import replay_altcoin_current_3d as rp

# The producer is commonly executed as a script, so pickle records Bar under
# __main__. Expose the identical dataclass here before loading that cache.
Bar = rp.Bar


def ranks_at(bars_by_symbol, indexes, boundary, cfg):
    formation = int(cfg["formation_hours"]) * 4
    ranks = []
    signal_ts = boundary - rp.BAR_MS
    for symbol, bars in bars_by_symbol.items():
        i = indexes[symbol].get(signal_ts)
        if i is None or i < max(formation, 95):
            continue
        volume = sum(bar.quote_volume for bar in bars[i - 95 : i + 1])
        if volume < float(cfg["min_24h_volume_usd"]):
            continue
        value = bars[i].close / bars[i - formation].close - 1.0
        if abs(value) <= 1.5:
            ranks.append((value, symbol, i))
    ranks.sort()
    return ranks


def select(ranks, cfg):
    names = int(cfg["names"])
    if len(ranks) < names:
        return None
    market = ranks[len(ranks) // 2][0]
    if market >= float(cfg["market_momentum_threshold"]):
        chosen, side, regime = list(reversed(ranks[-names:])), 1, "broad_up"
        if any(value <= 0 for value, _, _ in chosen):
            return None
    else:
        chosen, side = ranks[:names], -1
        regime = "broad_down_short" if market <= -float(cfg["market_momentum_threshold"]) else "neutral_short_bias"
        if any(value >= 0 for value, _, _ in chosen):
            return None
    excess = abs(chosen[0][0] - market)
    return chosen, side, market, regime, excess


def select_always_long(ranks, cfg):
    """Ablation: ignore breadth and always buy the strongest names."""
    names = int(cfg["names"])
    if len(ranks) < names:
        return None
    market = ranks[len(ranks) // 2][0]
    chosen = list(reversed(ranks[-names:]))
    excess = abs(chosen[0][0] - market)
    return chosen, 1, market, "always_long_no_breadth", excess


def fixed_shadow_return(bars, i, side, hold_bars, stop_pct, cost_bps):
    entry_i = i + 1
    exit_i = entry_i + hold_bars
    if exit_i >= len(bars):
        return None
    entry = bars[entry_i].open
    stop = entry * (1.0 - side * stop_pct)
    raw_exit = bars[exit_i].open
    for bar in bars[entry_i:exit_i]:
        if (bar.low <= stop if side > 0 else bar.high >= stop):
            raw_exit = bar.open if (bar.open < stop if side > 0 else bar.open > stop) else stop
            break
    return side * (raw_exit / entry - 1.0) - 2 * cost_bps / 10_000.0


def managed_return(bars, i, side, hold_bars, stop_pct, activation, trail, partial):
    entry_i = i + 1
    exit_i = entry_i + hold_bars
    if exit_i >= len(bars):
        return None
    slip = fee = 0.0005
    entry = bars[entry_i].open * (1.0 + side * slip)
    stop = entry * (1.0 - side * stop_pct)
    extreme = entry
    remaining = 1.0
    result = -fee
    partial_done = False

    def close_leg(raw, fraction):
        price = raw * (1.0 - side * slip)
        return fraction * (side * (price / entry - 1.0) - fee * price / entry)

    for bar in bars[entry_i:exit_i]:
        if (bar.low <= stop if side > 0 else bar.high >= stop):
            return result + close_leg(stop, remaining)
        extreme = max(extreme, bar.high) if side > 0 else min(extreme, bar.low)
        mfe = side * (extreme / entry - 1.0)
        if not partial_done and mfe >= activation:
            result += close_leg(entry * (1.0 + side * activation), partial)
            remaining -= partial
            partial_done = True
            stop = entry
        if mfe >= activation:
            proposed = extreme * (1.0 - side * trail)
            stop = max(stop, proposed) if side > 0 else min(stop, proposed)
            if (bar.low <= stop if side > 0 else bar.high >= stop):
                return result + close_leg(stop, remaining)
    return result + close_leg(bars[exit_i].open, remaining)


def replay(series, start_ms, end_ms, gate_window, gate_pf, params, gate_enabled=True):
    history = deque(maxlen=gate_window)
    equity = peak = 1_000.0
    max_dd = 0.0
    traded = blocked = wins = 0
    outcomes = []
    regimes = {}
    for item in series:
        if item["boundary"] < start_ms:
            history.append(item["shadow"])
            continue
        if item["boundary"] >= end_ms:
            break
        gains = sum(max(value, 0.0) for value in history)
        losses = sum(max(-value, 0.0) for value in history)
        pf = gains / losses if losses else 99.0
        gate_open = (
            not gate_enabled
            or (len(history) == gate_window and sum(history) > 0 and pf >= gate_pf)
        )
        history.append(item["shadow"])
        if not gate_open:
            blocked += 1
            continue
        gross = float(params["strong_gross"] if item["excess"] >= params["strong_excess"] else params["active_gross"])
        basket = sum(item["managed"](params) for _ in [0]) * gross
        equity *= max(0.01, 1.0 + basket)
        peak = max(peak, equity)
        max_dd = max(max_dd, 1.0 - equity / peak)
        traded += 1
        wins += basket > 0
        outcomes.append(basket)
        regimes[item["regime"]] = regimes.get(item["regime"], 0) + 1
    gp = sum(max(value, 0.0) for value in outcomes)
    gl = sum(max(-value, 0.0) for value in outcomes)
    return {
        "return_pct": (equity / 1_000.0 - 1.0) * 100.0,
        "max_drawdown_pct": max_dd * 100.0,
        "baskets": traded,
        "gate_blocked_baskets": blocked,
        "win_rate_pct": wins / traded * 100.0 if traded else 0.0,
        "profit_factor": gp / gl if gl else None,
        "traded_regimes": regimes,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", required=True)
    parser.add_argument("--strategy", default="config/strategy-altcoin-impulse.toml")
    parser.add_argument("--output", default="/tmp/greed-cross-section-7d.json")
    args = parser.parse_args()
    with gzip.open(args.cache, "rb") as handle:
        cached = pickle.load(handle)
    with open(args.strategy, "rb") as handle:
        document = tomllib.load(handle)
    cfg = document["altcoin_cross_section"]
    bars_by_symbol = cached["bars_15m"]
    indexes = {symbol: {bar.ts: i for i, bar in enumerate(bars)} for symbol, bars in bars_by_symbol.items()}
    start_ms, end_ms = cached["start_ms"], cached["end_ms"]
    hold_bars = int(cfg["hold_hours"]) * 4
    warmup_start = start_ms - int(cfg["gate_window"]) * int(cfg["hold_hours"]) * 3_600_000
    first = ((warmup_start + int(cfg["hold_hours"]) * 3_600_000 - 1) // (int(cfg["hold_hours"]) * 3_600_000)) * (int(cfg["hold_hours"]) * 3_600_000)
    raw = []
    raw_always_long = []
    for boundary in range(first, end_ms, int(cfg["hold_hours"]) * 3_600_000):
        ranks = ranks_at(bars_by_symbol, indexes, boundary, cfg)
        for series, chosen in (
            (raw, select(ranks, cfg)),
            (raw_always_long, select_always_long(ranks, cfg)),
        ):
            if not chosen:
                continue
            legs, side, market, regime, excess = chosen
            shadow_legs = [
                fixed_shadow_return(
                    bars_by_symbol[symbol],
                    i,
                    side,
                    hold_bars,
                    float(cfg["stop_pct"]),
                    float(cfg["assumed_cost_bps_per_side"]),
                )
                for _, symbol, i in legs
            ]
            if any(value is None for value in shadow_legs):
                continue

            def managed(params, legs=legs, side=side):
                values = [
                    managed_return(
                        bars_by_symbol[symbol],
                        i,
                        side,
                        hold_bars,
                        params["stop_pct"],
                        params["activation_pct"],
                        params["trail_pct"],
                        params["partial_fraction"],
                    )
                    for _, symbol, i in legs
                ]
                return sum(values) / len(values)

            series.append(
                {
                    "boundary": boundary,
                    "shadow": sum(shadow_legs) / len(shadow_legs),
                    "managed": managed,
                    "market": market,
                    "regime": regime,
                    "excess": excess,
                }
            )
    variants = []
    for active_gross in (0.5, 0.75, 1.0):
      for strong_gross in (active_gross, active_gross * 1.5):
       for stop in (0.02, 0.03, 0.04):
        for activation in (0.015, 0.02, 0.03, 0.05):
         for trail in (0.005, 0.0075, 0.01, 0.02):
          if trail >= activation:
           continue
          for partial in (0.4, 0.5):
                    params = {
                        "stop_pct": stop,
                        "activation_pct": activation,
                        "trail_pct": trail,
                        "partial_fraction": partial,
                        "active_gross": active_gross,
                        "strong_gross": strong_gross,
                        "strong_excess": float(cfg["strong_excess_return"]),
                    }
                    variants.append({"parameters": params, "result": replay(raw, start_ms, end_ms, int(cfg["gate_window"]), float(cfg["gate_min_profit_factor"]), params)})
    variants.sort(key=lambda item: (item["result"]["return_pct"], -item["result"]["max_drawdown_pct"]), reverse=True)
    positive = [item for item in variants if item["result"]["return_pct"] > 0.0]
    positive.sort(key=lambda item: (item["result"]["max_drawdown_pct"], -item["result"]["return_pct"]))
    current_params = {
        "stop_pct": float(cfg["stop_pct"]),
        "activation_pct": float(cfg["trail_activation_pct"]),
        "trail_pct": float(cfg["trail_pct"]),
        "partial_fraction": float(cfg["partial_take_profit_fraction"]),
        "active_gross": float(cfg["active_gross_multiple"]),
        "strong_gross": float(cfg["strong_gross_multiple"]),
        "strong_excess": float(cfg["strong_excess_return"]),
    }
    old_live_params = {
        "stop_pct": 0.04,
        "activation_pct": 0.05,
        "trail_pct": 0.02,
        "partial_fraction": 0.33,
        "active_gross": 1.0,
        "strong_gross": 1.5,
        "strong_excess": float(cfg["strong_excess_return"]),
    }
    current = replay(
        raw,
        start_ms,
        end_ms,
        int(cfg["gate_window"]),
        float(cfg["gate_min_profit_factor"]),
        current_params,
    )
    output = {
        "period": [rp.iso(start_ms), rp.iso(end_ms)],
        "assumptions": {
            "gate": f"causal {cfg['gate_window']}-basket PF >= {cfg['gate_min_profit_factor']}",
            "costs": "5bp adverse slippage + 5bp taker fee per fill",
            "execution": "independent 3h cohorts; same-symbol carry not credited; 15m stop-first",
            "historical_l2": "unavailable",
        },
        "signals": len(raw),
        "current": current,
        "ablations": {
            "breadth_pf_shrunk": {
                "parameters": current_params,
                "result": current,
            },
            "breadth_no_pf_shrunk": {
                "parameters": current_params,
                "result": replay(
                    raw,
                    start_ms,
                    end_ms,
                    int(cfg["gate_window"]),
                    float(cfg["gate_min_profit_factor"]),
                    current_params,
                    gate_enabled=False,
                ),
            },
            "breadth_pf_old_size_and_exit": {
                "parameters": old_live_params,
                "result": replay(
                    raw,
                    start_ms,
                    end_ms,
                    int(cfg["gate_window"]),
                    float(cfg["gate_min_profit_factor"]),
                    old_live_params,
                ),
            },
            "breadth_no_pf_old_size_and_exit": {
                "parameters": old_live_params,
                "result": replay(
                    raw,
                    start_ms,
                    end_ms,
                    int(cfg["gate_window"]),
                    float(cfg["gate_min_profit_factor"]),
                    old_live_params,
                    gate_enabled=False,
                ),
            },
            "always_long_no_breadth_no_pf_shrunk": {
                "parameters": current_params,
                "result": replay(
                    raw_always_long,
                    start_ms,
                    end_ms,
                    int(cfg["gate_window"]),
                    float(cfg["gate_min_profit_factor"]),
                    current_params,
                    gate_enabled=False,
                ),
            },
            "always_long_no_breadth_pf_shrunk": {
                "parameters": current_params,
                "result": replay(
                    raw_always_long,
                    start_ms,
                    end_ms,
                    int(cfg["gate_window"]),
                    float(cfg["gate_min_profit_factor"]),
                    current_params,
                ),
            },
        },
        "top": variants[:20],
        "lowest_drawdown_positive": positive[:20],
    }
    Path(args.output).write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"current": output["current"], "best": variants[0]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
