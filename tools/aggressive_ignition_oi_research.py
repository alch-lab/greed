#!/usr/bin/env python3
"""Broaden the accepted OI-controlled early-ignition family causally."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import early_ignition_oi_overlay as oi
import early_ignition_passive_execution as passive
import early_ignition_state_research as early


def main() -> None:
    root = Path("data/alpha-backtest/cache")
    minutes = early.load(root, 97)
    candidates = early.precompute(minutes)
    featured = oi.feature_rows(candidates, root)
    start = min(value.entry_ms for value in candidates)
    end = max(value.entry_ms for value in candidates) + early.MINUTE_MS
    days = (end - start) // early.DAY_MS
    folds = {
        "train": (start, start + int(days * .50) * early.DAY_MS),
        "validation": (start + int(days * .50) * early.DAY_MS,
                       start + int(days * .70) * early.DAY_MS),
        "audit": (start + int(days * .70) * early.DAY_MS,
                  start + int(days * .85) * early.DAY_MS),
        "locked": (start + int(days * .85) * early.DAY_MS, end),
    }
    params = [
        (early.EntryParams(impulse, volume, pressure, compression, prior, four_hour,
                           confirmation, side, "aligned"), market_return)
        for impulse in (.003, .005)
        for volume in (1.5, 2.5)
        for pressure in (.05, .15)
        for compression in (.85, 1.05)
        for prior in (.01, .025)
        for four_hour in (.025, .05)
        for confirmation in ("any", "retest")
        for side in (1, 0)
        for market_return in (.002, .005)
    ]
    exit_cfg = early.ExitParams(1.25, 2.0, 30)
    rows = []
    for cfg, minimum_market_return in params:
        chosen = [
            value for value, oi15, oi60, _ in featured
            if value.impulse >= cfg.min_impulse
            and value.volume_ratio >= cfg.min_volume
            and value.pressure >= cfg.min_pressure
            and value.compression <= cfg.max_compression
            and value.prior_move <= cfg.max_prior_move
            and value.four_hour_move <= cfg.max_four_hour_move
            and (cfg.confirmation == "any" or value.confirmation == cfg.confirmation)
            and (cfg.side == 0 or value.side == cfg.side)
            and value.side * value.market_one_hour_move >= minimum_market_return
            # Moderate, rising participation.  Explosive OI is deliberately
            # excluded because it was a late leverage-crowding state.
            and 0.0 <= oi15 <= .01
            and oi60 >= 0.0
        ]
        filled, fill_stats = passive.passive_fill(chosen, 4.0, 2)
        trades8 = early.portfolio(filled, exit_cfg, 8.0)
        trades12 = early.portfolio(filled, exit_cfg, 12.0)
        stats8 = {name: early.stats(trades8, *period) for name, period in folds.items()}
        stats12 = {name: early.stats(trades12, *period) for name, period in folds.items()}
        viable = (
            stats12["train"]["trades"] >= 35
            and stats12["validation"]["trades"] >= 5
            and all(stats12[name]["net_bps"] > 0 and stats12[name]["profit_factor"] > 1.0
                    for name in ("train", "validation"))
        )
        score = min(stats12[name]["profit_factor"] for name in ("train", "validation")) \
            + .1 * min(stats12[name]["trades_per_day"] for name in ("train", "validation"))
        rendered_params = asdict(cfg)
        rendered_params["minimum_directional_market_return_1h"] = minimum_market_return
        rows.append({"params": rendered_params, "fill": fill_stats, "viable": viable,
                     "score": score, "cost_8bps": stats8, "cost_12bps": stats12})
    viable = sorted((row for row in rows if row["viable"]), key=lambda row: row["score"], reverse=True)
    accepted = [row for row in viable
                if row["cost_12bps"]["audit"]["pnl_usd"] > 0
                and row["cost_12bps"]["locked"]["pnl_usd"] > 0
                and row["cost_12bps"]["locked"]["profit_factor"] > 1.0]
    report = {
        "strategy": "aggressive_ignition_oi",
        "symbols": len(minutes),
        "raw_candidates": len(candidates),
        "with_oi": len(featured),
        "grid": len(params),
        "development_survivors": len(viable),
        "accepted_all_folds": len(accepted),
        "selected": (accepted or viable or sorted(rows, key=lambda row: row["score"], reverse=True))[:1],
        "top_accepted": accepted[:10],
    }
    Path("data/alpha-backtest/aggressive-ignition-oi-walkforward.json").write_text(
        json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
