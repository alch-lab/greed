#!/usr/bin/env python3
"""Causal split test for acceleration entries and failed-breakout reversals."""

from __future__ import annotations

import gzip
import importlib.util
import json
import math
import pickle
import sys
import tomllib
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


rp = load_module("split_replay", ROOT / "scripts/replay_altcoin_current_3d.py")
adaptive = load_module("split_adaptive", ROOT / "scripts/exp_altcoin_adaptive_risk.py")


def main() -> None:
    with (ROOT / "config/strategy-altcoin-impulse.toml").open("rb") as handle:
        cfg = tomllib.load(handle)["altcoin_impulse"]
    with gzip.open("/private/tmp/altcoin-fast-grid-bars.pkl.gz", "rb") as handle:
        _, spot_symbols, _, all_bars = pickle.load(handle)
    reports = [
        Path("/private/tmp/altcoin-entry-grid-prior.json"),
        Path("/private/tmp/altcoin-entry-grid-latest.json"),
    ]
    windows = []
    for report in reports:
        period = json.loads(report.read_text())["period"]
        windows.append(
            tuple(int(datetime.fromisoformat(value).timestamp() * 1000) for value in period)
        )
    start_ms, end_ms = windows[0][0], windows[-1][1]
    bars = {symbol: all_bars[symbol] for symbol in spot_symbols if symbol in all_bars}
    batches = rp.build_signal_batches(bars, start_ms, end_ms, cfg)
    with gzip.open("/private/tmp/altcoin-adaptive-minute.pkl.gz", "rb") as handle:
        period, minutes = pickle.load(handle)
    assert period == (start_ms, end_ms)

    variants = [adaptive.Variant("baseline", long_scale=0.75)]
    for side in ("long", "short", "both"):
        for r1 in (0.04, 0.06, 0.08, 0.10):
            for r4 in (0.06, 0.08, 0.10):
                for volume in (4.0, 10.0, 20.0):
                    for risk in (0.15, 0.25, 0.33):
                        variants.append(adaptive.Variant(
                            f"accel_{side}_r1{r1:.2f}_r4{r4:.2f}_v{volume:.0f}_risk{risk:.2f}",
                            long_scale=0.75,
                            acceleration_direct=True,
                            acceleration_side=side,
                            acceleration_min_return_1h=r1,
                            acceleration_min_return_4h=r4,
                            acceleration_min_volume_ratio=volume,
                            acceleration_risk_scale=risk,
                            acceleration_stop_pct=0.01,
                            acceleration_activation_pct=0.015,
                            acceleration_trail_pct=0.005,
                        ))
    for require_retest in (False, True):
        for close_break in (0.0, 0.002, 0.005, 0.010):
            for location in (0.20, 0.30, 0.40):
                for risk in (0.15, 0.25, 0.33):
                    variants.append(adaptive.Variant(
                        f"failure_retest{int(require_retest)}_break{close_break:.3f}_loc{location:.2f}_risk{risk:.2f}",
                        long_scale=0.75,
                        failed_reversal=True,
                        failed_reversal_require_retest=require_retest,
                        failed_reversal_min_close_break_pct=close_break,
                        failed_reversal_max_close_location=location,
                        failed_reversal_risk_scale=risk,
                    ))
    # Non-competing sleeves keep the original pending setup and receive one
    # extra portfolio slot. This distinguishes genuine alpha from portfolio
    # path effects caused by replacing a later confirmed entry.
    for side in ("short", "both"):
        for volume in (10.0, 20.0):
            for risk in (0.15, 0.25, 0.33):
                variants.append(adaptive.Variant(
                    f"accel_separate_{side}_v{volume:.0f}_risk{risk:.2f}",
                    long_scale=0.75,
                    acceleration_direct=True,
                    acceleration_side=side,
                    acceleration_min_return_1h=0.08,
                    acceleration_min_return_4h=0.06,
                    acceleration_min_volume_ratio=volume,
                    acceleration_risk_scale=risk,
                    acceleration_stop_pct=0.01,
                    acceleration_activation_pct=0.015,
                    acceleration_trail_pct=0.005,
                    acceleration_preserve_pending=True,
                    acceleration_skip_cooldown=True,
                    max_positions=3,
                    max_gross_multiple=3.0,
                ))
    for stop in (0.005, 0.0075, 0.010):
        for activation in (0.010, 0.015, 0.020):
            for trail in (0.003, 0.005, 0.0075):
                variants.append(adaptive.Variant(
                    f"failure_exit_s{stop:.4f}_a{activation:.3f}_t{trail:.4f}",
                    long_scale=0.75,
                    failed_reversal=True,
                    failed_reversal_require_retest=True,
                    failed_reversal_min_close_break_pct=0.002,
                    failed_reversal_max_close_location=0.20,
                    failed_reversal_risk_scale=0.15,
                    failed_reversal_stop_pct=stop,
                    failed_reversal_activation_pct=activation,
                    failed_reversal_trail_pct=trail,
                    max_positions=3,
                    max_gross_multiple=3.0,
                ))
    for risk in (0.15, 0.25):
        variants.append(adaptive.Variant(
            f"combined_separate_risk{risk:.2f}",
            long_scale=0.75,
            acceleration_direct=True,
            acceleration_side="short",
            acceleration_min_return_1h=0.08,
            acceleration_min_return_4h=0.06,
            acceleration_min_volume_ratio=20.0,
            acceleration_risk_scale=risk,
            acceleration_stop_pct=0.01,
            acceleration_activation_pct=0.015,
            acceleration_trail_pct=0.005,
            acceleration_preserve_pending=True,
            acceleration_skip_cooldown=True,
            failed_reversal=True,
            failed_reversal_require_retest=True,
            failed_reversal_min_close_break_pct=0.002,
            failed_reversal_max_close_location=0.20,
            failed_reversal_risk_scale=0.15,
            max_positions=3,
            max_gross_multiple=3.0,
        ))

    results = {}
    for variant in variants:
        runs = [
            adaptive.replay_variant(batches, bars, minutes, a, b, cfg, variant)
            for a, b in windows
        ]
        results[variant.name] = {
            "settings": variant.__dict__,
            "windows": runs,
            "compound_return_pct": (
                math.prod(1.0 + run["return_pct"] / 100.0 for run in runs) - 1.0
            ) * 100.0,
            "worst_window_pct": min(run["return_pct"] for run in runs),
            "worst_drawdown_pct": max(run["max_drawdown_pct"] for run in runs),
            "entries": sum(run["entries"] for run in runs),
        }
    baseline = results["baseline"]
    ranked = sorted(
        results.items(),
        key=lambda item: (
            item[1]["worst_window_pct"] >= 0,
            item[1]["compound_return_pct"],
            -item[1]["worst_drawdown_pct"],
        ),
        reverse=True,
    )
    output = {
        "period": [rp.iso(start_ms), rp.iso(end_ms)],
        "baseline": baseline,
        "top": [{"name": name, **value} for name, value in ranked[:30]],
        "variants": results,
    }
    destination = Path("/private/tmp/altcoin-split-entry.json")
    destination.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    for name, value in ranked[:20]:
        branch = "baseline"
        if name.startswith("accel"):
            branch = "acceleration"
        elif name.startswith("failure"):
            branch = "failure_reversal"
        branch_pnl = sum(
            trade["pnl"]
            for run in value["windows"]
            for trade in run["trades"]
            if (
                (branch == "acceleration" and trade["trigger"] == "trend_acceleration_direct")
                or (branch == "failure_reversal" and trade["trigger"] == "failed_breakout_reversal")
            )
        )
        branch_trades = sum(
            trade["trigger"] in {"trend_acceleration_direct", "failed_breakout_reversal"}
            for run in value["windows"]
            for trade in run["trades"]
        )
        print(
            name,
            f"compound={value['compound_return_pct']:.3f}%",
            f"worst={value['worst_window_pct']:.3f}%",
            f"dd={value['worst_drawdown_pct']:.3f}%",
            f"entries={value['entries']}",
            f"branch_trades={branch_trades}",
            f"branch_pnl={branch_pnl:.2f}",
        )


if __name__ == "__main__":
    main()
