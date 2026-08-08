#!/usr/bin/env python3
"""Exact selected V14 portfolio replay (V13 + flow-confirmed trend pullback)."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from exp_tactical_overlay import TacticalConfig, replay


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--eval-dir", required=True, type=Path)
    parser.add_argument("--cash", default=5_000.0, type=float)
    args = parser.parse_args()
    config = TacticalConfig(
        trend_return_pct=0.005,
        trend_efficiency=0.10,
        trend_pullback_pct=0.002,
        trend_reclaim_pct=0.0006,
        max_range_width_pct=0.0,
    )
    result = replay(args.eval_dir, args.cash, config)
    for trade in result.pop("trade_rows"):
        stamp = datetime.fromtimestamp(trade["entry_ts"] / 1000).astimezone().strftime("%F %T")
        print(
            f"{stamp} {trade['sleeve']:8} {trade['side']:4} "
            f"{trade['entry']:.1f}->{trade['exit']:.1f} net={trade['net']:+.2f}"
        )
    result["return_pct"] = result["net"] / args.cash * 100.0
    result["parameters"] = {
        "trend_return_pct": config.trend_return_pct,
        "trend_efficiency": config.trend_efficiency,
        "trend_pullback_pct": config.trend_pullback_pct,
        "trend_reclaim_pct": config.trend_reclaim_pct,
        "range_trading_enabled": False,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, default=list))


if __name__ == "__main__":
    main()
