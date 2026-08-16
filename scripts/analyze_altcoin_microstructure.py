#!/usr/bin/env python3
"""Join altcoin microstructure observations to entries and realized outcomes.

Usage:
    python3 scripts/analyze_altcoin_microstructure.py data/altcoin-events.jsonl \
        --output /tmp/microstructure-outcomes.json
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def read_events(paths: list[Path]):
    for path in paths:
        with path.open(errors="replace") as handle:
            for line_no, line in enumerate(handle, 1):
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                value["_source"] = f"{path}:{line_no}"
                yield value


def key(event):
    signal = event.get("signal") or {}
    signal_ms = event.get("origin_signal_ms") or signal.get("signal_ms")
    symbol = event.get("symbol")
    return (symbol, signal_ms) if symbol and signal_ms else None


def summarize(rows):
    completed = [row for row in rows if row.get("trade_pnl") is not None]
    pnl = sum(row["trade_pnl"] for row in completed)
    winners = sum(row["trade_pnl"] > 0 for row in completed)
    return {
        "observed_setups": len(rows),
        "confirmed_snapshots": sum("confirmation" in row for row in rows),
        "entries": sum("entry" in row for row in rows),
        "completed_trades": len(completed),
        "realized_pnl": pnl,
        "win_rate": winners / len(completed) if completed else None,
        "note": "Do not fit a gate before at least 30 setups and 15 completed trades.",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("events", nargs="+", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    rows = {}
    open_by_symbol = {}
    unmatched = defaultdict(int)
    for event in sorted(read_events(args.events), key=lambda item: item.get("ts_ms", 0)):
        event_name = event.get("event")
        event_key = key(event)
        if event_name == "entry_microstructure_observation" and event_key:
            rows.setdefault(event_key, {"symbol": event_key[0], "signal_ms": event_key[1]})[
                "signal"
            ] = event.get("snapshot")
        elif event_name == "liquidity_check" and event_key:
            row = rows.setdefault(
                event_key, {"symbol": event_key[0], "signal_ms": event_key[1]}
            )
            row["confirmation"] = event.get("snapshot")
            row["liquidity_passed"] = event.get("passed")
        elif event_name == "entry":
            if not event_key:
                unmatched["entry_without_signal_key"] += 1
                continue
            row = rows.setdefault(
                event_key, {"symbol": event_key[0], "signal_ms": event_key[1]}
            )
            row["entry"] = {
                name: event.get(name)
                for name in (
                    "ts_ms",
                    "side",
                    "entry_price",
                    "notional",
                    "entry_trigger",
                    "entry_phase",
                )
            }
            open_by_symbol[event_key[0]] = event_key
        elif event_name in {"exit", "exit_detected"}:
            event_key = open_by_symbol.pop(event.get("symbol"), None)
            if not event_key:
                unmatched["exit_without_observed_entry"] += 1
                continue
            row = rows[event_key]
            row["exit"] = {
                name: event.get(name)
                for name in ("ts_ms", "reason", "price", "pnl", "trade_pnl", "hold_ms")
            }
            row["trade_pnl"] = event.get("trade_pnl", event.get("pnl"))

    payload = {
        "summary": summarize(list(rows.values())),
        "unmatched": dict(unmatched),
        "rows": sorted(rows.values(), key=lambda row: row["signal_ms"]),
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
