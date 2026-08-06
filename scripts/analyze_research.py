#!/usr/bin/env python3
"""Summarize production-gate shadow outcomes from exported research JSONL logs."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean


def jsonl_files(root: Path) -> list[Path]:
    return [root] if root.is_file() else sorted(root.rglob("*.jsonl"))


def load(root: Path) -> tuple[dict[int, dict], dict[tuple[int, int], dict]]:
    signals: dict[int, dict] = {}
    outcomes: dict[tuple[int, int], dict] = {}
    for path in jsonl_files(root):
        with path.open(encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise SystemExit(f"{path}:{line_no}: invalid JSON: {exc}") from exc
                data = event.get("data", {})
                if event.get("event_type") == "signal_confirmed":
                    signal = data.get("signal", {})
                    if event_id := signal.get("event_id"):
                        signals[int(event_id)] = signal
                elif event.get("event_type") == "shadow_outcome":
                    event_id = data.get("event_id")
                    horizon = data.get("horizon_min")
                    if event_id is not None and horizon is not None:
                        outcomes[(int(event_id), int(horizon))] = data
    return signals, outcomes


def eligible(signal: dict) -> bool:
    if "trade_eligible" in signal:
        return signal.get("trade_eligible") is True
    trdr = signal.get("trdr", {})
    return (
        signal.get("context_score", 0) >= 5
        and trdr.get("source_coverage_complete") is True
        and trdr.get("footprint_matches") is True
    )


def row(label: str, values: list[float]) -> str:
    if not values:
        return f"{label:<14} {'0':>5} {'—':>9} {'—':>12} {'—':>12}"
    wins = sum(value > 0 for value in values)
    return f"{label:<14} {len(values):>5} {wins / len(values):>8.1%} {mean(values):>+11.4f}% {sum(values):>+11.4f}%"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="research directory or one JSONL file")
    args = parser.parse_args()
    signals, outcomes = load(args.path)
    selected: list[dict] = []
    for (event_id, _), outcome in outcomes.items():
        signal = outcome.get("features") if isinstance(outcome.get("features"), dict) else None
        signal = signal if signal and "context_score" in signal else signals.get(event_id, {})
        if eligible(signal):
            selected.append(outcome)

    print("production gate: source coverage + footprint + context >= 5")
    print(f"confirmed={len(signals)} eligible_events={len({item['event_id'] for item in selected})}")
    print(f"{'window':<14} {'n':>5} {'winrate':>9} {'avg net':>12} {'sum net':>12}")
    by_horizon: dict[int, list[float]] = defaultdict(list)
    for item in selected:
        by_horizon[int(item["horizon_min"])].append(float(item["estimated_net_return_pct"]))
    for horizon in sorted(by_horizon):
        print(row(f"{horizon}m", by_horizon[horizon]))

    print("\n30m by UTC day")
    by_day: dict[str, list[float]] = defaultdict(list)
    for item in selected:
        if item.get("horizon_min") != 30:
            continue
        day = datetime.fromtimestamp(item["signal_ts_ms"] / 1000, timezone.utc).date().isoformat()
        by_day[day].append(float(item["estimated_net_return_pct"]))
    for day in sorted(by_day):
        print(row(day, by_day[day]))


if __name__ == "__main__":
    main()
