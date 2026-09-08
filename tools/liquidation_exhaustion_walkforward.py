#!/usr/bin/env python3
"""Replay observed Binance forced orders against executable top-of-book prices.

This research tool is deliberately separate from the production strategy.  It
uses the high-resolution ``impact-efficiency-v1`` captures, recreates the
three-second liquidation/depth/aligned-move gate, and compares delayed reversal
confirmation thresholds without looking ahead.  Entry is paid at ask/bid,
exit at bid/ask, and taker fees are charged on both legs.
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import datetime as dt
import glob
import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


THRESHOLDS_BPS = (0.0, 2.0, 4.0, 6.0, 8.0, 12.0)
HOLD_MINUTES = (1, 3, 5, 10, 15)
SIDE_MODES = ("reversal", "continuation")
FEE_RATE = 0.0004
MAJORS = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"}


@dataclass
class Signal:
    symbol: str
    side: int
    event_ms: int
    entry_ms: int
    entry: float
    depth_ratio: float
    dominance: float
    aligned_bps: float
    reversal_bps: float
    threshold_bps: float
    hold_minutes: int
    side_mode: str
    exit: float | None = None
    exit_ms: int | None = None
    reason: str | None = None
    mfe: float = 0.0
    mae: float = 0.0


def parse_started(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def eligible_files(root: Path, start: str, end: str) -> list[Path]:
    start_dt = parse_started(start + "T00:00:00Z")
    end_dt = parse_started(end + "T00:00:00Z")
    found = []
    for manifest in root.glob("capture-*.manifest.json"):
        value = json.loads(manifest.read_text())
        began = parse_started(value["started_at"])
        force_orders = value.get("records_by_event_type", {}).get("forceOrder", 0) or 0
        capture = Path(str(manifest).replace(".manifest.json", ".jsonl.zst"))
        if start_dt <= began < end_dt and force_orders and capture.exists():
            found.append(capture)
    return sorted(found)


def top_book(payload: dict) -> tuple[float, float, float, float] | None:
    bids = payload.get("b") or []
    asks = payload.get("a") or []
    if not bids or not asks:
        return None
    bid = float(bids[0][0])
    ask = float(asks[0][0])
    if bid <= 0 or ask <= bid:
        return None
    bid_depth = sum(float(price) * float(qty) for price, qty in bids)
    ask_depth = sum(float(price) * float(qty) for price, qty in asks)
    return bid, ask, bid_depth, ask_depth


def close_signal(signal: Signal, now_ms: int, bid: float, ask: float, stop_pct: float) -> bool:
    executable = bid if signal.side > 0 else ask
    raw = signal.side * (executable / signal.entry - 1.0)
    signal.mfe = max(signal.mfe, raw)
    signal.mae = min(signal.mae, raw)
    stopped = raw <= -stop_pct
    expired = now_ms >= signal.entry_ms + signal.hold_minutes * 60_000
    if not stopped and not expired:
        return False
    signal.exit = executable
    signal.exit_ms = now_ms
    signal.reason = "stop" if stopped else "hold"
    return True


def replay_file(
    path_text: str,
    stop_pct: float,
    evaluation_start_ms: int | None = None,
    evaluation_end_ms: int | None = None,
) -> dict[float, list[dict]]:
    path = Path(path_text)
    manifest = json.loads(Path(str(path).replace(".jsonl.zst", ".manifest.json")).read_text())
    symbols = set(manifest.get("symbols", ())) - MAJORS
    books: dict[str, tuple[int, float, float, float, float]] = {}
    mids: dict[str, collections.deque] = collections.defaultdict(collections.deque)
    liquidations: dict[str, collections.deque] = collections.defaultdict(collections.deque)
    pending: dict[str, dict] = {}
    keys = [
        (mode, threshold, hold)
        for mode in SIDE_MODES
        for threshold in THRESHOLDS_BPS
        for hold in HOLD_MINUTES
    ]
    opened: dict[tuple, list[Signal]] = {key: [] for key in keys}
    completed: dict[tuple, list[dict]] = {key: [] for key in keys}
    seen: dict[tuple, set[tuple[str, int]]] = {key: set() for key in keys}
    last_entry: dict[tuple, dict[str, int]] = {key: {} for key in keys}

    process = subprocess.Popen(["zstdcat", str(path)], stdout=subprocess.PIPE, text=True)
    assert process.stdout is not None
    for line in process.stdout:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        symbol = row.get("symbol")
        if symbol not in symbols:
            continue
        kind = row.get("event_type")
        now_ms = int(row.get("local_receive_time_us", 0)) // 1_000
        if kind == "depthUpdate":
            book = top_book(row.get("payload", {}))
            if book is None:
                continue
            bid, ask, bid_depth, ask_depth = book
            books[symbol] = (now_ms, bid, ask, bid_depth, ask_depth)
            midpoint = (bid + ask) * 0.5
            values = mids[symbol]
            values.append((now_ms, midpoint))
            while values and values[0][0] < now_ms - 20_000:
                values.popleft()

            for key, positions in opened.items():
                keep = []
                for signal in positions:
                    if signal.symbol != symbol or not close_signal(signal, now_ms, bid, ask, stop_pct):
                        keep.append(signal)
                        continue
                    gross = signal.side * (signal.exit / signal.entry - 1.0)
                    net = gross - FEE_RATE * (1.0 + signal.exit / signal.entry)
                    if (
                        (evaluation_start_ms is None or signal.entry_ms >= evaluation_start_ms)
                        and (evaluation_end_ms is None or signal.entry_ms <= evaluation_end_ms)
                    ):
                        completed[key].append(
                            {
                                **signal.__dict__,
                                "return": net,
                                "gross_return": gross,
                            }
                        )
                opened[key] = keep

            event = pending.get(symbol)
            if event is None or now_ms < event["received_ms"] + 1_000:
                continue
            if now_ms > event["received_ms"] + 12_000:
                pending.pop(symbol, None)
                continue
            selected = [x for x in liquidations[symbol] if event["received_ms"] - 3_000 <= x[0] <= event["received_ms"]]
            if not selected:
                continue
            long_usd = sum(x[2] for x in selected if x[1])
            short_usd = sum(x[2] for x in selected if not x[1])
            total = long_usd + short_usd
            if total <= 0:
                continue
            long_dominant = long_usd >= short_usd
            directional = max(long_usd, short_usd)
            dominance = directional / total
            matching = next((x for x in reversed(selected) if x[1] == long_dominant and x[3] > 0), None)
            depth_ratio = directional / matching[3] if matching else 0.0
            start_ms = event["received_ms"] - 3_000
            before = [x for x in values if x[0] <= start_ms and start_ms - x[0] <= 1_500]
            around = before[-1] if before else next((x for x in values if x[0] >= start_ms and x[0] - start_ms <= 1_500), None)
            if around is None or event["mid"] <= 0:
                continue
            aligned = (-1.0 if long_dominant else 1.0) * (event["mid"] / around[1] - 1.0) * 10_000.0
            if dominance < 0.80 or depth_ratio < 0.50 or aligned < 3.0:
                continue
            since_event = [x[1] for x in values if event["received_ms"] <= x[0] <= now_ms]
            if not since_event:
                continue
            reversal_side = 1 if long_dominant else -1
            reversal = (
                (midpoint / min(since_event) - 1.0)
                if reversal_side > 0
                else (max(since_event) / midpoint - 1.0)
            ) * 10_000.0
            key = (symbol, event["event_ms"])
            for mode in SIDE_MODES:
                side = reversal_side if mode == "reversal" else -reversal_side
                for threshold in THRESHOLDS_BPS:
                    if reversal < threshold:
                        continue
                    for hold in HOLD_MINUTES:
                        variant = (mode, threshold, hold)
                        if key in seen[variant]:
                            continue
                        seen[variant].add(key)
                        if any(position.symbol == symbol for position in opened[variant]):
                            # Match production: another signal may not stack on
                            # a symbol while its previous position is open.
                            continue
                        if now_ms - last_entry[variant].get(symbol, -10**18) < 30_000:
                            continue
                        last_entry[variant][symbol] = now_ms
                        opened[variant].append(
                            Signal(
                                symbol=symbol,
                                side=side,
                                event_ms=event["event_ms"],
                                entry_ms=now_ms,
                                entry=ask if side > 0 else bid,
                                depth_ratio=depth_ratio,
                                dominance=dominance,
                                aligned_bps=aligned,
                                reversal_bps=reversal,
                                threshold_bps=threshold,
                                hold_minutes=hold,
                                side_mode=mode,
                            )
                        )
        elif kind == "forceOrder":
            payload = row.get("payload", {})
            order = payload.get("o", {})
            book = books.get(symbol)
            price = float(order.get("ap") or order.get("p") or 0.0)
            quantity = float(order.get("z") or order.get("q") or 0.0)
            if book is None or price <= 0 or quantity <= 0 or now_ms - book[0] > 2_000:
                continue
            is_long = order.get("S") == "SELL"
            pressure = book[3] if is_long else book[4]
            midpoint = (book[1] + book[2]) * 0.5
            event_ms = int(payload.get("E") or now_ms)
            values = liquidations[symbol]
            values.append((now_ms, is_long, price * quantity, pressure, midpoint, event_ms))
            while values and values[0][0] < now_ms - 20_000:
                values.popleft()
            pending[symbol] = {"received_ms": now_ms, "event_ms": event_ms, "mid": midpoint}
    process.stdout.close()
    process.wait()
    # Incomplete trades at the end of an hourly capture are intentionally
    # discarded instead of inventing an exit across a data gap.
    return completed


def summarize(rows: list[dict], depth_floor: float) -> dict:
    selected = [row for row in rows if row["depth_ratio"] >= depth_floor]
    returns = [row["return"] for row in selected]
    gross_profit = sum(max(value, 0.0) for value in returns)
    gross_loss = -sum(min(value, 0.0) for value in returns)
    by_day = {}
    for row in selected:
        day = dt.datetime.fromtimestamp(row["entry_ms"] / 1_000, dt.UTC).date().isoformat()
        bucket = by_day.setdefault(day, [])
        bucket.append(row["return"])
    return {
        "trades": len(selected),
        "win_rate_pct": round(100 * sum(value > 0 for value in returns) / len(returns), 1) if returns else 0.0,
        "mean_return_bps": round(10_000 * sum(returns) / len(returns), 2) if returns else 0.0,
        "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss else None,
        "notional_2500_pnl_usd": round(2_500 * sum(returns), 2),
        "daily": {
            day: {
                "trades": len(values),
                "win_rate_pct": round(100 * sum(value > 0 for value in values) / len(values), 1),
                "notional_2500_pnl_usd": round(2_500 * sum(values), 2),
            }
            for day, values in sorted(by_day.items())
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--start", default="2026-09-05")
    parser.add_argument("--end", default="2026-09-08")
    parser.add_argument("--workers", type=int, default=min(6, os.cpu_count() or 1))
    parser.add_argument("--stop-pct", type=float, default=0.02)
    parser.add_argument(
        "--evaluation-start",
        help="optional inclusive ISO-8601 entry timestamp inside the selected file days",
    )
    parser.add_argument(
        "--evaluation-end",
        help="optional inclusive ISO-8601 entry timestamp inside the selected file days",
    )
    args = parser.parse_args()
    evaluation_start_ms = (
        int(parse_started(args.evaluation_start).timestamp() * 1_000)
        if args.evaluation_start
        else None
    )
    evaluation_end_ms = (
        int(parse_started(args.evaluation_end).timestamp() * 1_000)
        if args.evaluation_end
        else None
    )
    files = eligible_files(args.root, args.start, args.end)
    aggregate = {
        (mode, threshold, hold): []
        for mode in SIDE_MODES
        for threshold in THRESHOLDS_BPS
        for hold in HOLD_MINUTES
    }
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(
                replay_file,
                str(path),
                args.stop_pct,
                evaluation_start_ms,
                evaluation_end_ms,
            )
            for path in files
        ]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            for key, rows in result.items():
                aggregate[key].extend(rows)
    report = {
        "method": "causal forced-order plus executable top-book replay; hourly boundary trades discarded",
        "period": [args.start, args.end],
        "files": len(files),
        "stop_pct": args.stop_pct,
        "evaluation_start": args.evaluation_start,
        "evaluation_end": args.evaluation_end,
        "fee_rate_per_leg": FEE_RATE,
        "grid": {
            f"{mode}_{threshold:g}bps_{hold}m": {
                f"depth_{depth:g}": summarize(rows, depth)
                for depth in (0.5, 0.6, 0.8, 1.0, 1.2)
            }
            for (mode, threshold, hold), rows in aggregate.items()
        },
        "selected_trades": [
            {
                "symbol": row["symbol"],
                "side": row["side"],
                "entry_ms": row["entry_ms"],
                "exit_ms": row["exit_ms"],
                "return": row["return"],
            }
            for row in aggregate[("reversal", 12.0, 1)]
            if row["depth_ratio"] >= 1.2
        ],
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
