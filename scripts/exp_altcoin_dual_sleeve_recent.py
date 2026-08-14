#!/usr/bin/env python3
"""Blind replay of the dual-sleeve alpha on the recent one-minute cache.

The cache was collected independently of the January-August research archive.
Signals use only completed 15 minute bars, enter at the next bar open, and the
rolling gate is warmed on data before the evaluation boundary.
"""

from __future__ import annotations

import argparse
import gzip
import importlib.util
import json
import pickle
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


DAY_MS = 86_400_000
HOUR_MS = 3_600_000
MAJORS = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
    "DOGEUSDT", "TRXUSDT", "LINKUSDT", "BCHUSDT", "LTCUSDT",
}


@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float


def load_cache(path: Path) -> tuple[set[str], dict[str, list[object]]]:
    # The historical pickle references the replay module's Bar class.
    source = Path(__file__).with_name("replay_altcoin_current_3d.py")
    spec = importlib.util.spec_from_file_location("rp", source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["rp"] = module
    spec.loader.exec_module(module)
    with gzip.open(path, "rb") as handle:
        _, spot_symbols, _, bars = pickle.load(handle)
    return set(spot_symbols), bars


def aggregate_15m(values: list[object]) -> list[Bar]:
    if len(values) > 2 and values[1].ts - values[0].ts >= 800_000:
        return [Bar(x.ts, x.open, x.high, x.low, x.close, x.quote_volume) for x in values]
    groups: dict[int, list[object]] = defaultdict(list)
    for value in values:
        groups[value.ts // 900_000 * 900_000].append(value)
    output = []
    for ts, group in sorted(groups.items()):
        group.sort(key=lambda item: item.ts)
        # Requiring 14/15 observations avoids forming signals from sparse bars
        # while tolerating a single exchange/API gap.
        if len(group) < 14:
            continue
        output.append(Bar(ts, group[0].open, max(x.high for x in group), min(x.low for x in group), group[-1].close, sum(x.quote_volume for x in group)))
    return output


def leg_return(bars: list[Bar], entry_index: int, side: int, hold_hours: int, stop: float, slippage_bps: float) -> float:
    fee = 0.0005
    slip = slippage_bps / 10_000
    entry = bars[entry_index].open * (1 + side * slip)
    stop_price = entry * (1 - side * stop)
    final_index = min(entry_index + hold_hours * 4, len(bars) - 1)
    raw_exit = bars[final_index].open
    for bar in bars[entry_index:final_index]:
        hit = bar.low <= stop_price if side > 0 else bar.high >= stop_price
        if hit:
            gapped = bar.open < stop_price if side > 0 else bar.open > stop_price
            raw_exit = bar.open if gapped else stop_price
            break
    exit_price = raw_exit * (1 - side * slip)
    return side * (exit_price / entry - 1) - fee - fee * exit_price / entry


def build_series(
    bars_by_symbol: dict[str, list[Bar]], formation_hours: int, names: int,
    direction: str, stop: float, slippage_bps: float, hold_hours: int = 4,
    liquidity: str = "liquid",
) -> list[dict[str, object]]:
    snapshots: dict[int, list[tuple[str, int, float, float]]] = defaultdict(list)
    back = formation_hours * 4
    for symbol, bars in bars_by_symbol.items():
        prefix = [0.0]
        for bar in bars:
            prefix.append(prefix[-1] + bar.quote_volume)
        for index in range(max(back, 96), len(bars) - 17):
            bar = bars[index]
            if datetime.fromtimestamp(bar.ts / 1000, timezone.utc).minute != 45:
                continue
            if bar.ts - bars[index - back].ts > (back + 1) * 900_000:
                continue
            volume = prefix[index + 1] - prefix[index - 95]
            liquid = volume >= 50_000_000 if liquidity == "liquid" else volume >= 10_000_000
            if liquidity == "mid":
                liquid = 10_000_000 <= volume < 150_000_000
            if not liquid:
                continue
            snapshots[bars[index + 1].ts].append((symbol, index + 1, bar.close / bars[index - back].close - 1, volume))
    output = []
    for entry_ts, universe in sorted(snapshots.items()):
        if datetime.fromtimestamp(entry_ts / 1000, timezone.utc).hour % hold_hours:
            continue
        ordered = sorted((item for item in universe if abs(item[2]) <= 1.5), key=lambda item: item[2])
        if len(ordered) < names * 2:
            continue
        if direction in {"momentum", "neutral_momentum"}:
            selected = [(ordered[-1 - i], 1) for i in range(names)] + [(ordered[i], -1) for i in range(names)]
        elif direction in {"reversal", "neutral_reversal"}:
            selected = [(ordered[-1 - i], -1) for i in range(names)] + [(ordered[i], 1) for i in range(names)]
        elif direction == "long_winners":
            selected = [(ordered[-1 - i], 1) for i in range(names)]
        elif direction == "short_winners":
            selected = [(ordered[-1 - i], -1) for i in range(names)]
        elif direction == "long_losers":
            selected = [(ordered[i], 1) for i in range(names)]
        elif direction == "short_losers":
            selected = [(ordered[i], -1) for i in range(names)]
        else:
            continue
        if direction in {"momentum", "neutral_momentum", "reversal", "neutral_reversal"}:
            invalid = any(item[0][2] <= 0 for item in selected[:names]) or any(item[0][2] >= 0 for item in selected[names:])
        elif direction.endswith("winners"):
            invalid = any(item[0][2] <= 0 for item in selected)
        else:
            invalid = any(item[0][2] >= 0 for item in selected)
        if invalid:
            continue
        legs = []
        for (symbol, index, trailing, _), side in selected:
            legs.append({"symbol": symbol, "side": side, "trailing_return": trailing, "return": leg_return(bars_by_symbol[symbol], index, side, hold_hours, stop, slippage_bps)})
        output.append({"entry_ts": entry_ts, "exit_ts": entry_ts + hold_hours * HOUR_MS, "return": sum(float(x["return"]) for x in legs) / len(legs), "legs": legs})
    return output


def enabled(history: deque[float]) -> bool:
    if len(history) < history.maxlen:
        return False
    gains = sum(max(x, 0) for x in history)
    losses = sum(max(-x, 0) for x in history)
    return sum(history) > 0 and (gains / losses if losses else 99) >= 1


def simulate(momentum: list[dict[str, object]], reversal: list[dict[str, object]], evaluation_start: int, evaluation_end: int) -> dict[str, object]:
    by_sleeve = {"momentum": {int(x["exit_ts"]): x for x in momentum}, "reversal": {int(x["exit_ts"]): x for x in reversal}}
    timestamps = sorted(set(by_sleeve["momentum"]) & set(by_sleeve["reversal"]))
    histories = {name: deque(maxlen=10) for name in by_sleeve}
    equity = peak = 1_000.0
    day = None
    day_start = equity
    daily_latched = False
    baskets = legs = wins = 0
    gross_profit = gross_loss = 0.0
    max_drawdown = 0.0
    conflicts = []
    trades = []
    for exit_ts in timestamps:
        entry_ts = exit_ts - 4 * HOUR_MS
        active = {name: enabled(history) for name, history in histories.items()}
        in_evaluation = evaluation_start <= entry_ts < evaluation_end
        if in_evaluation:
            new_day = entry_ts // DAY_MS
            if new_day != day:
                day, day_start, daily_latched = new_day, equity, False
            if equity < day_start * 0.96:
                daily_latched = True
            selected = [name for name in by_sleeve if active[name] and not daily_latched]
            if selected:
                symbol_sides: dict[str, list[tuple[str, int]]] = defaultdict(list)
                for name in selected:
                    for leg in by_sleeve[name][exit_ts]["legs"]:
                        symbol_sides[str(leg["symbol"])].append((name, int(leg["side"])))
                overlap = {symbol: values for symbol, values in symbol_sides.items() if len(values) > 1}
                if overlap:
                    conflicts.append({"entry_ts": entry_ts, "symbols": overlap})
                value = sum(0.50 * float(by_sleeve[name][exit_ts]["return"]) for name in selected)
                count = sum(len(by_sleeve[name][exit_ts]["legs"]) for name in selected)
                equity *= max(0.01, 1 + value)
                peak = max(peak, equity)
                max_drawdown = max(max_drawdown, 1 - equity / peak)
                baskets += 1
                legs += count
                if value > 0:
                    wins += 1
                    gross_profit += value
                else:
                    gross_loss -= value
                trades.append({"entry": datetime.fromtimestamp(entry_ts / 1000, timezone.utc).isoformat(), "sleeves": selected, "return_pct": value * 100, "equity": equity})
        for name in histories:
            histories[name].append(float(by_sleeve[name][exit_ts]["return"]))
    days = max((evaluation_end - evaluation_start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "ending_equity": equity,
        "max_drawdown_pct": max_drawdown * 100,
        "baskets": baskets,
        "legs": legs,
        "legs_per_day": legs / days,
        "hours_per_leg": days * 24 / legs if legs else None,
        "basket_win_rate_pct": wins / baskets * 100 if baskets else 0,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "overlap_events": len(conflicts),
        "overlaps": conflicts,
        "trades": trades,
    }


def ts(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/private/tmp/altcoin-fast-grid-bars.pkl.gz")
    parser.add_argument("--start", default="2026-08-11T00:00:00")
    parser.add_argument("--end", default="2026-08-14T06:00:00")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-dual-sleeve-recent.json")
    args = parser.parse_args()
    spot, raw = load_cache(Path(args.cache))
    symbols = sorted((set(raw) & spot) - MAJORS)
    bars = {symbol: aggregate_15m(raw[symbol]) for symbol in symbols}
    bars = {symbol: values for symbol, values in bars.items() if len(values) >= 7 * 96}
    print(f"universe={len(bars)}", flush=True)
    report = {"evaluation": [args.start, args.end], "universe": len(bars), "results": {}}
    for slippage in (5.0, 15.0):
        momentum = build_series(bars, 72, 1, "momentum", 0.08, slippage)
        reversal = build_series(bars, 6, 2, "reversal", 0.04, slippage)
        report["results"][f"{slippage:g}bps"] = simulate(momentum, reversal, ts(args.start), ts(args.end))
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
