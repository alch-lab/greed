#!/usr/bin/env python3
"""Walk-forward test of a fast blow-off/retest fade for Binance altcoins.

The hypothesis is inspired by a recurring discretionary setup: do not short a
vertical pump immediately; wait for the first flush, a weak rebound toward the
peak, and a bearish rejection.  Signals use only closed 15-minute bars and are
filled at the next open.  The portfolio simulator supplies the production-like
4% daily loss gate, two-position cap, fees, slippage and cooldown.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_entry_exit_v2 import ExitProfile, simulate
from exp_altcoin_five_optimizations import load_symbol_bars
from exp_altcoin_oi_launch import BAR_MS, DAY_MS, MAJORS, Bar, Signal, load_bars


@dataclass(frozen=True)
class Profile:
    min_pump: float
    min_volume: float
    min_drop: float
    min_rebound: float
    require_spot: bool
    max_wait_bars: int = 12

    @property
    def name(self) -> str:
        return (
            f"P{int(self.min_pump*100)}_V{self.min_volume:g}_D{int(self.min_drop*100)}_"
            f"R{int(self.min_rebound*100)}_SP{int(self.require_spot)}"
        )


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july": ("2026-07-01", "2026-08-01"),
    "aug": ("2026-08-01", "2026-08-11"),
}


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def prefix(values: list[float]) -> list[float]:
    output = [0.0]
    for value in values:
        output.append(output[-1] + value)
    return output


def generate(
    symbol: str,
    bars: list[Bar],
    spot_bars: list[Bar],
    profiles: list[Profile],
) -> dict[str, list[Signal]]:
    output: dict[str, list[Signal]] = defaultdict(list)
    if len(bars) < 14 * 96 + 16 or not spot_bars:
        return output
    spot_closes = {
        (bar.ts // 1_000 if bar.ts > 10**15 else bar.ts): bar.close
        for bar in spot_bars
    }
    quote = prefix([bar.quote_volume for bar in bars])
    last_event: dict[str, int] = defaultdict(lambda: -10**9)
    event_cache: dict[int, tuple[float, float, float | None]] = {}
    warmup = 14 * 96
    for event in range(warmup, len(bars) - 2):
        r1 = bars[event].close / bars[event - 4].close - 1.0
        if r1 < 0.05:
            continue
        q1 = quote[event + 1] - quote[event - 3]
        q7d = quote[event] - quote[event - 7 * 96]
        volume_ratio = q1 / max(q7d / (7 * 24), 1.0)
        spot_now = spot_closes.get(bars[event].ts)
        spot_prev = spot_closes.get(bars[event - 4].ts)
        spot_r1 = spot_now / spot_prev - 1.0 if spot_now and spot_prev and spot_prev > 0 else None
        event_cache[event] = (r1, volume_ratio, spot_r1)

    for index in range(warmup + 2, len(bars) - 1):
        bar = bars[index]
        span = bar.high - bar.low
        close_location = (bar.close - bar.low) / span if span > 0 else 0.5
        if bar.close >= bar.open or close_location > 0.50:
            continue
        for profile in profiles:
            chosen_event = None
            event_values = None
            for event in range(index - 2, max(warmup - 1, index - profile.max_wait_bars) - 1, -1):
                values = event_cache.get(event)
                if values is None:
                    continue
                pump, volume_ratio, spot_r1 = values
                if pump < profile.min_pump or volume_ratio < profile.min_volume:
                    continue
                if profile.require_spot and (spot_r1 is None or spot_r1 < 0.50 * pump):
                    continue
                chosen_event = event
                event_values = values
                break
            if chosen_event is None or event_values is None or chosen_event == last_event[profile.name]:
                continue
            peak_index = max(range(chosen_event, index + 1), key=lambda cursor: bars[cursor].high)
            if peak_index >= index:
                continue
            peak = bars[peak_index].high
            trough = min(item.low for item in bars[peak_index + 1 : index + 1])
            drop = 1.0 - trough / peak
            if drop < profile.min_drop or peak <= trough:
                continue
            rebound = (bar.high - trough) / (peak - trough)
            if rebound < profile.min_rebound or bar.close >= peak * 0.995:
                continue
            pump, volume_ratio, spot_r1 = event_values
            q24 = quote[index + 1] - quote[index - 95]
            if q24 < 10_000_000:
                continue
            score = pump * math.log1p(volume_ratio) * rebound * (0.5 + 1.0 - close_location)
            output[profile.name].append(
                Signal(
                    symbol,
                    bars[index + 1].ts,
                    -1,
                    score,
                    "blowoff_retest_fade",
                    {
                        "signal_index": index,
                        "return_1h": pump,
                        "spot_return_1h": spot_r1 or 0.0,
                        "volume_ratio": volume_ratio,
                        "drop": drop,
                        "rebound": rebound,
                    },
                )
            )
            last_event[profile.name] = chosen_event
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-blowoff-retest.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    profiles = [
        Profile(pump, volume, drop, rebound, spot)
        for pump in (0.06, 0.10, 0.15)
        for volume in (2.0, 4.0)
        for drop in (0.02, 0.04)
        for rebound in (0.50, 0.70)
        for spot in (False, True)
    ]
    exits = [
        ExitProfile("S3_A1.5_T0.75_H2", 0.03, 0.015, 0.0075, 8),
        ExitProfile("S4_A2_T1_H4", 0.04, 0.02, 0.01, 16),
        ExitProfile("S5_A2_T1_H6", 0.05, 0.02, 0.01, 24),
    ]
    symbols = sorted(
        {path.name for path in (cache / "klines").iterdir() if path.is_dir()}
        & {path.name for path in (cache / "spot-klines").iterdir() if path.is_dir()}
        - MAJORS
    )
    bars_by_symbol: dict[str, list[Bar]] = {}
    signals: dict[str, list[Signal]] = defaultdict(list)
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        spot_paths = sorted((cache / "spot-klines" / symbol).glob("*.zip"))
        spot_paths += sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
        spot_bars = load_bars(spot_paths)
        made = generate(symbol, bars, spot_bars, profiles)
        if made:
            bars_by_symbol[symbol] = bars
            for name, values in made.items():
                signals[name].extend(values)
        if completed % 50 == 0:
            print(f"features {completed}/{len(symbols)} signals={sum(map(len, signals.values()))}", flush=True)

    train = tuple(map(timestamp, PERIODS["train"]))
    validation = tuple(map(timestamp, PERIODS["validation"]))
    rows: list[dict[str, object]] = []
    by_name = {profile.name: profile for profile in profiles}
    for profile in profiles:
        for exit_profile in exits:
            result = simulate(
                bars_by_symbol,
                signals[profile.name],
                *train,
                exit_profile,
                risk_per_trade=0.03,
                max_daily_entries=8,
                max_positions=2,
            )
            rows.append(
                {
                    "profile": profile.name,
                    "exit": exit_profile.name,
                    "signal_count": len(signals[profile.name]),
                    "train": result,
                }
            )
    rows.sort(
        key=lambda row: (
            float(row["train"]["return_pct"]) - 0.5 * float(row["train"]["max_drawdown_pct"]),
            float(row["train"]["profit_factor"] or 0.0),
        ),
        reverse=True,
    )
    train_shortlist = [row for row in rows if int(row["train"]["trades"]) >= 30][:40]
    by_exit = {profile.name: profile for profile in exits}
    for row in train_shortlist:
        row["validation"] = simulate(
            bars_by_symbol,
            signals[str(row["profile"])],
            *validation,
            by_exit[str(row["exit"])],
            risk_per_trade=0.03,
            max_daily_entries=8,
            max_positions=2,
        )
    train_shortlist.sort(
        key=lambda row: (
            min(float(row["train"]["return_pct"]), float(row["validation"]["return_pct"])),
            float(row["validation"]["profit_factor"] or 0.0),
        ),
        reverse=True,
    )
    finalists = train_shortlist[:12]
    for row in finalists:
        for period in ("july", "aug"):
            row[period] = simulate(
                bars_by_symbol,
                signals[str(row["profile"])],
                *tuple(map(timestamp, PERIODS[period])),
                by_exit[str(row["exit"])],
                risk_per_trade=0.03,
                max_daily_entries=8,
                max_positions=2,
            )
        row["profile_values"] = by_name[str(row["profile"])].__dict__
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": len(bars_by_symbol),
        "grid": len(rows),
        "assumptions": {
            "fee_each_side_bps": 5,
            "slippage_each_side_bps": 5,
            "risk_per_trade_pct": 3,
            "daily_new_entry_loss_gate_pct": 4,
            "max_positions": 2,
            "max_entries_day": 8,
        },
        "finalists": finalists,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
