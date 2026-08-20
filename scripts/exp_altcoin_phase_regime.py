#!/usr/bin/env python3
"""Regime ablation for the explosive-pump continuation candidate."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_alpha_search import (
    EXIT_PROFILES,
    PERIODS,
    build_signals,
    simulate,
    timestamp,
)
from exp_altcoin_five_optimizations import compute_regimes, load_symbol_bars, spot_history_symbols
from exp_altcoin_oi_launch import MAJORS, Bar, Signal


def btc_context(bars: list[Bar]) -> dict[int, tuple[float, float]]:
    output: dict[int, tuple[float, float]] = {}
    for index in range(96, len(bars) - 1):
        output[bars[index + 1].ts] = (
            bars[index].close / bars[index - 16].close - 1,
            bars[index].close / bars[index - 96].close - 1,
        )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-phase-regime.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    allowed = spot_history_symbols()
    symbols = sorted({path.parent.name for path in (cache / "klines").glob("*/*.zip")})
    symbols = [symbol for symbol in symbols if symbol in allowed and symbol not in MAJORS]
    bars_by_symbol: dict[str, list[Bar]] = {}
    signals_by_level: dict[int, list[Signal]] = {1: [], 2: [], 3: []}
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        if len(bars) < 15 * 96:
            continue
        bars_by_symbol[symbol] = bars
        made = build_signals(symbol, bars)
        for level in (1, 2, 3):
            signals_by_level[level].extend(made.get(f"pump_continuation_{level}", []))
        if completed % 50 == 0:
            print(f"features {completed}/{len(symbols)}", flush=True)
    regimes = compute_regimes(bars_by_symbol)
    btc = btc_context(load_symbol_bars(cache, "BTCUSDT"))
    exit_profile = next(profile for profile in EXIT_PROFILES if profile.name == "time_5_4h")
    configs: list[tuple[int, float, float, bool, bool]] = []
    for level in (3,):
        for breadth in (0.50, 0.55, 0.60, 0.65):
            for median in (0.0, 0.002, 0.005):
                for btc_required in (False, True):
                    configs.append((level, breadth, median, btc_required, True))
    results: dict[str, object] = {}
    for level, breadth, median, btc_required, dispersion_veto in configs:
        selected: list[Signal] = []
        for signal in signals_by_level[level]:
            regime = regimes.get(signal.execute_ts)
            btc_returns = btc.get(signal.execute_ts)
            if regime is None or (btc_required and btc_returns is None):
                continue
            if regime.breadth < breadth or regime.median_return_1h < median:
                continue
            if btc_required and not (btc_returns[0] > 0 and btc_returns[1] > 0):
                continue
            if dispersion_veto and regime.dispersion_iqr > regime.dispersion_p90_14d:
                continue
            selected.append(signal)
        name = f"L{level}_B{breadth:.2f}_M{median:.3f}_BTC{int(btc_required)}_DV{int(dispersion_veto)}"
        periods = {}
        for period, (start, end) in PERIODS.items():
            periods[period] = simulate(
                bars_by_symbol,
                selected,
                exit_profile,
                timestamp(start),
                timestamp(end),
                5.0,
            )
        results[name] = {
            "config": {
                "level": level,
                "breadth": breadth,
                "median_return_1h": median,
                "btc_positive_4h_24h": btc_required,
                "dispersion_p90_veto": dispersion_veto,
            },
            "signals": len(selected),
            "periods": periods,
        }
        print(name, periods["train"]["return_pct"], periods["validation"]["return_pct"], periods["july_test"]["return_pct"], periods["aug_holdout"]["return_pct"], flush=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Binance Vision USD-M 15m klines",
        "candidate": "pump continuation; 5% catastrophe stop; four-hour time exit",
        "results": results,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
