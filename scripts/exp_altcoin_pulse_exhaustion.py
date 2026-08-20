#!/usr/bin/env python3
"""Test a two-stage pulse-long and leverage-exhaustion-short state machine."""

from __future__ import annotations

import copy
import gzip
import importlib.util
import json
import math
import pickle
import statistics
import sys
import tomllib
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


rp = load_module("pulse_replay", ROOT / "scripts/replay_altcoin_current_3d.py")
adaptive = load_module("pulse_adaptive", ROOT / "scripts/exp_altcoin_adaptive_risk.py")
spot_led = load_module("pulse_market", ROOT / "scripts/exp_altcoin_spot_led_acceleration.py")


def merge_batches(base, additions):
    merged = defaultdict(list)
    for ts, values in base.items():
        merged[ts].extend(values)
    for ts, values in additions.items():
        merged[ts].extend(values)
    for values in merged.values():
        values.sort(key=lambda item: (-item.score, item.symbol))
    return merged


def independent_pulse_observations(bars_by_symbol, start_ms, end_ms, cfg):
    """Create pulse observations without applying main-strategy entry gates.

    The production scanner still shares the current tradable top-60 universe,
    minimum 24h turnover and fresh closed 15m bars.  It must not require a 24h
    breakout, the main 4h return band, path efficiency or a strong close before
    observing a possible leverage pulse.
    """
    indexes = {
        symbol: {bar.ts: i for i, bar in enumerate(bars)}
        for symbol, bars in bars_by_symbol.items()
    }
    output = defaultdict(list)
    for execute_ms in range((start_ms // rp.BAR_MS + 1) * rp.BAR_MS, end_ms, rp.BAR_MS):
        open_ms = execute_ms - rp.BAR_MS
        ranked = []
        for symbol, bars in bars_by_symbol.items():
            i = indexes[symbol].get(open_ms)
            if i is None or i < 7 * 96 + 16:
                continue
            volume_24h = sum(bar.quote_volume for bar in bars[i - 95:i + 1])
            change_24h = abs(bars[i].close / bars[i - 96].close - 1.0)
            if volume_24h >= float(cfg["min_24h_volume_usd"]) and change_24h >= 0.04:
                ranked.append((volume_24h * (1.0 + change_24h), symbol, i))
        ranked.sort(reverse=True)
        for _, symbol, i in ranked[:int(cfg["scan_limit"])]:
            bars = bars_by_symbol[symbol]
            close = bars[i].close
            return_1h = close / bars[i - 4].close - 1.0
            if return_1h <= 0.0:
                continue
            return_4h = close / bars[i - 16].close - 1.0
            hour_volume = lambda end: sum(
                bar.quote_volume for bar in bars[end - 3:end + 1]
            )
            current_hour_volume = hour_volume(i)
            historical = [hour_volume(end) for end in range(i - 7 * 96, i)]
            volume_ratio = current_hour_volume / max(statistics.median(historical), 1.0)
            signal = rp.Signal(
                symbol=symbol,
                signal_ms=bars[i].ts + rp.BAR_MS - 1,
                side=1,
                price=close,
                score=return_1h * math.log1p(volume_ratio),
                return_1h=return_1h,
                return_4h=return_4h,
                volume_ratio=volume_ratio,
                phase="pulse_impulse",
                breakout=close,
                trigger="independent_leverage_pulse",
            )
            output[execute_ms].append(signal)
        output[execute_ms].sort(key=lambda item: (-item.score, item.symbol))
    return output


FEATURE_CACHE = {}


def load_market_features(signal, cache: Path, at_ms: int):
    day = datetime.fromtimestamp((at_ms - 1) / 1000, timezone.utc).date().isoformat()
    cache_key = (signal.symbol, day, at_ms)
    if cache_key in FEATURE_CACHE:
        return FEATURE_CACHE[cache_key]
    spot_path = cache / "spot" / signal.symbol / f"{day}.zip"
    metrics_path = cache / "metrics" / signal.symbol / f"{day}.zip"
    if not spot_path.exists() or not metrics_path.exists():
        FEATURE_CACHE[cache_key] = None
        return None
    closes = spot_led.spot_closes(spot_path)
    open_ms = at_ms - rp.BAR_MS
    spot_now = closes.get(open_ms)
    spot_previous = closes.get(open_ms - 4 * rp.BAR_MS)
    metrics = spot_led.oi_values(metrics_path)
    oi_now = spot_led.nearest_before(metrics, at_ms - 1)
    oi_previous = spot_led.nearest_before(metrics, at_ms - 60 * rp.MINUTE_MS - 1)
    if not spot_now or not spot_previous or not oi_now or not oi_previous:
        FEATURE_CACHE[cache_key] = None
        return None
    FEATURE_CACHE[cache_key] = {
        "spot_return_1h": spot_now / spot_previous - 1.0,
        "oi_change_1h": oi_now / oi_previous - 1.0,
    }
    return FEATURE_CACHE[cache_key]


def exhaustion_batches(
    base,
    bars_by_symbol,
    cache: Path,
    *,
    min_initial_return_1h: float,
    min_initial_volume_ratio: float,
    min_perp_return_1h: float,
    min_oi_change_1h: float,
    max_spot_perp_ratio: float,
    min_peak_retrace: float,
    max_close_location: float,
    risk_scale: float,
):
    indexes = {
        symbol: {bar.ts: i for i, bar in enumerate(bars)}
        for symbol, bars in bars_by_symbol.items()
    }
    output = defaultdict(list)
    diagnostics = []
    for values in base.values():
        for signal in values:
            if (
                signal.side <= 0
                or signal.return_1h < min_initial_return_1h
                or signal.volume_ratio < min_initial_volume_ratio
            ):
                continue
            execute_ms = signal.signal_ms + 1
            bars = bars_by_symbol.get(signal.symbol, [])
            i = indexes.get(signal.symbol, {}).get(execute_ms)
            if i is None or i < 4:
                continue
            bar = bars[i]
            at_ms = bar.ts + rp.BAR_MS
            features = load_market_features(signal, cache, at_ms)
            if not features:
                continue
            perp_return = bar.close / bars[i - 4].close - 1.0
            if perp_return <= 0:
                continue
            spot_ratio = features["spot_return_1h"] / perp_return
            peak_retrace = bar.close / bar.high - 1.0
            candle_range = bar.high - bar.low
            close_location = (bar.close - bar.low) / candle_range if candle_range else 0.5
            accepted = (
                perp_return >= min_perp_return_1h
                and features["oi_change_1h"] >= min_oi_change_1h
                and spot_ratio <= max_spot_perp_ratio
                and -peak_retrace >= min_peak_retrace
                and close_location <= max_close_location
            )
            diagnostics.append({
                "symbol": signal.symbol,
                "signal_ms": signal.signal_ms,
                "at_ms": at_ms,
                "initial_return_1h": signal.return_1h,
                "initial_volume_ratio": signal.volume_ratio,
                "perp_return_1h": perp_return,
                "spot_return_1h": features["spot_return_1h"],
                "spot_perp_ratio": spot_ratio,
                "oi_change_1h": features["oi_change_1h"],
                "peak_retrace": -peak_retrace,
                "close_location": close_location,
                "accepted": accepted,
            })
            if not accepted:
                continue
            exhaustion = copy.copy(signal)
            exhaustion.signal_ms = at_ms - 1
            exhaustion.side = -1
            exhaustion.price = bar.close
            exhaustion.phase = "pulse_exhaustion"
            exhaustion.trigger = "pulse_exhaustion_short"
            exhaustion.risk_scale = risk_scale
            exhaustion.score = signal.score * (1.0 + features["oi_change_1h"])
            output[at_ms].append(exhaustion)
    return output, diagnostics


def compound(runs):
    return (math.prod(1.0 + run["return_pct"] / 100.0 for run in runs) - 1.0) * 100.0


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
    base = rp.build_signal_batches(bars, start_ms, end_ms, cfg)
    pulse_observations = independent_pulse_observations(
        bars, start_ms, end_ms, cfg
    )
    # Populate the signal-time spot/OI fields used by the acceleration leg.
    _, enriched = spot_led.enrich(
        base, Path("/private/tmp/greed-altcoin-spot-led"), include_overextended=True
    )
    with gzip.open("/private/tmp/altcoin-adaptive-minute.pkl.gz", "rb") as handle:
        period, minutes = pickle.load(handle)
    assert period == (start_ms, end_ms)

    baseline_variant = adaptive.Variant("baseline", long_scale=0.75)
    baseline_runs = [
        adaptive.replay_variant(base, bars, minutes, a, b, cfg, baseline_variant)
        for a, b in windows
    ]
    results = []
    for initial_r1 in (0.06, 0.08, 0.10):
        for initial_volume in (10.0, 20.0):
            for perp_r1 in (0.06, 0.10, 0.15):
                for oi in (0.10, 0.20, 0.30):
                    for spot_ratio in (0.90, 1.00, 1.10):
                        for retrace in (0.015, 0.025, 0.035):
                            for location in (0.30, 0.50):
                                exhaustion, diagnostics = exhaustion_batches(
                                    pulse_observations,
                                    bars,
                                    Path("/private/tmp/greed-altcoin-spot-led"),
                                    min_initial_return_1h=initial_r1,
                                    min_initial_volume_ratio=initial_volume,
                                    min_perp_return_1h=perp_r1,
                                    min_oi_change_1h=oi,
                                    max_spot_perp_ratio=spot_ratio,
                                    min_peak_retrace=retrace,
                                    max_close_location=location,
                                    risk_scale=0.15,
                                )
                                if not exhaustion:
                                    continue
                                combined = merge_batches(base, exhaustion)
                                variants = {
                                    "short_only": adaptive.Variant(
                                        "short_only",
                                        long_scale=0.75,
                                        direct_signal_triggers=("pulse_exhaustion_short",),
                                        max_positions=3,
                                        max_gross_multiple=3.0,
                                    ),
                                    "two_stage": adaptive.Variant(
                                        "two_stage",
                                        long_scale=0.75,
                                        acceleration_direct=True,
                                        acceleration_side="long",
                                        acceleration_allow_overextended=True,
                                        acceleration_min_return_1h=initial_r1,
                                        acceleration_min_return_4h=0.06,
                                        acceleration_min_volume_ratio=initial_volume,
                                        acceleration_risk_scale=0.15,
                                        acceleration_min_spot_return_1h=0.04,
                                        acceleration_min_spot_perp_ratio=0.50,
                                        acceleration_min_oi_change_1h=0.0,
                                        acceleration_max_perp_premium=0.01,
                                        acceleration_preserve_pending=True,
                                        acceleration_skip_cooldown=True,
                                        direct_signal_triggers=("pulse_exhaustion_short",),
                                        max_positions=3,
                                        max_gross_multiple=3.0,
                                    ),
                                }
                                for model, variant in variants.items():
                                    runs = [
                                        adaptive.replay_variant(
                                            combined, bars, minutes, a, b, cfg, variant
                                        )
                                        for a, b in windows
                                    ]
                                    pulse_trades = [
                                        trade
                                        for run in runs
                                        for trade in run["trades"]
                                        if trade["trigger"] in {
                                            "trend_acceleration_direct",
                                            "pulse_exhaustion_short",
                                        }
                                    ]
                                    short_trades = [
                                        trade for trade in pulse_trades
                                        if trade["trigger"] == "pulse_exhaustion_short"
                                    ]
                                    results.append({
                                        "model": model,
                                        "parameters": {
                                            "initial_r1": initial_r1,
                                            "initial_volume": initial_volume,
                                            "perp_r1": perp_r1,
                                            "oi": oi,
                                            "spot_ratio": spot_ratio,
                                            "retrace": retrace,
                                            "location": location,
                                        },
                                        "compound_return_pct": compound(runs),
                                        "worst_window_pct": min(run["return_pct"] for run in runs),
                                        "max_drawdown_pct": max(run["max_drawdown_pct"] for run in runs),
                                        "entries": sum(run["entries"] for run in runs),
                                        "pulse_entries": len(pulse_trades),
                                        "pulse_pnl": sum(trade["pnl"] for trade in pulse_trades),
                                        "short_entries": len(short_trades),
                                        "short_pnl": sum(trade["pnl"] for trade in short_trades),
                                        "windows": runs,
                                        "accepted_events": [item for item in diagnostics if item["accepted"]],
                                    })
    ranked = sorted(
        results,
        key=lambda item: (
            item["worst_window_pct"] >= 0,
            item["compound_return_pct"],
            item["short_pnl"],
        ),
        reverse=True,
    )
    best_by_model = {
        model: next(item for item in ranked if item["model"] == model)
        for model in ("short_only", "two_stage")
        if any(item["model"] == model for item in ranked)
    }
    robustness = []
    if "short_only" in best_by_model:
        best_parameters = best_by_model["short_only"]["parameters"]
        for risk_scale, slippage, stop, activation, trail in (
            [(risk, 5.0, 0.010, 0.015, 0.005) for risk in (0.10, 0.15, 0.20, 0.25)]
            + [(0.15, slip, 0.010, 0.015, 0.005) for slip in (7.5, 10.0, 15.0)]
            + [
                (0.15, 5.0, stop, activation, trail)
                for stop in (0.0075, 0.010, 0.0125)
                for activation in (0.010, 0.015, 0.020)
                for trail in (0.003, 0.005, 0.0075)
            ]
        ):
            exhaustion, _ = exhaustion_batches(
                pulse_observations,
                bars,
                Path("/private/tmp/greed-altcoin-spot-led"),
                min_initial_return_1h=best_parameters["initial_r1"],
                min_initial_volume_ratio=best_parameters["initial_volume"],
                min_perp_return_1h=best_parameters["perp_r1"],
                min_oi_change_1h=best_parameters["oi"],
                max_spot_perp_ratio=best_parameters["spot_ratio"],
                min_peak_retrace=best_parameters["retrace"],
                max_close_location=best_parameters["location"],
                risk_scale=risk_scale,
            )
            combined = merge_batches(base, exhaustion)
            variant = adaptive.Variant(
                "robustness",
                long_scale=0.75,
                slippage_bps=slippage,
                direct_signal_triggers=("pulse_exhaustion_short",),
                pulse_exhaustion_stop_pct=stop,
                pulse_exhaustion_activation_pct=activation,
                pulse_exhaustion_trail_pct=trail,
                max_positions=3,
                max_gross_multiple=3.0,
            )
            runs = [
                adaptive.replay_variant(combined, bars, minutes, a, b, cfg, variant)
                for a, b in windows
            ]
            short_trades = [
                trade for run in runs for trade in run["trades"]
                if trade["trigger"] == "pulse_exhaustion_short"
            ]
            robustness.append({
                "risk_scale": risk_scale,
                "slippage_bps": slippage,
                "stop_pct": stop,
                "activation_pct": activation,
                "trail_pct": trail,
                "compound_return_pct": compound(runs),
                "worst_window_pct": min(run["return_pct"] for run in runs),
                "max_drawdown_pct": max(run["max_drawdown_pct"] for run in runs),
                "entries": sum(run["entries"] for run in runs),
                "short_entries": len(short_trades),
                "short_pnl": sum(trade["pnl"] for trade in short_trades),
            })
    output = {
        "period": [rp.iso(start_ms), rp.iso(end_ms)],
        "enriched_signals": enriched,
        "baseline": {
            "compound_return_pct": compound(baseline_runs),
            "worst_window_pct": min(run["return_pct"] for run in baseline_runs),
            "max_drawdown_pct": max(run["max_drawdown_pct"] for run in baseline_runs),
            "entries": sum(run["entries"] for run in baseline_runs),
            "windows": baseline_runs,
        },
        "tested": len(results),
        "best_by_model": best_by_model,
        "robustness": robustness,
        "top": ranked[:50],
    }
    Path("/private/tmp/altcoin-pulse-exhaustion.json").write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n"
    )
    print(json.dumps({
        "baseline": {key: output["baseline"][key] for key in (
            "compound_return_pct", "worst_window_pct", "max_drawdown_pct", "entries"
        )},
        "enriched_signals": enriched,
        "tested": len(results),
        "best_by_model": {
            model: {key: item[key] for key in (
                "parameters", "compound_return_pct", "worst_window_pct",
                "max_drawdown_pct", "entries", "pulse_entries", "pulse_pnl",
                "short_entries", "short_pnl"
            )}
            for model, item in best_by_model.items()
        },
        "top": [
            {key: item[key] for key in (
                "model", "parameters", "compound_return_pct", "worst_window_pct",
                "max_drawdown_pct", "entries", "pulse_entries", "pulse_pnl",
                "short_entries", "short_pnl"
            )}
            for item in ranked[:20]
        ],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
