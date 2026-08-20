#!/usr/bin/env python3
"""Test a small direct-entry sleeve for spot-led altcoin acceleration."""

from __future__ import annotations

import argparse
import csv
import gzip
import importlib.util
import io
import json
import math
import pickle
import sys
import time
import tomllib
import urllib.error
import urllib.request
import zipfile
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


rp = load_module("spot_led_replay", ROOT / "scripts/replay_altcoin_current_3d.py")
adaptive = load_module("spot_led_adaptive", ROOT / "scripts/exp_altcoin_adaptive_risk.py")


def download(url: str, path: Path) -> bool:
    if path.exists():
        return path.stat().st_size > 0
    path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                path.write_bytes(response.read())
            return True
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return False
        except (TimeoutError, urllib.error.URLError):
            pass
        time.sleep(0.5 * (2**attempt))
    return False


def spot_closes(path: Path) -> dict[int, float]:
    with zipfile.ZipFile(path) as archive:
        rows = csv.reader(io.TextIOWrapper(archive.open(archive.namelist()[0])))
        output = {}
        for row in rows:
            if not row or not row[0].isdigit():
                continue
            timestamp = int(row[0])
            if timestamp > 10**15:
                timestamp //= 1_000
            output[timestamp] = float(row[4])
        return output


def oi_values(path: Path) -> list[tuple[int, float]]:
    with zipfile.ZipFile(path) as archive:
        rows = csv.DictReader(io.TextIOWrapper(archive.open(archive.namelist()[0])))
        return [
            (
                int(datetime.strptime(row["create_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1_000),
                float(row["sum_open_interest_value"]),
            )
            for row in rows
        ]


def nearest_before(values: list[tuple[int, float]], timestamp: int) -> float | None:
    eligible = [value for ts, value in values if ts <= timestamp]
    return eligible[-1] if eligible else None


def enrich(signals, cache: Path, include_overextended: bool = False):
    candidates = [
        signal
        for values in signals.values()
        for signal in values
        if signal.side > 0
        and (signal.phase == "standard_impulse" or include_overextended)
        and signal.return_1h >= 0.05
        and signal.return_4h >= 0.06
    ]
    symbol_days = {
        (signal.symbol, datetime.fromtimestamp(signal.signal_ms / 1_000, timezone.utc).date().isoformat())
        for signal in candidates
    }
    for number, (symbol, day) in enumerate(sorted(symbol_days), 1):
        spot_path = cache / "spot" / symbol / f"{day}.zip"
        metrics_path = cache / "metrics" / symbol / f"{day}.zip"
        download(
            f"https://data.binance.vision/data/spot/daily/klines/{symbol}/15m/{symbol}-15m-{day}.zip",
            spot_path,
        )
        download(
            f"https://data.binance.vision/data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{day}.zip",
            metrics_path,
        )
        if number % 20 == 0:
            print(f"downloaded {number}/{len(symbol_days)} symbol-days", flush=True)

    enriched = 0
    for signal in candidates:
        day = datetime.fromtimestamp(signal.signal_ms / 1_000, timezone.utc).date().isoformat()
        spot_path = cache / "spot" / signal.symbol / f"{day}.zip"
        metrics_path = cache / "metrics" / signal.symbol / f"{day}.zip"
        if not spot_path.exists() or not metrics_path.exists():
            continue
        closes = spot_closes(spot_path)
        open_ms = (signal.signal_ms + 1) - rp.BAR_MS
        current = closes.get(open_ms)
        previous = closes.get(open_ms - 4 * rp.BAR_MS)
        metrics = oi_values(metrics_path)
        current_oi = nearest_before(metrics, signal.signal_ms)
        previous_oi = nearest_before(metrics, signal.signal_ms - 60 * rp.MINUTE_MS)
        if not current or not previous or not current_oi or not previous_oi:
            continue
        signal.spot_return_1h = current / previous - 1.0
        signal.oi_change_1h = current_oi / previous_oi - 1.0
        signal.perp_premium = signal.price / current - 1.0
        enriched += 1
    return candidates, enriched


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", type=Path, default=Path("/private/tmp/altcoin-fast-grid-bars.pkl.gz"))
    parser.add_argument("--market-cache", type=Path, default=Path("/private/tmp/greed-altcoin-spot-led"))
    parser.add_argument("--output", type=Path, default=Path("/private/tmp/greed-altcoin-spot-led.json"))
    args = parser.parse_args()
    with (ROOT / "config/strategy-altcoin-impulse.toml").open("rb") as handle:
        cfg = tomllib.load(handle)["altcoin_impulse"]
    with gzip.open(args.cache, "rb") as handle:
        _, spot_symbols, _, all_bars = pickle.load(handle)
    reports = [Path("/private/tmp/altcoin-entry-grid-prior.json"), Path("/private/tmp/altcoin-entry-grid-latest.json")]
    windows = []
    for report in reports:
        period = json.loads(report.read_text())["period"]
        windows.append(tuple(int(datetime.fromisoformat(value).timestamp() * 1_000) for value in period))
    start_ms, end_ms = windows[0][0], windows[-1][1]
    bars_15m = {symbol: all_bars[symbol] for symbol in spot_symbols if symbol in all_bars}
    batches = rp.build_signal_batches(bars_15m, start_ms, end_ms, cfg)
    candidates, enriched = enrich(batches, args.market_cache)
    symbols = sorted({signal.symbol for values in batches.values() for signal in values})
    with gzip.open("/private/tmp/altcoin-adaptive-minute.pkl.gz", "rb") as handle:
        period, minute_bars = pickle.load(handle)
    assert period == (start_ms, end_ms) and set(symbols) <= set(minute_bars)

    variants = [adaptive.Variant("baseline", long_scale=0.75)]
    for minimum_1h in (0.05, 0.06, 0.08):
        for minimum_4h in (0.06, 0.08, 0.10):
            for minimum_volume in (10.0, 20.0):
                for minimum_spot in (0.04, 0.06):
                    for minimum_oi in (0.00, 0.03, 0.05):
                        for maximum_premium in (0.01, 0.00):
                            for risk_scale in (0.25, 0.33):
                                variants.append(adaptive.Variant(
                                    f"r1{minimum_1h:.2f}_r4{minimum_4h:.2f}_v{minimum_volume:.0f}_spot{minimum_spot:.2f}_oi{minimum_oi:.2f}_prem{maximum_premium:.2f}_risk{risk_scale:.2f}",
                                    long_scale=0.75,
                                    acceleration_direct=True,
                                    acceleration_min_return_1h=minimum_1h,
                                    acceleration_min_return_4h=minimum_4h,
                                    acceleration_min_volume_ratio=minimum_volume,
                                    acceleration_risk_scale=risk_scale,
                                    acceleration_min_spot_return_1h=minimum_spot,
                                    acceleration_min_spot_perp_ratio=0.75,
                                    acceleration_min_oi_change_1h=minimum_oi,
                                    acceleration_max_perp_premium=maximum_premium,
                                ))
    for stop_pct in (0.015, 0.020, 0.025, 0.030):
        for activation_pct in (0.015, 0.025, 0.040):
            for trail_pct in (0.005, 0.010):
                for risk_scale in (0.25, 0.33):
                    variants.append(adaptive.Variant(
                        f"exit_stop{stop_pct:.3f}_activation{activation_pct:.3f}_trail{trail_pct:.3f}_risk{risk_scale:.2f}",
                        long_scale=0.75,
                        acceleration_direct=True,
                        acceleration_min_return_1h=0.05,
                        acceleration_min_return_4h=0.10,
                        acceleration_min_volume_ratio=20.0,
                        acceleration_risk_scale=risk_scale,
                        acceleration_min_spot_return_1h=0.06,
                        acceleration_min_spot_perp_ratio=0.75,
                        acceleration_min_oi_change_1h=0.05,
                        acceleration_max_perp_premium=0.0,
                        acceleration_stop_pct=stop_pct,
                        acceleration_activation_pct=activation_pct,
                        acceleration_trail_pct=trail_pct,
                    ))
    for minimum_oi in (0.00, 0.03):
        for risk_scale in (0.25, 0.33):
            for gross_multiple in (2.75, 3.00):
                variants.append(adaptive.Variant(
                    f"separate_probe_oi{minimum_oi:.2f}_risk{risk_scale:.2f}_gross{gross_multiple:.2f}",
                    long_scale=0.75,
                    acceleration_direct=True,
                    acceleration_min_return_1h=0.05,
                    acceleration_min_return_4h=0.06,
                    acceleration_min_volume_ratio=10.0,
                    acceleration_risk_scale=risk_scale,
                    acceleration_min_spot_return_1h=0.04,
                    acceleration_min_spot_perp_ratio=0.75,
                    acceleration_min_oi_change_1h=minimum_oi,
                    acceleration_max_perp_premium=0.01,
                    max_gross_multiple=gross_multiple,
                    max_positions=3,
                ))
                variants.append(adaptive.Variant(
                    f"non_competing_probe_oi{minimum_oi:.2f}_risk{risk_scale:.2f}_gross{gross_multiple:.2f}",
                    long_scale=0.75,
                    acceleration_direct=True,
                    acceleration_min_return_1h=0.05,
                    acceleration_min_return_4h=0.06,
                    acceleration_min_volume_ratio=10.0,
                    acceleration_risk_scale=risk_scale,
                    acceleration_min_spot_return_1h=0.04,
                    acceleration_min_spot_perp_ratio=0.75,
                    acceleration_min_oi_change_1h=minimum_oi,
                    acceleration_max_perp_premium=0.01,
                    acceleration_preserve_pending=True,
                    acceleration_skip_cooldown=True,
                    max_gross_multiple=gross_multiple,
                    max_positions=3,
                ))
    output = {"period": [rp.iso(start_ms), rp.iso(end_ms)], "candidate_events": len(candidates), "enriched_events": enriched, "variants": {}}
    for variant in variants:
        results = [adaptive.replay_variant(batches, bars_15m, minute_bars, a, b, cfg, variant) for a, b in windows]
        compound = (math.prod(1 + result["return_pct"] / 100 for result in results) - 1) * 100
        direct = [trade for result in results for trade in result["trades"] if trade.get("trigger") == "trend_acceleration_direct"]
        output["variants"][variant.name] = {
            "settings": variant.__dict__, "compound_return_pct": compound,
            "worst_window_pct": min(result["return_pct"] for result in results),
            "max_drawdown_pct": max(result["max_drawdown_pct"] for result in results),
            "entries": sum(result["entries"] for result in results),
            "direct_entries": len(direct), "direct_pnl": sum(trade["pnl"] for trade in direct),
            "direct_trades": direct, "windows": results,
        }
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    ranked = sorted(output["variants"].items(), key=lambda item: item[1]["compound_return_pct"], reverse=True)
    print(json.dumps({"candidate_events":len(candidates),"enriched_events":enriched,"top":[{"name":name,**{key:value[key] for key in ("compound_return_pct","worst_window_pct","max_drawdown_pct","entries","direct_entries","direct_pnl")}} for name,value in ranked[:20]]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
