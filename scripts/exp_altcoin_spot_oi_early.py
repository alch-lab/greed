#!/usr/bin/env python3
"""Enrich strict early-confirmation events with spot flow and futures OI."""

from __future__ import annotations

import argparse
import csv
import gzip
import importlib.util
import io
import json
import heapq
import sys
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    value = importlib.util.module_from_spec(spec)
    sys.modules[name] = value
    spec.loader.exec_module(value)
    return value


early = module("early_event", ROOT / "scripts/exp_altcoin_early_event.py")


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
        time.sleep(0.5 * 2**attempt)
    return False


def fetch_day(cache: Path, symbol: str, day: str) -> tuple[str, str, bool, bool]:
    if not symbol.isascii():
        return symbol, day, False, False
    spot = cache / "spot" / symbol / f"{day}.zip"
    metrics = cache / "metrics" / symbol / f"{day}.zip"
    spot_ok = download(f"https://data.binance.vision/data/spot/daily/klines/{symbol}/15m/{symbol}-15m-{day}.zip", spot)
    metrics_ok = download(f"https://data.binance.vision/data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{day}.zip", metrics)
    return symbol, day, spot_ok, metrics_ok


def spot_rows(path: Path) -> dict[int, tuple[float, float, float, float]]:
    output = {}
    with zipfile.ZipFile(path) as archive:
        for row in csv.reader(io.TextIOWrapper(archive.open(archive.namelist()[0]))):
            if not row or not row[0].isdigit():
                continue
            ts = int(row[0])
            if ts > 10**15:
                ts //= 1000
            quote, taker = float(row[7]), float(row[10])
            output[ts] = (float(row[1]), float(row[4]), quote, 2 * taker / quote - 1 if quote > 0 else 0.0)
    return output


def oi_rows(path: Path) -> list[tuple[int, float]]:
    with zipfile.ZipFile(path) as archive:
        rows = csv.DictReader(io.TextIOWrapper(archive.open(archive.namelist()[0])))
        return [
            (
                int(datetime.strptime(row["create_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000),
                float(row["sum_open_interest_value"]),
            )
            for row in rows
        ]


def before(rows: list[tuple[int, float]], ts: int) -> float | None:
    values = [value for at, value in rows if at <= ts]
    return values[-1] if values else None


def online_direction(signals, all_bars, cfg, window: int, method: str, slip_bps: float = 5.0):
    histories = {"continue": deque(maxlen=window), "fade": deque(maxlen=window)}
    pending = []
    selected = []

    def direction_signal(signal, mode):
        return signal if mode == "continue" else early.Signal(
            signal.symbol, signal.index, signal.execute_ts, -signal.side, signal.score, "confirmed_fade"
        )

    def score(values):
        if len(values) < window:
            return None
        if method == "sum":
            return sum(values)
        gains = sum(max(value, 0.0) for value in values)
        losses = sum(max(-value, 0.0) for value in values)
        return gains / losses if losses else 99.0

    for signal in sorted(signals, key=lambda item: item.execute_ts):
        while pending and pending[0][0] < signal.execute_ts:
            _, mode, value = heapq.heappop(pending)
            histories[mode].append(value)
        scores = {mode: score(values) for mode, values in histories.items()}
        if all(value is not None for value in scores.values()):
            mode = max(scores, key=lambda key: scores[key])
            threshold = 0.0 if method == "sum" else 1.0
            if scores[mode] > threshold:
                selected.append(direction_signal(signal, mode))
        for mode in ("continue", "fade"):
            candidate = direction_signal(signal, mode)
            result = early.outcome(candidate, all_bars[candidate.symbol], cfg, slip_bps)
            if result:
                heapq.heappush(pending, (result[0], mode, result[1]))
    return selected


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("/private/tmp/binance-testnet-15m-20260515-0820.json.gz"))
    parser.add_argument("--cache", type=Path, default=Path("/private/tmp/greed-altcoin-early-market"))
    parser.add_argument("--output", type=Path, default=Path("/private/tmp/greed-altcoin-spot-oi-early.json"))
    args = parser.parse_args()
    all_bars = early.load(args.data)
    grouped = early.features(all_bars)
    signals = early.build_signals(grouped, "confirm", 3)
    signal_features = {(item.symbol, item.index): item for rows in grouped.values() for item in rows}
    with gzip.open(args.data, "rt") as handle:
        raw = json.load(handle)["data"]
    jobs = sorted({(signal.symbol, datetime.fromtimestamp((signal.execute_ts - early.BAR_MS) / 1000, timezone.utc).date().isoformat()) for signal in signals})
    completed = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(fetch_day, args.cache, symbol, day) for symbol, day in jobs]
        for future in as_completed(futures):
            future.result()
            completed += 1
            if completed % 100 == 0:
                print(f"downloaded {completed}/{len(jobs)}", flush=True)
    cache_rows = {}
    enriched = []
    for signal in signals:
        feature = signal_features[(signal.symbol, signal.index)]
        day = datetime.fromtimestamp((signal.execute_ts - early.BAR_MS) / 1000, timezone.utc).date().isoformat()
        spot_path = args.cache / "spot" / signal.symbol / f"{day}.zip"
        metrics_path = args.cache / "metrics" / signal.symbol / f"{day}.zip"
        if not spot_path.exists() or not metrics_path.exists():
            continue
        key = (signal.symbol, day)
        if key not in cache_rows:
            cache_rows[key] = (spot_rows(spot_path), oi_rows(metrics_path))
        spot, oi = cache_rows[key]
        signal_ts = signal.execute_ts - early.BAR_MS
        spot_now = spot.get(signal_ts)
        spot_back = spot.get(signal_ts - 3 * early.BAR_MS)
        oi_now, oi_back = before(oi, signal_ts + early.BAR_MS - 1), before(oi, signal_ts - 15 * 60_000)
        if not spot_now or not spot_back or not oi_now or not oi_back or spot_back[1] <= 0:
            continue
        raw_bar = raw[signal.symbol][signal.index]
        perp_quote, perp_taker = float(raw_bar[7]), float(raw_bar[10])
        spot_return = spot_now[1] / spot_back[1] - 1
        perp_delta = 2 * perp_taker / perp_quote - 1 if perp_quote > 0 else 0.0
        spot_delta = spot_now[3]
        oi_change = oi_now / oi_back - 1
        premium = feature.r15 - spot_return
        enriched.append({"signal": signal, "feature": feature, "spot_return": spot_return, "spot_delta": spot_delta, "perp_delta": perp_delta, "oi_change": oi_change, "premium": premium})
    filters = {
        "spot_same": lambda x: x["signal"].side * x["spot_return"] > 0,
        "spot_leads_50": lambda x: x["signal"].side * x["spot_return"] >= 0.5 * x["signal"].side * x["feature"].r15,
        "spot_leads_delta": lambda x: x["signal"].side * x["spot_return"] >= 0.5 * x["signal"].side * x["feature"].r15 and x["signal"].side * x["spot_delta"] >= 0.10 and x["signal"].side * x["perp_delta"] >= 0.10,
        "spot_leads_uncrowded": lambda x: x["signal"].side * x["spot_return"] >= 0.5 * x["signal"].side * x["feature"].r15 and -0.02 <= x["oi_change"] <= 0.05,
        "full": lambda x: x["signal"].side * x["spot_return"] >= 0.75 * x["signal"].side * x["feature"].r15 and x["signal"].side * x["spot_delta"] >= 0.10 and x["signal"].side * x["perp_delta"] >= 0.10 and -0.02 <= x["oi_change"] <= 0.05 and x["signal"].side * x["premium"] <= 0.01,
    }
    exits = [early.Exit(stop, target, bars) for stop in (0.01, 0.015, 0.02) for target in (0.015, 0.02, 0.03) for bars in (2, 4, 8)]
    results = {}
    for name, accepts in filters.items():
        directional = [item["signal"] for item in enriched if accepts(item)]
        for mode in ("continue", "fade"):
            selected = directional if mode == "continue" else [
                early.Signal(signal.symbol, signal.index, signal.execute_ts, -signal.side, signal.score, "confirmed_fade")
                for signal in directional
            ]
            for cfg in exits:
                key = f"{name}_{mode}__{cfg.name}"
                results[key] = {"filter": name, "mode": mode, "exit": cfg.__dict__, "periods": {period: early.simulate(selected, all_bars, cfg, early.stamp(bounds[0]), early.stamp(bounds[1])) for period, bounds in early.PERIODS.items()}}
    eligible = []
    for name, item in results.items():
        train, valid = item["periods"]["train"], item["periods"]["validation"]
        if train["return_pct"] > 0 and valid["return_pct"] > 0 and train["trades"] >= 15 and valid["trades"] >= 10:
            eligible.append((valid["return_pct"] - 0.5 * valid["max_drawdown_pct"], name))
    ranked = [name for _, name in sorted(eligible, reverse=True)]
    online_results = {}
    for name, accepts in filters.items():
        directional = [item["signal"] for item in enriched if accepts(item)]
        for cfg in exits:
            for window in (5, 10, 20):
                for method in ("sum", "pf"):
                    selected = online_direction(directional, all_bars, cfg, window, method)
                    key = f"{name}_online_{method}{window}__{cfg.name}"
                    online_results[key] = {"filter": name, "window": window, "method": method, "exit": cfg.__dict__, "periods": {period: early.simulate(selected, all_bars, cfg, early.stamp(bounds[0]), early.stamp(bounds[1])) for period, bounds in early.PERIODS.items()}}
    online_eligible = []
    for name, item in online_results.items():
        train, valid = item["periods"]["train"], item["periods"]["validation"]
        if train["return_pct"] > 0 and valid["return_pct"] > 0 and train["trades"] >= 15 and valid["trades"] >= 10:
            online_eligible.append((valid["return_pct"] - 0.5 * valid["max_drawdown_pct"], name))
    online_ranked = [name for _, name in sorted(online_eligible, reverse=True)]
    for key in online_ranked[:10]:
        item = online_results[key]
        cfg = early.Exit(**item["exit"])
        directional = [value["signal"] for value in enriched if filters[item["filter"]](value)]
        item["stress"] = {}
        for slip_bps in (10.0, 15.0):
            selected = online_direction(
                directional, all_bars, cfg, item["window"], item["method"], slip_bps
            )
            item["stress"][f"slip_{slip_bps:.0f}bps"] = {
                period: early.simulate(
                    selected,
                    all_bars,
                    cfg,
                    early.stamp(bounds[0]),
                    early.stamp(bounds[1]),
                    slip_bps,
                )
                for period, bounds in early.PERIODS.items()
            }
    report = {"signals": len(signals), "enriched": len(enriched), "ranked_without_holdout": ranked, "online_ranked_without_holdout": online_ranked, "results": results, "online_results": online_results}
    args.output.write_text(json.dumps(report, default=lambda value: value.__dict__, indent=2) + "\n")
    compact = lambda periods: {period: {key: values[key] for key in ("return_pct", "max_drawdown_pct", "trades", "trades_per_day", "win_rate_pct", "profit_factor")} for period, values in periods.items()}
    print(json.dumps({"signals": len(signals), "enriched": len(enriched), "eligible": len(ranked), "online_eligible": len(online_ranked), "online_top": [{"name": name, "periods": compact(online_results[name]["periods"])} for name in online_ranked[:10]]}, indent=2))


if __name__ == "__main__":
    main()
