#!/usr/bin/env python3
"""Causal hourly cross-sectional ridge model for the aggressive altcoin sleeve."""

from __future__ import annotations

import argparse
import heapq
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from exp_altcoin_five_optimizations import load_symbol_bars
from exp_altcoin_oi_launch import DAY_MS, MAJORS, load_bars


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july_holdout": ("2026-07-01", "2026-08-01"),
    "aug_holdout": ("2026-08-01", "2026-08-11"),
}


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def efficiency(closes: np.ndarray) -> float:
    return abs(closes[-1] - closes[0]) / max(np.abs(np.diff(closes)).sum(), 1e-12)


def symbol_frame(cache: Path, symbol: str, horizon_hours: int) -> pd.DataFrame | None:
    bars = load_symbol_bars(cache, symbol)
    paths = sorted((cache / "spot-klines" / symbol).glob("*.zip")) + sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
    spot_days = {(bar.ts // 1_000 if bar.ts > 10**15 else bar.ts) // DAY_MS for bar in load_bars(paths)}
    horizon = horizon_hours * 4
    if len(bars) < 8 * 96 or not spot_days:
        return None
    close = np.asarray([bar.close for bar in bars], dtype=np.float64)
    high = np.asarray([bar.high for bar in bars], dtype=np.float64)
    low = np.asarray([bar.low for bar in bars], dtype=np.float64)
    open_ = np.asarray([bar.open for bar in bars], dtype=np.float64)
    volume = np.asarray([bar.quote_volume for bar in bars], dtype=np.float64)
    rows = []
    for i in range(72 * 4, len(bars) - horizon - 1):
        signal = bars[i]
        if datetime.fromtimestamp(signal.ts / 1_000, timezone.utc).minute != 45:
            continue
        entry_i = i + 1
        exit_i = entry_i + horizon
        entry_ts = bars[entry_i].ts
        if entry_ts // DAY_MS not in spot_days:
            continue
        volume_24h = float(volume[i - 95 : i + 1].sum())
        if volume_24h < 10_000_000:
            continue
        hour_volume = float(volume[i - 3 : i + 1].sum())
        avg_hour_volume = volume_24h / 24
        returns_24 = np.diff(np.log(close[i - 96 : i + 1]))
        span = high[i] - low[i]
        rows.append(
            (
                entry_ts, symbol, open_[entry_i], open_[exit_i] / open_[entry_i] - 1,
                float(low[entry_i:exit_i].min() / open_[entry_i] - 1),
                float(high[entry_i:exit_i].max() / open_[entry_i] - 1),
                volume_24h,
                close[i] / close[i - 4] - 1,
                close[i] / close[i - 16] - 1,
                close[i] / close[i - 48] - 1,
                close[i] / close[i - 96] - 1,
                close[i] / close[i - 288] - 1,
                hour_volume / max(avg_hour_volume, 1),
                float(returns_24[-16:].std()),
                float(returns_24.std()),
                efficiency(close[i - 16 : i + 1]),
                (close[i] - low[i]) / span if span > 0 else 0.5,
            )
        )
    if not rows:
        return None
    return pd.DataFrame(rows, columns=[
        "ts", "symbol", "entry", "future_return", "future_low", "future_high", "volume_24h",
        "r1", "r4", "r12", "r24", "r72", "volume_ratio", "vol4", "vol24", "eff4", "close_location",
    ])


def dataset(cache: Path, horizon_hours: int) -> pd.DataFrame:
    symbols = sorted(
        {path.name for path in (cache / "klines").iterdir() if path.is_dir()}
        & {path.name for root in (cache / "spot-klines", cache / "spot-klines-daily") if root.exists() for path in root.iterdir() if path.is_dir()}
        - MAJORS
    )
    frames = []
    for index, symbol in enumerate(symbols, 1):
        frame = symbol_frame(cache, symbol, horizon_hours)
        if frame is not None:
            frames.append(frame)
        if index % 100 == 0:
            print(f"h={horizon_hours} loaded {index}/{len(symbols)}", flush=True)
    data = pd.concat(frames, ignore_index=True)
    grouped = data.groupby("ts", sort=False)
    for feature in ("r1", "r4", "r12", "r24", "r72", "volume_24h", "volume_ratio", "vol24"):
        data[f"rank_{feature}"] = grouped[feature].rank(pct=True) - 0.5
    data["market_r24"] = grouped["r24"].transform("median")
    data["market_breadth"] = grouped["r24"].transform(lambda values: (values > 0).mean()) - 0.5
    data["dispersion_r24"] = grouped["r24"].transform("std").fillna(0)
    data["rank_market_interaction"] = data["rank_r24"] * data["market_r24"]
    data["r4_market_interaction"] = data["r4"] * data["market_r24"]
    return data


FEATURES = [
    "r1", "r4", "r12", "r24", "r72", "volume_ratio", "vol4", "vol24", "eff4", "close_location",
    "rank_r1", "rank_r4", "rank_r12", "rank_r24", "rank_r72", "rank_volume_24h", "rank_volume_ratio", "rank_vol24",
    "market_r24", "market_breadth", "dispersion_r24", "rank_market_interaction", "r4_market_interaction",
]


def fit_ridge(data: pd.DataFrame, ridge: float) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    start, end = map(timestamp, PERIODS["train"])
    train = data[(data.ts >= start) & (data.ts < end)]
    x = train[FEATURES].to_numpy(dtype=np.float64)
    y = train.future_return.clip(-0.20, 0.20).to_numpy(dtype=np.float64)
    mean = x.mean(axis=0)
    std = x.std(axis=0)
    std[std < 1e-9] = 1
    x = (x - mean) / std
    x = np.column_stack([np.ones(len(x)), x])
    penalty = np.eye(x.shape[1]) * ridge
    penalty[0, 0] = 0
    beta = np.linalg.solve(x.T @ x + penalty, x.T @ y)
    return beta, mean, std


def predict(data: pd.DataFrame, model: tuple[np.ndarray, np.ndarray, np.ndarray]) -> np.ndarray:
    beta, mean, std = model
    x = (data[FEATURES].to_numpy(dtype=np.float64) - mean) / std
    return beta[0] + x @ beta[1:]


def simulate(
    data: pd.DataFrame,
    start: int,
    end: int,
    horizon_hours: int,
    names: int,
    total_gross: float,
    threshold: float,
    stop: float,
    slippage_bps: float,
) -> dict[str, float | int | None]:
    frame = data[(data.ts >= start) & (data.ts < end) & (data.prediction.abs() >= threshold)].copy()
    frame["abs_prediction"] = frame.prediction.abs()
    selected = frame.sort_values(["ts", "abs_prediction"], ascending=[True, False]).groupby("ts", sort=True).head(names)
    fee_and_slippage = 2 * (0.0005 + slippage_bps / 10_000)
    cohort_gross = total_gross / horizon_hours / names
    equity = peak = 1_000.0
    max_drawdown = 0.0
    pending: list[tuple[int, float, float]] = []
    daily_latched = False
    current_day = None
    day_start = equity
    wins = losses = gross_profit = gross_loss = 0.0
    trades = blocked = 0

    def realize(until: int) -> None:
        nonlocal equity, peak, max_drawdown, wins, losses, gross_profit, gross_loss
        while pending and pending[0][0] <= until:
            _, pnl, net_return = heapq.heappop(pending)
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, 1 - equity / peak)
            if net_return > 0:
                wins += 1
                gross_profit += net_return
            else:
                losses += 1
                gross_loss -= net_return

    for ts, group in selected.groupby("ts", sort=True):
        ts = int(ts)
        realize(ts)
        day = ts // DAY_MS
        if day != current_day:
            current_day = day
            day_start = equity
            daily_latched = False
        if equity < day_start * 0.96:
            daily_latched = True
        if daily_latched:
            blocked += len(group)
            continue
        for row in group.itertuples():
            side = 1 if row.prediction > 0 else -1
            stopped = row.future_low <= -stop if side > 0 else row.future_high >= stop
            raw_return = -stop if stopped else side * row.future_return
            net_return = raw_return - fee_and_slippage
            exit_ts = ts + horizon_hours * 3_600_000
            heapq.heappush(pending, (exit_ts, equity * cohort_gross * net_return, cohort_gross * net_return))
            trades += 1
    realize(10**30)
    days = max((end - start) / DAY_MS, 1)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "trades": trades,
        "trades_per_day": trades / days,
        "hours_per_trade": days * 24 / trades if trades else None,
        "win_rate_pct": wins / max(wins + losses, 1) * 100,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "blocked_entries": blocked,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/private/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/private/tmp/greed-altcoin-hourly-ridge.json")
    args = parser.parse_args()
    all_results = []
    for horizon in (2, 4):
        data = dataset(Path(args.cache), horizon)
        for ridge in (1.0, 10.0, 100.0):
            model = fit_ridge(data, ridge)
            data["prediction"] = predict(data, model)
            for names in (1, 2):
                for gross in (0.5, 0.75, 1.0):
                    for threshold in (0.002, 0.004, 0.006, 0.01):
                        config = {"horizon_hours": horizon, "ridge": ridge, "names": names, "gross": gross, "threshold": threshold, "stop": 0.08}
                        results = {name: simulate(data, *map(timestamp, bounds), horizon, names, gross, threshold, 0.08, 5.0) for name, bounds in PERIODS.items()}
                        if results["train"]["return_pct"] > 0 and results["validation"]["return_pct"] > 0:
                            stress = {name: simulate(data, *map(timestamp, bounds), horizon, names, gross, threshold, 0.08, 15.0) for name, bounds in PERIODS.items() if "holdout" in name}
                            all_results.append({"config": config, **results, "stress_15bps": stress})
        del data
    all_results.sort(key=lambda row: min(row["train"]["return_pct"], row["validation"]["return_pct"]), reverse=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "train-only ridge; hourly closed-bar features; next-open entry; causal overlapping portfolio; fee + slippage both sides",
        "features": FEATURES,
        "eligible_train_validation": len(all_results),
        "finalists": all_results[:30],
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"eligible={len(all_results)} report={args.output}")


if __name__ == "__main__":
    main()
