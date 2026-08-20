#!/usr/bin/env python3
"""Walk-forward online gate for daily cross-sectional altcoin reversal."""

from __future__ import annotations

import argparse
import json
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_daily_reversal import (
    Config,
    Snapshot,
    build_snapshots,
    liquid,
    timestamp,
    trade_return,
)
from exp_altcoin_five_optimizations import load_symbol_bars, spot_history_symbols
from exp_altcoin_oi_launch import DAY_MS, MAJORS, Bar, download, fetch, load_bars, load_funding, month_range


PERIODS = {
    "train": ("2026-01-15", "2026-05-01"),
    "validation": ("2026-05-01", "2026-07-01"),
    "july_test": ("2026-07-01", "2026-08-01"),
    "aug_holdout": ("2026-08-01", "2026-08-11"),
    "full": ("2026-01-15", "2026-08-11"),
}


@dataclass(frozen=True)
class DayResult:
    ts: int
    basket_return: float
    trades: int
    members: tuple[str, ...]
    member_returns: tuple[float, ...]


def daily_series(
    config: Config,
    snapshots: dict[int, dict[int, list[Snapshot]]],
    bars_by_symbol: dict[str, list[Bar]],
    funding_by_symbol: dict[str, tuple[list[int], list[float]]],
    slippage_bps: float,
) -> list[DayResult]:
    output: list[DayResult] = []
    for ts in sorted(snapshots[config.formation_days]):
        universe = [
            item
            for item in snapshots[config.formation_days][ts]
            if liquid(item, config.liquidity) and abs(item.trailing_return) <= 1.50
        ]
        if len(universe) < config.names_per_leg:
            continue
        ordered = sorted(universe, key=lambda item: item.trailing_return)
        chosen = [item for item in ordered[-config.names_per_leg :] if item.trailing_return > 0]
        if not chosen:
            continue
        weight = config.gross / len(chosen)
        basket = 0.0
        member_returns: list[float] = []
        for snapshot in chosen:
            value, _ = trade_return(
                snapshot,
                bars_by_symbol[snapshot.symbol],
                -1,
                config.stop,
                slippage_bps,
                funding_by_symbol.get(snapshot.symbol),
            )
            basket += weight * value
            member_returns.append(weight * value)
        output.append(
            DayResult(
                ts,
                basket,
                len(chosen),
                tuple(snapshot.symbol for snapshot in chosen),
                tuple(member_returns),
            )
        )
    return output


def gated_result(
    series: list[DayResult],
    start: int,
    end: int,
    window: int,
    min_mean: float,
    min_pf: float,
    gate_source: list[DayResult] | None = None,
) -> dict[str, float | int | None]:
    source_returns = {day.ts: day.basket_return for day in (gate_source or series)}
    history: deque[float] = deque(maxlen=window)
    equity = 1_000.0
    peak = equity
    drawdown = 0.0
    active_days = 0
    trades = 0
    returns: list[float] = []
    active: list[DayResult] = []
    for day in series:
        if day.ts >= end:
            break
        ready = len(history) == window
        gross_profit = sum(max(value, 0.0) for value in history)
        gross_loss = sum(max(-value, 0.0) for value in history)
        mean = sum(history) / len(history) if history else 0.0
        pf = gross_profit / gross_loss if gross_loss > 0 else 99.0
        enabled = ready and mean >= min_mean and pf >= min_pf
        if start <= day.ts < end and enabled:
            equity *= max(0.01, 1 + day.basket_return)
            returns.append(day.basket_return)
            active.append(day)
            active_days += 1
            trades += day.trades
            peak = max(peak, equity)
            drawdown = max(drawdown, 1 - equity / peak)
        # The observation becomes available one day later. Since daily signals
        # and outcomes are spaced by one day, append only after today's decision.
        history.append(source_returns.get(day.ts, day.basket_return))
    wins = [value for value in returns if value > 0]
    losses = [-value for value in returns if value < 0]
    final_gross_profit = sum(max(value, 0.0) for value in history)
    final_gross_loss = sum(max(-value, 0.0) for value in history)
    final_mean = sum(history) / len(history) if history else 0.0
    final_pf = final_gross_profit / final_gross_loss if final_gross_loss > 0 else 99.0
    final_enabled = len(history) == window and final_mean >= min_mean and final_pf >= min_pf
    def day_record(day: DayResult) -> dict[str, object]:
        return {
            "ts": day.ts,
            "basket_return_pct": day.basket_return * 100,
            "symbols": list(day.members),
            "member_contribution_pct": [value * 100 for value in day.member_returns],
        }
    ordered_active = sorted(active, key=lambda day: day.basket_return)
    return {
        "return_pct": (equity / 1_000 - 1) * 100,
        "max_drawdown_pct": drawdown * 100,
        "active_days": active_days,
        "trades": trades,
        "win_rate_days_pct": len(wins) / len(returns) * 100 if returns else 0.0,
        "profit_factor_days": sum(wins) / sum(losses) if losses and sum(losses) > 0 else None,
        "top_two_days_share_gross_profit": sum(sorted(wins, reverse=True)[:2]) / sum(wins) if wins else 0.0,
        "best_days": [day_record(day) for day in ordered_active[-3:][::-1]],
        "worst_days": [day_record(day) for day in ordered_active[:3]],
        "next_signal_enabled": final_enabled,
        "rolling_mean_pct": final_mean * 100,
        "rolling_profit_factor": final_pf,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-daily-online.json")
    parser.add_argument("--download-aug-funding", action="store_true")
    parser.add_argument("--point-in-time-spot", action="store_true")
    parser.add_argument("--download-spot-history", action="store_true")
    args = parser.parse_args()
    cache = Path(args.cache)
    spot_symbols = spot_history_symbols()
    allowed = set(spot_symbols)
    try:
        exchange_info = json.loads(fetch("https://fapi.binance.com/fapi/v1/exchangeInfo"))
    except Exception:
        exchange_info = {"symbols": []}
    if not args.point_in_time_spot:
        allowed |= {
            str(item.get("symbol", ""))
            for item in exchange_info.get("symbols", [])
            if item.get("underlyingType") == "COIN"
        }
    symbols = sorted({path.parent.name for path in (cache / "klines").glob("*/*.zip")})
    symbols = [symbol for symbol in symbols if symbol in allowed and symbol not in MAJORS]
    if args.download_spot_history:
        spot_jobs: list[tuple[str, Path]] = []
        for symbol in symbols:
            for month in month_range("2026-01", "2026-07"):
                path = cache / "spot-klines" / symbol / f"{symbol}-15m-{month}.zip"
                url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/15m/{symbol}-15m-{month}.zip"
                spot_jobs.append((url, path))
            for day in range(1, 11):
                stamp = f"2026-08-{day:02d}"
                path = cache / "spot-klines-daily" / symbol / f"{symbol}-15m-{stamp}.zip"
                url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/15m/{symbol}-15m-{stamp}.zip"
                spot_jobs.append((url, path))
        with ThreadPoolExecutor(max_workers=48) as pool:
            futures = [pool.submit(download, url, path) for url, path in spot_jobs]
            for completed, future in enumerate(as_completed(futures), 1):
                future.result()
                if completed % 1000 == 0:
                    print(f"spot-history {completed}/{len(spot_jobs)}", flush=True)
    if args.download_aug_funding:
        def fetch_api_funding(symbol: str) -> None:
            path = cache / "funding-api" / f"{symbol}-2026-08.json"
            if path.exists():
                return
            url = (
                "https://fapi.binance.com/fapi/v1/fundingRate"
                f"?symbol={symbol}&startTime={timestamp('2026-08-01')}"
                f"&endTime={timestamp('2026-08-11')}&limit=1000"
            )
            try:
                payload = json.loads(fetch(url))
            except Exception:
                payload = []
            if not isinstance(payload, list):
                payload = []
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload))

        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = [pool.submit(fetch_api_funding, symbol) for symbol in symbols]
            for completed, future in enumerate(as_completed(futures), 1):
                future.result()
                if completed % 100 == 0:
                    print(f"funding-api {completed}/{len(symbols)}", flush=True)
    bars_by_symbol: dict[str, list[Bar]] = {}
    funding_by_symbol: dict[str, tuple[list[int], list[float]]] = {}
    spot_days: dict[str, set[int]] = {}
    snapshots: dict[int, dict[int, list[Snapshot]]] = {formation: {} for formation in (1, 3, 7)}
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        if len(bars) < 15 * 96:
            continue
        bars_by_symbol[symbol] = bars
        if args.point_in_time_spot:
            spot_paths = sorted((cache / "spot-klines" / symbol).glob("*.zip"))
            spot_paths += sorted((cache / "spot-klines-daily" / symbol).glob("*.zip"))
            spot_bars = load_bars(spot_paths)
            if not spot_bars:
                bars_by_symbol.pop(symbol, None)
                continue
            spot_days[symbol] = {
                (bar.ts // 1_000 if bar.ts > 10**15 else bar.ts) // DAY_MS
                for bar in spot_bars
            }
        funding_paths = sorted((cache / "funding" / symbol).glob("*.zip"))
        funding_times, funding_rates = load_funding(funding_paths)
        merged = dict(zip(funding_times, funding_rates))
        api_path = cache / "funding-api" / f"{symbol}-2026-08.json"
        if api_path.exists():
            try:
                api_rows = json.loads(api_path.read_text())
            except (OSError, json.JSONDecodeError):
                api_rows = []
            for row in api_rows if isinstance(api_rows, list) else []:
                try:
                    merged[int(row["fundingTime"])] = float(row["fundingRate"])
                except (KeyError, TypeError, ValueError):
                    continue
        funding_times = sorted(merged)
        funding_by_symbol[symbol] = (funding_times, [merged[item] for item in funding_times])
        for formation in snapshots:
            for item in build_snapshots(symbol, bars, formation):
                if args.point_in_time_spot:
                    if item.signal_ts // DAY_MS not in spot_days[symbol]:
                        continue
                snapshots[formation].setdefault(item.signal_ts, []).append(item)
        if completed % 50 == 0:
            print(f"snapshots {completed}/{len(symbols)}", flush=True)

    base_configs = [
        Config(
            f"R{formation}_short_N{names}_{liquidity}_S{int(stop * 100)}",
            formation,
            names,
            "short",
            liquidity,
            stop,
        )
        for formation in (1, 3, 7)
        for names in (3, 5)
        for liquidity in ("small", "mid", "all")
        for stop in (0.08, 0.12)
    ]
    train = tuple(map(timestamp, PERIODS["train"]))
    validation = tuple(map(timestamp, PERIODS["validation"]))
    candidates: list[tuple[float, str, Config, list[DayResult], dict[str, object], dict[str, object]]] = []
    for config in base_configs:
        series = daily_series(config, snapshots, bars_by_symbol, funding_by_symbol, 5.0)
        for window in (5, 10, 20, 40):
            for min_mean in (0.0, 0.002, 0.005):
                for min_pf in (1.0, 1.10, 1.20):
                    train_result = gated_result(series, *train, window, min_mean, min_pf)
                    if int(train_result["active_days"]) < 20:
                        continue
                    score = float(train_result["return_pct"]) - 0.5 * float(train_result["max_drawdown_pct"])
                    name = f"{config.name}_W{window}_M{min_mean:.3f}_PF{min_pf:.2f}"
                    candidates.append((score, name, config, series, {"window": window, "min_mean": min_mean, "min_pf": min_pf}, train_result))
    candidates.sort(key=lambda item: item[0], reverse=True)
    train_shortlist = candidates[:40]
    validated: list[tuple[float, str, Config, list[DayResult], dict[str, object], dict[str, object], dict[str, object]]] = []
    for _, name, config, series, gate, train_result in train_shortlist:
        validation_result = gated_result(series, *validation, **gate)
        score = float(validation_result["return_pct"]) - 0.5 * float(validation_result["max_drawdown_pct"])
        validated.append((score, name, config, series, gate, train_result, validation_result))
    validated.sort(key=lambda item: item[0], reverse=True)
    results: dict[str, object] = {}
    for _, name, config, series, gate, train_result, validation_result in validated[:10]:
        periods = {
            period: gated_result(series, timestamp(start), timestamp(end), **gate)
            for period, (start, end) in PERIODS.items()
        }
        stress_series = daily_series(config, snapshots, bars_by_symbol, funding_by_symbol, 15.0)
        stress = gated_result(
            stress_series,
            timestamp(PERIODS["full"][0]),
            timestamp(PERIODS["full"][1]),
            **gate,
            gate_source=series,
        )
        results[name] = {
            "config": config.__dict__,
            "gate": gate,
            "train_result": train_result,
            "validation_result": validation_result,
            "periods": periods,
            "stress_15bps": stress,
        }
    leverage_sensitivity: dict[str, object] = {}
    if validated:
        _, winner_name, winner_config, _, winner_gate, _, _ = validated[0]
        for gross in (0.5, 1.0, 1.5, 2.0, 2.5, 3.0):
            scaled = Config(
                winner_config.name,
                winner_config.formation_days,
                winner_config.names_per_leg,
                winner_config.direction,
                winner_config.liquidity,
                winner_config.stop,
                gross,
            )
            scaled_series = daily_series(scaled, snapshots, bars_by_symbol, funding_by_symbol, 5.0)
            leverage_sensitivity[f"gross_{gross:.1f}x"] = {
                period: gated_result(
                    scaled_series,
                    timestamp(start),
                    timestamp(end),
                    **winner_gate,
                )
                for period, (start, end) in PERIODS.items()
            }
    reference_config = Config("R1_short_N3_mid_S12", 1, 3, "short", "mid", 0.12, 2.0)
    reference_gate = {"window": 5, "min_mean": 0.0, "min_pf": 1.20}
    reference_series = daily_series(reference_config, snapshots, bars_by_symbol, funding_by_symbol, 5.0)
    reference_result = {
        period: gated_result(
            reference_series,
            timestamp(start),
            timestamp(end),
            **reference_gate,
        )
        for period, (start, end) in PERIODS.items()
    }
    robust_config = Config("R7_short_N5_mid_S8", 7, 5, "short", "mid", 0.08, 2.0)
    robust_gate = {"window": 20, "min_mean": 0.0, "min_pf": 1.20}
    robust_sensitivity: dict[str, object] = {}
    for gross in (0.5, 1.0, 1.5, 2.0, 2.5, 3.0):
        scaled = Config(
            robust_config.name,
            robust_config.formation_days,
            robust_config.names_per_leg,
            robust_config.direction,
            robust_config.liquidity,
            robust_config.stop,
            gross,
        )
        scaled_series = daily_series(scaled, snapshots, bars_by_symbol, funding_by_symbol, 5.0)
        robust_sensitivity[f"gross_{gross:.1f}x"] = {
            period: gated_result(
                scaled_series,
                timestamp(start),
                timestamp(end),
                **robust_gate,
            )
            for period, (start, end) in PERIODS.items()
        }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Binance Vision USD-M 15m klines",
        "method": "daily winner reversal with causal rolling completed-basket gate",
        "selection": "top 40 on train, top 10 on validation; July/Aug untouched by ranking",
        "results": results,
        "leverage_sensitivity_for_validation_winner": {
            "name": validated[0][1] if validated else None,
            "results": leverage_sensitivity,
        },
        "reference_original_spot_universe_winner": {
            "config": reference_config.__dict__,
            "gate": reference_gate,
            "periods": reference_result,
        },
        "robust_r7_candidate": {
            "config": robust_config.__dict__,
            "gate": robust_gate,
            "leverage_sensitivity": robust_sensitivity,
        },
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
