#!/usr/bin/env python3
"""Causal online gating for non-stationary altcoin alpha candidates.

Each expert is enabled only when its most recently *completed* hypothetical
signals retain positive net expectancy. Training selects gate parameters,
validation selects one candidate per expert, and July/August remain untouched.
"""

from __future__ import annotations

import argparse
import bisect
import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from exp_altcoin_alpha_search import (
    EXIT_PROFILES,
    PERIODS,
    ExitProfile,
    build_signals,
    simulate,
    timestamp,
)
from exp_altcoin_five_optimizations import load_symbol_bars, spot_history_symbols
from exp_altcoin_oi_launch import BAR_MS, MAJORS, Bar, Signal


EXPERTS = {
    "pump_continue": ("pump_continuation_3", "time_5_4h"),
    "blowoff_fade": ("blowoff_fade_1", "time_8_4h"),
    "dump_rebound": ("dump_rebound_1", "time_8_2h"),
}


def dedupe(signals: list[Signal], cooldown_bars: int) -> list[Signal]:
    last: dict[str, int] = {}
    output: list[Signal] = []
    for signal in sorted(signals, key=lambda item: (item.execute_ts, item.symbol)):
        if signal.execute_ts - last.get(signal.symbol, -10**18) < cooldown_bars * BAR_MS:
            continue
        output.append(signal)
        last[signal.symbol] = signal.execute_ts
    return output


def signal_outcome(
    signal: Signal,
    bars: list[Bar],
    exit_profile: ExitProfile,
    slippage_bps: float = 5.0,
) -> tuple[int, float] | None:
    times = [bar.ts for bar in bars]
    index = bisect.bisect_left(times, signal.execute_ts)
    if index >= len(bars) or bars[index].ts != signal.execute_ts:
        return None
    fee = 5.0 / 10_000
    slip = slippage_bps / 10_000
    entry = bars[index].open * (1 + signal.side * slip)
    stop = entry * (1 - signal.side * exit_profile.stop)
    end = min(index + exit_profile.max_bars, len(bars) - 1)
    raw_exit = bars[end].open
    exit_ts = bars[end].ts
    for cursor in range(index, end):
        bar = bars[cursor]
        gap = bar.open <= stop if signal.side > 0 else bar.open >= stop
        hit = bar.low <= stop if signal.side > 0 else bar.high >= stop
        if gap or hit:
            raw_exit = bar.open if gap else stop
            exit_ts = bar.ts
            break
    exit_price = raw_exit * (1 - signal.side * slip)
    net = signal.side * (exit_price / entry - 1.0) - fee - fee * exit_price / entry
    return exit_ts, net


def online_gate(
    signals: list[Signal],
    bars_by_symbol: dict[str, list[Bar]],
    exit_profile: ExitProfile,
    window: int,
    min_mean: float,
    min_pf: float,
) -> list[Signal]:
    outcomes: list[tuple[int, float]] = []
    for signal in signals:
        result = signal_outcome(signal, bars_by_symbol[signal.symbol], exit_profile)
        if result is not None:
            outcomes.append(result)
    ordered_signals = sorted(signals, key=lambda item: (item.execute_ts, item.symbol))
    matured = sorted(outcomes)
    history: deque[float] = deque(maxlen=window)
    output: list[Signal] = []
    cursor = 0
    for signal in ordered_signals:
        while cursor < len(matured) and matured[cursor][0] < signal.execute_ts:
            history.append(matured[cursor][1])
            cursor += 1
        if len(history) < window:
            continue
        gross_profit = sum(max(value, 0.0) for value in history)
        gross_loss = sum(max(-value, 0.0) for value in history)
        mean = sum(history) / len(history)
        pf = gross_profit / gross_loss if gross_loss > 0 else 99.0
        if mean >= min_mean and pf >= min_pf:
            enriched = dict(signal.features)
            enriched.update({"online_window": window, "online_mean": mean, "online_pf": pf})
            output.append(Signal(signal.symbol, signal.execute_ts, signal.side, signal.score, signal.kind, enriched))
    return output


def simple_event_summary(
    signals: list[Signal],
    bars_by_symbol: dict[str, list[Bar]],
    exit_profile: ExitProfile,
    start: int,
    end: int,
) -> dict[str, float | int | None]:
    values: list[float] = []
    for signal in signals:
        if not start <= signal.execute_ts < end:
            continue
        result = signal_outcome(signal, bars_by_symbol[signal.symbol], exit_profile)
        if result is not None:
            values.append(result[1])
    wins = [value for value in values if value > 0]
    losses = [-value for value in values if value < 0]
    return {
        "events": len(values),
        "mean_pct": sum(values) / len(values) * 100 if values else 0.0,
        "win_rate_pct": len(wins) / len(values) * 100 if values else 0.0,
        "profit_factor": sum(wins) / sum(losses) if losses and sum(losses) > 0 else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default="/tmp/greed-altcoin-oi-full")
    parser.add_argument("--output", default="/tmp/greed-altcoin-online-alpha.json")
    args = parser.parse_args()
    cache = Path(args.cache)
    allowed = spot_history_symbols()
    symbols = sorted({path.parent.name for path in (cache / "klines").glob("*/*.zip")})
    symbols = [symbol for symbol in symbols if symbol in allowed and symbol not in MAJORS]
    bars_by_symbol: dict[str, list[Bar]] = {}
    raw: dict[str, list[Signal]] = {expert: [] for expert in EXPERTS}
    for completed, symbol in enumerate(symbols, 1):
        bars = load_symbol_bars(cache, symbol)
        if len(bars) < 15 * 96:
            continue
        bars_by_symbol[symbol] = bars
        made = build_signals(symbol, bars)
        for expert, (profile_name, _) in EXPERTS.items():
            raw[expert].extend(made.get(profile_name, []))
        if completed % 50 == 0:
            print(f"features {completed}/{len(symbols)}", flush=True)

    exit_by_name = {profile.name: profile for profile in EXIT_PROFILES}
    gates: dict[str, dict[str, object]] = {}
    train = tuple(map(timestamp, PERIODS["train"]))
    validation = tuple(map(timestamp, PERIODS["validation"]))
    for expert, (_, exit_name) in EXPERTS.items():
        exit_profile = exit_by_name[exit_name]
        source = dedupe(raw[expert], exit_profile.max_bars)
        candidates: list[tuple[float, str, list[Signal], dict[str, object], dict[str, object]]] = []
        for window in (20, 40, 80):
            for min_mean in (0.0, 0.001, 0.002):
                for min_pf in (1.0, 1.10, 1.20):
                    selected = online_gate(source, bars_by_symbol, exit_profile, window, min_mean, min_pf)
                    train_event = simple_event_summary(selected, bars_by_symbol, exit_profile, *train)
                    if int(train_event["events"]) < 30:
                        continue
                    pf = float(train_event["profit_factor"] or 0.0)
                    score = float(train_event["mean_pct"]) + 0.5 * (pf - 1.0)
                    name = f"{expert}_W{window}_M{min_mean:.3f}_PF{min_pf:.2f}"
                    candidates.append((score, name, selected, {"window": window, "min_mean": min_mean, "min_pf": min_pf}, train_event))
        candidates.sort(key=lambda item: item[0], reverse=True)
        finalists = candidates[:5]
        evaluated: list[tuple[float, str, list[Signal], dict[str, object]]] = []
        for _, name, selected, config, train_event in finalists:
            train_portfolio = simulate(bars_by_symbol, selected, exit_profile, *train, 5.0)
            validation_portfolio = simulate(bars_by_symbol, selected, exit_profile, *validation, 5.0)
            validation_event = simple_event_summary(selected, bars_by_symbol, exit_profile, *validation)
            score = float(validation_portfolio["return_pct"]) - 0.5 * float(validation_portfolio["max_drawdown_pct"])
            gates[name] = {
                "expert": expert,
                "config": config,
                "signals": len(selected),
                "train_event": train_event,
                "validation_event": validation_event,
                "train_portfolio": train_portfolio,
                "validation_portfolio": validation_portfolio,
            }
            evaluated.append((score, name, selected, config))
        if not evaluated:
            continue
        _, winner, selected, config = max(evaluated, key=lambda item: item[0])
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
        gates[winner]["winner_for_expert"] = True
        gates[winner]["periods"] = periods
        gates[winner]["stress_15bps"] = simulate(
            bars_by_symbol,
            selected,
            exit_profile,
            timestamp(PERIODS["full"][0]),
            timestamp(PERIODS["full"][1]),
            15.0,
        )
        print("winner", winner, periods["validation"]["return_pct"], periods["july_test"]["return_pct"], periods["aug_holdout"]["return_pct"], flush=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Binance Vision USD-M 15m klines",
        "method": "rolling completed-signal expectancy gate; train shortlist, validation selection, July/Aug untouched",
        "results": gates,
    }
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report={args.output}")


if __name__ == "__main__":
    main()
