#!/usr/bin/env python3
"""Walk-forward research for confirmed tactical shorts.

Two independent ideas are tested against the current trend portfolio:

* A maker-miss is only an arm event.  A short is allowed after a missed long
  when price first extends, then prints a later bearish structure break with
  opposing taker flow.
* A relative-weakness short requires an established bearish EMA structure,
  underperformance versus BTC, a fresh range break, volume, and sell flow.

Parameters are selected on train and validation only.  Locked and recent
windows are opened after selection.  One-minute execution pays conservative
8 bps on both entry and exit, and ambiguous stop/target candles resolve stop
first.
"""

from __future__ import annotations

import itertools
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

import oi_taker_continuation_backtest as minute
import trend_execution_walkforward as current
import unified_compound_research as base


@dataclass(frozen=True)
class ReversalConfig:
    extension_bps: float
    retrace_bps: float
    min_opposing_flow: float
    wait_minutes: int
    stop_buffer_atr: float
    target_r: float
    hold_minutes: int


@dataclass(frozen=True)
class WeaknessConfig:
    max_relative_1h: float
    max_relative_4h: float
    max_flow: float
    min_volume_ratio: float
    breakdown_bars: int
    target_r: float
    hold_minutes: int


@dataclass(frozen=True)
class Signal:
    ts: int
    symbol: str
    side: int
    stop: float
    score: float
    lane: str


PER_SIDE_COST = 0.0008
START_EQUITY = 2000.0
PERIODS = current.PERIODS


def maker_missed(setup, values, indexes):
    index = indexes.get(setup.ts)
    if index is None:
        return None
    limit = setup.close - setup.side * setup.atr * 0.30
    bar = values[index]
    crossed = bar.low <= limit if setup.side > 0 else bar.high >= limit
    return None if crossed else index


def reversal_signals(setups, minutes, cfg: ReversalConfig):
    output = []
    indexes = {
        symbol: {bar.ts: index for index, bar in enumerate(values)}
        for symbol, values in minutes.items()
    }
    for setup in setups:
        # This lane addresses the observed long-only imbalance.  Symmetric
        # long reversals of missed shorts remain research diagnostics, not a
        # reason to dilute the short sample.
        if setup.side < 0:
            continue
        values = minutes.get(setup.symbol, [])
        start = maker_missed(setup, values, indexes.get(setup.symbol, {}))
        if start is None:
            continue
        extreme = setup.close
        extreme_index = start
        stop_at = min(len(values) - 1, start + cfg.wait_minutes)
        for index in range(start, stop_at):
            bar = values[index]
            if bar.high > extreme:
                extreme = bar.high
                extreme_index = index
            extension = (extreme / setup.close - 1.0) * 10_000.0
            retrace = (extreme / bar.close - 1.0) * 10_000.0
            flow = 2.0 * bar.taker_buy_quote / max(bar.quote_volume, 1.0) - 1.0
            prior = values[index - 1] if index > start else None
            range_ = max(bar.high - bar.low, 1e-12)
            bearish_location = (bar.close - bar.low) / range_ <= 0.40
            structure_break = prior is not None and bar.close < prior.low
            if (
                extension >= cfg.extension_bps
                and retrace >= cfg.retrace_bps
                and extreme_index < index
                and bar.close < bar.open
                and bearish_location
                and structure_break
                and flow <= -cfg.min_opposing_flow
            ):
                entry_index = index + 1
                if entry_index >= len(values):
                    break
                stop = extreme + cfg.stop_buffer_atr * setup.atr
                output.append(Signal(
                    values[entry_index].ts,
                    setup.symbol,
                    -1,
                    stop,
                    setup.score * extension * max(retrace, 1.0),
                    "confirmed_exhaustion_short",
                ))
                break
    return sorted(output, key=lambda row: (row.ts, -row.score, row.symbol))


def weakness_signals(bars, cfg: WeaknessConfig, start_ms: int, end_ms: int):
    btc = {bar.ts: bar for bar in bars["BTCUSDT"]}
    output = []
    for symbol, values in bars.items():
        if symbol == "BTCUSDT":
            continue
        quote_prefix = [0.0]
        for bar in values:
            quote_prefix.append(quote_prefix[-1] + bar.quote)
        for index in range(max(96, cfg.breakdown_bars + 1), len(values) - 1):
            bar = values[index]
            signal_ts = bar.ts + base.BAR_MS
            if not start_ms <= signal_ts < end_ms:
                continue
            btc_now = btc.get(bar.ts)
            btc_1h = btc.get(bar.ts - 4 * base.BAR_MS)
            btc_4h = btc.get(bar.ts - 16 * base.BAR_MS)
            if btc_now is None or btc_1h is None or btc_4h is None:
                continue
            relative_1h = bar.close / values[index - 4].close - btc_now.close / btc_1h.close
            relative_4h = bar.close / values[index - 16].close - btc_now.close / btc_4h.close
            hour_volume = quote_prefix[index + 1] - quote_prefix[index - 3]
            baseline = (quote_prefix[index] - quote_prefix[index - 96]) / 24.0
            volume_ratio = hour_volume / max(baseline, 1.0)
            flow = bar.imbalance
            prior_low = min(row.low for row in values[index - cfg.breakdown_bars:index])
            bearish_structure = bar.ema8 < bar.ema21 < bar.ema36
            fresh_break = bar.close < prior_low and values[index - 1].close >= min(
                row.low for row in values[index - cfg.breakdown_bars - 1:index - 1]
            )
            btc_4h_return = btc_now.close / btc_4h.close - 1.0
            # Relative weakness matters most when the broad tape is not already
            # in a waterfall.  In a crash the normal trend lane can short.
            if (
                relative_1h <= cfg.max_relative_1h
                and relative_4h <= cfg.max_relative_4h
                and flow <= cfg.max_flow
                and volume_ratio >= cfg.min_volume_ratio
                and bearish_structure
                and fresh_break
                and btc_4h_return >= -0.02
            ):
                recent_high = max(row.high for row in values[index - 3:index + 1])
                stop = recent_high + 0.10 * bar.atr
                score = -relative_1h * -relative_4h * volume_ratio * (1.0 - flow)
                output.append(Signal(
                    signal_ts, symbol, -1, stop, score, "relative_weakness_short"
                ))
    return sorted(output, key=lambda row: (row.ts, -row.score, row.symbol))


def replay(signal: Signal, values, indexes, target_r: float, hold_minutes: int):
    index = indexes.get(signal.ts)
    if index is None:
        return None
    raw_entry = values[index].open
    entry = raw_entry * (1.0 + signal.side * PER_SIDE_COST)
    stop_fraction = signal.side * (entry - signal.stop) / entry
    if not 0.003 <= stop_fraction <= 0.03:
        return None
    target = entry + signal.side * entry * stop_fraction * target_r
    exit_price = entry
    exit_ms = signal.ts
    reason = "time"
    for bar in values[index:index + hold_minutes]:
        stop_hit = bar.high >= signal.stop
        target_hit = bar.low <= target
        if stop_hit:
            exit_price = signal.stop * (1.0 + PER_SIDE_COST)
            exit_ms, reason = bar.ts, "stop"
            break
        if target_hit:
            exit_price = target * (1.0 + PER_SIDE_COST)
            exit_ms, reason = bar.ts, "target"
            break
        exit_price, exit_ms = bar.close, bar.ts
    else:
        return None
    if reason == "time":
        exit_price *= 1.0 + PER_SIDE_COST
    return {
        "entry_ts": signal.ts,
        "exit_ts": exit_ms,
        "symbol": signal.symbol,
        "side": signal.side,
        "score": signal.score,
        "lane": signal.lane,
        "entry": entry,
        "return": signal.side * (exit_price / entry - 1.0),
        "stop_fraction": stop_fraction,
        "reason": reason,
    }


def replay_signals(signals, minutes, minute_indexes, target_r, hold_minutes):
    output = []
    for signal in signals:
        trade = replay(
            signal,
            minutes.get(signal.symbol, []),
            minute_indexes.get(signal.symbol, {}),
            target_r,
            hold_minutes,
        )
        if trade is not None:
            output.append(trade)
    return output


def baseline_trades(setups, minutes, end_ms):
    outcomes = current.make_outcomes(
        setups,
        minutes,
        current.EntryConfig(0.30, 1),
        current.ExitConfig(0.0125, 2.0, 0.4, 0.5, 0.005),
        end_ms,
    )
    return [{
        "entry_ts": row.entry_ts,
        "exit_ts": row.exit_ts,
        "symbol": row.symbol,
        "side": row.side,
        "score": row.score,
        "lane": "trend_continuation",
        "entry": row.entry,
        "return": row.return_on_notional,
        "stop_fraction": 0.0125,
        "reason": row.reason,
    } for row in outcomes]


def portfolio(trades, start_ms, end_ms):
    rows = sorted(
        (row for row in trades if start_ms <= row["entry_ts"] < end_ms),
        key=lambda row: (row["entry_ts"], row["lane"] != "trend_continuation", -row["score"]),
    )
    equity = peak = START_EQUITY
    max_dd = 0.0
    active = []
    completed = []
    last_exit = defaultdict(lambda: -10**18)

    def settle(until):
        nonlocal equity, peak, max_dd, active
        keep = []
        for trade, notional in sorted(active, key=lambda item: item[0]["exit_ts"]):
            if trade["exit_ts"] > until:
                keep.append((trade, notional))
                continue
            pnl = notional * trade["return"]
            equity += pnl
            completed.append({**trade, "notional": notional, "pnl": pnl})
            last_exit[trade["symbol"]] = trade["exit_ts"]
            peak = max(peak, equity)
            max_dd = max(max_dd, 1.0 - equity / peak)
        active = keep

    for trade in rows:
        settle(trade["entry_ts"])
        if (
            len(active) >= 3
            or any(row["symbol"] == trade["symbol"] for row, _ in active)
            or trade["entry_ts"] - last_exit[trade["symbol"]] < 60 * 60_000
        ):
            continue
        gross = sum(notional for _, notional in active)
        risk_fraction = 0.01 if trade["lane"] == "trend_continuation" else 0.0015
        notional_cap = equity * (1.5 if trade["lane"] == "trend_continuation" else 1.0)
        notional = min(
            equity * risk_fraction / trade["stop_fraction"],
            notional_cap,
            equity * 4.0 - gross,
        )
        if notional >= 25.0:
            active.append((trade, notional))
    settle(end_ms)
    for trade, notional in active:
        pnl = notional * trade["return"]
        equity += pnl
        completed.append({**trade, "notional": notional, "pnl": pnl})
    gains = sum(max(0.0, row["pnl"]) for row in completed)
    losses = -sum(min(0.0, row["pnl"]) for row in completed)
    days = (end_ms - start_ms) / base.DAY_MS
    by_lane = {}
    for lane in sorted({row["lane"] for row in completed}):
        lane_rows = [row for row in completed if row["lane"] == lane]
        gp = sum(max(0.0, row["pnl"]) for row in lane_rows)
        gl = -sum(min(0.0, row["pnl"]) for row in lane_rows)
        by_lane[lane] = {
            "trades": len(lane_rows),
            "pnl_usd": sum(row["pnl"] for row in lane_rows),
            "pf": gp / gl if gl else None,
        }
    return {
        "start": START_EQUITY,
        "end": equity,
        "pnl_usd": equity - START_EQUITY,
        "return_pct": equity / START_EQUITY - 1.0,
        "max_dd_pct": max_dd,
        "trades": len(completed),
        "trades_per_day": len(completed) / days,
        "win_rate": sum(row["pnl"] > 0 for row in completed) / len(completed) if completed else None,
        "pf": gains / losses if losses else None,
        "short_trades": sum(row["side"] < 0 for row in completed),
        "by_lane": by_lane,
    }, completed


def score(train, validation):
    return min(train["return_pct"], validation["return_pct"]) \
        - 0.35 * max(train["max_dd_pct"], validation["max_dd_pct"])


def reversal_configs():
    detection = itertools.product(
        (20.0, 40.0, 80.0),
        (10.0, 20.0, 40.0),
        (0.0, 0.10, 0.20),
        (10, 20, 30),
        (0.10, 0.25),
    )
    for values in detection:
        for target, hold in itertools.product((0.5, 0.75, 1.0), (5, 10, 20)):
            yield ReversalConfig(*values, target, hold)


def weakness_configs():
    detection = itertools.product(
        (-0.005, -0.010, -0.015),
        (-0.010, -0.020, -0.030),
        (-0.05, -0.10, -0.20),
        (0.8, 1.0, 1.2),
        (4, 8),
    )
    for values in detection:
        # This is a tactical breakdown scalp, not a second trend position.
        # Keeping the horizon at 30 minutes or less is part of the strategy
        # definition and prevents slow rebounds from turning it into a fade.
        for target, hold in itertools.product((0.75, 1.0, 1.5), (15, 30)):
            yield WeaknessConfig(*values, target, hold)


def choose(rows, lane):
    eligible = []
    for row in rows:
        train_lane = row["train"]["by_lane"].get(lane, {})
        validation_lane = row["validation"]["by_lane"].get(lane, {})
        if (
            train_lane.get("trades", 0) >= 10
            and validation_lane.get("trades", 0) >= 3
            and train_lane.get("pnl_usd", 0.0) > 0.0
            and validation_lane.get("pnl_usd", 0.0) > 0.0
            and (train_lane.get("pf") or 0.0) > 1.05
            and (validation_lane.get("pf") or 0.0) > 1.05
        ):
            eligible.append(row)
    return max(eligible or rows, key=lambda row: row["score"]), len(eligible)


def main():
    root = Path("data/alpha-backtest/cache")
    established = [
        symbol for symbol in base.SYMBOLS if (root / "klines" / symbol).exists()
    ]
    recent = sorted(
        path.name for path in (root / "klines").iterdir()
        if any(path.glob("*-2026-08-26.zip"))
    )
    symbols = sorted(set(established) | set(recent) | {"BTCUSDT"})
    bars = {symbol: base.load_bars(root, symbol) for symbol in symbols}
    minutes = {symbol: minute.load_minutes(root, symbol) for symbol in symbols}
    minute_indexes = {
        symbol: {bar.ts: index for index, bar in enumerate(values)}
        for symbol, values in minutes.items()
    }
    setups = {}
    baselines = {}
    for name, period in PERIODS.items():
        period_symbols = recent if name == "recent" else established
        period_bars = {symbol: bars[symbol] for symbol in period_symbols}
        setups[name] = current.make_setups(period_bars, period_symbols, *period)
        baselines[name] = baseline_trades(setups[name], minutes, period[1])

    reversal_rows = []
    reversal_cache = {}
    for cfg in reversal_configs():
        key = (
            cfg.extension_bps, cfg.retrace_bps, cfg.min_opposing_flow,
            cfg.wait_minutes, cfg.stop_buffer_atr,
        )
        if key not in reversal_cache:
            reversal_cache[key] = {
                name: reversal_signals(setups[name], minutes, cfg) for name in PERIODS
            }
        stats = {}
        for name, period in PERIODS.items():
            extra = replay_signals(
                reversal_cache[key][name], minutes, minute_indexes,
                cfg.target_r, cfg.hold_minutes
            )
            stats[name], _ = portfolio(baselines[name] + extra, *period)
        reversal_rows.append({
            "score": score(stats["train"], stats["validation"]),
            "config": asdict(cfg), **stats,
        })
    selected_reversal, reversal_eligible = choose(
        reversal_rows, "confirmed_exhaustion_short"
    )

    weakness_rows = []
    weakness_cache = {}
    for cfg in weakness_configs():
        key = (
            cfg.max_relative_1h, cfg.max_relative_4h, cfg.max_flow,
            cfg.min_volume_ratio, cfg.breakdown_bars,
        )
        if key not in weakness_cache:
            weakness_cache[key] = {
                name: weakness_signals(
                    {symbol: bars[symbol] for symbol in (recent if name == "recent" else established)},
                    cfg, *period,
                ) for name, period in PERIODS.items()
            }
        stats = {}
        for name, period in PERIODS.items():
            extra = replay_signals(
                weakness_cache[key][name], minutes, minute_indexes,
                cfg.target_r, cfg.hold_minutes
            )
            stats[name], _ = portfolio(baselines[name] + extra, *period)
        weakness_rows.append({
            "score": score(stats["train"], stats["validation"]),
            "config": asdict(cfg), **stats,
        })
    selected_weakness, weakness_eligible = choose(
        weakness_rows, "relative_weakness_short"
    )

    combined = {}
    combined_trades = {}
    reversal_cfg = ReversalConfig(**selected_reversal["config"])
    weakness_cfg = WeaknessConfig(**selected_weakness["config"])
    reversal_key = (
        reversal_cfg.extension_bps, reversal_cfg.retrace_bps,
        reversal_cfg.min_opposing_flow, reversal_cfg.wait_minutes,
        reversal_cfg.stop_buffer_atr,
    )
    weakness_key = (
        weakness_cfg.max_relative_1h, weakness_cfg.max_relative_4h,
        weakness_cfg.max_flow, weakness_cfg.min_volume_ratio,
        weakness_cfg.breakdown_bars,
    )
    for name, period in PERIODS.items():
        reversal_extra = replay_signals(
            reversal_cache[reversal_key][name], minutes, minute_indexes,
            reversal_cfg.target_r, reversal_cfg.hold_minutes,
        ) if reversal_eligible > 0 else []
        weakness_extra = replay_signals(
            weakness_cache[weakness_key][name], minutes, minute_indexes,
            weakness_cfg.target_r, weakness_cfg.hold_minutes,
        ) if weakness_eligible > 0 else []
        combined[name], combined_trades[name] = portfolio(
            baselines[name] + reversal_extra + weakness_extra, *period
        )
    baseline_stats = {
        name: portfolio(baselines[name], *period)[0]
        for name, period in PERIODS.items()
    }
    locked_weakness = selected_weakness["locked_test"]["by_lane"].get(
        "relative_weakness_short", {}
    )
    recent_weakness = selected_weakness["recent"]["by_lane"].get(
        "relative_weakness_short", {}
    )
    deploy_reversal = reversal_eligible > 0
    deploy_weakness = (
        weakness_eligible > 0
        and locked_weakness.get("pnl_usd", 0.0) > 0.0
        and (locked_weakness.get("pf") or 0.0) > 1.0
        and recent_weakness.get("pnl_usd", 0.0) > 0.0
    )
    report = {
        "strategy": "confirmed_short_balance_walkforward_v1",
        "selection": "train and validation only; locked and recent untouched",
        "cost_model": {
            "entry_bps": 8, "exit_bps": 8,
            "ambiguous_bar": "stop_first",
            "tactical_risk_fraction": 0.0015,
            "tactical_notional_cap": 1.0,
        },
        "coverage": {
            "established_symbols": len(established),
            "recent_symbols": len(recent),
        },
        "baseline": baseline_stats,
        "reversal_eligible_configs": reversal_eligible,
        "weakness_eligible_configs": weakness_eligible,
        "selected_reversal": selected_reversal,
        "selected_weakness": selected_weakness,
        "combined": combined,
        "decision": {
            "deploy_confirmed_exhaustion_short": deploy_reversal,
            "deploy_relative_weakness_short": deploy_weakness,
        },
        "combined_trades": combined_trades,
        "top_reversal": sorted(reversal_rows, key=lambda row: row["score"], reverse=True)[:20],
        "top_weakness": sorted(weakness_rows, key=lambda row: row["score"], reverse=True)[:20],
    }
    output = Path("data/alpha-backtest/confirmed-short-balance-walkforward.json")
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({
        "decision": report["decision"],
        "eligible": {
            "reversal": reversal_eligible,
            "weakness": weakness_eligible,
        },
        "reversal_config": selected_reversal["config"],
        "weakness_config": selected_weakness["config"],
        "baseline": baseline_stats,
        "reversal": {name: selected_reversal[name] for name in PERIODS},
        "weakness": {name: selected_weakness[name] for name in PERIODS},
        "combined": combined,
    }, indent=2))


if __name__ == "__main__":
    main()
