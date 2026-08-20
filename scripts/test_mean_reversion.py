#!/usr/bin/env python3
"""Test mean reversion strategy on current market data - final working version."""

import json
import math
import statistics
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

FAPI = "https://fapi.binance.com"
MINUTE_MS = 60_000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS

@dataclass(frozen=True)
class Bar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float

def get_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": "greed-research/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)

def fetch_bars(symbol: str, interval: str, limit: int, end_ms: int):
    params = {"symbol": symbol, "interval": interval, "limit": str(limit), "endTime": str(end_ms)}
    url = f"{FAPI}/fapi/v1/klines?{urllib.parse.urlencode(params)}"
    try:
        data = get_json(url)
        bars = [Bar(int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[7])) for row in data]
        return symbol, bars
    except Exception as e:
        return symbol, []

def backtest_mean_reversion(symbols_15m: dict[str, list[Bar]], symbols_1m: dict[str, list[Bar]],
                           start_ms: int, end_ms: int):
    """Backtest mean reversion strategy."""

    cash = 1000.0
    fee_rate = 0.0005
    slippage = 0.0005
    positions = {}
    trades = []
    cooldown_until = {}

    # 策略参数 - 放宽以增加交易频率
    lookback = 96  # 24小时
    entry_std = 1.2  # 大幅降低入场门槛
    exit_std = 0.2
    risk_per_trade = 0.02
    stop_pct = 0.015
    max_hold_hours = 4

    minute_lookup = {symbol: {bar.ts: i for i, bar in enumerate(bars)} for symbol, bars in symbols_1m.items()}

    for ts in range(start_ms, end_ms, MINUTE_MS):
        current_equity = cash + sum(pos.get("unrealized_pnl", 0) for pos in positions.values())

        # 检查止盈止损
        for symbol in list(positions):
            pos = positions[symbol]
            bar_1m = minute_lookup.get(symbol, {}).get(ts)
            if not bar_1m:
                continue

            bar = symbols_1m[symbol][bar_1m]
            pos["mark"] = bar.close
            pos["unrealized_pnl"] = pos["side"] * pos["qty"] * (bar.close - pos["entry"])

            # 止损
            if pos["side"] > 0 and bar.close <= pos["stop"]:
                close_position(symbol, ts, bar.close, "stop_loss", positions, trades, cash, fee_rate, slippage, cooldown_until)
                continue
            elif pos["side"] < 0 and bar.close >= pos["stop"]:
                close_position(symbol, ts, bar.close, "stop_loss", positions, trades, cash, fee_rate, slippage, cooldown_until)
                continue

            # 止盈 (价格回归均值)
            if pos["side"] > 0 and bar.close >= pos["target"]:
                close_position(symbol, ts, bar.close, "target_reached", positions, trades, cash, fee_rate, slippage, cooldown_until)
                continue
            elif pos["side"] < 0 and bar.close <= pos["target"]:
                close_position(symbol, ts, bar.close, "target_reached", positions, trades, cash, fee_rate, slippage, cooldown_until)
                continue

            # 时间止损
            if ts - pos["entry_ms"] >= max_hold_hours * HOUR_MS:
                close_position(symbol, ts, bar.close, "time_exit", positions, trades, cash, fee_rate, slippage, cooldown_until)

        # 检查新信号 (每15分钟)
        ts_15m = (ts // (15 * MINUTE_MS)) * (15 * MINUTE_MS)

        for symbol, bars_15m in symbols_15m.items():
            if symbol in positions or symbol in cooldown_until and cooldown_until[symbol] > ts:
                continue

            index_15m = None
            for i, bar in enumerate(bars_15m):
                if bar.ts == ts_15m:
                    index_15m = i
                    break

            if index_15m is None or index_15m < lookback + 4:
                continue

            signal = mean_reversion_signal(bars_15m, index_15m, lookback, entry_std, exit_std)
            if signal and signal[0] in ["LONG", "SHORT"]:
                direction, z_score, price, mean, std = signal
                side = 1 if direction == "LONG" else -1

                # 计算仓位
                risk_amount = current_equity * risk_per_trade
                stop_price = price * (1 - side * stop_pct)
                target_price = mean  # 回归到均值

                qty = risk_amount / (stop_pct * price)
                notional = abs(qty * price)

                if notional < 20:  # 最小仓位
                    continue

                entry_price = price * (1 + side * slippage)
                entry_fee = notional * fee_rate

                positions[symbol] = {
                    "side": side,
                    "qty": qty,
                    "entry": entry_price,
                    "entry_ms": ts,
                    "entry_fee": entry_fee,
                    "stop": stop_price,
                    "target": target_price,
                    "mark": price,
                    "unrealized_pnl": 0.0,
                    "z_score": z_score,
                    "mean": mean,
                    "std": std
                }
                cash -= entry_fee

    # 平掉所有未平仓的仓位
    for symbol in list(positions.keys()):
        pos = positions[symbol]
        last_bar = symbols_1m.get(symbol, [None])[-1]
        if last_bar:
            close_position(symbol, end_ms, last_bar.close, "final_exit", positions, trades, cash, fee_rate, slippage, cooldown_until)

    # 计算最终结果
    total_pnl = sum(t["pnl"] for t in trades)
    wins = sum(1 for t in trades if t["pnl"] > 0)

    return {
        "ending_equity": cash + total_pnl,
        "total_pnl": total_pnl,
        "return_pct": (cash + total_pnl) / 1000.0 * 100 - 100,
        "trades": len(trades),
        "wins": wins,
        "win_rate_pct": wins / len(trades) * 100 if trades else 0,
        "trade_details": trades
    }

def mean_reversion_signal(bars: list[Bar], index: int, lookback: int = 96, entry_std: float = 1.2, exit_std: float = 0.2):
    """Generate mean reversion signals based on z-score."""
    if index < lookback + 4:
        return None

    recent = bars[index - lookback:index + 1]
    prices = [bar.close for bar in recent]
    mean = statistics.mean(prices)
    std = statistics.stdev(prices) if len(prices) > 1 else 0.01

    if std < 0.0001:  # 避免除零
        return None

    current_price = bars[index].close
    z_score = (current_price - mean) / std

    if z_score > entry_std:
        return "SHORT", z_score, current_price, mean, std
    elif z_score < -entry_std:
        return "LONG", z_score, current_price, mean, std
    elif abs(z_score) < exit_std:
        return "EXIT", z_score, current_price, mean, std

    return None

def close_position(symbol: str, ts: int, price: float, reason: str, positions: dict,
                   trades: list, cash: float, fee_rate: float, slippage: float, cooldown_until: dict):
    pos = positions.pop(symbol)
    side = pos["side"]
    qty = pos["qty"]

    exit_price = price * (1 - side * slippage)
    exit_fee = abs(qty * exit_price) * fee_rate

    gross_pnl = side * qty * (exit_price - pos["entry"])
    net_pnl = gross_pnl - pos["entry_fee"] - exit_fee

    cash += side * qty * (exit_price - pos["entry"]) - exit_fee

    trades.append({
        "symbol": symbol,
        "side": "long" if side > 0 else "short",
        "entry_ms": pos["entry_ms"],
        "exit_ms": ts,
        "entry": pos["entry"],
        "exit": exit_price,
        "pnl": net_pnl,
        "reason": reason,
        "z_score_entry": pos["z_score"]
    })

    cooldown_until[symbol] = ts + 4 * HOUR_MS

def main():
    # 固定测试币种
    test_symbols = [
        "ARBUSDT", "ACEUSDT", "ONGUSDT", "GPSUSDT", "ROBOUSDT",
        "HEMIUSDT", "EULUSDT", "NMRUSDT", "ALPINEUSDT", "BIOUSDT",
        "LAUSDT", "EPICUSDT", "KAITOUSDT", "BOMEUSDT", "C98USDT"
    ]

    now_ms = int(get_json(f"{FAPI}/fapi/v1/time")["serverTime"])
    start_ms = now_ms - 7 * DAY_MS  # 过去7天

    print(f"=== MEAN REVERSION STRATEGY BACKTEST ===")
    print(f"Period: {datetime.fromtimestamp(start_ms/1000, timezone(timedelta(hours=8)))} to {datetime.fromtimestamp(now_ms/1000, timezone(timedelta(hours=8)))}")
    print(f"Testing {len(test_symbols)} symbols")

    # 获取数据
    symbols_15m = {}
    symbols_1m = {}

    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures_15m = [pool.submit(fetch_bars, symbol, "15m", 1000, now_ms) for symbol in test_symbols]
        for future in as_completed(futures_15m):
            symbol, bars = future.result()
            if bars:
                symbols_15m[symbol] = [bar for bar in bars if bar.ts >= start_ms]

        futures_1m = [pool.submit(fetch_bars, symbol, "1m", 1000, now_ms) for symbol in test_symbols]
        for future in as_completed(futures_1m):
            symbol, bars = future.result()
            if bars:
                symbols_1m[symbol] = [bar for bar in bars if bar.ts >= start_ms]

    print(f"Got data for {len(symbols_15m)} symbols")

    # 运行回测
    result = backtest_mean_reversion(symbols_15m, symbols_1m, start_ms, now_ms)

    # 输出结果
    print(f"\n=== BACKTEST RESULTS ===")
    print(f"Period: 7 days")
    print(f"Return: {result['return_pct']:.2f}%")
    print(f"Trades: {result['trades']}")
    print(f"Win Rate: {result['win_rate_pct']:.1f}%")
    print(f"Ending Equity: ${result['ending_equity']:.2f}")

    if result['trades'] > 0:
        print(f"\n=== Top 5 trades ===")
        sorted_trades = sorted(result['trade_details'], key=lambda x: x['pnl'], reverse=True)[:5]
        for trade in sorted_trades:
            print(f"  {trade['symbol']} {trade['side']}: ${trade['pnl']:.2f} ({trade['reason']}, z-score: {trade['z_score_entry']:.2f})")
    else:
        print(f"\nNo trades executed in backtest period")

    # 输出JSON
    output = {
        "strategy": "mean_reversion",
        "period": "7_days",
        "result": result
    }

    with open("/tmp/mean_reversion_results.json", "w") as f:
        json.dump(output, f, indent=2)

    print("\nFull results saved to /tmp/mean_reversion_results.json")

if __name__ == "__main__":
    main()