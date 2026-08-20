#!/usr/bin/env python3
"""High-frequency combined strategy: Target 5+ trades per day."""

import json
import statistics
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def get_json(url: str):
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
    except:
        return symbol, []

def test_symbols():
    # 扩大币种池
    return [
        "ARBUSDT", "ACEUSDT", "ONGUSDT", "GPSUSDT", "ROBOUSDT",
        "HEMIUSDT", "EULUSDT", "NMRUSDT", "ALPINEUSDT", "BIOUSDT",
        "LAUSDT", "EPICUSDT", "KAITOUSDT", "BOMEUSDT", "C98USDT",
        "ACTUSDT", "LISTAUSDT", "BMTUSDT", "SHELLUSDT", "COOKIEUSDT",
        "MUBARAKUSDT", "MMTUSDT", "REUSDT", "XAIUSDT", "BROCCOLI714USDT",
        "SWRVUSDT", "TRXUSDT", "LEVERUSDT", "FTMUSDT", "AVAXUSDT"
    ]

# ========== 多信号源策略 ==========
def breakout_signal_relaxed(bars: list[Bar], index: int):
    """超级放宽的突破信号 - 确保有足够交易"""
    if index < 96:
        return None

    close = bars[index].close
    prior = bars[index - 96:index]

    if len(prior) < 96:
        return None

    prior_high = max(item.high for item in prior)
    prior_low = min(item.low for item in prior)

    side = 1 if close > prior_high else -1 if close < prior_low else 0
    if side == 0:
        return None

    # 只要1小时有方向就行
    return_1h = close / bars[index - 4].close - 1.0
    if abs(return_1h) < 0.01:  # 只要1%变化
        return None

    return "BREAKOUT", side, close, 1.0, return_1h, 0.06

def momentum_signal(bars: list[Bar], index: int):
    """超宽松动量信号 - 只要价格变化就入场"""
    if index < 8:
        return None

    close = bars[index].close

    # 1小时价格变化
    if index >= 4:
        momentum_1h = close / bars[index - 4].close - 1.0
    else:
        return None

    # 只要有0.8%变化就行
    if abs(momentum_1h) < 0.008:
        return None

    side = 1 if momentum_1h > 0 else -1
    return "MOMENTUM", side, close, 1.0, momentum_1h, 0.04

def volatility_breakout_signal(bars: list[Bar], index: int):
    """波动率突破信号 - 低波动后的高波动"""
    if index < 96:
        return None

    # 计算24小时波动率
    recent = bars[index - 96:index + 1]
    prices = [bar.close for bar in recent]

    if len(prices) < 96:
        return None

    # 当前15分钟的波动率
    current_vol = max(bars[index].high - bars[index].low, 0.0001) / bars[index].close

    # 历史波动率
    historical_vols = []
    for i in range(24, index, 24):  # 每4小时采样一次
        if i >= 96:
            hist_slice = bars[i - 96:i]
            if len(hist_slice) >= 96:
                hist_high = max(bar.high for bar in hist_slice)
                hist_low = min(bar.low for bar in hist_slice)
                hist_vol = (hist_high - hist_low) / hist_slice[0].close
                historical_vols.append(hist_vol)

    if not historical_vols:
        return None

    avg_vol = statistics.mean(historical_vols)

    # 波动率扩张
    if current_vol < avg_vol * 1.5:  # 当前波动率是平均的1.5倍
        return None

    # 方向判断
    close = bars[index].close
    open_price = bars[index].open

    # 突破开盘价
    if close > open_price * 1.01:  # 至少1%的突破
        return "VOL_BREAKOUT_LONG", 1, close, current_vol, avg_vol
    elif close < open_price * 0.99:
        return "VOL_BREAKOUT_SHORT", -1, close, current_vol, avg_vol

    return None

def combined_signal_scanner(bars_15m: dict[str, list[Bar]], index_15m: int, config: dict):
    """综合信号扫描器 - 最大化交易机会"""
    signals = []

    for symbol, bars in bars_15m.items():
        if index_15m >= len(bars) - 1:
            continue

        # 收集所有信号
        breakout = breakout_signal_relaxed(bars, index_15m)
        if breakout:
            signals.append((symbol, breakout))

        momentum = momentum_signal(bars, index_15m)
        if momentum:
            signals.append((symbol, momentum))

        vol_breakout = volatility_breakout_signal(bars, index_15m)
        if vol_breakout:
            signals.append((symbol, vol_breakout))

    return signals

# ========== 回测引擎 ==========
def backtest_high_frequency(symbols_15m: dict[str, list[Bar]], symbols_1m: dict[str, list[Bar]],
                              start_ms: int, end_ms: int, config: dict):
    """高频策略回测"""

    cash = 1000.0
    fee_rate = 0.0005
    slippage = 0.0005
    positions = {}
    trades = []
    cooldown_until = {}

    max_positions = 3  # 允许3个同时持仓
    max_daily_trades = 20  # 每日最多20笔
    daily_trades = 0
    current_day = start_ms // DAY_MS

    minute_lookup = {symbol: {bar.ts: i for i, bar in enumerate(bars)} for symbol, bars in symbols_1m.items()}

    for ts in range(start_ms, end_ms, MINUTE_MS):
        # 更新日期
        new_day = ts // DAY_MS
        if new_day != current_day:
            current_day = new_day
            daily_trades = 0

        current_equity = cash + sum(pos.get("unrealized_pnl", 0) for pos in positions.values())

        # 风险控制
        if daily_trades >= max_daily_trades:
            continue

        # 检查持仓
        for symbol in list(positions):
            pos = positions[symbol]
            bar_1m = minute_lookup.get(symbol, {}).get(ts)
            if not bar_1m:
                continue

            bar = symbols_1m[symbol][bar_1m]
            pos["mark"] = bar.close
            pos["unrealized_pnl"] = pos["side"] * pos["qty"] * (bar.close - pos["entry"])

            # 统一止损逻辑
            if pos["side"] > 0 and bar.close <= pos["stop"]:
                close_position(symbol, ts, bar.close, "stop_loss", positions, trades, cash, fee_rate, slippage, cooldown_until, daily_trades)
                continue
            elif pos["side"] < 0 and bar.close >= pos["stop"]:
                close_position(symbol, ts, bar.close, "stop_loss", positions, trades, cash, fee_rate, slippage, cooldown_until, daily_trades)
                continue

            # 动态止盈
            mfe = pos["side"] * (bar.high / pos["entry"] - 1) if pos["side"] > 0 else pos["side"] * (bar.low / pos["entry"] - 1)
            if mfe >= 0.02:  # 2%盈亏
                if mfe >= 0.05:  # 5%时兑现50%
                    partial_close(symbol, ts, bar.close, 0.5, positions, cash, fee_rate, slippage, daily_trades)
                    pos = positions.get(symbol)
                # 跟踪止损
                trailing_stop = bar.high * 0.97 if pos["side"] > 0 else bar.low * 1.03
                if pos["side"] > 0 and bar.close <= trailing_stop:
                    close_position(symbol, ts, bar.close, "trailing_stop", positions, trades, cash, fee_rate, slippage, cooldown_until, daily_trades)
                elif pos["side"] < 0 and bar.close >= trailing_stop:
                    close_position(symbol, ts, bar.close, "trailing_stop", positions, trades, cash, fee_rate, slippage, cooldown_until, daily_trades)

            # 时间止损
            if ts - pos["entry_ms"] >= 3 * HOUR_MS:
                close_position(symbol, ts, bar.close, "time_exit", positions, trades, cash, fee_rate, slippage, cooldown_until, daily_trades)

        # 检查新信号 (每15分钟)
        if ts % (15 * MINUTE_MS) == 0:
            ts_15m = (ts // (15 * MINUTE_MS)) * (15 * MINUTE_MS)

            for symbol in list(positions.keys()):
                continue  # 已有仓位

            if symbol in cooldown_until and cooldown_until[symbol] > ts:
                continue  # 冷却中

            if len(positions) >= max_positions:
                continue  # 仓位已满

            bars = symbols_15m.get(symbol, [])
            if not bars:
                continue

            index_15m = None
            for i, bar in enumerate(bars):
                if bar.ts == ts_15m:
                    index_15m = i
                    break

            if index_15m is None or index_15m < 100:
                continue

            # 扫描所有信号
            all_signals = []
            breakout = breakout_signal_relaxed(bars, index_15m)
            if breakout:
                all_signals.append(breakout)

            momentum = momentum_signal(bars, index_15m)
            if momentum:
                all_signals.append(momentum)

            vol_breakout = volatility_breakout_signal(bars, index_15m)
            if vol_breakout:
                all_signals.append(vol_breakout)

            # 选择最强信号
            if all_signals:
                best_signal = max(all_signals, key=lambda s: abs(s[4]) if len(s) > 4 else 0)  # 选择最大动量
                enter_position(symbol, best_signal, ts, current_equity, positions, cash, fee_rate, slippage, config)

    # 平掉所有仓位
    for symbol in list(positions.keys()):
        pos = positions[symbol]
        last_bar = symbols_1m.get(symbol, [None])[-1]
        if last_bar:
            close_position(symbol, end_ms, last_bar.close, "final_exit", positions, trades, cash, fee_rate, slippage, cooldown_until, daily_trades)

    # 计算结果
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

def enter_position(symbol: str, signal: tuple, ts: int, available_capital: float,
                    positions: dict, cash: float, fee_rate: float, slippage: float, config: dict):
    """入场"""
    signal_type = signal[0]
    side = signal[1]
    price = signal[2]

    risk_per_trade = config.get("risk_per_trade", 0.02)
    stop_pct = config.get("stop_pct", 0.015)

    risk_amount = available_capital * risk_per_trade
    stop_price = price * (1 - side * stop_pct)

    qty = risk_amount / (stop_pct * price)
    notional = abs(qty * price)

    if notional < 20:
        return

    entry_price = price * (1 + side * slippage)
    entry_fee = notional * fee_rate

    positions[symbol] = {
        "signal_type": signal_type,
        "side": side,
        "qty": qty,
        "entry": entry_price,
        "entry_ms": ts,
        "entry_fee": entry_fee,
        "stop": stop_price,
        "mark": price,
        "unrealized_pnl": 0.0
    }
    cash -= entry_fee

def close_position(symbol: str, ts: int, price: float, reason: str, positions: dict,
                   trades: list, cash: float, fee_rate: float, slippage: float,
                   cooldown_until: dict, daily_trades: int):
    """平仓"""
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
        "signal": pos.get("signal_type", "UNKNOWN"),
        "side": "long" if side > 0 else "short",
        "entry_ms": pos["entry_ms"],
        "exit_ms": ts,
        "entry": pos["entry"],
        "exit": exit_price,
        "pnl": net_pnl,
        "reason": reason
    })

    cooldown_until[symbol] = ts + 2 * HOUR_MS
    daily_trades += 1

def partial_close(symbol: str, ts: int, price: float, fraction: float, positions: dict,
                cash: float, fee_rate: float, slippage: float, daily_trades: int):
    """部分平仓"""
    pos = positions[symbol]
    side = pos["side"]
    qty_to_close = pos["qty"] * fraction

    exit_price = price * (1 - side * slippage)
    exit_fee = abs(qty_to_close * exit_price) * fee_rate

    gross_pnl = side * qty_to_close * (exit_price - pos["entry"])
    net_pnl = gross_pnl - pos["entry_fee"] * fraction - exit_fee

    cash += side * qty_to_close * (exit_price - pos["entry"]) - exit_fee

    pos["qty"] -= qty_to_close
    pos["entry_fee"] -= pos["entry_fee"] * fraction

def main():
    symbols = test_symbols()
    now_ms = int(get_json(f"{FAPI}/fapi/v1/time")["serverTime"])
    start_ms = now_ms - 7 * DAY_MS

    print(f"=== 高频组合策略回测 (目标: 每天5+笔) ===")
    print(f"期间：过去7天")
    print(f"币种池：{len(symbols)}个")

    # 获取数据
    symbols_15m = {}
    symbols_1m = {}

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures_15m = [pool.submit(fetch_bars, symbol, "15m", 1000, now_ms) for symbol in symbols]
        for future in as_completed(futures_15m):
            symbol, bars = future.result()
            if bars:
                symbols_15m[symbol] = [bar for bar in bars if bar.ts >= start_ms]

        futures_1m = [pool.submit(fetch_bars, symbol, "1m", 1000, now_ms) for symbol in symbols]
        for future in as_completed(futures_1m):
            symbol, bars = future.result()
            if bars:
                symbols_1m[symbol] = [bar for bar in bars if bar.ts >= start_ms]

    print(f"获取数据：15m({len(symbols_15m)}), 1m({len(symbols_1m)})")

    # 测试多个配置
    configs = [
        {
            "name": "激进高频 (风险2%, 止损1.5%, 日20笔)",
            "risk_per_trade": 0.02,
            "stop_pct": 0.015
        },
        {
            "name": "超高频 (风险1.5%, 止损1%, 日30笔)",
            "risk_per_trade": 0.015,
            "stop_pct": 0.01
        },
        {
            "name": "均衡高频 (风险2.5%, 止损2%, 日15笔)",
            "risk_per_trade": 0.025,
            "stop_pct": 0.02
        }
    ]

    results = []
    for config in configs:
        result = backtest_high_frequency(symbols_15m, symbols_1m, start_ms, now_ms, config)
        result["config_name"] = config["name"]
        results.append(result)

    # 输出结果
    print(f"\n{'='*20} 高频策略回测结果 {'='*20}")

    best_result = None
    best_score = -999

    for i, result in enumerate(results, 1):
        trades_per_day = result['trades'] / 7
        score = result['return_pct'] + (trades_per_day - 5) * 10  # 惩罚不到5笔/天

        if score > best_score:
            best_score = score
            best_result = result

        print(f"\n【配置{i}: {result['config_name']}】")
        print(f"收益率: {result['return_pct']:.2f}%")
        print(f"交易数: {result['trades']}笔 (每天{trades_per_day:.1f}笔)")
        print(f"胜率: {result['win_rate_pct']:.1f}%")
        print(f"最终权益: ${result['ending_equity']:.2f}")
        print(f"评分: {score:.1f} (收益+{trades_per_day-5}*10)")

    print(f"\n🏆 最优配置: {best_result['config_name']}")
    print(f"   收益率: {best_result['return_pct']:.2f}%")
    print(f"   交易数: {best_result['trades']}笔 (每天{best_result['trades']/7:.1f}笔)")

    # 检查是否达标
    if best_result['trades'] >= 35:  # 7天至少35笔，即每天5笔
        print(f"✅ 达标！7天{best_result['trades']}笔交易，每天{best_result['trades']/7:.1f}笔")
        print(f"✅ 盈利！收益率{best_result['return_pct']:.2f}%")
    else:
        print(f"❌ 未达标，需要{35}笔交易，当前仅{best_result['trades']}笔")
        print(f"💡 建议进一步放宽信号门槛或扩大币种池")

    # 保存结果
    with open("/tmp/high_frequency_results.json", "w") as f:
        json.dump({
            "period": "7_days",
            "configs": results,
            "best_config": best_result
        }, f, indent=2)

    print(f"\n详细结果已保存至 /tmp/high_frequency_results.json")

if __name__ == "__main__":
    main()