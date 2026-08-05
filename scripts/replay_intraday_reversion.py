#!/usr/bin/env python3
"""用 OrderFlowExhaustion 的 10 秒评估流水重建分钟线，回放 V8 日内 MR 腿。

评估流水只保存每个 10 秒桶的收盘价，因此分钟内高低点是保守近似；正式结论仍应
以 data/lake 的逐笔成交回测为准。脚本计入双边各 5 bps 费用，并按“同根同时触发
先止损”的保守顺序撮合。
"""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import json
from pathlib import Path


def load_bars(path: Path) -> list[dict[str, float]]:
    rows: list[tuple[int, float, float, float]] = []
    with path.open() as fh:
        for line in fh:
            row = json.loads(line)
            note = row.get("note", {})
            if row.get("source") == "OrderFlowExhaustion" and "price" in note:
                rows.append(
                    (
                        int(row["ts_ms"]),
                        float(note["price"]),
                        float(note.get("volume_usd", 0.0)),
                        float(note.get("delta_usd", 0.0)),
                    )
                )
    bars = []
    for minute, group in itertools.groupby(rows, key=lambda x: x[0] // 60_000):
        bucket = list(group)
        prices = [x[1] for x in bucket]
        bars.append(
            {
                "ts": minute * 60_000,
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "volume": sum(x[2] for x in bucket),
                "delta": sum(x[3] for x in bucket),
            }
        )
    return bars


def signals(bars: list[dict[str, float]]) -> list[dict[str, float]]:
    result = []
    setup = None
    last_confirm = -(10**9)
    for i in range(30, len(bars) - 1):
        window = bars[i - 29 : i + 1]
        if window[-1]["ts"] - window[0]["ts"] > 30 * 60_000:
            setup = None
            continue
        change = window[-1]["close"] / window[0]["open"] - 1.0
        path = sum(abs(window[j]["close"] - window[j - 1]["close"]) for j in range(1, 30))
        efficiency = abs(window[-1]["close"] - window[0]["open"]) / path if path else 0.0
        volume = sum(x["volume"] for x in window)
        delta = sum(x["delta"] for x in window)
        delta_share = delta / volume if volume else 0.0
        extension = (
            1
            if change >= 0.006 and efficiency >= 0.45 and delta_share >= 0.08
            else -1
            if change <= -0.006 and efficiency >= 0.45 and delta_share <= -0.08
            else 0
        )
        if setup and i > setup["index"] + 5:
            setup = None
        if setup:
            setup["volume"] += bars[i]["volume"]
            setup["delta"] += bars[i]["delta"]
            confirm_share = setup["delta"] / setup["volume"] if setup["volume"] else 0.0
            price_reversed = (
                bars[i]["close"] < bars[i - 1]["low"]
                if setup["extension"] > 0
                else bars[i]["close"] > bars[i - 1]["high"]
            )
            if i > setup["index"] and price_reversed and -setup["extension"] * confirm_share >= 0.03:
                result.append(
                    {
                        "index": i + 1,
                        "side": -setup["extension"],
                        "extension_return": setup["return"],
                        "efficiency": setup["efficiency"],
                    }
                )
                last_confirm = i
                setup = None
                continue
        if extension and setup is None and i - last_confirm >= 60:
            setup = {
                "index": i,
                "extension": extension,
                "return": change,
                "efficiency": efficiency,
                "volume": 0.0,
                "delta": 0.0,
            }
    return result


def replay(bars: list[dict[str, float]], candidates: list[dict[str, float]]) -> list[dict[str, float]]:
    trades = []
    busy_until = -1
    for signal in candidates:
        i = int(signal["index"])
        if i <= busy_until or i >= len(bars):
            continue
        side = int(signal["side"])
        entry = bars[i]["open"]
        stop = 0.005
        tp1 = 0.005
        end = min(i + 240, len(bars) - 1)
        exit_index = end
        gross = None
        outcome = "4h_time"
        for j in range(i, end + 1):
            stopped = bars[j]["low"] <= entry * (1 - stop) if side > 0 else bars[j]["high"] >= entry * (1 + stop)
            target = bars[j]["high"] >= entry * (1 + tp1) if side > 0 else bars[j]["low"] <= entry * (1 - tp1)
            if stopped:
                gross, exit_index, outcome = -stop, j, "stop"
                break
            if target:
                # TP1 平 50%，其余仓位按最保守的保本退出；这是生产退出逻辑的下界近似。
                gross, exit_index, outcome = tp1 * 0.5, j, "tp1_then_breakeven"
                break
        if gross is None:
            gross = side * (bars[end]["close"] / entry - 1.0)
        net = gross - 0.001  # 市价开仓 + 平仓，各 5 bps
        trades.append({**signal, "entry": entry, "exit_index": exit_index, "gross": gross, "net": net, "outcome": outcome})
        busy_until = exit_index
    return trades


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("eval_jsonl", type=Path)
    args = parser.parse_args()
    bars = load_bars(args.eval_jsonl)
    trades = replay(bars, signals(bars))
    for trade in trades:
        stamp = dt.datetime.fromtimestamp(bars[int(trade["index"])]["ts"] / 1000).astimezone()
        print(
            stamp.strftime("%F %T %z"),
            "LONG" if trade["side"] > 0 else "SHORT",
            f"extension={trade['extension_return'] * 100:+.3f}%",
            f"net_asset={trade['net'] * 100:+.3f}%",
            trade["outcome"],
        )
    wins = sum(t["net"] > 0 for t in trades)
    total = sum(t["net"] for t in trades)
    print(json.dumps({"bars": len(bars), "trades": len(trades), "wins": wins, "win_rate": wins / len(trades) if trades else 0.0, "net_asset_pct": total * 100}, ensure_ascii=False))


if __name__ == "__main__":
    main()
