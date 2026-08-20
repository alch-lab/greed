#!/usr/bin/env python3
"""Download point-in-time Binance Futures Testnet klines for alpha research.

Public Futures Testnet data matches the market used by this project's paper
account.  The downloader keeps one compact gzip JSON file and never uses API
credentials.  It is deliberately separate from production trading code.
"""

from __future__ import annotations

import argparse
import gzip
import json
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


BASE = "https://testnet.binancefuture.com"


def get_json(path: str, params: dict[str, object] | None = None):
    query = "?" + urllib.parse.urlencode(params) if params else ""
    request = urllib.request.Request(BASE + path + query, headers={"User-Agent": "greed-research/1"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def fetch_symbol(symbol: str, interval: str, start_ms: int, end_ms: int) -> tuple[str, list[list[object]]]:
    rows: list[list[object]] = []
    cursor = start_ms
    for attempt in range(8):
        try:
            while cursor < end_ms:
                batch = get_json(
                    "/fapi/v1/klines",
                    # 499 rows use the low request-weight tier on the Futures
                    # API, so eight paced workers remain below the
                    # advertised 6,000-weight/minute public limit.
                    {"symbol": symbol, "interval": interval, "startTime": cursor, "endTime": end_ms, "limit": 499},
                )
                if not batch:
                    break
                rows.extend(batch)
                next_cursor = int(batch[-1][0]) + 1
                if next_cursor <= cursor:
                    break
                cursor = next_cursor
                time.sleep(0.18)
            deduped = {int(row[0]): row for row in rows if start_ms <= int(row[0]) < end_ms}
            return symbol, [deduped[key] for key in sorted(deduped)]
        except Exception:
            if attempt == 7:
                raise
            time.sleep(min(2**attempt, 20))
    raise RuntimeError(f"unreachable retry state for {symbol}")


def timestamp(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000)


def liquid_during_window(symbol: str, start_ms: int, end_ms: int, minimum_daily_quote: float) -> tuple[str, bool]:
    for attempt in range(6):
        try:
            rows = get_json(
                "/fapi/v1/klines",
                {"symbol": symbol, "interval": "1d", "startTime": start_ms, "endTime": end_ms, "limit": 200},
            )
            return symbol, any(float(row[7]) >= minimum_daily_quote for row in rows)
        except Exception:
            if attempt == 5:
                raise
            time.sleep(min(2**attempt, 10))
    raise RuntimeError(f"unreachable daily retry state for {symbol}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="UTC date, for example 2026-05-15")
    parser.add_argument("--end", required=True, help="exclusive UTC date")
    parser.add_argument("--interval", default="15m")
    parser.add_argument("--spot-info", default="/private/tmp/binance-spot-exchangeInfo.json")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--minimum-daily-quote", type=float, default=5_000_000.0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spot = json.loads(Path(args.spot_info).read_text())
    spot_symbols = {
        item["symbol"]
        for item in spot["symbols"]
        if item.get("quoteAsset") == "USDT"
        and item.get("status") == "TRADING"
        and item.get("isSpotTradingAllowed")
    }
    futures = get_json("/fapi/v1/exchangeInfo")
    futures_symbols = {
        item["symbol"]
        for item in futures["symbols"]
        if item.get("quoteAsset") == "USDT"
        and item.get("contractType") == "PERPETUAL"
        and item.get("status") == "TRADING"
    }
    symbols = sorted(spot_symbols & futures_symbols - {"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"})
    start_ms, end_ms = timestamp(args.start), timestamp(args.end)
    # One cheap daily request removes contracts that never approached the
    # strategy's point-in-time liquidity threshold during the study window.
    # This is only an I/O optimization: 15m features still enforce their own
    # rolling 24h volume rule at every historical timestamp.
    liquid_symbols: list[str] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        pending = {
            pool.submit(liquid_during_window, symbol, start_ms, end_ms, args.minimum_daily_quote): symbol
            for symbol in symbols
        }
        for future in as_completed(pending):
            symbol, eligible = future.result()
            if eligible:
                liquid_symbols.append(symbol)
    symbols = sorted(liquid_symbols)
    print(f"daily prescreen retained {len(symbols)} liquid symbols", flush=True)
    data: dict[str, list[list[object]]] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        pending = {pool.submit(fetch_symbol, symbol, args.interval, start_ms, end_ms): symbol for symbol in symbols}
        for completed, future in enumerate(as_completed(pending), 1):
            symbol = pending[future]
            try:
                name, rows = future.result()
                if rows:
                    data[name] = rows
            except Exception as error:
                errors[symbol] = str(error)
            if completed % 20 == 0 or completed == len(symbols):
                print(f"downloaded {completed}/{len(symbols)} symbols; errors={len(errors)}", flush=True)

    payload = {"start": start_ms, "end": end_ms, "interval": args.interval, "data": data, "errors": errors}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output, "wt", compresslevel=6) as handle:
        json.dump(payload, handle, separators=(",", ":"))
    print(f"saved {len(data)} symbols to {output}; errors={len(errors)}")


if __name__ == "__main__":
    main()
