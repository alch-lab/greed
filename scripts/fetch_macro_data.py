#!/usr/bin/env python3
"""回补币安公开宏观数据（资金费率 + 5m OI/持仓比 metrics）。

用法:
  python3 scripts/fetch_macro_data.py [symbol] [start] [end]
  默认: BTCUSDT 2025-01-01 2026-06-30

输出:
  data/lake/funding/binance_futures/<SYM>/funding.csv   (funding_time_ms,rate,mark_price)
  data/lake/metrics/binance_futures/<SYM>/YYYY-MM-DD.csv (币安原始 5m 列)

需代理: HTTPS_PROXY=http://127.0.0.1:7897（本机直连币安超时）
幂等: 已存在的日期文件跳过。
"""
import csv
import io
import json
import os
import sys
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

SYM = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
START = sys.argv[2] if len(sys.argv) > 2 else "2025-01-01"
END = sys.argv[3] if len(sys.argv) > 3 else "2026-06-30"

FUNDING_DIR = f"data/lake/funding/binance_futures/{SYM}"
METRICS_DIR = f"data/lake/metrics/binance_futures/{SYM}"
os.makedirs(FUNDING_DIR, exist_ok=True)
os.makedirs(METRICS_DIR, exist_ok=True)

PROXY = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
opener = urllib.request.build_opener(
    urllib.request.ProxyHandler({"http": PROXY, "https": PROXY}) if PROXY else urllib.request.ProxyHandler({})
)


def get(url: str, retries: int = 3) -> bytes:
    last = None
    for i in range(retries):
        try:
            with opener.open(url, timeout=30) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"{url}: {last}")


def ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def fetch_funding() -> None:
    start, end = ms(date.fromisoformat(START)), ms(date.fromisoformat(END) + timedelta(days=1))
    rows, cur = [], start
    while cur < end:
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={SYM}&startTime={cur}&endTime={end}&limit=1000"
        batch = json.loads(get(url))
        if not batch:
            break
        rows.extend(batch)
        cur = batch[-1]["fundingTime"] + 1
        if len(batch) < 1000:
            break
        time.sleep(0.2)
    out = f"{FUNDING_DIR}/funding.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["funding_time_ms", "rate", "mark_price"])
        for r in rows:
            w.writerow([r["fundingTime"], r["fundingRate"], r.get("markPrice", "")])
    print(f"funding: {len(rows)} 条 -> {out}")


def fetch_metrics_one(d: date) -> str:
    ds = d.isoformat()
    out = f"{METRICS_DIR}/{ds}.csv"
    if os.path.exists(out):
        return "skip"
    url = f"https://data.binance.vision/data/futures/um/daily/metrics/{SYM}/{SYM}-metrics-{ds}.zip"
    try:
        blob = get(url, retries=2)
    except Exception as e:  # noqa: BLE001
        return f"FAIL {ds}: {e}"
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        name = z.namelist()[0]
        with z.open(name) as src, open(out, "wb") as dst:
            dst.write(src.read())
    return "ok"


def fetch_metrics() -> None:
    d0, d1 = date.fromisoformat(START), date.fromisoformat(END)
    days = [d0 + timedelta(days=i) for i in range((d1 - d0).days + 1)]
    ok = skip = 0
    fails = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for res in ex.map(fetch_metrics_one, days):
            if res == "ok":
                ok += 1
            elif res == "skip":
                skip += 1
            else:
                fails.append(res)
            if (ok + skip + len(fails)) % 100 == 0:
                print(f"metrics 进度: {ok + skip + len(fails)}/{len(days)}", flush=True)
    print(f"metrics: 新下载 {ok}，跳过 {skip}，失败 {len(fails)}")
    for f_ in fails[:10]:
        print(" ", f_)


if __name__ == "__main__":
    fetch_funding()
    fetch_metrics()
