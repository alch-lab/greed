#!/usr/bin/env python3
"""BTC 日线 EMA 多空趋势的长周期稳健性实验（2020 至今）。"""
import csv, datetime as dt, json, math, time, urllib.request
from pathlib import Path

CACHE = Path(__file__).resolve().parents[1] / "data/exp/btc_1d.csv"

def fetch():
    start = int(dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
    end = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    rows = []
    while start < end:
        url = ("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1d"
               f"&startTime={start}&endTime={end}&limit=1500")
        with urllib.request.urlopen(url, timeout=30) as r:
            part = json.load(r)
        if not part: break
        rows.extend(part); start = part[-1][0] + 86_400_000; time.sleep(0.1)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    with CACHE.open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["ts", "open", "close"])
        w.writerows((x[0], x[1], x[4]) for x in rows)

def load():
    if not CACHE.exists(): fetch()
    with CACHE.open() as f:
        return [(dt.datetime.fromtimestamp(int(x["ts"])/1000, dt.timezone.utc).date(),
                 float(x["close"])) for x in csv.DictReader(f)]

def run(rows, fast, slow, leverage=.5, cost=.0007):
    ef=es=None; pos=0; eq=peak=1.; dd=0.; turns=0; prev=None; curve=[]
    for day,c in rows:
        if prev is not None: eq *= 1 + pos*leverage*(c/prev-1)
        ef = c if ef is None else c*2/(fast+1)+ef*(fast-1)/(fast+1)
        es = c if es is None else c*2/(slow+1)+es*(slow-1)/(slow+1)
        want = 1 if ef > es else -1
        if want != pos:
            eq *= 1-abs(want-pos)*leverage*cost; turns += abs(want-pos); pos=want
        peak=max(peak,eq); dd=max(dd,1-eq/peak); curve.append((day,eq,pos,c,ef,es)); prev=c
    years=(rows[-1][0]-rows[0][0]).days/365.25
    return curve, eq**(1/years)-1, dd, turns

def annual(curve, year):
    z=[x for x in curve if x[0].year==year]
    if not z:return None
    base=z[0][1]; eq=peak=1.;dd=0
    for x in z:
        eq=x[1]/base;peak=max(peak,eq);dd=max(dd,1-eq/peak)
    return (eq-1,dd)

def main():
    rows=load(); print(f"days={len(rows)} {rows[0][0]}..{rows[-1][0]} leverage=0.5x cost=7bps/side-change")
    for fast,slow in ((10,80),(10,100),(10,120),(15,100),(15,120),(20,80),(20,100),(20,120),(25,100),(25,120)):
        c,cagr,dd,turns=run(rows,fast,slow)
        ys=[annual(c,y) for y in range(2020,rows[-1][0].year+1)]
        yearly=" ".join(f"{y}:{r[0]*100:+.1f}%" for y,r in zip(range(2020,rows[-1][0].year+1),ys) if r)
        print(f"EMA{fast}/{slow} CAGR={cagr*100:+.2f}% DD={dd*100:.1f}% turns={turns} {yearly}")

if __name__ == '__main__': main()
