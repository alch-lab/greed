#!/usr/bin/env python3
"""组合当前 MR journal 与预热后的 EMA10/100 日线趋势收益。"""
import csv, datetime as dt, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def trend_returns():
    rows=[]
    with (ROOT/'data/exp/btc_1d.csv').open() as f:
        for x in csv.DictReader(f):
            rows.append((dt.datetime.fromtimestamp(int(x['ts'])/1000,dt.timezone.utc).date(),float(x['close'])))
    ef=es=None;pos=0;prev=None;out={}
    for day,c in rows:
        r=0 if prev is None else pos*(c/prev-1)
        ef=c if ef is None else c*2/11+ef*9/11
        es=c if es is None else c*2/101+es*99/101
        want=1 if ef>es else -1
        r -= abs(want-pos)*0.0007
        pos=want;prev=c;out[day]=r
    return out

def journal_returns(path):
    x=json.load(path.open()); values={}
    for p in x['equity_curve']:
        if p['ts_ms']>0:
            day=dt.datetime.fromtimestamp(p['ts_ms']/1000,dt.timezone.utc).date()
            values[day]=p['equity']
    prev=x['meta']['initial_cash'];out={}
    for day in sorted(values):
        out[day]=values[day]/prev-1;prev=values[day]
    return out

def stats(rs):
    eq=peak=1.;dd=0.
    for r in rs:
        eq*=1+r;peak=max(peak,eq);dd=max(dd,1-eq/peak)
    return (eq-1,dd)

def main():
    tr=trend_returns()
    ranges={'mr-2025':(dt.date(2025,1,1),dt.date(2026,1,1)),
            'mr3-2025':(dt.date(2025,1,1),dt.date(2026,1,1)),
            'mr-2026h1':(dt.date(2026,1,1),dt.date(2026,7,1)),
            'mr3-2026h1':(dt.date(2026,1,1),dt.date(2026,7,1))}
    for tag in ('mr-2025','mr-2026h1','mr3-2025','mr3-2026h1'):
        mr=journal_returns(ROOT/f'out/portfolio/{tag}-journal.json')
        start,end=ranges[tag];days=[];day=start
        while day<end: days.append(day);day+=dt.timedelta(days=1)
        print(f'\n{tag}')
        for weight in (0,.10,.15,.20,.25):
            net,dd=stats([mr.get(d,0)+weight*tr.get(d,0) for d in days])
            print(f'trend={weight:.2f}x net={net*100:+.2f}% DD={dd*100:.2f}%')

if __name__=='__main__':main()
