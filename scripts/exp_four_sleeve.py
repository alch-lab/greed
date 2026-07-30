#!/usr/bin/env python3
"""MR + EMA趋势 + delta-neutral funding carry + 执行探针的统一组合实验。"""
import csv,datetime as dt,importlib.util
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('p',ROOT/'scripts/exp_portfolio.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)

def funding_daily():
    out={}
    with (ROOT/'data/lake/funding/binance_futures/BTCUSDT/funding.csv').open() as f:
        for x in csv.DictReader(f):
            day=dt.datetime.fromtimestamp(int(x['funding_time_ms'])/1000,dt.timezone.utc).date()
            out[day]=out.get(day,0)+float(x['rate'])
    return out

def days_between(a,b):
    out=[]
    while a<b:out.append(a);a+=dt.timedelta(days=1)
    return out

def stats(rs):
    eq=peak=1.;dd=0.
    for r in rs:eq*=1+r;peak=max(peak,eq);dd=max(dd,1-eq/peak)
    return eq-1,dd

def run(tag,start,end,trend_weight,carry_notional,probe_notional=10):
    mr=p.journal_returns(ROOT/f'out/portfolio/{tag}-journal.json')
    trend=p.trend_returns();fund=funding_daily();days=days_between(start,end)
    # 双腿常驻的保守开平成本：现货往返20bps + 永续往返8bps = 28bps/名义本金。
    carry_cost=0.0028*carry_notional
    # 每日一次$10永续探针往返，按10bps总成本；只验证链路，不贡献alpha。
    probe_daily=probe_notional/100000*0.0010
    rs=[]
    for i,d in enumerate(days):
        r=mr.get(d,0)+trend_weight*trend.get(d,0)+carry_notional*fund.get(d,0)-probe_daily
        if i==0:r-=carry_cost/2
        if i==len(days)-1:r-=carry_cost/2
        rs.append(r)
    return stats(rs),sum(fund.get(d,0) for d in days),len(days)

def main():
    periods=[('2025',dt.date(2025,1,1),dt.date(2026,1,1)),
             ('2026H1',dt.date(2026,1,1),dt.date(2026,7,1))]
    print('假设：EMA10/100=0.10x；carry为现货多+永续空常驻；每日一次$10执行探针')
    for risk,tag_prefix in [('MR1.5%','mr'),('MR3%','mr3')]:
        print(f'\n== {risk} ==')
        for carry in (0,.25,.50,1.0):
            line=[]
            for name,a,b in periods:
                tag=f'{tag_prefix}-2025' if name=='2025' else f'{tag_prefix}-2026h1'
                (net,dd),fund,n=run(tag,a,b,.10,carry)
                line.append(f'{name} net={net*100:+.2f}% DD={dd*100:.2f}% funding名义={fund*100:.2f}%')
            print(f'carry={carry:.2f}x | '+' | '.join(line))
    print('\n可观测事件：funding约3次/日；探针1次往返/日；MR约21笔/年；趋势约4-6次切换/年。')

if __name__=='__main__':main()
