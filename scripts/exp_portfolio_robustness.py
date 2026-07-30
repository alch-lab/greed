#!/usr/bin/env python3
"""20% 候选组合的块自助法、成本和 maker 漏单压力测试。"""
import datetime as dt, importlib.util, json, math, random, statistics
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('portfolio',ROOT/'scripts/exp_portfolio.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)

def load_period(tag,start,end,weight=.10):
    mr=p.journal_returns(ROOT/f'out/portfolio/{tag}-journal.json');tr=p.trend_returns()
    days=[];d=start
    while d<end:days.append(d);d+=dt.timedelta(days=1)
    return days,[mr.get(d,0)+weight*tr.get(d,0) for d in days],mr,tr

def metric(rs):
    eq=peak=1.;dd=0.
    for r in rs:eq*=1+r;peak=max(peak,eq);dd=max(dd,1-eq/peak)
    return eq-1,dd

def bootstrap(rs,block=30,years=3,n=10000,seed=20260731):
    rng=random.Random(seed);need=365*years;out=[]
    for _ in range(n):
        sample=[]
        while len(sample)<need:
            i=rng.randrange(0,len(rs)-block+1);sample.extend(rs[i:i+block])
        net,dd=metric(sample[:need]);cagr=(1+net)**(1/years)-1;out.append((cagr,dd))
    return out

def q(xs,pct):
    z=sorted(xs);return z[min(len(z)-1,int(pct*(len(z)-1)))]

def fee_stress(tag,days,base,weight):
    x=json.load((ROOT/f'out/portfolio/{tag}-journal.json').open());fees={}
    for f in x['fills']:
        d=dt.datetime.fromtimestamp(f['ts']/1000,dt.timezone.utc).date()
        fees[d]=fees.get(d,0)+f['fee']/100000
    # 趋势基准成本已含在收益里；切换成本很小，相比 MR 费用可忽略不计。
    for mult in (1,1.5,2,3):
        rs=[r-(mult-1)*fees.get(d,0) for d,r in zip(days,base)]
        print(f'  cost×{mult:g}: net={metric(rs)[0]*100:+.2f}% DD={metric(rs)[1]*100:.2f}%')

def maker_dropout(tag,days,mr,tr,weight,rates=(.05,.10,.20),n=5000):
    journal=json.load((ROOT/f'out/portfolio/{tag}-journal.json').open());trips=[];pnl=0.
    for f in journal['fills']:
        pnl += f['realized_pnl']-f['fee']
        if f['position_side_after'] is None:
            trips.append(pnl);pnl=0.
    trend_net=metric([weight*tr.get(d,0) for d in days])[0]
    rng=random.Random(731)
    for rate in rates:
        nets=[]
        for _ in range(n):
            kept=sum(v for v in trips if rng.random()>=rate)/journal['meta']['initial_cash']
            # 固定后续仓位近似：漏单会改变复利仓位，精确值需逐笔重放；这里只作敏感性筛查。
            nets.append(kept+trend_net)
        print(f'  maker漏单{rate:.0%}: net中位={statistics.median(nets)*100:+.2f}% '
              f'net P10={q(nets,.10)*100:+.2f}%（固定后续仓位近似）')

def main():
    all_rs=[]
    for tag,start,end in [('mr3-2025',dt.date(2025,1,1),dt.date(2026,1,1)),
                          ('mr3-2026h1',dt.date(2026,1,1),dt.date(2026,7,1))]:
        days,rs,mr,tr=load_period(tag,start,end);all_rs+=rs
        net,dd=metric(rs);print(f'\n{tag}: net={net*100:+.2f}% DD={dd*100:.2f}%')
        fee_stress(tag,days,rs,.10);maker_dropout(tag,days,mr,tr,.10)
    print('\n3年块自助法（基于18个月日收益，不能替代真正样本外）')
    for block in (30,60,90):
        z=bootstrap(all_rs,block=block)
        cs=[x[0] for x in z];dds=[x[1] for x in z]
        print(f'  block={block}d CAGR P10/P50/P90={q(cs,.1)*100:.1f}%/{q(cs,.5)*100:.1f}%/{q(cs,.9)*100:.1f}% '
              f'P(CAGR>=20%)={sum(x>=.20 for x in cs)/len(cs):.1%} DD P90={q(dds,.9)*100:.1f}%')

    # 31 个 MR 回合直接重采样：每年按观测频率约 21 笔，另加趋势 sleeve 约 4%/年。
    trips=[]
    for tag in ('mr3-2025','mr3-2026h1'):
        journal=json.load((ROOT/f'out/portfolio/{tag}-journal.json').open());pnl=0.
        for f in journal['fills']:
            pnl += f['realized_pnl']-f['fee']
            if f['position_side_after'] is None:trips.append(pnl/100000);pnl=0.
    rng=random.Random(731);nets=[];dds=[]
    for _ in range(100000):
        eq=peak=1.;dd=0.
        for _ in range(21):
            eq*=1+rng.choice(trips);peak=max(peak,eq);dd=max(dd,1-eq/peak)
        nets.append(eq-1+.04);dds.append(dd)
    print(f'\nMR回合自助法（31笔→每年21笔，趋势贡献固定4%）：'
          f'收益P10/P50/P90={q(nets,.1)*100:.1f}%/{q(nets,.5)*100:.1f}%/{q(nets,.9)*100:.1f}% '
          f'P(>=20%)={sum(x>=.2 for x in nets)/len(nets):.1%} P(亏损)={sum(x<0 for x in nets)/len(nets):.1%} '
          f'DD P90={q(dds,.9)*100:.1f}%')

if __name__=='__main__':main()
