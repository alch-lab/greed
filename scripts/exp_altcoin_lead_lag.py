#!/usr/bin/env python3
"""Causal cross-asset laggard continuation study."""
from __future__ import annotations
import argparse,gzip,json,math,statistics,heapq
from datetime import datetime,timezone
from pathlib import Path

BAR=900_000;DAY=86_400_000
PERIODS={"train":("2026-05-22","2026-07-01"),"validation":("2026-07-01","2026-08-01"),"holdout":("2026-08-01","2026-08-15"),"recent":("2026-08-15","2026-08-21")}
EXCLUDED={"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","TRXUSDT","LINKUSDT","AVAXUSDT","SUIUSDT"}
def stamp(x):return int(datetime.fromisoformat(x).replace(tzinfo=timezone.utc).timestamp()*1000)
def day(ms):return ms//DAY*DAY
def corr_beta(xs,ys):
    if len(xs)<80:return 0,0
    ax=sum(xs)/len(xs);ay=sum(ys)/len(ys);vx=sum((x-ax)**2 for x in xs);vy=sum((y-ay)**2 for y in ys)
    if vx<=0 or vy<=0:return 0,0
    cov=sum((x-ax)*(y-ay) for x,y in zip(xs,ys));return cov/math.sqrt(vx*vy),cov/vy
def prefix_stats(values,market):
    out=[[0.,0.,0.,0.,0.,0.]]
    for x,y in zip(values,market):
        c,sx,sy,sxx,syy,sxy=out[-1]
        if x is not None and y is not None:out.append([c+1,sx+x,sy+y,sxx+x*x,syy+y*y,sxy+x*y])
        else:out.append(out[-1].copy())
    return out
def prefix_corr_beta(pref,left,right):
    c,sx,sy,sxx,syy,sxy=(pref[right][i]-pref[left][i] for i in range(6))
    if c<80:return 0,0
    vx=sxx-sx*sx/c;vy=syy-sy*sy/c;cov=sxy-sx*sy/c
    if vx<=0 or vy<=0:return 0,0
    return cov/math.sqrt(vx*vy),cov/vy
def prepare(path):
    with gzip.open(path,"rt") as f:raw=json.load(f)["data"]
    rows={s:{int(x[0]):x for x in rs} for s,rs in raw.items() if s.endswith("USDT") and s not in EXCLUDED and s.isascii()}
    times=sorted(set.intersection(*(set(v) for v in sorted(rows.values(),key=len,reverse=True)[:3])))
    start=min(times);end=max(times);universes={}
    for d in range((start+DAY-1)//DAY*DAY,end+1,DAY):
        ranks=[]
        for s,v in rows.items():
            q=[v.get(t) for t in range(d-DAY,d,BAR)]
            if sum(x is not None for x in q)>=95:ranks.append((sum(float(x[7]) for x in q if x),s))
        universes[d]=[s for _,s in sorted(ranks,reverse=True)[:20]]
    return rows,universes
def feature_rows(rows,universes):
    out=[]
    for d,universe in universes.items():
      if not universe:continue
      all_times=list(range(d-97*BAR,d+DAY,BAR));time_index={t:i for i,t in enumerate(all_times)}
      returns={s:{t:float(rows[s][t][4])/float(rows[s][t-BAR][4])-1 for t in all_times if t in rows[s] and t-BAR in rows[s]} for s in universe}
      market_series={}
      for t in all_times:
          vals=[returns[s][t] for s in universe if t in returns[s]]
          if len(vals)>=15:market_series[t]=statistics.median(vals)
      market_values=[market_series.get(t) for t in all_times]
      prefixes={s:prefix_stats([returns[s].get(t) for t in all_times],market_values) for s in universe}
      for ts in range(d,d+DAY,BAR):
        if any(ts not in returns[s] for s in universe):continue
        current={s:returns[s][ts] for s in universe}
        market=statistics.median(current.values())
        if abs(market)<.002:continue
        side=1 if market>0 else -1;breadth=sum(side*r>0 for r in current.values())/len(current)
        idx=time_index[ts]
        for s in universe:
            c,beta=prefix_corr_beta(prefixes[s],idx-96,idx)
            lag=side*(beta*market-current[s])
            if beta>0 and side*current[s]<abs(market)*.5:
                out.append((ts+BAR,lag*c,s,side,rows[s],abs(market),breadth,lag,c))
    return out
def events(features,market_thr,breadth_thr,lag_thr,corr_thr):
    return [x[:5] for x in features if x[5]>=market_thr and x[6]>=breadth_thr and x[7]>=lag_thr and x[8]>=corr_thr]
def outcome(e,stop,hours,slip):
    ts,_,_,side,b=e
    if ts not in b:return None
    entry=float(b[ts][1])*(1+side*slip/10000);sp=entry*(1-side*stop);end=ts+hours*3_600_000;raw=None;done=end
    for t in range(ts,end,BAR):
        if t not in b:return None
        x=b[t];hit=float(x[3])<=sp if side>0 else float(x[2])>=sp
        if hit:raw=float(x[1]) if (float(x[1])<sp if side>0 else float(x[1])>sp) else sp;done=t;break
    if raw is None:
        if end not in b:return None
        raw=float(b[end][1])
    ep=raw*(1-side*slip/10000);return done,side*(ep/entry-1)-.0005-.0005*ep/entry
def simulate(es,start,end,stop,hours,slip=5):
    cs=[]
    for e in es:
        if start<=e[0]<end and (o:=outcome(e,stop,hours,slip)) and o[0]<end:cs.append((*e[:4],o))
    eq=peak=ds=1000.;dd=0;act=[];cool={};serial=n=wins=0;vals=[];dy=None;daily=0
    for ts,score,sym,side,(done,ret) in sorted(cs,key=lambda x:(x[0],-x[1])):
        while act and act[0][0]<=ts:
            fin,_,s,p=heapq.heappop(act);eq+=p;cool[s]=fin;vals.append(p);wins+=p>0;peak=max(peak,eq);dd=max(dd,1-eq/peak)
        d=(ts+8*3_600_000)//DAY
        if d!=dy:dy,ds,daily=d,eq,0
        if daily>=10 or eq<=ds*.96 or len(act)>=2 or ts-cool.get(sym,-10**18)<2*3_600_000:continue
        serial+=1;heapq.heappush(act,(done,serial,sym,eq*1.5*ret));n+=1;daily+=1
    while act:
        _,_,_,p=heapq.heappop(act);eq+=p;vals.append(p);wins+=p>0;peak=max(peak,eq);dd=max(dd,1-eq/peak)
    gp=sum(max(x,0) for x in vals);gl=sum(max(-x,0) for x in vals)
    return {"return_pct":100*(eq/1000-1),"max_drawdown_pct":100*dd,"trades":n,"trades_per_day":n/max((end-start)/DAY,1),"win_rate_pct":100*wins/len(vals) if vals else 0,"profit_factor":gp/gl if gl else None}
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--data",type=Path,default=Path("/private/tmp/binance-testnet-15m-20260515-0820.json.gz"));ap.add_argument("--output",type=Path,default=Path("/private/tmp/greed-altcoin-lead-lag.json"));a=ap.parse_args();rows,u=prepare(a.data);features=feature_rows(rows,u);results={};cache={}
    for mt in (.003,.005,.0075,.01):
     for bt in (.55,.65,.75):
      for lag in (.0025,.005,.0075,.01):
       for ct in (.3,.5,.7):
        es=events(features,mt,bt,lag,ct);cache[(mt,bt,lag,ct)]=es
        for stop in (.0075,.01,.015):
         for h in (1,2,4):
          k=f"m{mt}_b{bt}_l{lag}_c{ct}_s{stop}_h{h}";results[k]={"params":[mt,bt,lag,ct,stop,h],"signals":len(es),"periods":{p:simulate(es,stamp(x[0]),stamp(x[1]),stop,h) for p,x in PERIODS.items()}}
    eligible=[]
    for k,v in results.items():
        x,y=v["periods"]["train"],v["periods"]["validation"]
        if min(x["return_pct"],y["return_pct"])>0 and min(x["trades_per_day"],y["trades_per_day"])>=3:eligible.append((y["return_pct"]-.5*y["max_drawdown_pct"],k))
    ranked=[k for _,k in sorted(eligible,reverse=True)]
    for k in ranked[:100]:
        mt,bt,lag,ct,s,h=results[k]["params"];es=cache[(mt,bt,lag,ct)];results[k]["stress10"]={p:simulate(es,stamp(x[0]),stamp(x[1]),s,h,10) for p,x in PERIODS.items()}
    a.output.write_text(json.dumps({"eligible":len(ranked),"ranked_without_holdout":ranked,"results":results},indent=2)+"\n");print(json.dumps({"eligible":len(ranked),"top":ranked[:10]},indent=2))
if __name__=="__main__":main()
