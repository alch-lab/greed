#!/usr/bin/env python3
"""Causal Binance top-trader divergence study on a daily liquid universe."""

from __future__ import annotations

import argparse, csv, gzip, heapq, io, json, math, time, urllib.error, urllib.request, zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

DAY=86_400_000; BAR=900_000
PERIODS={"train":("2026-05-22","2026-07-01"),"validation":("2026-07-01","2026-08-01"),"holdout":("2026-08-01","2026-08-15"),"recent":("2026-08-15","2026-08-21")}
EXCLUDED={"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","TRXUSDT","LINKUSDT","AVAXUSDT","SUIUSDT"}

def stamp(x): return int(datetime.fromisoformat(x).replace(tzinfo=timezone.utc).timestamp()*1000)
def daystr(ms): return datetime.fromtimestamp(ms/1000,timezone.utc).date().isoformat()
def num(x):
    try:return float(x)
    except:return None

def selected_jobs(path):
    with gzip.open(path,"rt") as f:data=json.load(f)["data"]
    parsed={s:{int(r[0]):float(r[7]) for r in rows} for s,rows in data.items() if s.endswith("USDT") and s not in EXCLUDED and s.isascii()}
    start=min(min(v) for v in parsed.values());end=max(max(v) for v in parsed.values());out=set()
    for day in range((start+DAY-1)//DAY*DAY,end,DAY):
        ranks=[]
        for symbol,values in parsed.items():
            recent=[values.get(ts) for ts in range(day-DAY,day,BAR)]
            if sum(x is not None for x in recent)>=95:ranks.append((sum(x or 0 for x in recent),symbol))
        out.update((s,daystr(day)) for _,s in sorted(ranks,reverse=True)[:5])
    return out,data

def download(cache,job):
    symbol,day=job;path=cache/symbol/f"{day}.zip"
    if path.exists():return True
    path.parent.mkdir(parents=True,exist_ok=True)
    url=f"https://data.binance.vision/data/futures/um/daily/metrics/{symbol}/{symbol}-metrics-{day}.zip"
    for n in range(4):
        try:
            with urllib.request.urlopen(url,timeout=30) as r:path.write_bytes(r.read())
            return True
        except urllib.error.HTTPError as e:
            if e.code==404:return False
        except Exception:pass
        time.sleep(.5*2**n)
    return False

def metrics(path):
    out={}
    with zipfile.ZipFile(path) as z:
        for row in csv.DictReader(io.TextIOWrapper(z.open(z.namelist()[0]))):
            ts=int(datetime.fromisoformat(row["create_time"]).replace(tzinfo=timezone.utc).timestamp()*1000)
            out[ts]=(num(row["sum_open_interest_value"]),num(row["count_toptrader_long_short_ratio"]),num(row["sum_toptrader_long_short_ratio"]),num(row["count_long_short_ratio"]),num(row["sum_taker_long_short_vol_ratio"]))
    return out

def slog(x):return math.log(max(x,1e-9))

def load_metric_data(jobs,cache):
    by_symbol=defaultdict(dict)
    for symbol,day in jobs:
        p=cache/symbol/f"{day}.zip"
        if p.exists():by_symbol[symbol].update(metrics(p))
    return by_symbol

def build_events(jobs,bars_data,by_symbol,profile,threshold,price_cap,oi_min):
    events=[]
    selected=set(jobs)
    for symbol,m in by_symbol.items():
        raw=bars_data.get(symbol,[]); bars={int(x[0]):x for x in raw}; times=sorted(m)
        for ts in times:
            if ts%BAR or (symbol,daystr(ts)) not in selected or ts-3*BAR not in m or ts-BAR not in bars or ts not in bars:continue
            cur,old=m[ts],m[ts-3*BAR]
            if any(x is None or x<=0 for x in (*cur[:4],*old[:4])):continue
            oi=slog(cur[0]/old[0]); top_pos=slog(cur[2]/old[2]); top_acc=slog(cur[1]/old[1]); crowd=slog(cur[3]/old[3]); taker=slog(cur[4]) if cur[4] else 0
            b0=bars[ts-3*BAR];b1=bars[ts-BAR]; price=float(b1[4])/float(b0[1])-1
            divergence=top_pos-crowd
            if profile=="divergence": score=divergence; side=1 if score>0 else -1; passed=abs(score)>=threshold and abs(price)<=price_cap and oi>=oi_min
            elif profile=="position": score=top_pos; side=1 if score>0 else -1; passed=abs(score)>=threshold and abs(price)<=price_cap and oi>=oi_min
            elif profile=="account_divergence": score=top_acc-crowd;side=1 if score>0 else -1;passed=abs(score)>=threshold and abs(price)<=price_cap and oi>=oi_min
            elif profile=="confirmed": score=divergence;side=1 if score>0 else -1;passed=abs(score)>=threshold and side*price>=0 and abs(price)<=price_cap and oi>=oi_min and side*taker>0
            else:continue
            if passed:events.append((ts+BAR,abs(score)*(1+max(oi,0)),symbol,side,bars))
    return events

def outcome(event,stop,hours,slip):
    ts,_,_,side,bars=event
    if ts not in bars:return None
    entry=float(bars[ts][1])*(1+side*slip/10000); stop_px=entry*(1-side*stop); end=ts+hours*3_600_000; raw=None;done=end
    for t in range(ts,end,BAR):
        if t not in bars:return None
        b=bars[t];hit=float(b[3])<=stop_px if side>0 else float(b[2])>=stop_px
        if hit:raw=float(b[1]) if (float(b[1])<stop_px if side>0 else float(b[1])>stop_px) else stop_px;done=t;break
    if raw is None:
        if end not in bars:return None
        raw=float(bars[end][1])
    exit_px=raw*(1-side*slip/10000);fee=.0005
    return done,side*(exit_px/entry-1)-fee-fee*exit_px/entry

def simulate(events,start,end,stop,hours,slip=5,gross=1.5):
    candidates=[]
    for e in events:
        if start<=e[0]<end and (o:=outcome(e,stop,hours,slip)) and o[0]<end:candidates.append((*e[:4],o))
    equity=peak=day_start=1000.;dd=0.;active=[];cool={};serial=entries=wins=0;vals=[];day=None;daily=0
    for ts,score,symbol,side,(done,ret) in sorted(candidates,key=lambda x:(x[0],-x[1])):
        while active and active[0][0]<=ts:
            fin,_,sym,pnl=heapq.heappop(active);equity+=pnl;cool[sym]=fin;vals.append(pnl);wins+=pnl>0;peak=max(peak,equity);dd=max(dd,1-equity/peak)
        d=(ts+8*3_600_000)//DAY
        if d!=day:day,day_start,daily=d,equity,0
        if daily>=10 or equity<=day_start*.96 or len(active)>=2 or ts-cool.get(symbol,-10**18)<2*3_600_000:continue
        pnl=equity*gross*ret;serial+=1;heapq.heappush(active,(done,serial,symbol,pnl));entries+=1;daily+=1
    while active:
        _,_,_,pnl=heapq.heappop(active);equity+=pnl;vals.append(pnl);wins+=pnl>0;peak=max(peak,equity);dd=max(dd,1-equity/peak)
    gain=sum(max(x,0) for x in vals);loss=sum(max(-x,0) for x in vals)
    return {"return_pct":(equity/1000-1)*100,"max_drawdown_pct":dd*100,"trades":entries,"trades_per_day":entries/max((end-start)/DAY,1),"win_rate_pct":100*wins/len(vals) if vals else 0,"profit_factor":gain/loss if loss else None}

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--data",type=Path,default=Path("/private/tmp/binance-testnet-15m-20260515-0820.json.gz"));ap.add_argument("--cache",type=Path,default=Path("/private/tmp/greed-altcoin-smart-money"));ap.add_argument("--output",type=Path,default=Path("/private/tmp/greed-altcoin-smart-money.json"));a=ap.parse_args()
    jobs,bars_data=selected_jobs(a.data)
    with ThreadPoolExecutor(max_workers=8) as pool:
        fs=[pool.submit(download,a.cache,j) for j in jobs]
        for n,f in enumerate(as_completed(fs),1):f.result();print(f"download {n}/{len(fs)}",flush=True) if n%100==0 else None
    metric_data=load_metric_data(jobs,a.cache)
    results={}
    for profile in ("divergence","position","account_divergence","confirmed"):
      for threshold in (.03,.05,.08,.12,.20):
       for price_cap in (.005,.01,.02,.04):
        for oi_min in (-.02,0,.02):
         events=build_events(jobs,bars_data,metric_data,profile,threshold,price_cap,oi_min)
         for stop in (.0075,.01,.015,.02):
          for hours in (1,2,4):
           key=f"{profile}_d{threshold}_p{price_cap}_oi{oi_min}_s{stop}_h{hours}"
           results[key]={"profile":profile,"threshold":threshold,"price_cap":price_cap,"oi_min":oi_min,"stop":stop,"hours":hours,"signals":len(events),"periods":{p:simulate(events,stamp(b[0]),stamp(b[1]),stop,hours) for p,b in PERIODS.items()}}
    eligible=[]
    for k,v in results.items():
        x=v["periods"]["train"];y=v["periods"]["validation"]
        if min(x["return_pct"],y["return_pct"])>0 and min(x["trades_per_day"],y["trades_per_day"])>=3:eligible.append((y["return_pct"]-.5*y["max_drawdown_pct"],k))
    ranked=[k for _,k in sorted(eligible,reverse=True)]
    for k in ranked[:100]:
        v=results[k];events=build_events(jobs,bars_data,metric_data,v["profile"],v["threshold"],v["price_cap"],v["oi_min"])
        v["stress10"]={p:simulate(events,stamp(b[0]),stamp(b[1]),v["stop"],v["hours"],10) for p,b in PERIODS.items()}
    a.output.write_text(json.dumps({"jobs":len(jobs),"eligible":len(ranked),"ranked_without_holdout":ranked,"results":results},indent=2)+"\n")
    print(json.dumps({"jobs":len(jobs),"eligible":len(ranked),"top":[{"name":k,**results[k]} for k in ranked[:10]]},indent=2))

if __name__=="__main__":main()
