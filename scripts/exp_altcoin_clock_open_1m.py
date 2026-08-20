#!/usr/bin/env python3
"""Exact first-minute clock-phase flow test on a causal liquid universe."""

from __future__ import annotations

import argparse, csv, gzip, heapq, io, json, time, urllib.error, urllib.request, zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

DAY=86_400_000; MINUTE=60_000
PERIODS={"train":("2026-05-22","2026-07-01"),"validation":("2026-07-01","2026-08-01"),"holdout":("2026-08-01","2026-08-15"),"recent":("2026-08-15","2026-08-21")}
EXCLUDED={"BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT","DOGEUSDT","TRXUSDT","LINKUSDT","AVAXUSDT","SUIUSDT"}

def stamp(x): return int(datetime.fromisoformat(x).replace(tzinfo=timezone.utc).timestamp()*1000)
def daystr(ms): return datetime.fromtimestamp(ms/1000,timezone.utc).date().isoformat()

def download(url,path):
    if path.exists(): return path.stat().st_size>0
    path.parent.mkdir(parents=True,exist_ok=True)
    for n in range(4):
        try:
            with urllib.request.urlopen(url,timeout=30) as r:path.write_bytes(r.read())
            return True
        except urllib.error.HTTPError as e:
            if e.code==404:return False
        except (TimeoutError,urllib.error.URLError):pass
        time.sleep(.5*2**n)
    return False

def liquid_jobs(path):
    with gzip.open(path,"rt") as f:data=json.load(f)["data"]
    parsed={s:{int(r[0]):float(r[7]) for r in rows} for s,rows in data.items() if s.endswith("USDT") and s not in EXCLUDED and s.isascii()}
    start=min(min(x) for x in parsed.values()); end=max(max(x) for x in parsed.values())
    selected=set(); jobs=set()
    for day in range((start+DAY-1)//DAY*DAY,end,DAY):
        ranked=[]
        for symbol,values in parsed.items():
            recent=[values.get(ts) for ts in range(day-DAY,day,15*MINUTE)]
            if sum(v is not None for v in recent)>=95: ranked.append((sum(v or 0 for v in recent),symbol))
        for _,symbol in sorted(ranked,reverse=True)[:5]:
            selected.add((symbol,daystr(day)))
            jobs.add((symbol,daystr(day)))
            jobs.add((symbol,daystr(day+DAY)))
    return selected,jobs

def fetch(cache,job):
    symbol,day=job; path=cache/symbol/f"{day}.zip"
    ok=download(f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{day}.zip",path)
    return symbol,day,ok

def rows(path):
    out=[]
    with zipfile.ZipFile(path) as z:
        for r in csv.reader(io.TextIOWrapper(z.open(z.namelist()[0]))):
            if not r or not r[0].isdigit():continue
            ts=int(r[0]); ts=ts//1000 if ts>10**15 else ts
            q,t=float(r[7]),float(r[10]); out.append((ts,float(r[1]),float(r[2]),float(r[3]),float(r[4]),q,2*t/q-1 if q else 0))
    return out

def outcome(signal,bars,index,stop,horizon,slip):
    side=signal; i=index+1
    if i>=len(bars) or bars[i][0]!=bars[index][0]+MINUTE:return None
    fee=.0005; entry=bars[i][1]*(1+side*slip/10000); stop_price=entry*(1-side*stop); end=min(i+horizon,len(bars)-1)
    raw,ts=bars[end][1],bars[end][0]
    for k in range(i,end):
        b=bars[k]; hit=b[3]<=stop_price if side>0 else b[2]>=stop_price
        if hit:
            gap=b[1]<stop_price if side>0 else b[1]>stop_price; raw,ts=(b[1] if gap else stop_price),b[0]; break
    exit_price=raw*(1-side*slip/10000); return ts,side*(exit_price/entry-1)-fee-fee*exit_price/entry

def simulate(events,start,end,stop,horizon,slip=5,gross=.5):
    candidates=[]
    for score,side,symbol,index,bars in events:
        ts=bars[index][0]+MINUTE
        if start<=ts<end and (o:=outcome(side,bars,index,stop,horizon,slip)) and o[0]<end:candidates.append((ts,-score,symbol,side,o))
    equity=peak=day_start=1000.; dd=0.; active=[]; cooldown={}; serial=entries=wins=0; vals=[]; day=None; daily=0
    side_stats={"long":{"trades":0,"pnl":0.},"short":{"trades":0,"pnl":0.}}; daily_entries=defaultdict(int)
    for ts,negscore,symbol,side,(done,ret) in sorted(candidates):
        while active and active[0][0]<=ts:
            finished,_,s,pnl=heapq.heappop(active); equity+=pnl;cooldown[s]=finished;vals.append(pnl);wins+=pnl>0;peak=max(peak,equity);dd=max(dd,1-equity/peak)
        d=(ts+8*3_600_000)//DAY
        if d!=day:day,day_start,daily=d,equity,0
        if daily>=10 or equity<=day_start*.96 or len(active)>=2 or ts-cooldown.get(symbol,-10**18)<4*3_600_000:continue
        pnl=equity*gross*ret; serial+=1;heapq.heappush(active,(done,serial,symbol,pnl));entries+=1;daily+=1
        label="long" if side>0 else "short"; side_stats[label]["trades"]+=1; side_stats[label]["pnl"]+=pnl; daily_entries[str(d)]+=1
    while active:
        _,_,_,pnl=heapq.heappop(active);equity+=pnl;vals.append(pnl);wins+=pnl>0;peak=max(peak,equity);dd=max(dd,1-equity/peak)
    gain=sum(max(x,0) for x in vals);loss=sum(max(-x,0) for x in vals); ordered=sorted(vals)
    return {"return_pct":(equity/1000-1)*100,"max_drawdown_pct":dd*100,"trades":entries,"trades_per_day":entries/max((end-start)/DAY,1),"win_rate_pct":wins/len(vals)*100 if vals else 0,"profit_factor":gain/loss if loss else None,"avg_pnl":sum(vals)/len(vals) if vals else 0,"median_pnl":ordered[len(ordered)//2] if ordered else 0,"top5_gross_profit_share_pct":sum(ordered[-5:])/gain*100 if gain else None,"side_stats":side_stats,"daily_entries":daily_entries}

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--data",type=Path,default=Path("/private/tmp/binance-testnet-15m-20260515-0820.json.gz"));ap.add_argument("--cache",type=Path,default=Path("/private/tmp/greed-altcoin-clock-1m"));ap.add_argument("--output",type=Path,default=Path("/private/tmp/greed-altcoin-clock-open-1m.json"));a=ap.parse_args()
    selected_jobs,jobs=liquid_jobs(a.data)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures=[pool.submit(fetch,a.cache,j) for j in jobs]
        for n,f in enumerate(as_completed(futures),1):f.result(); print(f"downloaded {n}/{len(jobs)}",flush=True) if n%100==0 else None
    by_symbol=defaultdict(list)
    for symbol,day in jobs:
        p=a.cache/symbol/f"{day}.zip"
        if p.exists():by_symbol[symbol].extend(rows(p))
    clean_by_symbol={}
    for symbol,values in by_symbol.items():
        values.sort(); seen=set(); clean=[]
        for value in values:
            if value[0] not in seen: seen.add(value[0]); clean.append(value)
        clean_by_symbol[symbol]=clean
    events_by_key={}
    for threshold in (.10,.20,.25,.30,.35,.40,.45):
        for minimum_quote in (10_000,25_000,50_000,75_000,100_000):
            events=[]
            for symbol,clean in clean_by_symbol.items():
                for i,b in enumerate(clean[:-1]):
                    if (symbol,daystr(b[0])) in selected_jobs and b[0]%(15*MINUTE)==0 and abs(b[6])>=threshold and b[5]>=minimum_quote:events.append((abs(b[6])*b[5],1 if b[6]>0 else -1,symbol,i,clean))
            events_by_key[(threshold,minimum_quote)]=events
    results={}
    for (threshold,quote),events in events_by_key.items():
        for stop in (.01,.02,.025,.03,.035):
            for hours in (4,8,10,12,14):
                key=f"d{threshold:.2f}_q{quote:.0f}_s{stop:.2f}_h{hours}"
                results[key]={"delta":threshold,"min_quote":quote,"stop":stop,"hours":hours,"periods":{p:simulate(events,stamp(bounds[0]),stamp(bounds[1]),stop,hours*60) for p,bounds in PERIODS.items()}}
    eligible=[]
    for k,v in results.items():
        x,y=v["periods"]["train"],v["periods"]["validation"]
        if x["return_pct"]>0 and y["return_pct"]>0 and x["trades"]>=25 and y["trades"]>=15:eligible.append((y["return_pct"]-.5*y["max_drawdown_pct"],k))
    ranked=[k for _,k in sorted(eligible,reverse=True)]
    for key in ranked[:50]:
        item=results[key]; events=events_by_key[(item["delta"],item["min_quote"])]
        item["stress"]={str(slip):{p:simulate(events,stamp(bounds[0]),stamp(bounds[1]),item["stop"],item["hours"]*60,slip) for p,bounds in PERIODS.items()} for slip in (10,15,20)}
    phase_results={}; phase_events={}
    for phase in range(15):
        events=[]
        for symbol,clean in clean_by_symbol.items():
            for i,b in enumerate(clean[:-1]):
                if (symbol,daystr(b[0])) in selected_jobs and b[0]%(15*MINUTE)==phase*MINUTE and abs(b[6])>=.35 and b[5]>=50_000:
                    events.append((abs(b[6])*b[5],1 if b[6]>0 else -1,symbol,i,clean))
        phase_events[phase]=events
        phase_results[str(phase)]={str(slip):{p:simulate(events,stamp(bounds[0]),stamp(bounds[1]),.03,12*60,slip) for p,bounds in PERIODS.items()} for slip in (5,10)}
    phase_combinations={}
    for name,phases in {"0+12":(0,12),"0+12+14":(0,12,14)}.items():
        events=[event for phase in phases for event in phase_events[phase]]
        phase_combinations[name]={str(slip):{p:simulate(events,stamp(bounds[0]),stamp(bounds[1]),.03,12*60,slip) for p,bounds in PERIODS.items()} for slip in (5,10)}
    sizing={str(gross):{p:simulate(phase_events[0],stamp(bounds[0]),stamp(bounds[1]),.03,12*60,10,gross) for p,bounds in PERIODS.items()} for gross in (.25,.5,.75,1.,1.5,2.)}
    a.output.write_text(json.dumps({"selected_jobs":len(selected_jobs),"download_jobs":len(jobs),"ranked_without_holdout":ranked,"results":results,"clock_phase_placebo":phase_results,"clock_phase_combinations":phase_combinations,"clock_open_sizing_10bps":sizing},indent=2)+"\n")
    compact=lambda ps:{p:{k:v[k] for k in ("return_pct","max_drawdown_pct","trades","trades_per_day","win_rate_pct","profit_factor")} for p,v in ps.items()}
    print(json.dumps({"selected_jobs":len(selected_jobs),"download_jobs":len(jobs),"eligible":len(ranked),"top":[{"name":k,"periods":compact(results[k]["periods"])} for k in ranked[:10]]},indent=2))
if __name__=="__main__":main()
