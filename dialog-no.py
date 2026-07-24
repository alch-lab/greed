#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
力竭扳机条件否决率统计（PR-8 诊断）

用法：
    python3 诊断-条件否决率统计.py out/diag-conditions.txt
    python3 诊断-条件否决率统计.py out/diag-conditions.txt --near 0.15   # 放宽"接近通过"阈值
    python3 诊断-条件否决率统计.py out/diag-conditions.txt --csv out/diag.csv
"""
import re, sys, argparse, math
from collections import Counter

def strip_ansi(s):
    return re.sub(r'\x1b\[[0-9;]*m', '', s)

FIELD = r'[-0-9.eE]+|true|false|NaN'
def parse_line(line):
    line = strip_ansi(line)
    if 'exhaustion 评估' not in line:
        return None
    out = {}
    for k in ['chain_rate','base','need','dur_ms','body_ratio','delta_pct','prev_dp','chain_n','c1','c2','c3']:
        m = re.search(rf'\b{k}=({FIELD})', line)
        if m:
            v = m.group(1)
            out[k] = (v == 'true') if v in ('true','false') else float(v)
    return out if len(out) >= 10 else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('file')
    ap.add_argument('--near', type=float, default=0.15, help='“接近通过”的边际（默认 ±15%%）')
    ap.add_argument('--csv', default=None)
    args = ap.parse_args()

    rows = []
    with open(args.file, encoding='utf-8', errors='ignore') as f:
        for line in f:
            r = parse_line(line)
            if r: rows.append(r)
    if not rows:
        print('未解析到任何评估行——确认文件来自 RUST_LOG=strategy=debug 且含 "exhaustion 评估"')
        sys.exit(1)

    n = len(rows)
    c = Counter()
    for r in rows:
        for k in ('c1','c2','c3'):
            if r[k]: c[k] += 1
    near = args.near

    print(f'=== 力竭扳机条件否决率（n={n} 个 ≥min_chain 的反转砖）===\n')
    print(f'{"条件":<6}{"通过":>8}{"否决":>8}{"否决率":>9}   说明')
    desc = {
        'c1':'放量快速（链速率≥3×基线×时段系数 ∧ 反转砖≤dur_max）',
        'c2':'力竭不推进（body/range ≤ prog_ratio=0.4）',
        'c3':'Delta翻向（前砖与链同号 ∧ 当前反号 ∧ |Δ%|≥8%）',
    }
    for k in ('c1','c2','c3'):
        p, q = c[k], n - c[k]
        print(f'{k:<6}{p:>8}{q:>8}{q/n*100:>8.1f}%   {desc[k]}')
    all3 = sum(1 for r in rows if r['c1'] and r['c2'] and r['c3'])
    print(f'\n三条件全过（应触发）: {all3} / {n}')

    # ---- c1 细分：是"量不够"还是"太慢"？----
    print('\n=== c1 否决细分 ===')
    c1_fail = [r for r in rows if not r['c1']]
    vol_fail = sum(1 for r in c1_fail if r['chain_rate'] < r['need'])
    slow     = sum(1 for r in c1_fail if r['dur_ms'] > 30000)
    both     = sum(1 for r in c1_fail if r['chain_rate'] < r['need'] and r['dur_ms'] > 30000)
    only_vol = sum(1 for r in c1_fail if r['chain_rate'] < r['need'] and r['dur_ms'] <= 30000)
    only_slow= sum(1 for r in c1_fail if r['chain_rate'] >= r['need'] and r['dur_ms'] > 30000)
    print(f'  c1 否决 {len(c1_fail)} 次中：')
    print(f'    仅量不足（rate<need, dur 合格）: {only_vol}')
    print(f'    仅太慢（dur>30s, rate 合格）:    {only_slow}')
    print(f'    两者兼有:                      {both}')
    if c1_fail:
        # 边际：rate/need 的对数分布
        import statistics
        ratios = [r['chain_rate']/r['need'] for r in c1_fail if r['need']>0]
        durs   = [r['dur_ms'] for r in c1_fail]
        print(f'    rate/need 中位数: {statistics.median(ratios):.2f}（1.0=刚好达标）')
        print(f'    dur_ms 中位数:    {statistics.median(durs)/1000:.1f}s（阈值 30s）')

    # ---- "接近通过"分析：放宽单一条件能多救回多少？----
    print(f'\n=== 单条件放宽的边际收益（"接近通过"= 该条件否决但其余两条件全过）===')
    for k, others in (('c1',('c2','c3')),('c2',('c1','c3')),('c3',('c1','c2'))):
        rescue = sum(1 for r in rows if not r[k] and r[others[0]] and r[others[1]])
        print(f'  若只放宽 {k}: 可救回 {rescue} 笔（占全部 {rescue/n*100:.1f}%）')

    # ---- 各条件的"接近阈值"候选（供参数高原分析）----
    print(f'\n=== 阈值邻域（±{near*100:.0f}% 内"差一点就过"的候选数）===')
    # c1: rate/need ∈ [1-near, 1)
    c1_near = sum(1 for r in rows if not r['c1'] and r['need']>0 and (1-near) <= r['chain_rate']/r['need'] < 1.0 and r['dur_ms'] <= 30000)
    c2_near = sum(1 for r in rows if not r['c2'] and 0.4 < r['body_ratio'] <= 0.4*(1+near*2))  # prog_ratio 0.4 → 0.4~0.48
    c3_near = sum(1 for r in rows if not r['c3'] and abs(r['delta_pct']) >= 8*(1-near) and (r['prev_dp']*r['delta_pct']<0))
    print(f'  c1: rate/need ∈ [{1-near:.2f},1) 且 dur 合格: {c1_near}')
    print(f'  c2: body_ratio ∈ (0.40, 0.48]:                {c2_near}')
    print(f'  c3: |Δ%| ∈ [{8*(1-near):.1f},8) 且符号翻向:    {c3_near}')

    # ---- 链长与时段分布 ----
    print('\n=== 候选的链长分布 ===')
    cn = Counter(int(r['chain_n']) for r in rows)
    for k in sorted(cn):
        bar = '█'*int(cn[k]/max(cn.values())*30)
        print(f'  {k:>2} 连: {cn[k]:>5} {bar}')

    if args.csv:
        import csv
        with open(args.csv,'w',newline='',encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
        print(f'\n明细已导出 {args.csv}')

if __name__ == '__main__':
    main()
