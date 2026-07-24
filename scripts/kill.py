#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
诊断：C2 veto 错杀反事实分析（PR-8 定位病灶）

回答一个问题：被 C2 否决的候选，如果没有 C2，胜率能不能过无效性基线？
  A) c1∧c3 过、c2 否决的候选胜率 >= 基线 → C2 是唯一病灶（错杀好交易）
  B) 该组胜率 < 基线 → 信号内核（放量+翻向）本身没边际，得换信号

用法：
  python3 kill.py out/diag-cf.txt
  python3 kill.py out/diag-cf.txt --horizon 30 --sl-buffer 150 --baseline 0.605
  python3 kill.py out/diag-cf.txt --csv out/cf.csv
"""
import re, sys, argparse

# ---- ANSI 剥离：双保险 ----
# 完整转义序列 \x1b[...字母（文件直读时）
ANSI_FULL = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')
# 裸碎片 [...m（转义符已在终端/剪贴板丢失时，如 "[2m" "[0m" "[34m"）
ANSI_BARE = re.compile(r'\[[0-9;]*m')
def strip_ansi(s):
    return ANSI_BARE.sub('', ANSI_FULL.sub('', s))

# 数值 / 布尔 通用字段提取
VAL = r'true|false|[-0-9.eE]+|NaN'
def fval(line, key):
    # 字段间允许任意空白（tracing 对齐会产生双空格）
    m = re.search(rf'\b{key}\s*=\s*({VAL})', line)
    if not m:
        return None
    v = m.group(1)
    if v == 'true':
        return True
    if v == 'false':
        return False
    try:
        return float(v)
    except ValueError:
        return None

def wilson_ci(w, n, z=1.96):
    if n == 0:
        return (0.0, 1.0)
    p = w / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return ((c - m) / d, (c + m) / d)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('file')
    ap.add_argument('--horizon', type=int, default=30, help='最大持仓砖数（默认 30）')
    ap.add_argument('--sl-buffer', type=float, default=150.0, help='止损外扩美元（默认 150）')
    ap.add_argument('--baseline', type=float, default=0.605, help='无效性基线（默认 0.605）')
    ap.add_argument('--tick', type=float, default=0.5, help='tick 美元（默认 0.5）')
    ap.add_argument('--csv', default=None)
    args = ap.parse_args()

    evals = []   # 每个评估砖: {dir, chain_n, c1,c2,c3, high, low, brick_idx}
    bricks = []  # 完整砖序列: {dir, high, low, close}
    pend = {}    # 等待 high/low 的评估（字段在评估行之后到达）

    cur = None   # 当前砖缓存
    def flush_brick():
        nonlocal cur
        if cur is not None and cur.get('high') is not None and cur.get('low') is not None:
            bricks.append(cur)
        cur = None

    with open(args.file, encoding='utf-8', errors='ignore') as f:
        for raw in f:
            line = strip_ansi(raw)
            if 'exhaustion 评估' not in line:
                continue

            dirn = fval(line, 'dir')
            if dirn is None:
                continue
            dirn = int(dirn)

            h = fval(line, 'high')
            l = fval(line, 'low')
            c = fval(line, 'close')

            # 新砖判定：方向变了 → 旧砖落盘开新砖
            if cur is None or cur['dir'] != dirn:
                flush_brick()
                cur = {'dir': dirn, 'high': None, 'low': None, 'close': None}

            if h is not None: cur['high'] = h
            if l is not None: cur['low'] = l
            if c is not None: cur['close'] = c

            c1 = fval(line, 'c1')
            if c1 is None:
                continue  # 非评估字段行
            ev = {
                'dir': dirn,
                'chain_n': fval(line, 'chain_n') or 0,
                'c1': c1 is True,
                'c2': fval(line, 'c2') is True,
                'c3': fval(line, 'c3') is True,
                'body_ratio': fval(line, 'body_ratio'),
                'delta_pct': fval(line, 'delta_pct'),
            }
            if h is not None and l is not None:
                ev['high'], ev['low'] = h, l
                ev['brick_idx'] = len(bricks)  # 当前砖即将落入的索引
                evals.append(ev)
            else:
                pend.setdefault(dirn, []).append(ev)
                if len(pend) > 4:
                    for k in list(pend)[:2]:
                        pend.pop(k)

    flush_brick()

    # 回填缺 high/low 的评估（用同方向最近的完整砖近似——仅占极少数）
    if pend and bricks:
        by_dir = {}
        for i, b in enumerate(bricks):
            by_dir.setdefault(b['dir'], []).append(i)
        for dirn, evs in pend.items():
            cand = by_dir.get(dirn) or []
            for ev in evs:
                if cand:
                    i = cand[-1]
                    ev['high'], ev['low'] = bricks[i]['high'], bricks[i]['low']
                    ev['brick_idx'] = i
                    evals.append(ev)

    if not evals:
        print('未解析到评估行——确认日志含 "exhaustion 评估" 且字段带 dir/high/low/c1/c2/c3')
        sys.exit(1)
    if len(bricks) < args.horizon + 5:
        print(f'砖序列太短（{len(bricks)} 块 < horizon+5），无法做反事实——确认日志覆盖完整回测区间')
        sys.exit(1)

    n_b = len(bricks)
    tick, buf, H = args.tick, args.sl_buffer, args.horizon

    def counterfactual(ev):
        i0 = ev.get('brick_idx')
        if i0 is None:
            return None
        d = ev['dir']  # +1 做多 / -1 做空
        if d == 1:
            entry = ev['low'] + tick          # maker 成交于砖低点上一跳（保守）
            sl = ev['low'] - buf
            risk = entry - sl
        else:
            entry = ev['high'] - tick
            sl = ev['high'] + buf
            risk = sl - entry
        if risk <= 0:
            return None
        end = min(i0 + H, n_b - 1)
        if end <= i0:
            return None
        stopped = False
        exit_px = bricks[end]['close'] if bricks[end]['close'] is not None else bricks[end]['low']
        for j in range(i0 + 1, end + 1):
            b = bricks[j]
            if d == 1 and b['low'] <= sl:
                exit_px, stopped = sl, True; break
            if d == -1 and b['high'] >= sl:
                exit_px, stopped = sl, True; break
        r = ((exit_px - entry) if d == 1 else (entry - exit_px)) / risk
        return {'r': r, 'stopped': stopped}

    groups = {'all': [], 'vetoed_by_c2': [], 'c1&c3_pass': [], 'would_fire': []}
    for ev in evals:
        cf = counterfactual(ev)
        if cf is None:
            continue
        groups['all'].append(cf)
        if ev['c1'] and not ev['c2']:
            groups['vetoed_by_c2'].append(cf)
        if ev['c1'] and ev['c3'] and not ev['c2']:
            groups['c1&c3_pass'].append(cf)
        if ev['c1'] and ev['c2'] and ev['c3']:
            groups['would_fire'].append(cf)

    base = args.baseline
    print(f'=== C2 veto 错杀反事实（horizon={H} 砖 / SL=反转砖极值±{buf:.0f}$ / 基线={base:.1%}）===')
    print(f'解析：评估 {len(evals)} 次，砖序列 {n_b} 块\n')
    print(f'{"分组":<28}{"n":>6}{"胜率":>8}{"95%CI":>18}{"meanR":>8}{"止损%":>7}  判定')

    verdict = {}
    labels = {
        'all': '全部评估砖',
        'vetoed_by_c2': '被 C2 否决（c1∧¬c2）',
        'c1&c3_pass': '★ c1∧c3过、c2否决',
        'would_fire': '三条件全过（本会触发）',
    }
    for k in ('all', 'vetoed_by_c2', 'c1&c3_pass', 'would_fire'):
        g = groups[k]
        n = len(g)
        if n == 0:
            print(f'{labels[k]:<28}{0:>6}   无样本')
            verdict[k] = None
            continue
        w = sum(1 for x in g if x['r'] > 0)
        lo, hi = wilson_ci(w, n)
        mr = sum(x['r'] for x in g) / n
        sp = sum(1 for x in g if x['stopped']) / n
        ok = lo > base
        verdict[k] = (w / n, lo, hi, mr, ok)
        mark = '✅ 过基线' if ok else ('❌ 低于基线' if hi < base else '— CI 跨基线')
        print(f'{labels[k]:<28}{n:>6}{w/n:>8.1%}[{lo:>6.1%},{hi:>6.1%}]{mr:>8.2f}{sp:>7.0%}  {mark}')

    print('\n=== 病灶判定 ===')
    v = verdict.get('c1&c3_pass')
    if v is None:
        print('c1∧c3∧¬c2 组无样本——无法判定。把评估条件放宽（如 min_chain=2）重跑。')
    elif v[4]:
        print(f'A) C2 是唯一病灶：无 C2 时 c1∧c3 候选胜率 {v[0]:.1%}，CI 下限 {v[1]:.1%} > 基线 {base:.1%}')
        print('   → 砍掉 C2（或降为打分项），扣扳机 = c1∧c3，重新校准出场。')
    elif v[2] < base:
        print(f'B) 信号内核没边际：即使没 C2，c1∧c3 候选胜率 {v[0]:.1%}，CI 上限 {v[2]:.1%} < 基线 {base:.1%}')
        print('   → 放量+翻向这个内核在该形态上不成立。换信号维度（资金费率/OI）或换出入场结构。')
    else:
        print(f'?) 统计不定：c1∧c3 候选胜率 {v[0]:.1%}，CI [{v[1]:.1%}, {v[2]:.1%}] 跨过基线 {base:.1%}')
        print('   → 样本不足或边际太薄。加大样本（跑 3 个月）或放宽 min_chain 后再判。')

    if args.csv:
        with open(args.csv, 'w', encoding='utf-8') as f:
            f.write('group,n,winrate,ci_lo,ci_hi,mean_r,stop_pct\n')
            for k, g in groups.items():
                n = len(g)
                if n == 0:
                    continue
                w = sum(1 for x in g if x['r'] > 0)
                lo, hi = wilson_ci(w, n)
                mr = sum(x['r'] for x in g) / n
                sp = sum(1 for x in g if x['stopped']) / n
                f.write(f'{k},{n},{w/n:.4f},{lo:.4f},{hi:.4f},{mr:.4f},{sp:.4f}\n')
        print(f'\nCSV 已导出: {args.csv}')

if __name__ == '__main__':
    main()
