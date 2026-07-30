#!/usr/bin/env python3
"""自适应阈值 × 趋势框架 双期回测矩阵（2026-07-27）。

6 个配置 × 2 个时期（2025 全年 / 2026H1），顺序执行（全年 run 的 events
Vec 内存峰值大，不并行）。每组结果写出到 out/exp/，汇总落
data/exp_matrix_results.json。

既可直接 `python scripts/exp_matrix.py` 跑，也可作为 Blueprint code
Automation 入口（提供 run(ctx)）。
"""
import json
import subprocess
import time

ROOT = "/Users/wonder/Code/greed"
BIN = f"{ROOT}/target/release/greed"

CONFIGS = [
    "exp-t1-03",     # T1 软趋势过滤：中性带 0.3%
    "exp-t1-05",     # 中性带 0.5%
    "exp-t1-08",     # 中性带 0.8%
    "exp-t2-n03",    # T2 积极限价：offset -0.03%
    "exp-t2-n05",    # offset -0.05%
    "exp-t2-n08",    # offset -0.08%
]
PERIODS = {
    "2025": ("2025-01-01", "2025-12-31"),
    "2026H1": ("2026-01-01", "2026-06-30"),
}


def run(ctx=None):
    results_path = f"{ROOT}/data/exp_matrix_results.json"
    results = {}
    try:
        results = json.load(open(results_path))
    except Exception:
        pass

    for name in CONFIGS:
        for per, (frm, to) in PERIODS.items():
            key = f"{name}-{per}"
            if key in results and results[key].get("rc") == 0:
                print(f"skip {key}（已有成功结果）", flush=True)
                continue
            outp = f"{ROOT}/out/exp/{key}"
            t0 = time.time()
            r = subprocess.run(
                [BIN, "backtest", "--from", frm, "--to", to,
                 "--strategy", f"{ROOT}/config/exp/{name}.toml",
                 "--out", outp],
                capture_output=True, text=True, timeout=3600, cwd=ROOT,
            )
            dur = time.time() - t0
            try:
                rep = json.load(open(outp + ".json"))
            except Exception as e:
                rep = {"parse_error": str(e)}
            results[key] = {
                "rc": r.returncode,
                "duration_s": round(dur),
                "report": rep,
                "stderr_tail": r.stderr[-400:],
            }
            json.dump(results, open(results_path, "w"), indent=1)
            print(f"done {key} in {dur:.0f}s rc={r.returncode}", flush=True)

    summary = f"matrix done: {len(results)} runs"
    print(json.dumps({"artifact": {"summary": summary,
                                   "results_path": results_path}}), flush=True)
    return {"summary": summary, "results_path": results_path}


if __name__ == "__main__":
    run()
