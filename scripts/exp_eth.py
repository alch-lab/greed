#!/usr/bin/env python3
"""ETHUSDT 组合扩展验证（2026-07-29）。

用 strategy-final.toml 原参数（BTC 上调好，零重新拟合）跑 ETH 双期回测。
若 ETH 也过双期不劣化门槛，则 BTC+ETH 组合是冲击 20% 的组合路径。
结果写 data/exp_eth_results.json 与 out/exp/eth-final-{period}.json。
"""
import json
import subprocess
import time

ROOT = "/Users/wonder/Code/greed"
BIN = f"{ROOT}/target/release/greed"
CONFIGS = ["exp-eth-25", "exp-eth-30"]
PERIODS = {
    "2025": ("2025-01-01", "2025-12-31"),
    "2026H1": ("2026-01-01", "2026-06-30"),
}


def run(ctx=None):
    results_path = f"{ROOT}/data/exp_eth_results.json"
    results = {}
    try:
        results = json.load(open(results_path))
    except Exception:
        pass
    for cfg in CONFIGS:
      for per, (frm, to) in PERIODS.items():
        key = f"{cfg}-{per}"
        if key in results and results[key].get("rc") == 0:
            print(f"skip {key}", flush=True)
            continue
        outp = f"{ROOT}/out/exp/{key}"
        t0 = time.time()
        r = subprocess.run(
            [BIN, "backtest", "--symbol", "ETHUSDT",
             "--from", frm, "--to", to,
             "--strategy", f"{ROOT}/config/exp/{cfg}.toml",
             "--out", outp],
            capture_output=True, text=True, timeout=3600, cwd=ROOT,
        )
        dur = time.time() - t0
        try:
            rep = json.load(open(outp + ".json"))
        except Exception as e:
            rep = {"parse_error": str(e)}
        results[key] = {"rc": r.returncode, "duration_s": round(dur),
                        "report": rep, "stderr_tail": r.stderr[-400:]}
        json.dump(results, open(results_path, "w"), indent=1)
        print(f"done {key} in {dur:.0f}s rc={r.returncode}", flush=True)
    summary = f"eth dual-period done: {len(results)} runs"
    print(json.dumps({"artifact": {"summary": summary,
                                   "results_path": results_path}}), flush=True)
    return {"artifact": {"summary": summary, "results_path": results_path}}


if __name__ == "__main__":
    run()
