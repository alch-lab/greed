#!/usr/bin/env python3
"""Promote exactly one approved and validated candidate, with rollback."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path


PROJECT = Path(os.environ.get("GREED_PROJECT_DIR", "/opt/greed")).resolve()
OUTPUT = Path(os.environ.get("GREED_RESEARCH_OUTPUT_DIR", PROJECT / "data/research/agent")).resolve()
REQUEST = OUTPUT / "promote-request.json"
MANIFEST = OUTPUT / "latest-candidate.json"
RESULT = OUTPUT / "latest-promotion.json"


def run(*args, **options):
    cwd = options.pop("cwd", PROJECT)
    env = options.pop("env", None)
    if options:
        raise TypeError("unexpected run options: " + ", ".join(options))
    completed = subprocess.run(
        args, cwd=cwd, env=env, check=True, universal_newlines=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    return completed.stdout.strip()


def atomic(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def finish(candidate, status, detail):
    now = int(time.time() * 1000)
    result = {
        "schema_version": 1,
        "candidate_id": candidate.get("candidate_id"),
        "candidate_commit": candidate.get("candidate_commit"),
        "status": status,
        "detail": detail[-6000:],
        "completed_ms": now,
    }
    atomic(RESULT, result)
    candidate["status"] = "promoted" if status == "promoted" else "promotion_failed"
    candidate["promotion"] = result
    candidate["updated_ms"] = now
    atomic(MANIFEST, candidate)


def main():
    request = json.loads(REQUEST.read_text(encoding="utf-8"))
    candidate = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if request.get("status") != "requested" or candidate.get("status") != "ready":
        return 0
    for field in ("candidate_id", "candidate_commit", "base_commit", "branch"):
        if request.get(field) != candidate.get(field):
            raise ValueError(f"approval does not match candidate {field}")
    if not candidate.get("gates") or not all(row.get("passed") is True for row in candidate["gates"]):
        raise ValueError("candidate gates are incomplete")
    performance = candidate.get("performance_validation") or {}
    if performance.get("status") != "passed" or performance.get("passed") is not True:
        raise ValueError("candidate has no passing deterministic historical performance replay")

    run("git", "fetch", "origin")
    origin_main = run("git", "rev-parse", "origin/main")
    if origin_main != candidate["base_commit"]:
        raise ValueError("main changed after validation; generate a fresh candidate")
    remote_candidate = run("git", "rev-parse", f"origin/{candidate['branch']}")
    if remote_candidate != candidate["candidate_commit"]:
        raise ValueError("remote candidate commit does not match the approved manifest")
    run("git", "merge-base", "--is-ancestor", origin_main, remote_candidate)

    epoch = run("sed", "-nE", "s/^rolling_pf_epoch[[:space:]]*=[[:space:]]*([0-9]+).*$/\\1/p", "config/demo.toml")
    deployment_env = os.environ.copy()
    deployment_env.update({
        "BACKEND_DIR": str(PROJECT),
        "FRONTEND_DIR": os.environ.get("GREED_FRONTEND_DIR", "/opt/greed-web"),
        "BACKEND_SERVICE": os.environ.get("GREED_BACKEND_SERVICE", "greed-paper"),
        "DEPLOY_BRANCH": candidate["branch"],
        "DEPLOY_FRONTEND": "0",
        "RUN_TESTS": "0",
        "EXPECTED_EPOCH": epoch,
    })
    deployed = False
    try:
        deploy_output = run(str(PROJECT / "deploy/deploy-paper-stack.sh"), env=deployment_env)
        deployed = True

        # Publish main only after the candidate binary is healthy on the server.
        run("git", "push", "origin", f"{remote_candidate}:refs/heads/main")
        run("git", "switch", "main")
        run("git", "merge", "--ff-only", remote_candidate)
    except Exception:
        # deploy-paper-stack restores the old binary on a failed health check.
        # If publishing main fails after a healthy candidate deployment, deploy
        # the unchanged origin/main again so the running binary and Git agree.
        if deployed:
            rollback_env = deployment_env.copy()
            rollback_env["DEPLOY_BRANCH"] = "main"
            try:
                run(str(PROJECT / "deploy/deploy-paper-stack.sh"), env=rollback_env)
            except Exception as rollback_error:
                print(f"candidate rollback also failed: {rollback_error}", file=sys.stderr)
        else:
            try:
                run("git", "switch", "main")
            except Exception:
                pass
        raise
    finish(candidate, "promoted", deploy_output)
    request["status"] = "completed"
    request["completed_ms"] = int(time.time() * 1000)
    atomic(REQUEST, request)
    return 0


if __name__ == "__main__":
    candidate = {}
    try:
        candidate = json.loads(MANIFEST.read_text(encoding="utf-8"))
        raise SystemExit(main())
    except Exception as error:
        if candidate:
            finish(candidate, "failed", str(error))
        print(f"candidate promotion failed: {error}", file=sys.stderr)
        raise
