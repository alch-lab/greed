#!/usr/bin/env python3
"""Turn the latest structured review into a tested, pushed candidate branch.

The model can only propose exact replacements inside strategy recipe files. It
cannot touch execution, risk, credentials, deployment, configuration, or the
active checkout. Generated code is rejected before compilation when it adds
host/process/network/file-system primitives.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


PROJECT = Path(os.environ.get("GREED_PROJECT_DIR", "/opt/greed")).resolve()
OUTPUT = Path(os.environ.get("GREED_RESEARCH_OUTPUT_DIR", PROJECT / "data/research/agent")).resolve()
REVIEW = OUTPUT / "latest-review.json"
MANIFEST = OUTPUT / "latest-candidate.json"
CANDIDATE_ROOT = Path(os.environ.get("GREED_CANDIDATE_ROOT", "/opt/greed-candidates")).resolve()
API_BASE = os.environ.get("GREED_RESEARCH_API_BASE", "https://open.bigmodel.cn/api/coding/paas/v4").rstrip("/")
MODEL = os.environ.get("GREED_RESEARCH_MODEL", "glm-5.2")
ALLOWED_FILES = {
    "crates/greed-strategy/src/recipes/trend_continuation.rs",
    "crates/greed-strategy/src/recipes/fast_trend_activation.rs",
    "crates/greed-strategy/src/recipes/liquidation_exhaustion_reversal.rs",
}
FORBIDDEN_ADDITIONS = (
    "unsafe", "std::process", "Command::", "std::fs", "std::env", "reqwest",
    "tokio::net", "TcpStream", "UdpSocket", "include_str!", "include_bytes!",
    "env!", "option_env!", "extern \"C\"", "#[link",
)
MAX_REPLACEMENTS = 16
MAX_REPLACEMENT_BYTES = 24_000


class CandidateDeferred(Exception):
    pass


class CandidateOutputError(ValueError):
    pass


def run(*args, **options):
    cwd = options.pop("cwd", PROJECT)
    capture = options.pop("capture", True)
    if options:
        raise TypeError("unexpected run options: " + ", ".join(options))
    try:
        completed = subprocess.run(
            args,
            cwd=cwd,
            check=True,
            universal_newlines=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.STDOUT if capture else None,
        )
    except subprocess.CalledProcessError as error:
        output = (error.stdout or "").strip()
        command = " ".join(args)
        raise RuntimeError(f"{command} failed ({error.returncode})\n{output[-6000:]}") from error
    return completed.stdout.strip() if capture else ""


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def fail_manifest(review_ms, stage, detail):
    atomic_json(MANIFEST, {
        "schema_version": 1,
        "candidate_id": f"failed-{int(time.time() * 1000)}",
        "review_ms": review_ms,
        "status": "failed",
        "stage": stage,
        "detail": detail[-4000:],
        "updated_ms": int(time.time() * 1000),
        "gates": [],
    })


def select_recipe(review):
    recipes = [
        (
            "crates/greed-strategy/src/recipes/liquidation_exhaustion_reversal.rs",
            ("liquidation_exhaustion_reversal", "liquidation exhaustion", "liquidation reversal"),
        ),
        (
            "crates/greed-strategy/src/recipes/fast_trend_activation.rs",
            ("fast_trend_activation", "fast trend activation", "fast activation"),
        ),
        (
            "crates/greed-strategy/src/recipes/trend_continuation.rs",
            ("trend_continuation", "trend continuation"),
        ),
    ]
    for experiment in review.get("experiments") or []:
        text = json.dumps(experiment, ensure_ascii=False).lower()
        for path, aliases in recipes:
            if any(alias in text for alias in aliases):
                return path
    text = json.dumps(review, ensure_ascii=False).lower()
    scored = []
    for path, aliases in recipes:
        score = sum(text.count(alias) for alias in aliases)
        if score:
            scored.append((score, path))
    return max(scored)[1] if scored else None


def source_bundle(paths):
    result = []
    for relative in sorted(paths):
        text = (PROJECT / relative).read_text(encoding="utf-8")
        result.append({
            "path": relative,
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
            "content": text,
        })
    return result


def decode_candidate_envelope(envelope):
    try:
        choice = envelope["choices"][0]
        message = choice["message"]
        content = message.get("content")
    except (KeyError, IndexError, TypeError) as error:
        raise CandidateOutputError("candidate response has no assistant message") from error
    if not isinstance(content, str) or not content.strip():
        reasoning = message.get("reasoning_content") or ""
        usage = envelope.get("usage") or {}
        raise CandidateOutputError(
            "candidate response content is empty; finish_reason={}; reasoning_bytes={}; completion_tokens={}".format(
                choice.get("finish_reason"), len(reasoning.encode()), usage.get("completion_tokens")
            )
        )
    try:
        return json.loads(content)
    except ValueError as error:
        raise CandidateOutputError(
            "candidate response is not JSON: {}; content_bytes={}".format(error, len(content.encode()))
        )


def call_model(review, sources, correction=None):
    key = os.environ.get("ZHIPU_API_KEY")
    if not key:
        raise ValueError("ZHIPU_API_KEY is missing")
    schema = {
        "decision": "candidate or no_candidate",
        "title": "short title",
        "summary": "what changes and why",
        "changes": [{"path": "allowed path", "search": "exact unique old text", "replace": "replacement text"}],
    }
    instructions = (
        "You are preparing one minimal Rust strategy candidate from an already completed research review. "
        "Return JSON only. Never change execution, accounting, portfolio risk, credentials, deployment, "
        "epoch, dependencies, or public APIs. Do not add file/process/network/environment access, unsafe, "
        "FFI, include macros, or tests that perform side effects. Prefer no_candidate when evidence is weak. "
        "Every search string must occur exactly once in the supplied file. Only the single supplied recipe "
        "may be changed. Keep the patch small and use no more than {} exact replacements. "
        "The JSON shape is: ".format(MAX_REPLACEMENTS) + json.dumps(schema)
    )
    if correction:
        instructions += (
            " Your previous answer was rejected by the local validator: "
            + correction[:600]
            + ". Regenerate the complete JSON object and obey the shape exactly. "
              "Use decision=no_candidate when no valid exact replacement is available."
        )
    payload = json.dumps({"review": review, "editable_sources": sources}, ensure_ascii=False)
    request = urllib.request.Request(
        API_BASE + "/chat/completions",
        data=json.dumps({
            "model": MODEL,
            "messages": [
                {"role": "system", "content": instructions},
                {"role": "user", "content": payload},
            ],
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
            "max_tokens": 12288,
            "temperature": 0.2,
            "stream": False,
        }).encode(),
        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            envelope = json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode()[:2000]
        try:
            provider_code = str(json.loads(detail).get("error", {}).get("code", ""))
        except (TypeError, ValueError):
            provider_code = ""
        if provider_code == "1308":
            raise CandidateDeferred("Zhipu Coding Plan quota is temporarily exhausted")
        raise RuntimeError(f"Zhipu candidate request returned {error.code}: {detail}") from error
    return decode_candidate_envelope(envelope)


def validate_proposal(proposal, sources):
    if proposal.get("decision") == "no_candidate":
        return []
    if proposal.get("decision") != "candidate":
        raise ValueError("candidate decision must be candidate or no_candidate")
    changes = proposal.get("changes")
    if not isinstance(changes, list) or not 1 <= len(changes) <= MAX_REPLACEMENTS:
        count = len(changes) if isinstance(changes, list) else "not_a_list"
        raise ValueError(
            "candidate must contain 1..{} replacements; received {}".format(
                MAX_REPLACEMENTS, count
            )
        )
    originals = {row["path"]: row["content"] for row in sources}
    total = 0
    for change in changes:
        path = change.get("path")
        search = change.get("search")
        replacement = change.get("replace")
        if path not in originals or not isinstance(search, str) or not isinstance(replacement, str):
            raise ValueError("candidate contains an invalid path or replacement")
        if not search or search == replacement or originals[path].count(search) != 1:
            raise ValueError(f"replacement search is not unique in {path}")
        total += len(search.encode()) + len(replacement.encode())
        added = replacement.replace(search, "")
        if any(token in added for token in FORBIDDEN_ADDITIONS):
            raise ValueError(f"replacement adds a forbidden capability in {path}")
        originals[path] = originals[path].replace(search, replacement, 1)
    if total > MAX_REPLACEMENT_BYTES:
        raise ValueError("candidate replacement payload is too large")
    return changes


def main():
    if not REVIEW.is_file():
        atomic_json(MANIFEST, {
            "schema_version": 1, "candidate_id": f"waiting-{int(time.time() * 1000)}",
            "review_ms": None, "status": "no_candidate",
            "summary": "The latest research run did not produce a complete review. It will retry on schedule.",
            "updated_ms": int(time.time() * 1000), "gates": [],
        })
        return 0
    review_artifact = json.loads(REVIEW.read_text(encoding="utf-8"))
    review_ms = int(review_artifact.get("generated_ms") or 0)
    latest_input = OUTPUT / "latest-input.json"
    if latest_input.is_file():
        input_ms = int(json.loads(latest_input.read_text(encoding="utf-8")).get("generated_ms") or 0)
        if input_ms != review_ms:
            atomic_json(MANIFEST, {
                "schema_version": 1, "candidate_id": f"waiting-{input_ms}",
                "review_ms": input_ms, "status": "no_candidate",
                "summary": "The latest research response was incomplete. The previous review was not reused.",
                "updated_ms": int(time.time() * 1000), "gates": [],
            })
            return 0
    review = review_artifact.get("review") or {}
    if review.get("decision") != "run_experiments" or not review.get("data_quality", {}).get("usable"):
        atomic_json(MANIFEST, {
            "schema_version": 1, "candidate_id": f"review-{review_ms}", "review_ms": review_ms,
            "status": "no_candidate", "summary": review.get("summary", "No validated change proposed."),
            "updated_ms": int(time.time() * 1000), "gates": [],
        })
        return 0
    if MANIFEST.exists():
        previous = json.loads(MANIFEST.read_text(encoding="utf-8"))
        if previous.get("review_ms") == review_ms and previous.get("status") in {
            "ready", "blocked", "no_candidate", "promoted"
        }:
            return 0

    run("git", "fetch", "origin", "main")
    base_commit = run("git", "rev-parse", "origin/main")
    selected_recipe = select_recipe(review)
    if selected_recipe is None:
        atomic_json(MANIFEST, {
            "schema_version": 1, "candidate_id": "blocked-{}".format(review_ms),
            "review_ms": review_ms, "status": "blocked", "stage": "recipe_selection",
            "summary": "The review did not identify one strategy recipe precisely enough to edit safely.",
            "updated_ms": int(time.time() * 1000),
            "gates": [{"name": "Single recipe identified", "passed": False}],
        })
        return 0
    sources = source_bundle([selected_recipe])
    retry_used = False
    try:
        proposal = call_model(review, sources)
    except CandidateDeferred as error:
        atomic_json(MANIFEST, {
            "schema_version": 1, "candidate_id": "blocked-{}".format(review_ms),
            "review_ms": review_ms, "status": "blocked", "stage": "provider_quota",
            "summary": "Candidate generation is waiting for the next Coding Plan quota window.",
            "detail": str(error), "updated_ms": int(time.time() * 1000),
            "gates": [{"name": "Provider quota available", "passed": False}],
        })
        return 0
    except CandidateOutputError as first_error:
        retry_used = True
        try:
            proposal = call_model(review, sources, str(first_error))
        except CandidateDeferred as error:
            atomic_json(MANIFEST, {
                "schema_version": 1, "candidate_id": "blocked-{}".format(review_ms),
                "review_ms": review_ms, "status": "blocked", "stage": "provider_quota",
                "summary": "Candidate regeneration is waiting for the next Coding Plan quota window.",
                "detail": str(error), "updated_ms": int(time.time() * 1000),
                "gates": [{"name": "Provider quota available", "passed": False}],
            })
            return 0
        except CandidateOutputError as second_error:
            atomic_json(MANIFEST, {
                "schema_version": 1, "candidate_id": "blocked-{}".format(review_ms),
                "review_ms": review_ms, "status": "blocked", "stage": "provider_output",
                "summary": "Zhipu returned no usable candidate JSON after one compact retry.",
                "detail": str(second_error)[:2000], "updated_ms": int(time.time() * 1000),
                "gates": [{"name": "Complete candidate response", "passed": False}],
            })
            return 0
    try:
        changes = validate_proposal(proposal, sources)
    except ValueError as first_error:
        if retry_used:
            atomic_json(MANIFEST, {
                "schema_version": 1,
                "candidate_id": "blocked-{}".format(review_ms),
                "review_ms": review_ms,
                "status": "blocked",
                "stage": "proposal_validation",
                "summary": "The regenerated candidate was not mechanically applicable.",
                "detail": str(first_error)[:2000],
                "updated_ms": int(time.time() * 1000),
                "gates": [{"name": "Candidate patch shape", "passed": False}],
            })
            return 0
        try:
            proposal = call_model(review, sources, str(first_error))
        except CandidateDeferred as error:
            atomic_json(MANIFEST, {
                "schema_version": 1, "candidate_id": "blocked-{}".format(review_ms),
                "review_ms": review_ms, "status": "blocked", "stage": "provider_quota",
                "summary": "Candidate correction is waiting for the next Coding Plan quota window.",
                "detail": str(error), "updated_ms": int(time.time() * 1000),
                "gates": [{"name": "Provider quota available", "passed": False}],
            })
            return 0
        except CandidateOutputError as error:
            atomic_json(MANIFEST, {
                "schema_version": 1, "candidate_id": "blocked-{}".format(review_ms),
                "review_ms": review_ms, "status": "blocked", "stage": "provider_output",
                "summary": "Zhipu returned no usable candidate JSON after one compact retry.",
                "detail": str(error)[:2000], "updated_ms": int(time.time() * 1000),
                "gates": [{"name": "Complete candidate response", "passed": False}],
            })
            return 0
        try:
            changes = validate_proposal(proposal, sources)
        except ValueError as second_error:
            atomic_json(MANIFEST, {
                "schema_version": 1,
                "candidate_id": "blocked-{}".format(review_ms),
                "review_ms": review_ms,
                "status": "blocked",
                "stage": "proposal_validation",
                "summary": "The model did not produce a safe, mechanically applicable patch.",
                "detail": str(second_error)[:2000],
                "updated_ms": int(time.time() * 1000),
                "gates": [{"name": "Candidate patch shape", "passed": False}],
            })
            return 0
    if not changes:
        atomic_json(MANIFEST, {
            "schema_version": 1, "candidate_id": f"review-{review_ms}", "review_ms": review_ms,
            "status": "no_candidate", "summary": proposal.get("summary", "No safe code candidate."),
            "updated_ms": int(time.time() * 1000), "gates": [],
        })
        return 0

    candidate_id = f"auto-{review_ms}"
    branch = f"research/{candidate_id}"
    CANDIDATE_ROOT.mkdir(parents=True, exist_ok=True)
    worktree = CANDIDATE_ROOT / candidate_id
    if worktree.exists():
        try:
            run("git", "worktree", "remove", "--force", str(worktree))
        except Exception:
            shutil.rmtree(worktree, ignore_errors=True)
    run("git", "worktree", "prune")
    if subprocess.run(
        ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=PROJECT,
    ).returncode == 0:
        run("git", "branch", "-D", branch)
    run("git", "worktree", "add", "--detach", str(worktree), base_commit)
    try:
        for change in changes:
            path = worktree / change["path"]
            text = path.read_text(encoding="utf-8")
            path.write_text(text.replace(change["search"], change["replace"], 1), encoding="utf-8")
        changed = set(run("git", "diff", "--name-only", cwd=worktree).splitlines())
        if not changed or not changed.issubset(ALLOWED_FILES):
            raise ValueError("candidate modified files outside the strategy recipe allowlist")
        run("git", "diff", "--check", cwd=worktree)
        run("cargo", "fmt", "--check", cwd=worktree)
        run("cargo", "test", "--workspace", "-j", "1", cwd=worktree)
        run("cargo", "build", "--release", "-j", "1", cwd=worktree)
        run(str(worktree / "target/release/greed"), "validate", "--config", "config/demo.toml", cwd=worktree)
        run("git", "switch", "-c", branch, cwd=worktree)
        run("git", "add", *sorted(changed), cwd=worktree)
        run("git", "-c", "user.name=greed-research", "-c", "user.email=research@localhost",
            "commit", "-m", str(proposal.get("title") or "automated strategy candidate"), cwd=worktree)
        candidate_commit = run("git", "rev-parse", "HEAD", cwd=worktree)
        run("git", "push", "--force-with-lease", "origin", f"HEAD:refs/heads/{branch}", cwd=worktree)
        gates = [
            {"name": "Research data usable", "passed": True},
            {"name": "Strategy-only source boundary", "passed": True},
            {"name": "Forbidden capability scan", "passed": True},
            {"name": "Rust formatting", "passed": True},
            {"name": "Workspace tests", "passed": True},
            {"name": "Release build and config validation", "passed": True},
            {"name": "Historical performance replay", "passed": False},
        ]
        performance_validation = {
            "status": "not_run",
            "passed": False,
            "reason": (
                "The review defines a proposed experiment, not a deterministic replay result. "
                "An arbitrary Rust patch cannot be promoted on engineering checks alone."
            ),
        }
        atomic_json(MANIFEST, {
            "schema_version": 1, "candidate_id": candidate_id, "review_ms": review_ms,
            "prompt_version": "candidate-v3-single-recipe",
            "status": "blocked",
            "title": proposal.get("title"), "summary": proposal.get("summary"),
            "base_commit": base_commit, "branch": branch, "candidate_commit": candidate_commit,
            "changed_files": sorted(changed), "gates": gates,
            "performance_validation": performance_validation,
            "created_ms": int(time.time() * 1000), "updated_ms": int(time.time() * 1000),
        })
        return 0
    except Exception:
        raise
    finally:
        try:
            run("git", "worktree", "remove", "--force", str(worktree))
        except Exception:
            shutil.rmtree(worktree, ignore_errors=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        review_ms = None
        try:
            review_ms = json.loads(REVIEW.read_text(encoding="utf-8")).get("generated_ms")
        except Exception:
            pass
        fail_manifest(review_ms, "candidate_build", str(error))
        print(f"candidate build failed: {error}", file=sys.stderr)
        raise
