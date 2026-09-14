# Greed research agent

The research agent turns the current paper journal, rotated journal summaries,
strategy configuration, and compact live status into a structured experiment
proposal. It never has Binance credentials, never places orders, never edits the
active strategy, and never deploys code.

## Safety boundary

- `greed paper` remains the only process that owns exchange execution.
- `greed research` reads paper artifacts and writes under
  `data/research/agent/`.
- The Zhipu request contains no exchange credential or trading authority.
- Proposed changes must pass deterministic replay, at least three walk-forward
  windows, and a paper canary before a later promotion workflow may use them.
- A review may explicitly return `no_change`; running the timer does not imply a
  strategy mutation.

## Local dry run

```bash
./target/release/greed research \
  --config config/demo.toml \
  --dry-run
```

This writes the exact sanitized model input to
`data/research/agent/latest-input.json` without making a network request.

## Server setup

Create `/etc/greed-research.env` with mode `0600`:

```text
ZHIPU_API_KEY=replace_me
GREED_RESEARCH_API_BASE=https://open.bigmodel.cn/api/coding/paas/v4
GREED_RESEARCH_MODEL=glm-5.2
```

The Coding Plan quota is separate from BigModel's general pay-as-you-go balance.
Its key must use the dedicated `/api/coding/paas/v4` base URL; the general
`/api/paas/v4` endpoint can authenticate the same key but returns provider error
`1113` when no general-billing balance is available.

Then build greed and install the timer:

```bash
sudo PROJECT_DIR=/opt/greed /opt/greed/deploy/install-research-agent.sh
```

The timer runs at 00:05, 06:05, 12:05, and 18:05 Asia/Shanghai. Results are
written atomically as timestamped files and as
`data/research/agent/latest-review.json`.
API, response-parsing, and schema-validation failures are written to
`data/research/agent/latest-error.json` and emitted to journald with their full
error chain. Requests use Zhipu JSON mode and results are also validated
locally.

Research output is bounded to a small number of concise observations,
hypotheses and experiments. If Zhipu still returns truncated or malformed JSON,
the agent regenerates one compact response from the original input. A second
malformed response is recorded as deferred instead of failing the systemd job,
and the candidate builder never reuses an older review for that run.

## Candidate and promotion loop

When a review returns `run_experiments` with usable data, systemd starts a
separate candidate builder. It makes one additional Zhipu request and permits
exact replacements only in the three strategy recipe files. Execution,
accounting, portfolio risk, configuration, epoch, deployment code and secrets
are not editable. Added host, process, file-system, network, environment, FFI,
include-macro and unsafe capabilities are rejected before compilation.

The builder uses a detached worktree at the current `origin/main`, then runs
Rust formatting, all workspace tests, a release build and configuration
validation. A passing candidate is committed and pushed as
`research/auto-<review timestamp>`. The temporary worktree and build artifacts
are removed afterwards to keep disk use bounded. A public, patch-free summary
is written to `data/research/agent/latest-candidate.json`.

The dashboard shows the candidate and all gates. Guests can only inspect it.
An authenticated Operator can request promotion only after new entries are
paused and the account has no open positions. The root-owned promotion path
unit rechecks the candidate ID, commit, base commit, remote branch and every
gate. It deploys the candidate without changing epoch or runtime data, waits
for backend health, and only then fast-forwards `main`. A failed deployment
keeps or restores the previous binary and records the failure in
`data/research/agent/latest-promotion.json`.

The model never receives exchange credentials and cannot call the promotion
unit directly. Human approval is the only bridge from a pushed candidate
branch to the running paper strategy.

When Zhipu reports Coding Plan error `1308`, the run exits successfully with a
`deferred` result instead of leaving the oneshot service failed. The timer does
not retry in a tight loop; it waits for its next six-hourly run, by which time
the five-hour rolling allowance has normally recovered.

## Bounded storage

The live decision journal and raw research journal are each bounded to four
64 MiB files by `config/demo.toml`. Durable trade/PnL history and execution
recovery state are separate and are not removed by those limits. High-volume
diagnostics are sampled: complete graph/data-health snapshots every 30 seconds,
profit-guard observations every five seconds, and execution latency every
minute. Trade, exit, rejection, protection, operator and accounting events are
still recorded immediately.

Use `deploy/prune-paper-data.sh` without arguments to preview reclaimable files.
Pass `--apply` only after reviewing the exact list. The script never removes the
current journal, trade history, status, execution state, or research-agent
reviews.
