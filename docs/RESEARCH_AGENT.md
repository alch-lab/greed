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
GREED_RESEARCH_API_BASE=https://open.bigmodel.cn/api/paas/v4
GREED_RESEARCH_MODEL=glm-5.2
```

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
