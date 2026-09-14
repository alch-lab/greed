# Greed research agent

The research agent turns the current paper journal, rotated journal summaries,
strategy configuration, and compact live status into a structured experiment
proposal. It never has Binance credentials, never places orders, never edits the
active strategy, and never deploys code.

## Safety boundary

- `greed paper` remains the only process that owns exchange execution.
- `greed research` reads paper artifacts and writes under
  `data/research/agent/`.
- The OpenAI request uses `store: false` and contains no exchange credential.
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
OPENAI_API_KEY=replace_me
GREED_RESEARCH_MODEL=gpt-6-astra
```

Then build greed and install the timer:

```bash
sudo PROJECT_DIR=/opt/greed /opt/greed/deploy/install-research-agent.sh
```

The timer runs at 00:05, 06:05, 12:05, and 18:05 Asia/Shanghai. Results are
written atomically as timestamped files and as
`data/research/agent/latest-review.json`.
