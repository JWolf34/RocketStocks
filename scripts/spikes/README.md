# Phase-0 Schwab spikes — Monday runbook

Throwaway code (roadmap pre-flight). The deliverables are the two **findings docs**
committed under `docs/v2/specs/fixtures/` — inputs to S003's spec review (Schwab-client
question) and the Phase-2 detection design session. **All raw recordings stay local**
under the gitignored `data/` tree (stream JSONL in `data/spike-recordings/`, chain
snapshots in `data/chain-snapshots/`) — never committed. These scripts are deleted once
the Phase-2 session confirms no re-runs are needed.

## One-time setup (before Monday)

1. Put credentials in the repo-root `.env` (gitignored, never committed):
   ```
   SCHWAB_API_KEY=...
   SCHWAB_API_SECRET=...
   ```
2. Fetch the OAuth token (opens a browser; log in once):
   ```bash
   .venv/bin/python scripts/spikes/fetch_token.py
   ```
   Success prints a SPY quote. Token lands at `data/schwab-spike-token.json` (gitignored).

## Monday, ~09:25 ET — start both, in two terminals

```bash
.venv/bin/python scripts/spikes/stream_spike.py    # records SPY level-1 all session
.venv/bin/python scripts/spikes/chain_spike.py     # SPY chain every 1 min, 9 others every 5 min
```

Both stop themselves at session close (calendar-derived). Watch for: reconnect lines
in the stream log (survivable, counted), 429 lines in the chain log (rate-limit
evidence — wanted for ADR-0020), snapshot sizes (~KB numbers each cycle).

## After the close

```bash
.venv/bin/python scripts/spikes/stream_compare.py   # stream-built vs REST candles → comparison md
.venv/bin/python scripts/spikes/chain_diff.py       # delta analysis → findings md
```

Then commit the two findings docs under `docs/v2/specs/fixtures/` on this branch.
Raw data (`data/spike-recordings/`, `data/chain-snapshots/`) lives on this machine
only — never in git.

## Dry runs (no market, no credentials)

```bash
.venv/bin/python scripts/spikes/stream_spike.py --dry-run --out /tmp/dry.jsonl
.venv/bin/python scripts/spikes/stream_compare.py --jsonl /tmp/dry.jsonl --dry-run
.venv/bin/python scripts/spikes/chain_spike.py --dry-run
.venv/bin/python scripts/spikes/chain_diff.py --dry-run
```
