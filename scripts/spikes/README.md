# Phase-0 Schwab spikes — Monday runbook

Throwaway code (roadmap pre-flight). The outputs are two **findings docs**
written under `data/spike-findings/` (gitignored, local-only, never committed). **All raw
recordings stay local** under the same gitignored `data/` tree (stream JSONL in
`data/spike-recordings/`, chain snapshots in `data/chain-snapshots/`) - never committed.
Measured facts worth keeping are summarized by hand in
`docs/reference/schwab-facts.md`.
These scripts are deleted once
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

`caffeinate -is` keeps the Mac awake for the full session — a lid-close or idle
sleep would silently kill both recordings. Full day is the plan (machine stays up
until ~15:02 CT); both scripts stop themselves at session close (calendar-derived).

```bash
caffeinate -is .venv/bin/python scripts/spikes/stream_spike.py   # SPY level-1 + ~20 near-ATM option contracts, all session
caffeinate -is .venv/bin/python scripts/spikes/chain_spike.py    # SPY chain every 1 min, 9 others every 5 min
```

Watch for: reconnect lines in the stream log (survivable, counted), 429 lines in
the chain log (rate-limit evidence — wanted for ADR-0020), snapshot sizes (~KB
numbers each cycle).

## After the close

```bash
.venv/bin/python scripts/spikes/stream_compare.py   # stream-built vs REST candles → comparison md
.venv/bin/python scripts/spikes/chain_diff.py       # delta analysis → findings md
.venv/bin/python scripts/spikes/burst_probe.py      # ramp quotes to first 429 → ADR-0020 evidence
```

The burst probe runs AFTER the close only — never during RTH — so any rate-limit
fallout can't hit the live ingest daemon's market-hours calls (the script refuses to run
while the session is open). Passive 429s during the day are unlikely: the chain
cadence is ~120 requests/hour against a documented ~120/minute limit, which is
exactly why the probe exists.

Then summarize anything durable in `docs/reference/schwab-facts.md`.
Findings and raw data (`data/spike-findings/`,
`data/spike-recordings/`, `data/chain-snapshots/`) live on this machine
only - never in git (vendor-data rule).

## Dry runs (no market, no credentials)

```bash
.venv/bin/python scripts/spikes/stream_spike.py --dry-run --out /tmp/dry.jsonl
.venv/bin/python scripts/spikes/stream_compare.py --jsonl /tmp/dry.jsonl --dry-run
.venv/bin/python scripts/spikes/chain_spike.py --dry-run
.venv/bin/python scripts/spikes/chain_diff.py --dry-run
```
