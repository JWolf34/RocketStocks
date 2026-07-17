# S008 — `options_chain_snapshot` + curated 5-min chain scan

**Phase**: 2 · **Appetite**: 4 days · **Depends on**: S003 · **ADRs / notes**:
[0032](../adr/0032-options-flow-detection-model.md) (D5/D7/D8),
[0020](../adr/0020-three-datasets-and-backtest-constraints.md),
[0017](../adr/0017-failure-mode-trimmed-a.md),
[0030](../adr/0030-detection-rederived-single-alert-class.md) §PIT,
[0008](../adr/0008-detection-in-ingest.md),
[schwab-rest-budget.md](../schwab-rest-budget.md),
[fixtures/chains/findings-2026-07-06.md](fixtures/chains/findings-2026-07-06.md)

## Goal

After this slice the `options_chain_snapshot` hypertable exists and a curated 5-min
REST chain scan writes real point-in-time rows into it every RTH session, unattended —
the forward-only `curated_5m` dataset starts accumulating, right-shaped, and S009/S012
have their table and conventions to build on.

## Interface

- **Migration 0005** — `options_chain_snapshot` hypertable (author on a branch
  containing 0004 — phase2-plan §0). Time dimension `fetched_at`, 1-day chunks;
  compression policy: compress chunks older than 3 days, `segmentby (dataset,
  underlying)`, `orderby (fetched_at)`; **no retention policy** (forward-only dataset).
  Two enums: `dataset` = `curated_5m | eod_full` (ADR-0032 D5); `source_status` =
  `ok | partial | failed | stale | skipped` (extends ADR-0017's three — see Decisions
  surfaced). Rows are immutable/append-only.
- **Columns (binding, 30)** — bound against
  [excerpt-spy-chain-2026-07-06.json](fixtures/chains/excerpt-spy-chain-2026-07-06.json),
  D8's six groups:
  - *identity (5)*: `underlying` text · `option_symbol` text (OCC symbol — the S009
    day-over-day OI-join key) · `expiry` date · `strike` numeric · `put_call` char C/P
  - *provenance (5)*: `dataset` · `cycle_id` timestamptz · `fetched_at` timestamptz ·
    `quote_time` timestamptz null · `source_status`
  - *quote (8)*: `bid` `ask` `last` `mark` numeric · `bid_size` `ask_size` `last_size`
    int · `trade_time` timestamptz null
  - *flow (2)*: `volume` bigint (cumulative day) · `open_interest` bigint
  - *risk (6)*: `iv` `delta` `gamma` `theta` `vega` `rho` real
  - *chain context (4)*: `underlying_price` numeric (from the same response) ·
    `multiplier` smallint · `expiration_type` char · `non_standard` bool

  Consciously excluded (derivable / presentation / constant): option day-OHLC,
  netChange/percentChange/markChange, timeValue/intrinsic/extrinsic/theoretical,
  breakEven, 52-week fields, deliverables list, exchangeName, bidAskSize.
- **`ChainScanner`** in `rs_ingest` — a second asyncio task started/stopped by
  `run_session()` alongside the stream, sharing the one schwab-py client (the token
  row's single-writer constraint), engine, calendar, and metrics port. Own fault
  boundary: a scan-loop exception is logged + counted and the loop resumes at the next
  boundary — it never stops stream capture (ADR-0008 posture; both are Tier-1, failing
  independently). Every request carries a timeout so a hang cannot hold a train open.
- **Cycle model** (Cycle per [CONTEXT.md](../../../CONTEXT.md)):
  - RTH-only, sessions/half-days from `rs_sources.PandasMarketCalendar`; no off-hours
    cycles (ADR-0007's hourly cadence superseded). S009's EOD scan is the overnight
    record.
  - Cycles launch at wall-clock 5-min boundaries ET (aligned with bar buckets);
    **`cycle_id` = the launch boundary as UTC timestamptz**; deterministic scan order
    (lexicographic by underlying).
  - Per-row `fetched_at`/`quote_time` is the event-time rule — never nominal T.
  - **Overrun**: a running train is never aborted; a boundary passing mid-run launches
    nothing, and the skipped nominal T gets one tombstone row per curated name
    (`source_status='skipped'`, no payload) — snapshot presence stays the
    scan-membership record (ADR-0030).
  - **502s**: retry once, then tombstone (`source_status='failed'`, no payload).
  - **Stale guard**: per underlying, the new snapshot's median `quote_time` must not
    regress vs the last accepted snapshot's; on regression the whole snapshot's rows
    are stored with `source_status='stale'` (stored, never dropped — the observed
    event was a silent 200-OK chain-wide cached response; see findings-2026-07-06).
- **Capture width** (ADR-0032 D7): request bounded server-side by expiry only
  (`fromDate`/`toDate`, ≤30 DTE — the bound that avoids the measured SPY/QQQ 502);
  ±10% moneyness window and the row cap applied client-side against the response's own
  `underlyingPrice` before persist. **Row cap: 4,000 rows/name/cycle (config), whole
  expiries nearest-first, nearest expiry always kept** — a pathology guard rail sized
  from measured books to be non-binding (real SPY ±10%/≤30DTE book: ≈2,650 rows —
  2,654 on the pinned 16:00 capture the tests assert; the count varies by capture
  minute; see Decisions surfaced).
- **Config** (pydantic-settings, `RS_INGEST_` prefix): moneyness window (±0.10), row
  cap (4000), request timeout, bootstrap universe = the spike's 10 tickers (static,
  replaced by S010).
- **Metrics** (bare names per ADR-0015; the S012 contract):
  `chain_scan_cycles_total{outcome=completed|skipped}` ·
  `chain_scan_cycle_duration_seconds` (histogram) ·
  `chain_scan_fetches_total{outcome=<source_status values>}` ·
  `chain_scan_retries_total` · `chain_snapshot_rows_written_total{source_status}` ·
  `source_last_success_seconds{source="options_curated_5m"}` (ADR-0017's pinned name;
  the source key equals the dataset value).

## Non-goals

No EOD full scan (S009), no universe management (S010 — static 10-ticker list here),
no `FreshnessPolicy`/`max_age` config table (S012 — this slice only writes the columns
and metrics S012 consumes), no detection or baselines (S015–S017), no bot/serve
changes.

## Acceptance criteria

- [ ] Migration 0005 applies on a TimescaleDB; `options_chain_snapshot` is a
      hypertable with 1-day chunks and the compression policy present.
- [ ] Normalization maps a real captured chain response to exactly the 30 columns;
      in-window/cap filtering reproduces measured counts on the real SPY book (whole
      expiries, nearest-first, cap non-binding at 4,000).
- [ ] Deterministic scan-loop tests (fake clock/transport, no ambient time —
      clock-discipline gate passes): boundary launch, overrun → skip → per-name
      `skipped` tombstones, 502 retry-once → `failed` tombstone, stale-guard
      regression + recovery replayed from the real SPY 17:48/17:49 pair.
- [ ] Committed recorded-cycle fixture (truncated to in-window rows) including that
      real stale pair.
- [ ] One supervised RTH session on the 10-ticker bootstrap list: rows for every
      (cycle × name), row counts consistent with cycle count, zero unexplained
      tombstones, `stale`/`failed` rows only where the log explains them; stream
      capture unaffected throughout.

## Verification

```bash
.venv/bin/pytest libs/persistence services/ingest -q   # unit + DB (Timescale on :5499, visible skip otherwise)
# Measured-book tests (full real SPY/QQQ books through window+cap) are gated on
# the local capture dir data/chain-snapshots/2026-07-06 (gitignored) and skip
# visibly otherwise; the committed fixtures are pre-truncated to in-window rows.
.venv/bin/ruff check libs services tools
scripts/check_clock_discipline.sh                      # no ambient time in scanner code (ADR-0023)
# Live acceptance (bootstrap list, one RTH session), then:
psql "$DB" -c "SELECT cycle_id, source_status, count(*) FROM options_chain_snapshot
               WHERE dataset='curated_5m' GROUP BY 1,2 ORDER BY 1,2"   # gapless membership
```

## Decisions surfaced

- **`source_status` enum extended** to `ok|partial|failed|stale|skipped` — `stale`
  (quote_time-regression guard) and `skipped` (overrun tombstones) are S008 additions;
  ADR-0017 flagged for editorial amendment.
- **Row cap re-sized from fixture evidence (2026-07-16 spec review)**: a 2,000 cap
  with nearest-expiry-first ordering would have cut SPY's 07-17 monthly (445k day
  volume; the two largest builds in the canonical OI fixture) and QQQ's 07-31 (its top
  four builds). Builds concentrate in weeklies/monthlies 1–4 weeks out, so the cap is
  a guard rail (4,000, non-binding on measured books), not a routine selector.
- **Restart-state horizon (7 days)** — stale-guard baselines rebuild at scanner start
  from each name's last accepted snapshot, looking back at most 7 days: older state is
  meaningless to a 5-min regression guard, and the bound keeps the restart query in
  uncompressed chunks.
- **Missing/invalid `underlyingPrice` disables the moneyness window** for that
  response — capture the whole expiry-bounded chain rather than guess a center (the
  row cap still applies).
- **Schwab sentinels stored as NULL** — `-999` greek values and non-positive
  `quoteTimeInLong`/`tradeTimeInLong` epochs map to NULL columns, never sentinel
  values in the dataset.
- **`chain_scan_errors_total`** — an internal seventh metric counting scan-loop
  exceptions (the ADR-0008 "logged + counted" counter), additive to the six-name S012
  contract above.
- **Universe env override** — `RS_INGEST_CHAIN_SCAN_UNIVERSE` (comma-separated)
  overrides the bootstrap list; an empty value disables the scan (stream-only ops
  mode). "Static" here means no universe management (S010's job), not unconfigurable.
