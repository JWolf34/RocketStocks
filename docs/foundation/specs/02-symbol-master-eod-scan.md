# Slice 2 - Symbol master and options EOD scan

Status: draft for review.
Decisions marked **(open)** need John's call; each carries a recommendation.

## Goal

Every night, v2 knows which US listings exist and which are optionable, stores a daily bar for every listing in scope, and captures the full option chain of every optionable underlying, so the liquid band (slice 4), the Options Report (slice 6) and the 250-session clock for an options-flow study can start.

## Scope

In: the symbol master with listing spans and an optionable flag, daily bars for every in-scope listing (a deep seed plus a nightly update), the options EOD full scan with its roster, retry and split ladder, deadline and tombstones, the ops alarms these need, a disk alarm, and a slice-2 runbook.
Out: see the end of this page.

## Interface

**Symbol master** (carries universe-note decisions D1, D2, D4 and D5 from the archive tag).

- Sources, each behind an `rs_sources` adapter with synthetic fixtures: Nasdaq Trader `nasdaqlisted.txt` and `otherlisted.txt` for listings, Nasdaq Trader `options.txt` and the Cboe symbol directory for optionability.
  OCC is not used because its download sits behind a bot wall.
- One row per listing span: a recycled ticker gets a new span, and a span is never reopened.
- A rename is a delist plus an add; no stitching.
- `instrument_class` is computed at ingest (common, ADR, ETF/ETN, preferred, warrant, right, unit, test, other) and filtered at read; financial status is stored, never filtered.
- `optionable` = in `options.txt` or in the Cboe directory, joined to active spans, with symbol-convention normalisation (`BRK.B` and `BRKB`).
- A failed fetch keeps yesterday's spans and raises staleness; a file that would delist more than a threshold of active spans aborts the diff and alarms instead.
- Pre-go-live delistings are out of scope (universe-note D3); the survivorship caveat stays.

**Daily bars for every listing.**

- Scope **(open, D-A)**: active spans of class common, ADR or ETF/ETN, plus every surge-event ticker (so slice 1's base rate keeps its bars).
  Recommendation: yes; `surge_bars` is folded into this job.
- Source **(open, D-B)**: one Schwab price-history call per symbol, fetching only the missing sessions.
  Recommendation: per-symbol price history, the same call the seed uses, over a batch-quote bar built from day OHLC (about 20 calls a night instead of about 8,000, but a different source than the history it extends).
- Deep seed: a symbol with no bars is seeded to Schwab's maximum daily depth (about 20 years) once, including every new span; it runs as a low-priority job that yields the limiter to the nightly scan **(open, D-C)**.
  Recommendation: seed at most N symbols a night outside the scan window until the backlog is empty (about 8,000 calls, so a few nights), rather than one long first-night run.
- Every bar stays rejected unless `exchange_calendars` lists its session, and an epoch-zero timestamp is rejected.
- A symbol Schwab cannot price is recorded in `unpriced_symbol` and retried on the slice-1 cadence.

**Options EOD full scan** (carries the decisions on the superseded options EOD scan map, #6-#11).

- Roster: active ∩ optionable ∩ not test, plus a cooling tail of names that left the roster within the last K sessions, frozen at train start and scanned in lexicographic order.
  A roster that shrinks past a threshold alarms but proceeds.
- One chain per underlying, every expiry and strike (no DTE bound), into `options_chain_snapshot` with `dataset = 'eod_full'`.
- Launch at 22:00 America/Chicago on each session day; hard deadline 03:00 America/Chicago.
  `cycle_id` is the nominal launch instant, so a restart lands in the same cycle.
- Ladder per underlying: fetch; on a transport-class failure retry once; then split into three expiry buckets (with an open-ended tail) and retry each bucket once; then a `failed` tombstone.
- Statuses gain `rejected` (a 400 symbol rejection) and `empty` (a 200 with no contracts), giving seven: `ok`, `partial`, `failed`, `stale`, `skipped`, `rejected`, `empty`.
- Stale guard: a chain whose median non-null `quote_time` is before that session's open is stored whole as `stale`.
- At the deadline every unscanned roster name gets a `skipped` tombstone; there is no catch-up the next morning.
- Restart mid-scan **(open, D-D)**: recommendation: resume the same cycle, skipping names that already have a row for it, until the deadline.
- Dead token mid-scan **(open, D-E)**: recommendation: the scan pauses with the token gate and resumes if the token is renewed before the deadline; anything unscanned at the deadline is `skipped`.
- Divergence cross-check, alarms only and never auto-mutation: a 400 rejection on a flagged name alarms on night 1; a flagged name `empty` for 3 sessions alarms; a live chain on an unflagged name is logged.
- Reader rule, written into `rs_query`: a non-`ok` row means no observation, never zero.

**Rate budget** **(open, D-F)**.
Recommendation: keep the one shared Schwab limiter at 100 calls per minute, the scan's pacing comes from it (dropping S008's own 0.625s constant), and the nightly order is symbol master (no Schwab), daily-bar update after the 17:30 ET close, deep seed, then the scan with priority from 22:00 CT.
At about 6,500 chain requests the scan takes about 65-70 minutes, well inside the deadline.

**Storage and disk.**
Chains compress after 3 days, with no retention policy.
A `disk_free_bytes` gauge and an ops notice below a threshold **(open, D-G)**.
Recommendation: measure the first recorded night's compressed size before choosing the threshold, and record the per-night growth in this spec.

**Tables** (migrations from 0012).

| Table | Shape |
|---|---|
| `symbols` | `symbol`, `first_seen`, `last_seen`, `status` (`active`, `delisted`), `delisted_at`, `exchange`, `security_name`, `instrument_class`, `financial_status`, `etf`, `test_issue`, `optionable`, `source_as_of`; key (`symbol`, `first_seen`); at most one active span per symbol |
| `options_chain_snapshot` | unchanged columns; the `source_status` enum gains `rejected` and `empty` |
| `daily_bar` | unchanged; now covers every in-scope listing |

Grants: ingest writes `symbols`; bot, serve and research read it.

**Jobs** added to `services/ingest/src/rs_ingest/jobs.py`: `symbol_master` (nightly, before the scan, not a Schwab job), `daily_bars` (replaces `surge_bars`), `daily_bar_seed` (low priority, Schwab), `eod_scan` (22:00 CT session days, Schwab).

## Acceptance

| # | Criterion | Check | Done item |
|---|---|---|---|
| 1 | Gates pass | the slice-1 gate set | CI green |
| 2 | Spans are right | Synthetic files: a recycled ticker opens a second span; a missing symbol closes its span; a truncated file aborts and alarms | CI green |
| 3 | Optionable union | Synthetic `options.txt` and Cboe files: the flag is their union after normalisation | CI green |
| 4 | Bars are clean | A non-session date and an epoch-zero timestamp are rejected; the update fetches only missing sessions | CI green |
| 5 | Ladder and statuses | A fake chain source reaches each of the seven statuses, the split rung and the deadline `skipped` tail | CI green |
| 6 | Restart-deterministic | Kill the scan mid-train against a fake clock and source: the same `cycle_id`, no underlying scanned twice | CI green |
| 7 | A real night | A recorded nightly run finishes before 03:00 CT with tombstones for failures; no non-session bar stored; counts per status recorded | Live run |
| 8 | Token expiry | Expire the token before or during the scan: the scan pauses, one ops notice, recovery before the deadline resumes it | Rehearsal |
| 9 | Ingest restart | Kill ingest mid-scan: it resumes the same cycle with no duplicate underlyings | Rehearsal |
| 10 | Backup restore | Restore a snapshot that includes chain data to a scratch VM; row counts match | Rehearsal |

## Out of scope

The liquid band and its priority and eviction (slice 4); the curated 5-minute chain scan and the stream (slice 4); any options signal or verdict; the Options Report (slice 6); 1-minute backfill; historical delistings and survivorship-free history; OI settle-time analysis.

## Links

[Data collection scope](https://github.com/JWolf34/RocketStocks/issues/49) - [Options EOD scan map](https://github.com/JWolf34/RocketStocks/issues/6) and its decisions #7-#11 - archived design note `docs/v2/universe-manager.md` D1-D5 and `docs/v2/schwab-rest-budget.md` at tag `v2-plan-archive-2026-10` - [architecture.md](../architecture.md)
