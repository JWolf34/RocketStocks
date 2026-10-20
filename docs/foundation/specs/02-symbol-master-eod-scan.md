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

- Scope (D-A, John 2026-10-20): active spans of class common, ADR or ETF/ETN, plus every surge-event ticker and SPY, so slice 1's base rate keeps its bars; `surge_bars` is folded into this job.
  This narrows issue #49's "every active listing": the other classes have no reader, and because `instrument_class` is stored for every span, widening the scope later is a config change plus a seed run.
- Source (D-B, John 2026-10-20): one Schwab price-history call per symbol, the same call the seed uses, starting one stored session back so each update overlaps a bar already held (the split check, D-H).
  About 8,000 calls, about 80 minutes at 100 per minute, after the 17:30 ET close.
  Batch quotes (about 20 calls) were rejected: a quote's day fields are not the official candle, they carry no history to detect a split, and a missed night needs a history call anyway.
- Deep seed (D-C, John 2026-10-20): not a separate job.
  `daily_bars` fetches a symbol with no bars from 1990-01-01 (Schwab serves about 30 years; the calendar starts in 1990), so a new span or relisted ticker seeds on its first night and an interrupted seed resumes on the next run.
  The first night makes the same ~8,000 calls as any night, with larger responses (about 25M rows); the job runs at 17:30 ET with a 4-hour timeout, clear of the 22:00 CT scan.
  Some seed rows land in already-compressed chunks (compression after 180 days); the build tests that path against the real hypertable settings.
- Splits (D-H, John 2026-10-20): Schwab has no corporate-actions feed, so the history is the only split signal.
  A one-call probe (NVDA around its 2024-06-10 10:1 split) first establishes whether daily history is split- and dividend-adjusted; if split-adjusted, new rows are stamped `adjusted = true` and a migration restamps slice 1's rows.
  Each update's overlapping session is compared with the stored close; beyond a tolerance (0.5% working figure, raised above typical dividend moves if the probe shows dividend adjustment) the symbol's whole history is refetched and replaced in one transaction, logged and counted (`daily_bar_reseeds`).
  This is the only path that rewrites a bar; it also absorbs Schwab data corrections.
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
- Restart mid-scan (D-D, John 2026-10-20): resume the same cycle.
  The scan is a window job: the daemon starts it at launch, at startup inside an open window (launch to deadline, cycle unfinished), and when the token gate clears inside one; this is a generic daemon extension.
  Each underlying's rows (payload or tombstone) commit in one transaction, so a resumed scan recomputes the roster with the same query and skips names that already have a row; `symbol_master` never runs inside the window, and the cooling tail derives from earlier cycles, so the roster is the same.
  A separate non-Schwab `eod_scan_close` step runs at 03:00 CT, and at startup for the latest cycle past its deadline, writing a `skipped` tombstone for every roster name with no row; it is idempotent, so the deadline holds even when the scan is not running.
  The scan starts nothing new after the deadline; a name in flight finishes and is written.
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

**Jobs** added to `services/ingest/src/rs_ingest/jobs.py`: `symbol_master` (nightly, before the scan, not a Schwab job), `daily_bars` (replaces `surge_bars`; seeds and updates, Schwab), `eod_scan` (window job from 22:00 CT on session days, Schwab), `eod_scan_close` (03:00 CT and at startup, not a Schwab job).

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
