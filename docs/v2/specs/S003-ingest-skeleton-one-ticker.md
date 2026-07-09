# S003 — `ingest` skeleton: one ticker, stream → bar → detect → alert → NOTIFY

**Phase**: 1 · **Appetite**: 5 days · **Depends on**: S002 · **ADRs / notes**: [0005](../adr/0005-event-bus-shape.md), [0008](../adr/0008-detection-in-ingest.md), [0021](../adr/0021-intraday-persistence-and-bar-close-detection.md), [0023](../adr/0023-test-strategy-deterministic-replay.md)

## Goal

After this slice a long-running `ingest` daemon streams one hardcoded ticker (SPY) from Schwab `level_one_equity` live, builds 5-minute bars, and on each `BarClosed` runs the ADR-0005 ordered fan-out ending in a deliberately dumb detector that writes an `alerts`+`outbox` row in one transaction and `NOTIFY`s. This is the data model's first contact with the real feed — surprises here are cheap; report them, don't absorb them silently.

## Interface

- `services/ingest` — runnable daemon (`python -m rs_ingest`), config via pydantic-settings.
- **Bus 1** — a fixed, ordered, synchronous handler list per `BarClosed`: `persister → detector`. Plain code, not a pub/sub framework (binding per ADR-0005/0023). The list stays extensible; a `cache_updater` handler joins on the UNLOGGED cache's reintroduction (ADR-0003/0005).
- **Detector port** — `decide(window) -> Alert | None` in `rs_domain.ports`, called behind an error boundary (a raising detector is logged + counted, never kills capture — ADR-0008). `rs_domain.Alert` *is* the decision type — no separate `AlertDecision` model. `window` is the pipeline-owned session-scoped bar window (see pinned decision 5). Skeleton detector: fixed volume threshold, tuned low enough to actually fire.
- **Live `EventSource` adapter** — wraps the Schwab stream behind the S002 port; assigns `(event_time, sequence)`.
- **Recorded fixture** — the session's normalized domain events written as JSONL (the recorded-adapter input for S007); raw payload samples kept alongside under `docs/v2/specs/fixtures/`.
- `NOTIFY` channel name is **`alerts`**; alert-row shape as created in S002 rev-001 (extend via migration if the live feed forces a shape change — that's a finding, record it). Wire shapes pinned in decision 6 below.
- **Daemon lifecycle (this slice): single-session-per-process.** Non-session day → log + exit 0. Started pre-open → connect immediately (premarket ticks warm the cumulative-volume baseline, making the 09:30 bar buildable). Session close → final bar flushed, fixture finalized, disconnect, exit 0. No auto-restart or multi-day loop here — but `run_session()` is shaped so an outer session loop can wrap it without redesign, because **by v2 cutover `ingest` must run unattended across sessions** (multi-session loop + ADR-0016 token-expiry notification are later slices).

## Non-goals

No universe manager (one hardcoded ticker), no options scans, no warm-up, no real detectors, no bot/serve changes, no reconnect *reconciliation* (reconnect itself must work; gap healing is Phase 2's warm-up job).

## Acceptance criteria

- [ ] Runs one full live market session on the dev box without operator intervention; disconnects are survived via reconnect and visible in logs/metrics.
- [ ] Bars persisted for the session (RTH ≈ 78 five-minute bars; gaps allowed only where the stream was down — and then visible in logs).
- [ ] Persisted bars sanity-check against Schwab REST price-history for the same day within a stated tolerance. **Tolerance (seeded from the 2026-07-06 spike comparison, [fixtures/streaming/findings-2026-07-06.md](fixtures/streaming/findings-2026-07-06.md)): max per-field price deviation ≤ 0.25** (observed ex-partial worst 0.11 on SPY ≈ 1.5 bps; 0.25 ≈ 2× worst, absorbs one bad print). **Volume is reported, not gated** — stream-built volume runs systematically ~+9–15% above REST (consolidated tape); the comparison prints it, the nightly gauge (ADR-0021) measures it.
- [ ] ≥1 alert written by the threshold detector; `outbox` row count equals `alerts` row count; alert write + outbox write + `NOTIFY` share one transaction (transactional NOTIFY, delivered at commit — ADR-0005).
- [ ] A separate `psql` session holding `LISTEN` receives the notification for each alert.
- [ ] A planted always-raises detector run shows capture continuing (error boundary proven), then is removed.
- [ ] Session fixture recorded as JSONL; it parses back into domain events that re-sort to the identical order.

## Verification

```bash
.venv/bin/pytest libs/ services/ingest -q   # unit + DB: aggregator, pipeline/boundary, fixture round-trip, compare logic
.venv/bin/ruff check libs services tools
scripts/check_clock_discipline.sh           # no ambient time in ingest src (ADR-0023)
# Live-session acceptance (≈78 bars, alert/outbox parity, LISTEN witness, compare_bars, fixture recording,
# error-boundary proof) is the executable "Acceptance-run operations (B8 runbook)" section below.
```

## Open questions

To resolve at spec review (each carries a recommendation):

1. **Schwab client — RESOLVED 2026-07-06: adopt `schwab-py`** (decision: John, after the Phase-0 streaming spike). Evidence from the 2026-07-06 spike session: schwab-py 1.5.1 ran OAuth login, token auto-refresh, REST (quotes, chains, price history), the `LEVELONE_EQUITIES` stream, and a concurrent `LEVELONE_OPTIONS` leg (20 contracts) on one streamer connection for a full session with zero library-attributable failures; reconnect/re-subscribe worked under test. The pre-spike default (porting v1's token flow) is dropped; v1's re-auth tooling patterns still inform the ADR-0016 expiry-notification path. v2 wraps the client behind the S002 event-source port, so the dependency stays swappable. Spike surprises the adapter must respect: Schwab streams only *changed* fields per message (handlers must tolerate any field subset); unbounded index-ETF chain requests 502 (bound by expiry window); REST candles are split-adjusted, not dividend-adjusted, no basis parameter (ADR-0025).
2. **v2 token storage — RESOLVED 2026-07-06: Postgres single-row store, `ingest` is the single writer** (decision: John, at spec review).
   - Alembic **rev-002** in `rs_persistence`: `schwab_tokens` (`id = 1` PK, `token_data` JSONB, `updated_at`). Repository `SchwabTokenStore` in `rs_persistence` (v1 pattern: `legacy` `schwab_token_store.py`). schwab-py wiring via `client_from_access_functions` (custom read/write funcs) in `rs_ingest`.
   - **Bootstrap**: one-shot `python -m rs_ingest.tools.login`. Primary path `--from-file data/schwab-spike-token.json` — a one-time import of the Phase-0 spike token into the DB row (no browser dance); the browser OAuth flow is the fallback and the long-term recovery path. After seeding, `ingest` uses the DB row exclusively — no runtime file dependency. The spike file is deleted when Phase-0 chain capture closes; the DB row is then the sole credential.
   - **Sharing semantics** (ADR-0024): `ingest` refreshes and writes; future readers (`serve`) treat the row as read-only and re-read on 401. Least-privilege GRANTs (token row ingest-only) are a named **S004/S006 follow-on** — not built while `ingest` is the only DB user.
   - **Operational fact — 7-day refresh-token expiry**: Schwab refresh tokens hard-expire every 7 days; there is no unattended workaround. On refresh failure the daemon logs loudly and exits. Until the ADR-0016 expiry-notification slice lands, the weekly `python -m rs_ingest.tools.login` re-auth is a documented operator chore.
3. **Bar bucketing rule — RESOLVED 2026-07-06: trade-event-time bucketing with a wall-clock watchdog flush** (decision: John, at spec review).
   - Bucket key: `TRADE_TIME_MILLIS` floored to 5 min; bar = `[T, T+5m)`; `Bar.bucket = T` (UTC).
   - Close triggers: (a) first tick with trade time ≥ `T+5m` — normal path; (b) `Clock`-driven watchdog at `T+5m+grace` (grace 30 s, configurable; deadline tracked per symbol) so a quiet/stalled feed can't hold a bar open; (c) session close from `TradingCalendar` closes the final bar (handles half-days).
   - `BarClosed.event_time` = bucket end instant (`T+5m`); `sequence` = count of same-`event_time` events already in the durable log (effectively 0 for one symbol; rule documented for the multi-symbol future).
   - RTH-only (ADR-0021): ticks with trade time outside `[session_open, session_close)` are dropped and counted; day's first bar is 09:30–09:35 ET. Session boundaries come from the `TradingCalendar` port (S002; `pandas-market-calendars`, XNYS); unscheduled halts and surprise late opens are feed-observable only (ADR-0017).
   - OHLC from `LAST_PRICE` ticks; **bar volume = delta of cumulative `TOTAL_VOLUME`** (last cumulative value in bucket − last before bucket), so volume-only messages are captured. Late/out-of-order ticks for an already-closed bucket are dropped + counted, never reopen a bar.

## Pinned decisions (plan grilling, John, 2026-07-06)

1. **Mid-session startup/restart: discard to boundary.** Ticks for the in-progress bucket are dropped + counted; the first built bar is the next full bucket; the skipped bucket is logged as a gap. No partial bars, no REST backfill (gap healing is Phase-2 warm-up). Empirical support: the spike comparison's only price deviation > 0.11 was its partial first bucket.
2. **Two-layer RTH filter + volume baseline.** The cumulative-volume tracker updates on *every* observed message (including pre-RTH and discarded-bucket ticks); only bar-building is RTH/boundary-gated. A bar is built only if a cumvol observation exists from before its bucket start — **no baseline → no bar** (logged as gap).
3. **Disconnect-scarred buckets still build bars** (the cumvol delta stays exact across gaps; only OHLC can clip) — logged + counted (`bars_built_degraded_total`), no schema flag. Zero-*trade* buckets (connected, quiet) intentionally produce **no bar row** — matches Schwab REST's omission of no-trade candles, keeps `compare_bars` clean. Three-way logging: built / no-trade / gap.
4. **Failure asymmetry (criticality tiering).** The bar log is Tier-1, the reaction path Tier-2 (alerts are a replayable projection — ADR-0023). Persister exception → daemon crashes loud (no retry/buffer machinery in the skeleton); the detector runs only after persist succeeded, so every alert's input bar exists in the log. **Two transactions per `BarClosed`**: the bar insert commits alone; alerts + outbox + NOTIFY share one transaction. The **whole reaction stage** — `decide()` *and* the alert write — sits behind the ADR-0008 boundary: a raising detector or a failed alert write is logged + counted (`detector_errors` / `alerts_dropped`, the drop carrying the full alert payload), capture continues. A systemic DB outage is not masked — the next bar's crash-loud persist fails fast within one bar interval. Why the alert write is tolerant rather than crash-loud: Decisions surfaced #2.
5. **Detector seam.** Pipeline-owned **session-scoped window** (all of today's RTH bars, newest last, cleared at session roll; day-cumulative = sum over window; cross-day context is Phase-2 warm-up state, ADR-0018). Detectors are stateless pure functions. Skeleton detector: level-trigger, no cooldown, `volume >= 500_000` default (≈ p80 of the 2026-07-06 measured SPY bars → ~6–10 fires/day); dedup/cooldown semantics belong to the Phase-2 detection design session.
6. **Bus-2 wire shapes.** NOTIFY channel `alerts`, payload = **alert id only** (NOTIFY is a wake-up hint; the row is the truth). Outbox row: `event_type = "alert_created"` (snake_case vocabulary), payload self-contained (`alert_id` + full alert content) so the ledger stands alone. The bot (S004) consumes the `alerts` table + NOTIFY only — **the outbox has no v2 reader**; it is the durable ledger.
7. **Alert payload rules.** The JSONB payload carries **semantic facts only, never presentation strings**; every queryable dimension is a typed column; a payload field that turns out to need querying is promoted to a column by migration. Skeleton `volume_threshold` payload: `{volume, threshold}`.

## Acceptance-run operations (B8 runbook)

The live acceptance run targets a **dedicated persistent TimescaleDB container** (named volume, port 5498 — separate from the disposable 5499 test container so pytest can never touch acceptance evidence). Operator (John) starts the run pre-open; starting *before* 09:30 ET is the recommended procedure (warms the cumvol baseline → first bar buildable):

```bash
# 1. once — the persistent acceptance DB (separate from the 5499 pytest container)
docker run -d --name rs-acceptance -p 5498:5432 -v rs-acceptance-data:/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=<pw> timescale/timescaledb:latest-pg16
RS_DB_URL=postgresql+psycopg://postgres:<pw>@localhost:5498/postgres .venv/bin/python -m rs_migrate upgrade

# 2. pre-open — run the session; RS_INGEST_FIXTURE_PATH records the BarClosed fixture
#    (gitignored data/; a curated excerpt is committed afterward). RS_DB_URL lives in .env.
RS_INGEST_FIXTURE_PATH=data/session-fixture-$(date +%F).jsonl \
  .venv/bin/python -m rs_ingest 2>&1 | tee data/acceptance-run-$(date +%F).log

# 3. after close — validation (DATE = the session's UTC calendar day, YYYY-MM-DD)
DB=postgresql://postgres:<pw>@localhost:5498/postgres
psql "$DB" -c "SELECT count(*) FROM stream_bars_5m
               WHERE symbol='SPY' AND bucket >= '<DATE> 00:00+00'
                 AND bucket < '<DATE> 00:00+00'::timestamptz + interval '1 day'"      # ≈ 78
psql "$DB" -c "SELECT (SELECT count(*) FROM alerts) = (SELECT count(*) FROM outbox) AS parity"   # t
RS_DB_URL=postgresql+psycopg://postgres:<pw>@localhost:5498/postgres \
  .venv/bin/python -m rs_ingest.tools.compare_bars --date <DATE>   # prices gated ≤ 0.25; volume + coverage reported
wc -l data/session-fixture-$(date +%F).jsonl                       # ≈ bars persisted (round-trip is the unit test)
```

During the session, a second shell holding `psql "$DB" -c "LISTEN alerts;"` (keep it open — e.g. append `\watch 5`) witnesses the transactional `NOTIFY` as each alert commits (criterion 29). The session-end log prints `pipeline counters`; on a clean run confirm `detector_errors: 0` and `alerts_dropped: 0` (no spurious ADR-0008 boundary or Tier-2 drop activity).

**Error-boundary proof (criterion 30) — replay (chosen 2026-07-08).** After the run, replay the recorded fixture through the pipeline with a planted always-raising detector, into a throwaway DB the tool creates and drops — proving the ADR-0008 boundary on real session data with zero risk to the acceptance capture (it is also unit-proven by `test_pipeline` and the B6c/B7 real-Postgres verifications; this is the acceptance belt-and-suspenders):

```bash
.venv/bin/python -m rs_ingest.tools.replay --fixture data/session-fixture-<DATE>.jsonl --plant-raiser \
  --admin-db postgresql+psycopg://postgres:<pw>@localhost:5498/postgres   # replay DB is separate + auto-dropped
# -> "BOUNDARY HELD" (exit 0): bars_persisted == fixture length AND detector_errors == fixture length
#    (every raise swallowed) AND alerts still emitted — capture never stopped for a detector fault.
```

Post-session analysis (interpreting counts / `compare_bars` deviations / coverage gaps, committing the fixture excerpt + findings) is delegated (pinned #13).

## Decisions surfaced

1. **Skeleton detector end-state — decided 2026-06-12, pre-recorded:** the threshold detector built here is **promoted, not deleted**. Once the options-flow detector ships (Phase 2), it converts to a permanent **metrics-only canary** on `BarClosed` — same detection port, increments `canary_decisions_total`, stops writing alert rows — so the streaming→detection seam stays production-exercised (see roadmap Phase 2). Build it cleanly enough to survive that promotion.

2. **Alert-write failure is tolerated, not fatal — decided 2026-07-08 (B6c).** The failure-asymmetry pin (#4) left the alert *write's* failure mode unstated; the first cut inferred crash-loud, by symmetry with the bar write. Reversed on review: capture is the irreplaceable Tier-1 asset and the reaction path is a replayable Tier-2 projection, so a Tier-2 write failure must never stop Tier-1 capture (bulkhead / graceful degradation — Nygard *Release It!*, Google SRE). Crash-loud is a reliability strategy only with a supervisor that restarts to a known-good state, and the skeleton has none (pinned #11), so a tolerable fault must not be fatal. The drop is made loud and auditable — ERROR log with the full payload + an `alerts_dropped` counter — never silent. **v2 forward path:** make the drop recoverable via a dead-letter, and decouple alerting entirely to an out-of-band **outbox / bar-log consumer** (the outbox seam in #6 exists for exactly this), at which point the reaction path's failures are structurally unable to touch capture and the in-process boundary question dissolves.
