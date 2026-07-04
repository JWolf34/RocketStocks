# S003 — `ingest` skeleton: one ticker, stream → bar → detect → alert → NOTIFY

**Phase**: 1 · **Appetite**: 5 days · **Depends on**: S002 · **ADRs / notes**: [0005](../adr/0005-event-bus-shape.md), [0008](../adr/0008-detection-in-ingest.md), [0021](../adr/0021-intraday-persistence-and-bar-close-detection.md), [0023](../adr/0023-test-strategy-deterministic-replay.md)

## Goal

After this slice a long-running `ingest` daemon streams one hardcoded ticker (SPY) from Schwab `level_one_equity` live, builds 5-minute bars, and on each `BarClosed` runs the ADR-0005 ordered fan-out ending in a deliberately dumb detector that writes an `alerts`+`outbox` row in one transaction and `NOTIFY`s. This is the data model's first contact with the real feed — surprises here are cheap; report them, don't absorb them silently.

## Interface

- `services/ingest` — runnable daemon (`python -m rs_ingest`), config via pydantic-settings.
- **Bus 1** — a fixed, ordered, synchronous handler list per `BarClosed`: `persister → detector`. Plain code, not a pub/sub framework (binding per ADR-0005/0023). The list stays extensible; a `cache_updater` handler joins on the UNLOGGED cache's reintroduction (ADR-0003/0005).
- **Detector port** — `decide(window) -> AlertDecision | None`, called behind an error boundary (a raising detector is logged + counted, never kills capture — ADR-0008). Skeleton detector: fixed volume threshold, tuned low enough to actually fire.
- **Live `EventSource` adapter** — wraps the Schwab stream behind the S002 port; assigns `(event_time, sequence)`.
- **Recorded fixture** — the session's normalized domain events written as JSONL (the recorded-adapter input for S007); raw payload samples kept alongside under `docs/v2/specs/fixtures/`.
- `NOTIFY` channel name + alert-row shape as created in S002 rev-001 (extend via rev-002 if the live feed forces a shape change — that's a finding, record it).

## Non-goals

No universe manager (one hardcoded ticker), no options scans, no warm-up, no real detectors, no bot/serve changes, no reconnect *reconciliation* (reconnect itself must work; gap healing is Phase 2's warm-up job).

## Acceptance criteria

- [ ] Runs one full live market session on the dev box without operator intervention; disconnects are survived via reconnect and visible in logs/metrics.
- [ ] Bars persisted for the session (RTH ≈ 78 five-minute bars; gaps allowed only where the stream was down — and then visible in logs).
- [ ] Persisted bars sanity-check against Schwab REST price-history for the same day within a stated tolerance (stream-built vs. official bars need not be identical; the comparison + tolerance is part of the deliverable).
- [ ] ≥1 alert written by the threshold detector; `outbox` row count equals `alerts` row count; alert write + outbox write + `NOTIFY` share one transaction (transactional NOTIFY, delivered at commit — ADR-0005).
- [ ] A separate `psql` session holding `LISTEN` receives the notification for each alert.
- [ ] A planted always-raises detector run shows capture continuing (error boundary proven), then is removed.
- [ ] Session fixture recorded as JSONL; it parses back into domain events that re-sort to the identical order.

## Verification

```bash
python -m rs_ingest                                  # run across a market session (dev box)
psql "$DB" -c "SELECT count(*) FROM stream_bars_5m WHERE symbol='SPY' AND bucket::date = current_date"
psql "$DB" -c "SELECT (SELECT count(*) FROM alerts) = (SELECT count(*) FROM outbox)"
psql "$DB" -c "LISTEN alerts;"                       # in a second session; observe NOTIFY on next alert
.venv/bin/pytest libs/ services/ingest -q            # unit tests incl. fixture round-trip
python -m rs_ingest.tools.compare_bars --date today  # stream-built vs REST bars, prints max deviation
```

## Open questions

To resolve at spec review (each carries a recommendation):

1. **Schwab client.** Port v1's proven auth/token handling into a `libs/` source adapter vs. adopt `schwab-py`. **Recommendation:** decide after the Phase-0 streaming spike; default to porting v1's token flow (it works and the re-auth tooling exists) with the stream layer written fresh against the spike's payload samples.
2. **v2 token storage** location/format (must serve the ADR-0016 expiry-notification path later). The resolution must also state the sharing semantics with `serve` (ADR-0024): the single-writer rule (`ingest` refreshes) and how `serve` detects/handles a stale token read (e.g. re-read on 401).
3. **Bar bucketing rule** — which timestamp closes a bar (event time vs. wall clock at close), plus the session boundaries (RTH-only per ADR-0021: day's first bar 09:30–09:35 ET; half-day handling). Session boundaries come from the `TradingCalendar` port (S002; `pandas-market-calendars`, XNYS) — note that unscheduled halts and surprise late opens are feed-observable only (ADR-0017 staleness covers them; the calendar cannot know them). May be finalized during implementation rather than at review, but it must come out of this slice explicitly documented, since detection, warm-up, and replay all hang off it.

## Decisions surfaced

1. **Skeleton detector end-state — decided 2026-06-12, pre-recorded:** the threshold detector built here is **promoted, not deleted**. Once the options-flow detector ships (Phase 2), it converts to a permanent **metrics-only canary** on `BarClosed` — same detection port, increments `canary_decisions_total`, stops writing alert rows — so the streaming→detection seam stays production-exercised (see roadmap Phase 2). Build it cleanly enough to survive that promotion.
