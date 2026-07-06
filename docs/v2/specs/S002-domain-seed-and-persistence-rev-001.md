# S002 — Domain seed (Stage 2 as code) + persistence rev-001

**Phase**: 0 · **Appetite**: 4 days · **Depends on**: S001 · **ADRs / notes**: [0003](../adr/0003-postgres-only-timescale-cache.md), [0004](../adr/0004-persistence-layer-shape.md), [0018](../adr/0018-temporal-hot-cold-model-and-warmup.md), [0021](../adr/0021-intraday-persistence-and-bar-close-detection.md), [0023](../adr/0023-test-strategy-deterministic-replay.md)

## Goal

After this slice the Stage-2 domain model exists **as code** — the binding replay seams from ADR-0023 (events, clock, event-source port) plus the first migration — proven against an ephemeral TimescaleDB. This is deliberately minimal; S003's live-feed contact is allowed to revise it.

## Interface

*(Package names follow the locked convention — `rs_` + member dir name; see S001.)*

- `rs_domain.events` — `BarClosed` (frozen, plain-data, JSON-serializable) carrying a concrete `event_time` and `sequence`; total ordering key `(event_time, sequence)`. **Ordering contract:** `sequence` is monotonic within a single `event_time` and derived from the durable log on warm-up — never a resettable in-process counter (a mid-day restart must not let a same-`event_time` pair sort differently on replay than it did live). The key governs Bus-1 domain events only; scan-pipeline inputs are ordered by their hypertable rows.
- `rs_domain.ports` — `Clock` protocol (`now() -> datetime`); `EventSource` protocol (async iterator of domain events) — the seam live and recorded adapters will share; `TradingCalendar` protocol (is-session, session open/close incl. early closes, next/previous session — the XNYS market calendar seam). Implementation = a `pandas-market-calendars` adapter living in a pandas-bearing member, **not** in `rs_domain` (decided 2026-07-03; researched vs. exchange-calendars — pmc wins on v1 familiarity, mirrors exchange_calendars, `schedule()` covers half-days).
- `rs_domain.models` — Pydantic: `Bar`, `Alert` (minimum needed by S003; grows later).
- `rs_persistence` — SQLAlchemy 2.x async models + Alembic env; **rev-001** creates `stream_bars_5m` (the detection log — ADR-0021/0022; hypertable via raw DDL in the revision), `alerts`, `outbox`. The research-history family (`bars_1m` + continuous aggregates) is a Phase-2 revision, not rev-001.
- CI gate — grep/lint forbidding ambient time (`datetime.now(`, `datetime.utcnow(`, `time.time(`) in `rs_domain`/detection code outside the `Clock` implementation; `time-machine` added as the test-only escape hatch for incidental time (per ADR-0023).

## Non-goals

No service code, no detection logic, no cache table, no options/popularity/earnings tables (later revisions), no repository layer beyond what the round-trip test needs.

## Acceptance criteria

- [ ] `alembic upgrade head` succeeds against a fresh TimescaleDB container; `stream_bars_5m` is a hypertable (verified via `timescaledb_information.hypertables`). *(Test written; runs in CI's service container and locally once Docker Desktop is installed.)*
- [ ] Round-trip: insert a `Bar` via the async session, read it back, equality on all fields (plus float bit-exactness). *(Same gating as above.)*
- [x] `BarClosed` JSON round-trip is lossless; a shuffled list of events sorts deterministically by `(event_time, sequence)` — including same-`event_time` tie-breaking.
- [x] Domain models reject malformed input (one negative test per model — the ADR-0004 validation boundary exists from day 1; naive/non-UTC datetimes are also rejected per the timezone convention).
- [x] Clock-discipline grep gate runs in CI and fails on a planted violation (proven 2026-07-05: planted `datetime.now()` → exit 1 naming file:line; removed → exit 0).

## Verification

```bash
docker run -d --rm -p 5499:5432 -e POSTGRES_PASSWORD=t timescale/timescaledb:latest-pg16
.venv/bin/pytest libs/ -q            # DB tests target RS_TEST_DB_URL (default = the container above); they skip visibly when unreachable
scripts/check_clock_discipline.sh    # grep gate; exits non-zero on ambient time in domain code
RS_DB_URL=... .venv/bin/python -m rs_migrate current
```

## Open questions

*(Resolved at spec review 2026-07-05:)* Exact hypertable `chunk_time_interval` for `stream_bars_5m` — **resolved: Timescale's default** (7-day chunks); ADR-0022 tuning comes with the compression slice.

## Decisions surfaced

Spec review + grilling, 2026-07-05 (all John-approved):

- **Test-DB strategy:** John installs Docker Desktop on the dev Mac (no Docker/Homebrew existed) so the spec's `docker run` works locally; CI *also* runs a `timescale/timescaledb:latest-pg16` service container. Tests read `RS_TEST_DB_URL` (default = the spec container at `:5499`), create a throwaway database per session, migrate it to head, drop it after — and **skip visibly** when no DB is reachable.
- **Driver: psycopg3** (`postgresql+psycopg`) over asyncpg — one driver family repo-wide while legacy and v2 coexist; Alembic runs plain-sync through the same driver (no async env.py boilerplate); asyncpg's bulk-fetch edge is irrelevant at detection-log volumes. Revisit is a one-line URL change behind SQLAlchemy.
- **Calendar adapter home: `rs_sources`** — an exchange calendar is an external world-fact adapter; `rs_analysis` stays pure math.
- **Prices are float64 / DOUBLE PRECISION** in `Bar` and `stream_bars_5m`: the wire delivers JSON floats and DOUBLE PRECISION round-trips them bit-exactly, which ADR-0023's replay property depends on. **NUMERIC/Decimal is explicitly reserved for future accounting-grade tables** (auto-trader-era orders/positions/fills) — do not inherit floats there by inertia.
- **Timezone convention (binding, enforced):** every domain datetime is tz-aware UTC — `UtcDatetime` validators reject naive and non-UTC values at construction; all DB time columns are `timestamptz`; bar buckets are labeled by UTC start; ET exists only inside the TradingCalendar adapter and future presentation code; env `TZ` is inert by construction.
- **`alerts.posted_at` ships in rev-001** — ADR-0005 pins it by name as the bot's Bus-2 cursor (contract, not speculation). Held out as speculative: retry/poison counters, dedup keys, sweep indexes, Discord artifacts — each waits for its consumer slice.
- Implementation notes: Alembic config is built **programmatically** (`rs_persistence.migration_config(db_url)` — no alembic.ini) with migrations shipped inside the package, so it works identically for editable installs, wheels, and Docker images; `rs_migrate` is a thin `upgrade|downgrade|current` wrapper reading `RS_DB_URL`; the clock-gate script carries an extensible scope list (add `services/ingest` src when S003 lands).

Code review of the slice diff (2026-07-05) surfaced and fixed in-slice:

- **Session-timezone hazard closed at two layers:** psycopg3 decodes `timestamptz` into the connection's `TimeZone`, so a non-UTC server (e.g. v1's `TZ=America/Chicago` habit) would have made every `BarRepository.get` raise on its own data. `build_engine` now pins `-c timezone=UTC` per connection, and the repository additionally normalizes the bucket to UTC on read (ADR-0004: coerce at the boundary; the validator stays as backstop).
- **`UtcDatetime` normalizes accepted values to `timezone.utc`:** a zero-offset variable zone (Europe/London in winter) passed the offset check but kept its DST-shifting tzinfo — later wall-clock arithmetic could drift off the true instant. Rejection semantics unchanged; accepted values can no longer smuggle a variable zone.
- **Clock gate hardened:** a missing/renamed scope directory now exits 2 with a message instead of reporting `OK` over nothing (the `|| true` swallowed grep's error status); proven both ways.
- **CI skip-drift closed:** when `RS_TEST_DB_URL` is explicitly set (as CI does) an unreachable DB **fails** the suite; the skip path exists only for the unset-local case — port/env drift can't turn acceptance tests into silent green skips.
- **Calendar adapter:** `next/previous_session` raise a clean `ValueError` instead of bare `IndexError` on an empty window (window widened to 30 days); single-day schedules are memoized (is_session → open → close costs one `schedule()` build, not three); `PandasMarketCalendar` exported from `rs_sources` with the member docstring updated.
- **Known sharp edge, deliberately left:** `Alert` is frozen but carries a dict payload, so it is **not hashable** (Bar/BarClosed are) — S003's dedup design decides whether Alert needs hash semantics; don't discover this in production.
