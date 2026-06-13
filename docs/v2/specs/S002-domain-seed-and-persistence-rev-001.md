# S002 — Domain seed (Stage 2 as code) + persistence rev-001

**Phase**: 0/1 boundary · **Appetite**: 4 days · **Depends on**: S001 · **ADRs / notes**: [0003](../adr/0003-postgres-only-timescale-cache.md), [0004](../adr/0004-persistence-layer-shape.md), [0018](../adr/0018-temporal-hot-cold-model-and-warmup.md), [0021](../adr/0021-intraday-persistence-and-bar-close-detection.md), [0023](../adr/0023-test-strategy-deterministic-replay.md)

## Goal

After this slice the Stage-2 domain model exists **as code** — the binding replay seams from ADR-0023 (events, clock, event-source port) plus the first migration — proven against an ephemeral TimescaleDB. This is deliberately minimal; S003's live-feed contact is allowed to revise it.

## Interface

*(Package names follow the locked convention — `rs_` + member dir name; see S001.)*

- `rs_domain.events` — `BarClosed` (frozen, plain-data, JSON-serializable) carrying a concrete `event_time` and `sequence`; total ordering key `(event_time, sequence)`.
- `rs_domain.ports` — `Clock` protocol (`now() -> datetime`); `EventSource` protocol (async iterator of domain events) — the seam live and recorded adapters will share.
- `rs_domain.models` — Pydantic: `Bar`, `Alert` (minimum needed by S003; grows later).
- `rs_persistence` — SQLAlchemy 2.x async models + Alembic env; **rev-001** creates `bars` (hypertable via raw DDL in the revision), `alerts`, `outbox`.
- CI gate — grep/lint forbidding ambient time (`datetime.now(`, `datetime.utcnow(`, `time.time(`) in `rs_domain`/detection code outside the `Clock` implementation; `time-machine` added as the test-only escape hatch for incidental time (per ADR-0023).

## Non-goals

No service code, no detection logic, no cache table, no options/popularity/earnings tables (later revisions), no repository layer beyond what the round-trip test needs.

## Acceptance criteria

- [ ] `alembic upgrade head` succeeds against a fresh TimescaleDB container; `bars` is a hypertable (verified via `timescaledb_information.hypertables`).
- [ ] Round-trip: insert a `Bar` via the async session, read it back, equality on all fields.
- [ ] `BarClosed` JSON round-trip is lossless; a shuffled list of events sorts deterministically by `(event_time, sequence)`.
- [ ] Domain models reject malformed input (one negative test per model — the ADR-0004 validation boundary exists from day 1).
- [ ] Clock-discipline grep gate runs in CI and fails on a planted violation (prove the gate, then remove the plant).

## Verification

```bash
docker run -d --rm -p 5499:5432 -e POSTGRES_PASSWORD=t timescale/timescaledb:latest-pg16
.venv/bin/pytest libs/ -q                            # domain + persistence tests (incl. ephemeral-DB round-trip)
scripts/check_clock_discipline.sh                    # grep gate; exits non-zero on ambient time in domain code
```

## Open questions

Exact hypertable `chunk_time_interval` for `bars` — recommendation: start with Timescale's default; ADR-0022 tuning comes with the compression slice.

## Decisions surfaced

Filled during implementation.
