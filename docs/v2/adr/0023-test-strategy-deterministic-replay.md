# ADR-0023: Test strategy — deterministic replay as the primary reliability mechanism

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

## Context

Reliability is v2's #1 goal, yet no test-strategy decision existed. The risk in this system is concentrated in the *temporal detection pipeline* (rolling windows, eviction, ordering, dedup, confirm-in-place, outbox writes) — exactly where unit tests are weakest. The temporal model (ADR-0018) already makes warm-up and backtest the *same replay over the durable log*, so deterministic replay-as-test is nearly free, and it directly addresses v1's worst flakiness (event-loop pollution, missing golden fixtures, a stale hardcoded date).

## Decision

**Deterministic replay is the primary reliability mechanism for the detection path** — golden-master / characterization over the *real* pipeline, asserting **semantic outcomes** (alert type / contract / fields), never raw embed bytes. The core property under test: *state rebuilt by replay == state that would exist had `ingest` run live the whole time.*

**Tiers:** replay (primary) > unit (pure `decide()` + the Pydantic validation boundary) > integration (real ephemeral Postgres — mocking psycopg under Timescale + LISTEN/NOTIFY + outbox tests nothing real). Property-based and a live-sandbox smoke test are optional complements, not the backbone.

**Binding seams (these shape the Stage-2 domain model):**

1. **Injected clock** — no ambient `datetime.now()` in domain code; inject at the engine/service layer (domain events carry concrete timestamps); a thin `Clock` protocol, not a framework; enforce via lint/grep. The replay engine *advances* the clock per event, so time is a runtime-controlled input — which is why injection beats test-only monkeypatching. For incidental time reads outside the clock path, use **time-machine** (C-layer, async-safe), not freezegun or hand-mocked datetimes.
2. **Event-source port** — interchangeable live + recorded adapters (live aggregator in prod; recorded fixtures / hypertable tail for replay).
3. **Serializable plain-data domain events** (`BarClosed`, …).
4. **Deterministic total ordering** `(event_time, sequence)`.

## Consequences

- Replay reuses the warm-up (ADR-0018) + backtest (ADR-0020) machinery — three users, one seam; a replay test exercises the real persist-and-replay path that runs on every deploy, and makes change-heavy detection safe to refactor.
- The one ongoing tax is clock discipline; broad fixture coverage is a deferrable dial.
- Hazard: golden-fixture rot (mindless re-blessing; narrow fixtures → false confidence) — mitigated by semantic assertions and few, meaningful fixtures.

## See also

- [ADR-0018: Temporal model + warm-up](./0018-temporal-hot-cold-model-and-warmup.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
- [ADR-0020: Three datasets + backtest constraints](./0020-three-datasets-and-backtest-constraints.md)
