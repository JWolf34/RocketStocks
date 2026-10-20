# ADR-0023: Test strategy - injected clock, golden fixtures, real Postgres

**Status**: Accepted (amended 2026-10-19)
**Date locked**: 2026-06-05 · **Amended**: 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** keeps the injected clock, the clock-discipline gate, golden fixtures and real-Postgres tests; drops the "replay is the primary mechanism" mandate and the event-source/total-order mandates (replay of every input is a post-v2 follow-up per the charter; [#50](https://github.com/JWolf34/RocketStocks/issues/50) trims replay tooling).

## Context

Reliability is v2's #1 goal, yet no test-strategy decision existed. The risk in this system is concentrated in the *temporal detection pipeline* (rolling windows, ordering, dedup, alert writes) - exactly where unit tests are weakest. Injected time and golden fixtures directly address v1's worst flakiness (event-loop pollution, missing golden fixtures, a stale hardcoded date).

## Decision

**Reliability tests assert semantic outcomes over the real pipeline, never raw embed bytes.** *(Amended 2026-10-19: deterministic replay is no longer the primary mechanism, and the event-source port and `(event_time, sequence)` total-ordering mandates are dropped. Replay of every input remains a post-v2 follow-up. The golden-fixture pattern is kept as one tool among several.)*

**Tiers:** unit (pure detector decisions + the Pydantic validation boundary) and integration against a **real ephemeral Postgres** (mocking psycopg under Timescale + LISTEN/NOTIFY tests nothing real) are the backbone. **Golden fixtures** - a small synthetic input run through the real pipeline, asserting the committed semantic decisions - cover the temporal detection path. Fixtures are synthetic or gitignored-local; vendor data is never committed.

**Binding seam:**

1. **Injected clock** - no ambient `datetime.now()` in domain code; inject at the engine/service layer (domain events carry concrete timestamps); a thin `Clock` protocol, not a framework; enforced by `scripts/check_clock_discipline.sh`. Time is a runtime-controlled input, which is why injection beats test-only monkeypatching. For incidental time reads outside the clock path, use **time-machine** (C-layer, async-safe), not freezegun or hand-mocked datetimes.

## Consequences

- A golden-fixture test exercises the real persist-and-detect path and makes change-heavy detection safe to refactor.
- The one ongoing tax is clock discipline; broad fixture coverage is a deferrable dial.
- Hazard: golden-fixture rot (mindless re-blessing; narrow fixtures give false confidence) - mitigated by semantic assertions, hand-derived expectations and few, meaningful fixtures.

## Change log

- 2026-06-05 - locked (replay as the primary mechanism).
- 2026-10-19 - amended (foundation re-plan): replay-primary and the event-source/total-order mandates dropped; see the note above.

## See also

- [architecture.md § Data flow](../../foundation/architecture.md#data-flow)
