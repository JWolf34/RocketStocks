# ADR-0018: Temporal model — hot/cold split, "memory is a projection," startup warm-up

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

## Context

The initial v2 plan leaned "store everything point-in-time and stream everything." Pressure-testing against the product (swing-trade horizon, options-flow-first, reliability as #1 goal) and Schwab's limits (per-service stream budget, REST ~100/min, option chains not backfillable) showed that detection *cadence* and persistence *granularity* are independent axes that were being conflated. We need an explicit temporal model that makes restart-safety and backtest-replay fall out for free rather than being bolted on.

## Decision

**Hot path / cold path split.** A **hot live path** processes high-frequency data and holds little; a **cold research path** keeps coarse, durable, queryable history. Detection cadence and persistence granularity are decided independently.

**Invariant — "memory is a projection" (hard rule):** in-memory detector state is always a *derived projection* of persisted data + a bounded REST reseed — **never a source of truth.** No detector may hold un-rebuildable in-memory state. Restart-safety and backtest-replay are then the *same operation* over the durable log.

**Persist at *strategy* granularity, not *alert* granularity.** Latency is a product/UX property, not a backtest input.

**Startup warm-up (state recovery).** On `ingest` start, detectors are disabled and the stream unsubscribed until `warmup()` completes: (1) reconstruct the subscription universe from Postgres; (2) rebuild rolling windows by replaying recent hypertable rows (== a short backtest replay); (3) REST-reseed the current snapshot; (4) load dedup/lifecycle state from Postgres — then subscribe and enable detectors. A warm-up delay (seconds–minutes) is accepted in exchange for never having a silent blind detection window; fine given the swing horizon. (If the UNLOGGED cache is reintroduced — ADR-0003 as amended — repopulating it slots back in as a warm-up step, healing a Postgres-crash truncate.)

## Consequences

- Restart, warm-up, and backtest all ride one replay code path (ADR-0023) — the strongest reliability lever in the design.
- When the UNLOGGED cache is reintroduced (ADR-0003 as amended), it can be crash-disposable precisely because warm-up rebuilds the hot path from the cold path; warm-up never reads the cache.
- Cost: a bounded startup delay before live detection; acceptable at swing horizon.

## Change log

- 2026-06-05 — locked.
- 2026-07-03 — aligned with the ADR-0003 amendment: the repopulate-the-cache warm-up step is removed while the UNLOGGED cache is deferred; it returns with the cache. No change to the temporal model or the memory-is-a-projection invariant.

## See also

- [ADR-0003: Postgres + Timescale + cache](./0003-postgres-only-timescale-cache.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
- [ADR-0023: Test strategy — deterministic replay](./0023-test-strategy-deterministic-replay.md)
