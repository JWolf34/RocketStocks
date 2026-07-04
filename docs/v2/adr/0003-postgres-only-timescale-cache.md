# ADR-0003: Postgres-only infrastructure + TimescaleDB + UNLOGGED cache

**Status**: Accepted (amended 2026-07-03 — UNLOGGED cache deferred out of v2)
**Date locked**: 2026-05-27 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

## Context

v2 needs time-series storage (bars, options snapshots, popularity history), hot-read caching (quotes/movers/popularity served to the bot and Cowork at bursty concurrency), cross-service signaling, and durable event records. The default reach would be Postgres + Redis (cache + pub/sub) + maybe a TSDB. Each added piece of stateful infrastructure is another thing to deploy, back up, monitor, and reason about — and this is a personal-scale, single-operator project.

## Decision

**Postgres is the only stateful infrastructure. No Redis.** Postgres covers every need via native features:

- **TimescaleDB extension** for time-series tables. Hypertables created as raw DDL inside Alembic revisions; SQLAlchemy queries treat them as normal tables. Compression handles the options-snapshot volume.
- **UNLOGGED cache table — deferred out of v2** *(amended 2026-07-03)*. Designed as the hot-read surface (quote / movers / popularity / fundamentals; no WAL overhead, crash-disposable), but after ADR-0024 fixed serve's current-data endpoints to Schwab read-through and the bot data-access rule routed all bot market data through `serve`, **no v2 consumer remains** — it would be written at quote rate and read by nothing. The design is retained on paper (UNLOGGED, warm-up-rebuilt — ADR-0018; `stale_since` freshness — ADR-0017) and reintroduced behind the trigger conditions below; reintroduction is additive and replay-neutral (a `cache_updater` Bus-1 handler — ADR-0005; cache writes never feed detection).
- **LISTEN/NOTIFY** for cross-service signaling (see ADR-0005) — replaces Redis pub/sub.
- **Transactional outbox** table for durable events (see ADR-0005) — replaces a message broker.

## Consequences

- One thing to deploy, back up, and monitor. Matches project scale.
- Everything composes cleanly: Timescale is an extension, LISTEN/NOTIFY and UNLOGGED tables are Postgres-native, the outbox is just another table (an append-only event ledger — no relay worker in v2; ADR-0005).
- Cost: Postgres LISTEN/NOTIFY has an 8KB payload limit and no consumer-group semantics — fine for "an alert was written, go look," not for high-fanout real-time streams. Documented upgrade path to Redis Streams in ADR-0005.
- When reintroduced, the UNLOGGED cache loses contents on crash/restart — acceptable because it's recomputable from source-of-truth tables (warm-up rebuilds it, ADR-0018).

## Trigger conditions for revisit

- Real-time dashboards or multiple external real-time consumers → introduce Redis Streams alongside Postgres (ADR-0005).
- **Reintroduce the UNLOGGED cache** when any of: (a) portfolio/watchlist read-through REST cost or latency bites (Phase 3 / burn-in); (b) the post-v2 auto-trader build starts (its blueprint polls the cache — ADR-0027); (c) serving last-known quotes through a Schwab REST outage becomes a real requirement (the `STALE_OK` surface — ADR-0017).

## Change log

- 2026-05-27 — locked (Postgres-only, Timescale, UNLOGGED cache, LISTEN/NOTIFY, outbox).
- 2026-07-03 — amended: **UNLOGGED cache deferred out of v2** — the zero-consumer review (post ADR-0024 read-model + bot data-access rule) found nothing reads it; demoted to a designed, trigger-conditioned reintroduction. Decision by John Wolf.

## See also

- [architecture-brief.md § Persistence](../architecture-brief.md#persistence)
- [ADR-0004: Persistence layer shape](./0004-persistence-layer-shape.md)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
