# ADR-0003: Postgres-only infrastructure + TimescaleDB + UNLOGGED cache

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

v2 needs time-series storage (bars, options snapshots, popularity history), hot-read caching (quotes/movers/popularity served to the bot and Cowork at bursty concurrency), cross-service signaling, and durable event records. The default reach would be Postgres + Redis (cache + pub/sub) + maybe a TSDB. Each added piece of stateful infrastructure is another thing to deploy, back up, monitor, and reason about — and this is a personal-scale, single-operator project.

## Decision

**Postgres is the only stateful infrastructure. No Redis.** Postgres covers every need via native features:

- **TimescaleDB extension** for time-series tables. Hypertables created as raw DDL inside Alembic revisions; SQLAlchemy queries treat them as normal tables. Compression handles the options-snapshot volume.
- **UNLOGGED cache table** for hot reads (quote / movers / popularity / fundamentals) — no WAL overhead, crash-disposable (acceptable: it's a cache).
- **LISTEN/NOTIFY** for cross-service signaling (see ADR-0005) — replaces Redis pub/sub.
- **Transactional outbox** table for durable events (see ADR-0005) — replaces a message broker.

## Consequences

- One thing to deploy, back up, and monitor. Matches project scale.
- Everything composes cleanly: Timescale is an extension, LISTEN/NOTIFY and UNLOGGED tables are Postgres-native, the outbox is just another table (an append-only event ledger — no relay worker in v2; ADR-0005).
- Cost: Postgres LISTEN/NOTIFY has an 8KB payload limit and no consumer-group semantics — fine for "an alert was written, go look," not for high-fanout real-time streams. Documented upgrade path to Redis Streams in ADR-0005.
- UNLOGGED cache loses contents on crash/restart — acceptable because it's recomputable from source-of-truth tables.

## Trigger conditions for revisit

- Real-time dashboards or multiple external real-time consumers → introduce Redis Streams alongside Postgres (ADR-0005).
- Cache rebuild-on-restart becomes a latency problem → consider a logged cache table or external cache.

## See also

- [architecture-brief.md § Persistence](../architecture-brief.md#persistence)
- [ADR-0004: Persistence layer shape](./0004-persistence-layer-shape.md)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
