# ADR-0003: Postgres-only infrastructure + TimescaleDB

**Status**: Accepted (amended 2026-07-03, 2026-10-19)
**Date locked**: 2026-05-27 · **Amended**: 2026-07-03 · 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** the UNLOGGED-cache and transactional-outbox lines are deleted (no reader for the cache, outbox cut in [#52](https://github.com/JWolf34/RocketStocks/issues/52)); no-Redis, Timescale and LISTEN/NOTIFY stand.

## Context

v2 needs time-series storage (bars, options snapshots, popularity history), cross-service signaling, and durable records. The default reach would be Postgres + Redis (cache + pub/sub) + maybe a TSDB. Each added piece of stateful infrastructure is another thing to deploy, back up, monitor, and reason about - and this is a personal-scale, single-operator project.

## Decision

**Postgres is the only stateful infrastructure. No Redis.** Postgres covers every need via native features:

- **TimescaleDB extension** for time-series tables. Hypertables created as raw DDL inside Alembic revisions; SQLAlchemy queries treat them as normal tables. Compression handles the options-snapshot volume.
- **LISTEN/NOTIFY** for cross-service signaling (see ADR-0005) — replaces Redis pub/sub.

## Consequences

- One thing to deploy, back up, and monitor. Matches project scale.
- Everything composes cleanly: Timescale is an extension and LISTEN/NOTIFY is Postgres-native.
- Cost: Postgres LISTEN/NOTIFY has an 8KB payload limit and no consumer-group semantics - fine for "an alert was written, go look," not for high-fanout real-time streams.

## Trigger conditions for revisit

- Real-time dashboards or multiple external real-time consumers → introduce Redis Streams alongside Postgres (ADR-0005).

## Change log

- 2026-05-27 — locked (Postgres-only, Timescale, UNLOGGED cache, LISTEN/NOTIFY, outbox).
- 2026-07-03 — amended: **UNLOGGED cache deferred out of v2** — the zero-consumer review (post ADR-0024 read-model + bot data-access rule) found nothing reads it; demoted to a designed, trigger-conditioned reintroduction. Decision by John Wolf.
- 2026-10-19 - amended (foundation re-plan): UNLOGGED-cache design and outbox lines deleted; see the note above.

## See also

- [architecture.md § Storage](../../foundation/architecture.md#storage)
- [ADR-0004: Persistence layer shape](./0004-persistence-layer-shape.md)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
