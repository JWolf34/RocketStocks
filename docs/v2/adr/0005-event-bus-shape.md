# ADR-0005: Event bus shape (LISTEN/NOTIFY wake-up + consumer-owned sweep)

**Status**: Accepted (amended 2026-06-05, 2026-07-03, 2026-10-19)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05 · 2026-07-03 · 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** the outbox table, the Bus-1 ordered-fan-out text and the options-pipeline text are cut ([#52](https://github.com/JWolf34/RocketStocks/issues/52)); the NOTIFY plus `posted_at IS NULL` sweep consumer contract stands and is the only bus.

## Context

Across services, `ingest` must tell `bot` "an alert was written, go post it." The signal is low-frequency, cross-process and durability-sensitive. Postgres-only (ADR-0003) rules out Redis/Kafka.

## Decision

One bus, Postgres LISTEN/NOTIFY, behind a small abstraction:

- **Postgres LISTEN/NOTIFY for cross-service signaling.** `ingest` writes the `alerts` row and issues `NOTIFY` **inside the same transaction** - Postgres holds a transactional `NOTIFY` and delivers it at commit, so the write and the signal are atomic and no "crash between write and NOTIFY" window exists. `bot` LISTENs and posts. This is the single cross-process growth layer for any future consumer.
- **Consumer contract** (what makes new consumers additive): a consumer = `LISTEN` on the channel + a **consumer-owned cursor/completion marker** + an **idempotent catch-up sweep** run on every reconnect *and* on a periodic interval while connected. NOTIFY is the latency optimization; the sweep is the correctness mechanism (it covers ephemeral-NOTIFY loss, consumer downtime, and mid-work crashes). The **bot is the reference implementation**: its cursor is `alerts.posted_at`, its sweep is `SELECT ... WHERE posted_at IS NULL`. A new consumer brings its own cursor table - zero changes to `ingest`.
- An **`EventBus` abstraction** so the implementation can swap without changing consumers.

## Consequences

- The `alerts` table is the durable record and NOTIFY is only the wake-up, so delivery stays correct without a second table.
- The abstraction keeps a later LISTEN/NOTIFY → Redis Streams migration localized.
- The consumer contract makes the bot robust to missed NOTIFYs (8KB payload limit, dropped connections, quiet losses while connected) and makes any additional consumer an additive change.
- Worst-case alert latency when a NOTIFY is lost = one sweep interval — bounded, boring, and visible; no component whose silent stall could stop announcements entirely.
- Cost: LISTEN/NOTIFY has no consumer groups or replay; it's a wake-up signal, not a stream. Acceptable because the `alerts` table is the source of truth and each consumer's sweep covers gaps.
- Cost: a permanently unpostable alert is retried by every sweep with no backoff; a retry-count column + alarm (poison-row handling) is a bot-phase spec concern, not a bus concern.

## Trigger conditions for revisit

- Real-time dashboards or other external real-time consumers arrive → migrate to Redis Streams (consumer groups, replay, fanout). The abstraction layer makes this bounded.

## Change log

- 2026-05-27 — locked. Bus 1 was originally described as in-process pubsub carrying source events (`QuoteTick`, `MinuteBar`, `MentionsIngested`, `FilingIngested`) and derived events (`SurgeDetected`, `MomentumConfirmed`) to subscribers including an `alert_writer`.
- 2026-06-05 — amended: Bus 1 pinned to a deterministic ordered fan-out (replay mandate, ADR-0023); options detection moved to a standalone linear pipeline; event names updated (`MinuteBar` → `BarClosed`); SEC/news events out of v2 (ADR-0025); derived-event names retired with the v1 alert types (ADR-0030). Bus 2 unchanged.
- 2026-07-03 — editorial: amendment banner folded into the body; no decision change.
- 2026-07-03 — amended (outbox semantics): transactional NOTIFY pinned (write + signal atomic at commit); **no relay in v2** — the outbox is an append-only event ledger with no processed-marking; Bus-2 consumer contract defined (LISTEN + consumer-owned cursor + idempotent sweep, bot as reference implementation); relay construction tied to the external-broker migration trigger.
- 2026-07-03 — amended: `cache_updater` removed from the Bus-1 handler list (UNLOGGED cache deferred out of v2 — ADR-0003 amendment); rejoins on the cache's reintroduction.
- 2026-10-19 - amended (foundation re-plan): outbox, Bus-1 ordered fan-out, event-source port and options-pipeline text cut; contract kept (see the note above).

## See also

- [architecture.md § Data flow](../../foundation/architecture.md#data-flow)
- [ADR-0008: Detection in ingest](./0008-detection-in-ingest.md)
- [ADR-0023: Test strategy](./0023-test-strategy-deterministic-replay.md)
- [ADR-0017: Failure mode](./0017-failure-mode-trimmed-a.md)
