# ADR-0005: Event bus shape (Bus 1 ordered fan-out + Bus 2 LISTEN/NOTIFY + outbox)

**Status**: Accepted (amended 2026-06-05, 2026-07-03)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05 · 2026-07-03
**Decision-maker**: John Wolf

## Context

Two distinct messaging needs exist in v2: (1) inside `ingest`, source data and derived signals must fan out to several subscribers (persister, detectors); (2) across services, `ingest` must tell `bot` "an alert was written, go post it." These have very different characteristics — (1) is high-frequency, in-process, low-latency; (2) is low-frequency, cross-process, durability-sensitive. One mechanism for both would over- or under-serve one of them. Postgres-only (ADR-0003) rules out Redis/Kafka.

## Decision

Two buses behind one abstraction:

- **Bus 1 — a deterministic, synchronous, *ordered fan-out* inside `ingest`. Not a pub/sub framework.** Per `BarClosed`, a fixed ordered handler list runs: `persister → detector`. (A `cache_updater` handler joins the list if/when the UNLOGGED cache is reintroduced — ADR-0003 as amended; additive and replay-neutral, since cache writes never feed detection.) Determinism is required by the replay test strategy (ADR-0023 — a concurrent pub/sub would have to be crippled to pass replay tests) and justified by the small fixed subscriber set; it stays reversible to a subscriber registry if subscribers ever churn. Events carry data only — no freshness metadata (see ADR-0017).
- The **event-source port** (ADR-0023) is the *ingress* seam upstream of the fan-out: a live aggregator adapter in production; a recorded adapter for warm-up, backtest, and tests.
- **Options detection is a standalone linear pipeline** (5-min REST scan → `decide()` → write `alert`+`outbox`), not a Bus-1 subscriber (ADR-0021).
- **Bus 2 — Postgres LISTEN/NOTIFY for cross-service signaling.** `ingest` writes the `alerts` row, the `outbox` row, and issues `NOTIFY` **all inside one transaction** — Postgres holds a transactional `NOTIFY` and delivers it at commit, so the write and the signal are atomic and no "crash between write and NOTIFY" window exists. `bot` LISTENs and posts. Bus 2 is the single cross-process growth layer for all future consumers (serve push/dashboards, new clients, the post-v2 auto-trader + detection worker).
- **Bus-2 consumer contract** (what makes new consumers additive): a Bus-2 consumer = `LISTEN` on the channel + a **consumer-owned cursor/completion marker** + an **idempotent catch-up sweep** run on every reconnect *and* on a periodic interval while connected. NOTIFY is the latency optimization; the sweep is the correctness mechanism (it covers ephemeral-NOTIFY loss, consumer downtime, and mid-work crashes). The **bot is the reference implementation**: its cursor is `alerts.posted_at`, its sweep is `SELECT ... WHERE posted_at IS NULL`. A new consumer brings its own cursor table — zero changes to `ingest`.
- **Transactional outbox — an append-only durable event ledger. No relay ships in v2.** The classic outbox relay exists to bridge a database and an *external* broker (the dual-write problem); with Postgres as the broker and transactional NOTIFY, there is nothing for a relay to do that the consumer sweep doesn't already cover. Outbox rows carry no processed-marking (delivery state belongs to each consumer's cursor); the ledger is the audit trail and the attachment point for future consumers. A producer-side relay is introduced **only with the external-broker migration** below.
- An **`EventBus` abstraction** so the implementation can swap without changing subscribers.

## Consequences

- Each need is served by the right mechanism; Bus 1 stays fast and exactly replayable, Bus 2 stays durable.
- The abstraction + outbox means the LISTEN/NOTIFY → Redis Streams migration is localized (~half a day) when it's needed; the relay is built then, as part of that migration, because that is when the dual-write problem first exists.
- The consumer contract makes the bot robust to missed NOTIFYs (8KB payload limit, dropped connections, quiet losses while connected) and makes consumer #2 (post-v2 auto-trader) an additive change.
- Worst-case alert latency when a NOTIFY is lost = one sweep interval — bounded, boring, and visible; no component whose silent stall could stop announcements entirely.
- Cost: LISTEN/NOTIFY has no consumer groups or replay; it's a wake-up signal, not a stream. Acceptable because the `alerts` table is the source of truth and each consumer's sweep covers gaps.
- Cost: a permanently unpostable alert is retried by every sweep with no backoff; a retry-count column + alarm (poison-row handling) is a bot-phase spec concern, not a bus concern.

## Trigger conditions for revisit

- Real-time dashboards or other external real-time consumers arrive → migrate Bus 2 to Redis Streams (consumer groups, replay, fanout) **and build the producer-side relay** as part of that migration. The abstraction layer + outbox make this bounded.

## Change log

- 2026-05-27 — locked. Bus 1 was originally described as in-process pubsub carrying source events (`QuoteTick`, `MinuteBar`, `MentionsIngested`, `FilingIngested`) and derived events (`SurgeDetected`, `MomentumConfirmed`) to subscribers including an `alert_writer`.
- 2026-06-05 — amended: Bus 1 pinned to a deterministic ordered fan-out (replay mandate, ADR-0023); options detection moved to a standalone linear pipeline; event names updated (`MinuteBar` → `BarClosed`); SEC/news events out of v2 (ADR-0025); derived-event names retired with the v1 alert types (ADR-0030). Bus 2 unchanged.
- 2026-07-03 — editorial: amendment banner folded into the body; no decision change.
- 2026-07-03 — amended (outbox semantics): transactional NOTIFY pinned (write + signal atomic at commit); **no relay in v2** — the outbox is an append-only event ledger with no processed-marking; Bus-2 consumer contract defined (LISTEN + consumer-owned cursor + idempotent sweep, bot as reference implementation); relay construction tied to the external-broker migration trigger.
- 2026-07-03 — amended: `cache_updater` removed from the Bus-1 handler list (UNLOGGED cache deferred out of v2 — ADR-0003 amendment); rejoins on the cache's reintroduction.

## See also

- [architecture-brief.md § Event flow](../architecture-brief.md#event-flow)
- [ADR-0008: Detection in ingest](./0008-detection-in-ingest.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
- [ADR-0023: Test strategy — deterministic replay](./0023-test-strategy-deterministic-replay.md)
- [ADR-0017: Failure mode](./0017-failure-mode-trimmed-a.md)
