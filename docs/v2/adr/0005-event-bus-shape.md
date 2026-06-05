# ADR-0005: Event bus shape (Bus 1 in-process + Bus 2 LISTEN/NOTIFY + outbox)

**Status**: Accepted (**amended** 2026-06-05)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05
**Decision-maker**: John Wolf

> **✎ AMENDED 2026-06-05** (ledger L19, + L3/L16). **Bus 1 is a deterministic, synchronous, *ordered fan-out* — not a pub/sub framework.** Per `BarClosed`, `ingest` runs a fixed ordered handler list (persist → update cache → detect); required by L16's deterministic-replay mandate (a concurrent pub/sub would have to be crippled to pass replay tests) and justified by the small fixed subscriber set, while staying reversible to a registry if subscribers ever churn. **Options detection is a standalone linear pipeline** (5-min REST scan → `decide()` → write `alert`+`outbox`), not a Bus-1 subscriber. The **event-source port** (L16) is the *ingress* seam upstream of the fan-out (live aggregator in prod; recorded adapter for warm-up/backtest/test). **Bus 2 (LISTEN/NOTIFY + outbox) is unchanged** and is the single cross-process growth layer for all future consumers (serve push/dashboards, new clients, the post-v2 auto-trader + detection worker). Illustrative event names below are dated: `MinuteBar`→`BarClosed`; `FilingIngested`/SEC is out of v2 (ledger L8/L10).

## Context

Two distinct messaging needs exist in v2: (1) inside `ingest`, source data and derived signals must fan out to several subscribers (persister, cache updater, detectors, alert writer); (2) across services, `ingest` must tell `bot` "an alert was written, go post it." These have very different characteristics — (1) is high-frequency, in-process, low-latency; (2) is low-frequency, cross-process, durability-sensitive. One mechanism for both would over- or under-serve one of them. Postgres-only (ADR-0003) rules out Redis/Kafka.

## Decision

Two buses behind one abstraction:

- **Bus 1 — in-process pubsub inside `ingest`.** Carries source events (`QuoteTick`, `MinuteBar`, `MentionsIngested`, `FilingIngested`, …) and derived events (`SurgeDetected`, `MomentumConfirmed`, …). Subscribers: persister, cache_updater, detectors, alert_writer. Events carry data only — no freshness metadata (see ADR-0017).
- **Bus 2 — Postgres LISTEN/NOTIFY for cross-service signaling.** `ingest` writes an `alerts` row in a transaction, then `NOTIFY`s a channel. `bot` LISTENs and posts. On reconnect, `bot` reconciles via `SELECT ... WHERE posted_at IS NULL`.
- **Transactional outbox** for durable event records, so a crash between "write alert" and "NOTIFY" is recoverable (the relay re-emits unprocessed outbox rows).
- An **`EventBus` abstraction** so the implementation can swap without changing subscribers.

## Consequences

- Each need is served by the right mechanism; Bus 1 stays fast, Bus 2 stays durable.
- The abstraction + outbox means the LISTEN/NOTIFY → Redis Streams migration is localized (~half a day) when it's needed.
- Reconciliation makes the bot robust to missed NOTIFYs (8KB payload limit, dropped connections).
- Cost: LISTEN/NOTIFY has no consumer groups or replay; it's a wake-up signal, not a stream. Acceptable because the `alerts` table is the source of truth and reconciliation covers gaps.

## Trigger conditions for revisit

- Real-time dashboards or other external real-time consumers arrive → migrate Bus 2 to Redis Streams (consumer groups, replay, fanout). The abstraction layer + outbox make this bounded.

## See also

- [architecture-brief.md § Event flow](../architecture-brief.md#event-flow)
- [ADR-0008: Detection in ingest](./0008-detection-in-ingest.md)
- [ADR-0017: Failure mode](./0017-failure-mode-trimmed-a.md)
