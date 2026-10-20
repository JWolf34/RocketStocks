# ADR-0017: Failure mode - fail-loud mutations + freshness stamps at boundaries

**Status**: Accepted (amended 2026-06-12, 2026-10-19; earlier amendments folded into body 2026-07-03)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-12 · 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** the `FreshnessPolicy` enum, `max_age` budget machinery and serve-envelope text are dropped as deferrals with no reader; fail-loud mutations, `fetched_at`/`source_status` stamps and tombstones stay (charter "trusted data", [#52](https://github.com/JWolf34/RocketStocks/issues/52)).

## Context

v2's failure surfaces are heterogeneous: a dropped Schwab stream can serve stale quotes; a failed ApeWisdom poll can skip a cycle harmlessly; but a failed `alerts` or `paper_trades` write must not vanish silently. We need a coherent policy. The question was also whether to propagate freshness metadata on every bus event or only at boundaries. Carrying it on every event was found to be noise - most subscribers don't read it. This matches the service-mesh / SRE pattern (freshness as source-health metrics + per-row columns) rather than event-time watermarks.

## Decision

**Fail loud on mutations; stamp freshness at the boundaries; bus events stay clean.** *(Amended 2026-10-19: the per-dataset `FreshnessPolicy` enum (`STRICT`/`STALE_OK`/`BEST_EFFORT`), the `max_age` budget procedure and the serve-envelope `meta` text are dropped. Revisit them when a reader needs a staleness threshold; the original text is recoverable from git tag `v2-plan-archive-2026-10`.)*

- **Mutations fail-loud is an invariant, not a configurable policy** — silent write failure is the worst class of bug.
- Reads of recoverable data degrade by skipping the cycle and retrying next time; nothing swallows a failure without a log line and a metric.
- Freshness lives at boundaries, **not on bus events** (events carry data only):
  - persistence rows carry `fetched_at` + `source_status` - row provenance with a small enum (`ok`/`partial`/`failed`/`stale`/`skipped`). Failed per-ticker fetches write **tombstone rows** (`source_status='failed'`, no payload), so scan membership stays truthful under failure.
  - Prometheus exposes `source_last_success_seconds{source=...}`.
  - A streamed source gets a stream-level **liveness** alarm ("no tick on any subscribed ticker in N seconds during RTH") rather than a per-row age budget.

## Consequences

- Reads degrade gracefully where data is recoverable; writes never fail silently.
- Subscribers stay simple — they don't handle freshness.
- Cost: freshness is stamped on rows rather than carried once on the event; accepted because it keeps the bus clean and matches who actually consumes freshness.

## Trigger conditions for revisit (→ circuit breakers)

- A single source exceeds N% failure rate over a 24h window for K days running → add per-source circuit breakers (open after N failures in M seconds, half-open probes). ~100 LOC.

## Change log

- 2026-05-27 — locked ("trimmed A": per-source policy enum, freshness at boundaries, bus events clean, mutations fail-loud).
- 2026-06-12 — amended (freshness review): (1) serve envelope splits `meta.as_of` from `meta.fetched_at`; (2) `max_age` budgets added per dataset, with the derivation procedure; (3) grain moved from per-source to per-dataset (source × data-class); (4) `source_status` enum pinned + tombstone rows for failed per-ticker fetches.
- 2026-07-03 — editorial: amendment banner folded into the body; budget-storage wording disambiguated (Postgres config table read by `DatasetFreshness`); no decision change.
- 2026-07-03 — grain examples extended with the nightly 1-min equity backfill and its 7-day recovery bound (ADR-0022 amendment).
- 2026-07-16 - editorial: `source_status` enum extended to `ok`/`partial`/`failed`/`stale`/`skipped` by the S008 spec review - `stale` marks stored rows rejected by the quote-time-regression guard; `skipped` marks overrun-skip tombstones, distinguishable from `failed` fetches (S008, archived). No decision change.
- 2026-07-03 — aligned with the ADR-0003 amendment (UNLOGGED cache deferred out of v2): cache `stale_since` removed from the freshness surfaces and the `STALE_OK` example re-pointed; the equity-stream grain example now cites the liveness budget directly. Semantics return with the cache.
- 2026-10-19 - amended (foundation re-plan): enum, budgets and serve-envelope text dropped; see the note above.

## See also

- [architecture.md § Data flow](../../foundation/architecture.md#data-flow)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [ADR-0015: Observability stack](./0015-observability-stack-personalized.md)
