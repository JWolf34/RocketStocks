# ADR-0017: Failure mode — per-source policy + freshness at boundaries

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

v2's failure surfaces are heterogeneous: a dropped Schwab stream can serve stale quotes; a failed ApeWisdom poll can skip a cycle harmlessly; but a failed `alerts` or `paper_trades` write must not vanish silently. We need a coherent policy. The question was also whether to propagate freshness metadata on every bus event (belt-and-suspenders) or only at boundaries. Carrying `FreshnessInfo` on every event was found to be noise — ~90% of subscribers (persister, detectors, alert writer) don't read it; only the cache updater cares, and it can read source status elsewhere. This matches industry "Pattern 2" (service-mesh / SRE: freshness as source-health metrics + per-row columns + API headers) rather than "Pattern 1" (event-time watermarks, for big-data streaming).

## Decision

**Per-source policy, freshness at the boundaries, bus events clean.**

- Each source adapter declares a `FreshnessPolicy`:
  - `STRICT` — raise on failure (e.g. `paper_trades`/`alerts` writes)
  - `STALE_OK` — return last-known with a flag (e.g. quotes)
  - `BEST_EFFORT` — skip this cycle, retry next (e.g. ApeWisdom mentions)
- Freshness lives at boundaries, **not on bus events**:
  - persistence rows carry `fetched_at` + `source_status`
  - the UNLOGGED cache carries `stale_since`
  - `serve` response envelopes synthesize `meta.fetched_at` / `meta.stale_since`
  - Prometheus exposes `source_last_success_seconds{source=...}`
- **Mutations fail-loud** — silent write failure is the worst class of bug.
- Bus events (ADR-0005) carry data only.

## Consequences

- Reads degrade gracefully where data is recoverable; writes never fail silently.
- Subscribers stay simple — they don't handle freshness.
- Cowork sees freshness explicitly via the API envelope and can decide whether to trust stale data.
- Industry-standard shape (Pattern 2) for a service-to-service system at this scale.
- Cost: freshness is synthesized in two places (cache row + API envelope) rather than carried once on the event; accepted because it keeps the bus clean and matches who actually consumes freshness.

## Trigger conditions for revisit (→ circuit breakers)

- A single source exceeds N% failure rate over a 24h window for K days running → add per-source circuit breakers (open after N failures in M seconds, half-open probes). ~100 LOC; defers cleanly on top of the policy enum.

## See also

- [architecture-brief.md § Failure mode](../architecture-brief.md#failure-mode)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [ADR-0015: Observability stack](./0015-observability-stack-personalized.md)
