# ADR-0017: Failure mode — per-source policy + freshness at boundaries

**Status**: Accepted (**amended** 2026-06-12)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-12
**Decision-maker**: John Wolf

> **✎ AMENDED 2026-06-12** (freshness review). Four refinements. **(1) Two times, named apart:** the serve envelope carries `meta.as_of` (market/event time of the data) *and* `meta.fetched_at` (retrieval time); `meta.stale_since` appears only when a policy has degraded. `as_of` is what off-hours consumers (Cowork) actually need; `fetched_at` measures the pipeline. **(2) Freshness budgets:** each policy declaration carries a `max_age` per dataset (market-hours-scoped for market data) — the policy says *how* to degrade, the budget says *when* "stale" trips; without it every consumer invents its own threshold. **(3) Grain:** policies attach **per dataset (source × data-class), not per source** — Schwab alone spans the equity stream (STALE_OK at the cache), the 5-min curated scan (BEST_EFFORT + tombstones), the EOD full scan (BEST_EFFORT, retry-in-window, alarm on a missed night), and serve's read-through (effectively STRICT for reads — no stored fallback for arbitrary tickers, ADR-0024). Mutations failing loud remains an **invariant**, not a configurable policy. **(4)** `source_status` is row provenance with a small enum pinned at Stage 2 (`ok`/`partial`/`failed`; rename to `fetch_status` if ambiguity bites); failed per-ticker fetches write **tombstone rows** so scan membership stays truthful under failure (ADR-0030) and gate G2's gap audit has rows to count.

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

## Setting the `max_age` budgets

Budgets are **derived, not invented** — an arbitrary threshold is worse than none (too tight cries wolf on healthy data; too loose green-lights data that already misleads). Each dataset's budget sits in a band between a mechanical floor and a product ceiling:

- **Floor — cadence × miss-tolerance** (engineering). Data can't be fresher than its update cadence, so the budget must clear `cadence × N`, where N is how many consecutive update cycles may fail before it matters (N≈2 for *warn*, N≈3 for *critical*). A budget under the cadence fires constantly on a healthy source.
- **Ceiling — value decay** (product). How fast the number stops being *true* for the consumer acting on it (an RTH quote in seconds; popularity in tens of minutes; an earnings date in days). Beyond this, the data misleads regardless of pipeline health.
- **Squeeze check.** If `floor > ceiling`, the source can't deliver inside its useful life — that's a sourcing/product problem no `max_age` fixes, and the budget exercise is what surfaces it early.

**Polled vs. streamed need different questions.** The floor formula applies only to *polled* sources (ApeWisdom, the options scans), which get a true **age budget**. The equity stream has no cadence — an illiquid ticker legitimately doesn't tick for minutes — so it gets a **stream-level liveness budget** ("no tick on *any* subscribed ticker in N seconds during RTH" → ops alarm) while per-ticker quote rows only expose `as_of` for the consumer to judge. Polled = age question; streamed = liveness question.

**Procedure:** (1) write down cadence + decay ceiling; (2) provisional budget = `cadence × 2`, clamped under the ceiling, run the squeeze check; (3) **market-hours-scope** market-data budgets (the clock only runs during RTH, else overnight reads as falsely stale — `as_of` carries the off-hours explanation); (4) **calibrate at burn-in** — the `source_last_success_seconds` gauge already measures the healthy per-dataset distribution, so set each budget above the observed healthy p99 and below the decay ceiling. Provisional-from-theory is just the starting guess burn-in refines.

Budgets are **config, not code** — they live in the `DatasetFreshness` declaration and tune without a deploy; treat the first burn-in window's values as provisional. The producer's budget is the *default stale flag + the ops trigger*, not the last word: a stricter/looser consumer (Cowork) reads `as_of` and applies its own tolerance at the boundary. Specific numbers live in the Stage-2 config table (dated, post-burn-in), not in this ADR.

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
