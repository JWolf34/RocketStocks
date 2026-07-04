# ADR-0017: Failure mode — per-dataset policy + freshness at boundaries

**Status**: Accepted (amended 2026-06-12; amendments folded into body 2026-07-03)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-12
**Decision-maker**: John Wolf

## Context

v2's failure surfaces are heterogeneous: a dropped Schwab stream can serve stale quotes; a failed ApeWisdom poll can skip a cycle harmlessly; but a failed `alerts` or `paper_trades` write must not vanish silently. We need a coherent policy. The question was also whether to propagate freshness metadata on every bus event (belt-and-suspenders) or only at boundaries. Carrying `FreshnessInfo` on every event was found to be noise — ~90% of subscribers (persister, detectors) don't read it; only a cache-updater-style boundary consumer would care, and it can read source status elsewhere. This matches industry "Pattern 2" (service-mesh / SRE: freshness as source-health metrics + per-row columns + API headers) rather than "Pattern 1" (event-time watermarks, for big-data streaming).

## Decision

**Per-dataset policy, freshness at the boundaries, bus events clean.**

- Each dataset declares a `FreshnessPolicy` saying *how* to degrade:
  - `STRICT` — raise on failure (e.g. serve's Schwab read-through: no stored fallback for arbitrary tickers — ADR-0024)
  - `STALE_OK` — return last-known with a flag (e.g. popularity served from its last stored poll; quotes-at-the-cache returns as an instance if the UNLOGGED cache is reintroduced — ADR-0003)
  - `BEST_EFFORT` — skip this cycle, retry next (e.g. ApeWisdom mentions; the options scans)
- **Grain: policies attach per dataset (source × data-class), not per source.** Schwab alone spans the equity stream (a stream-level **liveness** budget — below), the 5-min curated options scan (`BEST_EFFORT` + tombstones), the options EOD full scan (`BEST_EFFORT`, retry-in-window, alarm on a missed night), the nightly 1-min equity backfill (`BEST_EFFORT`, retry within Schwab's **7-day** 1-min window, alarm on a missed night — ADR-0022), and serve's read-through (effectively `STRICT` for reads).
- **Mutations fail-loud is an invariant, not a configurable policy** — silent write failure is the worst class of bug.
- Freshness lives at boundaries, **not on bus events** (ADR-0005 — events carry data only):
  - persistence rows carry `fetched_at` + `source_status` — row provenance with a small enum pinned at Stage 2 (`ok`/`partial`/`failed`; rename to `fetch_status` if ambiguity bites). Failed per-ticker fetches write **tombstone rows** (`source_status='failed'`, no payload), so scan membership stays truthful under failure (ADR-0030) and gate G2's gap audit has rows to count.
  - **the serve envelope carries two times, named apart:** `meta.as_of` (market/event time of the data) *and* `meta.fetched_at` (retrieval time); `meta.stale_since` appears only when a policy has degraded. `as_of` is what off-hours consumers (Cowork) actually need; `fetched_at` measures the pipeline.
  - Prometheus exposes `source_last_success_seconds{source=...}`

## Setting the `max_age` budgets

Each policy declaration carries a **`max_age` budget per dataset** — the policy says *how* to degrade, the budget says *when* "stale" trips; without it every consumer invents its own threshold. Budgets are **derived, not invented** — an arbitrary threshold is worse than none (too tight cries wolf on healthy data; too loose green-lights data that already misleads). Each dataset's budget sits in a band between a mechanical floor and a product ceiling:

- **Floor — cadence × miss-tolerance** (engineering). Data can't be fresher than its update cadence, so the budget must clear `cadence × N`, where N is how many consecutive update cycles may fail before it matters (N≈2 for *warn*, N≈3 for *critical*). A budget under the cadence fires constantly on a healthy source.
- **Ceiling — value decay** (product). How fast the number stops being *true* for the consumer acting on it (an RTH quote in seconds; popularity in tens of minutes; an earnings date in days). Beyond this, the data misleads regardless of pipeline health.
- **Squeeze check.** If `floor > ceiling`, the source can't deliver inside its useful life — that's a sourcing/product problem no `max_age` fixes, and the budget exercise is what surfaces it early.

**Polled vs. streamed need different questions.** The floor formula applies only to *polled* sources (ApeWisdom, the options scans), which get a true **age budget**. The equity stream has no cadence — an illiquid ticker legitimately doesn't tick for minutes — so it gets a **stream-level liveness budget** ("no tick on *any* subscribed ticker in N seconds during RTH" → ops alarm) while per-ticker quote rows only expose `as_of` for the consumer to judge. Polled = age question; streamed = liveness question.

**Procedure:** (1) write down cadence + decay ceiling; (2) provisional budget = `cadence × 2`, clamped under the ceiling, run the squeeze check; (3) **market-hours-scope** market-data budgets (the clock only runs during RTH, else overnight reads as falsely stale — `as_of` carries the off-hours explanation); (4) **calibrate at burn-in** — the `source_last_success_seconds` gauge already measures the healthy per-dataset distribution, so set each budget above the observed healthy p99 and below the decay ceiling. Provisional-from-theory is just the starting guess burn-in refines.

Budgets are **config, not code** — each dataset's `DatasetFreshness` declaration reads its `max_age` from a **Postgres config table** (created with the Stage-2 schema work), so budgets tune without a redeploy; treat the first burn-in window's values as provisional. The producer's budget is the *default stale flag + the ops trigger*, not the last word: a stricter/looser consumer (Cowork) reads `as_of` and applies its own tolerance at the boundary. Specific numbers live in that config table (dated, finalized post-burn-in), not in this ADR.

## Consequences

- Reads degrade gracefully where data is recoverable; writes never fail silently.
- Subscribers stay simple — they don't handle freshness.
- Cowork sees freshness explicitly via the API envelope and can decide whether to trust stale data.
- Industry-standard shape (Pattern 2) for a service-to-service system at this scale.
- Cost: freshness is synthesized at the boundary (the API envelope; plus a `stale_since` cache column if the UNLOGGED cache returns — ADR-0003) rather than carried once on the event; accepted because it keeps the bus clean and matches who actually consumes freshness.

## Trigger conditions for revisit (→ circuit breakers)

- A single source exceeds N% failure rate over a 24h window for K days running → add per-source circuit breakers (open after N failures in M seconds, half-open probes). ~100 LOC; defers cleanly on top of the policy enum.

## Change log

- 2026-05-27 — locked ("trimmed A": per-source policy enum, freshness at boundaries, bus events clean, mutations fail-loud).
- 2026-06-12 — amended (freshness review): (1) serve envelope splits `meta.as_of` from `meta.fetched_at`; (2) `max_age` budgets added per dataset, with the derivation procedure; (3) grain moved from per-source to per-dataset (source × data-class); (4) `source_status` enum pinned + tombstone rows for failed per-ticker fetches.
- 2026-07-03 — editorial: amendment banner folded into the body; budget-storage wording disambiguated (Postgres config table read by `DatasetFreshness`); no decision change.
- 2026-07-03 — grain examples extended with the nightly 1-min equity backfill and its 7-day recovery bound (ADR-0022 amendment).
- 2026-07-03 — aligned with the ADR-0003 amendment (UNLOGGED cache deferred out of v2): cache `stale_since` removed from the freshness surfaces and the `STALE_OK` example re-pointed; the equity-stream grain example now cites the liveness budget directly. Semantics return with the cache.

## See also

- [architecture-brief.md § Failure mode](../architecture-brief.md#failure-mode)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [ADR-0015: Observability stack](./0015-observability-stack-personalized.md)
- [ADR-0024: Serve read-model boundary](./0024-serve-read-model-boundary.md)
- [ADR-0030: Detection re-derived (tombstones / scan membership)](./0030-detection-rederived-single-alert-class.md)
