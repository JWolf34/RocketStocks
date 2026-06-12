# ADR-0030: Detection re-derived for v2 — one alert class, extensible framework, PIT lean mandate

**Status**: Accepted
**Date locked**: 2026-06-12
**Decision-maker**: John Wolf

## Context

The brief's north star said "functional parity with v1," which implicitly included v1's six alert types and its detection philosophy (two-tier leading-indicator + confirmation pairs: Popularity Surge → Momentum Confirmation, Volume Accumulation → Breakout, plus reactive Watchlist/Earnings movers) — a philosophy that lived only in v1 docs, never in an ADR. The 2026-06-11 philosophy review forced the question: is that philosophy the product, or v1 baggage? Two existing decisions already pointed away from parity: ADR-0025's surviving-surface list says "options alerts," and ADR-0026 only ever designed an options-flow embed.

## Decision

**Detection is re-derived from scratch for v2's infrastructure. v1's two-tier alert system is not used and v1 detectors are not ported.**

- **v2 ships exactly ONE alert class: the options-flow alert.** Presentation is already pinned by ADR-0026 (embed, four buttons, next-day OI-confirmation update-in-place — that lifecycle is intrinsic to this alert and survives).
- **The detection model itself is designed in a dedicated just-in-time design session at Phase 2 start** (informed by real payloads from the Phase-0 spike and the Phase-1 skeleton), closing as its own ADR/design note before detector build slices begin.
- **All six v1 alert types are retired**: popularity surge, momentum confirmation, volume accumulation, breakout, watchlist mover, earnings mover. ADR-0014's "v1 must not anchor v2" now explicitly extends from UX to detection.
- **ApeWisdom and Nasdaq remain** — as universe-priority inputs (ADR-0019), report data, and serve datasets (ADR-0024/0025). They are not alert sources in v2.
- **The detector framework must be extensible** so future detection models are additive: a detection port (`decide()`) behind the error boundary (ADR-0008), a generic alert-type taxonomy in the `alerts` table, per-detector config, and pluggable embed rendering per alert type. Adding a second detection model must not rework the pipeline.

**Point-in-time lean mandate** (the reconstructability requirement Stage 2 carries):

- **All v2 detection is 100% replayable.** Already true for the shipping detector by construction: its inputs are options-chain snapshots (immutable, `fetched_at` + `quote_time` — ADR-0020) and equity bars (immutable, whole-universe, read-time-adjusted — ADR-0021/0022).
- **Every alert row carries a detector-config-version stamp** (or a config JSON snapshot) — for auditability ("why didn't it fire on March 3?") and honest per-config-era live hit rates.
- **Snapshot presence IS the scan-membership record**: which tickers were in the curated scan at time T = which tickers have `curated_5m` rows at T. No separate membership table.
- A **universe-membership history table is deferred** until an equity-bar detector ships and needs it.
- ADR-0021/0022's tagged-non-replayable-overlay clause remains the documented, **unused** escape hatch; nothing feeding a future auto-trader may use it.

**North-star amendment:** parity with v1 applies to **platform surfaces** — watchlists, manual paper trading, reports, screeners, admin/config — **not to detection**.

## Consequences

- v2's detection scope shrinks to one signal designed once, well; replay fixtures, event studies, and the burn-in gate all concentrate on it.
- Users lose the v1 alert types at cutover — a larger user-facing change than the clean slate alone (ADR-0010); communicate it at cutover.
- v1's pure signal functions (carried to `libs/core/` per ADR-0008) become *available inputs* to the design session, not predestined components — the session decides what survives.
- Risk: the product rides on one signal's quality. Mitigated by event-study validation from day one and the extensibility requirement, which makes fast follow-on detectors cheap.
- The brief's scope-discipline section, roadmap Phase 2, and the v1 alert docs (`docs/alerts/`, now historical) are updated to match.

## See also

- [ADR-0029: Product thesis](./0029-product-thesis-trade-horizon.md) · [ADR-0026: Alert embed + reports](./0026-charting-deprecation-and-reports.md)
- [ADR-0008: Detection in ingest](./0008-detection-in-ingest.md) · [ADR-0020](./0020-three-datasets-and-backtest-constraints.md) / [0021](./0021-intraday-persistence-and-bar-close-detection.md) / [0022](./0022-bar-retention-storage-compression.md) (the PIT substrate)
- [ADR-0025: Live source set](./0025-live-source-set-schwab-apewisdom-nasdaq.md) (the "options alerts" surface list this confirms)
