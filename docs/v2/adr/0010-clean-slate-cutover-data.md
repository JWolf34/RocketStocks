# ADR-0010: Clean-slate data at cutover

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

v2 redesigns the schema freely (ADR-0004). The question is what, if anything, to migrate from v1 at cutover. The handoff's earlier position preserved one dataset — ApeWisdom popularity history → Parquet — on the grounds it shortens time-to-value for popularity-surge detection (which compares current mentions to ~24h-ago). Everything else (watchlists, alerts, paper trades, price history) was already disposable. During Stage 1 close-out the user widened this to "do not focus on preserving data."

## Decision

**Clean slate at cutover. Nothing is migrated, including ApeWisdom popularity history.**

- Users recreate their watchlists after cutover.
- No user state is preserved.
- Popularity-surge detection accepts a **cold-start window** after cutover: it produces no surge signals until enough fresh ApeWisdom polls accumulate to compute the 24h-ago baseline.
- Historical data needed for backtest (ADR-0011) is rebuilt via REST backfill where the upstream APIs sell history (Schwab price history, etc.), not migrated from v1.

## Consequences

- Simplest possible cutover — no migration script, no row-count/sample-query verification harness, no schema-translation layer.
- The new schema owes nothing to v1's shape.
- Cost: a popularity-surge blackout window immediately post-cutover (the most user-facing alert type starts cold). Bounded — it self-heals as polls accumulate.
- Cost: backtest windows start short and grow as live data accumulates (accepted in ADR-0011).

## See also

- [architecture-brief.md § Out of scope for v2](../architecture-brief.md#out-of-scope-for-v2)
- [ADR-0004: Persistence layer shape](./0004-persistence-layer-shape.md)
- [ADR-0011: Backtest in v2](./0011-backtest-in-v2-eda-deferred.md)
