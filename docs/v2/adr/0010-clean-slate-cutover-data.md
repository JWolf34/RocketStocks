# ADR-0010: Clean-slate data at cutover

**Status**: Accepted (amended 2026-10-19)
**Date locked**: 2026-05-27 · **Amended**: 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** v1 popularity history is now imported to seed base rates ([#49](https://github.com/JWolf34/RocketStocks/issues/49)); daily bars, watchlists and user state are still not migrated.

## Context

v2 redesigns the schema freely (ADR-0004). The question is what, if anything, to migrate from v1 at cutover. The handoff's earlier position preserved one dataset — ApeWisdom popularity history → Parquet — on the grounds it shortens time-to-value for popularity-surge detection (which compares current mentions to ~24h-ago). Everything else (watchlists, alerts, paper trades, price history) was already disposable. During Stage 1 close-out the user widened this to "do not focus on preserving data."

## Decision

**Clean slate at cutover, with one exception: v1's ApeWisdom popularity history is imported to seed base rates** *(amended 2026-10-19)*.

- Users recreate their watchlists after cutover.
- Daily bars are rebought from Schwab, not migrated.
- No user state is preserved.
- The imported popularity history (via the `backfill` tool) removes the popularity cold-start window and seeds base rates; see architecture.md.
- Historical data needed for research is rebuilt via REST backfill where the upstream APIs sell history (Schwab price history, etc.), not migrated from v1.

## Consequences

- Simple cutover - one import for popularity history, no other migration script and no schema-translation layer.
- The new schema owes nothing to v1's shape.
- Cost: research windows other than popularity start short and grow as live data accumulates.

## Change log

- 2026-10-19 - amended (foundation re-plan): popularity history imported; see the note above.

## See also

- [ADR-0004: Persistence layer shape](./0004-persistence-layer-shape.md)
- [architecture.md § Module layout](../../foundation/architecture.md#module-layout) (the `backfill` tool)
