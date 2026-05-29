# ADR-0007: Three-layer options data pipeline

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

Two real needs drive options data in v2: (1) generate helpful real-time alerts for retail traders (and feed the auto-trader); (2) analyze options flow over time — full OI/volume across the market. These need different cadences, universes, and storage. A single approach can't serve both: full-universe streaming is infeasible (Schwab streamer limits), and EOD-only snapshots miss intraday IV/volume-build signals. Backtesting (ADR-0011) also requires that whatever is persisted is point-in-time queryable.

## Decision

Three layers, each serving a distinct purpose:

| Layer | What | Cadence | Universe | Purpose |
|---|---|---|---|---|
| **Tier 1** | in-memory current-state map | streaming, sub-second | curated | real-time alerts + auto-trader |
| **Tier 2** | intraday snapshots → `options_chain_history` hypertable | 5-min during market / hourly off-hours | curated | intraday IV/volume-build signals |
| **EOD full** | REST pull of every optionable underlying's chain | nightly | full market | market-wide OI/volume flow over time |

- Tier 1 is fed by `level_one_option` streaming (ADR-0006), held in memory by the OptionsSubscriptionManager.
- Tier 2 periodically snapshots Tier 1 state into a Timescale hypertable; compression keeps storage modest (~tens of MB/day compressed for ~200 curated underlyings).
- EOD full-universe is a REST job; ~800K rows/day, comparable compressed footprint.
- All persisted rows carry `fetched_at` + `source_status` and are immutable (point-in-time queryable for backtest).

## Consequences

- Alerts get sub-second current state; flow analysis gets both intraday depth (curated) and market-wide breadth (EOD).
- Cowork can query historical option metrics at two granularities.
- Storage is bounded and within a single Postgres instance (~25GB/yr per persisted layer, compressed).
- Cost: the most ingestion complexity of the approaches considered — three independent paths to build and reconcile. Each is independent, so they can land incrementally.

## Trigger conditions for revisit

- Storage growth outpaces expectations → tune Tier 2 cadence or curated-universe size, or add Timescale retention/downsampling policies.
- A need for full per-contract tick logs (Tier 3) emerges → add post-v2 for contracts of interest only.

## See also

- [architecture-brief.md § Schwab + options pipeline](../architecture-brief.md#schwab--options-pipeline)
- [ADR-0006: Schwab L1-only streaming scope](./0006-schwab-l1-only-streaming-scope.md)
- [ADR-0011: Backtest in v2](./0011-backtest-in-v2-eda-deferred.md)
