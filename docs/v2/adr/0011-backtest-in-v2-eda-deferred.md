# ADR-0011: Backtest framework in v2; EDA deferred

**Status**: Accepted (**amended** 2026-06-05)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05
**Decision-maker**: John Wolf

> **✎ AMENDED 2026-06-05.** **Options backtest is forward-only** — option *chains aren't backfillable* (Schwab returns only the current chain), so the options-research window starts empty at cutover and grows; equity history *is* backfillable. **Backtest the underlying equity as the tradeable; options flow is the *predictor*** (option-leg P&L isn't reliably reconstructable from 5-min snapshots). **Event studies** (flow events → forward *equity* returns) are the day-one validation. **Selection bias:** the curated 5-min options tier is biased (universe chosen by the signals under test) → prefer the **EOD full-universe** tier for research conclusions. Backtest shares the **deterministic-replay machinery** (event-source port + injected clock, same as warm-up/live — ADR-0023) and runs signals **at the resolution they run live** (ADR-0021/0022). REST backfill seeds equity bars/earnings only — **not option chains**.

## Context

v1 has two research subsystems: a backtest framework (`backtest/`, built on backtesting.py) and an EDA framework (`eda/`, signal-research engines). The initial v2 plan cut both, to be rebuilt post-v2 on the new data/logic models. The open question was whether *building* backtest in v2 is worth it — specifically, whether it's the forcing function that proves the v2 ingestion shape is correct (complete, point-in-time queryable, immutable). The alternatives considered were a minimal signal-replay harness or design-constraints-only. The user chose to build the full framework.

## Decision

- **Backtest framework is rebuilt in v2** on new data/logic models (not migrated from v1). It lives in `tools/backtest/` and consumes the `serve` API like any other client — making `serve` a three-consumer contract (bot, Cowork, backtest).
- **REST backfill is in scope** (`tools/backfill/`) to seed historical equity bars + earnings for backtest windows, where upstream APIs sell history. *(Amended 2026-06-05: option chains are NOT backfillable — forward-only; see the amendment note above.)*
- **Persistence is point-in-time queryable by mandate** — every persisted shape now has two consumers (live detection + replay), so immutability and `fetched_at`/`source_status` columns are hard requirements, not nice-to-haves.
- **EDA stays deferred post-v2** — rebuilt on the new shapes once v2 is stable.

## Consequences

- Building backtest forces the ingestion shape to be right *by measurement*, not by hope — persistence gaps surface during v2 build, not six months later when history can't be backfilled.
- Pure signal functions (`libs/analysis/`, renamed from `libs/core/` — ADR-0001 amendment 2026-06-12) are shared verbatim between live detectors (ADR-0008) and backtest — no adapters.
- Cost: ~25% scope increase vs cutting backtest; two evolving shapes (new ingestion + new backtest logic) interact during the rewrite.
- Cost: backtest windows are short at cutover (clean-slate, ADR-0010) and grow as live data accumulates — acceptable; the framework's value is the forcing function and regression-smoke-testing, not deep history on day one.

## See also

- [architecture-brief.md § Scope discipline](../architecture-brief.md#scope-discipline)
- [ADR-0010: Clean-slate cutover data](./0010-clean-slate-cutover-data.md)
- [ADR-0008: Detection in ingest](./0008-detection-in-ingest.md)
