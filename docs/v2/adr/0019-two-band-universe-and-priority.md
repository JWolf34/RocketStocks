# ADR-0019: Universe model — two bands (equity stream + options REST), priority & eviction

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

*(Design ledger L4. Supersedes the universe portions of ADR-0006.)*

## Context

Schwab's streaming budget is per-service and effectively bounded (the legacy "500" is a soft, build-verified figure). Options streaming bills per-contract, so even a few underlyings at ATM±5 / ≤30 DTE would exhaust the budget — making a streamed options band infeasible (ADR-0006 superseded). We need an explicit, reconstructible universe model with a priority order for the scarce stream slots.

## Decision

**Two bands:**

- **Equity band** — `level_one_equity` streaming, **≤ ~500 underlyings** (the whole tracked universe). One symbol per underlying; conserves REST budget; acts as the trigger rail for on-demand option pulls.
- **Options band** — **REST chain snapshots only** (ADR-0020). **No `level_one_option` streaming in v2** (a documented post-v2 upgrade behind a trigger condition).

**Universe priority (inclusion + eviction order), LOCKED:**

1. paper-trade open positions → 2. active alert subjects → 3. watchlists → 4. screener top-10 (per sort combo) → 5. top-100 popularity → 6. discovery slots.

A **`UniverseManager`** recomputes desired sets from DB-backed sources on a cadence, diffs against current subscriptions, ADD/UNSUBs, and evicts the lowest-priority members when over budget. It is fully reconstructible from Postgres on restart (ADR-0018).

## Consequences

- A bounded, explicit symbol budget that fits Schwab streamer limits and is rebuildable on warm-up.
- Open paper positions stay streamed (priority #1) for live portfolio marks even though the auto-trader is cut (ADR-0009/0027) — manual positions still want live P&L.
- Cost: priority #4/#5 churn means periodic ADD/UNSUB traffic; the manager reconciles on a cadence rather than per-event.

## Trigger conditions for revisit

- A signal genuinely needs streamed option contracts → reconsider an options-streaming band (per-contract budget implications).

## See also

- [ADR-0006: Schwab L1-only streaming (superseded)](./0006-schwab-l1-only-streaming-scope.md)
- [ADR-0020: Three datasets + backtest constraints](./0020-three-datasets-and-backtest-constraints.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
