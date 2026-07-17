# ADR-0019: Universe model — two bands (equity stream + options REST), priority & eviction

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

*(Supersedes the universe portions of ADR-0006.)*

## Context

Schwab's streaming budget is per-service and effectively bounded (the legacy "500" is a soft, build-verified figure). Options streaming bills per-contract, so even a few underlyings at ATM±5 / ≤30 DTE would exhaust the budget — making a streamed options band infeasible (ADR-0006 superseded). We need an explicit, reconstructible universe model with a priority order for the scarce stream slots.

## Decision

**Two bands:**

- **Equity band** — `level_one_equity` streaming, **≤ ~500 underlyings** (the whole tracked universe). One symbol per underlying; conserves REST budget; acts as the trigger rail for on-demand option pulls.
- **Options band** — **REST chain snapshots only** (ADR-0020). **No `level_one_option` streaming in v2** (a documented post-v2 upgrade behind a trigger condition).

**Universe priority (inclusion + eviction order), LOCKED:**

1. paper-trade open positions → 2. active alert subjects → 3. watchlists → 4. screener top-10 (per sort combo) → 5. top-100 popularity → 6. discovery slots.

**Invariant (amended 2026-07-16): curated ⊆ streamed.** The stream desired set is derived *from* the curated set (curated names first, remaining priority names up to the stream ceiling), so every options-scanned name has live bars — load-bearing for equity-bar fusion (ADR-0032 D2). Enforced structurally by the desired-set computation, not by configuration.

A **`UniverseManager`** recomputes desired sets from DB-backed sources on a cadence, diffs against current subscriptions, ADD/UNSUBs, and evicts the lowest-priority members when over budget. It is fully reconstructible from Postgres on restart (ADR-0018). The symbol master behind these sets is the Nasdaq Trader listing files (US-listed universe — ADR-0025); filter rules, listing-lifecycle handling, band structure, and sizes live in the [`UniverseManager` design note](../universe-manager.md).

## Consequences

- A bounded, explicit symbol budget that fits Schwab streamer limits and is rebuildable on warm-up.
- Open paper positions stay streamed (priority #1) for live portfolio marks even though the auto-trader is cut (ADR-0009/0027) — manual positions still want live P&L. (Marks are served via `serve` read-through until the UNLOGGED cache returns — ADR-0003/0024; streaming held names also keeps them in the detection log.)
- Cost: priority #4/#5 churn means periodic ADD/UNSUB traffic; the manager reconciles on a cadence rather than per-event.

## Change log

- 2026-06-05 — locked.
- 2026-07-16 — amended (UniverseManager design session): the **curated ⊆ streamed invariant** made explicit (previously implied only via ADR-0032 D2's citation of this ADR); design-note pointer updated. No change to bands, priority order, or eviction.

## Trigger conditions for revisit

- A signal genuinely needs streamed option contracts → reconsider an options-streaming band (per-contract budget implications).

## See also

- [ADR-0006: Schwab L1-only streaming (superseded)](./0006-schwab-l1-only-streaming-scope.md)
- [ADR-0020: Three datasets + backtest constraints](./0020-three-datasets-and-backtest-constraints.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
