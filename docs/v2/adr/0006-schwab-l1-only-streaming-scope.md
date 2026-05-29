# ADR-0006: Schwab L1-only streaming scope

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

The Schwab streaming API exposes a large surface: L1 quotes, L2 order books (`nasdaq_book`/`nyse_book`/`options_book`), `chart_equity`, `screener_equity`, `level_one_option`, `account_activity`, and forex/futures feeds. Subscribing to everything is neither necessary for v2's parity scope nor feasible given streamer symbol limits and storage. We need a bounded, explicit subscription surface.

## Decision

**Level 1 only.** Explicitly in scope:

- `level_one_equity` — real-time equity quotes
- `chart_equity` — minute bars
- `screener_equity` — top movers / most active
- `level_one_option` — for a **curated** chains universe only (see ADR-0007)

Explicitly out of scope: all L2 books (`nasdaq_book`, `nyse_book`, `options_book`), `account_activity` (live trading deferred), forex/futures.

Curated options universe = watchlist ∪ popular ∪ active alert subjects ∪ paper-trade positions, ATM ± N strikes, near-dated. An **OptionsSubscriptionManager** owns add/unsub decisions and reconciles subscriptions on a cadence as the universe shifts.

## Consequences

- Bounded symbol budget that fits Schwab streamer limits.
- Covers the full equity signal surface v1 has, plus a real-time options surface v1 lacks.
- No order-book depth — acceptable; v2 signals are quote/volume/OI-driven, not microstructure.
- No `account_activity` — consistent with paper-only trading in v2 (ADR-0009).
- Off-hours: REST polling fallback for sources that don't stream (ApeWisdom/SEC/news) + REST chain snapshots.

## Trigger conditions for revisit

- Live trading arrives → add `account_activity`.
- A signal genuinely needs order-book depth → reconsider L2 (storage + symbol-budget implications).

## See also

- [architecture-brief.md § Schwab + options pipeline](../architecture-brief.md#schwab--options-pipeline)
- [ADR-0007: Options data pipeline](./0007-options-data-pipeline-three-tiers.md)
- [ADR-0009: Paper trading + auto-trader](./0009-paper-trading-and-auto-trader-scope.md)
