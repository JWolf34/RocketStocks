# ADR-0027: Manual paper trading only in v2; auto-trader deferred post-v2 as a separate service

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

*(Supersedes ADR-0009.)*

## Context

ADR-0009 scoped an auto-trader hosted in `bot` (a pure library that runs on alert LISTEN/NOTIFY and polls the cache for position management) alongside manual paper trading. On reflection the auto-trader is **not v1 parity** — and reliability is goal #1, so a fallible, change-heavy trading loop should not share the bot process. The auto-trader's shape will also diverge from the bot's as it grows.

## Decision

**The auto-trader is cut from v2.** It will be built ground-up post-v2 as its **own service**, not hosted in `bot`.

**Manual paper trading stays** (v1 parity): the `holdings` / `transactions` / `portfolio_snapshots` schema (with a `source` column — `manual` now; `auto_trader` returns with the post-v2 service), the `💵 Trade` button (ADR-0026), and `/trade buy|sell`. Open positions are streamed for live marks (ADR-0019). The **`/trade bot ...` command group is removed** (ADR-0014 amendment).

**Post-v2 auto-trader blueprint** (preserved in the superseded ADR-0009 body, and here as the starting point): a pure library behind data-access **ports**, with its **own LISTEN/NOTIFY subscription** to the `alerts` table (Bus 2), **direct cache reads** for position marks and **direct trade writes**, and the `bot` demoted to a serve-backed performance UI. This is the symmetric mirror of the detection-extraction seam (ADR-0008): design the seam, defer the build.

## Consequences

- v2 ships only human-initiated paper trades — simpler, and it keeps the bot free of a trading loop.
- The `source` discriminator and the alerts/Bus-2 contract mean the post-v2 service attaches without reworking v2.
- Advisory, human-filtered alerts may use a faster trigger than their backtested thesis (ADR-0022) precisely because nothing is auto-executed.

## See also

- [ADR-0009: Paper trading + auto-trader (superseded)](./0009-paper-trading-and-auto-trader-scope.md)
- [ADR-0014: Bot interface (amended)](./0014-bot-interface-slash-plus-components.md)
- [ADR-0008: Detection in ingest (amended)](./0008-detection-in-ingest.md)
