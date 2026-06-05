# ADR-0009: Paper trading (two tracks) + auto-trader scope

**Status**: **Superseded** by ADR-0027 (manual paper trading only; auto-trader deferred post-v2) — 2026-06-05
**Date locked**: 2026-05-27 · **Superseded**: 2026-06-05
**Decision-maker**: John Wolf

> **⚠️ SUPERSEDED 2026-06-05** (ledger "Auto-trader scope cut"). The **auto-trader is cut from v2** — not v1 parity; it will be built ground-up post-v2 as its **own service**, not hosted in `bot`. **Manual paper trading stays** (v1 parity): the `paper_trades`/holdings/portfolio schema, the L13 `💵 Trade` button, and `/trade buy|sell`. The `source` discriminator stays (`manual` now; `auto_trader` returns with the post-v2 service). The **`/trade bot ...` command group is removed** (ADR-0014 amendment). The **post-v2 auto-trader blueprint is intentionally preserved in the Decision section below** (pure library behind data-access ports; own LISTEN/NOTIFY alert subscription; direct cache reads + trade writes; bot demoted to a serve-backed performance UI) — it seeds the post-v2 service. See ADR-0027. Original retained below for history.

## Context

The handoff scoped the auto-trader as a foundation: a pure library that places paper trades automatically on alerts, with users only *viewing* the results (`/performance` + `/papertrades`). During Stage 1 close-out the user added **manual paper trading** (`/trade buy`/`/trade sell`), which means two sources of paper trades now coexist. We must decide whether they share one portfolio or are tracked separately, and pin down the auto-trader's boundaries.

## Decision

**Two separate tracks**, distinguished by a `paper_trades.source` column (`manual` / `auto_trader`). Positions, P&L, and stats are computed per-track so the auto-trader can be evaluated in isolation from the user's own decisions.

- **Auto-trader** is a pure library in `libs/auto_trader/`: `decide_entry` / `decide_exit`, equity-only, rule-based policy (fixed sizing, fixed stops, optional regime gates), ~150 LOC. The **`bot`** runs it on new-alert LISTEN/NOTIFY, writes `paper_trades` (`source=auto_trader`), and polls the cache every 5–30s for position management. Rich `paper_trades` schema captures entry signal context, exit reason, P&L, and market state at entry.
- **Manual paper trading** writes the same table (`source=manual`) via slash commands.

**Command surface** (Discord 3-level nesting respected):
- `/trade buy|sell <ticker> <qty> [limit]` — manual, direct subcommands
- `/trade bot performance|positions|history|trade <id>|config` — auto-trader views + policy, under the `bot` subcommand group

**Deferred post-v2**: auto-trader engine (sized positions, regime conditioning, A/B harness, ML), multi-leg options positions, live broker integration.

## Consequences

- Clean performance attribution — the auto-trader's track record isn't muddied by manual trades.
- One store, one schema, a `source` discriminator — minimal extra complexity for the second track.
- The auto-trader being a pure library means it's testable in isolation and reusable by backtest (ADR-0011).
- Cost: per-track stats queries must always filter by `source`; the bot hosts trading logic (acceptable until live trading forces a split).

## Trigger conditions for revisit

- Live trading arrives → split the auto-trader out of `bot` into its own service (introduce Redis Streams in the same migration; see ADR-0002/0005).

## See also

- [architecture-brief.md § Paper trading + auto-trader](../architecture-brief.md#paper-trading--auto-trader)
- [ADR-0008: Detection in ingest](./0008-detection-in-ingest.md)
- [ADR-0014: Bot interface](./0014-bot-interface-slash-plus-components.md)
