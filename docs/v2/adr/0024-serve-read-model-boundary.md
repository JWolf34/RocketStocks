# ADR-0024: Serve read-model boundary — DB-read vs Schwab read-through

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-06-05 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

*(Reconciles ADR-0012.)*

## Context

`serve` is the external read contract (Cowork + dogfooded by `bot`). Cowork builds reports on *arbitrary* tickers, infrequently, mostly off-hours — not guaranteed to be in the curated universe or during market hours. We must decide, per data class, whether `serve` reads stored rows or fetches live from Schwab, and reconcile ADR-0012 (whose rate-limit rationale assumed `serve` can pressure the Schwab budget).

## Decision

`serve` is **read-mostly over Postgres**; it does not write market/derived data. Each endpoint's source is fixed by data nature, decided once — **not** a runtime fetch-on-miss.

- **Current market data → thin Schwab read-through** (no write-back caching): quote, fundamentals, current options chain, movers/screeners. Cowork's access is infrequent, so a live call is practically free, and "now" snapshots aren't worth storing per arbitrary ticker.
- **History / derived / cadence-polled / user state → DB read:** OHLCV history, options-chain history (from stored snapshots), popularity (ApeWisdom), earnings (Nasdaq), alerts/signals/surges, universe membership, backtest results, watchlists/paper-trades/portfolio.
- The read-through surface is **Schwab-only**; ApeWisdom + Nasdaq are always DB reads (cadence-polled — a per-request fetch wouldn't be fresher).
- The `options-chain` endpoint is parameterized: `?as_of=<date>` → DB, current → read-through.

**Reconciles ADR-0012:** because `serve` *can* trigger Schwab fetches on current-data endpoints, ADR-0012's per-consumer rate limits genuinely protect the shared Schwab budget. `serve` holds a Schwab client; `ingest` refreshes the token, `serve` reads it; the global REST budget is shared — coordinate simply, revisit if it bites.

**Cowork consumer profile (stated 2026-07-03; an input to the REST budget):** off-hours operation, DB-history-mostly, read-through for **≤5–10 tickers per session**. The profile is *enforced*, not assumed — Cowork's `service_clients` rate limit (ADR-0012) is sized to it, so the RTH REST headroom (roadmap pre-flight) never budgets for Cowork. A legitimate change in Cowork's usage is a deliberate rate-limit change, reviewed against the budget table.

**Bot data-access rule (amended 2026-07-03):** the bot holds **no Schwab client** and never reads market-data tables directly — **all market data reaches the bot through `serve`** (the bot dogfoods Cowork's contract; this also keeps Schwab-token coordination two-party: `ingest` writes, `serve` reads). Direct DB access is limited to (a) **alert rows** (ADR-0008 LISTEN/NOTIFY + reconcile — the intentional event contract; alert embeds render **from the alert row alone**, no post-time enrichment — mixing "what detection saw" with "now" would break ADR-0026's transparency principle) and (b) **user/config state** (watchlists, paper trades, notification routes, settings), of which the bot is the single writer via `rs_persistence` repositories. Writes stay direct — not through `serve` — because `serve` is read-mostly by design, a write API would put `serve` on the critical path of Discord interactions (3-second deadline) and require per-user delegated auth (the ADR-0012 OAuth trigger), and no second write consumer exists. ADR-0029's hot-read posture is realized *through* `serve`; a cache-backed fast path returns with the UNLOGGED cache's reintroduction (ADR-0003 triggers).

**Access matrix (who reads / who writes):**

| Table family | `ingest` | `serve` | `bot` |
|---|---|---|---|
| Market data (bars, options snapshots, popularity, earnings, universe) | **sole writer** | reads (DB / read-through per above) | via `serve` only |
| `alerts` + `outbox` | **sole writer** | reads (alerts endpoints) | reads direct (event contract); owns its `posted_at` cursor column (ADR-0005) |
| User/config state (watchlists, holdings/transactions/snapshots, routes, settings) | — | reads (portfolio/watchlist endpoints) | **sole writer** + reads direct |
| UNLOGGED cache *(deferred out of v2 — ADR-0003)* | *(sole writer, when reintroduced)* | *(candidate reader, when reintroduced)* | never direct |
| `service_clients` | — | reads (auth); rows operator-provisioned | — |

**Deferred:** the UNLOGGED cache itself is deferred out of v2 (ADR-0003 as amended — the zero-consumer review); cache-from-stream serving of streamed-ticker currents is part of its reintroduction case, with portfolio P&L marks (open positions streamed — ADR-0019) the likeliest first consumer. Until then, streamed-ticker currents ride the same read-through as any other ticker. Backtest reads history-only (no read-through) for point-in-time integrity.

**Trigger condition:** user-state writes move behind `serve` (a write API) when a web UI or a second user-facing write client arrives — the same event that fires ADR-0012's OAuth and ADR-0013's `users`-table triggers; design them together.

## Consequences

- Arbitrary off-hours ticker requests are served correctly without storing per-ticker "now" data.
- The data contract is stable and predictable per endpoint (source fixed by nature, not by runtime state).
- Cost: `serve` holds a Schwab client and shares the REST budget; bounded by ADR-0012 rate limits.

## Change log

- 2026-06-05 — locked.
- 2026-07-03 — amended: Cowork consumer profile recorded and tied to its ADR-0012 rate limit as the enforcement mechanism (REST budget review).
- 2026-07-03 — amended (bot data-access rule, final pre-implementation review): all bot market data via `serve`; no Schwab client in `bot`; alert embeds render from the alert row alone; access matrix added; user-state writes stay direct via repositories with a writes-behind-serve trigger condition documented.
- 2026-07-03 — aligned with the ADR-0003 amendment: UNLOGGED cache deferred out of v2; the cache-from-stream serving optimization folds into the cache's reintroduction case; matrix row marked deferred.

## See also

- [ADR-0012: Serving API auth + rate limits](./0012-serving-api-auth-keys-plus-rate-limits.md)
- [ADR-0008: Detection in ingest (amended)](./0008-detection-in-ingest.md)
- [ADR-0020: Three datasets + backtest constraints](./0020-three-datasets-and-backtest-constraints.md)
