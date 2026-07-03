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

**Bot read path:** alerts = direct DB (ADR-0008 LISTEN/NOTIFY + reconcile — the intentional event contract); all other reads via `serve` (the bot dogfoods Cowork's contract).

**Deferred:** serving streamed-ticker currents from the UNLOGGED cache (a cache-from-stream optimization). Keeping open positions streamed (ADR-0019) revives a candidate consumer (portfolio P&L) — settle if/when it bites. Backtest reads history-only (no read-through) for point-in-time integrity.

## Consequences

- Arbitrary off-hours ticker requests are served correctly without storing per-ticker "now" data.
- The data contract is stable and predictable per endpoint (source fixed by nature, not by runtime state).
- Cost: `serve` holds a Schwab client and shares the REST budget; bounded by ADR-0012 rate limits.

## Change log

- 2026-06-05 — locked.
- 2026-07-03 — amended: Cowork consumer profile recorded and tied to its ADR-0012 rate limit as the enforcement mechanism (REST budget review).

## See also

- [ADR-0012: Serving API auth + rate limits](./0012-serving-api-auth-keys-plus-rate-limits.md)
- [ADR-0008: Detection in ingest (amended)](./0008-detection-in-ingest.md)
- [ADR-0020: Three datasets + backtest constraints](./0020-three-datasets-and-backtest-constraints.md)
