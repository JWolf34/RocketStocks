# ADR-0026: Charting deprecated; alert embed, buttons, and two report surfaces

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

*(Design ledger L13.)*

## Context

v1 rendered chart images (mplfinance), a subsystem + dependency that created an earlier parity-gap flag. v2's Discord surface is mobile-first (ADR-0014). We must pin the alert presentation, its action buttons, and the report surfaces — without anchoring on v1.

## Decision

**Charting is deprecated in v2** — no mplfinance, no chart images. Reports are text/embed only (removes a subsystem + dependency).

**Alert embed (mobile-first):** color = green calls / red puts; hero field = vol/OI ratio + raw numbers (transparent, no black box); premium = conviction line; underlying price + %; greeks line = **IV + delta only**; footer = detected-time + freshness flag; **OI-confirmation via update-in-place** the next day (lifecycle state in Postgres).

**Buttons (4):** `💵 Trade` · `⭐ Watchlist` · `📄 Stock Report` · `📊 Options Report`. Trade = manual paper BUY of the **underlying** into the **clicker's** portfolio (`source=manual`, not an auto-trader — ADR-0027).

**Two report surfaces** (button + slash command + serve/Cowork):

- **Stock Report** — Schwab fundamentals (P/E, margins, rev growth, market cap) + price/technicals + earnings date. No chart.
- **Options Report** — top 3 calls / top 3 puts, **full greeks** (the ones dropped from the alert), IV, put/call ratio, OI, flow context.

Exact field sets, the "top-3" ranking basis (volume / premium / unusualness), and whether reports are exposed via `serve` for Cowork are deferred to a focused report-design session.

## Consequences

- One fewer subsystem/dependency; mobile-first embeds for a majority-mobile audience.
- Greeks split deliberately: IV + delta on the alert (glanceable), full greeks in the Options Report.
- Cost: users wanting a chart use their own tools — acceptable; charts weren't a differentiator.

## See also

- [ADR-0014: Bot interface (amended)](./0014-bot-interface-slash-plus-components.md)
- [ADR-0025: Live data source set](./0025-live-source-set-schwab-apewisdom-nasdaq.md)
- [ADR-0027: Manual paper trading only](./0027-manual-paper-trading-only.md)
