# ADR-0025: Live data source set — Schwab + ApeWisdom + Nasdaq

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

*(Design ledger L8/L10/L14.)*

## Context

v1 fused ~10 data sources. The v2 direction is data-driven insights leaning hard on Schwab, with minimal external dependencies (a reliability + maintenance win). We must lock which sources survive, against the surviving v2 feature surface (options alerts, watchlists, stock/options reports, earnings, user screeners, popularity-driven universe).

## Decision

**Live source set: Schwab (core) + ApeWisdom (popularity) + Nasdaq (earnings calendar/EPS).** Nothing else ships live in v2.

- **Schwab** covers quotes/bars/options chains/movers + the **FUNDAMENTAL projection**, which supplies report *insights* (valuation, margins TTM+MRQ, ROE/ROA, growth, debt ratios, book value, market cap/float) in one free call — confirmed against a live NVDA payload. The **validation boundary coerces `0.0`/missing → null**; **short interest is not reliable from Schwab** (deferred).
- **No statement-line-item source.** Schwab gives derived ratios, not dollar IS/BS/CF history. **Cowork self-sources SEC XBRL directly** (free public data; the serve-principle is to expose differentiated data, not re-broker commodity data).
- **No news source.** Cowork already searches the web; a passthrough would be a lossy, license-encumbered subset. Discord users search news themselves.

**Cut:** Tiingo, TradingView, Capitol Trades, yfinance, Finnhub, Stooq (live), all news providers, and SEC (as a RocketStocks source).

**Deferred post-v2:** short interest / float; a Stooq backfill fallback; news×flow fusion; and an **SEC Form-4 insider feed fused onto the options alert** (pre-filtered to open-market, exec/director, material-size) — uniquely backfillable, so it can seed insider→return event studies on arrival.

## Consequences

- Three live dependencies instead of ten — a major reliability and maintenance win; the serve API exposes only differentiated/derived data.
- Cost: report depth is limited to Schwab-derived insights in v2; dollar-statement depth is Cowork's job, not RocketStocks'.

## See also

- [ADR-0024: Serve read-model boundary](./0024-serve-read-model-boundary.md)
- [ADR-0026: Charting deprecation + report surfaces](./0026-charting-deprecation-and-reports.md)
