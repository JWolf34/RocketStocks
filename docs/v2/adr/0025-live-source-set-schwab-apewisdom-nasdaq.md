# ADR-0025: Live data source set — Schwab + ApeWisdom + Nasdaq

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-06-05 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

## Context

v1 fused ~10 data sources. The v2 direction is data-driven insights leaning hard on Schwab, with minimal external dependencies (a reliability + maintenance win). We must lock which sources survive, against the surviving v2 feature surface (options alerts, watchlists, stock/options reports, earnings, user screeners, popularity-driven universe).

## Decision

**Live source set: Schwab (core) + ApeWisdom (popularity) + Nasdaq (earnings calendar/EPS) + Cboe (options symbol directory — added 2026-07-16).** Nothing else ships live in v2.

- **Schwab** covers quotes/bars/options chains/movers + the **FUNDAMENTAL projection**, which supplies report *insights* (valuation, margins TTM+MRQ, ROE/ROA, growth, debt ratios, book value, market cap/float) in one free call — confirmed against a live NVDA payload. The **validation boundary coerces `0.0`/missing → null**; **short interest is not reliable from Schwab** (deferred).
- **No statement-line-item source.** Schwab gives derived ratios, not dollar IS/BS/CF history. **Cowork self-sources SEC XBRL directly** (free public data; the serve-principle is to expose differentiated data, not re-broker commodity data).
- **No news source.** Cowork already searches the web; a passthrough would be a lossy, license-encumbered subset. Discord users search news themselves.
- **Corporate actions (splits/dividends — feeds ADR-0022's read-time adjustment):** a **daily Nasdaq calendar poll** captures actions forward from cutover; **Schwab's adjusted price-history candles provide the historical baseline** — each seeded/backfilled row stores an adjustment basis (`adjusted_through`), and read-time adjustment composes only actions *after* that basis, so no historical-actions feed is needed and the source set stays at three. Spike check (Phase 0): confirm Schwab candles are split-adjusted (and any raw/adjusted parameter). **Fallback if either leg disappoints:** Polygon.io's splits/dividends reference API (free tier suffices for a daily poll) at the cost of a fourth source.
- **Symbol master (US-listed universe — feeds the `UniverseManager`, ADR-0019):** the **Nasdaq Trader listing files** (`nasdaqlisted.txt` + `otherlisted.txt` — nightly, free, no auth; a different Nasdaq surface than the calendar API). They enumerate the complete US-listed universe (~10–11k: Nasdaq/NYSE/NYSE American/ARCA/BATS; no OTC, no foreign exchanges — deliberate). Instrument filter rules, diff-based listing/delisting/rename lifecycle, and the sizes glossary live in the [`UniverseManager` design note](../universe-manager.md). Schwab cannot enumerate the universe (per-symbol lookup only) but qualifies members (tradeable/fundamentals).
- **Optionability (feeds the options EOD scan roster + curated eligibility):** the **two-directory union** — Nasdaq Trader `options.txt` (same nightly SymbolDirectory family as the symbol master) ∪ the **Cboe options symbol directory CSV** (one unauthenticated GET). Neither venue directory is a superset (probed 2026-07-16: 4,185 vs 5,319 underlyings, 4,020 shared); the union spans both families and removes any single point of failure. **Cboe is the fourth live source** — admitted because it *replaces* machinery (a ~10k Schwab bootstrap probe night, a weekly re-probe scheduler, and silent-demotion heuristics), a net reliability win; the Schwab-empirical qualification is the documented fallback if either endpoint proves unstable. OCC, the authoritative directory, is bot-walled (403 to non-browser clients) — disqualified for unattended automation. Details: [`UniverseManager` design note](../universe-manager.md) D5.

**Cut:** Tiingo, TradingView, Capitol Trades, yfinance, Finnhub, Stooq (live), all news providers, and SEC (as a RocketStocks source).

**Deferred post-v2:** short interest / float; a Stooq backfill fallback; news×flow fusion; and an **SEC Form-4 insider feed fused onto the options alert** (pre-filtered to open-market, exec/director, material-size) — uniquely backfillable, so it can seed insider→return event studies on arrival.

## Consequences

- Four live dependencies instead of ten — a major reliability and maintenance win; the serve API exposes only differentiated/derived data. (The fourth, Cboe, cleared a bar the seven cut sources didn't: it deletes more machinery than it adds.)
- Cost: report depth is limited to Schwab-derived insights in v2; dollar-statement depth is Cowork's job, not RocketStocks'.

## Change log

- 2026-06-05 — locked.
- 2026-07-03 — amended: corporate-actions sourcing pinned (Nasdaq forward poll + Schwab adjusted baseline, Polygon fallback documented) and the symbol master pinned (Nasdaq Trader listing files; Schwab as per-symbol qualifier).
- 2026-07-16 — amended (UniverseManager design session): **Cboe options symbol directory added as the fourth live source**; optionability = Nasdaq `options.txt` ∪ Cboe directory (neither is a superset — probed); Schwab-empirical qualification demoted to documented fallback; OCC disqualified (bot wall).

## See also

- [ADR-0024: Serve read-model boundary](./0024-serve-read-model-boundary.md)
- [ADR-0026: Charting deprecation + report surfaces](./0026-charting-deprecation-and-reports.md)
- [ADR-0019: Two-band universe](./0019-two-band-universe-and-priority.md) · [ADR-0022: Bar retention (read-time adjustment)](./0022-bar-retention-storage-compression.md)
