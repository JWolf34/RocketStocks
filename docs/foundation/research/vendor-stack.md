# Research: vendor stack (issue #48)

Research date: 2026-10-19.
Scope: one private Discord server, non-commercial, John plus friends.
Not legal advice.
Confidence tags: (P) read on the primary source today, (S) secondary source, (U) unverified.

## Answer

Run the whole launch on free sources plus one paid subscription: Sharadar Prices, Full History, for survivorship-free daily history.
Schwab stays the live spine (quotes, intraday, daily bars for the band, option chains, fundamentals, paper-trading quotes), with raw display allowed per #42.
Everything else is free: ApeWisdom, SEC EDGAR, Nasdaq earnings calendar, Nasdaq Trader symbol files, `exchange_calendars`.
Total is $24.92/month on the annual plan ($299/year) or $39/month month-to-month.
The daily full options-chain snapshot is affordable on the Schwab quota and should start now, because the 250-session clock only runs while collecting.

## Table

| Need | Primary | Fallback | Cost/month | Terms fit | Notes |
|---|---|---|---|---|---|
| Quotes, intraday moves, paper-trading fills | Schwab Trader API (batched quotes) | none that may be shown in Discord | $0 | Raw display accepted by John (#42); risk recorded | 7-day token, see below |
| Daily bars, liquid band and watchlists (live pipeline) | Schwab price history | Sharadar (lands 17:30 ET) | $0 | Same as above | Schwab depth is about 30 years but survivors only |
| Survivorship-free daily history, ticker master, corporate actions, base rates | Sharadar Prices, Full History | EODHD Historian, $19.99 | $24.92 annual or $39 monthly | Derived stats and backtests are allowed; raw bars may not be shared or shown | See comparison below |
| Session calendar, phantom-row rejection | `exchange_calendars` 4.13.2 checked at ingest | `pandas_market_calendars` 5.5.0 | $0 | Library | Reject any bar not on a session |
| Liquid band (top ~500-1,000 by dollar volume, price > $5) | Nasdaq Trader symbol files plus 20-day dollar volume from Sharadar bulk table | Schwab batched quotes | $0 | Public files; Nasdaq terms page not retrieved (U) | Rebuild weekly, no per-symbol calls |
| Attention surge trigger | ApeWisdom `/api/v1.0/filter/all-stocks` | none | $0 | No terms published (404) | Single point of failure, flagged |
| Recent 8-Ks for the facts block | SEC EDGAR submissions API | none needed | $0 | Public; 10 req/s, declared User-Agent | Item numbers verified, see below |
| Next earnings date (forward) | Nasdaq earnings calendar JSON | Alpha Vantage free `EARNINGS_CALENDAR`, then an EDGAR-cadence estimate labelled as estimated | $0 | Unofficial endpoint; Alpha Vantage terms unreadable (U) | Flagged |
| Earnings history and timing (reaction base rates) | EDGAR 8-K Item 2.02 with acceptance timestamp | Nasdaq historical earnings | $0 | Public | Gives pre-market vs after-close from the timestamp |
| Fundamentals | Schwab fundamental projection plus EDGAR XBRL `companyfacts` | Sharadar fundamentals, +$30/month for the bundle (not recommended) | $0 | Schwab per #42; EDGAR public | Analyst, ownership, insider cut with yfinance |
| Options Report and daily chain snapshot | Schwab option chains | none | $0 | Schwab per #42; stored for John's own study | See affordability |
| Technicals, charts, IV rank, put/call, max pain | Computed locally from the above | - | $0 | Derived | No vendor |

## Survivorship-free history, compared

Event studies need about 20 years including delisted names, because v1 has only 69 ended tickers out of 10,654 (#45).

| Option | Price | Delisted coverage | Terms | Verdict |
|---|---|---|---|---|
| Sharadar Prices, Full History | $39/month or $299/year (P: https://www.sharadar.com/pricing) | Active and delisted, back to December 1997, about 21,000 stock tickers, daily files at 17:30 and 23:30 ET (P: https://sharadar.com/prices) | One person; no sharing or display of the data; research outputs and summary statistics need no attribution; on termination only derived works that cannot reproduce the data survive (P: https://www.sharadar.com/terms) | Recommended |
| EODHD Historian | $19.99/month or $199/year (P: https://eodhd.com/pricing) | `exchange-symbol-list/US?delisted=1`; EOD only for names delisted before 2018 (P: https://eodhd.com/financial-apis/delisted-stock-companies-data-2); ticker reuse returns the wrong company (S: https://incoherency.co.uk/notes/20260507.html) | Bars "displaying" and redistribution; derived data unaddressed (P: https://eodhd.com/financial-apis/terms-conditions) | Fallback; cheaper, weaker identity handling |
| Tiingo Power | $30/month (P: https://www.tiingo.com/about/pricing) | I counted its public `supported_tickers` file today: 7,523 US stock listings ended before Oct 2025, but only 8 ended in 1997-2008 and 49 through 2009 | Internal use only | Rejected: dot-com and 2008 losers are missing |
| Norgate Platinum | $630/year, 6- or 12-month terms only (P: https://norgatedata.com/stockmarketpackages.php) | Delisted back to 1990 | Sharing barred; the Python package needs the Windows-only updater running (P: https://pypi.org/project/norgatedata/) | Rejected: Windows dependency on a Linux VM |
| Massive Starter / Advanced | $29 for 5 years; $199 for 20+ years (P: https://massive.com/pricing?product=stocks) | Delisted aggregates not confirmed (U) | Bars derived analytics from display (landscape section 5) | Rejected |
| Databento | $199/month Standard | Daily US equity bars not confirmed on the pricing page (U) | Personal use cannot be shared | Rejected: overpriced for daily bars |

The Sharadar terms matter in one place: the base rate shown inside an alert is a summary statistic, which is allowed.
Charts and quotes in Discord must be built from Schwab bars, never from Sharadar bars.
If John cancels, the raw table must be purged, so treat the cost as permanent for as long as base rates are refreshed.

## Schwab exposure

The refresh token lives 7 days, cannot be extended, and may expire earlier; the access token lasts 30 minutes (S: https://schwab-py.readthedocs.io/en/latest/auth.html).
Re-auth needs John in a browser; v1's Discord paste-URL flow is the one piece worth keeping.
Plan: track token age, DM John at 24 hours and 6 hours before expiry, and make Sunday the standing re-auth day.
While the token is dead, Schwab-fed alerts and Reports pause with one ops notice, paper orders queue, Sharadar supplies daily bars, and ApeWisdom, EDGAR and earnings keep running.
A true Schwab outage uses the same degrade path.
No licensed substitute exists that may be shown in Discord (Alpaca, Massive, Tiingo and EODHD all bar display), so there is no live-quote fallback for friends.
Unofficial client libraries only: `schwabdev` and `schwab-py`; v1 wraps `schwab-py`.

## Options snapshot affordability

Calls per day: about 800 optionable names in the band, a few extra calls for index and mega-cap chains that return 502 unbounded, and a 5 percent retry allowance (39 of 900 bounded calls returned 502, per `docs/v2/schwab-rest-budget.md`) is roughly 1,000-1,300 calls.
At the documented 120/min that is 9-11 minutes; the July probe ran clean at 240/min.
Market data has no documented daily cap; documented capacity is about 172,000 calls/day, so the snapshot uses under 1 percent.
Batched quotes and daily bars for 1,000 names add a few hundred calls per day.
Run it after the close; the next day's snapshot carries the next-morning open interest the study needs, so no second pass is required.
Storage: v1 averaged 1.3 million rows per day over about 3.9k tickers (25.9M rows in 20 days, #41), and a 750-name liquid band should land near 1-1.5 million rows per day.
That is 250-380 million rows per year, about 20-40 GB raw at roughly 100 bytes per row, and about 2-5 GB compressed (assumed 8-10x; measure in week one).
Verdict: affordable on quota and disk; start at launch.

## Flagged sources

- Nasdaq earnings calendar JSON: undocumented, needs a spoofed browser User-Agent, can break any day.
- ApeWisdom: documented API but no terms, no SLA, no history endpoint, so v1's 16.5 months of rows are irreplaceable; keep collecting.
- Nasdaq Trader symbol files: official static files, terms text not retrieved.
- Schwab client libraries: third-party, not Schwab SDKs.
- Cut by ADR-0025 and not at launch: yfinance, TradingView, Capitol Trades (bot-challenged), Stooq (now needs a key), News API (free plan bars production), Tiingo.

## EDGAR check

I called `data.sec.gov/submissions/CIK0000320193.json` today with a declared User-Agent.
It returned `items` (for example `2.02,9.01`) and `acceptanceDateTime` per filing, so 8-K item filtering and earnings timing need no scraping (P).

## Open risks

- Schwab terms: the Developer Program Agreement is still unread; John accepted the risk in #42.
- Schwab and Sharadar volumes differ (stream volume ran 9-15 percent above official candles, per the roadmap); store a source stamp and compute base rates and detection on one source per signal.
- Sharadar delisted coverage was read from its product page; verify against a known set of delisted names (for example 2001-2002 and 2008 failures) on day one before relying on it.
- Nasdaq calendar failure degrades the next-earnings date to an estimate, and earnings-reaction detection to 8-K Item 2.02 timestamps.
- Reddit, news and options-flow vendors (Theta Data about $160/month) stay post-launch.

## Questions for John

1. Sharadar billing: annual $299 (recommended), month-to-month $39, or EODHD at $19.99 despite weaker identity handling and display ban?
2. Start the daily options-chain snapshot immediately, before the rest of the launch is built? Recommended yes.
3. Accept the unofficial Nasdaq calendar as primary behind an adapter, with an estimated-date fallback? Recommended yes.
4. Accept the weekly Sunday Schwab re-auth ritual as the permanent cost of the free data spine?
