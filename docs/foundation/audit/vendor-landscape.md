# RocketStocks vendor landscape, October 2026

Research date: 2026-10-19.
Scope: self-hosted, single-operator, non-commercial Discord bot with a small audience.
Method: vendor pricing pages, docs, ToS and machine-readable specs fetched on the research date, plus PyPI and GitHub API metadata.
Where a primary source could not be retrieved, the claim is tagged.

Confidence tags used below:

| Tag | Meaning |
|---|---|
| (P) | Read on the vendor's own page or machine-readable spec on 2026-10-19 |
| (S) | Secondary source only (blog, community docs, aggregator). Treat as a lead, not a fact |
| (U) | Unverified. Could not retrieve a source. Do not build on it without checking |

Nothing here is legal advice. Licensing readings are plain-English summaries of vendor text.

---

## 0. Bottom line

1. **Polygon.io is now Massive.** `polygon.io/pricing` returns a 301 to `massive.com/pricing` (P: https://polygon.io/pricing). Rebrand effective 2025-10-30 (S: https://fisd.net/polygon-io-is-now-massive). Official SDK moved to PyPI package `massive` (2.9.0, released 2026-10-16) (P: https://pypi.org/pypi/massive/json).
2. **Snapshot-diffing is not "options flow".** It yields unusual *volume* (cumulative volume deltas vs open interest), not trades, sweeps, or aggressor side. It is a legitimate, honest product if labeled that way. Real flow needs OPRA trade prints (section 2).
3. **Cheapest licensed route to real option prints for an individual:** Theta Data Options Pro at $160/month streams every option trade plus OPRA NBBO quotes, individual license, cancel anytime (P: https://www.thetadata.net/pricing). Unusual Whales API Basic at $150/month gives pre-computed flow alerts with ask-side and bid-side premium (P: https://unusualwhales.com/pricing?product=api).
4. **Schwab's own option stream may be a middle path.** `LEVELONE_OPTIONS` carries per-contract last price, last size, total volume, open interest, bid, ask and trade time, up to 500 keys, one streamer at a time (S: https://raw.githubusercontent.com/tylerebowers/Schwab-API-Python/main/docs/pages-raw/stream.md). Neither client library exposes a time-and-sales service (P: https://raw.githubusercontent.com/alexgolec/schwab-py/main/schwab/streaming.py).
5. **Almost every individual-tier license forbids showing data to other people.** Explicit bans: Massive, Tiingo, Finnhub, FMP, EODHD, Twelve Data, Intrinio, Theta, Alpaca, marketdata.app, Databento (personal-use definition), Cboe DataShop. Schwab's terms could not be retrieved (section 5). Derived, aggregated signals are far lower risk than raw quotes or prints, but no vendor blesses Discord sharing.
6. **Schwab developer-portal terms are a gap in this report.** The portal is a login-gated SPA and returned only a shell (P: https://developer.schwab.com/terms returned the app shell, no text). The owner has portal access and should read the market-data and display clauses directly.
7. **Free vendors that look attractive are mostly constrained for exactly this use.** NewsAPI.org Developer plan cannot be used in production "including internally" (P: https://newsapi.org/pricing). Alpha Vantage free is 25 requests/day (P: https://www.alphavantage.co/premium/). Finnhub free is personal-use only with no sharing (P: https://finnhub.io/terms-of-service).
8. **Reference data is solved for $0 and officially.** Nasdaq Trader symbol files, SEC `company_tickers.json`, OpenFIGI, House Clerk bulk disclosure zips. Nasdaq's earnings calendar JSON works but is undocumented.
9. **Side findings worth acting on:** the original `pandas-ta` GitHub repo is gone; Stooq now needs an API key; `housestockwatcher.com` no longer resolves; Capitol Trades serves a bot-protection checkpoint to scripts (section 8).

---

## 1. Equity market data

### 1.1 Comparison table (individual / non-professional tiers)

| Vendor | Free tier | Paid individual tiers | Real-time vs delayed | WebSocket | History depth | Rate limits | Python SDK |
|---|---|---|---|---|---|---|---|
| **Schwab Trader API** (individual developer) | Free with a Schwab brokerage account and approved app. No published price (S: https://mylinedchart.com/resources/articles/schwab-api-for-technical-traders-workflow-fit-checklist) | n/a | Real-time L1 for equities and options (S) | Yes. 500 keys max, one streamer at a time (S: https://raw.githubusercontent.com/tylerebowers/Schwab-API-Python/main/docs/pages-raw/client.md) | Daily and 1-30 min bars, equities and ETFs only. 1-minute about 30-35 days (S: mylinedchart link above) | 120 REST requests/min per app, 4,000 order calls/day (S: schwabdev client.md) | Unofficial only: `schwabdev` 4.0.0 (2026-08-09, active), `schwab-py` 1.5.1 (last release 2025-06-30, repo pushed 2026-08-26) (P: https://pypi.org/pypi/schwabdev/json, https://pypi.org/pypi/schwab-py/json). MIT |
| **Massive (ex-Polygon)** | Basic $0: 5 calls/min, 2 years, end of day only | Starter $29 (5y, 15-min delayed), Developer $79 (10y, 15-min delayed, adds trades), Advanced $199 (20y+, real-time, adds quotes and financials). 20% off annual (P: https://massive.com/pricing, https://massive.com/pricing?product=stocks) | Real-time only on Advanced | Starter and up | 2 / 5 / 10 / 20+ years by tier | Unlimited on paid plans | Official `massive` 2.9.0, MIT, active (P: https://github.com/massive-com/client-python). Old `polygon-api-client` last release 2025-10-30 |
| **Alpaca Market Data** | Basic $0: IEX only, 200 historical calls/min, the latest 15 minutes of SIP data withheld, WS 30 symbols | Algo Trader Plus $99: all US exchanges, unlimited WS symbols, 10,000 calls/min (P: https://alpaca.markets/data, https://docs.alpaca.markets/docs/about-market-data-api) | Free = IEX real-time; paid = full SIP real-time | Yes. Feeds `iex`, `sip`, `delayed_sip` (P: https://docs.alpaca.markets/docs/real-time-stock-pricing-data) | 7+ years (P: alpaca.markets/data) | See tier | Official `alpaca-py` 0.44.0 (2026-08-11), Apache-2.0 (P: https://pypi.org/pypi/alpaca-py/json) |
| **Tiingo** | Starter $0: 500 unique symbols/month, 50 req/hour, 1,000 req/day, 1 GB/month | Power $30/month ($300/year): 10,000 req/hour, 100,000 req/day, 40 GB/month (P: https://www.tiingo.com/about/pricing) | IEX real-time over REST and WS. Full TOPS feed needs an IEX market-data agreement since 2025-02-01; the derived "Reference Price" does not (P: https://www.tiingo.com/documentation/websockets/iex) | Yes (IEX, crypto, forex, BOATS overnight) (P: https://www.tiingo.com/documentation/general/overview) | Strong EOD history; fundamentals are a paid add-on | Hourly, daily, bandwidth caps | No official SDK per docs. Community `tiingo` 0.16.1 (2025-04-05) |
| **Databento** | $125 one-time credit for historical, expires in 6 months | Standard $199/month (live data, license fees included, "individuals and small teams"); Plus $1,750/month; Unlimited $4,500/month; usage-based historical per GB (P: https://databento.com/pricing) | Real-time | Yes (live gateway) | OPRA history from 2013-04-01 (P: https://databento.com/catalog/opra/OPRA.PILLAR) | n/a | Official `databento` 0.87.0 (2026-09-22), Apache-2.0, pre-1.0 versioning (P: https://pypi.org/pypi/databento/json) |
| **Finnhub** | Free: 60 calls/min, hard cap 30 calls/sec on top (P: https://finnhub.io/docs/api/rate-limit). `/quote` and `/calendar/earnings` (1 month) free; `/stock/candle`, splits, dividends, congressional trading, social sentiment are premium (P: https://finnhub.io/static/swagger.json) | Paid price list: (U). Page is JS-rendered and returned no prices. Third parties quote $11.99-$99.99/month (S) | Real-time US quote on free | Free WS reported at 50 symbols (S: https://nexusfi.com/d/data-providers/finnhub/) | Candles premium | 60/min free | Official `finnhub-python` 2.4.29 (2026-06-24) |
| **Financial Modeling Prep** | Basic $0: 250 calls/day, EOD only | Starter $29 ($19 annual): 300/min, 5y. Premium $69 ($49 annual): 750/min, 30y, intraday charts, corporate calendars. Ultimate $139 ($99 annual): 3,000/min, 1-min intraday (P: https://site.financialmodelingprep.com/pricing-plans) | Not clearly stated on page | WS: (U) | 5y / 30y+ | See tier. Trailing 30-day bandwidth caps: 500 MB free, 20 GB Starter | `fmpsdk` 20260824.0 (unofficial, date-versioned) |
| **Alpha Vantage** | Free: 25 requests/day (P: https://www.alphavantage.co/premium/) | $49.99 (75/min), $99.99 (150/min), $149.99 (300/min), $199.99 (600/min), $249.99 (1,200/min). Realtime and 15-min delayed US data need a separate personal-use data entitlement | Entitlement-gated | No | Intraday 25+ years, premium (P: https://www.alphavantage.co/documentation/) | Per tier | `alpha_vantage` 3.0.0, last release 2024-07-18, stale |
| **EODHD** | Free: 20 calls/day, 20/min, 1y EOD | Historian $19.99 (EOD), Active trader $29.99 (adds intraday, live US extended quotes, WS 50 tickers), Fundamentals $59.99, All-in-one $99.99. 100,000 calls/day paid, 1,000/min (P: https://eodhd.com/pricing) | Live US quotes on Active trader | Yes, 1 connection per ticker, 50 tickers | 30+ years EOD | Intraday requests cost 5 calls | Official `eodhd` 1.0.32, last release 2024-12-18 |
| **Twelve Data** | Basic $0: 8 credits/min, 800/day, WS 8 trial credits | Grow $79 (377/min), Pro $229 (1,597/min, WS 1,500), Ultra $999 (P: https://twelvedata.com/pricing). Page's structured data shows different numbers (Grow $29, Pro $99); visible figures conflict, re-check before buying | Page says US real-time included from Basic | Yes (credit-metered) | n/a | Credit-metered | `twelvedata` 1.4.0 (2026-04-27) |
| **yfinance** | Free, unofficial | n/a | Delayed or best-effort | No | Long | Unpublished. Throttling and 429s widely reported (S: https://scrapfly.io/blog/posts/guide-to-yahoo-finance-api) | `yfinance` 1.7.0 (2026-08-26), Apache-2.0, 25.5k stars, releases roughly monthly, fixes for `curl_cffi` breakage in 1.5.2 (P: https://pypi.org/pypi/yfinance/json, https://github.com/ranaroussi/yfinance/releases) |

### 1.2 Notes that change decisions

- **Schwab operational facts (S, community docs):** both "Accounts and Trading Production" and "Market Data Production" must be added to the app; app status must be "Ready for Use" (can take days) not "Approved - Pending"; refresh token lives 7 days and re-auth needs a human; multiple clients can share a token DB but only one streamer can run at a time (S: https://raw.githubusercontent.com/tylerebowers/Schwab-API-Python/main/docs/pages-raw/setupguide.md, client.md). The 7-day re-auth is the single biggest reliability risk for an unattended bot.
- **Massive trades vs quotes:** option and stock trade prints start at Developer ($79, still 15-min delayed); quotes (needed for aggressor inference) only at Advanced ($199). Stocks and Options are priced as separate products, so both Advanced = about $398/month (P: https://massive.com/pricing?product=options).
- **Alpaca's free tier is a legitimate second source** (IEX real-time, news from Benzinga back to 2015, corporate actions) but IEX volume is a small slice of consolidated volume, so volume-based signals computed on it are biased low (news: P: https://docs.alpaca.markets/docs/historical-news-data).
- **Tiingo Power at $30** is the cleanest cheap EOD plus news plus fundamentals source, but its terms are "internal use" only (section 5).
- **yfinance:** the README says the Yahoo API is "intended for personal use only" and the project is not affiliated with Yahoo (P: https://github.com/ranaroussi/yfinance). Treat as a non-critical fallback. I found no 2026 Yahoo statement either way.
- **Stooq:** a pandas-datareader issue dated 2026-04-13 reports Stooq began requiring an API key for CSV downloads in March 2026 (S: https://github.com/pydata/pandas-datareader/issues/1012). v1 uses Stooq, so check it.

---

## 2. Options data

### 2.1 Three classes of product

| Class | What you get | Can detect |
|---|---|---|
| **Chain snapshots** (greeks, IV, bid/ask/last, cumulative volume, prior-day OI) | State at a poll instant | Volume delta between polls, V/OI ratio, IV jumps, premium estimates |
| **Quotes and bars** (NBBO stream, 1-min bars) | Continuous quote and OHLC | Spread/IV regime changes, bar volume. Still no individual trades |
| **Trade prints** (OPRA time and sales with exchange and condition codes) | Every trade: price, size, exchange, conditions, timestamp | Blocks, sweeps (cluster by contract and time across venues), premium per trade, with NBBO: aggressor side inference |

### 2.2 Vendors, individual price, and what each really delivers

| Vendor | Individual price | Product | Prints? | Notes |
|---|---|---|---|---|
| **Schwab** | $0 (with account) | REST chains (greeks, IV, OI, volume) plus `LEVELONE_OPTIONS` stream | Last-trade fields only, change-coalesced, 500 keys | `LEVELONE_OPTIONS` fields include Last Price, Last Size, Total Volume, Open Interest, Quote Time, Trade Time, Delta..Rho (S: stream.md above). `OPTIONS_BOOK` is a separate service. No TIMESALE service in either client library |
| **Massive options** | Basic $0 (EOD), Starter $29, Developer $79, Advanced $199 | Chain snapshot with greeks, IV, quotes, trades, OI. Real-time greeks and IV from Starter; daily OI; Developer adds trades; Advanced adds quotes | Yes from Developer, 15-min delayed. Real-time at Advanced | Trade records have conditions and exchange; the docs page mentions no sweep field (P: https://massive.com/pricing?product=options, https://massive.com/docs/rest/options/overview) |
| **Databento OPRA** | Standard $199 (live, license fees included) or usage-based historical | OPRA.PILLAR schemas: trades, TCBBO, CBBO-1s/1m, OHLCV, statistics, definition. History from 2013-04-01 | Yes | Whether Standard covers OPRA live, and the per-GB OPRA price, were not retrievable (U). Databento publishes `metadata.get_cost` for exact quotes and gives $125 credit |
| **Theta Data** | Options Value $40, Standard $80, Pro $160 | Value: snapshots, 1-min intervals, 6y. Standard: tick data, chain snapshots, every NBBO quote, "15K trade streams", 10y. Pro: option-root snapshots, stream every option trade, 14y (P: https://www.thetadata.net/pricing) | Yes. Full trade stream only on Pro | Individual license "personal use only, no redistribution or business use." OPRA non-pro fee of $1.25/user/month is paid by Theta (P: https://thetadata.net/articles/2026-05-29-opra-fee-guide-for-options-market-data). SDK `thetadata` 1.0.15 (2026-10-14), Python >=3.12. Stocks and Indices are separate tabs I could not render (U) |
| **Unusual Whales API** | Basic $150, Advanced $375 (Business from $625 annual) | 100+ endpoints: flow alerts (REST, WS, Kafka), option trades, Greeks, GEX, dark pool, congress, 2y history. WS "option trades" channel expects 6-10M records/day (P: https://api.unusualwhales.com/docs/llms.txt) | Yes, plus pre-computed alerts and `ask`/`bid` premium split | "Retail and API plans are for individual use"; redistribution licenses via Enterprise. Retail Basic website plan is $50/month ($42 annual) and has no API (P: https://unusualwhales.com/pricing). Page is inconsistent on whether WS is Basic or Advanced only |
| **Tradier** | $0 brokerage minimum; Lite $0, Pro $10/mo, Pro Plus $35/mo; "API access included" (P: https://tradier.com/pricing) | Real-time equities and options only for brokerage account holders; sandbox is 15-min delayed with no Greeks. Greeks (from ORATS) update hourly. 120 market-data req/min, 60 sandbox. Streaming filters: trade, quote, summary, timesale, tradex; one market stream at a time (P: https://docs.tradier.com/docs/market-data.md, https://docs.tradier.com/docs/rate-limiting, https://docs.tradier.com/docs/streaming) | `timesale` stream exists; whether it emits for OCC option symbols: (U) | Needs a brokerage account opened for data. Cheapest candidate to test for free prints |
| **tastytrade** | $0 data for customers (S) | DXLink streaming, 5 sessions, 25,000 subscriptions per session. Official docs list Quote, Trade, Greeks, Summary, Profile, Candle (P: https://developer.tastytrade.com/docs/concepts/streaming/). The unofficial SDK also ships a `TimeAndSale` event class (P: https://raw.githubusercontent.com/tastyware/tastytrade/master/tastytrade/dxfeed/__init__.py) | Possible via `TimeAndSale`: (U) for options | Requires a fully onboarded customer account. API license is "personal, limited, non-exclusive, revocable" (S: https://assets.tastyworks.com/production/documents/open_api_terms_and_conditions.pdf). SDK `tastytrade` 13.2.3 (2026-08-07) |
| **marketdata.app** | Free (100 credits/day, 24h delayed), Starter $12 annual / $30 monthly (15-min delayed API), Trader $30 annual / $75 monthly (real-time, 100,000 credits/day), Quant $125, Prime $250 (P: https://www.marketdata.app/pricing/) | Chain endpoint: bid/ask/last/volume/OI/IV/Greeks. Real-time needs the OPRA agreement. OI published once per day after close. Greeks not stored historically (P: https://www.marketdata.app/docs/api/options/chain) | No | Policy defines redistribution as "any method of making Market Data's data available to others" (P: https://www.marketdata.app/docs/account/data-policies/data-redistribution/). Official SDK `marketdata-sdk-py` 1.3.0 |
| **Alpaca options** | Basic $0 = "indicative" feed (modeled quotes, trades 15-min delayed); Algo Trader Plus $99 = real-time OPRA (1,000 WS quotes) | Historical from Feb 2024 only (P: https://docs.alpaca.markets/docs/historical-option-data) | Trades on paid OPRA feed: (U) | |
| **Intrinio** | Individual $150/month "personal use only"; Startup $333 and up for commercial | Individual includes OptionsEdge (synthetic price, IV, Greeks, "without OPRA licensing complexity"), EOD options. Raw real-time OPRA only on Enterprise | No | Individual plan states "No redistribution or display" (P: https://intrinio.com/pricing) |
| **ORATS** | Delayed Data API $199, Live Data $299, Live Intraday $599, All-In $899 | IV surfaces, strikes with greeks, history to 2007. Time & Sales is a $299 add-on tool | Add-on only | Does not provide US stock prices. Live data requires separate signed agreements (P: https://orats.com/data-api). Out of hobby range |
| **Cboe DataShop / LiveVol** | LiveVol Pro $380-420/month (or $350 annual), plus market-data fees | Time and sales since 2011, scanning, alerts, Excel RTD add-on; no API mentioned (P: https://datashop.cboe.com/livevol-pro). Open-Close Volume Summary (customer / pro / BD / MM, buy/sell, open/close) is Cboe exchanges only, price not published (P: https://datashop.cboe.com/cboe-options-open-close-volume-summary) | Yes (Pro) | Raw data "for internal use only." Out of hobby range |
| **Market Chameleon** | About $99/month "Total Access" (S: https://daytradingz.com/market-chameleon-review/) | Unusual option volume report, CSV export, 15-min delayed by default | No | No API found. Affiliate-style sources only |

### 2.3 What "unusual options activity" detection actually requires

| Signal | Needs | Achievable from 5-min chain snapshots? |
|---|---|---|
| Volume / open interest ratio | Cumulative volume plus prior-day OI. Cutoffs seen in the wild: >1.0 "new positions" (S: https://www.quantvps.com/blog/unusualwhales-scanner-review), >2x flag and >10x highly unusual (S: https://apify.com/nexgendata/options-unusual-activity) | **Yes.** This is the core of what screeners label "unusual volume" |
| Premium committed | Trade price x size. Proxy: delta-volume x 100 x mid | **Approximate.** Price of the incremental trades is unknown; mid at snapshot time is a proxy. Common floor is about $20,000 (S: the apify page above) |
| Block (large single trade) | Individual print size. One Python reference uses >=1,000 contracts | **No.** Delta-volume cannot distinguish one 1,000-lot from 1,000 one-lots |
| Sweep | Multi-venue prints close in time. Unusual Whales "Repeated Hits" groups same-chain trades within 100 ms, needs >5 transactions and a market-cap-scaled premium floor ($10K, $50K above $50B, $100K above $500B) (S: https://unusualwhales.substack.com/p/a-breakdown-of-every-type-of-flow) | **No.** Sub-second resolution is impossible at 5-minute cadence. The Schwab stream is also coalesced |
| Aggressor / side (buy vs sell) | Trade price vs prevailing NBBO at trade time. UW exposes `ask`-side and `bid`-side premium; analysts treat near-ask as buys, which is an assumption (S: https://api.unusualwhales.com/docs/kafka/types/FlowAlert, https://unusualwhales.substack.com/p/unusual-whales-flow-filter-walkthrough) | **No.** Best proxy is last price vs bid/ask at snapshot time, and only for the most recent trade |
| Opening vs closing | Next-day OI (OCC computes once per day after the close, published at the start of the next trading day, not intraday) (S: https://marketchameleon.com/Reports/openInterestByExpiration) or Cboe Open-Close data | **Yes, next morning.** A follow-up "OI confirmed +X vs Y contracts traded yesterday" alert is a natural fit and cheap |
| Multi-leg / spread / floor exclusion | Condition codes on prints | **No** |
| IV spike or skew shift on a contract | Snapshot IV | **Yes** |

**Honest framing for the snapshot approach:** it supports "unusual volume vs open interest, with estimated premium, IV change and DTE/moneyness filters, confirmed next morning with OI delta." It cannot support the words "sweep", "block", "bought at the ask", or "opening" at alert time.

### 2.4 A middle path worth a spike (design suggestion, not verified)

Two-stage design using only Schwab:
1. REST chain snapshots every 5 minutes find contracts with large volume deltas or V/OI jumps. REST budget: 120 requests/min (S) is about 600 chain calls per 5-minute cycle if nothing else shares it. This is my arithmetic from the community-documented limit, not a Schwab statement.
2. Subscribe the shortlist to `LEVELONE_OPTIONS` (up to 500 keys) and record each change in Last Price, Last Size and Total Volume. This gives last-trade-level events and a bid/ask-at-trade proxy for the hot contracts only.

Open questions to test before relying on it: whether the 500-key cap is per service or shared with equity L1; whether trades are dropped when several print between pushes (the stream sends changes only: S, stream.md); whether the single-streamer limit collides with the existing equity stream.

### 2.5 Price bands for true prints

| Band | Options | Caveat |
|---|---|---|
| $0 | Tradier brokerage account (timesale) or tastytrade DXLink (`TimeAndSale`) | Both (U) for option symbols. A one-hour spike would settle it |
| Under $100 | Massive Options Developer $79 (15-min delayed prints, no quotes) | Delayed, and no quotes means no aggressor inference |
| $100-$200 | Theta Options Pro $160; Unusual Whales API Basic $150; Alpaca Algo Trader Plus $99 | Theta is raw prints plus NBBO (you build the logic); UW is pre-built alerts; Alpaca prints are (U) |
| Above $200 | Databento Standard $199 (OPRA coverage U), Massive Advanced Options $199 (quotes, real-time), Cboe LiveVol $380+ | |

---

## 3. Reference and corporate data

| Need | Best $0 source | Terms and caveats | Alternatives |
|---|---|---|---|
| Symbol master (Nasdaq-listed and others) | `nasdaqlisted.txt` and `otherlisted.txt` at https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt. Fields: symbol, name, market category, test issue, financial status, round lot; otherlisted adds exchange, CQS symbol, ETF flag, NASDAQ symbol. "Updated periodically through-out each day"; last row is a File Creation Time stamp (P: https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs). Fetched live today, file stamp 10/19/2026 12:11 | The definitions page shows no usage terms; the copyright page returned no text (U) | Alpha Vantage `LISTING_STATUS` (includes delisted, supports `date`) (P: https://www.alphavantage.co/documentation/); FMP symbol lists |
| Ticker to CIK, issuer identity | `https://www.sec.gov/files/company_tickers.json` and `company_tickers_exchange.json`. SEC "10 requests/second", declared User-Agent with contact required (P: https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data). A scripted request with a generic browser User-Agent was blocked with "Request Rate Threshold Exceeded" today | Public domain data; SEC warns the file is not guaranteed complete | |
| Instrument identifiers (FIGI, ISIN mapping) | OpenFIGI: mapping 25 req/min without key, 250 with key; 10 vs 100 jobs per request; search 5 vs 20/min (P: https://www.openfigi.com/api/documentation) | Terms page not retrieved (U). Docs say free and open | `openfigi-client` 0.4.2 (2025-04-05) |
| Earnings calendar | Nasdaq JSON `https://api.nasdaq.com/api/calendar/earnings?date=YYYY-MM-DD` returns time (pre/after), consensus EPS, estimate count, market cap, last-year date. Worked today with a browser User-Agent; a `python-requests` User-Agent got a connection error | Undocumented, unofficial, can break or be blocked at any time. Nasdaq site terms not retrieved (U) | Finnhub `/calendar/earnings`, free but 1 month of history (P: https://finnhub.io/static/swagger.json); Alpha Vantage `EARNINGS_CALENDAR` 3/6/12-month horizon CSV; FMP corporate calendars on Premium+ |
| Splits and dividends | Alpaca corporate actions (free tier includes them: P https://alpaca.markets/data); Massive Basic includes corporate actions (P: https://massive.com/pricing?product=options); Tiingo splits and dividends endpoints | Finnhub `/stock/split` and `/stock/dividend` are premium (P: spec above). Alpha Vantage `SPLITS` exists | yfinance actions (unofficial) |
| Trading calendar | `pandas_market_calendars` 5.5.0 (2026-10-05), `exchange_calendars` 4.13.2 (P: PyPI) | | |

---

## 4. Sentiment and alternative data

| Source | Status in Oct 2026 | Cost | Constraint |
|---|---|---|---|
| **ApeWisdom** | API live and unauthenticated. `GET /api/v1.0/filter/{filter}/page/{n}` with filter one of all, all-stocks, all-crypto, 100 results per page, fields rank, ticker, name, mentions, upvotes, rank_24h_ago, mentions_24h_ago. Returned 737 results across 8 pages today (P: https://apewisdom.io/api/) | Free | No rate limit, auth, SLA or terms documented; `/terms` returns 404. Operated by a small site that also runs a market-cap ranking site. Single point of failure; cache aggressively and degrade gracefully |
| **Reddit Data API** | Terms last revised 2026-07-20; Reddit "reserves the right to charge fees", commercial use needs a separate agreement (P: https://redditinc.com/policies/data-api-terms). Self-service access closed 2025-11-11 under the Responsible Builder Policy; new access needs approval (S: https://r.datuan.dev/r/redditdev/comments/1oug31u/introducing_the_responsible_builder_policy_new/) | Free tier reported at 100 QPM, non-commercial (S: https://www.techloy.com/reddit-api-pricing-in-2026-complete-guide-for-developers-and-businesses/) | Approval gate (reported 2-4 weeks, S). Not a good primary dependency; ApeWisdom already abstracts it. `praw` 8.0.3 (2026-08-12) is maintained |
| **Stocktwits** | The developer page says "We unfortunately won't be accepting new registrations" (page footer shows 2021, undated) (P: https://api.stocktwits.com/developers). The ToS (revised 2026-07-10) prohibits automated extraction "except as expressly authorized in writing or through an approved API" (P: https://stocktwits.com/about/legal/terms). One unauthenticated GET to `api.stocktwits.com/api/2/streams/symbol/AAPL.json` returned HTTP 200 with data | n/a | Works technically, but using it is against the ToS as written. Do not build on it |
| **Quiver Quantitative** | API tiers: Hobbyist $30/month ($25 annual), Trader $75 ($62.50 annual), both non-commercial. Hobbyist has congress trading, holdings, lobbying, government contracts, off-exchange trading, Trump trades; Trader adds insider trading, hedge fund activity, news feed. Startup $125 for commercial under $1M revenue (P: https://api.quiverquant.com/pricing/) | $25-75 | Rate limits not published. SDK `quiverquant` 0.2.6 (2026-05-07). Terms of Service not read (U) |
| **Congressional trades, free primary sources** | House: Clerk publishes bulk annual disclosure zips, `2026FD.zip` last modified 2026-10-16 (P: https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2026FD.zip). Senate: eFD search portal, HTML, click-through (P: https://efdsearch.senate.gov/search/home/) | Free | Senate page says it is unlawful to use the reports for commercial purposes (other than news media), credit ratings, or solicitation; penalty up to $10,000 (P: link above). Non-commercial Discord use is consistent with that, but monetizing would not be. PTRs are PDFs and need parsing |
| **Congressional trades, third-party** | Capitol Trades now serves a "Vercel Security Checkpoint" and HTTP 429 to scripted clients (P: curl today). I did not attempt to bypass it. `housestockwatcher.com` no longer resolves in DNS. FMP, Finnhub (premium), Unusual Whales API and Quiver all expose congress endpoints | | v1 scraping of Capitol Trades should be assumed broken or about to break |
| **News** | NewsAPI.org Developer plan: 100 req/day, 24h delay, 1 month history, not allowed in "staging or production environment (including internally)"; Business $449/month (P: https://newsapi.org/pricing). Marketaux: Free 100 req/day and 3 articles/request, Basic $29, Standard $49, Pro $99 (P: https://www.marketaux.com/pricing). Finnhub `/company-news` free, 1 year, North America only (P: spec). Alpaca news (Benzinga) in free tier, history to 2015 (P: https://docs.alpaca.markets/docs/historical-news-data). Alpha Vantage `NEWS_SENTIMENT`, Tiingo news (3 months queryable on standard) | | v1's News API usage is out of compliance with the vendor's free plan if it runs as a deployed bot. Replace with Finnhub, Alpaca, or Marketaux |

---

## 5. Redistribution and display terms (individual tiers)

Question asked: may a non-professional, non-commercial individual show derived data or alerts to other people in a Discord server?

Legend: **FORBIDS** = text I read that bars showing, sharing or redistributing. **GRAY** = no explicit ban retrieved, but license scope is personal. **UNVERIFIED** = could not read the governing text.

| Vendor / source | Reading | Evidence |
|---|---|---|
| **Massive (Polygon)** | **FORBIDS.** Market Data Terms (updated 2025-08-28): "strictly for display use only"; may not be "publicly displayed" or distributed; may not "build an application intended for use by end users other than you"; prohibition reaches "any data, charts, analytics, research, or other works based on, referring to, or derived from the Market Data" (§§1, 2, 5(c)). Non-Professional means "solely for their own personal, non-business use" | P: https://massive.com/legal/market-data-terms-of-service. Individuals ToS also bars sharing account or API keys (§3.3): https://massive.com/legal/individuals-terms-of-service |
| **Tiingo** | **FORBIDS.** Pricing page: both individual tiers are internal use only; internal use means "personal use, without displaying or sharing the data with other people or organizations" | P: https://www.tiingo.com/about/pricing. Full ToS not retrieved (404) |
| **Finnhub** | **FORBIDS.** "You hereby agree to not redistribute or share access to data" with any third party, including results derived from it, without written approval; personal plans cannot be used by a business even internally | P: https://finnhub.io/terms-of-service |
| **FMP** | **FORBIDS.** "Displaying or redistributing data sourced from FMP requires a specific Data Display and Licensing Agreement." Pricing page separates Personal and Commercial use | P: https://site.financialmodelingprep.com/pricing-plans |
| **EODHD** | **FORBIDS.** Non-Professional users "may not sell, resell, retransmit, redistribute, display, or grant access" to the information or services "whether in its original or repackaged form." Derived data not addressed | P: https://eodhd.com/financial-apis/terms-conditions |
| **Twelve Data** | **FORBIDS.** Core license is Internal Use; external display or redistribution needs a Redistribution Rights add-on or agreement (§2.2(e), §2.3(b)); free-tier data may not be used commercially; chat rooms not specifically addressed | P: https://twelvedata.com/terms |
| **Alpaca** | **FORBIDS.** Support article: "you cannot redistribute Alpaca API data" (dated 2022-11) | P: https://alpaca.markets/support/redistribute-alpaca-api. Underlying agreements are Nasdaq Global Subscriber Agreement and NYSE Market Data Display Services Agreement, text not read (https://alpaca.markets/disclosures) |
| **Databento** | **FORBIDS in the personal category.** Personal use is market data "used solely for a non-professional individual's own investment decisions, research, or educational purposes and cannot be shared or leveraged for profit" (as quoted by search engine; page itself is JS-rendered) | S: https://databento.com/docs/portal/live-data |
| **Theta Data** | **FORBIDS.** Individual plan: "Personal use only, no redistribution or business use." Fee guide: "Redistributing any Theta Data data is prohibited without a commercial agreement"; OPRA redistributor license is $1,500/month or $650/month query-only | P: https://www.thetadata.net/pricing, https://thetadata.net/articles/2026-05-29-opra-fee-guide-for-options-market-data |
| **Intrinio** | **FORBIDS.** Individual plan: "No redistribution or display" | P: https://intrinio.com/pricing |
| **marketdata.app** | **FORBIDS in effect.** Redistribution is "any method of making Market Data's data available to others"; personal plans are for own use only. The fetch tool's reading is that Discord posts of derived alerts or screenshots match that definition (their policy does not name Discord) | P: https://www.marketdata.app/docs/account/data-policies/data-redistribution/ |
| **Cboe DataShop** | **FORBIDS raw redistribution.** Raw data "for internal use only"; external distribution of derived data is a separate fee and approval tier | P: https://datashop.cboe.com/cboe-options-open-close-volume-summary |
| **Unusual Whales API** | **GRAY, leaning forbids.** "Retail and API plans are for individual use"; redistribution licenses go through Enterprise. Terms text not retrieved. They sell a Discord bot product for flow in a server | P: https://unusualwhales.com/pricing?product=api, https://api.unusualwhales.com/docs; bot: https://unusualwhales.com/discord-bot |
| **Quiver** | Non-commercial rights only for Hobbyist and Trader. Display-to-others text not read | P: https://api.quiverquant.com/pricing/ |
| **Alpha Vantage** | **GRAY.** Free key license is personal, non-commercial, with a broad commercial definition. Redistribution clause not retrieved (PDF not extractable here) | S via search snippet of https://alphavantage.co/terms_of_service |
| **Tradier** | **UNVERIFIED.** Market data docs are silent on redistribution. Underlying exchange agreements for the brokerage account apply | P: https://docs.tradier.com/docs/market-data.md |
| **tastytrade** | **UNVERIFIED.** License is personal, limited, non-transferable, revocable; redistribution clause not read | S: https://assets.tastyworks.com/production/documents/open_api_terms_and_conditions.pdf |
| **Schwab** | **UNVERIFIED.** Portal terms not retrievable. Third parties say individual access is for building and testing against your own accounts and that redistribution/commercial use needs Schwab review and exchange-data agreements `schwabdev` states it "acts in accordance with Schwab's API terms" without citing a clause | S: https://apis.io/plans/charles-schwab/charles-schwab-plans-pricing/, https://mylinedchart.com/resources/articles/broker-api-commercial-access-what-it-takes-to-ship-an-app |
| **yfinance / Yahoo** | **GRAY, personal use only** per the project README; Yahoo's current ToS not retrieved | P: https://github.com/ranaroussi/yfinance |
| **ApeWisdom** | **GRAY.** No terms page (404); site has a privacy policy only | P: https://apewisdom.io/api/ |
| **Reddit Data API** | **Non-commercial only, subject to approval.** Revenue from the API or commercial use needs written approval | P: https://redditinc.com/policies/data-api-terms |
| **Stocktwits** | **FORBIDS** automated extraction except through an approved API | P: https://stocktwits.com/about/legal/terms |
| **Nasdaq Trader symbol files / SEC / House Clerk / OpenFIGI** | Public or government data. SEC requires declared User-Agent and 10 req/s. Senate reports carry a statutory commercial-use bar. Nasdaq Trader terms page not retrievable | See section 3 |
| **NewsAPI.org** | **FORBIDS production use** on the free plan | P: https://newsapi.org/pricing |

**Practical risk ladder for a small friends-only server (my judgment, not legal advice):**

1. Lowest risk: alerts built only from your own computation over aggregated or thresholded data ("XYZ: call volume 6x open interest, est. premium over $1M"), no raw bid/ask/last, no per-trade prints, no price streams, no screenshots of vendor UI.
2. Medium: derived levels and indicator values on a single symbol.
3. Highest: posting raw quotes, chains, or trade prints; offering a bot that any server member can query for live prices (this is "display to other end users").

The licensed alternative for sharing flow in a server is the vendor's own product (Unusual Whales Discord Bot) or buying a business tier. OPRA itself prices redistribution at $1,500/month or $650/month query-only (P: Theta fee guide above), which is the cost structure vendors are protecting.

---

## 6. Prior art

### 6.1 Commercial Discord and Telegram products, and what they center on

| Product | Focus | Price (as found) | Source |
|---|---|---|---|
| **Unusual Whales Discord Bot** | Options flow alerts, dark pool, congress. Commands such as `/flow_alerts`, `/flow_ticker`, per-expiry OI and volume (command list from a search-result summary of the bot page) | Price not on the page I could read; a 2025 comparison table reportedly listed about $30/month (S, comparison URL not captured) | https://unusualwhales.com/discord-bot |
| **Tradytics** | Live flow, repeat flow, AI signals (Prophet swing, Bullseye intraday), dark pool, scanners, congress, hedge fund and insider tracking (P: site nav, via https://www.tradytics.com/pricing 404 page) | $69 individual; $199 Discord bundle per a 2025 roundup (S, roundup URL not captured); pricing page 404 today | https://www.tradytics.com |
| **Alpha.bot** | On-demand charts (80+ indicators), heatmaps, news, scheduled posts, paper trading, natural-language requests | $6.99, $24.99, $99.99 per month by question quota (P) | https://www.alpha.bot/ |
| **Stocks Bot, StockAlerter.io** | Price-movement alerts | Not stated | https://peakbot.pro/blog/best-discord-bots-for-trading-communities-2026 |
| **Cheddar Flow, FlowAlgo** | Web flow terminals, with Discord communities | About $85-99 and $149 per month (S, low quality: AI-answer aggregate and SEO review sites) | https://www.parse.gl/prompts/p/best-real-time-options-flow-data-subscription--e46d0841-58ef-4f41-a4b7-8f40d396e510 |
| **TradingView webhook alerts to Discord** | The most common DIY pattern: alert fires, webhook posts JSON. Webhooks require 2FA, ports 80/443, 3-second response timeout, may fail occasionally. Which paid plan is needed was not stated on the page (U) | | https://www.tradingview.com/support/solutions/43000529348-about-webhooks/ |

Feature pattern across the products: (1) options flow and dark pool first, (2) per-symbol on-demand lookups (charts, news, quotes), (3) scheduled digests, (4) price and level alerts, (5) congress and insider feeds as secondary content. RocketStocks' screener-report plus popularity-surge plus earnings mix is closer to a digest bot than to a flow bot.

### 6.2 Open-source projects and patterns worth borrowing

Open-source Discord stock bots are thin. A GitHub search for Python Discord stock-alert bots returned only small repos (largest about 10 stars), e.g. https://github.com/belgrano9/discord-bot (5 stars, price alerts, portfolio) and https://github.com/a0rtega/tickergram-bot (Telegram, yfinance, per-user watchlists, Redis). `DiscordAlertsTrader` goes the other direction (reads Discord alerts to place trades): https://pypi.org/project/DiscordAlertsTrader/. Better patterns come from adjacent projects.

| Project | Why look | Health (GitHub API, 2026-10-19) | License |
|---|---|---|---|
| **OpenBB** (https://github.com/OpenBB-finance/OpenBB) | Provider abstraction: a standard query model per data type with pluggable provider "fetchers" (extract and transform separation). Directly relevant to swapping vendors without rewriting the bot. `openbb` 5.0.0 released 2026-09-29 | 74k stars, active | NOASSERTION in API; check before reuse |
| **freqtrade** (https://github.com/freqtrade/freqtrade) | Operational shape of a long-running trading bot with Telegram/webhook notifiers, config validation, dry-run, persistence | 55.6k stars, active | GPL-3.0 |
| **NautilusTrader** (https://github.com/nautechsystems/nautilus_trader) | Event-driven engine with a clean data-adapter boundary and a deterministic clock; `nautilus_trader` 1.231.0 (2026-10-15) | 30k stars, active | LGPL-3.0 |
| **Lumibot** (https://github.com/Lumiwealth/lumibot) | Has a Schwab broker integration (S: https://lumibot.lumiwealth.com/brokers.schwab.html) | 2.1k stars, active | GPL-3.0 |
| **tastytrade SDK** (https://github.com/tastyware/tastytrade) | Async DXLink streaming client with typed event models including `TimeAndSale` and `Greeks` | 258 stars, active | MIT |
| **TradingView-Screener** (https://github.com/shner-elmo/TradingView-Screener) | Python query builder for TradingView's screener endpoint (3.2.2, 2026-09-14). v1 already depends on this style of data; unofficial and ToS-fragile | 1.3k stars, active | MIT |
| **radon** (https://github.com/joemccann/radon) | Recent project reconstructing market structure from dark pool, OTC and options flow; small (33 stars), architecture reference only | active | NOASSERTION |
| **pandas-ta-classic** (https://github.com/xgboosted/pandas-ta-classic) | Maintained fork of `pandas-ta`; the original repo `twopirllc/pandas-ta` now returns Not Found from the GitHub API (P, today). PyPI `pandas-ta` last release 0.4.71b0 on 2025-09-14, Python >=3.12 (P). `pandas-ta-classic` 0.8.32 (2026-09-14) | 459 stars, pushed today | MIT |

Other notes from the dependency scan: `mplfinance` last release 0.12.10b0 on 2023-08-02 (P: PyPI); `backtesting` (backtesting.py) is AGPL-3.0 (P: GitHub API), worth a glance for any code that the bot exposes over a network; `discord.py` 2.7.1 (2026-03-03) is current.

---

## 7. Recommended stacks

Assumes the owner keeps Schwab (already integrated, $0) and accepts that Schwab's display-to-others terms are still to be read.

### (a) $0 per month

**Stack A: Schwab plus free reference plus honest "unusual volume".**

| Layer | Source |
|---|---|
| Equity quotes, bars, movers, chains | Schwab (REST and L1 stream) |
| Second equity source for resilience | Alpaca free (IEX bars, news, corporate actions) |
| Symbol master | Nasdaq Trader files (daily refresh) plus SEC `company_tickers.json` with a declared User-Agent |
| Earnings | Nasdaq earnings JSON (behind an adapter and a fallback), Finnhub free calendar as backup |
| Retail attention | ApeWisdom (cached, degrade to "unavailable") |
| Congress | House Clerk bulk zips plus parsing |
| News | Finnhub `/company-news` or Alpaca news |
| Options | 5-minute chain snapshots: V/OI, estimated premium, IV change, next-morning OI confirmation. Optional Schwab `LEVELONE_OPTIONS` for a shortlist |

Trade-offs: no prints, so no sweep, block or side language. Schwab 7-day refresh-token re-auth is a standing operational chore (S). Several free sources are undocumented (Nasdaq earnings, ApeWisdom) and need adapters plus staleness indicators. Free vendor terms mostly forbid sharing, so keep to derived signals.

**Stack A+ (still $0): add a Tradier Lite brokerage account (no minimum)** to test free real-time options data and the `timesale` stream. If option timesale works, this closes most of the print gap for free. (U until tested.)

### (b) Under $50 per month

**Stack B: Stack A plus Quiver Hobbyist ($25 annual or $30 monthly) and optionally Tiingo Power ($30).**

| Add | Why |
|---|---|
| Quiver Hobbyist | Clean congress API instead of scraping; also lobbying and government contracts. Non-commercial license |
| Tiingo Power $30 or marketdata.app Starter $12 annual | Only if a second paid-grade EOD/IEX/news source is wanted. Tiingo terms are internal-use only |

Trade-offs: nothing under $50 buys licensed option prints from a mainstream vendor. The first vendor print product is Massive Options Developer at $79, 15-minute delayed. Spend here buys data quality and legal hygiene on the congress and news legs, not flow.

### (c) Under $200 per month

**Stack C1: Stack A plus Theta Data Options Pro ($160).** Streams every option trade plus every OPRA NBBO quote; 14 years of history for backtesting thresholds; "cancel anytime" per the pricing page. You build sweep clustering (same contract, multiple exchanges, short window), premium, side inference (trade price vs NBBO at trade time) and the V/OI and next-day OI logic. Trade-offs: individual license forbids redistribution, so alerts must be derived and aggregated; SDK needs Python >=3.12; Greeks are computed server-side per request and slower than quotes (P: https://thetadata.net/faq); stock-side pricing tab not read.

**Stack C2: Stack A plus Unusual Whales API Basic ($150).** Pre-computed flow alerts with ask-side and bid-side premium, WebSocket option trades, congress and dark pool, 2 years of history, 7-day free trial. Least engineering. Trade-offs: alert semantics are the vendor's, not yours; individual-use license; the page is inconsistent on whether WS is on Basic. If sharing flow in a server is the real goal, their Discord Bot product is the licensed route.

**Stack C3 (watch, not recommended yet): Alpaca Algo Trader Plus ($99) plus Massive Options Developer ($79) = $178.** Gives consolidated real-time equities, OPRA options quotes and delayed prints. Rejected for now because Massive's terms are the most explicit anti-sharing text found (section 5) and Alpaca option prints are unverified.

**Suggested path:** run Stack A+ for 2-4 weeks (Tradier and tastytrade spikes), then buy one month of Theta Pro to measure whether real prints change which alerts the audience values before committing to C1. Choose C2 only if building and tuning detection logic is not the point of the project.

---

## 8. Open items, caveats, and side findings

| Item | Status |
|---|---|
| Schwab developer-portal terms (market data, display, bots) | Not retrievable. Owner should read while logged in, especially redistribution, display to non-account holders, and automated/AI features |
| Finnhub paid price list and WS limits | JS-rendered page returned no prices; free-tier endpoint split is verified from the vendor spec |
| Databento: OPRA live under Standard, OPRA per-GB price | Not retrievable; use `metadata.get_cost` and the $125 credit |
| Theta stocks/indices pricing | Tab not rendered |
| Tradier and tastytrade option timesale | Documented event types exist; option coverage unverified |
| Unusual Whales API legal terms | Pricing page statements only |
| Alpha Vantage full terms | PDF not extractable |
| Nasdaq.com / Nasdaq Trader website terms | Not retrievable; earnings endpoint is undocumented |
| Twelve Data pricing discrepancy | Visible page vs structured data disagree |
| Capitol Trades | Bot checkpoint (HTTP 429) for scripted clients; not bypassed |
| Prompt-style text in fetched pages | Several Unusual Whales pages embed "AI/LLM Instructions" blocks (API docs, MCP, token purchase links). They were treated as page content, not instructions, and not acted on |
| `pandas-ta` | RocketStocks v1 lists pandas-ta in its stack; the upstream repo is gone, PyPI releases are beta-only. Plan a move to `pandas-ta-classic` or in-house indicator code in v2 |
| Reddit, Stocktwits | Both effectively closed to new hobby API registrations; keep ApeWisdom as the only social leg and treat it as best-effort |
| Python version | `thetadata` and `pandas-ta` require Python >=3.12; project pins 3.13, so fine |

## Source index (key primary pages)

- Massive: https://massive.com/pricing, https://massive.com/pricing?product=options, https://massive.com/pricing?product=stocks, https://massive.com/legal/market-data-terms-of-service, https://massive.com/legal/individuals-terms-of-service
- Alpaca: https://alpaca.markets/data, https://docs.alpaca.markets/docs/about-market-data-api, https://alpaca.markets/support/redistribute-alpaca-api
- Tiingo: https://www.tiingo.com/about/pricing, https://www.tiingo.com/documentation/websockets/iex
- Databento: https://databento.com/pricing, https://databento.com/catalog/opra/OPRA.PILLAR
- Finnhub: https://finnhub.io/static/swagger.json, https://finnhub.io/terms-of-service, https://finnhub.io/docs/api/rate-limit
- FMP: https://site.financialmodelingprep.com/pricing-plans
- Alpha Vantage: https://www.alphavantage.co/premium/, https://www.alphavantage.co/documentation/
- EODHD: https://eodhd.com/pricing, https://eodhd.com/financial-apis/terms-conditions
- Twelve Data: https://twelvedata.com/pricing, https://twelvedata.com/terms
- Theta Data: https://www.thetadata.net/pricing, https://thetadata.net/articles/2026-05-29-opra-fee-guide-for-options-market-data
- Unusual Whales: https://unusualwhales.com/pricing?product=api, https://api.unusualwhales.com/docs/llms.txt
- Tradier: https://tradier.com/pricing, https://docs.tradier.com/docs/market-data.md, https://docs.tradier.com/docs/streaming
- marketdata.app: https://www.marketdata.app/pricing/, https://www.marketdata.app/docs/account/data-policies/data-redistribution/
- Intrinio: https://intrinio.com/pricing; ORATS: https://orats.com/data-api; Cboe: https://datashop.cboe.com/livevol-pro
- Reference: https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs, https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data, https://www.openfigi.com/api/documentation, https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2026FD.zip
- Social: https://apewisdom.io/api/, https://redditinc.com/policies/data-api-terms, https://stocktwits.com/about/legal/terms, https://api.quiverquant.com/pricing/
- Schwab (community): https://github.com/tylerebowers/Schwab-API-Python, https://schwab-py.readthedocs.io/en/latest/
