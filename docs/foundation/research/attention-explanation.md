# Research: explaining why a stock is getting attention (issue #43)

Research date: 2026-10-19.
Scope: one private Discord server, non-commercial, John plus friends.
Not legal advice.

## Answer

Partly, and not from a licensed text feed.
Every text source that is cheap and reliable forbids showing its data or derived output to other people, and Reddit now requires approval that a hobby bot may not get.
The feasible launch path is catalyst facts from public-domain sources (SEC 8-K filings, earnings calendar, the price move itself) plus an LLM call with Anthropic's web search tool, which returns cited sources and avoids signing a vendor license.
Estimated running cost is about $15-65 per month, and the output must say "no clear catalyst found" when sources do not support a cause.
A richer text pipeline (Reddit posts, a paid news license) should be a post-launch feature.

## Evidence by source

### ApeWisdom
The API returns only rank, ticker, name, mentions, upvotes, rank_24h_ago and mentions_24h_ago, so no links, posts or reasons (https://apewisdom.io/api/).
It documents no terms or rate limits on that page.

### Reddit
The Data API Terms were last revised 2026-07-20 (https://redditinc.com/policies/data-api-terms).
They bind use of Reddit content "whether using the Data APIs or not", so scraping `.json` or RSS does not escape them (same page, preamble).
Section 3.1 reserves the right to charge fees and requires a separate agreement for commercial use or anything "not expressly permitted".
Section 2.4 licenses displaying user content only "as necessary" to run an app for its users, bars modifying it except to format it, and bars ML or AI training on it without rightsholder permission (same page).
Whether an LLM summary counts as modification is unsettled, so this is a terms risk even if access is granted.
Reddit's help-center article for the Responsible Builder Policy sits behind a bot challenge and I did not bypass it (https://support.reddithelp.com/hc/articles/42728983564564).
Only secondary reports say self-service closed on 2025-11-11, with approval via a support ticket and one commenter reporting repeated denials (https://r.datuan.dev/r/redditdev/comments/1oug31u/introducing_the_responsible_builder_policy_new/).
Treat Reddit as a lead that needs an approval attempt, not a dependency.

### News APIs
All of these were checked against the vendor's own page on 2026-10-19.

| Source | Access | Display to others |
|---|---|---|
| NewsAPI.org | Developer $0, 100 requests/day, 24h delay, 1 month; Business $449/month (https://newsapi.org/pricing) | Developer plan "cannot be used in staging or production (including internally)" |
| Finnhub | `/company-news` free, 1 year, headline + summary + url, North America only (https://finnhub.io/static/swagger.json) | ToS bars sharing "data or derived results" without written approval (https://finnhub.io/terms-of-service) |
| Tiingo | News API with `tickers` filter, 3 months queryable (https://www.tiingo.com/documentation/news, https://www.tiingo.com/about/pricing) | "Internal use" means no displaying or sharing with others (pricing page) |
| Massive (ex-Polygon) | News in all Stocks plans, with per-ticker sentiment and reasoning (https://massive.com/docs/rest/stocks/news) | Market data terms ban display of derived "analytics, research" (https://massive.com/legal/market-data-terms-of-service); news-specific license not read |
| Alpaca | Benzinga-sourced news, headline/summary/url, 50 per page (https://docs.alpaca.markets/reference/news-3) | Not found; the plan page does not mention news (https://alpaca.markets/data) |
| Marketaux | Free is 100 requests/day and 3 articles per request; Basic $29 gives 20 (https://www.marketaux.com/pricing) | Terms not retrieved |
| Alpha Vantage | `NEWS_SENTIMENT` labeled premium (https://www.alphavantage.co/documentation/) | License is personal, non-commercial (https://www.alphavantage.co/terms_of_service/) |
| Benzinga direct | Sales-led, no public price or terms (https://www.benzinga.com/apis/) | Unknown |

Free Marketaux cannot supply 20-50 texts per surge.
Alpha Vantage and Alpaca are the only plausible gray-area candidates, and neither blesses Discord sharing.

### X
Current docs describe only pay-per-usage pricing, no free or fixed tiers (https://docs.x.com/x-api/getting-started/pricing).
Post reads cost $0.005 each, so 1,000 posts a day (20 surges x 50 posts) is about $150 per month.
I did not read the developer agreement, so terms on AI use and display are unknown.

### SEC EDGAR and earnings
EDGAR APIs need no key and return filing history per company, which can be filtered to 8-K client-side (https://www.sec.gov/search-filings/edgar-application-programming-interfaces).
The limit is 10 requests/second with a declared User-Agent (https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data).
8-K item numbers (for example 2.02 results, 5.02 officer changes) come from the form instructions (https://www.sec.gov/files/form8-k.pdf, not machine-verified here).
Earnings dates: Nasdaq's JSON calendar is unofficial and breakable, Finnhub's free calendar is a backup (https://github.com/JWolf34/RocketStocks/blob/plan/v2-foundation/docs/foundation/audit/vendor-landscape.md).
These explain only filing-driven or earnings-driven attention, not meme or sector attention.

### LLM cost
Prices per million tokens, input/output (https://platform.claude.com/docs/en/about-claude/pricing):

- `claude-haiku-5-5`: $0.10 / $0.50 (prompts up to 100k).
- `claude-sonnet-5-5`: $2 / $10.
- Web search: $10 per 1,000 searches plus tokens.

Text summarization, assuming 20 surges/day, 50 texts of about 150 tokens, 8.5k tokens in and 1.5k out per surge:

- Haiku 5.5 is about $1 per month.
- Sonnet 5.5 is about $19 per month.
- At 5 surges/day with 20 texts, Haiku is about $0.10 and Sonnet about $2.

Web-search approach on Sonnet 5.5, assuming 3 searches and about 30k result tokens per surge (my assumption): about $0.10 per surge, so about $16 per month at 5/day and $63 at 20/day.
Caching one explanation per ticker per day lowers this.

## Recommendation

Ship a scoped version at launch, defer the text-mining version.

1. Trigger on an ApeWisdom surge, then gather facts RocketStocks already may use: the price and volume move, recent 8-Ks from EDGAR, and the next earnings date.
2. Ask Sonnet 5.5 with the web search tool for the likely cause, requiring cited URLs and an explicit "no clear catalyst" answer.
3. Post the explanation as an advisory note with citations, in line with the charter's advisory and research-grade principles.
4. Cache per ticker per day and cap searches per day to bound cost.
5. Post-launch, apply for Reddit approval via the support-ticket route and revisit a paid news license only if the web-search output proves weak.

## Open risks

- LLM causal claims can be wrong, so every claim needs a citation and a "no clear catalyst" path.
- Anthropic's web-search terms for showing results to third parties were not read.
- Reddit's own policy text and X's developer agreement were not read, and Marketaux terms were not retrieved.
- ApeWisdom has no documented terms or SLA.
- Costs above rest on assumed token volumes and should be measured with `usage` fields in the first week.
