# RocketStocks v1 inventory and audit

Snapshot: 2026-10-19, branch `phase-2-spec-010`, HEAD `986feb7`. Read-only audit of `legacy/src/rocketstocks/` (v1, code-frozen since 2026-06-11).
Paths below are relative to `legacy/src/rocketstocks/` unless they start with `docs/`, `legacy/tests/` or `.github/`.

Evidence labels used on every claim about usage or value:

- **S** (strong): reproduced by running code/tests/commands, or directly readable in code with no inference.
- **W** (weak): inferred from git history, docs, design intent, or naming.
- **N** (none): no evidence exists in the repo or on this machine.

Important up front: **there is no usage data anywhere I can reach.** No command/telemetry logging exists in code (grep, section 4.1), local `logs/` and `legacy/logs/` are 0-byte files, and I have no DB access. So every "is this feature used" claim below is W or N. Section 4.6 lists the production-DB queries that would turn that into S.

---

## 0. Headline numbers

| Metric | Value | Evidence |
|---|---|---|
| v1 source | 32,116 LOC, 197 `.py` files (`bot` 7,296 / `core` 11,171 / `data` 4,898 / `backtest` 4,676 / `eda` 2,778 / `api` 1,267) | S (`wc -l`) |
| v1 tests | 33,191 LOC, 2,768 test functions, **2,804 passed, 0 failed, 0 skipped, 0 xfail**, 14.4 s (second run 12.8 s, same result) | S (`.venv/bin/pytest legacy/tests -q -p no:cacheprovider`) |
| Slash commands | 61 (no prefix commands; `command_prefix='$'` at `bot/bot.py:114` but zero `@commands.command`) | S |
| Scheduled units | 14 APScheduler cron jobs + 16 `discord.ext.tasks` loops = 30 | S |
| Alert types | 6 user-visible (+ edit-in-place update path) | S |
| DB tables | 28 created by `data/schema.py` | S |
| Vendor clients | 10 (`data/clients/*`, 1,295 LOC) | S |
| History | 1,943 commits, 2023-06-05 to 2026-07-18. v1 code last touched functionally 2026-05-21; freeze declared 2026-06-11; **one** code change since (`2b1a8d0`, import-path fix, 2026-07-05) | S |
| Share of v1 LOC that is NOT the live alert/report/data path | **about 40%** (12.7k of 32.1k): backtest 4,676 + eda 2,778 + api 1,267 + direction_model 607 + dead charting 461 + orphaned composite_score/signal_confirmation 260 + paper trading 2,693 | S (LOC), W (the judgment that these are "off path") |

For scale against the bloat worry: v2 at this snapshot is about 4.9k LOC of `src` and about 5.0k LOC of tests across `libs/ services/ tools/`, plus 32 ADRs, 8 specs and about 4.6k lines under `docs/v2/` (S, `wc -l`). v2 currently has roughly as many lines of design prose as lines of code.

---

## 1. User-facing surface

### 1.1 Slash commands (61)

LOC is the approximate length of the command handler body (from `bot/cogs/*.py`, via a decorator-to-next-def scan, scripted in `scratchpad/scripts/cmds.py`). "Tables" lists tables touched directly or through repositories.

**Watchlists (`bot/cogs/watchlists.py`, 360 LOC, 8 commands).** Create/rename/delete/list/view/set/add/remove named and personal watchlists. Ticker validation via `tickers`. Tables: `watchlists`, `tickers`.

| Cmd | LOC |
|---|---|
| `/watchlist add` `remove` `view` `set` `create` `delete` `list` `rename` | 46 / 43 / 27 / 39 / 38 / 40 / 23 / 30 |

**Reports (`bot/cogs/reports.py`, 1,162 LOC; 7 + `/news`)**

| Cmd | LOC | What the user gets | Sources | Tables |
|---|---|---|---|---|
| `/report ticker` | 57 | Stock report embed (+ "full" variant): quote, fundamentals, technicals, next earnings, SEC filings, analyst, EPS forecast, recent bot alerts | Schwab quote+fundamentals, yfinance, NASDAQ, SEC | `tickers`, `ticker_stats`, `daily_price_history`, `upcoming_earnings`, `historical_earnings`, `alerts`, `channel_config` |
| `/report watchlist` | 64 | Same report for every ticker on a watchlist | same | + `watchlists` |
| `/report compare` | 60 | Up to 5 tickers side by side vs benchmark | Schwab quotes+fundamentals | `daily_price_history`, `popularity`, `ticker_stats` |
| `/report technical` | 43 | Trend/momentum/volatility/volume/levels/confluence cards for one ticker (+SPY) | Schwab, yfinance float | `daily_price_history`, `ticker_stats` |
| `/report options` | 43 | IV, max pain, skew, put/call, unusual activity, greeks | Schwab chain | `options_chain_history` (IV history), `daily_price_history` |
| `/report popularity` | 37 | Top mentioned stocks by subreddit source | ApeWisdom live | none |
| `/report politician` | 40 | Recent trades by a US politician | Capitol Trades scrape | `ct_politicians` |
| `/news` | 18 | News search | News API | none |

**Data lookups (`bot/cogs/data.py`, 836 LOC, 19 commands).** Mostly DM'd CSV/JSON exports or small embeds. Docs say "about 15"; actual is 19 (S).

| Cmd | LOC | Output | Sources | Tables |
|---|---|---|---|---|
| `/data price` | 78 | OHLCV CSV (daily or 5m), DM | DB, Schwab | `daily_price_history`, `five_minute_price_history` |
| `/data financials` | 57 | IS/BS/CF CSVs, DM | yfinance | none |
| `/data tickers` | 22 | CSV of tracked universe | DB | `tickers` |
| `/data earnings` | 72 | EPS table + CSV | DB | `historical_earnings` |
| `/data sec-filing` | 29 | Recent filings | SEC EDGAR | `tickers` (CIK) |
| `/data fundamentals` | 70 | JSON, DM | Schwab | none |
| `/data options` | 68 | Full chain JSON, DM | Schwab | none |
| `/data popularity` | 50 | Mention history CSV | DB | `popularity` |
| `/data quote` | 34 | Quote card | Schwab | none |
| `/data stats` | 20 | Volatility/class/BB card | DB | `ticker_stats` |
| `/data movers` `losers` | 25 / 36 | Top 10 up/down | Schwab movers | none |
| `/data analyst` `ownership` `insider` `short-interest` | 26 / 20 / 39 / 49 | Cards | yfinance (+Schwab for short interest) | none |
| `/data news` | 26 | Headlines | News API | none |
| `/data forecast` | 28 | EPS forecasts | NASDAQ | none |
| `/data screener` | 27 | On-demand TradingView screen | TradingView | none |

**Alerts (`bot/cogs/alerts.py`, 1,311 LOC, group `/alert`)**

| Cmd | LOC | What | Tables |
|---|---|---|---|
| `/alert stats` | 78 | Surge/signal confirmation rates + price outcomes for 24h/7d/30d | `popularity_surges`, `market_signals`, `alerts`, `daily_price_history` |
| `/alert history` | 53 | Recent alerts for a ticker with price outcomes | `alerts`, `daily_price_history` |
| `/alert summary` | 35 | Alerts grouped by type since a point in time | `alerts` |
| `/alert subscribe` | 11 | Dropdown to get role-pinged per alert type | `alert_roles` |

**Paper trading (`bot/cogs/paper_trading.py`, 952 LOC, group `/trade`, 8 commands).** `buy` 116, `sell` 112, `portfolio` 17, `history` 21, `cancel` 62, `reset` 36, `leaderboard` 46, `performance` 47. Per-guild virtual $10,000 portfolios, market-closed orders queued and filled at open. Schwab quotes. Tables: `paper_portfolios`, `paper_positions`, `paper_transactions`, `paper_snapshots`, `paper_pending_orders` (`data/paper_trading_store.py`, 574 LOC).

**Ops/config**

| Cmd | File | LOC | What | Tables |
|---|---|---|---|---|
| `/server setup`, `/server status` | `config.py` (520) | 32 / 73 | Per-guild channel (reports/alerts/screeners/notifications/trade) and tz/notification-filter setup | `channel_config`, `bot_settings`, `alert_roles` |
| `/notifications filter`, `status` | `notifications.py` (146) | 20 / 28 | Job-health event filter, last events | none (in-memory ring buffer) |
| `/schwab status`, `/schwab auth` | `schwab_auth.py` (274) | 30 / 46 | Token health; Discord-native OAuth (paste redirect URL in a modal) | `schwab_tokens` |
| `/admin logs` `update-5m` `update-daily` `test-alert` `test-screener` `test-report` | `admin.py` (622) | 34 / 9 / 9 / 30 / 27 / 30 | Log download, forced price refresh, dummy previews | `daily_price_history`, `five_minute_price_history` |
| `/sync`, `/help` | `utils.py` (165) | 10 / 13 | Command sync, category help | none |

### 1.2 Components / views / modals (27 classes)

| Class | File | Notes |
|---|---|---|
| `AlertButtons` | `bot/views/alert_views.py:57` | URL buttons + callbacks "Add to Watchlist" (works) and "Generate Report" (works). `timeout=None` but no `custom_id`, not re-registered (see W6) |
| `PopularitySurgeAlertButtons` | `alert_views.py:118` | + ApeWisdom link |
| `WatchlistSelect` | `alert_views.py:17` | dropdown, 60 s timeout |
| `PoliticianTradeButtons` | `alert_views.py:130` | **dead: 0 references anywhere** (S) |
| `StockReportButtons` | `report_views.py:9` | 4 URL buttons; "Get news" works; **"Generate chart" is a stub that replies the literal text "Generate chart!"** (`report_views.py:30-32`; the same stub string exists since commit `cd8224c`, 2024-11-15, i.e. about 23 months) (S) |
| 8 URL-only screener/report button views | `report_views.py:46-155` | Gainer/Volume/Popularity screener, Popularity/Politician/Technical/Comparison/Options report |
| `TradeConfirmView`, `ConfirmResetView` | `paper_trading_views.py` | buy/sell and reset confirmation |
| `AlertSubscriptionSelect/View`, `ManageSubscriptionsButton`, `SubscriptionEntryView` | `subscription_views.py` | only view registered persistently (`bot/bot.py:100`, `custom_id="manage_subscriptions"`) |
| `ChannelSetupView`, `_ChannelTypeSelect`, `BotSettingsView` | `cogs/config.py:155-303` | `/server setup` UI |
| `HelpView`, `HelpCategorySelect` | `cogs/utils.py:114-135` | `/help` |
| `ConfirmDeleteView` | `cogs/watchlists.py:11` | |
| `PasteURLView`, `SchwabCallbackModal` | `cogs/schwab_auth.py:59-130` | OAuth paste flow |

### 1.3 Scheduled jobs

**APScheduler, `core/scheduler/jobs.py:218-243`** (UTC, `misfire_grace_time=600`, in-memory jobstore, all wrapped with `emitter.job_wrapper` except the token check):

| Job | Schedule (UTC) | Source | Writes |
|---|---|---|---|
| Update tickers | daily 05:00 | NASDAQ screener | `tickers` |
| Insert new tickers | Sun 06:00 | NASDAQ + SEC | `tickers` |
| Update upcoming earnings | Fri 06:00 | NASDAQ (50 days, one call per weekday) | `upcoming_earnings` |
| Remove past earnings | Tue-Sat 06:00 | none | `upcoming_earnings` |
| Update historical earnings | Tue-Sat 07:00 | NASDAQ | `historical_earnings` |
| Daily price history | Tue-Sat 03:00 | Schwab, one call per ticker for the whole universe | `daily_price_history` |
| 5m price history | Tue-Sat 04:00 | Schwab, whole universe | `five_minute_price_history` |
| Update politicians | Sun 07:00 | Capitol Trades HTML scrape | `ct_politicians` |
| Check Schwab token expiry | every 6 h | DB | emits notifications |
| Classify tickers | Tue-Sat 05:30 | Schwab quotes, ApeWisdom top 100, DB history | `ticker_stats` |
| Collect options chain history | Tue-Sat 21:30 | Schwab chain per ticker, whole universe | `options_chain_history` |
| Enrich tickers | hourly :05 (240/run) | Tiingo + SEC | `tickers` |
| Import delisted tickers | Sun 09:00 | Tiingo | `tickers` |
| Load delisted price history | daily 10:00 | Tiingo then Stooq | `daily_price_history` |

**`discord.ext.tasks` loops (16)**

| Loop | Where | Schedule | Behavior |
|---|---|---|---|
| `post_alerts_date` | `alerts.py:153` | 06:00 UTC | date-separator message |
| `detect_popularity_surges` | `alerts.py:167` | 30 min | Tier-1 alert detection (market days only) |
| `process_alerts` | `alerts.py:177` | 5 min | 5 parallel pipelines (market days, not EOD) |
| `post_popularity_screener` | `reports.py:118` | 30 min, **no market-hours gate** | inserts `popularity` snapshot, posts screener, updates alert universe |
| `post_volume_screener` | `reports.py:144` | 5 min | unusual-volume screener |
| `post_volume_at_time_screener` | `reports.py:162` | 5 min | **not posted**, only feeds the alert-ticker universe |
| `post_gainer_screener` | `reports.py:173` | 5 min | pre/intra/post-market gainers |
| `post_earnings_spotlight` | `reports.py:212` | 12:30 UTC | random ticker reporting today |
| `post_weekly_earnings` | `reports.py:243` | Mon 12:00 UTC | weekly earnings screener |
| `post_earnings_results` | `reports.py:261` | 10 min, 12:00-00:59 UTC | EPS actual vs estimate for **named-watchlist** tickers reporting today (yfinance) |
| `update_earnings_calendar` | `reports.py:331` | 06:00 UTC | creates Discord scheduled events for watchlist earnings |
| `execute_pending_orders` | `paper_trading.py:103` | 1 min | fills queued paper orders at open |
| `daily_snapshot` | `paper_trading.py:179` | 21:05 UTC | portfolio snapshots |
| `weekly_roundup` | `paper_trading.py:781` | Sun 18:00 UTC | paper-trading leaderboard post |
| `drain_notifications` | `notifications.py:53` | 10 s | posts job success/failure events |
| `check_latency` | `notifications.py:72` | 30 s | websocket latency warning |

### 1.4 Alert types (6)

Pipeline code: `bot/cogs/alerts.py`; content classes `core/content/alerts/*`; logic `core/analysis/*`. Documented in `docs/alerts/*.md` (1,156 lines, written 2026-03-25).

| Alert | Trigger (as coded) | Sources | Tables | Approx LOC (pipeline + content + logic) |
|---|---|---|---|---|
| Popularity Surge (Tier 1) | ApeWisdom top 1,000, mention surge / rank jump / new entrant / velocity spike, min 15 mentions; every 30 min | ApeWisdom, Schwab quotes | R `popularity`; W `popularity_surges`, `alerts` | detect 130 + content 155 + `popularity_signals` 239 + `surge_store` 113 |
| Momentum Confirmation | surge confirmed when price-since-flag z-score >= 1.5 (>= 15 min after flag, within 24 h) | Schwab quotes | R/W `popularity_surges`; R `ticker_stats`; W `alerts` | pipeline 83 + content 154 + `alert_strategy` 302 |
| Volume Accumulation | vol z >= 2.5 AND abs(price z) < 1.0 AND divergence >= 1.5 AND an options contract with volume/OI >= 3 | Schwab quotes + options chain | W `market_signals`, `alerts` | pipeline 158 + content 150 + `volume_divergence` 79 + `options_flow` 95 + `market_signal_store` 220 |
| Breakout | confirms pending VA signals after >= 10 min with price z >= 1.5; expires after 8 h | Schwab quotes | R/W `market_signals`; W `alerts` | pipeline 119 + content 164 |
| Watchlist Mover | z-score vs ticker's own 20d returns, threshold scaled 1.5-3.0 by volatility; blue-chip BB + volume path | Schwab quotes | R `watchlists`, `ticker_stats`, `daily_price_history`; W `alerts` | pipeline 46 + content 54 + `alert_strategy`/`classification` |
| Earnings Mover | same logic for tickers with earnings today, enriched with EPS result | Schwab quotes | R `upcoming_earnings`, `earnings_results`; W `alerts` | pipeline about 107 + content 116 |

Shared lifecycle: `bot/senders/alert_sender.py` (120 LOC) dedupes per (date, ticker, alert_type), edits/re-posts via a momentum z-score rule (`core/analysis/momentum.py`), optional role pings (`alert_roles`).

### 1.5 Screeners and reports (posted)

| Item | Cadence | Notes |
|---|---|---|
| Popularity screener | 30 min | top-20 table, edit-in-place per day (`bot/senders/report_sender.py:40`) |
| Unusual volume screener | 5 min market hours | TradingView |
| Gainers screener | 5 min | premarket / intraday / aftermarket variants, TradingView |
| Weekly earnings screener | Mondays | file attachment |
| Earnings spotlight | daily 12:30 UTC | random reporting ticker |
| Earnings result report | 10 min poll | watchlist tickers only |
| Paper-trading weekly roundup | Sundays | |

Screeners also create system-generated watchlists (`reports.py:806-822`) and populate the in-memory alert universe (`StockData.alert_tickers`, `data/stockdata.py:77-92`).

### 1.6 Dead or stub user-visible pieces (S)

- "Generate chart" button: stub (above). No `/chart` command exists; `core/charting/` (461 LOC, 3 files, 25 tests) has **zero importers** in `bot/ core/ api/` (grep for `mplfinance|charting|Chart\b`).
- `PoliticianTradeButtons`: unused. `Alerts.build_volume_accumulation` (`alerts.py:1030`): unused.

---

## 2. Data sources (`data/clients/`)

None of the clients has retry/backoff (grep for tenacity/backoff/retry finds only `ratelimit.sleep_and_retry`, which is a blocking throttle, not a retry). **13 `requests.get` calls, zero `timeout=` anywhere in `bot core data api`** (S).

| Client | Provides | Auth | Cost | Robustness | Features that depend on it |
|---|---|---|---|---|---|
| Schwab (`schwab.py`, 295) | quotes, daily + 5m history, options chains, movers, fundamentals | OAuth app key/secret; token JSON in `schwab_tokens`; the code models an expiring token (`TokenStatus.EXPIRING_SOON`, 6-hourly check) so manual `/schwab auth` is a recurring operator task (17 commits on the token flow; Schwab's 7-day refresh-token life is vendor behavior I did not re-verify, W) | brokerage account; no price in repo (W) | client-side `AsyncLimiter(120/60s)`; 429 raises `SchwabRateLimitError` (callers `break` out of their loop, no backoff); `OAuthError` sets `_token_invalid` and fires a notification; HTTP errors on history calls are logged and return an **empty DataFrame** (silent gap); no timeout/retry | **everything live**: all alerts, `/report *`, `/data` quotes/options/fundamentals/movers, `/trade`, nightly history and chain jobs, classification |
| NASDAQ (`nasdaq.py`, 102) | ticker universe, earnings by date, EPS, forecasts | none (browser User-Agent spoofed, `nasdaq.py:13-18`); `nasdaq_api_key` in settings is **never read** | free, unofficial endpoint | `@limits(5/60s)` + `sleep_and_retry` (blocking `time.sleep`), `raise_for_status`, no timeout | ticker universe, earnings calendar/pipeline/spotlight, `/data forecast`, `/data earnings` |
| ApeWisdom (`ape_wisdom.py`, 67) | Reddit mention rankings | none | free | `requests.get` per page, no status check, no timeout; `data['results']` KeyError on an error body | popularity screener, surge + confirmation alerts, classification (meme), `/report popularity`, `/data popularity` |
| TradingView (`tradingview.py`, 169) | gainers, unusual volume, market caps | none (`tradingview_screener` lib, unofficial scanner API) | free, unofficial | no limiter, no try/except in client; callers use `to_thread` and `_run_task` isolation | 3 screeners, alert-ticker universe, `/data screener` |
| yfinance (`yfinance_client.py`, 169) | financials, analyst, ownership, insider, float/short interest, earnings result | none | free, unofficial scraper | `@limits(5/s)`, broad `except Exception` returning empty | `/data` (6 cmds), full stock/technical report sections, earnings-result report |
| SEC EDGAR (`sec.py`, 107) | CIK map, submissions/filings, SIC | User-Agent with the owner's email hard-coded (`sec.py:14`) | free, official | `@limits(5/s)`, no timeout; `get_cik_from_ticker`, `get_filings_from_today`, `get_accounts_payable`, `get_company_facts` have **0 callers** | `/data sec-filing`, stock report filings, ticker enrichment |
| Tiingo (`tiingo.py`, 123) | ticker metadata, delisted universe + prices | API key | free tier assumed; not recorded in repo (W) | `@limits(10/s)`, errors swallowed to empty/None | ticker enrichment, delisted import/history |
| Stooq (`stooq.py`, 36) | fallback delisted prices via pandas-datareader | none | free | returns empty on any exception | delisted history fallback only |
| News API (`news.py`, 40) | headlines | API key | free dev tier assumed; not recorded (W) | none | `/news`, `/data news`, "Get news" button |
| Capitol Trades (`capitol_trades.py`, 187) | politicians, trades | none; **HTML scraping** with Tailwind class selectors (`:96-118`) | free | `raise_for_status`, no timeout, brittle selectors | `/report politician`, weekly refresh job |

Unused settings: `eodhd_api_token`, `dolthub_api_token`, `nasdaq_api_key` (`core/config/settings.py:25-27`) have no readers (S). Five of the ten sources are unofficial or scraped (NASDAQ screener/earnings endpoints, TradingView scanner, yfinance, Capitol Trades HTML, Stooq via pandas-datareader); the other five have public or keyed APIs (Schwab, SEC, Tiingo, News API, ApeWisdom). That fragility is the main reliability risk ADR-0025 already identifies.

---

## 3. Database tables (28; `data/schema.py:35-380`)

Cross-referenced with `scratchpad/scripts/tablexref.py` (regex over all SQL) and then by caller inspection.

| Table | Writer(s) | Reader(s) | Notes |
|---|---|---|---|
| `tickers` | insert/update/enrich/import-delisted jobs | nearly everything; nightly loops iterate all rows | the universe |
| `upcoming_earnings` | Fri job; remove-past job | earnings pipeline, spotlight, calendar events, reports | |
| `historical_earnings` | Tue-Sat job | `get_historical_earnings`: reports, alerts, `/data earnings`, api | |
| `earnings_results` | `post_earnings_results` (`reports.py:320`) | earnings pipeline enrichment (`alerts.py:909`), posted-today check | |
| `watchlists` | `/watchlist *`, screeners (system), seed | alerts, reports, classify (`class:` overrides), backtest filters | |
| `popularity` | popularity loop (`reports.py:131`) | surge detection, reports, `/data popularity`, eda, api, backtest | |
| `popularity_surges` | `alerts.py:307` | confirmation, confidence stat, `/alert stats` | |
| `market_signals` | VA pipeline (`alerts.py:664`) | breakout pipeline, `/alert stats` | column `composite_score` holds the divergence score (name drift) |
| `alerts` | `alert_sender` via `DiscordState` | dedupe/edit, `/alert history|summary|stats`, stock report "recent alerts" | closest thing to a usage record (see 4.6) |
| `reports` | `send_screener` | `send_screener` | screener message ids |
| `daily_price_history` | nightly Schwab loader; delisted loader | alerts, reports, classify, `/data price`, backtest, eda, api | |
| `five_minute_price_history` | nightly Schwab loader, whole universe | **only** `/data price` (5m), api, backtest, eda | no live alert/report reads it (S) |
| `ct_politicians` | Sun scrape | `/report politician` only | |
| `channel_config` | `/server setup`, legacy env migration | every poster via `iter_channels` | |
| `ticker_stats` | classify job | alerts z-scores, reports, `/data stats`, backtest filters | **`avg_rvol_20d`, `std_rvol_20d` are read (stats card shows "Avg RVOL 20d") but never written** (`jobs.py` stats dict omits them; `ticker_stats.py:27` writes `None`) (S) |
| `alert_roles` | `/alert subscribe`, setup | role mention helper | |
| `schwab_tokens` | OAuth flow, client `token_write_func` | `Schwab.init_client` | singleton row |
| `bot_settings` | `/server setup` | `setup_hook` | tz, notification filter |
| `options_chain_history` | nightly chain job, whole universe, partitioned by month | `get_iv_history`/`get_latest_iv` (`/report options`, api) only; **`get_oi_history`, `get_chain_snapshot` have 0 callers** | partitions created only at startup (W5 below) |
| `backtest_runs/results/strategy_stats/trades` (4) | `backtest` CLI only | `backtest` CLI only | nothing in the bot touches them (S) |
| `paper_portfolios/positions/transactions/snapshots/pending_orders` (5) | `/trade *`, loops | `/trade *`, roundup | `CLAUDE.md` calls these `holdings/transactions/portfolio_snapshots` (wrong) |
| `iv_history` | none: dropped 2026-05-20 (`2f31631`) | none | still named in `_DROP_ALL_SCRIPT` (`schema.py:401`) and in `.claude/docs/data.md` |

Written-but-never-read: no table is completely unread. Closest cases: `five_minute_price_history` and the backtest tables (read only by offline tooling), `options_chain_history` (only two derived views are consumed). Read-but-never-written: two `ticker_stats` columns. Dead repo methods: 31 data-layer methods have no non-test callers (`scratchpad/scripts/deadmethods.py data`), e.g. `paper_trading_store.upsert_position/delete_position/get_user_transactions_in_range`, `market_signal_store.is_already_signaled/update_observation/get_latest_signal`, `surge_store.is_already_flagged`, `discord_state.get_volume_message_id/update_volume_message_id`.

---

## 4. Evidence of value and use

### 4.1 Telemetry in code (N)

None. Grep for `telemetry|analytics|usage|prometheus|statsd|sentry|on_app_command_completion|on_interaction` finds nothing in the bot path; there is no command-usage logging and no global app-command error handler (`bot/bot.py` has none; only `admin.py:612` and `schwab_auth.py:117` handle errors). What exists instead:

- `EventEmitter` job health events posted to a Discord channel (`core/notifications/`, 191 LOC): success/failure/elapsed per job. That is operational telemetry, not usage.
- Every alert is persisted in `alerts` with a JSONB `alert_data` payload, and `/alert stats` computes surge/signal "confirmation rates" from `popularity_surges` and `market_signals` (`core/analysis/alert_performance.py`). Caveat: "confirmed" is defined by the alert's own price-z trigger, so it is self-referential, not an independent hit rate.
- `eda/_memlog.py` peak-RSS logging (research tool only).

### 4.2 Git churn per feature area (S for counts; W for what they imply)

Method: full `git log --name-only` (8,704 lines saved in `scratchpad/audit/gitlog.txt`), paths normalized across `discord/cogs/*` (2024-2025 layout), `src/rocketstocks/*` and `legacy/src/rocketstocks/*`, excluding the two mass-move commits `2aa30ef` (S001 relocation) and `87e0284` ("init on WSL"). `git log --follow` **undercounts** (alerts cog shows 40 commits vs 168 by path union) because the 2026-02-21 restructure was a copy, not a rename. Script: `scratchpad/scripts/churn2.py`.

| Area | Commits total | before 2026 | 2026 | since freeze (2026-06-11) | Active months | First - last |
|---|---|---|---|---|---|---|
| cog reports | 273 | 234 | 39 | 1 (import fix) | 11 | 2024-11-15 - 2026-07-05 |
| cog alerts | 168 | 129 | 39 | 0 | 10 | 2024-11-25 - 2026-04-14 |
| cog data | 59 | 35 | 24 | 1 (import fix) | 9 | 2024-11-16 - 2026-07-05 |
| cog watchlists | 27 | 16 | 11 | 0 | 6 | 2024-11-16 - 2026-03-21 |
| scheduler/jobs | 137 | 111 | 26 | 0 | 14 | 2023-08-01 - 2026-05-21 |
| client schwab | 16 | 4 | 12 | 0 | 5 | 2025-05-27 - 2026-04-14 |
| client tradingview | 12 | 9 | 3 | 0 | 4 | 2025-05-23 - 2026-03-30 |
| client sec / capitol_trades / news / ape_wisdom / nasdaq / yfinance | 14 / 9 / 6 / 6 / 4 / 7 | mostly 2025 | | 0 | 2-4 | |
| alert popularity_surge (files) | 25 | 0 | 25 | 0 | **1** | 2026-03-02 - 03-24 |
| alert momentum_confirmation | 13 | 0 | 13 | 0 | **1** | 2026-03-02 - 03-25 |
| alert volume_accumulation | 5 | 0 | 5 | 0 | 2 | 2026-03-24 - 04-14 |
| alert breakout (file) | 1 | 0 | 1 | 0 | **1** | 2026-03-24 |
| alert watchlist/earnings mover (files) | 9 / 12 | 0 | | 0 | 2 | 2026-02-21 - 03-20 |
| cog paper_trading | 4 (13 incl. all paper files) | 0 | 4 | 0 | **1** | 2026-03-27 - 03-28 |
| cog config / admin / notifications / schwab_auth / help | 12 / 8 / 5 / 7 / 18 | help 11, rest 0 | rest all | 0 | 2 (help 6) | 2026-02-21 - 04-14 (admin to 07-05 import fix) |
| data cards (`core/content/data`) | 8 | 0 | 8 | 0 | **1** | 2026-03-22 - 03-23 |
| backtest/ | 44 | 0 | 44 | 0 | 2 | 2026-03-20 - 04-01 |
| eda/ | 15 | 0 | 15 | 0 | **1** | 2026-04-06 - 04-15 |
| api/ | 9 | 0 | 9 | 0 | **1** | 2026-05-01 - 05-21 |
| direction_model | 2 | 0 | 2 | 0 | **1** | 2026-04-01 |
| options_chain_history | 3 | 0 | 3 | 0 | **1** | 2026-05-20 - 05-21 |
| charting | 10 | 2 | 8 | 0 | 3 | 2025-01-07 - 2026-03-03 (dead since) |

Caveats (W): per-file counts understate features living inside large files (breakout is 1 file commit but its pipeline sits in `alerts.py`, 39 commits in 2026). The 2026 features were built in a burst (521 commits in 2026-03, up to 46 in a day) so "one active month" says "built once, not revisited", not "unused" and not "bad".

Which of v1 the owner actually invested in, human-era evidence: of the 579 commits between 2024-11-01 and 2025-07-31, the most-touched files were `discord/cogs/reports.py` (234), `stockdata.py` (160), `discord/cogs/alerts.py` (129), then `data`, `watchlists`, `scheduler` (S). Reports plus screeners plus alerts plus data plumbing is the original product; everything else arrived in 2026.

v1 commit volume by month (py-touching): 2025-06: 111, 2025-07: 3, 2026-02: 34, 2026-03: 483, 2026-04: 48, 2026-05: 31, 2026-07: 1.

### 4.3 Features added and not iterated on (W)

Single-month builds with no later commits: popularity surge / momentum confirmation (the largest 2026 alert build), paper trading (2 days, 13 commits), `/data` card layer, eda, direction_model, api, options chain history. Plus: backtest (2 months).

### 4.4 Bugfixes during the freeze (S)

One: `2b1a8d0` (2026-07-05) removed `from src.rocketstocks...` imports in `__main__.py` and three cogs that only resolved from a particular cwd. No functional bugfix since the last real runtime fixes on 2026-04-14 ("notifications fix for invalid token") and 2026-04-22 (a log level). Interpretation is ambiguous (stable, or unattended): W.

### 4.5 Issues and PRs (`gh issue list --state all --limit 100`, S)

34 issues, **all** are v2 spec-design/research items labelled `wayfinder:*` (S009/S010/S011 map and grilling tasks, created 2026-07-17 to 2026-08-01). **Zero** v1 bug reports or v1 feature requests exist. 5 PRs, all from 2023-09 (collaboration with `bennettkang`: Dockerfile, "Generate charts and post them to discord daily"). So the tracker says nothing about what v1 users want (N).

### 4.6 Docs describing intent, and v2 decisions already taken

- `README.md`: "alerts and reports on stocks that generate buy and sell signals ... price and volume movement and sentiment analysis from Reddit ... quick overview of a stock ... access a stock's price data".
- `docs/alerts/overview.md`: intent is leading-indicator alerts (social traction, volume divergence) plus confirmation, plus reactive watchlist/earnings movers. No document in the repo records measured results of those alerts (grep of `docs/alerts` and `.claude/docs/backtesting_guide.md` for results/findings: nothing) (N).
- v2 ADRs already cut or reshaped most of v1: ADR-0030 retires all six alert types (v2 ships one options-flow alert); ADR-0025 cuts Tiingo, TradingView, Capitol Trades, yfinance, Stooq, News, SEC (keeps Schwab, ApeWisdom, Nasdaq, adds Cboe); ADR-0026 drops charting; ADR-0027 keeps manual paper trading, cuts the auto-trader; ADR-0011 rebuilds backtest from scratch and defers EDA; `docs/v2/architecture-brief.md:26` states v2 parity applies to "watchlists, paper trading, reports, screeners, admin", not detection.
- External consumer: `docs/v2/architecture-brief.md:69` calls Cowork.ai "a real external consumer at cutover"; v1 `api/` (built 2026-05-01) and its `COWORK` export format are the only v1 code written for it (W).

**Queries on the production DB that would convert weak to strong evidence** (none run, no access):

```sql
SELECT alert_type, date_trunc('month', date) m, count(*) FROM alerts GROUP BY 1,2 ORDER BY 2,1;
SELECT surge_types, count(*), avg(confirmed::int), avg(expired::int) FROM popularity_surges GROUP BY 1;
SELECT status, count(*) FROM market_signals GROUP BY 1;
SELECT guild_id, user_id, count(*) FROM paper_transactions GROUP BY 1,2 ORDER BY 3 DESC;
SELECT id, watchlist_type, cardinality(string_to_array(tickers, ' ')) FROM watchlists;
SELECT count(DISTINCT guild_id) FROM channel_config;
SELECT min(snapshot_date), max(snapshot_date), count(DISTINCT ticker) FROM options_chain_history;
SELECT run_id, strategy_name, created_at FROM backtest_runs ORDER BY created_at;   -- was backtest ever run?
```

Also worth reading the Discord channels themselves: which alert channel messages got reactions or replies.

---

## 5. Research and side systems

| System | LOC (src / tests) | Surface | Maturity | Consumers |
|---|---|---|---|---|
| `backtest/` | 4,676 / 4,291 | CLI `python -m rocketstocks.backtest`: `run list stats results compare optimize walk-forward correlation monte-carlo decay trades`; strategies `buy_hold volume_accumulation popularity_surge leading_indicator_combo direction_prediction`; own 4 tables | feature-complete framework (walk-forward, Monte Carlo, regime tagging, t-tests), 337 tests | none in bot (grep: no import of backtest/eda/api from `bot core data`). No recorded results in repo (N). v2 rebuilds it (ADR-0011) |
| `eda/` | 2,778 / 1,521 | CLI `forward-returns`, `lead-lag`, `regime`; sentiment + volume detectors | complete, memory-streamed, 99 tests | none; deferred post-v2 (ADR-0011) |
| `core/analysis/direction_model.py` | 607 | logistic-regression direction model | used only as an opt-in entry filter in backtest `LeadingIndicatorStrategy` and the `direction_prediction` strategy (`backtest/strategies/base.py:53,147`); 2 commits, one day | backtest only. "ML signals" are out of v2 scope |
| `api/` + `rocketstocks-data` CLI | 1,267 / 1,525 | `DataAPI` facade (about 40 methods), parallel batch fetches, CSV/JSON exporters, CLI `export quote info watchlists version` | complete, 146 tests, built in one day 2026-05-01 | external Cowork workflow (W); nothing in the repo imports it. `DataAPI` has 18 plain (non-async) public methods wrapping yfinance, NASDAQ, ApeWisdom, News, Capitol Trades and TradingView that block if called from async code |
| paper trading | 2,693 / 238 tests | `/trade *`, queued orders, snapshots, leaderboard, weekly roundup | complete, 5 tables | users of the bot only; no usage evidence (N). v2 keeps manual paper trading (ADR-0027) |
| auto-trader | 0 | **does not exist in v1** (grep `auto.?trad` in `legacy/src`: nothing). It was a v2 plan, cut in commits `0a260ae`, `b91df3c`, `4f2cdd2` | n/a | n/a |
| `core/charting/` | 461 | mplfinance candlestick/line charts | **dead**: zero importers; chart button is a stub | none |
| orphaned analysis | 148 + 112 | `composite_score.py`, `signal_confirmation.py` (plus the function `volume_pressure.mfi_signal`; the rest of `volume_pressure.py` is used by `direction_model`) | no importer anywhere in `bot core data api backtest eda` (S: import grep); about 50 test references keep them green | none |

---

## 6. Quality

### 6.1 Patterns worth keeping (all S unless noted)

1. **Pure-core / EmbedSpec boundary.** `core/` has no `discord` imports (verified by grep) and content classes return `EmbedSpec` (`core/content/models.py:43`), converted once at `bot/senders/embed_utils.py:8`. This is why 644 content tests run with no mocks.
2. **Pure analysis functions** (DataFrame in, dataclass out) with real-data tests (448 tests in `core/analysis`).
3. **Volatility-adaptive, per-ticker z-score thresholds** with stock classification (`core/analysis/alert_strategy.py`, `classification.py`) instead of fixed percent moves. The most reusable detection idea in v1.
4. **Alert lifecycle**: dedupe key (date, ticker, type), edit/re-post by momentum z-score, back-link to prior message (`alert_sender.py`).
5. **Failure isolation**: `asyncio.gather(return_exceptions=True)` over pipelines (`alerts.py:411`), per-ticker try/except, chunked quote fetch that continues on partial failure, batch prefetch to remove N+1 (commit `42126af`).
6. **Operator visibility**: every job/loop emits success/failure with elapsed time to a Discord channel (`EventEmitter`); Schwab token health check and in-Discord re-auth. With no telemetry elsewhere this is the one thing that makes a single-operator deployment survivable.
7. **Repository classes with parameterized SQL.** All f-string SQL interpolates only column constants and placeholder lists; values are `%s` (reviewed all hits, no injection path found).
8. **Constructor-injected `StockData` container** (`data/stockdata.py:38-75`) makes whole-bot tests possible.
9. **DB-stored Schwab token with a runtime-invalid flag** and a client-side `AsyncLimiter`.
10. Layering is real: `core -> data` only in `scheduler/jobs.py`; no `discord` import in `core` or `data` (grep). `data -> core` imports exist beyond the one documented exception (settings, date/market utils, token manager: 8 imports) but are benign.

### 6.2 Concrete weaknesses

| # | Weakness | Evidence | Strength |
|---|---|---|---|
| W1 | **Watchlist-mover and earnings-mover alerts only evaluate tickers that also appear in a screener list or have an active surge/signal.** `_process_alerts_impl` quotes `screener_tickers + surge_tickers + signal_tickers` (`alerts.py:339-353`) but never watchlist or earnings tickers; the pipelines then filter that quote dict (`alerts.py:840`, `:895-896`). `docs/alerts/watchlist_mover.md` says "processed regardless of whether they also appear in the screener list". I reproduced it by calling the real `_process_alerts_impl` with mocked I/O (`scratchpad/scripts/repro_quotes.py`): with screener tickers AAA,BBB, a watchlist ticker WLX and an earnings ticker ERNX, `get_quotes` was asked only for AAA,BBB. The unit tests mock `get_quotes` and cannot see this. Same behavior existed before the 2026-03-02 refactor (`812ef50^`), so possibly an accidental pre-filter rather than a regression | repro run | S (behavior), W (whether intended) |
| W2 | **Event-loop blocking in scheduled jobs.** `update_upcoming_earnings` / `update_historical_earnings` call `nasdaq.get_earnings_by_date` synchronously in `async def` (`data/earnings.py:41,127`); the method is `@limits(5/60s)` + `@sleep_and_retry` (`nasdaq.py:21-22`), i.e. `time.sleep` on the Discord/APScheduler loop, minutes long for about 36 calls. Also sync: `tiingo.get_ticker_metadata` in hourly enrich (`data/tickers.py:159`, 240 calls), NASDAQ/SEC/Tiingo in `tickers.py:89,117,123,222`, ApeWisdom in classify (`jobs.py:60`), Tiingo/Stooq in delisted loader (`price_history.py:122-126`), and `News().get_news` in a button callback (`report_views.py:40`, which also builds a second `News()` outside the `StockData` singleton). Impact on gateway heartbeats not measured | code reading | S (code), W (runtime impact) |
| W3 | No HTTP timeouts anywhere (13 `requests.get`, zero `timeout=`); no retry or backoff for any vendor; a 429 aborts the whole loop and nightly jobs do not resume (`options_chain_history.py:241-246`, `jobs.py:44-52`) | grep | S |
| W4 | Silent failures: Schwab history errors return empty frames (`schwab.py:161-168,191-194`), options snapshot job counts errors but the wrapper still reports SUCCESS (`options_chain_history.py:224-272`), Stooq/Tiingo/yfinance swallow everything | code | S |
| W5 | **Options partitions created only at startup** (`schema.py:482-505`, called only from `create_tables`). A bot that runs more than about 1-2 months without restart starts failing every `options_chain_history` insert (per-ticker exceptions are swallowed per W4). Real if the deployed container has not restarted since mid-August | code | S (mechanism), W (whether currently failing) |
| W6 | **Buttons die on restart**: only `SubscriptionEntryView` is persistent (`bot.py:100`). `AlertButtons`, `StockReportButtons` etc. are `timeout=None` with auto-generated `custom_id`s and are not re-added, so "Add to Watchlist", "Generate Report", "Get news" on any message older than the last restart fail (URL buttons still work) | code + discord.py semantics, not tested live | W |
| W7 | `on_ready` (`bot.py:164-170`) loads every extension and calls `bot.tree.sync()` each time it fires; `on_ready` re-fires after full reconnects and a second `load_extension` raises `ExtensionAlreadyLoaded`. Not observed | code reading | W |
| W8 | `alert_tickers` (the alert universe) lives in process memory only (`stockdata.py:77`): after a restart alerts see only active surges/signals until screener loops repopulate it | code | S |
| W9 | God objects: `Alerts` cog 1,311 LOC and `Reports` cog 1,162 LOC mix loops, commands and builders; views call `get_cog("Reports")` (`alert_views.py:103`); `StockData` aggregates 25 collaborators; `sections_card.py` is 2,302 LOC / 53 functions used by 29 content classes | LOC | S |
| W10 | Dead code kept alive by tests: charting 461, composite_score 148, signal_confirmation 112, 31 unused data-layer methods (4 of them SEC), `PoliticianTradeButtons`, `Alerts.build_volume_accumulation`, 3 unused settings keys | grep | S |
| W11 | `ticker_stats` RVOL columns displayed but never written (section 3) | code | S |
| W12 | Config rule broken: `CLAUDE.md` says "never `os.environ` directly", but `bot/cogs/config.py:140-144,228,243,279-284` and `bot/bot.py:67,74,139` read `os.environ`/`os.getenv`; `Settings()` is built at import time so any import without env fails (tests set env at conftest import) | grep | S |
| W13 | Time handling: 23 `datetime.utcnow()` calls (deprecated, naive), 35 deprecation warnings in the test run; a 2026-03-09 fix for naive/aware comparison (`df151b8`) shows this has bitten before | grep, git | S |
| W14 | Operational weight with no live consumer: nightly whole-universe 5m history and options chains (each is thousands of sequential Schwab calls at 120/min) feed only offline tools and two report sections. Universe size not determinable offline (NASDAQ screener, thousands) | code | S (consumers), W (cost) |
| W15 | A conceptual mismatch in confirmation scoring: `evaluate_confirmation` docstring says z-score "against the ticker's own intraday distribution" but callers pass 20-day **daily** mean/std return (`alerts.py:478-487`) | code | S |

### 6.3 Tests

- `.venv/bin/pytest legacy/tests -q -p no:cacheprovider`: **2804 passed, 0 failed, 0 skipped, 106 warnings, 14.39 s** (second run 12.79 s, identical). No flakiness observed in two runs (not a flake proof).
- Warnings: 35 `datetime.utcnow()` deprecations, 7 backtesting.py "trades remain open", 1 numpy precision, 1 websockets.
- `ruff check legacy/src --isolated`: 59 findings (26 unused imports, 15 empty f-strings, 11 unused variables, 4 `== True`, 3 one-line statements). Legacy is intentionally never linted in CI.
- CI runs the v1 suite (`.github/workflows/ci.yml`, job `legacy`) and publishes an image on `main`/`dev` (`docker-publish.yml`).
- **Meaningfulness.** Strong where logic is pure (analysis 448, content 644, backtest 337 tests with real DataFrames). Weak at the seams: every repository test mocks `db.execute` and asserts the SQL string and params (`legacy/tests/data/test_surge_store.py:37-55`); **no test runs SQL against Postgres**, and no test exercises a real discord.py command tree. About 658 `assert_called` sites are implementation-coupled. W1 (the quote-set gap) and W5 (partition creation) are exactly the class of bug this style cannot see. The root `conftest.py` `mock_db` fixture describes an API (`_cursor`, `select`, `insert`) that `Postgres` no longer has; local fixtures shadow it. The suite writes into the repo working tree when run (`discord/attachments/*.csv`, gitignored).

### 6.4 Documentation drift (S)

`CLAUDE.md` DB list names `holdings/transactions/portfolio_snapshots/iv_history` (actual: `paper_*`, `options_chain_history`, `earnings_results`, `schwab_tokens`, `ct_politicians`). `.claude/docs/bot.md` and `core.md` name `/config set-channel`, `/schwab-status`, `collect_iv_history`, `iv_history_store.py`, "about 15" `/data` subcommands (actual: `/server setup|status`, `/schwab status|auth`, `collect_options_chain_history`, 19). A runtime message tells users to run `/schwab-auth` (`jobs.py` token check) but the command is `/schwab auth`.

---

## 7. Assessment: core vs long tail (my judgment; evidence labels as above)

### The real core (what v1 is for)

1. **Schwab data spine**: quotes, nightly daily history, token lifecycle with in-Discord re-auth. Everything else depends on it (S).
2. **Market-hours screeners posted and edited in place** (gainers, unusual volume, popularity) which also define the alert universe. Original product from 2024-11; 9+ commits in 2025-06 alone (S for history, N for current use).
3. **Stock report** (`/report ticker`, `watchlist`, plus the button path) and the earnings suite (calendar sync, spotlight, results, Discord events). `reports.py` is the single most-touched file in the project (273 commits) (S).
4. **Watchlists** (named, personal, `class:` overrides): the user-owned input that feeds alerts, reports and classification (S).
5. **Price-move alerts for watchlist and earnings tickers** with volatility-scaled z-score thresholds and edit-in-place momentum updates: the oldest alert types (2024-11) and the best-engineered detection idea. Subject to W1 (S for design, W for current effectiveness).
6. **Ops layer**: per-guild channel setup, notification channel, job wrappers, Schwab auth flow. Required for one operator to run unattended (S).
7. **Ticker universe + classification** (`tickers`, `ticker_stats`): small, shared by alerts, reports and research (S).
8. **Popularity ingestion** (ApeWisdom into `popularity`): the only non-Schwab signal source that is both free and keyless, feeds screener, surge alerts, classification, research. The surge/confirmation alerts built on it are the largest 2026 build but have zero recorded validation (S for dependency, N for value).

### The long tail

- Cheap, low-risk on-demand lookups: `/data` (19 commands, 1.5k LOC with cards), `/report compare|technical|options|popularity|politician`, `/news`. Each is thin glue over a vendor call (W: low value density, low cost).
- Heavy and unvalidated alerting: volume accumulation + breakout (options-gated, 2 tables, 570 LOC) and the surge/confirmation pair. No results documented anywhere; v2 ADR-0030 already retired all six.
- Paper trading (2.7k LOC, 5 tables, 2-day build, never revisited). v2 still carries it as "parity".
- Research stack: backtest 4.7k, eda 2.8k, direction_model 0.6k, api 1.3k. Built in about 7 weeks, no bot consumer, no recorded outputs. v2 rebuilds backtest, defers eda.
- Dead weight: charting, orphaned composite/confirmation modules, stub button, dead repo methods.
- Politician trades (HTML scraper): brittle and v2 cuts the source.
- Nightly whole-universe 5m history and options chains: large operational load with only offline/IV-report consumers. The options chain collector is the one 2026 piece closest to v2's options-flow thesis, but its design (swallowed errors, startup-only partitions) is not worth carrying as is.

### Carry-forward candidates that v2 has not already decided against (W)

- Volatility-scaled per-ticker z-score thresholds and the classification idea (`alert_strategy.py`, `classification.py`) as inputs to the Phase-2 detection design.
- Options analytics in `core/analysis/options.py` (IV, max pain, skew, put/call) and the `parse_chain` / chain-history derivations, as report inputs.
- The notification/job-wrapper pattern and the Discord-native Schwab OAuth flow, since the refresh-token problem is inherent to Schwab (v2 issue #38 is already about it).
- Watchlist model (`named`/`personal`/`class:`), channel-per-guild routing, role-based alert subscriptions.
- The lessons rather than the code: quote-universe coverage must be explicit and tested (W1), nightly jobs must create their own partitions and surface partial failure (W4, W5), all outbound HTTP needs timeouts, and the data layer needs at least some tests against real Postgres.

---

## Appendix A. Method and reproducibility

- Orientation: `CLAUDE.md`, `.claude/docs/*.md` (all read; found stale in places, see 6.4, so everything above was re-verified against code).
- LOC: `wc -l`; per-area sums via a script over globs (areas defined in the transcript).
- Commands/loops: regex scan of `bot/cogs/*.py` plus manual read of `alerts.py`, `reports.py`, `jobs.py`, `paper_trading.py`, `notifications.py`.
- Table cross-reference: `scratchpad/scripts/tablexref.py`; dead-method scan: `scratchpad/scripts/deadmethods.py`; git: `scratchpad/scripts/churn.py`, `churn2.py`, `era.py`, `bigcommits.py` over `scratchpad/audit/gitlog.txt`; repro: `scratchpad/scripts/repro_quotes.py` (run from the scratchpad with `PYTHONDONTWRITEBYTECODE=1`, imports repo code, writes nothing to the repo).
- Test output saved at `scratchpad/audit/pytest.out`.
- Repo state: no files edited or created in the repo, no commits, no checkouts. Running the test suite wrote gitignored CSVs into `discord/attachments/` (a side effect of v1 tests, noted in 6.3).
- Not done: no live Discord, no production DB, no vendor pricing lookups (cost column is from repo contents only), no runtime measurement of event-loop stalls.

All scratchpad paths are under `/private/tmp/claude-501/-Users-john-Documents-Projects-RocketStocks/48d0c7cb-5c8a-4505-9777-88518f6d85b5/scratchpad/`.
