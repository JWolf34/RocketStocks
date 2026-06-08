# Data Layer (`src/rocketstocks/data/`)

Data access. **No discord imports. No core imports** — the one exception is `db.py:7` which imports `settings` from `core/config/` for the connection string.

---

## data/db.py — Postgres Async Wrapper

`Postgres` class at `db.py:40-95`. Async wrapper around psycopg3 + psycopg_pool (`AsyncConnectionPool`).

- `__init__(minconn, maxconn)` at `:41-48` — creates pool (deferred open)
- `open()` at `:51-53` / `close()` at `:56-58` — lifecycle called from `bot.py:60` and `:97`
- `execute(query, params, fetchone)` at `:61-71` — parameterized query with `%s` placeholders. Returns rows for SELECT, None for DML.
- `execute_batch(query, values)` at `:73-77` — bulk insert/upsert via `executemany`
- `transaction()` at `:79-95` — async context manager yielding `_TransactionConn` (`:22-37`) for multi-statement atomic operations. The wrapped connection exposes the same `execute()` API.

**Rule:** All queries use `%s` parameterized placeholders — never f-string or string-format SQL.

---

## data/stockdata.py — Service Container

`StockData` class at `stockdata.py:37-100`. Composes all clients and repositories via constructor injection.

- Constructor at `:38-45` — every parameter defaults to `None` and falls back to creating the real instance at `:47-75`
- All clients and repos accessible as `stock_data.<name>` (e.g., `stock_data.schwab`, `stock_data.surge_store`)
- Alert ticker tracking: `_alert_tickers` dict at `:77`, `update_alert_tickers()` at `:90`
- Schwab init: `init_schwab()` at `:83-84`, called in `bot.py:85`

Created once in `__main__.py:16`, attached to `bot.stock_data` at `bot.py:163`. Never re-instantiate.

---

## data/clients/ — API Client Wrappers

One file per external API. All are async or wrapped via `asyncio.to_thread`.

| File | API | Notes |
|------|-----|-------|
| `schwab.py` | Charles Schwab | Quotes, options chains, movers, fundamentals. Raises `SchwabRateLimitError`, `SchwabTokenError`. OAuth via token file. |
| `nasdaq.py` | NASDAQ | Earnings calendar, EPS forecasts. |
| `news.py` | News API | News search by query/ticker. |
| `tradingview.py` | TradingView | Screener data (premarket, intraday, unusual volume). |
| `ape_wisdom.py` | ApeWisdom | Stock popularity rankings and mention counts. |
| `sec.py` | SEC EDGAR | SEC filings. Takes `db` in constructor for CIK lookups. |
| `capitol_trades.py` | Capitol Trades | Politician trading data. Takes `db`. |
| `tiingo.py` | Tiingo | Historical prices, ticker metadata. |
| `stooq.py` | Stooq | Free historical prices (used for delisted tickers). |
| `yfinance_client.py` | YFinance | Financials, analyst ratings, ownership, insider, short interest. |

---

## data/ — Repository Classes

**Pattern:** each repo takes a `db` parameter (`Postgres` instance), owns one table, defines `_TABLE` and `_FIELDS` module-level constants. All methods are async using `await self._db.execute()`. See `surge_store.py:7-13` for the canonical `_TABLE`/`_FIELDS` example.

| File | Class | Table(s) |
|------|-------|----------|
| `tickers.py` | `TickerRepository` | `tickers` |
| `price_history.py` | `PriceHistoryRepository` | `daily_price_history`, `five_minute_price_history` |
| `earnings.py` | `Earnings` | `upcoming_earnings`, `historical_earnings` |
| `watchlists.py` | `Watchlists` | `watchlists` |
| `popularity_store.py` | `PopularityRepository` | `popularity` |
| `surge_store.py:17` | `SurgeRepository` | `popularity_surges` |
| `ticker_stats.py` | `TickerStatsRepository` | `ticker_stats` |
| `channel_config.py` | `ChannelConfigRepository` | `channel_config` |
| `discord_state.py` | `DiscordState` | `alerts`, `reports` |
| `market_signal_store.py` | `MarketSignalRepository` | `market_signals` |
| `alert_roles.py` | `AlertRolesRepository` | `alert_roles` |
| `bot_settings.py` | `BotSettingsRepository` | `bot_settings` |
| `schwab_token_store.py` | `SchwabTokenRepository` | `schwab_tokens` |
| `earnings_results_store.py` | `EarningsResultsRepository` | `historical_earnings` (results) |
| `iv_history_store.py` | `IVHistoryRepository` | `iv_history` |
| `paper_trading_store.py` | `PaperTradingRepository` | `holdings`, `transactions`, `portfolio_snapshots` |

### Key repositories in detail

**`discord_state.py`** — tracks Discord message IDs for alerts and screeners (edit-in-place support):
- `get_alert_message_id(date, ticker, alert_type)` — lookup by date+ticker+type
- `insert_alert_message_id(date, ticker, alert_type, message_id, alert_data)` — record new alert
- `update_alert_message_data(date, ticker, alert_type, messageid, alert_data)` — update on re-post
- `get_screener_message_id(screener_type)` / `insert_screener_message_id()` / `update_screener_message_id()`

**`price_history.py`** — primary data store for backtesting:
- `fetch_daily_price_history(ticker, start_date, end_date) -> DataFrame`
- `fetch_5m_price_history(ticker, start_datetime, end_datetime) -> DataFrame`
- `update_daily_price_history(tickers)` / `update_5m_price_history(tickers)` — called by APScheduler jobs

**`watchlists.py`** — supports named watchlists, personal watchlists (user-owned), and classification overrides:
- `get_classification_overrides()` — returns `{ticker: class_override}` dict for `classify_tickers` job

---

## data/schema.py — DDL

- `_CREATE_SCRIPT` at `:34+` — CREATE TABLE IF NOT EXISTS for all tables (runs at startup)
- `_MIGRATION_SCRIPT` at `:6-32` — ALTER TABLE migrations (idempotent, also runs at startup)
- `create_tables(db)` called from `bot.py:62` during `setup_hook()`

---

## data/seed.py

`seed_boilerplate_watchlists(watchlists)` — seeds default named watchlists on first run. Called from `bot.py:158` in `on_ready`.

---

## data/auth/

- `token_exchange.py` — `exchange_code_for_token(api_key, app_secret, auth_context, received_url)` wraps `schwab.auth.client_from_received_url()`. Called by `bot/cogs/schwab_auth.py` after user pastes the OAuth callback URL.
