# Public Data SDK (`legacy/src/rocketstocks/api/`)

Standalone data SDK for collecting stock market data outside the Discord bot. Intended for internal callers and external coworker projects that need OHLCV history, fundamentals, options, sentiment, and related data for analysis and report generation. **No discord imports.** Depends on `data/` (via `StockData`) and `core/config/` (for DB connection settings).

**Rule:** consumers import only from `rocketstocks.api`. Importing from sub-modules (`rocketstocks.api.client`, etc.) is unsupported.

---

## Quick Start

**Install** (from repo root, Python ≥3.13):
```
uv sync --all-packages
```

**Required env vars** (in `.env`):
```
POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
SCHWAB_API_KEY, SCHWAB_API_SECRET, TIINGO_API_KEY
NASDAQ_API_KEY, NEWS_API_KEY
```

**Async usage:**
```python
import asyncio
from rocketstocks.api import build_data_api, DataNotAvailable

async def main():
    api = await build_data_api()
    try:
        df = await api.get_daily_history("AAPL", "2026-01-01", "2026-04-30")
        batch = await api.batches.get_deep_dive("AAPL", history_days=730)
        if batch.errors:
            print("partial failures:", list(batch.errors))
    finally:
        await api._sd.db.close()

asyncio.run(main())
```

**Sync usage** (wraps `asyncio.run`; do not call inside a running event loop):
```python
api.sync.get_quote("AAPL")
api.sync.get_daily_history("AAPL", "2026-01-01", "2026-04-30")
```

**CLI:**
```
rocketstocks-data export AAPL MSFT --to ./out --include ohlcv-daily,fundamentals --history-days 730
rocketstocks-data quote AAPL MSFT
rocketstocks-data watchlists
```

---

## api/\_\_init\_\_.py — Public Surface

Single re-export point (`__init__.py:22-30`). `__all__`:

- `DataAPI` — async facade
- `DataNotAvailable` — exception
- `build_data_api` — async factory
- `TickerBatch`, `PeerComparisonBatch`, `OptionsSnapshot`, `FundamentalsSnapshot` — batch dataclasses

---

## api/\_factory.py — Bootstrap

- `build_data_api()` at `_factory.py:10-27` — async; creates `StockData`, opens DB pool via `await sd.db.open()`, attempts `init_schwab()` (failure is a warning, not fatal — `SchwabTokenError` surfaces only when a Schwab method is called). Returns a ready `DataAPI`.
- Caller is responsible for closing the pool: `await api._sd.db.close()`. The CLI handles this in a `finally` block at `cli.py:286-289`.

---

## api/client.py — DataAPI

`DataAPI` at `client.py:216-225` — async facade over the `StockData` service container. Constructor: `__init__(self, stock_data)`. Exposes:
- `self.batches` — `BatchAPI` instance for multi-field parallel fetches
- `self.sync` — `_SyncWrapper` at `client.py:227-229`; each method wraps the corresponding async call in `asyncio.run`

`DataNotAvailable` at `client.py:13-14` — raised when `get_daily_history(..., db_only=True)` finds no cached rows.

### Reference data (`client.py:235-259`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_ticker_info(ticker)` | `dict \| None` | Row from `tickers` table |
| `get_ticker_infos(tickers)` | `list[dict \| None]` | Concurrent via `gather` |
| `validate_ticker(ticker)` | `bool` | True if ticker exists in DB |
| `get_ticker_stats(ticker)` | `dict \| None` | Row from `ticker_stats` table |
| `get_ticker_stats_batch(tickers)` | `dict[str, dict \| None]` | `{ticker: stats}` |

### Quotes (`client.py:265-273`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_quote(ticker)` | `dict` | Live Schwab quote |
| `get_quotes(tickers, fields=None)` | `dict` | Schwab batch quote call |

### Price history (`client.py:279-355`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_daily_history(ticker, start, end, force_live=False, db_only=False)` | `pd.DataFrame` | DB-first → Tiingo → Stooq fallback; `force_live` skips DB; `db_only` raises `DataNotAvailable` on miss |
| `get_daily_histories(tickers, start, end)` | `dict[str, pd.DataFrame]` | Batch DB query |
| `get_5m_history(ticker, start, end)` | `pd.DataFrame` | DB only |
| `get_5m_histories(tickers, start, end)` | `dict[str, pd.DataFrame]` | Concurrent |

`start` / `end` accept `str` (ISO date), `datetime.date`, or `datetime.datetime`.

### Fundamentals & financials (`client.py:361-423`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_schwab_fundamentals(ticker)` | `dict` | Schwab single-ticker fundamentals |
| `get_schwab_fundamentals_batch(tickers)` | `dict` | Schwab batch fundamentals |
| `get_financials(ticker)` | `dict` | **Sync.** yfinance; keys: `income_statement`, `quarterly_income_statement`, `balance_sheet`, `quarterly_balance_sheet`, `cash_flow`, `quarterly_cash_flow` (each a `pd.DataFrame`) |
| `get_eps_history(ticker)` | `pd.DataFrame` | Historical EPS from DB |
| `get_analyst_price_targets(ticker)` | `dict \| None` | **Sync.** yfinance |
| `get_recommendations(ticker)` | `pd.DataFrame` | **Sync.** yfinance |
| `get_upgrades_downgrades(ticker)` | `pd.DataFrame` | **Sync.** yfinance |
| `get_float_data(ticker)` | `dict` | **Sync.** yfinance float + short interest |
| `get_institutional_holders(ticker)` | `pd.DataFrame` | **Sync.** yfinance |
| `get_major_holders(ticker)` | `pd.DataFrame` | **Sync.** yfinance |
| `get_insider_transactions(ticker)` | `pd.DataFrame` | **Sync.** yfinance |
| `get_insider_purchases(ticker)` | `pd.DataFrame` | **Sync.** yfinance |

### Earnings (`client.py:429-447`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_next_earnings(ticker)` | `dict \| None` | Next upcoming earnings from DB |
| `get_earnings_calendar(date)` | `pd.DataFrame` | All earnings on a given date |
| `get_eps_estimate(ticker, period="quarter")` | `pd.DataFrame` | **Sync.** NASDAQ; `period="quarter"` or `"year"` |

### Options & IV (`client.py:453-479`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_options_chain(ticker)` | `dict` | Schwab options chain |
| `get_options_chains_batch(tickers)` | `dict[str, dict]` | Concurrent; failures silently dropped |
| `get_iv_history(ticker, days=365)` | `pd.DataFrame` | Last N days of IV from DB |
| `get_latest_iv(ticker)` | `float \| None` | Most recent root-level IV from DB |

### Sentiment & flow (`client.py:485-534`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_popularity(ticker, start_date=None, end_date=None)` | `pd.DataFrame` | Historical ApeWisdom data from DB |
| `get_popular_tickers(filter_name="all stock subreddits", limit=100)` | `pd.DataFrame` | **Sync.** Live ApeWisdom |
| `get_news(query, **kwargs)` | `dict` | **Sync.** NewsAPI |
| `get_recent_filings(ticker, latest=10)` | `pd.DataFrame` | SEC EDGAR |
| `get_filing_link(ticker, filing)` | `str \| None` | Direct SEC EDGAR URL |
| `get_politician_trades(politician_id)` | `pd.DataFrame` | **Sync.** Capitol Trades |
| `get_all_politicians()` | `list` | All politicians from DB |

### Movers (`client.py:540-568`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_premarket_gainers()` | `pd.DataFrame` | **Sync.** TradingView |
| `get_intraday_gainers()` | `pd.DataFrame` | **Sync.** TradingView |
| `get_postmarket_gainers()` | `pd.DataFrame` | **Sync.** TradingView |
| `get_unusual_volume_movers()` | `pd.DataFrame` | **Sync.** TradingView |
| `get_market_caps(limit=10000)` | `pd.DataFrame` | **Sync.** TradingView |
| `get_schwab_movers(sort_order=None)` | `dict` | Schwab top equity movers |

### Watchlists (`client.py:574-582`)

| Method | Returns | Notes |
|--------|---------|-------|
| `get_watchlists(types=None)` | `list[str]` | Named watchlist identifiers |
| `get_watchlist_tickers(watchlist_id)` | `list[str]` | Tickers for a given watchlist |

---

## api/batches.py — Batch Operations

Accessed via `api.batches`. All methods fan out sub-fetches in parallel via `_gather_fields` at `batches.py:73-94` (`asyncio.gather(return_exceptions=True)`). Per-field failures land in `result.errors[field_name]`; the field gets its default value — the dataclass is always constructible.

| Method | Returns | Notes |
|--------|---------|-------|
| `get_deep_dive(ticker, history_days=365, intraday_days=5)` | `TickerBatch` | 16 fields |
| `get_peer_comparison(tickers, benchmarks=None, days=365)` | `PeerComparisonBatch` | `benchmarks` defaults to `["SPY"]`; tickers+benchmarks deduped, order preserved |
| `get_options_snapshot(ticker)` | `OptionsSnapshot` | — |
| `get_fundamentals_snapshot(ticker)` | `FundamentalsSnapshot` | — |

**`TickerBatch`** (`batches.py:17-35`): `info: dict`, `quote: dict`, `schwab_fundamentals: dict`, `yfinance_financials: dict`, `daily_history: pd.DataFrame`, `five_min_history: pd.DataFrame | None`, `options_chain: dict`, `iv_history: pd.DataFrame`, `eps_history: pd.DataFrame`, `next_earnings: dict | None`, `analyst_targets: dict | None`, `recommendations: pd.DataFrame`, `float_data: dict`, `insider_transactions: pd.DataFrame`, `popularity: pd.DataFrame`, `recent_filings: pd.DataFrame`, `errors: dict[str, Exception]`

**`PeerComparisonBatch`** (`batches.py:39-44`): `daily_histories: dict[str, pd.DataFrame]`, `schwab_fundamentals: dict[str, dict]`, `ticker_stats: dict[str, dict | None]`, `errors`

**`OptionsSnapshot`** (`batches.py:48-54`): `chain: dict`, `iv_history: pd.DataFrame`, `quote: dict`, `ticker_stats: dict | None`, `errors`

**`FundamentalsSnapshot`** (`batches.py:58-66`): `financials: dict`, `eps_history: pd.DataFrame`, `analyst_targets: dict | None`, `recommendations: pd.DataFrame`, `upgrades_downgrades: pd.DataFrame`, `insider_transactions: pd.DataFrame`, `errors`

---

## api/exporters.py — File Writers

Pure functions, no API calls. All take `(data, dest_dir: Path, ticker: str)` and return `Path` or `list[Path]`. Column orders are frozen constants at `exporters.py:10-23`.

| Function | Output file(s) | Columns / notes |
|----------|----------------|-----------------|
| `write_daily_csv` | `TICKER_daily_data.csv` | `ticker, open, high, low, close, volume, date` |
| `write_5m_csv` | `TICKER_5m_data.csv` | `ticker, open, high, low, close, volume, datetime` |
| `write_options_json` | `TICKER_options_chain.json` | Raw Schwab payload |
| `write_fundamentals_json` | `TICKER_fundamentals.json` | Raw Schwab payload |
| `write_financials_csvs` | up to 6 CSVs | `TICKER_income_statement.csv`, `_quarterly_income_statement.csv`, `_balance_sheet.csv`, `_quarterly_balance_sheet.csv`, `_cash_flow.csv`, `_quarterly_cash_flow.csv`; skips empty DataFrames; returns `list[Path]` |
| `write_eps_csv` | `TICKER_eps.csv` | — |
| `write_popularity_csv` | `TICKER_popularity.csv` | `datetime, rank, ticker, name, mentions, upvotes, rank_24h_ago, mentions_24h_ago` |

---

## api/cli.py — `rocketstocks-data` CLI

Entry point registered in `pyproject.toml:43`: `rocketstocks-data = "rocketstocks.api.cli:main"`. DB pool closed in `finally` at `cli.py:286-289` after every non-version command.

Valid `--include` values (`cli.py:22-25`): `eps`, `financials`, `fundamentals`, `ohlcv-5m`, `ohlcv-daily`, `options`, `popularity`.

| Subcommand | Args / flags | Behavior |
|------------|-------------|----------|
| `export TICKER…` | `--to PATH` (required); `--include LIST` (comma-sep subset, default = all); `--history-days N` (default 365); `--intraday-days N` (default 5); `--watchlist NAME`; `--format COWORK\|RAW` (default `COWORK`); `--force-live`; `--dry-run` | Fetches each requested data kind per ticker sequentially; per-field failures are isolated. Watchlist tickers merged into positional list. Creates `--to` dir unless `--dry-run`. Prints `TICKER: N file(s)` per ticker. |
| `quote TICKER…` | 1+ tickers | Prints live Schwab quote JSON |
| `info TICKER` | one ticker | Prints `get_ticker_info` result as JSON |
| `watchlists` | — | Prints each watchlist name on its own line |
| `version` | — | Prints package version; never builds the API |

---

## Reference: Underlying Sources

| DataAPI group | StockData attribute(s) |
|---------------|----------------------|
| Reference data | `sd.tickers`, `sd.ticker_stats` |
| Quotes | `sd.schwab` |
| Price history | `sd.price_history` (DB), `sd.tiingo`, `sd.stooq` |
| Fundamentals | `sd.schwab`, `sd.yfinance` |
| Earnings | `sd.earnings`, `sd.nasdaq` |
| Options & IV | `sd.schwab`, `sd.iv_history` |
| Sentiment & flow | `sd.popularity`, `sd.popularity_client`, `sd.news`, `sd.sec`, `sd.capitol_trades` |
| Movers | `sd.trading_view`, `sd.schwab` |
| Watchlists | `sd.watchlists` |

See [data.md](data.md) for client and repository implementation detail.
