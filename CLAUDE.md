# RocketStocks

A Discord bot that delivers real-time stock market alerts and screener reports using technical analysis, sentiment data, and multiple financial APIs.

## Tech Stack

- **Language:** Python 3.12+
- **Bot Framework:** discord.py (cog-based extension system)
- **Scheduler:** APScheduler (AsyncIOScheduler with cron triggers) + discord.ext.tasks loops
- **Database:** PostgreSQL via psycopg3 (AsyncConnectionPool)
- **Data Sources:** Charles Schwab API, Tiingo, Stooq, NASDAQ, TradingView, Capitol Trades, News API, ApeWisdom, YFinance
- **Analysis:** pandas-ta (RSI, MACD, ADX, OBV, RVOL), mplfinance (charts), backtesting.py
- **Notifications:** EventEmitter (thread-safe Queue) for scheduler job status
- **Infrastructure:** Docker + Docker Compose

## Package Layout

`src/` layout installed via `pyproject.toml`.

```
src/rocketstocks/
├── __main__.py       # Entry point
├── core/             # Business logic — ZERO discord imports
├── data/             # Data access — NO discord or core imports
├── bot/              # Presentation — ALL discord-specific code
├── api/              # Public data SDK — DataAPI facade + CLI exporter
└── backtest/         # Signal validation and strategy research
```

See the documentation index below for per-directory detail.

## Layer Dependency Rules

```
bot/  → core/  ✓    bot/  → data/  ✓
core/ ✗ bot/        core/ → data/  ✗  (exception: scheduler/jobs.py)
data/ ✗ bot/        data/ ✗ core/
```

**Never import discord in `core/` or `data/`.**

## Key Conventions

- **StockData singleton** — created at `__main__.py:16`, attached at `bot/bot.py:163`; never re-instantiate
- **Content classes** — return `EmbedSpec` (`core/content/models.py:43`); converted via `bot/senders/embed_utils.py:8`
- **DB access** — always via `Postgres.execute()` (`data/db.py:61`) or a repository class; `%s` placeholders only
- **Logger** — `logger = logging.getLogger(__name__)` in every module
- **Config** — read via pydantic-settings `settings` singleton at `core/config/settings.py:34`; never `os.environ` directly

## Startup Sequence

- `__main__.py:11-26` — creates `StockData`, `EventEmitter`, `NotificationConfig`; calls `run_bot()`
- `bot/bot.py:150-166` — creates bot, attaches dependencies, calls `bot.run()`
- `bot/bot.py:58-91` — `setup_hook()` opens DB pool, creates tables, starts APScheduler
- `bot/bot.py:118-124` — `load()` auto-loads all `.py` files in `bot/cogs/`

## Workflow Rules

**Always create a new branch** before starting a new feature or bug fix. Once complete, run all relevant tests and confirm success. Write new test cases, adjust existing tests, and remove unnecessary tests for any changes you make.

## Run / Test / Deploy

```bash
# Run locally (after pip install -e .)
python -m rocketstocks

# Run tests
.venv/bin/pytest tests/ -q

# Run specific test file
.venv/bin/pytest tests/bot/cogs/test_alerts_cog.py -v

# Backtesting CLI
python -m rocketstocks.backtest run volume_accumulation --benchmark SPY

# Docker
docker-compose up -d
```

## Environment Variables (`.env`)

```
DISCORD_TOKEN, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST, POSTGRES_PORT
SCHWAB_API_KEY, SCHWAB_API_SECRET, TIINGO_API_KEY
NASDAQ_API_KEY, NEWS_API_KEY
TZ=America/Chicago
```

## Database Tables

Auto-created on startup via `data/schema.py:34`. Tables: `tickers`, `ticker_stats`, `upcoming_earnings`, `historical_earnings`, `watchlists`, `popularity`, `popularity_surges`, `alerts`, `reports`, `daily_price_history`, `five_minute_price_history`, `holdings`, `transactions`, `portfolio_snapshots`, `channel_config`, `alert_roles`, `bot_settings`, `market_signals`, `iv_history`, `backtest_runs`, `backtest_results`, `strategy_stats`, `backtest_trades`.

## Documentation Index

| Topic | File |
|-------|------|
| Architectural patterns & cross-cutting design | [.claude/docs/architectural_patterns.md](.claude/docs/architectural_patterns.md) |
| `core/` — analysis, content classes, scheduler, notifications | [.claude/docs/core.md](.claude/docs/core.md) |
| `data/` — DB wrapper, repositories, API clients | [.claude/docs/data.md](.claude/docs/data.md) |
| `bot/` — cogs, senders, views, bot lifecycle | [.claude/docs/bot.md](.claude/docs/bot.md) |
| `backtest/` — strategy framework architecture | [.claude/docs/backtesting.md](.claude/docs/backtesting.md) |
| Backtesting CLI usage & interpreting results | [.claude/docs/backtesting_guide.md](.claude/docs/backtesting_guide.md) |
| `api/` — public DataAPI facade, batch fetches, `rocketstocks-data` CLI | [.claude/docs/api.md](.claude/docs/api.md) |
| `eda/` — exploratory signal research (event detectors + analysis engines) | [.claude/docs/eda.md](.claude/docs/eda.md) |
| Test structure, fixtures & mocking patterns | [.claude/docs/tests.md](.claude/docs/tests.md) |
