# RocketStocks

A Discord bot that delivers real-time stock market alerts and screener reports using technical analysis, sentiment data, and multiple financial APIs.

## v2 rewrite in progress - read this first

- **v1 (`legacy/src/rocketstocks/`) is in code freeze** (since 2026-06-11): bugfixes only, no feature work. In prod it runs muted (Discord posting silenced), collecting only ApeWisdom popularity until v2 slice 1 takes over; then it is retired.
- **All v2 work starts at [docs/foundation/README.md](docs/foundation/README.md)** - charter, [architecture](docs/foundation/architecture.md) (current truth), roadmap, current slice spec. Decisions live on the [v2 foundation map](https://github.com/JWolf34/RocketStocks/issues/40) tickets and the surviving ADRs in `docs/v2/adr/`; the pre-2026-10 plan is archived under git tag `v2-plan-archive-2026-10`.
- **Each slice ships a half-page spec in its implementing PR** (template: `docs/foundation/specs/README.md`) and must meet the definition of done in `docs/foundation/README.md`. Keep `architecture.md` current in the same PR; a new ADR only for a contested decision.
- **The rest of this file describes v1.** `docs/alerts/` is v1-historical reference; v2 folds those alert types into three informational alerts (attention surge, earnings reaction, large move).

## Tech Stack

- **Language:** Python 3.13 (pinned via `.python-version`; managed with uv)
- **Bot Framework:** discord.py (cog-based extension system)
- **Scheduler:** APScheduler (AsyncIOScheduler with cron triggers) + discord.ext.tasks loops
- **Database:** PostgreSQL via psycopg3 (AsyncConnectionPool)
- **Data Sources:** Charles Schwab API, Tiingo, Stooq, NASDAQ, TradingView, Capitol Trades, News API, ApeWisdom, YFinance
- **Analysis:** pandas-ta (RSI, MACD, ADX, OBV, RVOL), mplfinance (charts), backtesting.py
- **Notifications:** EventEmitter (thread-safe Queue) for scheduler job status
- **Infrastructure:** Docker + Docker Compose

## Repo Layout (monorepo since S001, 2026-07-05)

uv workspace: root `pyproject.toml` lists the members, one `uv.lock`, one shared `.venv` (`uv sync --all-packages`). Python pinned to 3.13 (`.python-version`).

```
legacy/       # v1, moved intact (legacy/src/rocketstocks/, legacy/tests/, legacy/pyproject.toml)
libs/         # v2 shared libraries: domain, persistence, query, observability, analysis, sources
services/     # v2 long-running daemons (ingest; serve/bot come with their slices)
tools/        # v2 one-shot CLIs (migrate; backfill/research come with their slices)
```

v2 naming rule (ADR-0001): import name = `rs_` + member dir (`libs/domain` → `rs_domain`); every member is `<root>/<member>/` with `pyproject.toml` + `src/rs_<member>/` + `tests/`. `rs_common`/`rs_utils` grab-bags are banned.

## v1 Package Layout

`src/` layout under `legacy/`, installed via `legacy/pyproject.toml`.

```
legacy/src/rocketstocks/
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
# Set up / update the shared venv (installs legacy + all v2 members editable)
uv sync --all-packages

# Run v1 locally
.venv/bin/python -m rocketstocks

# Run v1 tests
.venv/bin/pytest legacy/tests -q

# Run specific v1 test file
.venv/bin/pytest legacy/tests/bot/cogs/test_alerts_cog.py -v

# Run v2 member tests only (bare `.venv/bin/pytest` runs EVERYTHING: legacy + v2)
.venv/bin/pytest libs/ services/ tools/ -q

# Lint v2 members (legacy is never linted — code freeze)
.venv/bin/ruff check libs services tools

# Type-check v2 members (mypy + pydantic plugin; config in root pyproject.toml)
.venv/bin/mypy

# Import-layer contracts (import-linter; contracts in root pyproject.toml).
# A new member needs a line in root_packages and in each contract that covers it.
.venv/bin/lint-imports

# Clock-discipline gate (no ambient time in v2 src - ADR-0023)
scripts/check_clock_discipline.sh

# Migrations vs ORM models (run against a migrated DB; fails on drift)
RS_DB_URL=postgresql+psycopg://... .venv/bin/python -m rs_migrate check

# v2 DB tests need a TimescaleDB (skip visibly otherwise); spec container:
#   docker run -d --rm -p 5499:5432 -e POSTGRES_PASSWORD=t timescale/timescaledb:2.30.2-pg16
# Set RS_TEST_DB_URL to make DB tests mandatory (unreachable = failure, as in CI).
# Migrations CLI (DSN via RS_DB_URL; upgrade also applies the per-service roles and
# needs RS_DB_PASSWORD_{INGEST,BOT,SERVE,RESEARCH}, or pass --skip-roles on a bare dev DB):
RS_DB_URL=postgresql+psycopg://... .venv/bin/python -m rs_migrate upgrade

# Backtesting CLI
.venv/bin/python -m rocketstocks.backtest run volume_accumulation --benchmark SPY

# Docker (v1 image builds from legacy/ via the root Dockerfile)
docker compose up -d

# v2 stack: TimescaleDB (127.0.0.1:5498) + one-shot migrate gate +
# ingest/serve/bot from the shared Dockerfile.v2 image; JSON logs with
# correlation_id; metrics on 127.0.0.1:9090 (ingest) / :9091 (bot).
# Needs RS_DB_PASSWORD + service credentials in .env.
docker compose -f docker-compose.v2.yml up -d

# v2 operator tools (host-side, shared venv; exact procedures in docs/foundation/runbooks/slice-1.md)
.venv/bin/python -m rs_backfill verify-offset --dump PATH   # then popularity-import --dump PATH --zone ZONE
.venv/bin/python -m rs_ingest.tools.login                   # first Schwab login; later logins use /setup schwab
RS_RESEARCH_DB_URL=... .venv/bin/python -m rs_research event-study attention_surge
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
| **v2** foundation plan - charter, architecture, roadmap, slice specs | [docs/foundation/README.md](docs/foundation/README.md) |
| Architectural patterns & cross-cutting design | [.claude/docs/architectural_patterns.md](.claude/docs/architectural_patterns.md) |
| `core/` — analysis, content classes, scheduler, notifications | [.claude/docs/core.md](.claude/docs/core.md) |
| `data/` — DB wrapper, repositories, API clients | [.claude/docs/data.md](.claude/docs/data.md) |
| `bot/` — cogs, senders, views, bot lifecycle | [.claude/docs/bot.md](.claude/docs/bot.md) |
| `backtest/` — strategy framework architecture | [.claude/docs/backtesting.md](.claude/docs/backtesting.md) |
| Backtesting CLI usage & interpreting results | [.claude/docs/backtesting_guide.md](.claude/docs/backtesting_guide.md) |
| `api/` — public DataAPI facade, batch fetches, `rocketstocks-data` CLI | [.claude/docs/api.md](.claude/docs/api.md) |
| `eda/` — exploratory signal research (event detectors + analysis engines) | [.claude/docs/eda.md](.claude/docs/eda.md) |
| Test structure, fixtures & mocking patterns | [.claude/docs/tests.md](.claude/docs/tests.md) |

## Agent skills

### Issue tracker

Issues/PRDs live in GitHub Issues (`JWolf34/RocketStocks`) via the `gh` CLI. Note v2 slice specs are a separate surface under `docs/foundation/specs/`. See `docs/agents/issue-tracker.md`.

### Domain docs

Single-context: one root `CONTEXT.md`; ADRs live at `docs/v2/adr/` (**not** the default `docs/adr/`). See `docs/agents/domain.md`.
