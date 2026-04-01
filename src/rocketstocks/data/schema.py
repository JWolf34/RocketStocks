"""DDL helpers — create/drop tables using a Postgres instance."""
import logging

logger = logging.getLogger(__name__)

_MIGRATION_SCRIPT = """
ALTER TABLE popularity_surges ADD COLUMN IF NOT EXISTS mention_acceleration float;
ALTER TABLE tickers DROP COLUMN IF EXISTS url;
ALTER TABLE tickers ADD COLUMN IF NOT EXISTS exchange varchar(16);
ALTER TABLE tickers ADD COLUMN IF NOT EXISTS security_type varchar(8);
ALTER TABLE tickers ADD COLUMN IF NOT EXISTS sic_code varchar(8);
ALTER TABLE tickers ADD COLUMN IF NOT EXISTS delist_date date;
ALTER TABLE tickers ALTER COLUMN cik DROP NOT NULL;
ALTER TABLE alerts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
UPDATE tickers SET delist_date = NULL
WHERE delist_date IS NOT NULL
  AND delist_date > CURRENT_DATE - INTERVAL '30 days';
ALTER TABLE alerts ALTER COLUMN alert_data TYPE jsonb USING alert_data::jsonb;
ALTER TABLE market_signals ALTER COLUMN signal_data TYPE jsonb USING signal_data::jsonb;
ALTER TABLE market_signals ADD COLUMN IF NOT EXISTS signal_source varchar(32) DEFAULT 'composite';
ALTER TABLE market_signals ADD COLUMN IF NOT EXISTS price_at_flag float;
ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS watchlist_type VARCHAR(16) NOT NULL DEFAULT 'named';
ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS owner_id BIGINT;
ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS display_name VARCHAR(255);
ALTER TABLE watchlists ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
CREATE INDEX IF NOT EXISTS idx_watchlists_type ON watchlists (watchlist_type);
CREATE INDEX IF NOT EXISTS idx_watchlists_owner ON watchlists (owner_id) WHERE owner_id IS NOT NULL;
ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS exchange VARCHAR(16);
ALTER TABLE strategy_stats ADD COLUMN IF NOT EXISTS mean_excess_return FLOAT;
ALTER TABLE strategy_stats ADD COLUMN IF NOT EXISTS pct_beating_buy_hold FLOAT;
ALTER TABLE strategy_stats ADD COLUMN IF NOT EXISTS mean_exposure_pct FLOAT;
"""

_CREATE_SCRIPT = """
CREATE TABLE IF NOT EXISTS tickers (
    ticker        varchar(8) PRIMARY KEY,
    cik           char(10),
    name          varchar(255) NOT NULL,
    country       varchar(40),
    ipoyear       char(4),
    industry      varchar(64),
    sector        varchar(64),
    exchange      varchar(16),
    security_type varchar(8),
    sic_code      varchar(8),
    delist_date   date
);

CREATE TABLE IF NOT EXISTS upcoming_earnings (
    date                    date NOT NULL,
    ticker                  varchar(8) PRIMARY KEY,
    time                    varchar(32),
    fiscal_quarter_ending   varchar(10),
    eps_forecast            varchar(8),
    no_of_ests              varchar(8),
    last_year_eps           varchar(8),
    last_year_rpt_dt        varchar(10)
);

CREATE TABLE IF NOT EXISTS watchlists (
    ID              varchar(255) PRIMARY KEY,
    tickers         TEXT,
    systemgenerated boolean,
    watchlist_type  varchar(16) NOT NULL DEFAULT 'named',
    owner_id        BIGINT,
    display_name    varchar(255),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS popularity (
    datetime            timestamp,
    rank                int,
    ticker              varchar(8),
    name                varchar(255),
    mentions            int,
    upvotes             int,
    rank_24h_ago        int,
    mentions_24h_ago    int,
    PRIMARY KEY (datetime, rank)
);

CREATE TABLE IF NOT EXISTS historical_earnings (
    date                date,
    ticker              varchar(8),
    eps                 float,
    surprise            float,
    epsforecast         float,
    fiscalquarterending varchar(10),
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS reports (
    type        varchar(64) PRIMARY KEY,
    messageid   bigint
);

CREATE TABLE IF NOT EXISTS alerts (
    date        date,
    ticker      varchar(8),
    alert_type  varchar(64),
    messageid   bigint,
    alert_data  jsonb DEFAULT '{}'::jsonb,
    created_at  timestamptz DEFAULT NOW(),
    PRIMARY KEY (date, ticker, alert_type)
);

CREATE TABLE IF NOT EXISTS daily_price_history (
    ticker  varchar(8),
    open    float,
    high    float,
    low     float,
    close   float,
    volume  bigint,
    date    date,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS five_minute_price_history (
    ticker      varchar(8),
    open        float,
    high        float,
    low         float,
    close       float,
    volume      bigint,
    datetime    timestamp,
    PRIMARY KEY (ticker, datetime)
);

CREATE TABLE IF NOT EXISTS ct_politicians (
    politician_id   char(7) PRIMARY KEY,
    name            varchar(64),
    party           varchar(16),
    state           varchar(32)
);

CREATE TABLE IF NOT EXISTS channel_config (
    guild_id    BIGINT      NOT NULL,
    config_type VARCHAR(64) NOT NULL,
    channel_id  BIGINT      NOT NULL,
    PRIMARY KEY (guild_id, config_type)
);

CREATE TABLE IF NOT EXISTS ticker_stats (
    ticker          varchar(8) PRIMARY KEY,
    market_cap      bigint,
    classification  varchar(16) NOT NULL DEFAULT 'standard',
    volatility_20d  float,
    mean_return_20d float,
    std_return_20d  float,
    mean_return_60d float,
    std_return_60d  float,
    avg_rvol_20d    float,
    std_rvol_20d    float,
    bb_upper        float,
    bb_lower        float,
    bb_mid          float,
    updated_at      timestamp DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS popularity_surges (
    ticker               varchar(8) NOT NULL,
    flagged_at           timestamp NOT NULL,
    surge_types          varchar(255) NOT NULL,
    current_rank         int,
    mention_ratio        float,
    rank_change          int,
    price_at_flag        float,
    alert_message_id     bigint,
    confirmed            boolean DEFAULT FALSE,
    confirmed_at         timestamp,
    expired              boolean DEFAULT FALSE,
    mention_acceleration float,
    PRIMARY KEY (ticker, flagged_at)
);

CREATE TABLE IF NOT EXISTS market_signals (
    ticker           varchar(8) NOT NULL,
    detected_at      timestamp NOT NULL,
    composite_score  float NOT NULL,
    price_z          float,
    vol_z            float,
    pct_change       float,
    dominant_signal  varchar(16),
    rvol             float,
    status           varchar(16) NOT NULL DEFAULT 'pending',
    confirmed_at     timestamp,
    alert_message_id bigint,
    signal_data      jsonb DEFAULT '[]'::jsonb,
    signal_source    varchar(32) DEFAULT 'composite',
    price_at_flag    float,
    PRIMARY KEY (ticker, detected_at)
);

CREATE TABLE IF NOT EXISTS alert_roles (
    guild_id   BIGINT      NOT NULL,
    role_key   VARCHAR(64) NOT NULL,
    role_id    BIGINT      NOT NULL,
    PRIMARY KEY (guild_id, role_key)
);

CREATE TABLE IF NOT EXISTS schwab_tokens (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    token_data  JSONB NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT  schwab_tokens_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS bot_settings (
    key   VARCHAR(64) PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS earnings_results (
    date          date NOT NULL,
    ticker        varchar(8) NOT NULL,
    eps_actual    float,
    eps_estimate  float,
    surprise_pct  float,
    posted_at     timestamptz NOT NULL DEFAULT NOW(),
    source        varchar(16) NOT NULL DEFAULT 'yfinance',
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS iv_history (
    ticker          varchar(8)  NOT NULL,
    date            date        NOT NULL,
    iv              real,
    atm_iv          real,
    put_call_ratio  real,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id          SERIAL PRIMARY KEY,
    strategy_name   VARCHAR(64) NOT NULL,
    timeframe       VARCHAR(8) NOT NULL,
    parameters      JSONB DEFAULT '{}'::jsonb,
    filters         JSONB DEFAULT '{}'::jsonb,
    ticker_count    INT NOT NULL DEFAULT 0,
    start_date      DATE,
    end_date        DATE,
    cash            FLOAT NOT NULL DEFAULT 10000,
    commission      FLOAT NOT NULL DEFAULT 0.002,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_results (
    result_id       SERIAL PRIMARY KEY,
    run_id          INT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    ticker          VARCHAR(8) NOT NULL,
    classification  VARCHAR(16),
    sector          VARCHAR(64),
    return_pct      FLOAT,
    sharpe_ratio    FLOAT,
    max_drawdown    FLOAT,
    win_rate        FLOAT,
    num_trades      INT,
    avg_trade_pct   FLOAT,
    profit_factor   FLOAT,
    exposure_pct    FLOAT,
    equity_final    FLOAT,
    buy_hold_pct    FLOAT,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, ticker)
);

CREATE TABLE IF NOT EXISTS strategy_stats (
    stat_id            SERIAL PRIMARY KEY,
    run_id             INT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    group_key          VARCHAR(64) NOT NULL,
    group_value        VARCHAR(64),
    ticker_count       INT,
    mean_return        FLOAT,
    median_return      FLOAT,
    std_return         FLOAT,
    mean_sharpe        FLOAT,
    mean_win_rate      FLOAT,
    total_trades       INT,
    mean_max_dd        FLOAT,
    mean_profit_factor FLOAT,
    t_stat             FLOAT,
    p_value            FLOAT,
    significant        BOOLEAN,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, group_key)
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    trade_id        SERIAL PRIMARY KEY,
    run_id          INT NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
    ticker          VARCHAR(8) NOT NULL,
    entry_time      TIMESTAMPTZ NOT NULL,
    exit_time       TIMESTAMPTZ NOT NULL,
    entry_price     FLOAT NOT NULL,
    exit_price      FLOAT NOT NULL,
    size            INT NOT NULL,
    pnl             FLOAT NOT NULL,
    return_pct      FLOAT NOT NULL,
    commission      FLOAT NOT NULL DEFAULT 0,
    duration_bars   INT NOT NULL,
    regime          VARCHAR(16)
);
CREATE INDEX IF NOT EXISTS idx_bt_trades_run ON backtest_trades(run_id);
CREATE INDEX IF NOT EXISTS idx_bt_trades_ticker ON backtest_trades(run_id, ticker);

CREATE TABLE IF NOT EXISTS paper_portfolios (
    guild_id    BIGINT      NOT NULL,
    user_id     BIGINT      NOT NULL,
    cash        FLOAT       NOT NULL DEFAULT 10000.0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (guild_id, user_id)
);

CREATE TABLE IF NOT EXISTS paper_positions (
    guild_id        BIGINT     NOT NULL,
    user_id         BIGINT     NOT NULL,
    ticker          VARCHAR(8) NOT NULL,
    shares          INT        NOT NULL,
    avg_cost_basis  FLOAT      NOT NULL,
    PRIMARY KEY (guild_id, user_id, ticker)
);

CREATE TABLE IF NOT EXISTS paper_transactions (
    id          SERIAL      PRIMARY KEY,
    guild_id    BIGINT      NOT NULL,
    user_id     BIGINT      NOT NULL,
    ticker      VARCHAR(8)  NOT NULL,
    side        VARCHAR(4)  NOT NULL,
    shares      INT         NOT NULL,
    price       FLOAT       NOT NULL,
    total       FLOAT       NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_snapshots (
    guild_id        BIGINT NOT NULL,
    user_id         BIGINT NOT NULL,
    snapshot_date   DATE   NOT NULL,
    portfolio_value FLOAT  NOT NULL,
    cash            FLOAT  NOT NULL,
    positions_value FLOAT  NOT NULL,
    PRIMARY KEY (guild_id, user_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS paper_pending_orders (
    id              SERIAL      PRIMARY KEY,
    guild_id        BIGINT      NOT NULL,
    user_id         BIGINT      NOT NULL,
    ticker          VARCHAR(8)  NOT NULL,
    side            VARCHAR(4)  NOT NULL,
    shares          INT         NOT NULL,
    quoted_price    FLOAT       NOT NULL,
    status          VARCHAR(10) NOT NULL DEFAULT 'pending',
    queued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_at     TIMESTAMPTZ,
    execution_price FLOAT
);
"""

_DROP_ALL_SCRIPT = """
DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS daily_price_history;
DROP TABLE IF EXISTS five_minute_price_history;
DROP TABLE IF EXISTS historical_earnings;
DROP TABLE IF EXISTS popularity;
DROP TABLE IF EXISTS reports;
DROP TABLE IF EXISTS tickers;
DROP TABLE IF EXISTS upcoming_earnings;
DROP TABLE IF EXISTS watchlists;
DROP TABLE IF EXISTS ct_politicians;
DROP TABLE IF EXISTS channel_config;
DROP TABLE IF EXISTS ticker_stats;
DROP TABLE IF EXISTS popularity_surges;
DROP TABLE IF EXISTS market_signals;
DROP TABLE IF EXISTS alert_roles;
DROP TABLE IF EXISTS schwab_tokens;
DROP TABLE IF EXISTS bot_settings;
DROP TABLE IF EXISTS earnings_results;
DROP TABLE IF EXISTS iv_history;
DROP TABLE IF EXISTS backtest_trades;
DROP TABLE IF EXISTS backtest_results;
DROP TABLE IF EXISTS strategy_stats;
DROP TABLE IF EXISTS backtest_runs;
DROP TABLE IF EXISTS paper_pending_orders;
DROP TABLE IF EXISTS paper_transactions;
DROP TABLE IF EXISTS paper_snapshots;
DROP TABLE IF EXISTS paper_positions;
DROP TABLE IF EXISTS paper_portfolios;
"""


async def migrate_tickers_schema(db) -> None:
    """Apply schema migrations to existing tables (idempotent)."""
    logger.debug("Running schema migrations...")
    await db.execute(_MIGRATION_SCRIPT)
    logger.debug("Schema migrations completed successfully!")


async def migrate_watchlists_metadata(db) -> None:
    """Backfill watchlist_type and owner_id for existing rows (idempotent).

    Personal watchlist IDs (all-digit strings, e.g. "123456789") are renamed to
    "personal:<user_id>" and their watchlist_type set to 'personal'.  Classification
    watchlists ("class:volatile") get watchlist_type='classification'.  System rows
    (systemgenerated=true) get watchlist_type='system'.  Everything else stays 'named'.
    """
    # Idempotency guard — if any personal: rows already exist the migration has run.
    row = await db.execute(
        "SELECT 1 FROM watchlists WHERE id LIKE 'personal:%' LIMIT 1",
        fetchone=True,
    )
    if row is not None:
        logger.debug("Watchlist metadata migration already applied — skipping.")
        return

    logger.debug("Running watchlist metadata migration...")

    # Mark system rows
    await db.execute(
        "UPDATE watchlists SET watchlist_type = 'system' WHERE systemgenerated = true"
    )

    # Mark classification rows and extract display_name from prefix
    await db.execute(
        "UPDATE watchlists SET watchlist_type = 'classification', "
        "display_name = SUBSTRING(id FROM 7) "
        "WHERE id LIKE 'class:%'"
    )

    # Rename personal rows (numeric IDs) — requires PK change, done row by row in a transaction
    personal_rows = await db.execute(
        "SELECT id, tickers, systemgenerated FROM watchlists WHERE id ~ '^[0-9]+$'"
    )
    if personal_rows:
        async with db.transaction() as conn:
            for row in personal_rows:
                old_id, tickers_str, system_gen = row[0], row[1], row[2]
                new_id = f"personal:{old_id}"
                owner_id = int(old_id)
                await conn.execute(
                    "INSERT INTO watchlists (id, tickers, systemgenerated, watchlist_type, owner_id) "
                    "VALUES (%s, %s, %s, 'personal', %s) ON CONFLICT DO NOTHING",
                    [new_id, tickers_str, system_gen, owner_id],
                )
                await conn.execute(
                    "DELETE FROM watchlists WHERE id = %s",
                    [old_id],
                )

    logger.debug("Watchlist metadata migration completed.")


async def create_tables(db) -> None:
    """Create all application tables (idempotent)."""
    logger.debug("Running script to create tables in database...")
    await db.execute(_CREATE_SCRIPT)
    await migrate_tickers_schema(db)
    await migrate_watchlists_metadata(db)
    logger.debug("Create script completed successfully!")


async def drop_all_tables(db) -> None:
    """Drop all application tables."""
    await db.execute(_DROP_ALL_SCRIPT)
    logger.debug("All database tables dropped")


async def drop_table(db, table: str) -> None:
    """Drop a single table by name."""
    from psycopg import sql as _sql
    drop_script = _sql.SQL("DROP TABLE IF EXISTS {t};").format(t=_sql.Identifier(table))
    await db.execute(drop_script)
    logger.debug(f"Dropped table '{table}' from database")
