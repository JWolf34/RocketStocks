"""DDL helpers — create/drop tables using a Postgres instance."""
import logging

logger = logging.getLogger(__name__)

_MIGRATION_SCRIPT = """
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
    systemgenerated boolean
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
    ticker          varchar(8) NOT NULL,
    flagged_at      timestamp NOT NULL,
    surge_types     varchar(255) NOT NULL,
    current_rank    int,
    mention_ratio   float,
    rank_change     int,
    price_at_flag   float,
    alert_message_id bigint,
    confirmed       boolean DEFAULT FALSE,
    confirmed_at    timestamp,
    expired         boolean DEFAULT FALSE,
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
"""


async def migrate_tickers_schema(db) -> None:
    """Apply schema migrations to existing tables (idempotent)."""
    logger.debug("Running schema migrations...")
    await db.execute(_MIGRATION_SCRIPT)
    logger.debug("Schema migrations completed successfully!")


async def create_tables(db) -> None:
    """Create all application tables (idempotent)."""
    logger.debug("Running script to create tables in database...")
    await db.execute(_CREATE_SCRIPT)
    await migrate_tickers_schema(db)
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
