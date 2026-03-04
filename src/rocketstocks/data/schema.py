"""DDL helpers — create/drop tables using a Postgres instance."""
import logging
from psycopg2 import sql

logger = logging.getLogger(__name__)

_CREATE_SCRIPT = """
CREATE TABLE IF NOT EXISTS tickers (
    ticker      varchar(8) PRIMARY KEY,
    cik         char(10) NOT NULL,
    name        varchar(255) NOT NULL,
    country     varchar(40),
    ipoyear     char(4),
    industry    varchar(64),
    sector      varchar(64),
    url         varchar(64)
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
    epsForecast         float,
    fiscalQuarterEnding varchar(10),
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
    alert_data  json,
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
"""


def create_tables(db) -> None:
    """Create all application tables (idempotent)."""
    logger.debug("Running script to create tables in database...")
    with db._cursor() as cur:
        cur.execute(_CREATE_SCRIPT)
    logger.debug("Create script completed successfully!")


def drop_all_tables(db) -> None:
    """Drop all application tables."""
    with db._cursor() as cur:
        cur.execute(_DROP_ALL_SCRIPT)
    logger.debug("All database tables dropped")


def drop_table(db, table: str) -> None:
    """Drop a single table by name."""
    from psycopg2 import sql as _sql
    drop_script = _sql.SQL("DROP TABLE IF EXISTS {t};").format(t=_sql.Identifier(table))
    with db._cursor() as cur:
        cur.execute(drop_script)
    logger.debug(f"Dropped table '{table}' from database")
