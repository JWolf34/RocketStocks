-- RocketStocks v1 production probe - READ-ONLY (SELECTs only).
-- Run against the production v1 database, e.g.:
--   psql "$V1_PROD_DSN" -f v1-prod-probe.sql > v1-prod-probe.out
-- Purpose: settle (a) whether a cheap options-flow / price-signal evidence study
-- is possible from data v1 already has, and (b) which v1 features actually get used.

\echo '== 1. options_chain_history coverage by month (partitions are only created at startup - gaps expected if the container ran >1 month without restart)'
SELECT date_trunc('month', snapshot_date)::date AS month,
       count(DISTINCT snapshot_date) AS days,
       count(DISTINCT ticker)        AS tickers,
       count(*)                      AS rows
FROM options_chain_history GROUP BY 1 ORDER BY 1;

\echo '== 2. daily / 5-minute price history depth'
SELECT 'daily' AS t, min(date)::text, max(date)::text, count(DISTINCT ticker) FROM daily_price_history
UNION ALL
SELECT '5min', min(datetime)::text, max(datetime)::text, count(DISTINCT ticker) FROM five_minute_price_history;

\echo '== 3. popularity history depth (ApeWisdom)'
SELECT min(datetime)::text, max(datetime)::text, count(DISTINCT ticker), count(*) FROM popularity;

\echo '== 4. alerts posted per type per month'
SELECT alert_type, date_trunc('month', date)::date AS month, count(*)
FROM alerts GROUP BY 1, 2 ORDER BY 2, 1;

\echo '== 5. popularity surges: confirm / expire rates'
SELECT surge_types, count(*), avg(confirmed::int) AS confirm_rate, avg(expired::int) AS expire_rate
FROM popularity_surges GROUP BY 1 ORDER BY 2 DESC;

\echo '== 6. market signals by status'
SELECT status, count(*) FROM market_signals GROUP BY 1;

\echo '== 7. paper trading activity per user'
SELECT guild_id, user_id, count(*), min(executed_at), max(executed_at)
FROM paper_transactions GROUP BY 1, 2 ORDER BY 3 DESC;

\echo '== 8. watchlists and their sizes'
SELECT id, watchlist_type, cardinality(string_to_array(tickers, ' ')) AS n FROM watchlists ORDER BY n DESC;

\echo '== 9. guilds configured'
SELECT count(DISTINCT guild_id) FROM channel_config;

\echo '== 10. was backtest ever run?'
SELECT run_id, strategy_name, created_at FROM backtest_runs ORDER BY created_at;
