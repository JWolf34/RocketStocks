# Slice 1 - Attention surge

## Goal

A real attention-surge alert, with a facts block and its measured base rate, reaches Discord from ApeWisdom data, and the foundation every later slice reuses exists: the multi-session ingest daemon, the versioned alert log, `rs_query`, `rs_research` and per-service database roles.

## Scope

In: ApeWisdom ingest, the v1 popularity import, daily bars for surge names, the surge rule, the facts block, one event study, posting and the alert log, plus the Schwab re-auth path: a `/setup schwab` paste in Discord becomes a request that ingest carries out, so the token-expiry rehearsal can recover ([Architecture one-pager](https://github.com/JWolf34/RocketStocks/issues/52)).
Out: see the end of this page.

## Interface

**Ingest daemon** (`services/ingest`, closes audit items H2, H3, H4 and M3 from the [v2 code audit](../audit/v2-code-audit.md)).
`run_session` (one session, then exit) becomes a supervised asyncio daemon with an in-process cron table.
Jobs: popularity poll every 30 minutes (v1's cadence, so imported and live rows are comparable), surge-bars, and the weekly `rs_research` subprocess.
A dead Schwab token pauses Schwab jobs and sends one ops notice, while the poll keeps running.
Alert writes carry a `dedup_key` (rule, ticker, poll boundary) and retry durably; failure calls `notify_ops()`, never swallow-and-count.
The stream, aggregator and single-symbol window (audit item H1) are left untouched until slice 4.
New `notify_ops()` in `rs_observability` posts to the ops webhook.
The migrate gate creates one role per service (ingest, bot, serve, research); only migrate is superuser; only ingest reads `schwab_tokens`.

**Tables** (new migrations; the `outbox` table is dropped).

| Table | Shape |
|---|---|
| `popularity_snapshot` (hypertable) | `polled_at` (UTC boundary), `ticker`, `rank`, `mentions`, `upvotes`, `rank_24h_ago`, `mentions_24h_ago`, `fetched_at`, `origin` (`live` or `v1_import`); key (`polled_at`, `ticker`), insert is `ON CONFLICT DO NOTHING` |
| `daily_bar` (hypertable) | `symbol`, `session_date`, OHLCV, `fetched_at`, `adjusted`; rejected unless `exchange_calendars` lists the session |
| `alerts` (extended) | adds `rule_version`, `alert_price`, `quote_time`, unique `dedup_key` |
| `base_rate` | `alert_type`, `rule_version`, `computed_at`, `input` (`reconstructed` or `logged`), N, hit rate and median excess vs SPY at 1/5/10 trading days, `survivorship_caveat` |

**Surge rule** (`rs_analysis.attention_surge`, `RULE_VERSION = "1"`), a pure function over one ticker's snapshots.
Derived from `evaluate_popularity_surge` in `legacy/src/rocketstocks/core/analysis/popularity_signals.py`.
All thresholds are constants in the rule module and tunable; changing any bumps the version.

- Skip when `mentions < 15`.
- **mention_surge:** `mentions >= 3.0 * mentions_24h_ago` and `mentions_24h_ago >= 15`; the multiplier is 5.0 for rank 25 or better, and the base is 10 beyond rank 500.
- **rank_jump:** `rank_24h_ago - rank >= 50` and that gain divided by `rank` is at least 1.5.
- An event is either rule, and the alert records which fired.
- At most one alert per ticker per 24 hours.
- Dropped from v1: new_entrant, velocity_spike and the acceleration gate, because they need rolling history and the signal study reproduced only the first two rules.

**Import** (`tools/backfill`, `python -m rs_backfill popularity-import --dump PATH`), reading a dump of v1's `popularity` table.
Mapping: `datetime` to `polled_at` in UTC, other columns one to one, `name` dropped, `origin = v1_import`.
v1 wrote a naive `datetime.now()` rounded down to 30 minutes (`round_down_nearest_minute` in `legacy/src/rocketstocks/core/utils/dates.py`, called from `post_popularity_screener` in `bot/cogs/reports.py`), in a container zone nobody documented.
The import refuses to run until the offset is verified against live data.
While v1 and v2 both poll ApeWisdom, match v1 rows to v2 rows by (ticker, rank, mentions, mentions_24h_ago) in adjacent buckets; the offset that matches at least 99% of the overlap is the zone.
Cross-check: a local zone leaves no rows in the spring-forward hour of 2026-03-08 and loses the repeated hour on 2025-11-02 to v1's primary key.
The result is recorded in the pull request.
Reruns insert zero rows.

**Daily bars.** The surge-bars job fetches Schwab price history for every ticker in a surge event (imported or live) that lacks bars, plus SPY.
Tickers Schwab cannot price are recorded as unpriced and counted in the base rate; this is the accepted survivorship caveat ([Vendor stack](https://github.com/JWolf34/RocketStocks/issues/48)).

**Facts block**, computed by ingest at write time and stored in the alert payload, so an alert never changes.

- Price move: alert-time Schwab quote against the previous close, and volume against the 20-day average.
- Recent 8-Ks from the EDGAR submissions API (filing date, items, link).
- Next earnings date from a Nasdaq calendar adapter, labelled estimated when it falls back.

**Base rate.** `rs_research event-study attention_surge` replays the rule over all snapshots, enters at the close of the first session after the event date (the [Signal evidence study](https://github.com/JWolf34/RocketStocks/issues/45) convention), and writes a `reconstructed` row.
It also writes a `logged` row from alerts, entering at `alert_price`; the bot shows the `logged` row once its N passes a tunable minimum (30), otherwise `reconstructed`.

**`rs_query`.** New library of typed async read functions with row caps and pagination: `popularity_history`, `daily_bars`, `current_base_rate`, `recent_alerts`.
Its readers in this slice are the bot and `rs_research`.

**Bot.** Renders the payload and the current base rate, posts once, never edits.
NOTIFY wakes the `posted_at IS NULL` sweep.
Retries are capped and then call `notify_ops()`, and rendering respects Discord field limits.

## Acceptance

| # | Criterion | Check | Done item |
|---|---|---|---|
| 1 | Gates pass | `ruff check libs services tools`, `pytest libs services tools -q` against Timescale, `scripts/check_clock_discipline.sh`, the type checker, `alembic check`, import-linter | CI green |
| 2 | Roles are least-privilege | A test connects as bot and serve roles and is denied `SELECT` on `schwab_tokens` | CI green |
| 3 | Writes are idempotent | Run the poll for one boundary twice; row and alert counts are unchanged | CI green |
| 4 | Rule is deterministic | A synthetic history yields the expected events and the same alerts on replay | CI green |
| 5 | Import is exact | A synthetic dump imports once and zero rows on rerun; the offset check passes on real data | Live run |
| 6 | Bars are clean | A non-session date and an epoch-zero timestamp are rejected | CI green |
| 7 | Base rate is stored | The event-study command writes a row `rs_query` reads, with N, hit rate and medians at 1/5/10 days | Base rate |
| 8 | A real alert posts | A recorded live poll produces a Discord alert with facts block, base rate, `rule_version` and `alert_price`, `posted_at` set, no edit | Live run |
| 9 | Token expiry | Expire the token: bars pause, one ops notice, polling continues, recovery after re-login | Rehearsal |
| 10 | Ingest restart | Kill ingest mid-poll: no lost or duplicated rows or alerts after restart | Rehearsal |
| 11 | Backup restore | Restore a snapshot to a scratch VM; row counts match | Rehearsal |

## Out of scope

Symbol master and the optionable flag; streaming and 1-minute bars; earnings-reaction and large-move alerts; digests; any Report; watchlists and paper trading; `serve`, the Data API and MCP; LLM or Reddit explanations; v1 daily bars; other ApeWisdom filters; retiring v1 (an operator step once criterion 5 passes).

## Links

[Launch push surfaces](https://github.com/JWolf34/RocketStocks/issues/46) - [Data collection scope](https://github.com/JWolf34/RocketStocks/issues/49) - [Architecture one-pager](https://github.com/JWolf34/RocketStocks/issues/52) - [Backtest and signal-evaluation tooling](https://github.com/JWolf34/RocketStocks/issues/51) - [Build process and cutover](https://github.com/JWolf34/RocketStocks/issues/53) - [architecture.md](../architecture.md)
