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
| `alerts` (extended) | adds `rule_version`, `alert_price`, `quote_time`, unique `dedup_key`, `post_attempts` (the bot's capped retry) |
| `base_rate` | `alert_type`, `rule_version`, `computed_at`, `input` (`reconstructed` or `logged`), N, hit rate and median excess vs SPY at 1/5/10 trading days, `survivorship_caveat`, `n_unpriced` |
| `unpriced_symbol` | `symbol`, `first_seen`, `last_attempt`, `reason`: tickers Schwab cannot price |
| `schwab_auth_request` | `id`, `created_at`, `requested_by` (Discord id), `state`, `auth_url`, `callback_url`, `error`, `completed_at`: the `/setup schwab` handshake |

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

## Deviations from the plan

What was built differs from the sections above in these ways; the current runtime shape is in [architecture.md](../architecture.md).

- Horizons of 1/5/10 trading days are counted in exchange-calendar sessions, and a horizon matures only up to the last stored SPY bar, so a missing bar is a missing exit and never shifts a horizon.
- The `logged` base rate enters SPY at the alert's own benchmark quote (a new `benchmark` fact in the payload), with a counted fallback to SPY's previous close, so ticker and benchmark are measured from the same instant.
- Hit rate is the share of events with strictly positive excess return, and no `base_rate` row is written when N is 0, so an empty row never replaces a stale one.
- `base_rate` is append-only (readers take the latest per rule version and input), and matured horizons with no exit bar are counted in the caveat text rather than a column.
- The `volume` fact needs all 20 stored bars and a newest bar within 7 days, otherwise it is unavailable with a reason, because a partial average would mislead; the first alert for a ticker therefore often has none.
- The Nasdaq calendar is read ahead by the `earnings_calendar_warm` job (the cold 30-day scan did not fit the 20 second fact budget), and a lookup that still misses the cache stops its Nasdaq scan after 8 seconds and falls back to the EDGAR estimate.
- Every fact is `ok` or `FactUnavailable(reason)`, so one failed vendor call never blocks an alert.
- A dead Schwab token met inside the popularity poll also trips the daemon's token gate, so ops hears within one poll rather than only when a Schwab job next runs.
- The daemon starts gated, not exited, when no token is stored, so `restart: unless-stopped` cannot crash-loop it.
- The poll re-judges the previous boundary as well as its own (live snapshots only), so a failed alert write is caught up one boundary later; spec row 10 holds within that one-boundary limit.
- ApeWisdom encodes "not ranked 24 hours ago" as `rank_24h_ago` 0 and upvotes are a net score that can be negative, so the adapter and the v1 import map a zero prior rank to none and keep negative upvotes.
- `surge_bars` fetches from 30 sessions before the first event when a ticker has no bars, from the day after the newest otherwise, and back-fills when an older event predates stored bars; `adjusted` is stamped `False` until measured against a split ticker.
- `unpriced_symbol` records a 400 or 404 only for a symbol with no bars; other failures make the job fail visibly instead.
- `schwab_auth_request` has the states `requested`, `url_ready`, `callback_received`, `done`, `failed` and `expired`, and terminal states clear `callback_url` because it carries the OAuth code.
- New tables beyond the list above: `import_verification` (the offset proof is stored with the database it was measured against) and `schwab_token_notice`, plus the `schwab_token_issue` view that lets the bot read token age without a grant on `schwab_tokens`.
- `popularity_snapshot.upvotes` is nullable because v1's column was never `NOT NULL`; the live adapter still requires it.
- The import's offset check needs at least 99% agreement and a 1000-row overlap; ties are resolved by the spring-forward evidence, otherwise the result is "ambiguous" and nothing is recorded.
- Imported rows set `fetched_at = polled_at`, and rows in the spring-forward gap are rejected and counted, never guessed.
- Roles: `upgrade` fails without the role passwords unless `--skip-roles` is passed, and passwords are restricted to `A-Za-z0-9._~-` (16+ characters) because compose cannot URL-encode them.
- The bot's retry cap is 5 attempts at one sweep interval apart, kept in memory, and failed-to-post is `post_attempts` at the cap with `posted_at` NULL rather than a new column.
- The bot DMs token-age notices at 24h and 6h and offers `/setup schwab`, which the spec's scope named but did not detail; the OAuth context lives in ingest memory, so a request open across an ingest restart must be redone.
- `SchwabAuthError` carries only the OAuth error code, and the database engine hides bound parameters in error logs, because both reached logs and ops notices.

## Out of scope

Symbol master and the optionable flag; streaming and 1-minute bars; earnings-reaction and large-move alerts; digests; any Report; watchlists and paper trading; `serve`, the Data API and MCP; LLM or Reddit explanations; v1 daily bars; other ApeWisdom filters; retiring v1 (an operator step once criterion 5 passes).

## Links

[Launch push surfaces](https://github.com/JWolf34/RocketStocks/issues/46) - [Data collection scope](https://github.com/JWolf34/RocketStocks/issues/49) - [Architecture one-pager](https://github.com/JWolf34/RocketStocks/issues/52) - [Backtest and signal-evaluation tooling](https://github.com/JWolf34/RocketStocks/issues/51) - [Build process and cutover](https://github.com/JWolf34/RocketStocks/issues/53) - [architecture.md](../architecture.md)
