# RocketStocks v2 architecture

The one-page runtime shape of v2.
Each decision's rationale lives in the linked ticket on the [v2 foundation map](https://github.com/JWolf34/RocketStocks/issues/40); this page is the current truth and is edited in place as slices land.
Surviving ADRs live in `docs/v2/adr/`; an archived ADR number cited in code resolves at git tag `v2-plan-archive-2026-10`.

## Processes

One TimescaleDB, a one-shot migrate gate, and three services from one shared image ([Architecture one-pager](https://github.com/JWolf34/RocketStocks/issues/52)).

- **ingest** brings data in: sources -> validate -> stamp -> database.
  It is the only Schwab caller and the only token holder.
  It runs the in-process cron table, the pollers, the detectors and the alert writer, as a multi-session daemon that survives weekends and token expiry (see [The ingest daemon](#the-ingest-daemon)).
  The stream and its aggregator are not wired into the daemon until slice 4; their modules stay in the tree untouched.
- **bot** owns Discord: posting alerts and digests, Reports, watchlists, paper trading, channel config.
  It never calls a vendor and never runs detection.
- **serve** is the LAN-only Data API for John's own tools: REST under `/v1` plus a hand-curated MCP server (5-10 tools) at `/mcp`, plain HTTP, one API key per client ([Data API shape](https://github.com/JWolf34/RocketStocks/issues/44)).
  It is read-only.

## Data flow

- **Streamed (slice 4):** Schwab level-one quotes for the ~500-name stream band (rule-based liquid band plus every watchlist ticker), aggregated to 1-minute bars and persisted going forward ([Data collection scope](https://github.com/JWolf34/RocketStocks/issues/49)).
- **Nightly (later slices):** symbol master, daily bars for every active listing, then the options EOD full scan of the optionable universe (22:00 CT launch, 03:00 CT hard deadline; the scan's roster, retry/split ladder and tombstone decisions are on the superseded [options EOD full scan map](https://github.com/JWolf34/RocketStocks/issues/6), restated by slice 2's spec).
- **Frequent polls:** ApeWisdom every 30 minutes (live in slice 1).
  SEC EDGAR 8-Ks are read per alert for its facts block, not polled on a schedule.
  The Nasdaq earnings calendar is read ahead by the `earnings_calendar_warm` job, so an alert's lookup is normally memory-only.
- **On demand:** the bot writes a collection request row; ingest fetches, validates and stores, then NOTIFYs; the bot reads the stored result through the query layer, falling back to the latest stored data labelled "as of" if ingest is down.
- Every vendor payload is validated at the edge; bars off an `exchange_calendars` session and epoch-zero timestamps are rejected ([Signal evidence study](https://github.com/JWolf34/RocketStocks/issues/45)).
- Stream bars are immutable once closed and never reconciled to REST bars; detection runs on bar close only.
- In-memory state is a projection of the database, never a source of truth.
- Detectors run on bar close and after each popularity poll and write an alert row with its alert-time price and rule version ([Launch push surfaces](https://github.com/JWolf34/RocketStocks/issues/46)).
- Alert writes use idempotent keys and a durable retry, never swallow-and-count.
- ingest wakes the bot with NOTIFY; the bot's `posted_at IS NULL` sweep is what makes delivery correct.
  The outbox table is cut.

## Storage

- Postgres with Timescale, no Redis.
- Hypertables for 1-minute bars, daily bars, options chain snapshots and popularity; plain tables for everything else.
- Options chains are forward-only, immutable, stamped snapshots: never updated in place, never backfilled.
- Compression by age on every hypertable and no retention policy, because forward-only data cannot be rebought; a disk alarm instead.
- Continuous aggregates and freshness-policy tables are deferred until a reader needs them.

## Query layer and readers

- `rs_query` is one library of typed async read functions with row caps and pagination, ignorant of HTTP, MCP and Discord.
- Its three readers are the bot (in-process), serve (REST and MCP adapters) and `rs_research`; none goes through another.
- Writes (watchlists, paper trades, config, `posted_at`, collection requests) go through persistence repositories, never `rs_query`.
- `rs_research` runs event studies that write versioned base rates the bot reads at post time, plus vectorised pandas backtests ([Backtest and signal-evaluation tooling](https://github.com/JWolf34/RocketStocks/issues/51)).

## Scheduling

- ingest's in-process cron table owns every Schwab-touching job against one rate budget (the Schwab limiter, 100 calls per minute by default).
  The registry is `services/ingest/src/rs_ingest/jobs.py`.
- Planned jobs for later slices: symbol master, nightly daily bars, options EOD scan.
- The bot schedules its own digests (daily post-close, Monday earnings preview), which only read the database.

### The ingest daemon

- One asyncio process supervises the jobs: each job runs as its own task with a timeout, a fault in one never stops another, and a slot that passes while the daemon is down or the job is still running is skipped, never back-filled.
- A job marked `run_on_start` runs once at startup, which is how a restart finishes the current boundary.
- Three consecutive failures of a job call `notify_ops()` once; the first success afterwards sends a recovery notice.
- Slice 1 jobs:

| Job | Schedule | Needs Schwab | Notes |
|---|---|---|---|
| `popularity_poll` | every 30 minutes on the UTC :00 and :30 boundary | no | stores the ApeWisdom snapshot, judges the surge rule, writes alerts with their facts; 20 minute timeout |
| `earnings_calendar_warm` | every 2 hours on the UTC even-hour boundary, plus once at startup | no | reads the next 30 weekdays of Nasdaq's earnings calendar into the cache the poll's lookups share, skipping days younger than 3 hours; any failed day fails the run; 10 minute timeout |
| `surge_bars` | daily 17:30 America/New_York, plus once at startup | yes | daily bars for surge names and SPY; weekends and holidays find nothing due; 3 hour timeout |
| `base_rate_weekly` | Sunday 20:00 America/Chicago | no | runs `python -m rs_research event-study attention_surge` as a subprocess of the same image, with only `RS_RESEARCH_DB_URL` (the `rs_research` role) and logging settings of ingest's environment; a non-zero exit calls `notify_ops()`; 2 hour timeout |

- **Token gate:** a `SchwabAuthError` from any job trips a shared gate.
  While it is tripped, `needs_schwab` jobs are skipped, one ops notice is sent, and every other job keeps running.
  The popularity poll is not a Schwab job, but its facts block quotes Schwab, so it trips the gate itself on a dead quote and stops calling Schwab for the rest of that run.
  The gate clears when the stored token is newer than the failure (polled every 60 seconds) or when the `/setup schwab` handler reports a renewal; the Schwab client is rebuilt first, then an ops notice says jobs resumed.
  With no stored token at all the daemon starts gated instead of exiting, so a restart policy cannot crash-loop it.
- The ingest healthcheck reads the daemon's own `/metrics` on `127.0.0.1:9090`.

## Notifications and observability

- The bot DMs John for routine notices: Schwab token age at 24h and 6h before expiry; Schwab-fed alerts pause with one ops notice while the token is dead ([Vendor stack](https://github.com/JWolf34/RocketStocks/issues/48)).
- Health failures (ingest down, stream stalled, scan deadline missed, backup failed) go to a private ops channel through a Discord webhook each service can call without the bot.
- One `notify_ops()` function; the routes table and Apprise are cut.
  Notices are deduplicated by key inside a window, and without `RS_OPS_WEBHOOK_URL` they are logged only.
- Keep structlog JSON logs, correlation ids and excepthooks; native Prometheus metrics on localhost; container healthchecks and an ingest heartbeat.
- Logs and notices never carry secrets: the database engine is built with `hide_parameters=True` so a failed statement does not log bound values (token rows, OAuth codes), and `SchwabAuthError` carries only the OAuth error code, never authlib's free-text description.

## Alert payload

- `alerts.payload` holds a versioned JSON document that ingest writes once and the bot renders typed; an alert never changes after it is written.
- The model is `rs_domain.AttentionSurgePayload` (`version` 1, bumped on any breaking change to the shape): `rule_version`, `polled_at`, the sub-rules that `fired` (`mention_surge`, `rank_jump`), the inputs the rule saw (`rank`, `mentions`, the 24h-ago values, `rank_gain`, `mention_ratio`, the tier multiplier and minimum prior in force), and the `facts` block.
- Each fact is either its `ok` shape or `FactUnavailable` with a reason the reader sees; a fact that could not be gathered is never omitted silently.
  The facts are `price_move` (alert-time Schwab quote against the previous close), `volume` (quote volume against the mean of the 20 newest stored bars, `intraday` when the quote came mid-session), `recent_filings` (EDGAR 8-Ks), `next_earnings` (Nasdaq calendar, `estimated` when derived from the last results filing) and `benchmark` (the SPY quote at alert time).
- The alert row also carries `rule_version`, `alert_price`, `quote_time` and a unique `dedup_key` (rule, ticker, poll boundary).
  `triggered_at` is the poll boundary, so a replay produces the same alerts.
- A stored payload that no longer parses is logged and ignored for the cooldown rather than stalling later polls.

## Base rate

- `rs_research event-study attention_surge` writes two append-only `base_rate` rows per rule version; readers take the latest per (`alert_type`, `rule_version`, `input`).
  `reconstructed` replays the rule over every stored snapshot and enters each event at the close of the first session after its event date.
  `logged` measures the alerts actually posted.
- A logged outcome enters the ticker at `alert_price` and SPY at the alert's own `benchmark` quote, the same instant.
  An alert without a benchmark fact falls back to SPY's previous close, and the caveat text counts those alerts because the fallback subtracts SPY's move before the alert.
- Horizons of 1, 5 and 10 trading days are counted in exchange-calendar sessions, never in rows of SPY's stored bars, so a missing bar is a missing exit and does not shift a horizon.
- A horizon has matured when its session is on or before the last stored SPY bar, so a lagging bar job reads as not yet matured rather than missing.
  N counts events with an entry price, and each horizon's sample is the events that matured for it.
- Hit rate is the share of events with a strictly positive excess return over SPY; medians are of excess return as a fraction.
  Events whose ticker has no price are counted in `n_unpriced`, and matured horizons with no exit bar are counted in the caveat text.
- No row is written when nothing can be measured, so a stale row is never replaced by an empty one.
- The bot shows the `logged` row once its N reaches 30 (`BASE_RATE_LOGGED_MIN_N`), otherwise `reconstructed`.
  Early-close days are not modelled for the reference-close rule.

## Delivery

- The bot's sweep posts every `posted_at IS NULL` alert in id order, once and never edited; NOTIFY only wakes it early, and a 60 second timer bounds a lost wake.
- Delivery is at-least-once: a crash between send and commit can re-post one alert.
- A failed send increments `post_attempts`; the next try waits one sweep interval (held in memory, so a restart retries at once).
  At the cap (`RS_BOT_POST_MAX_ATTEMPTS`, default 5) the alert is abandoned and one ops notice says so.
  The failed-to-post state is `post_attempts` at or above the cap with `posted_at` NULL, with no extra column; an operator re-queues it by resetting `post_attempts` by hand.
- Rendering respects Discord field limits.

## Missed-alert catch-up

- Each popularity poll judges its own boundary and the one before it.
  A write that failed, or a run that was cut short, is therefore finished one boundary (30 minutes) later.
- The lookback judges only `live` snapshots, so imported history never produces a stale alert.
- A catch-up write that fails again is lost, and `run_on_start` covers the restart case within the same boundary only.

## Schwab re-authentication

- `/setup schwab` in Discord (guild owner or `RS_BOT_ADMIN_USER_ID`) creates a `schwab_auth_request` row and wakes ingest.
  Ingest builds the authorization URL and the bot shows it; the user logs in with Schwab and pastes the callback URL back; ingest exchanges the code, stores the token, clears the token gate and clears the stored callback URL, which carries the OAuth code.
- States are `requested`, `url_ready`, `callback_received`, `done`, `failed` and `expired`, each transition guarded by its source state.
  A request lives 10 minutes, and the OAuth context is held in ingest memory, so a request open across an ingest restart fails with a message to run the command again.
- The bot DMs `RS_BOT_ADMIN_USER_ID` (or the application owner when unset) when the token has 24h and again 6h left of its 7 days.
  Each threshold is sent once per token issue and recorded in `schwab_token_notice`; if the bot was down across the 24h mark, only the 6h notice is sent.

## Configuration

- `RS_EDGAR_USER_AGENT` is required by ingest: SEC EDGAR rejects requests without a descriptive User-Agent that names a contact (for example `RocketStocks you@example.com`).
- `RS_RESEARCH_DB_URL` is the DSN of the `rs_research` role.
  `rs_research` reads only this variable, never `RS_DB_URL`, so a misconfigured environment cannot make it write as another role; ingest passes it to the weekly subprocess.
- `.env.example` lists every variable; the [slice 1 runbook](./runbooks/slice-1.md) says which are required for the first deploy.

## Operations

- Deploy is docker compose on the home-lab VM: build, migrate gate, up, with base images and the Timescale image pinned by tag and digest; every long-running service has a healthcheck.
- The [slice 1 runbook](./runbooks/slice-1.md) holds the exact commands for first deploy, `/setup schwab`, the v1 import, the live run and the rehearsals.
- Backups are off-host VM block snapshots; John verifies the ADR-0031 conditions explicitly.
- Unattended trust needs three rehearsals: token expiry, ingest restart, restore ([Build process and cutover](https://github.com/JWolf34/RocketStocks/issues/53)).

## Database roles

- migrate owns the schema and is the only superuser path.
- ingest writes market data, tokens and alerts; bot writes user state, `posted_at` and collection requests and cannot read tokens.
- serve and research are read-only, except research's INSERT on the base-rates table; neither can read tokens.
- Roles `rs_ingest`, `rs_bot`, `rs_serve` and `rs_research` are created and granted by `rs_migrate` after every `upgrade` to head (or alone with `rs_migrate roles`), with passwords from `RS_DB_PASSWORD_<SERVICE>`; `upgrade` fails without them unless `--skip-roles` is passed.
- `GRANTS` in `tools/migrate/src/rs_migrate/roles.py` is the one table of decisions, and a test fails when a table has no entry.
- The bot's UPDATE on `alerts` is limited to `posted_at` and `post_attempts`, and on `schwab_auth_request` to `state` and `callback_url`; the callback URL carries an OAuth code, so only the bot and ingest can read that table.
- The bot reads Schwab token age only through the `schwab_token_issue` view (it holds no grant on `schwab_tokens`) and records sent expiry notices in `schwab_token_notice`.
- Operator CLIs that write beyond a service's grants (`rs_serve.tools.provision`, `rs_bot.tools.stage_alert`) run from the host with the migrate DSN in `.env`, not from inside the service container.

## Module layout

- **libs:** `rs_domain`, `rs_persistence` (repositories by aggregate), `rs_query`, `rs_observability`, `rs_sources` (Schwab, ApeWisdom, EDGAR and its ticker-to-CIK map, Nasdaq, calendars; used by ingest only), `rs_analysis` (versioned event rules shared by detectors and research).
- **services:** ingest, bot, serve.
- **tools:** migrate, backfill (the v1 popularity import only; Schwab history backfills run as ingest jobs so ingest stays the only Schwab caller), research.
