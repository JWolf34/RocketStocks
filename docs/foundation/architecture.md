# RocketStocks v2 architecture

The one-page runtime shape of v2.
Each decision's rationale lives in the linked ticket on the [v2 foundation map](https://github.com/JWolf34/RocketStocks/issues/40); this page is the current truth and is edited in place as slices land.
Surviving ADRs live in `docs/v2/adr/`; an archived ADR number cited in code resolves at git tag `v2-plan-archive-2026-10`.

## Processes

One TimescaleDB, a one-shot migrate gate, and three services from one shared image ([Architecture one-pager](https://github.com/JWolf34/RocketStocks/issues/52)).

- **ingest** brings data in: sources -> validate -> stamp -> database.
  It is the only Schwab caller and the only token holder.
  It runs the stream, the pollers, the in-process cron table, the detectors and the alert writer, as a multi-session daemon that survives weekends and token expiry.
- **bot** owns Discord: posting alerts and digests, Reports, watchlists, paper trading, channel config.
  It never calls a vendor and never runs detection.
- **serve** is the LAN-only Data API for John's own tools: REST under `/v1` plus a hand-curated MCP server (5-10 tools) at `/mcp`, plain HTTP, one API key per client ([Data API shape](https://github.com/JWolf34/RocketStocks/issues/44)).
  It is read-only.

## Data flow

- **Streamed:** Schwab level-one quotes for the ~500-name stream band (rule-based liquid band plus every watchlist ticker), aggregated to 1-minute bars and persisted going forward ([Data collection scope](https://github.com/JWolf34/RocketStocks/issues/49)).
- **Nightly:** symbol master, daily bars for every active listing, then the options EOD full scan of the optionable universe (22:00 CT launch, 03:00 CT hard deadline; the scan's roster, retry/split ladder and tombstone decisions are on the superseded [S009 options EOD full scan map](https://github.com/JWolf34/RocketStocks/issues/6), restated by slice 2's spec).
- **Frequent polls:** ApeWisdom, the Nasdaq earnings calendar, SEC EDGAR 8-Ks for names in scope.
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

- ingest's in-process cron table owns every Schwab-touching job against one rate budget: symbol master, daily bars, options EOD scan, popularity, calendar and EDGAR polls.
- The weekly base-rate rerun runs `rs_research` as a subprocess of the same image.
  The `base_rate_weekly` job fires Sunday 20:00 America/Chicago and runs `python -m rs_research event-study attention_surge` with only `RS_RESEARCH_DB_URL` (the `rs_research` role) of ingest's environment.
  A non-zero exit calls `notify_ops()`.
- The bot schedules its own digests (daily post-close, Monday earnings preview), which only read the database.

## Notifications and observability

- The bot DMs John for routine notices: Schwab token age at 24h and 6h before expiry; Schwab-fed alerts pause with one ops notice while the token is dead ([Vendor stack](https://github.com/JWolf34/RocketStocks/issues/48)).
- Health failures (ingest down, stream stalled, scan deadline missed, backup failed) go to a private ops channel through a Discord webhook each service can call without the bot.
- One `notify_ops()` function; the routes table and Apprise are cut.
- Keep structlog JSON logs, correlation ids and excepthooks; native Prometheus metrics on localhost; container healthchecks and an ingest heartbeat.

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
