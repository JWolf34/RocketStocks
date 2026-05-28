# RocketStocks v2 — Architecture Brief

> The single entry point for the v2 architecture. Read this first. Per-decision rationale lives in [adr/](./adr/); this brief summarizes and links out.
>
> **Status**: Stage 1 (foundational decisions) closed 2026-05-27. Stage 2 (domain modeling) is next.

---

## Why v2?

v1 (`src/rocketstocks/`, ~1007 tests) works, but an architectural review of its data layer surfaced foundational limits that compose poorly with where the project is going:

- **God-object `StockData`** — a single singleton accreted every data concern, making boundaries and testing hard.
- **Event-loop blocking** — synchronous API clients block the asyncio loop, capping concurrency and latency.
- **Polling latency** — everything is poll-driven; there is no streaming path, so alerts lag the market.
- **Append-only migrations** — schema changes only ever added; no principled migration story.
- **No validation boundary** — external data enters the system unvalidated except for ad-hoc `_num`/`_bigint` coercion in one module.

These are not bugs to patch; they are shape problems. v2 is a from-scratch rewrite that fixes the shape: clear service boundaries, async-first I/O, a streaming ingestion path, principled migrations, and validation at the edges.

---

## Scope discipline

v2's north star is **functional parity with v1 plus the Cowork consumer, before any net-new features.** Three deliberate exceptions/cuts:

- **Auto-trader (foundation) is admitted** — it's a foundation, not a feature. Equity-only, rule-based, library-shaped. The engine (sizing, regime gates, ML) is deferred.
- **Manual paper trading is admitted** — small addition riding on the same `paper_trades` store as the auto-trader.
- **Backtest is rebuilt in v2** — on new data/logic models. It's the forcing function that proves the ingestion shape is right (point-in-time queryable, complete, immutable).
- **EDA is deferred** post-v2 — rebuilt on the new data shapes once v2 is stable.

Out of scope for v2 entirely: live broker integration, multi-leg options positions, ML signals, a web UI, ApeWisdom history preservation.

---

## Three services

v2 splits into three deployable services, each its own container, sharing one `Dockerfile` with a different `command:` per service in `docker-compose.yml`. See [ADR-0002](./adr/0002-three-services-topology.md).

```
                          ┌─────────────────────────────────────────┐
                          │                Postgres                  │
                          │   (TimescaleDB + UNLOGGED cache table     │
                          │    + outbox + LISTEN/NOTIFY channel)      │
                          └───────▲──────────────▲─────────────▲──────┘
                                  │              │             │
              writes alerts +     │              │             │  reads/writes
              NOTIFY              │              │             │  user state
                                  │              │             │
   Schwab / Tiingo / ApeWisdom    │              │             │
   SEC / News / TradingView ──►┌──┴────┐   ┌─────┴────┐   ┌────┴────┐
                               │ ingest │   │  serve   │   │   bot   │
                               │(daemon)│   │ (FastAPI)│   │(discord)│
                               └────────┘   └────▲─────┘   └────▲────┘
                                                 │              │
                                    ┌────────────┴───┐     ┌────┴───────┐
                                    │  Cowork (ext.)  │     │  Discord   │
                                    │  backtest (CLI) │     │   users    │
                                    │  bot (dogfood)  │     └────────────┘
                                    └────────────────┘
```

- **`ingest`** — long-running daemon. Streams (Schwab L1) + polls (ApeWisdom/SEC/news) all sources, persists them, runs detection, writes `alerts` rows and `NOTIFY`s the bot.
- **`serve`** — FastAPI read API. The contract surface for Cowork (external), backtest (internal CLI), and the bot (dogfoods the same API).
- **`bot`** — discord.py presentation layer. LISTENs for alerts, renders embeds, posts; runs the auto-trader library; serves slash commands.

Cowork.ai is a **real external consumer** at cutover (an LLM agent platform that pulls stock/industry data to build reports). It is the reason `serve` is a first-class, authenticated, versioned contract rather than an internal seam.

---

## Persistence

Postgres is the only stateful infrastructure — no Redis. See [ADR-0003](./adr/0003-postgres-only-timescale-cache.md).

- **TimescaleDB extension** for time-series tables (bars, options snapshots, popularity history). Hypertables created via raw DDL inside Alembic revisions.
- **UNLOGGED cache table** for hot reads (quote / movers / popularity / fundamentals) — fast, crash-disposable.
- **SQLAlchemy 2.x async** for DB models (`libs/persistence/models/`); **separate Pydantic models** for domain/API (`libs/domain/`). Repositories convert between them — internal computation fields never leak through the public API. See [ADR-0004](./adr/0004-persistence-layer-shape.md).
- **Alembic migrations** with autogeneration from SQLAlchemy models.

### Directory layout

The monorepo restructure (see [ADR-0001](./adr/0001-monorepo-restructure.md)) uses three top-level code roots whose names map one-to-one to a category:

| Dir | Holds | Run behavior |
|---|---|---|
| `libs/` | shared libraries — `domain`, `persistence`, `observability`, `core` analysis, `auto_trader`, source adapters | imported, never run alone |
| `services/` | `ingest`, `serve`, `bot` | long-running daemons, have compose entries |
| `tools/` | `migrate`, `backfill`, `backtest` | one-shot CLIs, invoked and exit |

Plus `legacy/` holding the old v1 `src/rocketstocks/` during the transition. (`libs/services/tools` was chosen over the JS-monorepo-style `packages/services/apps` because `services` and `apps` are near-synonyms; `libs/daemon/CLI` is unambiguous.)

---

## Event flow

Two buses, one abstraction. See [ADR-0005](./adr/0005-event-bus-shape.md).

- **Bus 1 — in-process pubsub inside `ingest`.** Carries source events (`QuoteTick`, `MinuteBar`, `MentionsIngested`, `FilingIngested`, …) and derived events (`SurgeDetected`, `MomentumConfirmed`, …). Subscribers: persister, cache_updater, detectors, alert_writer.
- **Bus 2 — Postgres LISTEN/NOTIFY for cross-service signaling.** `ingest` writes an `alerts` row inside a transaction, then `NOTIFY`s. `bot` LISTENs and posts. On every reconnect the bot reconciles missed work via `SELECT ... WHERE posted_at IS NULL`.
- **Transactional outbox** for durable event records, so a crash between "write alert" and "NOTIFY" is recoverable.
- An **EventBus abstraction** lets the implementation swap without touching subscribers — the documented upgrade path is LISTEN/NOTIFY → Redis Streams if/when dashboards or other real-time external consumers arrive.

---

## Schwab + options pipeline

**L1 only.** No L2 order books (`nasdaq_book`/`nyse_book`/`options_book`), no `account_activity` (live trading deferred), no forex/futures. See [ADR-0006](./adr/0006-schwab-l1-only-streaming-scope.md).

In scope: `level_one_equity` + `chart_equity` + `screener_equity` (full equity surface) and `level_one_option` for a curated chains universe (ATM ± N strikes, near-dated; underlying universe = watchlist + popular + active alert subjects + paper-trade positions). An **OptionsSubscriptionManager** owns add/unsub decisions and reconciles on a cadence.

The options data pipeline is three layers — see [ADR-0007](./adr/0007-options-data-pipeline-three-tiers.md):

| Layer | What | Cadence | Universe | Purpose |
|---|---|---|---|---|
| **Tier 1** | in-memory current state | streaming, sub-second | curated | real-time alerts + auto-trader |
| **Tier 2** | intraday snapshots → `options_chain_history` hypertable | 5-min market / hourly off | curated | intraday IV/volume-build signals |
| **EOD full** | REST pull of all optionable underlyings | nightly | full market | market-wide OI/volume flow over time |

Off-hours fallback: REST polling for ApeWisdom/SEC/news + REST chain snapshots. REST backfill seeds historical data for backtest windows.

---

## Detection + alerts

- **Detection lives in `ingest`** as Bus 1 subscribers. A detector consumes events, decides, writes an `alerts` row, and `NOTIFY`s. See [ADR-0008](./adr/0008-detection-in-ingest.md).
- **`bot` is a thin presentation layer** — it LISTENs, renders the embed, posts. No detection logic in the bot. The `alerts` table is the contract between the detection writer and its consumers (bot + auto-trader).

---

## Paper trading + auto-trader

Paper trading is **two separate tracks**, distinguished by `paper_trades.source` (`manual` / `auto_trader`), with positions/P&L/stats computed per-track for clean attribution. See [ADR-0009](./adr/0009-paper-trading-and-auto-trader-scope.md).

- **Auto-trader** is a pure library in `libs/auto_trader/`: `decide_entry` / `decide_exit`, equity-only, rule-based (fixed sizing, fixed stops, optional regime gates). The `bot` runs it on new-alert LISTEN/NOTIFY, writes `paper_trades` (`source=auto_trader`), and polls the cache every 5–30s for position management. Engine (sized positions, regime conditioning, A/B, ML) is deferred post-v2.
- **Manual paper trading** rides the same store via slash commands (`source=manual`).

Command surface (see Discord section): `/trade buy|sell <ticker> <qty> [limit]` (manual) and `/trade bot performance|positions|history|trade <id>|config` (auto-trader views + policy).

---

## Auth + identity

- **Serving API auth**: per-consumer API keys + per-consumer rate limits. A `service_clients` table (`api_key_hash`, `scopes` JSONB, `rate_limit_rps`, `rate_limit_burst`, `revoked_at`) + middleware. Rate limits from day one because Cowork's agent fan-out can burst. OAuth migration is documented with trigger conditions, not built now. See [ADR-0012](./adr/0012-serving-api-auth-keys-plus-rate-limits.md).
- **Identity**: Discord-ID-only. User-keyed tables FK to `discord_user_id BIGINT`; no `users` table. A retrofit playbook + trigger conditions (web UI, non-Discord identity provider, third user-bearing surface) are documented. See [ADR-0013](./adr/0013-identity-discord-id-only.md).

---

## Discord surface

Slash commands + interactive components (buttons, selects, modals). No `!` prefix. **Mobile-first** layout — embed widths, buttons-per-row, and field counts are treated as mobile constraints. **v1 designs do not anchor v2 designs.** See [ADR-0014](./adr/0014-bot-interface-slash-plus-components.md).

Two command umbrellas (both respect Discord's 3-level nesting limit):

- **`/trade`** — paper trading. `buy`/`sell` direct subcommands + a `bot` subcommand group for auto-trader views/config.
- **`/setup`** — all admin config. `notifications`, `channels`, `roles` subcommand groups + `schwab` (re-auth) subcommand.

---

## Observability

A four-layer stack, native-only (no external SaaS). See [ADR-0015](./adr/0015-observability-stack-personalized.md).

1. **Structured logs** — `structlog` JSON, `correlation_id` propagated through every record.
2. **Prometheus `/metrics`** — exposed per service (`serve` under FastAPI behind a metrics-scoped key; `ingest` `:9090`; `bot` `:9091`) but **not scraped in compose** — the operator scrapes externally. A Grafana dashboard JSON template is deferred post-v2. Metrics cover health, throughput, lag, status, and "fun" counters (`tickers_tracked_total`, `paper_trades_open_total`).
3. **Notifications layer** — pluggable backends (Discord, ntfy, Apprise), category-routed (`alerts.market`, `alerts.user`, `system.critical`, `system.warning`, `system.info`). Routing in a `notification_routes` DB table, edited via `/setup notifications`, defaults seeded by migration. Category routing is what keeps `system.critical` (e.g., Schwab token expiring → ntfy push) off the noisy alerts channel. See [ADR-0016](./adr/0016-notifications-layer-db-routing.md).
4. **Native exception capture** — `sys.excepthook` + `threading.excepthook` + the asyncio loop exception handler + framework hooks (FastAPI `@app.exception_handler`, discord.py `on_error`/`on_command_error`, APScheduler `EVENT_JOB_ERROR`). Each hook logs via `logger.exception` and fires a `SYSTEM_CRITICAL` notification. No aggregator, no `error_history` table — explicitly judged over-engineering for this scale; addable later as a one-line change.

---

## Failure mode

Per-source policy, freshness at the boundaries — industry "Pattern 2" (service-mesh / SRE style), not event-time watermarks. See [ADR-0017](./adr/0017-failure-mode-trimmed-a.md).

- Each source adapter declares a `FreshnessPolicy`: `STRICT` (raise), `STALE_OK` (serve last-known + flag), `BEST_EFFORT` (skip cycle, retry).
- Freshness lives in **persistence rows** (`fetched_at`, `source_status`), the **cache** (`stale_since`), the **API response envelope** (`meta.fetched_at` / `meta.stale_since`), and **Prometheus** (`source_last_success_seconds`).
- **Bus events stay clean** — no freshness propagation, because 90% of subscribers don't read it.
- **Mutations fail-loud** — silent write failure is the worst class of bug.
- Circuit breakers deferred behind documented trigger conditions.

---

## Out of scope for v2

EDA (deferred post-v2), live broker integration, multi-leg options positions, ML-based signals, web UI, ApeWisdom history preservation, auto-trader engine (sizing/regime/A-B/ML). Clean-slate data at cutover — users recreate watchlists; popularity-surge detection accepts a cold-start window. See [ADR-0010](./adr/0010-clean-slate-cutover-data.md) and [ADR-0011](./adr/0011-backtest-in-v2-eda-deferred.md).

---

## Migration-path notes (documented, not built)

| Trigger | Migration |
|---|---|
| Dashboards / other real-time external consumers arrive | LISTEN/NOTIFY → Redis Streams ([ADR-0005](./adr/0005-event-bus-shape.md)) |
| 4th external consumer / web UI delegated auth / leak-window concern | API keys → OAuth 2.0 ([ADR-0012](./adr/0012-serving-api-auth-keys-plus-rate-limits.md)) |
| A source becomes chronically flaky (>N% failure over 24h for K days) | add per-source circuit breakers ([ADR-0017](./adr/0017-failure-mode-trimmed-a.md)) |
| Web UI / non-Discord identity provider committed | retrofit `users` table ([ADR-0013](./adr/0013-identity-discord-id-only.md)) |
| Live trading arrives | split auto-trader into its own service ([ADR-0009](./adr/0009-paper-trading-and-auto-trader-scope.md)) |
| Notification storm becomes painful | add ErrorAggregator + `error_history` ([ADR-0015](./adr/0015-observability-stack-personalized.md)) |

---

## ADR index

| ADR | Title |
|---|---|
| [0001](./adr/0001-monorepo-restructure.md) | Monorepo restructure (`libs`/`services`/`tools`/`legacy`) + `feat/v2-rewrite` branch |
| [0002](./adr/0002-three-services-topology.md) | Three-services topology (`ingest`/`serve`/`bot`) |
| [0003](./adr/0003-postgres-only-timescale-cache.md) | Postgres-only + TimescaleDB + UNLOGGED cache |
| [0004](./adr/0004-persistence-layer-shape.md) | SQLAlchemy 2.x async + separate Pydantic + Alembic |
| [0005](./adr/0005-event-bus-shape.md) | Event bus shape (Bus 1 in-process + Bus 2 LISTEN/NOTIFY + outbox) |
| [0006](./adr/0006-schwab-l1-only-streaming-scope.md) | Schwab L1-only streaming scope |
| [0007](./adr/0007-options-data-pipeline-three-tiers.md) | Three-layer options data pipeline |
| [0008](./adr/0008-detection-in-ingest.md) | Detection lives in `ingest`; `bot` is thin |
| [0009](./adr/0009-paper-trading-and-auto-trader-scope.md) | Paper trading (two tracks) + auto-trader scope |
| [0010](./adr/0010-clean-slate-cutover-data.md) | Clean-slate data at cutover |
| [0011](./adr/0011-backtest-in-v2-eda-deferred.md) | Backtest in v2; EDA deferred |
| [0012](./adr/0012-serving-api-auth-keys-plus-rate-limits.md) | Serving API auth — API keys + rate limits |
| [0013](./adr/0013-identity-discord-id-only.md) | Identity — Discord-ID-only |
| [0014](./adr/0014-bot-interface-slash-plus-components.md) | Bot interface — slash + components, mobile-first |
| [0015](./adr/0015-observability-stack-personalized.md) | Observability stack (logs + Prom + notifications + native capture) |
| [0016](./adr/0016-notifications-layer-db-routing.md) | Notifications layer + DB-table routing |
| [0017](./adr/0017-failure-mode-trimmed-a.md) | Failure mode — per-source policy + boundary freshness |
