# RocketStocks v2 — Architecture Brief

> The single entry point for the v2 architecture. Read this first. Per-decision rationale lives in [adr/](./adr/); this brief summarizes and links out.
>
> **Vocabulary**: *Stage 1* = the architecture-decision pass (closed; produced the ADR set). *Stage 2* = domain modeling, delivered **as code** in roadmap **Phase 0** ([S002](./specs/S002-domain-seed-and-persistence-rev-001.md)). "Stage" is design vocabulary; "Phase 0–5" is build vocabulary from [roadmap.md](./roadmap.md).
>
> **Status**: Stage 1 foundational decisions closed 2026-05-27; the data plane was re-designed and re-closed **2026-06-05** (new ADRs 0018–0027; supersedes 0006/0007/0009; amends 0005/0008/0011/0014). The execution strategy was locked **2026-06-11** (ADR-0028: all-in greenfield, linear build, terminal burn-in gate — see [roadmap.md](./roadmap.md)). The product thesis + detection scope closed **2026-06-12** (ADR-0029/0030: intraday→swing, options-flow-first; **one novel options-flow alert class — all six v1 alert types retired**). **Stage 2 (domain modeling) proceeds as code per roadmap Phase 0; v1 is in code freeze.**

---

## Why v2?

v1 (`src/rocketstocks/`, ~1007 tests) works, but an architectural review surfaced foundational *shape* limits that compose poorly with where the project is going. The real problems are structural:

- **Poll-only, no streaming path** — everything is poll-driven, so alerts lag the market; there is no low-latency ingestion rail for an options-flow-first product.
- **Append-only schema** — migrations only ever add; no principled migration story, and no separation of live-projection state from durable history.
- **No validation boundary** — external data enters unvalidated except for ad-hoc `_num`/`_bigint` coercion in one module.
- **Coupling** — detection runs inside the bot's `discord.ext.tasks` loops, binding "decide an alert" to "post to Discord," so detection can't run, scale, or be tested without the bot.

These are shape problems, not bugs to patch. v2 is a from-scratch rewrite that fixes the shape: clear service boundaries, a **streaming ingestion rail** with a hot/cold temporal split (ADR-0018), detection separated into `ingest` (ADR-0008/0021), principled migrations, and **validation at the edges**.

---

## Scope discipline

v2's north star is **platform parity with v1 (watchlists, paper trading, reports, screeners, admin) plus the Cowork consumer, plus ONE novel options-flow alert class — before any further net-new features.** Detection is re-derived for v2, not ported (ADR-0029/0030). Deliberate cuts/admissions:

- **All six v1 alert types are RETIRED** — popularity surge, momentum confirmation, volume accumulation, breakout, watchlist mover, earnings mover. v2's detection model is novel, designed just-in-time at Phase-2 start, on an extensible detector framework (ADR-0030). ApeWisdom/Nasdaq stay as universe/report/serve inputs, not alert sources.
- **Auto-trader is CUT from v2** — it is not v1 parity. It returns post-v2 as its own ground-up service; v2 only preserves the extraction seam (ADR-0027).
- **Manual paper trading is admitted** — v1 parity, on the `holdings`/`transactions`/`portfolio_snapshots` store with a `source` discriminator.
- **Backtest is rebuilt in v2** — on new data/logic models; the forcing function that proves the ingestion shape is right (point-in-time queryable, complete, immutable). Options backtest is forward-only (ADR-0020).
- **EDA is deferred** post-v2 — rebuilt on the new data shapes once v2 is stable.

Out of scope for v2 entirely: the six v1 alert types (ADR-0030), the auto-trader, live broker integration, multi-leg options positions, ML signals, a web UI, ApeWisdom history preservation, charting (ADR-0026), and options streaming (ADR-0019).

---

## Three services

v2 splits into three deployable services, each its own container, sharing one `Dockerfile` with a different `command:` per service in `docker-compose.yml`. See [ADR-0002](./adr/0002-three-services-topology.md).

```
                          ┌─────────────────────────────────────────┐
                          │                Postgres                  │
                          │   (TimescaleDB + outbox                   │
                          │    + LISTEN/NOTIFY channel)               │
                          └───────▲──────────────▲─────────────▲──────┘
                                  │              │             │
              writes alerts +     │              │             │  reads/writes
              NOTIFY              │              │             │  user state
                                  │              │             │
                                  │              │             │
Schwab / ApeWisdom / Nasdaq ──►┌──┴────┐   ┌─────┴────┐   ┌────┴────┐
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

- **`ingest`** — long-running daemon. Streams Schwab `level_one_equity` + polls ApeWisdom (popularity) and Nasdaq (earnings) + pulls REST options-chain snapshots, persists them, runs detection (bar-close + options scan), writes `alerts` rows and `NOTIFY`s the bot.
- **`serve`** — FastAPI read API. The contract surface for Cowork (external), backtest (internal CLI), and the bot (dogfoods the same API). Read-mostly over Postgres with a thin Schwab read-through for current data (ADR-0024).
- **`bot`** — discord.py presentation layer. LISTENs for alerts, renders embeds, posts; serves slash commands and manual paper trading. No detection logic and no auto-trader — both live elsewhere. All market data reaches it via `serve`; its direct DB access is only user/config state (sole writer) + alert rows (ADR-0024's access matrix).

Cowork.ai is a **real external consumer** at cutover (an LLM agent platform that pulls stock/industry data to build reports). It is the reason `serve` is a first-class, authenticated, versioned contract rather than an internal seam.

---

## Persistence

Postgres is the only stateful infrastructure — no Redis. See [ADR-0003](./adr/0003-postgres-only-timescale-cache.md).

- **TimescaleDB extension** for time-series tables (bars, options snapshots, popularity history). Hypertables created via raw DDL inside Alembic revisions.
- **UNLOGGED cache table — deferred out of v2** (ADR-0003 as amended 2026-07-03): after the serve read-model (ADR-0024) and bot data-access decisions, nothing reads it; reintroduced behind triggers (read-through cost bites / auto-trader arrives / outage-tolerant quote serving needed).
- **SQLAlchemy 2.x async** for DB models (`libs/persistence/models/`); **separate Pydantic models** for domain/API (`libs/domain/`). Repositories convert between them — internal computation fields never leak through the public API. See [ADR-0004](./adr/0004-persistence-layer-shape.md).
- **Alembic migrations** with autogeneration from SQLAlchemy models.
- **Backups are operator infrastructure**, decoupled from the repo: block-level VM snapshots (daily/weekly, off-host), restore-rehearsed at burn-in; RPO ≤24h accepted; no in-repo backup tooling in v2. See [ADR-0031](./adr/0031-backup-dr-vm-snapshots.md).

### Directory layout

The monorepo restructure (see [ADR-0001](./adr/0001-monorepo-restructure.md)) uses three top-level code roots whose names map one-to-one to a category:

| Dir | Holds | Run behavior |
|---|---|---|
| `libs/` | shared libraries — `domain`, `persistence`, `observability`, `analysis` (pure math), `sources` (Schwab/ApeWisdom/Nasdaq) | imported, never run alone |
| `services/` | `ingest`, `serve`, `bot` | long-running daemons, have compose entries |
| `tools/` | `migrate`, `backfill`, `backtest` | one-shot CLIs, invoked and exit |

Plus `legacy/` holding the old v1 `src/rocketstocks/` during the transition. (`libs/services/tools` was chosen over the JS-monorepo-style `packages/services/apps` because `services` and `apps` are near-synonyms; `libs/daemon/CLI` is unambiguous.)

Python packages follow one rule: **import name = `rs_` + member dir name** (`rs_domain`, `rs_analysis`, …) with src-layout and per-member packaging, so the layer rules are enforced by each member's dependency closure (ADR-0001 amendment; locked in [S001](./specs/S001-monorepo-restructure.md)).

---

## Event flow

Two buses, one abstraction. See [ADR-0005](./adr/0005-event-bus-shape.md).

- **Bus 1 — a deterministic, synchronous, *ordered fan-out* inside `ingest`** (not a pub/sub framework — required by the replay test strategy, ADR-0023). Per `BarClosed`, a fixed handler list runs in order: persister → detector. The options interval scan is a standalone linear pipeline, not a Bus-1 subscriber. See [ADR-0005](./adr/0005-event-bus-shape.md) / [ADR-0021](./adr/0021-intraday-persistence-and-bar-close-detection.md).
- **Bus 2 — Postgres LISTEN/NOTIFY for cross-service signaling.** `ingest` writes the `alerts` row, the `outbox` row, and issues `NOTIFY` inside **one transaction** (transactional NOTIFY delivers at commit — write and signal are atomic). `bot` LISTENs and posts; per the **Bus-2 consumer contract** (ADR-0005) it reconciles missed work via `SELECT ... WHERE posted_at IS NULL` on every reconnect *and* on a periodic sweep — NOTIFY is the latency optimization, the sweep is the correctness mechanism.
- **Transactional outbox** — an append-only durable event ledger and the attachment point for future Bus-2 consumers. **No relay ships in v2**; a producer-side relay arrives only with the external-broker migration (ADR-0005).
- An **EventBus abstraction** lets the implementation swap without touching subscribers — the documented upgrade path is LISTEN/NOTIFY → Redis Streams if/when dashboards or other real-time external consumers arrive.

---

## Schwab + options pipeline

**Equity L1 only; options are REST, not streamed.** No L2 order books, no `account_activity` (no live trading), no forex/futures, and **no `level_one_option` streaming** — the per-service stream budget makes it infeasible. See [ADR-0019](./adr/0019-two-band-universe-and-priority.md) (supersedes 0006).

**Two-band universe** (ADR-0019): an **equity band** — `level_one_equity` (+ `screener_equity` for discovery), ≤ ~500 underlyings, the whole tracked universe — and an **options band** of **REST chain snapshots only**. A `UniverseManager` recomputes the subscription set from DB-backed sources on a cadence (priority: open positions → alert subjects → watchlists → screener top-10 → top-100 popularity → discovery) and is fully reconstructible on warm-up.

**Three datasets**, three purposes (ADR-0020, supersedes 0007):

| Dataset | Cadence | Universe | Backfillable | Purpose |
|---|---|---|---|---|
| Equity OHLCV | daily + intraday bars | whole (≤500 streamed; backfill deep) | **Yes** | equity backtests + forward returns for event studies |
| Options 5-min curated | 5-min REST scan | curated (ATM±5, ≤30 DTE) | **No (forward-only)** | live alerts + intraday event studies |
| Options EOD full | nightly REST (off-hours) | whole ~5–6k | **No (forward-only)** | swing backtests + day-over-day OI; selection-bias-free |

Equity bars live in **two families with two authorities** (ADR-0021/0022): the stream-built 5-minute **detection log** (written bar-on-close, immutable, never reconciled — the only thing replay/warm-up read) and the official **research history** — whole-universe 1-minute OHLCV via a nightly EOD backfill, with 5-minute/daily as lossless rollups under Timescale compression (read by serve/backtest/Cowork). REST backfill seeds historical **equity** data at daily depth for backtest windows (1-min only within Schwab's ~7-day window; historical 5m deliberately dropped behind a trigger condition — ADR-0022); option chains are **not** backfillable.

---

## Detection + alerts

- **Detection lives in `ingest`**, behind an error boundary so a faulting `decide()` can't crash the capture pipeline. It is **hybrid by data type**: equity = **bar-close** evaluation on `BarClosed` (ADR-0021); options = interval REST scan. A detector decides, writes an `alerts` row (+ outbox) in a transaction, and `NOTIFY`s. Detection is held **extractable** post-v2 (consumes only via the event-source port; only writes `alert`+`outbox`). See [ADR-0008](./adr/0008-detection-in-ingest.md) / [ADR-0021](./adr/0021-intraday-persistence-and-bar-close-detection.md).
- **One alert class ships in v2 — the options-flow alert** (ADR-0030). The detection model is novel, designed just-in-time at Phase-2 start; v1's six alert types are retired. Every alert row carries a **detector-config-version stamp**, and curated-snapshot presence doubles as the scan-membership record (the PIT lean mandate). The framework is extensible — detection port, generic alert taxonomy, per-detector config, pluggable rendering — so future detection models are additive.
- **`bot` is a thin presentation layer** — it LISTENs, renders the embed, posts. No detection logic in the bot. The `alerts` table is the contract; its sole v2 consumer is the `bot` (the auto-trader is cut — ADR-0027).
- Detection runs against a durable, deterministic event log that is also the warm-up and backtest input — the basis of the replay test strategy ([ADR-0023](./adr/0023-test-strategy-deterministic-replay.md)).

---

## Paper trading

**Manual paper trading only in v2** — the auto-trader is cut and returns post-v2 as its own service (ADR-0027, supersedes 0009). Trades ride the `holdings`/`transactions`/`portfolio_snapshots` store with a `source` column (`manual` now; `auto_trader` reserved for the post-v2 service), so attribution stays clean when it returns.

- Entered via the alert's `💵 Trade` button (a manual paper BUY of the underlying into the clicker's portfolio) and `/trade buy|sell <ticker> <qty> [limit]`.
- Open positions are streamed (universe priority #1, ADR-0019) for live portfolio marks (served via `serve` read-through until the UNLOGGED cache returns — ADR-0003/0024).
- The post-v2 auto-trader blueprint — pure library behind data-access ports, its own Bus-2 alert subscription, direct cache reads + trade writes, bot demoted to a serve-backed performance UI — is preserved in [ADR-0027](./adr/0027-manual-paper-trading-only.md) / the superseded ADR-0009.

---

## Auth + identity

- **Serving API auth**: per-consumer API keys + per-consumer rate limits. A `service_clients` table (`api_key_hash`, `scopes` JSONB, `rate_limit_rps`, `rate_limit_burst`, `revoked_at`) + middleware. Rate limits from day one because Cowork's agent fan-out can burst. OAuth migration is documented with trigger conditions, not built now. See [ADR-0012](./adr/0012-serving-api-auth-keys-plus-rate-limits.md).
- **Identity**: Discord-ID-only. User-keyed tables FK to `discord_user_id BIGINT`; no `users` table. A retrofit playbook + trigger conditions (web UI, non-Discord identity provider, third user-bearing surface) are documented. See [ADR-0013](./adr/0013-identity-discord-id-only.md).

---

## Discord surface

Slash commands + interactive components (buttons, selects, modals). No `!` prefix. **Mobile-first** layout — embed widths, buttons-per-row, and field counts are treated as mobile constraints. **v1 designs do not anchor v2 designs.** See [ADR-0014](./adr/0014-bot-interface-slash-plus-components.md).

Two command umbrellas (both respect Discord's 3-level nesting limit):

- **`/trade`** — manual paper trading. `buy`/`sell` subcommands. (The `bot` auto-trader subcommand group is removed — ADR-0027.)
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

Per-dataset policy, freshness at the boundaries — industry "Pattern 2" (service-mesh / SRE style), not event-time watermarks. See [ADR-0017](./adr/0017-failure-mode-trimmed-a.md).

- Each dataset (source × data-class) declares a `FreshnessPolicy` — `STRICT` (raise), `STALE_OK` (serve last-known + flag), `BEST_EFFORT` (skip cycle, retry) — plus a `max_age` budget saying when "stale" trips (finalized at burn-in; stored in a Postgres config table).
- Freshness lives in **persistence rows** (`fetched_at`, `source_status`; failed per-ticker fetches write tombstone rows), the **API response envelope** (`meta.as_of` + `meta.fetched_at`; `meta.stale_since` only when degraded), and **Prometheus** (`source_last_success_seconds`).
- **Bus events stay clean** — no freshness propagation, because 90% of subscribers don't read it.
- **Mutations fail-loud** — silent write failure is the worst class of bug.
- Circuit breakers deferred behind documented trigger conditions.

---

## Out of scope for v2

The **six v1 alert types** (retired — ADR-0030), the **auto-trader** (post-v2, its own service — ADR-0027), EDA (deferred post-v2), options **streaming**, **extended-hours market data** (RTH-only — ADR-0021), live broker integration, multi-leg options positions, ML-based signals, **charting** (ADR-0026), web UI, ApeWisdom history preservation. Clean-slate data at cutover — users recreate watchlists; popularity-surge detection accepts a cold-start window. See [ADR-0010](./adr/0010-clean-slate-cutover-data.md) and [ADR-0011](./adr/0011-backtest-in-v2-eda-deferred.md).

---

## Migration-path notes (documented, not built)

| Trigger | Migration |
|---|---|
| Dashboards / other real-time external consumers arrive | LISTEN/NOTIFY → Redis Streams ([ADR-0005](./adr/0005-event-bus-shape.md)) |
| 4th external consumer / web UI delegated auth / leak-window concern | API keys → OAuth 2.0 ([ADR-0012](./adr/0012-serving-api-auth-keys-plus-rate-limits.md)) |
| A source becomes chronically flaky (>N% failure over 24h for K days) | add per-source circuit breakers ([ADR-0017](./adr/0017-failure-mode-trimmed-a.md)) |
| Web UI / non-Discord identity provider committed | retrofit `users` table ([ADR-0013](./adr/0013-identity-discord-id-only.md)) |
| Web UI / second user-facing write client | user-state writes move behind `serve` as a write API ([ADR-0024](./adr/0024-serve-read-model-boundary.md)) |
| Post-v2: build the auto-trader | stand it up as its own service (Bus-2 alert subscription, data-access ports) ([ADR-0027](./adr/0027-manual-paper-trading-only.md)) |
| A signal needs sub-minute latency | 1-min bars (config flip) or a tagged intrabar fast-path ([ADR-0021](./adr/0021-intraday-persistence-and-bar-close-detection.md)) |
| Notification storm becomes painful | add ErrorAggregator + `error_history` ([ADR-0015](./adr/0015-observability-stack-personalized.md)) |
| A restore in anger hurts at 24h RPO / a consumer depends on snapshot completeness | in-repo WAL archiving (pgBackRest) — post-v2 ([ADR-0031](./adr/0031-backup-dr-vm-snapshots.md)) |
| Portfolio/watchlist read-through cost or latency bites, the auto-trader build starts, or outage-tolerant quote serving is needed | reintroduce the UNLOGGED cache ([ADR-0003](./adr/0003-postgres-only-timescale-cache.md)) |

---

## ADR index

| ADR | Title |
|---|---|
| [0001](./adr/0001-monorepo-restructure.md) | Monorepo restructure (`libs`/`services`/`tools`/`legacy`) + `feat/v2-rewrite` branch |
| [0002](./adr/0002-three-services-topology.md) | Three-services topology (`ingest`/`serve`/`bot`) — *amended: auto-trader refs updated (0027)* |
| [0003](./adr/0003-postgres-only-timescale-cache.md) | Postgres-only + TimescaleDB + UNLOGGED cache — *amended: cache deferred out of v2 (2026-07-03)* |
| [0004](./adr/0004-persistence-layer-shape.md) | SQLAlchemy 2.x async + separate Pydantic + Alembic |
| [0005](./adr/0005-event-bus-shape.md) | Event bus shape — *amended: deterministic fan-out (0021); no relay + consumer contract (2026-07-03)* |
| [0006](./adr/0006-schwab-l1-only-streaming-scope.md) | Schwab L1-only streaming scope — **superseded by 0019** |
| [0007](./adr/0007-options-data-pipeline-three-tiers.md) | Three-layer options data pipeline — **superseded by 0020** |
| [0008](./adr/0008-detection-in-ingest.md) | Detection lives in `ingest`; `bot` is thin — *amended (0021)* |
| [0009](./adr/0009-paper-trading-and-auto-trader-scope.md) | Paper trading + auto-trader scope — **superseded by 0027** |
| [0010](./adr/0010-clean-slate-cutover-data.md) | Clean-slate data at cutover |
| [0011](./adr/0011-backtest-in-v2-eda-deferred.md) | Backtest in v2; EDA deferred — *amended (forward-only options)* |
| [0012](./adr/0012-serving-api-auth-keys-plus-rate-limits.md) | Serving API auth — API keys + rate limits |
| [0013](./adr/0013-identity-discord-id-only.md) | Identity — Discord-ID-only |
| [0014](./adr/0014-bot-interface-slash-plus-components.md) | Bot interface — slash + components — *amended (no `/trade bot`)* |
| [0015](./adr/0015-observability-stack-personalized.md) | Observability stack (logs + Prom + notifications + native capture) |
| [0016](./adr/0016-notifications-layer-db-routing.md) | Notifications layer + DB-table routing |
| [0017](./adr/0017-failure-mode-trimmed-a.md) | Failure mode — per-dataset policy + boundary freshness — *amended (freshness budgets, tombstones)* |
| [0018](./adr/0018-temporal-hot-cold-model-and-warmup.md) | Temporal hot/cold model + startup warm-up |
| [0019](./adr/0019-two-band-universe-and-priority.md) | Two-band universe + priority/eviction (supersedes 0006) |
| [0020](./adr/0020-three-datasets-and-backtest-constraints.md) | Three datasets + forward-only options backtest (supersedes 0007) — *amended (seeding policy; budget-derived curated size)* |
| [0021](./adr/0021-intraday-persistence-and-bar-close-detection.md) | Intraday persistence + bar-close detection — *amended (detection log vs. research history)* |
| [0022](./adr/0022-bar-retention-storage-compression.md) | Bar retention — whole-universe 1-min + compression — *amended (two families, seeding policy, 7-day bound)* |
| [0023](./adr/0023-test-strategy-deterministic-replay.md) | Test strategy — deterministic replay |
| [0024](./adr/0024-serve-read-model-boundary.md) | Serve read-model boundary (DB-read vs Schwab read-through) — *amended (Cowork profile + limit enforcement)* |
| [0025](./adr/0025-live-source-set-schwab-apewisdom-nasdaq.md) | Live source set — Schwab + ApeWisdom + Nasdaq — *amended (corporate actions + symbol master)* |
| [0026](./adr/0026-charting-deprecation-and-reports.md) | Charting deprecated; alert-presentation principles + report surfaces — *amended (field layout → presentation spec)* |
| [0027](./adr/0027-manual-paper-trading-only.md) | Manual paper trading only; auto-trader deferred (supersedes 0009) |
| [0028](./adr/0028-execution-strategy-greenfield-terminal-burn-in.md) | Execution strategy — all-in greenfield + terminal burn-in gate (amends delivery posture of 0001/0010) |
| [0029](./adr/0029-product-thesis-trade-horizon.md) | Product thesis — intraday detection of swing-relevant events; options-flow-first |
| [0030](./adr/0030-detection-rederived-single-alert-class.md) | Detection re-derived — one alert class, extensible framework, PIT lean mandate (amends the parity north star) |
| [0031](./adr/0031-backup-dr-vm-snapshots.md) | Backup & DR — operator-level VM snapshots, restore-rehearsed; in-repo tooling post-v2 |
