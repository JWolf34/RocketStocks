# S006 — Spine: compose, shared image, structlog/correlation_id, CI

**Phase**: 1 · **Appetite**: 4 days · **Depends on**: S003, S004, S005 (S007 for the gate re-run) · **ADRs / notes**: [0002](../adr/0002-three-services-topology.md), [0015](../adr/0015-observability-stack-personalized.md), [0012](../adr/0012-serving-api-auth-keys-plus-rate-limits.md), [roadmap Phase 1](../roadmap.md)

## Goal

After this slice, the whole v2 skeleton runs as one composable stack: `docker compose -f docker-compose.v2.yml up` brings up TimescaleDB, migrations, and all three services; every service emits one JSON log stream with `correlation_id`; ingest and bot are Prometheus-scrapeable; CI builds the v2 image and validates the compose file. This closes the Phase-1 exit criteria "all three services run under compose; CI green" (the roadmap's tests/lint/clock-grep CI items have existed since S001 — the CI leg reduces to the docker additions).

## Interface

- **`rs_observability`** (deps: `structlog`, `prometheus-client`) — names binding, internals not:
  - `configure_logging(service: str)` — installs structlog's `ProcessorFormatter` on the root stdlib handler: **module code keeps `logging.getLogger(__name__)`** (zero churn in accepted S003–S005 modules; new code MAY use `structlog.get_logger()` — same pipeline, two front doors). One JSON stream to **stdout** (ADR-0015); third-party stdlib records (uvicorn, discord.py, schwab-py, asyncio, alembic) render through the same chain, including contextvars merge. Every record carries `service`, `level`, UTC ISO `timestamp`, `logger`, `event`, and any bound unit-of-work fields. `RS_LOG_FORMAT` ∈ {`json`, `console`, `auto`} (default `auto`: TTY → pretty console, else JSON); `RS_LOG_LEVEL` (default `INFO`).
  - `unit_of_work(**fields)` — context manager: binds a **fresh opaque UUID as `correlation_id`** plus the given domain-key fields via structlog contextvars. correlation_id is never a domain value; domain keys are first-class fields; cross-service tracing joins on domain keys (`alert_id` appears in ingest's insert log and bot's post log) — no schema change. Binding points (binding):
    | Service | Unit of work | Bound fields |
    |---|---|---|
    | serve | one HTTP request (middleware; id echoed as `X-Correlation-ID` response header) | method, route |
    | ingest | one event dispatch through the pipeline (bar close → persist → detect → alert write → NOTIFY commit) | symbol, bucket |
    | bot | one alert posting attempt (discord.py retry internals group under the alert) | alert_id |
  - `install_excepthooks()` — the S005-assigned shared wiring: `sys.excepthook`, `threading.excepthook`, asyncio loop exception handler → `logger.exception(...)`. The `notifications.send(SYSTEM_CRITICAL, …)` half joins when ADR-0016 lands. Framework hooks already exist (serve catch-all — S005) or log via stdlib loggers captured by the stream (discord.py).
  - `DictCounterCollector(counters: Mapping[str, int], namespace: str)` — read-only bridge: existing counter dicts stay the increment API (`rs_bot.counters`, ingest pipeline/aggregator counters — no S003/S004 rework; the per-sweep/session-end counter log lines stay); the collector emits them at scrape time with proper Counter type metadata so `rate()`/`increase()` work.
  - `start_metrics_server(port: int, collectors: Iterable[Collector])` — prometheus_client threaded exposition + process/platform/GC defaults. Ingest `:9090` (`RS_INGEST_METRICS_PORT`), bot `:9091` (`RS_BOT_METRICS_PORT`); **serve is untouched** (S005's per-app registry stands; S004 pin #8's consolidation is satisfied by this spec being the one home for exposition decisions).
- **`Dockerfile.v2`** (repo root; the frozen v1 `Dockerfile`/compose/publish surface is untouched) — one shared image for all three services (ADR-0002), multi-stage uv build (`uv sync --locked --all-packages --no-install-package rocketstocks --no-dev --no-editable`), python:3.13-slim runtime, non-root user, **no default CMD** — compose supplies `command:` per service.
- **`docker-compose.v2.yml`** — project `name: rocketstocks-v2`, lifecycle fully independent of the v1 stack (burn-in coexistence): `timescaledb` (timescale/timescaledb:latest-pg16 — the CI/spec image; named volume; pg_isready healthcheck; published `127.0.0.1:5498`, taking over the manual acceptance container's role — prior data disposable), one-shot `migrate` (`python -m rs_migrate upgrade`) gating app services via `depends_on: service_completed_successfully`, `ingest` (`restart: on-failure:3`), `serve` + `bot` (`restart: unless-stopped` — S004's "the container restart policy is the supervisor"). serve: `RS_SERVE_HOST=0.0.0.0` in-container, published `127.0.0.1:8000` (TLS proxy is the public face — ADR-0012; resolves S005's host-binding pointer). Metrics side-ports published `127.0.0.1` only, **not scraped in-compose** (ADR-0015). `TZ=UTC` in v2 containers; in-network `RS_DB_URL` composed from `RS_DB_PASSWORD` (new, `.env.example` extended); host-side `RS_DB_URL` stays for local tools. At cutover this file renames to `docker-compose.yml`.
- **CI** — existing docker job gains: build `Dockerfile.v2` (push false, gha-cached) and `docker compose -f docker-compose.v2.yml config -q`.

## Non-goals

No DB roles/GRANTs (the S003-Q2 follow-on gets its own slice — condition met, three DB users exist, but orthogonal to the spine); no ingest multi-session daemon loop (Phase-2 warm-up slice, ADR-0018 — **pinned understanding: ingest becomes a long-running daemon handling RTH sessions + EOD pulls autonomously; the on-failure:3 policy is interim and flips to unless-stopped then**; operator starts the stack on market mornings meanwhile); no ghcr publish of the v2 image (burn-in prep, Phase 3→4 boundary); no notifications wiring in the hooks (ADR-0016); no structlog-native migration of existing modules; no new metric names beyond the existing counters + process defaults (vocabulary growth belongs to Phase-2 slices); no Grafana/scrape config (operator-side — ADR-0015); no serve code changes beyond logging/middleware wiring.

## Pinned decisions (pre-draft grilling, 2026-07-13)

1. **Scope** — fixed core (image, compose, structlog/correlation, excepthooks, CI docker additions) + metrics exposition (S004 pin #8's recommended home); GRANTs / ingest loop / publish deferred with named owners (Non-goals).
2. **Logging mode** — stdlib front door, structlog engine (ProcessorFormatter + foreign_pre_chain); rejected structlog-native migration (churn in accepted slices for no Phase-1 payoff).
3. **correlation_id** — fresh opaque UUID per unit of work + domain keys as fields; rejected domain-key-as-id (mixed semantics, merged retry stories) and DB-stamped column (rev-005 churn for a join two greps already give).
4. **Compose layout** — separate v2 file + own project name + compose-owned TimescaleDB on 5498 + one-shot migrate gate; rejected profiles-in-one-file (edits frozen v1 artifact, shared namespace) and DB-less compose (undercuts "runs under compose").
5. **Ingest restart** — `on-failure:3` (transient crash resumes bar-writing, bounded poison-failure noise, quiet after clean exit); see the Non-goals pin for the daemonization trajectory.
6. **Metrics mechanism** — read-only DictCounterCollector bridge; rejected prometheus-native counter rewrite (reopens accepted S003/S004 internals) and re-homing serve's registry (behavior-neutral churn).

## Acceptance criteria

- [ ] Fresh-volume `docker compose -f docker-compose.v2.yml up` on the dev box: migrate runs to head 0004 and exits 0 before any app service starts; serve answers an authenticated `/v1/bars` request over `127.0.0.1:8000`; bot connects and sweeps.
- [ ] Every service's compose logs are one-line JSON records carrying `service`, `level`, UTC `timestamp`; within a unit of work `correlation_id` is present — including on a third-party record (e.g. a discord.py or uvicorn line) emitted inside the bound context.
- [ ] serve returns `X-Correlation-ID` and that id appears on the request's log records (pytest + live curl).
- [ ] Planted unhandled exceptions on each hook path (sys / threading / asyncio-loop) each produce a JSON record with full traceback (pytest).
- [ ] `curl 127.0.0.1:9091/metrics` (bot, after a staged-alert post) and `:9090` (ingest, during a run) serve Prometheus text with the ADR-0015 counter names as `counter`-typed samples plus process defaults.
- [ ] CI green including the two new docker steps; v1 suite and v2 lint/clock-gate/tests untouched-green.
- [ ] Phase-1 gate re-run: S007 replay of the 2026-07-09 session against the compose-run stack reports IDENTICAL (12=12).
- [ ] One live market session ingested under compose (bars written, any alert posted by the compose bot) — the "full live market day under compose" exit evidence.

## Verification

```bash
.venv/bin/pytest libs/ services/ tools/ -q          # incl. new rs_observability tests
.venv/bin/ruff check libs services tools && scripts/check_clock_discipline.sh
docker build -f Dockerfile.v2 .                      # also in CI
docker compose -f docker-compose.v2.yml config -q    # also in CI
docker compose -f docker-compose.v2.yml up -d        # dev box; then:
docker compose -f docker-compose.v2.yml logs serve | head -5 | jq -e '.service and .timestamp'
curl -s -D- -H "X-API-Key: $KEY" 'http://127.0.0.1:8000/v1/bars/SPY?start=...&end=...' | grep -i x-correlation-id
curl -s http://127.0.0.1:9091/metrics | grep -E '^# TYPE (discord_posts_total|sweeps_total)' # counter
python -m rs_ingest.tools.replay --live-db ... --expected ...   # S007 gate re-run, exit 0
```

## Decisions surfaced

- **`unit_of_work` yields the correlation_id** — serve's middleware must echo it as `X-Correlation-ID`, so the context manager returns the fresh id to its caller.
- **`install_excepthooks()` has a call-twice contract** — the asyncio handler can only attach to a live loop, and `asyncio.run()` creates one per call; each service calls the hook once at process start (sys/threading) and once inside the running loop (idempotent). serve's second call lives in the FastAPI lifespan.
- **serve binds `route` as the concrete request path** (`/v1/bars/SPY`), not the route template: the cardinality constraint that forces templates onto metrics labels doesn't apply to log fields, and the raw path is what gets grepped. The template stays a metrics-only label.
- **serve's 500 path carries the id via `request.state`** — the catch-all handler runs in Starlette's outermost `ServerErrorMiddleware`, after the unit_of_work context has exited, so the middleware stashes the id there and the handler stamps it onto the traceback record and the 500's response header.
- **uvicorn's access log is off** (`access_log=False`): its line is emitted outside the bound context and would carry no id; `CorrelationMiddleware`'s one `request completed` record (method, route, status) is the access log.
- **`namespace=""` at all DictCounterCollector call sites** — ADR-0015 names the counter vocabulary bare (`discord_posts_total`, …) and the acceptance greps expect it; scrape job labels, not name prefixes, distinguish services. The parameter stays for future collectors whose names would otherwise collide. Sample names are normalized to the `_total` convention at exposition (`bars_persisted` → `bars_persisted_total`), so the untouched S003 dicts still satisfy `rate()`/`increase()`.
- **ingest's alert-insert log gained `alert_id`** (via `extra`) — pin #3's join key existed on the bot side only; the insert log now carries the id `AlertRepository.insert` returns.
- **`Dockerfile.v2` sets `CMD []` explicitly** — the `python:3.13-slim` base leaks `CMD ["python3"]` (a REPL), which would silently violate the no-default-CMD pin; empty CMD makes a bare `docker run` fail loudly instead.
- **`rs_observability.testing`** (`captured_json_stream`, `json_records`) is the sanctioned cross-member helper for asserting on rendered log records — correlation fields merge at format time, invisible to `caplog`/`capsys`.
- **`rs_migrate` calls `configure_logging("migrate")`** (code-review finding) — alembic is in the interface's third-party logger list and migrate is a compose service, so its records join the JSON stream as `service: migrate`; the one-shot CLI keeps no unit_of_work/metrics.
