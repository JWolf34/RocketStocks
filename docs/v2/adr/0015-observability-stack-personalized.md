# ADR-0015: Observability stack — logs + Prometheus + notifications + native exception capture

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

Three services + real-time pipelines + an external API consumer need observability that answers two distinct questions: "what's broken?" (errors) and "what's degrading?" (rates/latencies). The default reach (Prometheus + Grafana in compose + Sentry/GlitchTip for errors) adds containers and an external SDK. The user wants a personalized, native-leaning stack: Prometheus exposed but not scraped in-compose, a flexible multi-backend notifications layer, and **no external error-tracking dependency** (Sentry explicitly rejected). v1's exception handling is spotty (mostly `logger.error` without traces, some silent swallows), so the real gap is *guaranteed capture*, not a browsing UI.

## Decision

Four independent layers, native-only:

1. **Structured logs** — `structlog` JSON to stdout, `correlation_id` propagated through every record. `structlog` wraps stdlib logging, so `.exception()` (with traceback) works identically to v1.
2. **Prometheus `/metrics`** — exposed per service (`serve` under FastAPI behind a metrics-scoped API key; `ingest` side-port `:9090`; `bot` side-port `:9091`). **Not in docker-compose** — operator scrapes externally. Grafana dashboard JSON template deferred post-v2. Metrics span health (`source_last_success_seconds`, `schwab_stream_connected`), throughput (`alerts_emitted_total`, `discord_posts_total`), lag (`notify_lag_seconds`, `cache_age_seconds`), status (`schwab_token_expires_in_seconds`), and fun (`tickers_tracked_total`, `paper_trades_open_total`).
3. **Notifications layer** — pluggable, category-routed (see ADR-0016).
4. **Native exception capture (no Sentry)** — wire all four runtime hooks in each service entry point: `sys.excepthook`, `threading.excepthook`, `asyncio` loop exception handler, and framework hooks (FastAPI `@app.exception_handler(Exception)`, discord.py `on_error`/`on_command_error`, APScheduler `EVENT_JOB_ERROR`). Each hook calls `logger.exception(...)` + `notifications.send(SYSTEM_CRITICAL, ...)`. ~80 LOC total. **No aggregator, no `error_history` table** — judged over-engineering for this scale.

## Consequences

- Closes v1's capture gap structurally — fire-and-forget asyncio tasks and scheduler jobs no longer silent-swallow.
- No external SaaS, no extra container, no SDK — matches the project's native-dependency preference.
- Metrics answer "what's degrading" without log-grepping; logs + `correlation_id` answer "what happened."
- Cost: no browsing UI, no fingerprint dedup, no frequency sparklines. Accepted — `psql` + logs + the future Grafana template suffice at this scale.

## Trigger conditions for revisit

- A notification storm during an incident becomes genuinely painful → add an `ErrorAggregator` (fingerprint dedup + storm suppression) + `error_history` table. Self-contained ~80 LOC, one-line change in each hook.

## See also

- [architecture-brief.md § Observability](../architecture-brief.md#observability)
- [ADR-0016: Notifications layer](./0016-notifications-layer-db-routing.md)
