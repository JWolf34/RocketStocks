# ADR-0015: Observability stack — logs + Prometheus + notifications + native exception capture

**Status**: Accepted (amended 2026-10-19)
**Date locked**: 2026-05-27 · **Amended**: 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** native Prometheus on localhost with no metrics-scoped serve key, and no category-routed notifications; ops notices go through one `notify_ops()` ([#52](https://github.com/JWolf34/RocketStocks/issues/52)).

## Context

Three services + real-time pipelines + an external API consumer need observability that answers two distinct questions: "what's broken?" (errors) and "what's degrading?" (rates/latencies). The default reach (Prometheus + Grafana in compose + Sentry/GlitchTip for errors) adds containers and an external SDK. The user wants a personalized, native-leaning stack: Prometheus exposed but not scraped in-compose, a flexible multi-backend notifications layer, and **no external error-tracking dependency** (Sentry explicitly rejected). v1's exception handling is spotty (mostly `logger.error` without traces, some silent swallows), so the real gap is *guaranteed capture*, not a browsing UI.

## Decision

Four independent layers, native-only:

1. **Structured logs** — `structlog` JSON to stdout, `correlation_id` propagated through every record. `structlog` wraps stdlib logging, so `.exception()` (with traceback) works identically to v1.
2. **Prometheus `/metrics`** - exposed per service (each service exposes native Prometheus metrics on a localhost side-port: `ingest` `:9090`, `bot` `:9091`, `serve` its own; no metrics-scoped API key). **Not in docker-compose** - operator scrapes externally. Grafana dashboard JSON template deferred post-v2. Metrics span health (`source_last_success_seconds`, `schwab_stream_connected`), throughput (`alerts_emitted_total`, `discord_posts_total`), lag (`notify_lag_seconds`), status (`schwab_token_expires_in_seconds`), and fun (`tickers_tracked_total`, `paper_trades_open_total`).
3. **Notifications** - one `notify_ops()` function posting to a private ops channel through a Discord webhook each service can call without the bot; no routes table, no Apprise (architecture.md).
4. **Native exception capture (no Sentry)** - wire all four runtime hooks in each service entry point: `sys.excepthook`, `threading.excepthook`, `asyncio` loop exception handler, and framework hooks (FastAPI `@app.exception_handler(Exception)`, discord.py `on_error`/`on_command_error`, APScheduler `EVENT_JOB_ERROR`). Each hook calls `logger.exception(...)` + `notify_ops(...)`. ~80 LOC total. **No aggregator, no `error_history` table** - judged over-engineering for this scale.

## Consequences

- Closes v1's capture gap structurally — fire-and-forget asyncio tasks and scheduler jobs no longer silent-swallow.
- No external SaaS, no extra container, no SDK — matches the project's native-dependency preference.
- Metrics answer "what's degrading" without log-grepping; logs + `correlation_id` answer "what happened."
- Cost: no browsing UI, no fingerprint dedup, no frequency sparklines. Accepted — `psql` + logs + the future Grafana template suffice at this scale.

## Trigger conditions for revisit

- A notification storm during an incident becomes genuinely painful → add an `ErrorAggregator` (fingerprint dedup + storm suppression) + `error_history` table. Self-contained ~80 LOC, one-line change in each hook.

## Change log

- 2026-10-19 - amended (foundation re-plan): metrics and notifications simplified; see the note above.

## See also

- [architecture.md § Notifications and observability](../../foundation/architecture.md#notifications-and-observability)
