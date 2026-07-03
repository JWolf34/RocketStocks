# ADR-0008: Detection lives in `ingest`; `bot` is a thin presentation layer

**Status**: Accepted (amended 2026-06-05; amendments folded into body 2026-07-03)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05
**Decision-maker**: John Wolf

## Context

In v1, alert detection runs inside the bot via `discord.ext.tasks` loops — the same process that renders embeds and handles interactions. This couples "deciding an alert should fire" with "posting it to Discord," puts detection latency at the mercy of Discord I/O, and means detection can't run without the bot. With v2 splitting into services (ADR-0002), we must decide where detection lives.

## Decision

**Detection lives in `ingest`**, wrapped in an **error boundary**: a faulting `decide()` is caught, logged, and counted — it cannot crash the capture pipeline. Capture is the irreplaceable asset; detection is replayable over the durable log.

**Detection is hybrid by data type** (ADR-0021): equity = **bar-close** evaluation on `BarClosed` (a Bus-1 handler — ADR-0005); options = interval REST scan (a standalone linear pipeline). A detector consumes events, decides, writes an `alerts` row (inside a transaction, with an outbox record), and the bot is `NOTIFY`ed (ADR-0005).

**`bot` is a thin presentation layer**: LISTEN → fetch the alert row → render embed → post → mark `posted_at`. No detection logic in the bot. Recovery follows the Bus-2 consumer contract (ADR-0005): the `posted_at IS NULL` reconcile runs on every reconnect **and on a periodic sweep while connected** — NOTIFY is the latency optimization, the sweep is the correctness mechanism.

**The `alerts` table is the contract** between the detection writer (`ingest`) and its consumers. In v2 it has exactly one consumer: `bot` (the auto-trader is cut — ADR-0027). The persisted row remains the single source of truth for any post-v2 consumer (detection worker / auto-trader) attaching via Bus 2.

**Detection is held extractable post-v2** as its own service, by two constraints: it consumes events *only* via the event-source port (ADR-0023), and its only outputs are `alert`+`outbox` rows. Extraction = add a log-tailing source adapter + a deploy target (Bus-2 consumer); detector code unchanged.

## Consequences

- Detection runs at ingestion speed, independent of Discord I/O and bot uptime.
- The bot can be restarted/redeployed without losing alerts — they're already persisted; the bot reconciles on reconnect.
- The error boundary means a bad detector degrades detection only; data capture continues.
- Cost: detection logic that a contributor might expect "in the bot" is now in `ingest`; this ADR + the brief document that.
- Pure signal functions (v1's `core/analysis/` style) carry over to `libs/analysis/` (ADR-0001 amendment) and are available to detectors and to backtest (ADR-0011) — as inputs to the Phase-2 detection design session, not predestined components (ADR-0030).

## Change log

- 2026-05-27 — locked (detection in `ingest` as Bus-1 subscribers; `alerts` contract had two planned consumers: `bot` + auto-trader).
- 2026-06-05 — amended: error boundary added; extractability constraints pinned; detection made hybrid by data type (bar-close + options scan); auto-trader consumer cut (ADR-0027), leaving `bot` the sole v2 consumer.
- 2026-06-12 — `libs/core` renamed `libs/analysis` (ADR-0001 amendment).
- 2026-07-03 — editorial: amendment banner folded into the body; no decision change.
- 2026-07-03 — bot recovery aligned to the Bus-2 consumer contract (reconnect reconcile + periodic sweep — ADR-0005 amendment).

## See also

- [architecture-brief.md § Detection + alerts](../architecture-brief.md#detection--alerts)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
- [ADR-0027: Manual paper trading only](./0027-manual-paper-trading-only.md)
- [ADR-0030: Detection re-derived](./0030-detection-rederived-single-alert-class.md)
