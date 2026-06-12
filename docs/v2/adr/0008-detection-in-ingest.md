# ADR-0008: Detection lives in `ingest`; `bot` is a thin presentation layer

**Status**: Accepted (**amended** 2026-06-05)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05
**Decision-maker**: John Wolf

> **✎ AMENDED 2026-06-05** (ledger L18, L3; ADR-0009 superseded). Detection **stays in `ingest`**, now wrapped in an **error boundary** — a faulting `decide()` is caught/logged and cannot crash the capture pipeline (capture is the irreplaceable asset; detection is replayable over the durable log). Detection is **extractable post-v2** as its own service, held open by two constraints: it consumes events *only* via the **event-source port** (L16) and its only outputs are `alert`+`outbox` rows; extraction = add a log-tailing source adapter + a deploy target (Bus-2 consumer), detector code unchanged. Detection is **hybrid by data type** (L3): equity = **bar-close** evaluation on `BarClosed` (L17); options = interval REST scan. The `alerts` table contract now has **one v2 consumer, `bot`** — the auto-trader is cut (ADR-0009/0027).

## Context

In v1, alert detection runs inside the bot via `discord.ext.tasks` loops — the same process that renders embeds and handles interactions. This couples "deciding an alert should fire" with "posting it to Discord," puts detection latency at the mercy of Discord I/O, and means detection can't run without the bot. With v2 splitting into services (ADR-0002), we must decide where detection lives.

## Decision

**Detection lives in `ingest`** as Bus 1 subscribers. A detector consumes source/derived events, decides, writes an `alerts` row (inside a transaction, with an outbox record), and the relay `NOTIFY`s the bot (ADR-0005).

**`bot` is a thin presentation layer**: LISTEN → fetch the alert row → render embed → post → mark `posted_at`. No detection logic in the bot.

The **`alerts` table is the contract** between the detection writer (`ingest`) and its consumer (`bot` for posting). *(Amended 2026-06-05: the auto-trader consumer is cut from v2 — ADR-0009/0027; a post-v2 detection worker or auto-trader would attach as a Bus-2 consumer tailing the durable log.)*

## Consequences

- Detection runs at ingestion speed, independent of Discord I/O and bot uptime.
- The bot can be restarted/redeployed without losing alerts — they're already persisted; the bot reconciles on reconnect.
- *(Amended 2026-06-05: auto-trader cut from v2.)* In v2 the `bot` is the sole consumer of `alerts` rows — it posts the alert; the persisted row remains the single source of truth for any post-v2 consumer (detection worker / auto-trader) attaching via Bus 2.
- Cost: detection logic that a contributor might expect "in the bot" is now in `ingest`; this ADR + the brief document that.
- Pure signal functions (v1's `core/analysis/` style) carry over to `libs/core/` and are reused by detectors and by backtest (ADR-0011) unchanged. *(Amended 2026-06-12: the member is named `libs/analysis` — ADR-0001 amendment.)*

## See also

- [architecture-brief.md § Detection + alerts](../architecture-brief.md#detection--alerts)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [ADR-0009: Paper trading + auto-trader](./0009-paper-trading-and-auto-trader-scope.md)
