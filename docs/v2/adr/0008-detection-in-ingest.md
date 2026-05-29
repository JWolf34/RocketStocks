# ADR-0008: Detection lives in `ingest`; `bot` is a thin presentation layer

**Status**: Accepted
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

In v1, alert detection runs inside the bot via `discord.ext.tasks` loops — the same process that renders embeds and handles interactions. This couples "deciding an alert should fire" with "posting it to Discord," puts detection latency at the mercy of Discord I/O, and means detection can't run without the bot. With v2 splitting into services (ADR-0002), we must decide where detection lives.

## Decision

**Detection lives in `ingest`** as Bus 1 subscribers. A detector consumes source/derived events, decides, writes an `alerts` row (inside a transaction, with an outbox record), and the relay `NOTIFY`s the bot (ADR-0005).

**`bot` is a thin presentation layer**: LISTEN → fetch the alert row → render embed → post → mark `posted_at`. No detection logic in the bot.

The **`alerts` table is the contract** between the detection writer (`ingest`) and its consumers (`bot` for posting, auto-trader for trade decisions).

## Consequences

- Detection runs at ingestion speed, independent of Discord I/O and bot uptime.
- The bot can be restarted/redeployed without losing alerts — they're already persisted; the bot reconciles on reconnect.
- Two consumers (bot + auto-trader) read the same `alerts` rows, so a fired alert drives both a post and a potential paper trade from one source of truth.
- Cost: detection logic that a contributor might expect "in the bot" is now in `ingest`; this ADR + the brief document that.
- Pure signal functions (v1's `core/analysis/` style) carry over to `libs/core/` and are reused by detectors and by backtest (ADR-0011) unchanged.

## See also

- [architecture-brief.md § Detection + alerts](../architecture-brief.md#detection--alerts)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [ADR-0009: Paper trading + auto-trader](./0009-paper-trading-and-auto-trader-scope.md)
