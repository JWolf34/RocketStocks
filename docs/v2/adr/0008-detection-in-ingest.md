# ADR-0008: Detection lives in `ingest`; `bot` is a thin presentation layer

**Status**: Accepted (amended 2026-06-05; amendments folded into body 2026-07-03, 2026-10-19)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05 · 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** the options-scan hybrid is dropped (no options alerts at launch, [#46](https://github.com/JWolf34/RocketStocks/issues/46)); detectors run on bar close and after each popularity poll ([#46](https://github.com/JWolf34/RocketStocks/issues/46)); the error boundary and alerts-as-contract stand.

## Context

In v1, alert detection runs inside the bot via `discord.ext.tasks` loops - the same process that renders embeds and handles interactions. This couples "deciding an alert should fire" with "posting it to Discord," puts detection latency at the mercy of Discord I/O, and means detection can't run without the bot. With v2 splitting into services (three services, see architecture.md), we must decide where detection lives.

## Decision

**Detection lives in `ingest`**, wrapped in an **error boundary**: a faulting `decide()` is caught, logged, and counted — it cannot crash the capture pipeline. Capture is the irreplaceable asset; detection is replayable over the durable log.

**Detectors run on bar close and after each popularity poll** (amended 2026-10-19; see architecture.md). A detector consumes events, decides, writes an `alerts` row (inside a transaction, with its alert-time price and rule version), and the bot is `NOTIFY`ed (ADR-0005).

**`bot` is a thin presentation layer**: LISTEN → fetch the alert row → render embed → post → mark `posted_at`. No detection logic in the bot. Recovery follows the consumer contract (ADR-0005): the `posted_at IS NULL` reconcile runs on every reconnect **and on a periodic sweep while connected** - NOTIFY is the latency optimization, the sweep is the correctness mechanism.

**The `alerts` table is the contract** between the detection writer (`ingest`) and its consumers. In v2 it has exactly one consumer: `bot`. The persisted row remains the single source of truth for any post-v2 consumer (such as an auto-trader) attaching via NOTIFY.

**Detection is held extractable post-v2** as its own service: its only outputs are `alerts` rows, so extraction = a deploy target; detector code unchanged.

## Consequences

- Detection runs at ingestion speed, independent of Discord I/O and bot uptime.
- The bot can be restarted/redeployed without losing alerts — they're already persisted; the bot reconciles on reconnect.
- The error boundary means a bad detector degrades detection only; data capture continues.
- Cost: detection logic that a contributor might expect "in the bot" is now in `ingest`; this ADR + the brief document that.
- Pure signal functions (v1's `core/analysis/` style) carry over to `libs/analysis/` (ADR-0001 amendment) and are available to detectors and to backtest (`rs_research`, #51), as inputs to detector design rather than predestined components.

## Change log

- 2026-05-27 — locked (detection in `ingest` as Bus-1 subscribers; `alerts` contract had two planned consumers: `bot` + auto-trader).
- 2026-06-05 — amended: error boundary added; extractability constraints pinned; detection made hybrid by data type (bar-close + options scan); auto-trader consumer cut (ADR-0027), leaving `bot` the sole v2 consumer.
- 2026-06-12 — `libs/core` renamed `libs/analysis` (ADR-0001 amendment).
- 2026-07-03 — editorial: amendment banner folded into the body; no decision change.
- 2026-07-03 — bot recovery aligned to the Bus-2 consumer contract (reconnect reconcile + periodic sweep — ADR-0005 amendment).
- 2026-10-19 - amended (foundation re-plan): options-scan hybrid, event-source port and outbox text dropped; detectors also run after popularity polls (#46).

## See also

- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [architecture.md § Data flow](../../foundation/architecture.md#data-flow)
- [ADR-0027: Manual paper trading only](./0027-manual-paper-trading-only.md)
