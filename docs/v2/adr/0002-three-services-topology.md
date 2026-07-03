# ADR-0002: Three-services topology (`ingest` / `serve` / `bot`)

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-05-27
**Decision-maker**: John Wolf

## Context

v1 runs as a single Discord bot process that also polls data and computes alerts on the event loop. That couples three concerns with very different runtime profiles: continuous data ingestion, request-driven read serving, and interactive Discord presentation. The coupling caps concurrency (sync clients block the loop), makes scaling impossible per-concern, and means an external consumer (Cowork) would have no clean surface to call.

## Decision

Split into three deployable services, each its own container, sharing one `Dockerfile` with a different `command:` per service in `docker-compose.yml`:

- **`ingest`** — long-running daemon. Owns all source adapters (streaming + polling), persistence writes, the in-process event bus, detection, and alert writing. The only writer of source data and `alerts` rows.
- **`serve`** — FastAPI read API. The authenticated, versioned contract surface consumed by Cowork (external), the backtest CLI (internal), and the bot (dogfoods it).
- **`bot`** — discord.py presentation. LISTENs for alerts, renders/posts embeds, serves slash commands and manual paper trading. No detection logic and no auto-trader (cut from v2 — ADR-0027).

## Consequences

- Each service scales and fails independently; an ingestion stall doesn't take down the read API.
- Cowork gets a real contract surface (`serve`) from day one; the bot dogfooding it keeps the contract honest.
- Clear ownership: `ingest` writes, `serve` reads, `bot` presents.
- Cost: three processes to operate, cross-service signaling needed (solved by Bus 2 — see ADR-0005), and shared code must live in `libs/` rather than being reachable by relative import.
- Single shared `Dockerfile` keeps image build simple; the trade-off is all three images carry all dependencies.

## Trigger conditions for revisit

- The post-v2 auto-trader arrives as its own (fourth) service, attaching via Bus 2 (ADR-0027) — not hosted in `bot`.
- If `serve` read load or `ingest` write load diverge enough to need independent dependency sets, split the shared `Dockerfile`.

## Change log

- 2026-05-27 — locked.
- 2026-07-03 — auto-trader references updated to match ADR-0027 (auto-trader cut from v2; originally the `bot` bullet said it "runs the auto-trader library" and the revisit trigger pointed at ADR-0009).

## See also

- [architecture-brief.md § Three services](../architecture-brief.md#three-services)
- [ADR-0005: Event bus shape](./0005-event-bus-shape.md)
- [ADR-0008: Detection in ingest](./0008-detection-in-ingest.md)
- [ADR-0027: Manual paper trading only](./0027-manual-paper-trading-only.md)
