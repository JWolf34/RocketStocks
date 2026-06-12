# ADR-0028: Execution strategy — all-in greenfield build + terminal burn-in gate

**Status**: Accepted
**Date locked**: 2026-06-11
**Decision-maker**: John Wolf

## Context

ADRs 0001–0027 closed the target architecture, but no decision existed on *how to deliver it* — the relationship between the v2 build and the live v1, and how much live validation precedes cutover. Four strategies were weighed:

1. **All-in greenfield, clean-slate cutover** — build beside `legacy/`, never touch v1, cut over at parity.
2. **Strangler fig** — extract services around live v1 progressively. Rejected: requires schema-bridge code ADR-0010 already rejected, constrains the new design to interoperate with the old shape, and its core payoff (continuity of the old system and data) is explicitly zero-valued.
3. **In-place modernization of v1** — rejected: the diagnosed problems (poll-only topology, append-only schema, temporal semantics) are the in-place-expensive kind, and ADR-0023's replay seams (injected clock, serializable ordered events) cannot honestly be retrofitted. Kept as the documented fallback if ambition contracts to "better alerts, no streaming/backtest."
4. **Greenfield + overlapped shadow-run** — start `ingest` in production months early to bank forward-only data and soak while surfaces are built. Considered and rejected: **all pre-cutover data is disposable** (widening ADR-0010 — early capture of *new* data is a non-goal, not just old-data migration), and a months-long two-stack window plus mid-build interrupts buys little once data banking is off the table.

What survives from option 4 is its validation job: "reliably store future data" — the project's #1 goal — is a property only demonstrable by running unattended against the real feed across real market weeks. Replay tests (ADR-0023) prove detection logic on recorded inputs; they cannot prove streamer long-run behavior, the 7-day Schwab token-expiry cycle, Timescale compression/retention jobs (which fire on multi-day schedules by design), silent-gap detection (ADR-0017) against real failures, or warm-up (ADR-0018) on a real mid-day restart.

## Decision

**All-in greenfield build, linear (no early soak), with cutover conditional on a terminal burn-in gate.**

- v1 is in **code freeze** (no feature work) for the duration; the parity target is fixed. v1 keeps serving users untouched until cutover.
- The build proceeds linearly per [roadmap.md](../roadmap.md): walking skeleton → data plane → surfaces.
- **Terminal burn-in**: the complete v2 stack runs **3–4 market weeks unattended** against the live market, alerts routed to a **private dev channel**. The window must span ≥1 Schwab refresh-token expiry rehearsed end-to-end, a deliberate mid-market-day `ingest` restart (warm-up proof), and a deliberate `bot` kill (NOTIFY-reconcile proof).
- The burn-in is **validation, not data banking** — no v1-vs-v2 comparison harness, no data-preservation goals. Burn-in data carries over at cutover by default (same schema, zero cost) but has no preservation value; wiping it is equally acceptable.
- Cutover happens on the measurable gate criteria in [roadmap.md](../roadmap.md) — never on vibes. After cutover, v1 stays dormant-but-restartable for ~2 weeks as a rollback hatch, then `legacy/` is retired.

## Consequences

- Zero bridge code; every build slice is "build X to spec," never "change X without breaking prod" — the cheapest shape for agent-executed work under review.
- Accepted: live-contact feedback arrives late, so a data-model mistake found in burn-in ripples through finished services. Mitigated by routing the Phase-1 walking skeleton through the live feed — the data model's first reality contact happens while change is cheapest.
- Accepted: ~3–4 weeks of two stacks on one box and one Schwab API budget (v1 polls REST only, so no streamer conflict; shared REST limits are a Phase-0 pre-flight check).
- Accepted: cutover is a discrete user-facing event (clean slate per ADR-0010 — watchlists recreated, popularity cold-start).
- The gate makes "reliably stores future data" an *evidenced* claim: zero **undetected** gaps is the criterion — gaps are allowed, undetected ones are not.

## See also

- [roadmap.md](../roadmap.md) — phases, slices, gate criteria
- [ADR-0001: Monorepo restructure](./0001-monorepo-restructure.md) (branch + merge posture)
- [ADR-0010: Clean-slate data at cutover](./0010-clean-slate-cutover-data.md) (widened here: pre-cutover capture is also a non-goal)
- [ADR-0017: Failure mode](./0017-failure-mode-trimmed-a.md) / [ADR-0018: Warm-up](./0018-temporal-hot-cold-model-and-warmup.md) / [ADR-0023: Deterministic replay](./0023-test-strategy-deterministic-replay.md) (what burn-in proves that tests cannot)
