# ADR-0022: Bar retention — whole-universe 1-minute, storage & Timescale compression

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-06-05 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

## Context

Live detection defaults to 5-minute bars (ADR-0021), but 1-minute history is the one "one-way door": it cannot be reconstructed beyond Schwab's minute-backfill window. To keep future 1-minute signals backtestable market-wide, we must decide *retention* (independently of live cadence) and confirm storage is affordable.

**Schwab per-frequency history depth** (stated 2026-07-03; confirm exact figures at Phase-0 pre-flight): **~7 days at 1-minute**, **~9 months at 5-minute**, **years at daily**. These windows *slide forward* — what's reachable is always the trailing window from "now."

## Decision

**Two bar families, two authorities** (with ADR-0021): the **detection log** (`stream_bars_5m` — stream-built, immutable, replay/warm-up input; ADR-0021) and the **research history** (this ADR): official, whole-universe, read by serve, backtest, and Cowork. The families never write into each other; divergence between them is a measured metric, not a defect to reconcile.

**Research history: retain 1-minute OHLCV across the whole equity universe** (~10k tickers) via a nightly **EOD backfill job** paralleling the daily job. This is *stored history*, not live detection cadence (which stays 5-minute).

- **Store the finest tier (1-minute); derive 5-minute + daily as lossless rollups** (open=first, high=max, low=min, close=last, volume=sum) via Timescale **continuous aggregates** — no double-store *within the research family*. EOD fetch ≈ 1 Schwab call/symbol/day (~10k calls ≈ ~100 min off-hours; the same call count at any frequency).
- **Store raw / unadjusted OHLCV; apply split/dividend adjustments at read time** (corporate-actions table — sourced per ADR-0025: Nasdaq forward poll + Schwab adjusted baseline with per-row `adjusted_through`) — never rewrite history.
- **TimescaleDB columnar compression:** compress 5m/1m chunks older than ~7 days; leave the tiny daily tier uncompressed; `segment_by => ticker_id, order_by => ts` to match consumer queries. Compression is **lossless / exact**, **transparent on read** (bot / serve / Cowork / backtest get identical rows; large scans get faster), and its mutability limits are **write-side only**.

**Seeding at cutover:** deep history is seeded at **daily** resolution via `tools/backfill` (years available — and daily is the resolution v2's validation uses: event studies measure forward returns at 1d/3d/5d/10d, ADR-0020); 1-minute is seeded only within its **7-day** window. **Historical 5-minute seeding is deliberately dropped.** A continuous aggregate cannot contain data its source table lacks, so reaching Schwab's 9-month 5m window would require a separate seed table + union view — complexity with no v2 consumer (the shipping detector is options-flow, forward-only regardless; ADR-0030). The drop is **reversible while the trigger below holds**, because the 9-month window slides forward.

**Missed-backfill recovery bound:** a failed nightly 1-min job is `BEST_EFFORT` with **alarm on a missed night** (ADR-0017) and must be retried **within the 7-day 1-min window**; beyond it, that day's 1-minute is permanently lost and recorded as a tombstoned gap. (The 9-month 5m window remains a second-chance source for patching 5m/daily continuity on such a day, via the same seed-table mechanism as the trigger path — introduced only if the incident demands it.)

**Storage envelope (whole-universe, compressed):** daily ~25 MB/yr · 5m ~1.5–2.5 GB/yr · **1m ~8–12 GB/yr** (~118 GB/yr uncompressed); a decade of 1-minute ≈ ~100 GB.

**Backtest-at-run-resolution:** signals are backtested at the resolution they run; deep history floors at the retained resolution. A live-1m / backtest-5m asymmetry is acceptable only for *advisory*, human-filtered alerts — never anything auto-executed (the auto-trader is cut, so this latitude exists).

## Trigger conditions for revisit

- A committed 5m-history consumer appears (e.g., an equity-bar detector design session) → run the **one-time historical 5m pull first** (captures the trailing ~9 months at that moment) into a plain seed hypertable, exposed alongside the derived 5m via a union view with an `origin` column.

## Consequences

- Future 1-minute signals are backtestable market-wide from v2 onward, for a few GB/year.
- Read-time adjustment keeps compressed history immutable (no mass-rewrite on corporate actions).
- The 7-day 1-min window makes the nightly backfill the sharpest one-way door in the system — hence the alarm + G2's backfill-completeness audit.
- Deep 5m history (−9mo → −7d) is forgone at cutover; accepted because no v2 consumer exists and the sliding window keeps the option open until one does.
- Cost: a ~100-minute nightly REST backfill window and the discipline of a corporate-actions table.

## Change log

- 2026-06-05 — locked.
- 2026-07-03 — amended (bar-dataset reconciliation with ADR-0021): two-family framing made explicit ("no double-store" scoped to the research family); Schwab per-frequency history depths recorded; seeding policy pinned (daily deep, 1-min ≤7 days, historical 5m dropped behind a trigger condition); 7-day missed-backfill recovery bound added.

## See also

- [ADR-0003: Postgres + Timescale + cache](./0003-postgres-only-timescale-cache.md)
- [ADR-0020: Three datasets + backtest constraints](./0020-three-datasets-and-backtest-constraints.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
- [ADR-0017: Failure mode (backfill policy + tombstones)](./0017-failure-mode-trimmed-a.md)
