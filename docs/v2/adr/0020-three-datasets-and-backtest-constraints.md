# ADR-0020: Three datasets, three purposes; options forward-only backtest

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-06-05 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

*(Supersedes ADR-0007; backtest specifics also amend ADR-0011.)*

## Context

Options data serves two different needs — live alerts (curated, intraday) and market-wide flow research (breadth, history) — at different cadences and universes. The superseded three-tier model (ADR-0007) used in-memory `level_one_option` streaming for Tier 1; v2 has no options streaming (ADR-0019). Schwab also cannot backfill option chains (only the current chain), which permanently constrains what backtest can measure.

## Decision

**Three datasets:**

| Dataset | Cadence | Universe | Backfillable | Purpose |
|---|---|---|---|---|
| Equity OHLCV | daily + intraday bars | whole (≤500 streamed live; backfill deep) | **Yes** (daily deep; 1-min ≤7 days; historical 5m dropped — ADR-0022) | equity backtests (deep history); forward returns for event studies |
| Options 5-min curated | 5-min REST scan | curated ~300–500 (±10% moneyness config window, ≤30 DTE — ADR-0032 D7) | **No (forward-only)** | live alerts + intraday event studies |
| Options EOD full | nightly REST (~1h off-hours) | whole ~5–6k | **No (forward-only)** | daily swing backtests + day-over-day OI; selection-bias-free |

All snapshots are immutable and point-in-time (`fetched_at`, `source_status`, per-contract `quote_time`), stored in one `options_chain_snapshot` hypertable with a `dataset` discriminator (`curated_5m` / `eod_full`; renamed from `tier` — ADR-0032 D5 reserves "tier" for alert escalation). (Full per-contract field schema is a Stage-2 domain-modeling artifact; pinned by ADR-0032.)

**The curated universe size is budget-derived, not fixed:** as many underlyings as the REST headroom rule allows at the 5-min cadence (roadmap pre-flight: RTH steady-state ≤ ~80% of the measured limit; at a working ~100/min that yields ~400 underlyings — the ~300–500 above is the working range). Overflow knobs in priority order: **shrink the curated universe first** (ADR-0019's priority/eviction order exists for exactly this); stretch the scan cadence only as a last resort (ADR-0029 ties the 5-minute cadence to the product thesis).

**Backtest / research constraints:**

- **Options backtest is forward-only** — the window starts empty at cutover and grows. Equity-only strategies have deep history immediately.
- **Backtest the underlying equity as the tradeable; options flow is the *predictor*** (option-leg P&L isn't reliably reconstructable from snapshots).
- **Event studies** (flow events → forward *equity* returns) are the day-one validation; distinct from strategy backtest by *what they measure*, not by data resolution.
- **Selection bias:** the curated 5-min tier is biased (universe chosen by the signals under test) — prefer the EOD full-universe tier for research conclusions.

## Consequences

- Live alerts get curated intraday depth; research gets bias-free market-wide breadth.
- The forward-only constraint makes capture-from-cutover the only irreversible task; the EOD-full tier is the primary bias-free research dataset.
- Cost: deep options backtests are impossible until history accumulates; equity history covers the gap.

## Change log

- 2026-06-05 — locked.
- 2026-07-03 — amended: equity "backfillable" qualified per the seeding policy (daily deep; 1-min ≤7 days; historical 5m dropped — ADR-0022); curated universe size made explicitly budget-derived with overflow-knob priority (REST budget review).
- 2026-07-16 — editorial: discriminator column renamed `tier` → `dataset` (ADR-0032 D5; "tier" is reserved for alert escalation). No decision change.
- 2026-07-16 — amended: curated capture width restated as a moneyness window (working ±10%, config) instead of ATM±5 strikes (ADR-0032 D7 — strike-count units are incoherent across strike spacings; dataset intent unchanged, ≤30 DTE stands).

## See also

- [ADR-0007: Options data pipeline (superseded)](./0007-options-data-pipeline-three-tiers.md)
- [ADR-0011: Backtest in v2 (amended)](./0011-backtest-in-v2-eda-deferred.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
- [ADR-0022: Bar retention, storage & compression](./0022-bar-retention-storage-compression.md)
