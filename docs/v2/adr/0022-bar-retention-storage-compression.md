# ADR-0022: Bar retention — whole-universe 1-minute, storage & Timescale compression

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

*(Design ledger L20.)*

## Context

Live detection defaults to 5-minute bars (ADR-0021), but 1-minute history is the one "one-way door": it cannot be reconstructed beyond Schwab's minute-backfill window. To keep future 1-minute signals backtestable market-wide, we must decide *retention* (independently of live cadence) and confirm storage is affordable.

## Decision

**Retain 1-minute OHLCV across the whole equity universe** (~10k tickers) via a nightly **EOD backfill job** paralleling the daily job. This is *stored history*, not live detection cadence (which stays 5-minute).

- **Store the finest tier (1-minute); derive 5-minute + daily as lossless rollups** (open=first, high=max, low=min, close=last, volume=sum) via Timescale **continuous aggregates** — no double-store. EOD fetch ≈ 1 Schwab call/symbol/day (~10k calls ≈ ~100 min off-hours; the same call count at any frequency).
- **Store raw / unadjusted OHLCV; apply split/dividend adjustments at read time** (corporate-actions table) — never rewrite history.
- **TimescaleDB columnar compression:** compress 5m/1m chunks older than ~7 days; leave the tiny daily tier uncompressed; `segment_by => ticker_id, order_by => ts` to match consumer queries. Compression is **lossless / exact**, **transparent on read** (bot / serve / Cowork / backtest get identical rows; large scans get faster), and its mutability limits are **write-side only**.

**Storage envelope (whole-universe, compressed):** daily ~25 MB/yr · 5m ~1.5–2.5 GB/yr · **1m ~8–12 GB/yr** (~118 GB/yr uncompressed); a decade of 1-minute ≈ ~100 GB.

**Backtest-at-run-resolution:** signals are backtested at the resolution they run; deep history floors at the retained resolution. A live-1m / backtest-5m asymmetry is acceptable only for *advisory*, human-filtered alerts — never anything auto-executed (the auto-trader is cut, so this latitude exists).

## Consequences

- Future 1-minute signals are backtestable market-wide from v2 onward, for a few GB/year.
- Read-time adjustment keeps compressed history immutable (no mass-rewrite on corporate actions).
- Cost: a ~100-minute nightly REST backfill window and the discipline of a corporate-actions table.

## See also

- [ADR-0003: Postgres + Timescale + cache](./0003-postgres-only-timescale-cache.md)
- [ADR-0020: Three datasets + backtest constraints](./0020-three-datasets-and-backtest-constraints.md)
- [ADR-0021: Intraday persistence + bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
