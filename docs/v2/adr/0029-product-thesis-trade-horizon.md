# ADR-0029: Product thesis — intraday detection of swing-relevant events

**Status**: Accepted
**Date locked**: 2026-06-11
**Decision-maker**: John Wolf

## Context

The product thesis existed only as side-comments scattered across the data-plane ADRs ("swing-trade horizon, options-flow-first" in ADR-0018's context; "acceptable at swing horizon" in ADR-0021's consequences; "daily swing backtests" in ADR-0020). A philosophy review (2026-06-11) found no single statement that future decisions — latency budgets, signal designs, feature requests — could be tested against. This ADR states it.

## Decision

**RocketStocks v2 performs intraday detection of swing-relevant events for retail traders.** Two clocks, deliberately separate (per ADR-0018: latency is a product/UX property, not a backtest input):

- **Detection clock — intraday.** Signals are detected on 5-minute bar closes and 5-minute options-chain scans. The alert's timeliness ("unusual flow is happening *now*") comes from this clock.
- **Thesis clock — swing.** What an alert *claims* is that the intraday observation predicts a multi-day-to-multi-week move in the underlying. Claims are validated by event studies: event at time T → forward equity returns at 1d/3d/5d/10d horizons (ADR-0011/0020).

**Boundaries:**

- **Options-flow-first**: short-dated options flow (≤30 DTE, ATM±5 — ADR-0020) is the predictive edge; **the underlying equity is the tradeable**.
- **Not HFT / not intraday execution guidance**: no ticks, no L2, no sub-minute detection, no scalp entry/exit guidance. Sub-bar latency was deliberately given up (ADR-0021); reclaiming it is a documented post-v2 trigger path, not a v2 goal.
- **Not long-horizon guidance**: fundamentals appear as report *context* (ADR-0025/0026), never as a signal class in v2.
- **Advisory, human-filtered**: alerts inform a human decision; nothing is auto-executed in v2 (ADR-0027), which is also what licenses the latitude clauses in ADR-0021/0022.

**Latency posture:** alert delivery is event-driven end to end (detection → outbox → NOTIFY → post) and needs no separate budget; bot interaction code is written performance-first (defer early, read hot paths from the cache), but **numeric interaction SLOs are deliberately deferred** to the bot-phase planning sessions.

## Consequences

- Future decisions get a yardstick: a proposal that needs sub-minute latency, portfolio-allocation advice, or buy-and-hold signals is out of thesis until this ADR is superseded.
- The 5-minute cadences are now product-aligned by statement, not by accident of the Schwab budget.
- Alerts may *highlight* intraday movement as evidence, but the promised value is the swing thesis — embed copy, report framing, and backtest validation horizons should all reflect that.

## See also

- [ADR-0018: Temporal model](./0018-temporal-hot-cold-model-and-warmup.md) · [ADR-0020: Three datasets](./0020-three-datasets-and-backtest-constraints.md) · [ADR-0021: Bar-close detection](./0021-intraday-persistence-and-bar-close-detection.md)
- [ADR-0030: Detection re-derived — single alert class](./0030-detection-rederived-single-alert-class.md)
