# ADR-0021: Intraday persistence + bar-close detection; hybrid by data type

**Status**: Accepted
**Date locked**: 2026-06-05
**Decision-maker**: John Wolf

*(Design ledger L3/L17. Amends ADR-0008 (detection model) and ADR-0005 (event flow); relates to ADR-0006, superseded.)*

## Context

With options streaming removed (ADR-0019) and a "memory is a projection" temporal model (ADR-0018), we must pin how equity intraday data is captured from the stream, what granularity detection runs at, and how that keeps deterministic replay (ADR-0023) exact.

## Decision

**Detection is hybrid by data type:** equity = event-driven on the equity stream; options = interval REST scan (ADR-0020), run as a standalone linear pipeline (ADR-0005).

**Equity streaming write path:** `level_one_equity` quotes → an in-memory aggregator builds the current bar → on the bar boundary it emits a **`BarClosed`** event → a **deterministic Bus-1 fan-out** (ADR-0005) runs in fixed order: the persister writes a durable row to the intraday hypertable, the detector folds the bar into its rolling window, the cache_updater writes the latest values. Continuous quotes update only the detector state + the UNLOGGED cache — no durable write at quote rate.

**Curated intraday equity is a durable (logged) hypertable, written bar-on-close.** Equity intraday is backfillable, so durability is an optimization — but load-bearing: it turns warm-up reseed from a full-day ×500-ticker REST storm into DB reads + a bounded gap fetch. It is distinct from the UNLOGGED cache (latest-quote hot reads, crash-disposable).

**Detection is bar-close only.** `decide()` evaluates *only* on `BarClosed`; alerts trigger at bar close, never against in-between-bar quotes. On restart the aggregator's partial bar rebuilds from the bounded REST gap; detection resumes at the next `BarClosed`. So the `BarClosed` log is the complete, exact detection input → replay reproduces every decision exactly. **No raw-tick log.**

**Default bar resolution = 5 minutes** (matches the options scan cadence; sufficient for every v2 signal). **1-minute detection and any intrabar fast-path are deferred post-v2.** Two cheap reversibility seams keep that deferral free: (1) **bar duration is a per-signal parameter** (5m→1m is a config flip); (2) **`decide()` takes a bar-*snapshot*** (a closed bar is the final snapshot) so a future intrabar path can feed partials with no signal-math refactor.

**EOD daily bar (off-hours REST batch) is authoritative** for the daily table; the streamed intraday is the intraday record; tick-miss discrepancies reconcile in favor of EOD.

## Consequences

- The bar-close log is an exact, replayable record of detection — the spine of the test strategy (ADR-0023).
- Sub-bar latency is given up in v2; acceptable at swing horizon, and the stream still earns its keep (bar construction, live cache marks, and keeping the price firehose off the REST budget).
- A future intrabar fast-path is additive (the partial bar already flows) and, when added, is an explicitly-tagged non-replayable overlay with bar-close as the system-of-record.

## See also

- [ADR-0008: Detection in ingest (amended)](./0008-detection-in-ingest.md)
- [ADR-0005: Event bus shape (amended)](./0005-event-bus-shape.md)
- [ADR-0018: Temporal model + warm-up](./0018-temporal-hot-cold-model-and-warmup.md)
- [ADR-0022: Bar retention, storage & compression](./0022-bar-retention-storage-compression.md)
