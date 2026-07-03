# ADR-0021: Intraday persistence + bar-close detection; hybrid by data type

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-06-05 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

*(Amends ADR-0008 (detection model) and ADR-0005 (event flow); relates to ADR-0006, superseded.)*

## Context

With options streaming removed (ADR-0019) and a "memory is a projection" temporal model (ADR-0018), we must pin how equity intraday data is captured from the stream, what granularity detection runs at, and how that keeps deterministic replay (ADR-0023) exact.

## Decision

**Detection is hybrid by data type:** equity = event-driven on the equity stream; options = interval REST scan (ADR-0020), run as a standalone linear pipeline (ADR-0005).

**Equity streaming write path:** `level_one_equity` quotes → an in-memory aggregator builds the current bar → on the bar boundary it emits a **`BarClosed`** event → a **deterministic Bus-1 fan-out** (ADR-0005) runs in fixed order (`persister → cache_updater → detector`): the persister writes a durable row to the intraday hypertable, the cache_updater writes the latest values, the detector folds the bar into its rolling window. Continuous quotes update only the detector state + the UNLOGGED cache — no durable write at quote rate.

**Stream-built 5-minute bars form the detection log** (`stream_bars_5m`) — a durable (logged) hypertable, written bar-on-close, streamed tickers only. It records exactly what detection saw: **rows are immutable, never reconciled against official REST data, and its retention is independent of every other job** (compress old chunks; any future retention policy is a separate dial, never coupled to backfill success). Replay and warm-up read *only* this log. Durability here is also load-bearing operationally: it turns warm-up reseed from a full-day ×500-ticker REST storm into DB reads + a bounded gap fetch. The detection log is distinct from the UNLOGGED cache (latest-quote hot reads, crash-disposable) and from the **research history** (official whole-universe 1-min + derived rollups — ADR-0022).

**Detection is bar-close only.** `decide()` evaluates *only* on `BarClosed`; alerts trigger at bar close, never against in-between-bar quotes. On restart the aggregator's partial bar rebuilds from the bounded REST gap; detection resumes at the next `BarClosed`. So the `BarClosed` log is the complete, exact detection input → replay reproduces every decision exactly. **No raw-tick log.**

**Session scope: regular trading hours only (09:30–16:00 ET).** Bars build and detection evaluates only within RTH; the options scan likewise. Extended-hours (pre/post-market) data is a documented deferral, not an accident — out of scope for v2. The stream may connect before the open (warm-up), but the day's first bar is 09:30–09:35 ET. Exact session-boundary rules (half-days, late opens) are pinned in S003 alongside the bar-bucketing rule, since detection, warm-up, and replay all hang off them.

**Default bar resolution = 5 minutes** (matches the options scan cadence; sufficient for every v2 signal). **1-minute detection and any intrabar fast-path are deferred post-v2.** Two cheap reversibility seams keep that deferral free: (1) **bar duration is a per-signal parameter** (5m→1m is a config flip); (2) **`decide()` takes a bar-*snapshot*** (a closed bar is the final snapshot) so a future intrabar path can feed partials with no signal-math refactor.

**Two authorities, deliberately:** official REST data (the nightly 1-min backfill and its 5-min/daily rollups — ADR-0022) is authoritative for **research and serving**; the stream-built detection log is authoritative for **what detection saw**. Tick-miss discrepancies between the two are *measured* — the S003 stream-vs-REST comparison graduates into a nightly gauge — never patched into the detection log (patching would break replay exactness, ADR-0023).

## Consequences

- The bar-close log is an exact, replayable record of detection — the spine of the test strategy (ADR-0023).
- Sub-bar latency is given up in v2; acceptable at swing horizon, and the stream still earns its keep (bar construction, live cache marks, and keeping the price firehose off the REST budget).
- A future intrabar fast-path is additive (the partial bar already flows) and, when added, is an explicitly-tagged non-replayable overlay with bar-close as the system-of-record.

## Change log

- 2026-06-05 — locked. Originally said "EOD daily bar is authoritative for the daily table … tick-miss discrepancies reconcile in favor of EOD."
- 2026-07-03 — amended (bar-dataset reconciliation with ADR-0022): the durable intraday hypertable is named the **detection log** (`stream_bars_5m`) — immutable, never reconciled, retention independent; authority split into detection log (what detection saw) vs. official research history (what serve/backtest read); discrepancies measured, not patched.
- 2026-07-03 — amended: session scope pinned to RTH-only; extended-hours data deferred out of v2.

## See also

- [ADR-0008: Detection in ingest (amended)](./0008-detection-in-ingest.md)
- [ADR-0005: Event bus shape (amended)](./0005-event-bus-shape.md)
- [ADR-0018: Temporal model + warm-up](./0018-temporal-hot-cold-model-and-warmup.md)
- [ADR-0022: Bar retention, storage & compression](./0022-bar-retention-storage-compression.md)
