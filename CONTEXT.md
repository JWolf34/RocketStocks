# RocketStocks v2

Ubiquitous language for the v2 rewrite (detection, data plane, alerting). Started at
the Phase-2 detection-model design session (2026-07-16); grows as terms are resolved.
Glossary only — decisions live in `docs/v2/adr/`.

## Language

**Subject**:
The thing one options-flow alert is about — an (underlying, side) pair. The grain for
dedup, cooldown, tier escalation, parent linkage, and warm-up state.
_Avoid_: ticker, symbol (those name the underlying alone)

**Side**:
The call-side or put-side of an underlying's options flow. Deliberately not
bullish/bearish — snapshot data has no aggressor side; interpretation defers to day-2
OI build/unwind.
_Avoid_: direction, sentiment

**Burst**:
A concentrated, abnormal run of opening-consistent options volume on one side of an
underlying's near-dated chain — the event an options-flow alert reports.
_Avoid_: spike (reserved for the Phase-0 data spike), signal (broader)

**Tier**:
A rung of the alert escalation ladder (T1/T2/T3) — thresholds on a subject's
accumulated abnormal flow within a session. Crossing a further tier posts a new,
parent-linked alert.
_Avoid_: level, rung; never use for the snapshot datasets

**Dataset**:
One of the three capture datasets (equity OHLCV, options `curated_5m`, options
`eod_full`); the discriminator column on `options_chain_snapshot`.
_Avoid_: tier (reserved for escalation), feed

**Cycle**:
One full pass of the curated 5-min scan train (~4 minutes of sequential chain calls).
A cycle has an id and a nominal T, but per-row `fetched_at`/`quote_time` is the
event-time rule — never the cycle's nominal T.
_Avoid_: snapshot (a single underlying's rows within a cycle), scan (the ongoing
activity)
