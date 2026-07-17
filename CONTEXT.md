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

**Symbol master**:
The authoritative enumeration of the US-listed universe (~10k) — the nightly Nasdaq
Trader listing files, persisted as listing spans in the `symbols` table.
_Avoid_: universe (that's the tracked/banded subset), symbol table (ambiguous)

**Listing span**:
One continuous period during which a ticker denotes one listed instrument
(first_seen → delisted_at). Tickers get recycled across instruments, so the span —
not the bare ticker — is the stable identity for historical data.
_Avoid_: listing (ambiguous with the event), symbol lifetime

**Curated band**:
The top-priority optionable names receiving the 5-min options chain scan (~400,
REST-budget ceiling 480). Always a subset of the stream band — every scanned name
has live bars.
_Avoid_: options band (data-type framing), curated list/universe (use band)

**Stream band**:
All names holding a `level_one_equity` stream slot (≤ ~500): the curated band plus
remaining priority names (streamed but not options-scanned).
_Avoid_: equity band (data-type framing), watchlist (a priority source, not the band)

**Discovery**:
The priority source that fills remaining curated capacity with the most liquid
optionable names (chain-premium-volume rank) not selected by higher sources. Sticky
membership with rank hysteresis — never time-rotated.
_Avoid_: rotation slots, sampling

**Optionable**:
A symbol with listed options — operationally, present in the two-directory union
(Nasdaq Trader `options.txt` ∪ Cboe symbol directory) while its listing span is
active. Sizes the options EOD full-scan roster (~5–6k).
_Avoid_: has options (vague), option-eligible

**Headroom**:
The gap between a band's nightly-fill target and its ceiling (curated ~400→480),
reserved so intraday adds land without evicting anyone. Forced (intraday) eviction
exists only when headroom is exhausted or the streamer rejects an ADD.
_Avoid_: buffer (vague), slack

**Cycle**:
One full pass of the curated 5-min scan train (~4 minutes of sequential chain calls).
A cycle has an id and a nominal T, but per-row `fetched_at`/`quote_time` is the
event-time rule — never the cycle's nominal T.
_Avoid_: snapshot (a single underlying's rows within a cycle), scan (the ongoing
activity)
