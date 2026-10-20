# RocketStocks v2

Ubiquitous language for the v2 rewrite (product, data plane, alerting). Grows as terms
are resolved. Glossary only - the current architecture is `docs/foundation/architecture.md`;
decisions live in the foundation map tickets (#40) and the surviving ADRs in `docs/v2/adr/`.

## Language

### Product

**Data API**:
The read-only, local-network interface through which clients John operates pull the data
RocketStocks collects, to produce their own analyses. Exposes data, not features.
_Avoid_: serve (a deployable, not the interface), public API

**Research-grade**:
The quality bar for what RocketStocks claims: signals carry a measured track record, and
data is validated, gap-aware, and stamped with when it was fetched and how it was adjusted.
_Avoid_: execution-grade (the post-v2 auto-trader bar)

**Base rate**:
A signal's historical outcome statistics - event count, hit rate, median forward return
at fixed horizons - shown in each alert as its confidence.
_Avoid_: confidence score, probability

**Alert**:
A push post reporting that an event happened to one stock (attention surge, earnings
reaction, large move), with the event type's measured base rate as context. Informational:
it claims the event, not a direction. Posted once, never edited.
_Avoid_: signal (reserved for an edge-backed claim), notification

**Signal**:
An alert type that also makes a directional claim, admitted only after its event study
shows an edge. None at launch.
_Avoid_: alert (the informational post), indicator

**Digest**:
A scheduled push post summarising a period (daily post-close digest, Monday earnings
preview). Posted once, never edited.
_Avoid_: screener (v1's edit-in-place lists), report (pull, not push)

**Alert scope**:
The stocks earnings-reaction and large-move alerts watch: a rule-based liquid band plus
every watchlist ticker. Attention surges watch everything ApeWisdom tracks.
_Avoid_: universe (everything collected)

**Report**:
An on-demand research read a server member pulls in Discord - one per subject kind
(Stock Report, Options Report, Watchlist Report). The only pull surface; raw data pulls
go through the Data API.
_Avoid_: lookup, data card, `/data`

**Watchlist**:
A named list of tickers, shared server-wide or personal, that feeds the Watchlist Report
and adds its tickers to what push alerts watch. Always user-curated.
_Avoid_: system list, screener list (v1 lists the bot built itself; not in v2)

### Data plane

**Dataset**:
A capture dataset, the discriminator column on `options_chain_snapshot`. Only
`eod_full` (the nightly full options chain) is collected at launch.
_Avoid_: tier, feed

**Symbol master**:
The authoritative enumeration of the US-listed universe (~10k) — the nightly Nasdaq
Trader listing files, persisted as listing spans in the `symbols` table.
_Avoid_: universe (that's the tracked/banded subset), symbol table (ambiguous)

**Listing span**:
One continuous period during which a ticker denotes one listed instrument
(first_seen → delisted_at). Tickers get recycled across instruments, so the span —
not the bare ticker — is the stable identity for historical data.
_Avoid_: listing (ambiguous with the event), symbol lifetime

**Liquid band**:
The rule-based set of the most liquid stocks - top ~500 by 20-day dollar volume with
price above $5 - rebuilt from Schwab daily bars (cadence set by slice 4's spec). One half of the Alert scope.
_Avoid_: curated list, universe (use band)

**Stream band**:
All names holding a `level_one_equity` stream slot (~500): the liquid band plus every
watchlist ticker.
_Avoid_: equity band (data-type framing), curated band (retired with the options-flow design)

**Optionable**:
A symbol with listed options — operationally, present in the two-directory union
(Nasdaq Trader `options.txt` ∪ Cboe symbol directory) while its listing span is
active. Sizes the options EOD full-scan roster (~5–6k).
_Avoid_: has options (vague), option-eligible

**Cycle**:
One nightly EOD pass of the options chain scan. Its id is the nominal launch boundary,
but per-row `fetched_at`/`quote_time` is the event-time rule - never the cycle's
nominal time.
_Avoid_: snapshot (a single underlying's rows within a cycle), scan (the ongoing
activity)

**Snapshot**:
One underlying's rows within one cycle — the unit a single chain request produces,
and the grain at which the stale guard accepts or rejects.
_Avoid_: cycle (the full pass over all names), chain (the instrument universe itself)

**Tombstone**:
A payload-less snapshot row recording that a fetch did not produce data (failed after
retry, or skipped by an overrun) — keeps scan membership truthful: every (cycle, name)
pair has rows, real or tombstone.
_Avoid_: gap (a tombstone is the *absence of a gap* in the membership record)

**Collection request**:
A row the bot writes asking ingest to fetch data for a ticker outside the stream band
(for example a Stock Report or a paper-trade mark). Ingest fetches, validates, stores
and NOTIFYs; the bot reads the stored result, labelled "as of" if ingest is down.
_Avoid_: on-demand call (the bot never calls a vendor), lookup

**Rule version**:
The version stamp of the event rule that fired an alert, stored on the alert row with
its alert-time price so base rates are always computed against the rule that produced
the alert.
_Avoid_: model version, config hash
