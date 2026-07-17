# `UniverseManager` — design note

**Status**: **Draft — pending review** (session opened 2026-07-16, closed 2026-07-17; D1–D11)
**Decision-maker**: John Wolf

## Context

[ADR-0019](./adr/0019-two-band-universe-and-priority.md) mandates a full design note
for the `UniverseManager` — the component that recomputes the two bands' desired sets
from DB-backed sources, diffs against current subscriptions, ADD/UNSUBs, and evicts by
the locked priority order. This note is that artifact (roadmap Phase 2 /
[phase2-plan.md](./phase2-plan.md) §2); it gates S010.

Required coverage (ADR-0019): the symbol master (Nasdaq Trader listing files —
[ADR-0025](./adr/0025-live-source-set-schwab-apewisdom-nasdaq.md)) · instrument filter
rules · the diff-based listing/delisting/rename lifecycle · the locked priority order
with eviction · the sizes glossary (~500 streamed / ~5–6k optionable / ~10k listed).

Adjacent constraints this note designs against: curated size is budget-derived
(~400, ceiling 480 — [ADR-0020](./adr/0020-three-datasets-and-backtest-constraints.md),
[schwab-rest-budget.md](./schwab-rest-budget.md)); the detector's baselines are
per-subject, forward-only, with a minimum-history gate
([ADR-0032](./adr/0032-options-flow-detection-model.md) D3) — so band churn has a
detection cost; warm-up rebuilds the subscription universe from Postgres alone
([ADR-0018](./adr/0018-temporal-hot-cold-model-and-warmup.md)); every scanned name
must have live bars (curated ⊆ streamed — ADR-0032 D2).

## Decisions

*(recorded as they close; D-numbering session order)*

### D1 — Symbol master: nightly file diff into listing-span rows *(closed 2026-07-16)*

The symbol master is ingested by a **nightly fetch of `nasdaqlisted.txt` +
`otherlisted.txt`** (ADR-0025), parsed behind an `rs_sources` adapter (the
calendar-adapter pattern), scheduled in the nightly train **before** the options EOD
full scan so the night's trains run on the fresh universe. The footer
`File Creation Time` row is the file's as-of stamp.

Storage is a single **`symbols` table with one row per listing span** — key
`(symbol, first_seen)`, columns `last_seen`, `status` (active/delisted),
`delisted_at`, plus the surviving file flags — with **at most one active span per
symbol** (partial unique index). Current-universe reads are one indexed query
(`status = 'active'`); the nightly diff against active spans is the lifecycle
detector. No separate event table, no raw-file retention, no daily snapshots.

- **Why spans, not one row per symbol: tickers get recycled.** Probed live
  2026-07-16 (2 price-history calls): **FB** — delisted as Meta's ticker 2022-06-09 —
  returns 266 daily candles with the latest *today*: the ticker was reassigned to a
  new instrument ~mid-2025. A single-row upsert would flip the old row back to
  active, erasing the delist episode; the Nasdaq files publish current state only,
  so an overwritten span is **unrecoverable**. Span rows cost a two-column key
  (diff logic and live-read cost identical) and preserve the one disambiguator
  research needs against ticker-keyed history: which instrument a ticker denoted on
  date X.
- **Failure handling:** a failed/missing nightly fetch is non-fatal — the universe
  runs on yesterday's spans, surfaced as staleness (ADR-0017 pattern). A parse that
  would mass-delist (file shrinkage past a sanity threshold vs yesterday) **aborts
  the diff and alarms instead of applying it** — a truncated download must never
  delist half the universe.

### D2 — Listing lifecycle: rename = delist + add; ADD triggers a deep seed *(closed 2026-07-16)*

Lifecycle events are exactly the nightly diff outcomes, applied to spans:

- **New symbol in the files** → open a new span → **trigger a deep daily-history
  seed for that symbol** (an S011 hook; genuine IPOs get their short history,
  relists get whatever Schwab serves).
- **Symbol missing from the files** → close the active span (`delisted_at`), which
  removes it from every desired set and from the nightly trains. (Schwab has **no
  delisting feed** — its API is per-symbol lookup only, verified against the API
  surface + ADR-0025 — so the file diff *is* the delisting source; no extra
  dependency needed.)
- **Renames are not tracked** — a rename is observed as delist + add. No old→new
  stitching, no rename event type: the files carry no linkage (no CUSIP), detection
  would be name-matching heuristics, and the events are rare in a liquid band.
  Probed live 2026-07-16: **META returns continuous daily history to the 2012 FB
  IPO** (3,559 candles, 2,531 pre-rename) — Schwab keys history to the instrument,
  so the ADD-seed **self-heals equity history across renames**. What breaks,
  acceptably: our own stored rows stay under the old ticker (orphaned tail, not
  corruption), and options baselines restart under the new subject (forward-only
  dataset; ADR-0032 D3's minimum-history gate absorbs the ramp).
- **Accepted, documented risk:** deep-seeding a *recycled* ticker can ingest stray
  candles from the prior instrument (the FB probe returned one Meta-era candle on
  the new instrument's history). Rare, research-grade impact; span boundaries are
  the future cleanup key.

### D3 — Historical delistings: out of scope for v2 *(closed 2026-07-16)*

Names delisted **before go-live** never enter the symbol master, and no live
function misses them: bands, scans, and alerts operate on currently-listed names
only, and both options datasets are forward-only. The exposed consumer is
deep-history equity research (survivorship bias in the daily seed) — and a
delisting-flag backfill alone would not fix it, because Schwab serves no price
history for symbols it no longer resolves. The real fix is a historical
prices+delistings vendor dataset (Sharadar/Norgate/Polygon-class): **post-v2, trigger
= equity strategy research becomes a first-class goal.** The span schema is
backfill-compatible by construction (a future backfill inserts closed spans for
never-seen names; file-derived columns stay nullable).

### D4 — Instrument filter: classify at ingest, filter at read *(closed 2026-07-16)*

**Policy:** universe consumers see **common shares + ADRs + ETFs/ETNs**. Excluded
classes: test issues, NextShares, preferreds, warrants, rights, units, when-issued.
ETFs are emphatically in (SPY/QQQ are core flow names); ADRs are in (liquid,
optionable). **Financial status is stored, never filtered** — deficient/delinquent/
bankrupt names still trade, and distress names are exactly where popularity-band
events live; filtering them would blind the bands to a GME-class event.

**Mechanism:** every file row is persisted as a span; ingest computes an
**`instrument_class`** column (common/adr/etf/preferred/warrant/right/unit/test/
other) and consumers filter on it. The files carry no instrument-type column —
classification is symbol-suffix + security-name heuristics, which *will* misfire —
so exclusion must be auditable and reversible: a misclassified stored row is fixable
by reclassification; a row dropped at ingest is invisible and unrecoverable. Exact
suffix/keyword rules are S010 spec material, tested against file fixtures; this note
pins the policy and the class set.

### D5 — Optionability: two-directory union, no probe machinery *(closed 2026-07-16)*

**`optionable` = symbol appears in (Nasdaq Trader `options.txt` ∪ Cboe symbol
directory), joined against active spans**, refreshed nightly with the symbol master.
Neither Schwab nor the listing files can enumerate optionability; the two venue
directories can, and the union is required because **neither is a superset** (probed
2026-07-16: options.txt 4,185 distinct underlyings · Cboe CSV 5,319 · shared 4,020 ·
Cboe-only 1,299 incl. unambiguously optionable ETFs — options.txt alone would
undersize ADR-0020's ~5–6k universe by ~20%; some gap on both sides is symbol-
convention skew, e.g. `BRKB` vs `BRK.B`, folded into the normalization mapping we
already owe Schwab).

- **Sources:** `options.txt` is the same nightly SymbolDirectory family as the
  symbol master (per-series rows, ~94 MB, `File Creation Time` footer — zero new
  dependencies). The Cboe CSV is one unauthenticated GET (~400 KB, no as-of stamp)
  and enters as a **fourth live source — ADR-0025 amended**. **OCC — the actually
  authoritative directory — is disqualified: its download sits behind a Cloudflare
  bot wall** (403 to non-browser clients); automating around it is exactly the
  fragility the reliability thesis forbids.
- **Failure shape:** per-source graceful-stale (D1 rule) — either directory failing
  degrades coverage only to the other's roster; no single point of failure; row-count
  anomaly guards on both.
- **Cross-check, never auto-mutation:** the S009 EOD scan alarms on divergence —
  a live Schwab chain on an unflagged name, or persistently empty chains on a
  flagged one. Alarms flag for review; nothing silently demotes.
- **Deleted machinery:** the Schwab-empirical qualification (10k bootstrap probe
  night, weekly re-probe scheduler, clean-empty demotion heuristics) is **not
  built**; it is the documented fallback if either directory endpoint proves
  unstable.

### D6 — Sequencing: symbol-master increment lands before S009 *(closed 2026-07-16)*

S009's defining property — bias-free breadth over the whole optionable universe —
requires the symbol master + D5's optionability flags, which live in S010, planned a
wave behind it. **Amendment (phase2-plan): the symbol-master ingest + optionability
union (D1/D4/D5) is S010's first increment and lands before or with S009**; the band
computation and priority/eviction machinery stay in S010 proper. S009's dependency
edge becomes "S008 + S010's symbol-master increment."

### D7 — Bands: curated ⊆ streamed as a structural invariant; config ceilings *(closed 2026-07-16)*

**Hard invariant: curated ⊆ streamed, enforced structurally** — the desired-set
computation derives the stream set *from* the curated set (curated names first, then
remaining priority names up to the stream ceiling), so no configuration, priority
order, or eviction can violate it. This is load-bearing for ADR-0032 D2: equity-bar
fusion grades every burst against live bar context, so a scanned name without a
stream slot would corrupt detection silently. ADR-0019 amended to state the
invariant (it was previously implied only by ADR-0032 D2's citation).

**Band structure:**

- **Curated band** — top-priority *optionable* names, sized by the REST budget
  (working ~400, ceiling 480 — [schwab-rest-budget.md](./schwab-rest-budget.md)).
- **Stream band** — the curated band **plus** remaining priority names up to the
  stream ceiling (~100 slots of streamed-but-not-scanned names: non-optionable
  watchlist names, paper positions without options — live bars, detection-log
  coverage, live marks).

**Ceilings are config with named owners, not constants.** The curated ceiling (480)
derives from the measured REST budget. The stream ceiling (~500) is the legacy
soft build-verified figure that v2 has never probed (Phase 1 streamed one ticker):
its true value is verified empirically at S010 acceptance by subscribing the full
desired set. The manager **treats streamer ADD rejections as a budget signal** —
evict lowest-priority, retry, alarm — never assuming the config number is truth.

**Sizes glossary** (working figures): ~10–11k listed → in-policy tracked
(common + ADR + ETF — D4) → ~5–6k optionable (D5) → **≤ ~500 streamed** (config,
build-verified) → **~400 curated** (config, REST-budget ceiling 480).

### D8 — Priority sources: precise sets, empty-source seam, verdict-conditioned stickiness *(closed 2026-07-16)*

The locked ADR-0019 order, with each source defined as a DB-backed set behind one
interface — *"give me your desired symbols, from the DB"* — and **absent sources
return empty** (no stub logic, no placeholder polls; S010 needs no Phase-3
knowledge):

1. **Paper positions** — distinct underlyings of open manual paper positions
   (ADR-0027). *Phase 2: empty.*
2. **Active alert subjects** — see stickiness below. Alerts fire only on curated
   names, so this source never admits a new name — it is **eviction protection**
   through the alert lifecycle. *Phase 2: live once S017 fires.*
3. **Watchlists** — distinct underlyings across user watchlists. *Phase 2: empty.*
4. **Screener top-10** — seam only; the v2 screener is a Phase-3 report-session
   design. *Phase 2: empty.*
5. **Top-100 popularity** — top 100 by rank from the latest ApeWisdom poll (S014).
   *Phase 2: live from wave 3.*
6. **Discovery** — everything left (own decision below).

Phase-2 consequence, accepted: until S014 lands the bands run on alert stickiness +
discovery alone; before S017 fires, discovery is effectively the whole curated band.
That is the design working as intended, not a degenerate state.

**Verdict-conditioned stickiness (source 2)** — slot spend proportional to evidence,
not to a clock (shape here, values config):

- **Current session:** alerted subjects are eviction-protected (automatic intraday
  follow-through).
- **Through the verdict:** the name stays curated until its D6 verdict posts
  (next session).
- **After the verdict:** *confirmed* → extend K sessions (working K=3 — follow the
  demonstrated accumulation); *denied* → release immediately.

Nothing hard requires post-session membership (the verdict computes from the EOD
full scan; the D4 ladder is intrasession; research forward-returns come from the
research history) — the window exists solely for intraday follow-through detection
on names the market confirmed, and a flat window was rejected because it holds
denied-verdict names exactly as long as confirmed ones.

### D9 — Discovery: liquidity-ranked, hysteresis-stable, never rotated *(closed 2026-07-16)*

Discovery fills curated capacity left after sources 1–5 (in Phase 2, most of the
band). **Selection is by options-liquidity rank; membership is stable; there is no
time-based rotation.**

- **Metric: chain premium volume** — per underlying, Σ(volume × mark × 100) over all
  contracts in that night's `eod_full` snapshot, calls + puts; ranking statistic =
  **median over trailing M sessions** (config, working M=20). Median so a single
  burst day cannot reshape the band (bursts are the detector's to catch, not the
  ranking's to chase); premium dollars so low-priced teenie churn doesn't outrank
  real flow.
- **Cold start:** the EOD dataset is empty at go-live — bootstrap the rank from
  equity dollar volume (median trailing close × volume over the seeded daily
  history) until M sessions of `eod_full` exist, then switch. One rule, then it
  self-feeds.
- **Stability via rank hysteresis:** membership changes only on displacement by
  higher-priority sources, delisting/filter changes, or a *sustained* rank fall —
  enter at rank ≤ E, exit only after rank > X for S consecutive sessions (all
  config). Every exit discards a matured baseline and every re-entry repays the
  D3 minimum-history ramp, so boundary names must not flap.
- **Why not rotation (recorded so future-us doesn't "improve" it):** rotation
  faster than baseline maturity produces slots that can never alert — data
  collection wearing a detection costume. Breadth is the EOD full scan's job.
- **Why liquidity over the alternatives** (cap/index membership, equity dollar
  volume permanent, IV rank, earnings proximity, social, random): alertability has
  a statistical substrate — D0's *material* component is a premium floor an
  illiquid chain structurally cannot clear, and *abnormal* needs usable median/MAD
  baselines — so premium-volume rank ≈ P(slot can ever produce a valid alert). It
  is also self-sourced, slow-moving (cooperates with hysteresis), and
  self-correcting (a waking name climbs the trailing median; the fast path for
  sudden wake-ups is the popularity source, a different slot budget). Accepted
  weakness, recorded: rich-get-richer at the margin — a name's *first* hot day
  happens off-band (caught by EOD data, next-day rank, or popularity), inherent to
  any curated-subset design.

### D10 — Timing: adds are intraday, evictions are session-boundary *(closed 2026-07-17)*

Membership changes follow **asymmetric cadences** — adds are cheap and immediately
valuable; evictions are destructive (a mid-session eviction tears that name's
baseline coverage for the day) and never urgent:

- **Nightly full recompute**, sequenced in the train after the symbol-master +
  optionability refresh and the discovery-rank update, applied **before session
  open**. This is the *only* place popularity and discovery changes enter, and where
  all deferred evictions apply (expired stickiness, popularity fall-outs, discovery
  hysteresis exits, trim back to target) — an overnight eviction costs nothing: the
  name just stops having sessions, no intra-session hole.
- **Intraday incremental reconcile** (short cadence / event-nudged), **additive and
  protective only**: a paper position opens or a watchlist name is added → ADD to the
  stream promptly, join the curated scan **at the next cycle boundary** (never
  mid-cycle); an alert fires → D8 stickiness protection engages. Popularity and
  discovery rank moves never act intraday.
- **Target vs ceiling — reserved headroom.** The nightly recompute fills each band to
  its **nominal target** (curated ~400; stream analogously below its ~500 limit), not
  its ceiling (480 / ~500). Discovery is the residual filler *up to target*. The
  target→ceiling gap is deliberately reserved **intraday-add headroom**, so adds land
  without evicting anyone. If nightly filled to the ceiling, every intraday add would
  force the one kind of eviction the design exists to avoid.
- **Forced eviction** — the only intraday eviction — happens when headroom is
  exhausted or the streamer rejects an ADD (D7 budget signal). **Eviction total
  order: (source index, within-source rank, symbol), worst first, skipping
  D8-protected members.** Every member's admitting source is the highest source
  selecting it; ranked sources (popularity, discovery) supply within-source rank,
  unranked sources (positions, alerts, watchlists) rank equal with the symbol
  tiebreak for determinism. The margin therefore lives in the discovery /
  popularity tail; a position or watchlist name is only the margin if sources 1–4
  alone overflow the ceiling — a config-sizing failure, not an operating state. On
  a stream-band forced eviction the total order takes a stream-only tail name first,
  so no options baseline is destroyed.
- **Per-cycle membership snapshot:** each S008 cycle runs against the curated
  membership as of its start — the manager's contract with the scanner's
  deterministic-order cycle model.

### D11 — Runtime: ingest component, pure-function recompute, tombstones as the PIT record *(closed 2026-07-17)*

- **Placement:** `UniverseManager` is a **component inside `ingest`**, not a service.
  The streamer and the scan train are its consumers in the same process — no IPC, no
  new deployment unit.
- **Desired-set computation is a pure function of Postgres.** Every input is DB-backed
  by prior decisions: priority sources 1–5 (D8), discovery ranks persisted nightly
  (D9), stickiness protection derived from alert + verdict state (D8), symbol master
  and optionability in `symbols` (D1/D5). ADR-0018 warm-up step 1 is therefore
  literally *run the recompute, subscribe the result* — no membership checkpointing.
  Because the forced-eviction total order is deterministic (D10), a restart under
  ceiling pressure converges to the same membership a running process would hold.
- **Diff → ADD/UNSUB:** the manager holds the current subscription set in memory (a
  projection — ADR-0018), diffs desired vs current, issues ADD/UNSUB to the streamer,
  and treats rejections per D7. The curated list hands off to the scanner as the
  per-cycle snapshot (D10); the scanner never reads priority sources itself.
- **No band-membership-history table.** S012's per-cycle scan tombstones are the
  point-in-time curated-band record (ADR-0020 selection-bias note): finer-grained
  than any membership log and written by the scanner as ground truth (what actually
  ran), not by the manager (what was intended). Membership changes emit structured
  JSON log lines for debugging, not rows. A stream-band PIT record has no research
  consumer today — a consumer appearing is the trigger to revisit, not machinery to
  pre-build.

## Consequences

- **The universe is fully derived, never checkpointed.** Listing spans + the
  optionability union + DB-backed priority sources + persisted discovery ranks make
  both bands a pure recompute from Postgres (D11) — restart-safe by construction,
  and deterministic even under forced eviction (D10). S010's warm-up story is one
  sentence: recompute, subscribe.
- **Detection economics drive every stability rule.** Baselines are forward-only
  with a minimum-history gate (ADR-0032 D3), so churn is a detection cost, not a
  cosmetic one. The invariant curated ⊆ streamed (D7), verdict-conditioned
  stickiness (D8), discovery hysteresis with no rotation (D9), and
  session-boundary-only evictions with reserved intraday headroom (D10) all exist
  to protect matured baselines.
- **One new live dependency.** Cboe's options symbol directory becomes the fourth
  live source (ADR-0025 amended) — accepted because the two-directory union deletes
  more machinery (Schwab bootstrap probe, re-probe scheduler, demotion heuristics)
  than it adds (D5).
- **Sequencing ripple:** the symbol-master increment is S010's first deliverable and
  gates S009's roster (D6) — phase2-plan amended.
- **Accepted risks, recorded:** recycled-ticker seed contamination on relists (D2);
  historical delistings absent until a vendor dataset justifies backfill (D3);
  discovery's rich-get-richer margin — a name's first hot day happens off-band
  (D9); no stream-band PIT record until a research consumer appears (D11). In
  Phase 2, bands run on discovery + popularity alone until Phase-3 sources exist
  (D8) — by design, not degeneracy.

## Change log

- 2026-07-16 — session opened; skeleton committed.
- 2026-07-16 — D1 symbol master (nightly file diff → listing-span rows; mass-delist
  guard) · D2 lifecycle (rename = delist + add; ADD → deep-seed hook for S011;
  recycled-ticker risk documented) · D3 historical delistings out of scope (vendor
  trigger named). Live probe evidence: META history continuous to 2012 under the
  new ticker; FB recycled ~mid-2025.
- 2026-07-16 — D4 instrument filter (common+ADR+ETF policy; financial status stored
  not filtered; `instrument_class` computed at ingest, filtered at read).
- 2026-07-16 — D5 optionability closed after live source research: two-directory
  union (Nasdaq `options.txt` + Cboe CSV; neither is a superset — 4,185/5,319/4,020
  shared), OCC disqualified (bot wall), Schwab-empirical machinery not built
  (documented fallback), ADR-0025 amended (fourth source). D6 sequencing: symbol-
  master increment precedes S009 (phase2-plan amended).
- 2026-07-16 — D7 bands: curated ⊆ streamed as a structural invariant (ADR-0019
  amended), two-layer band structure, config ceilings with ADD-rejection-as-signal,
  sizes glossary pinned.
- 2026-07-16 — D8 priority sources: per-source set definitions, empty-source seam
  (Phase-3 sources return empty), verdict-conditioned stickiness for alerted
  subjects (flat window rejected — spend follows evidence).
- 2026-07-16 — D9 discovery: chain-premium-volume median rank (M=20 working),
  equity-dollar-volume bootstrap, rank hysteresis, explicit no-rotation rule;
  alternatives and the rich-get-richer weakness recorded.

- 2026-07-17 — D10 timing: nightly recompute fills to target (ceiling gap = reserved
  intraday-add headroom), intraday reconcile additive/protective only, evictions at
  session boundary except forced (headroom exhausted / streamer ADD rejection),
  eviction total order (source, rank, symbol) skipping protected members, per-cycle
  membership snapshot.
- 2026-07-17 — D11 runtime: ingest-internal component, desired sets as a pure
  function of Postgres (warm-up = recompute + subscribe), diff → ADD/UNSUB with
  in-memory subscription projection, scan tombstones accepted as the PIT membership
  record (no membership-history table).

## See also

- [ADR-0019: Two-band universe](./adr/0019-two-band-universe-and-priority.md)
- [ADR-0025: Live source set](./adr/0025-live-source-set-schwab-apewisdom-nasdaq.md)
- [ADR-0020: Three datasets](./adr/0020-three-datasets-and-backtest-constraints.md)
- [ADR-0032: Options-flow detection model](./adr/0032-options-flow-detection-model.md)
- [phase2-plan.md](./phase2-plan.md) §2
