# ADR-0032: The options-flow detection model

**Status**: **Draft — design session in progress** (opened 2026-07-16)
**Decision-maker**: John Wolf

## Context

ADR-0030 ships exactly one v2 alert class — the options-flow alert — and defers its
detection model to a just-in-time design session at Phase-2 open, conducted against
real payloads. This ADR is that session's record; it closes before any detector slice
(S015–S017) starts and supplies the S008 spec's capture width + per-contract field
schema ([phase2-plan.md](../phase2-plan.md) §1, amended 2026-07-16).

Evidence base: the chain-spike delta findings
([findings-2026-07-06.md](../specs/fixtures/chains/findings-2026-07-06.md)), the
day-over-day OI fixture
([oi-change-2026-07-09-to-2026-07-10.md](../specs/fixtures/chains/oi-change-2026-07-09-to-2026-07-10.md)),
the scan-cycle reality (~4-min rolling call train; per-row `fetched_at`/`quote_time`
event-time rule), and the REST budget ([schwab-rest-budget.md](../schwab-rest-budget.md)).

What the 5-min curated snapshot can and cannot say (empirical constraints the model
is built on): per-contract cumulative day volume (→ 5-min deltas), bid/ask/mark, IV,
greeks, and OI that is **static intraday** — no time-and-sales, no aggressor side, no
sweep/block detection. "Someone just bought X" is outside this dataset; "abnormal
activity is accumulating" is inside it.

## Decision

### D0 — Working frame *(accepted 2026-07-16)*

**The 5-min alert reports a burst: a concentrated, abnormal run of opening-consistent
options volume on one side of an underlying's near-dated chain, caught while it is
happening.** Day-2 settled OI is the lifecycle's second act — the nightly diff
confirms the burst (OI built where the burst was) or denies it — not a separate alert
class. Intraday flow leads; OI confirms one session later (the PIT lag the fixtures
document).

The burst has five components, each to be pinned as its own decision below:

1. **Abnormal** — volume deltas large relative to the subject's own baseline.
2. **Concentrated** — the burst sits in a few strikes/expiries, not smeared across
   the chain.
3. **Opening-consistent** — high volume/OI ratio on the burst contracts (the intraday
   proxy for new positioning; only the day-2 OI-delta sign proves build vs unwind).
4. **Material** — a premium floor (delta-volume × mark × 100), filtering
   teenie-volume noise.
5. **Context-graded** — underlying price action grades the burst (flat tape →
   anticipatory; chasing a move → reactive/hedging). Whether this is a firing input
   is the equity-bar-fusion decision (open).

### D1 — Alert subject: (underlying, side) *(closed 2026-07-16)*

One alert is about a **subject = (underlying, call-side | put-side)**. The subject is
the grain for dedup/cooldown, tier escalation, parent linkage of re-posts, and
per-subject warm-up state (ADR-0018).

- *Side, not direction:* at REST cadence there is no aggressor side, so call volume
  ≠ bullish. Bullish/bearish interpretation defers to day-2 OI build/unwind.
- *Why not per-contract:* one event fires many contracts (NVDA 7/10: five call
  strikes built +15k–19k each — one event); contract grain multi-posts it and
  explodes lifecycle state.
- *Why not underlying-only:* a genuine put-side event during a call-side cooldown
  must stay alertable.
- The burst's contract set is carried as **evidence** inside the alert (rendered per
  ADR-0026); warm-up state stays ≤2 subjects per name.

### D2 — Equity-bar fusion: fuse, as a firing input *(closed 2026-07-16)*

**The detector consumes `BarClosed` equity context and it grades firing**: the same
burst against a flat tape grades up (anticipatory positioning — the differentiating
case), against a moving tape grades down or requires higher thresholds (chasing/
hedging — the NVDA 7/10 call build was exactly this). Feasible because ADR-0019 pins
curated ⊆ streamed, so every scanned name has live bars.

Consequences:

- **Canary disposition (phase2-plan §4): the S003 threshold detector demotes to a
  test fixture.** The options-flow detector itself keeps the streaming→detection seam
  production-hot — the seam is now load-bearing for the real alert class.
- The detector merges two event streams with different timing (5-min wall-boundary
  bars vs the rolling event-timed scan train); the replay harness and warm-up handle
  the merged timeline.

### D3 — Baseline: per-subject, time-of-day-bucketed, robust *(closed 2026-07-16)*

**"Abnormal" is measured against the subject's own trailing-N-day distribution of
5-min volume deltas, bucketed by time of day, with a robust center/spread
(median/MAD).** A burst component is abnormal when its delta exceeds a config
multiple of the bucket baseline.

- **The ADR pins the shape only.** Trailing-window length, bucket width, and
  multiples are per-detector **config** (config-version-stamped per ADR-0030), tuned
  via replay fixtures and burn-in — threshold changes are config eras, not ADR
  amendments.
- **Minimum-history gate:** the dataset is forward-only, so a subject is not
  alertable until its baseline has a config-minimum number of sessions; at go-live
  the detector ramps up silently. Burn-in (3–4 market weeks) absorbs the ramp.
- Warm-up (ADR-0018) rebuilds baselines by replaying recent `options_chain_snapshot`
  rows — the baseline must be a pure function of stored rows (it is: deltas of
  cumulative volume, stale-guarded).

### D4 — Escalation ladder: cumulative per session; dedup is structural *(closed 2026-07-16)*

**Tiers are config thresholds on the subject's accumulated abnormal flow across the
session** (working statistic: cumulative excess premium vs baseline expectation —
exact form pinned in the detector spec, thresholds config). T1 posts when a burst
first qualifies under the D0 components; T2/T3 post as the session's flow keeps
building, each parent-linked per ADR-0026 (messages are never edited).

- **The ladder is monotonic, so intraday dedup is structural:** a subject posts at
  most once per tier per session, tiers only ascend, and no cooldown clocks exist.
  The roadmap's "cooldown windows" input is resolved as *not needed intraday*.
- The ladder resets at session close; the day-2 OI lifecycle (open below) is the
  cross-session act.
- Per-subject lifecycle state = (session, highest tier posted, running accumulator).

### D5 — "Tier" means escalation only; the snapshot discriminator is `dataset` *(closed 2026-07-16)*

The `options_chain_snapshot` discriminator column is named **`dataset`** (values
unchanged: `curated_5m` / `eod_full`) — ADR-0020 amended editorially; S008's
migration uses `dataset` from day one. **Tier** exclusively means the escalation
rung on alerts. Resolves the collision before both migrations land.

### D6 — Day-2 OI verdict: confirm and deny both post *(closed 2026-07-16)*

The nightly EOD diff (S009, shared-roster netting) delivers each alerted subject's
**verdict**: **confirmation** (OI built where the burst was — with build/unwind
evidence) or **denial** (OI flat/unwound — the volume was churn). **Both post**,
parent-linked to the subject's tier chain (ADR-0026: never edited), and the verdict
is written to the alert row for per-config-era hit-rate accounting.

- PIT rule: a day-D burst's verdict comes from the first EOD snapshot whose settled
  OI reflects day D's trading (empirically the next session's — the fixture's lag);
  exact scheduling pinned in the detector spec.
- "Awaiting verdict" is derived from alert rows (previous session's alerts without a
  verdict child) — no unrebuildable in-memory state.

### D7 — Curated capture width: moneyness window, not strike count *(closed 2026-07-16)*

**The curated scan captures strikes within a config moneyness window of spot
(working value ±10%), ≤30 DTE**, with a per-name row cap (nearest-expiry-first) for
dense-strike daily-expiry ETFs (bounded params are mandatory there anyway — the
measured 502 ceiling). Amends ADR-0020's ATM±5: strike-count units are incoherent
across names ($1-spacing names got <±1% moneyness — outside the very OTM band the
burst thesis targets), while the dataset's intent is unchanged.

- Shape in the ADR, value in config (D3's altitude rule): the window can widen if
  replay shows misses, without amendment.
- Width changes payload/storage only, never request count (1 chain call/underlying).

### D8 — Snapshot row schema: broad typed row, no payload blob *(closed 2026-07-16)*

`options_chain_snapshot` rows are **broad typed columns (~25), no raw-JSON payload
column**, serving both datasets (`curated_5m` and S009's `eod_full`) and future
research — the dataset is forward-only, so a field not captured is gone for every
past day. Field groups (exact columns bound in the S008 spec against the captured
excerpt fixture):

- **identity** — underlying, expiry, strike, put/call
- **provenance** — `dataset`, `cycle_id`, `fetched_at`, `quote_time`,
  `source_status`
- **quote** — bid/ask/last/mark + sizes
- **flow** — volume (cumulative day), open interest
- **risk** — IV + greeks
- **chain context** — underlying price at fetch

The detector consumes a subset; extra numeric columns are cheap under columnar
compression, a JSONB blob at ~5–8M rows/day is not.

### D9 — Warm-up state for S015 *(derived from D1–D6)*

Everything the detector needs at restart is a **pure function of existing durable
tables** — warm-up (ADR-0018) rebuilds, in order, before detectors enable:

1. **Baselines** (D3): trailing-N-session time-of-day-bucketed delta distributions
   per subject — replayed from `options_chain_snapshot` curated rows,
   stale-guarded.
2. **Current-session ladder state** (D4): per-subject running accumulator replayed
   from today's rows; highest-tier-posted read from alert rows.
3. **Pending verdicts** (D6): prior-session alerts without a verdict child — a
   query over alert rows.
4. **Equity context windows** (D2): recent bars for curated names — the existing
   bar-replay path.

No new durable store is introduced; ADR-0018's "load dedup/last-tier lifecycle
state" step is satisfied by (2)+(3).

### D10 — Alert-taxonomy fields for S016 *(derived from D1, D4, D6; ADR-0030)*

Migration input for the generic alert taxonomy (exact DDL in the S016 spec):

- `detector` — alert-type/detector id (the generic taxonomy key)
- `subject` — detector-defined subject key (here: underlying + side)
- `tier` — escalation rung (D4)
- `parent_alert_id` — nullable self-FK; tier re-posts and verdict posts link to
  their parent (ADR-0026)
- `config_version` — the detector-config era stamp (ADR-0030)
- `session` — the market session the alert belongs to (D4 reset grain, D6 verdict
  lookup)
- `verdict` — nullable confirm/deny outcome written by the day-2 pass (D6)
- `evidence` — the burst's contract set + firing statistics, sufficient for the
  bot to render the embed **from the alert row alone** (ADR-0024 as amended)

## Consequences

- One event → one alert per (underlying, side); multi-strike bursts do not
  multi-post; intraday re-fire suppression is structural (monotonic ladder), with
  no cooldown clocks to configure or warm up.
- The streaming→detection seam is load-bearing for the real alert class (D2); the
  S003 canary demotes to a test fixture — phase2-plan §4's conditional resolves.
- Detection is 100% replayable by construction: every input (snapshot rows, bars,
  alert rows) is durable and PIT; every tunable is config, version-stamped.
- Go-live has a silent ramp (D3's minimum-history gate) — absorbed by burn-in;
  the detector's first alerts arrive only after baselines mature.
- ADR-0020 amended twice (D5 naming, D7 width); S008 is unblocked (D7 width + D8
  schema), S015 (D9) and S016 (D10) have their design inputs.
- Risk: cumulative-ladder entry (D4) may post the first alert later than an
  instantaneous trigger would; if replay fixtures show T1 arriving too slow, the
  hybrid entry (instantaneous T1, cumulative escalation) is the named fallback.

## Change log

- 2026-07-16 — session opened; D0 frame + D1 subject grain recorded; D2 fusion
  closed (canary demotes to test fixture); D3 baseline shape closed; D4 cumulative
  ladder closed (no intraday cooldown clocks); D5 tier/dataset naming split closed;
  D6 day-2 verdict posts (confirm + deny) closed; D7 moneyness-window capture width
  closed (ADR-0020 amended); D8 broad typed row closed; D9 warm-up state + D10
  taxonomy fields derived. All agenda items resolved — pending review to Accepted.

## See also

- [ADR-0030: Detection re-derived](./0030-detection-rederived-single-alert-class.md)
- [ADR-0026: Alert embed + escalation lifecycle](./0026-charting-deprecation-and-reports.md)
- [ADR-0020: Three datasets](./0020-three-datasets-and-backtest-constraints.md) ·
  [ADR-0018: Warm-up](./0018-temporal-hot-cold-model-and-warmup.md)
- [phase2-plan.md §1](../phase2-plan.md) (session agenda + outputs)
