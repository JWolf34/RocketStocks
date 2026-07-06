# Schwab REST budget — Phase-0 pre-flight

> Output of the roadmap pre-flight "Schwab API budget check" (rules: ADR-0020/0024).
> Drafted 2026-07-06; **closed 2026-07-06 post-close.** The burst probe did not find a
> ceiling: 120 requests ramped to 4 req/s (240/min pace) — all 200, no 429, no
> `Retry-After`, no rate-limit headers; the day-long chain collector adds 900 requests,
> zero 429s. Measured result is a **lower bound ≥ 240/min**; planning stays on the
> **documented 120/min** as the conservative L. Evidence:
> `data/spike-recordings/burst-probe-2026-07-06.jsonl` + findings in
> [specs/fixtures/streaming/findings-2026-07-06.md](./specs/fixtures/streaming/findings-2026-07-06.md).

## Named assumptions (consumer profiles)

| Consumer | Profile (assumption, not measurement) |
|---|---|
| Curated 5-min options scan | 1 chain call per underlying per 5-min cycle; **bounded params for index ETFs** (unfiltered SPY/QQQ chains 502 — measured 2026-07-06; ceiling > ~6 MB/~5k contracts) |
| Equity intraday | 0 REST during RTH — bars come from the L1 stream (ADR-0021) |
| Nightly 1-min backfill | ~10,000 requests (1/listed symbol), off-hours, sequenced |
| Options EOD full scan | ~5,000–6,000 requests (1 chain/optionable underlying), off-hours, sequenced |
| Daily-bar job | ~10,000 requests worst case (1/symbol; batching TBD in its slice), off-hours |
| Reserve (the 20%) | warm-up recovery, retries, bot interactive read-through via `serve` |
| Cowork | excluded from RTH budgeting — off-hours/DB-mostly profile enforced by ADR-0012 per-consumer limits |
| v1 during burn-in | RTH ≈ negligible (hourly enrich tick + interactive commands); nightly: daily/5m updates 03:00–04:00 CT, chain history 21:30 CT (from `legacy` scheduler crons, read 2026-07-06) |

## RTH steady state (v2 alone)

Only the curated scan spends REST during RTH. With curated size **N**:
rate = N / 5 per minute. Headroom rule (roadmap): rate ≤ 80% of the measured limit **L**.

| L (req/min) | Max N (= 4·L) | Note |
|---|---|---|
| 120 (documented) | 480 | upper bound if the probe confirms the docs |
| 100 (ADR-0020 working figure) | 400 | matches the ADR's sanity point |
| **measured: no ceiling ≤ 240 (probe clean at 4 req/s)** | **480 (planning L = 120 documented)** | **final curated-universe ceiling; ADR-0019 priority/eviction sizes the actual list. Probe proves ≥ 2× headroom above the planning number** |

## Nightly (sequenced, off-hours)

~10k (1-min backfill) + ~5–6k (options EOD) + ≤10k (daily job) ≈ **≤ 26k requests**.
At L=100/min run flat-out sequenced: ≈ 4.3h — fits the 20:00→03:00 CT window even
avoiding v1's 21:30 and 03:00–04:00 slots during burn-in. At the planning L=120/min:
≈ 3.6h — fits with more slack (and the probe suggests real capacity is higher still).
One addition from the day-long collector: **39/900 bounded chain requests returned
sporadic 502s** — nightly trains need a retry-once-then-tombstone policy (Phase-2
scan-slice spec concern, noted here so the budget's request counts include retries).

## Burn-in coexistence

Shared account, shared limit. v1's RTH load is ~0 (see profile row), so v2's 80% rule
already leaves v1's interactive use inside the reserve. Nightly: schedule v2's train to
avoid 21:30 CT (v1 chain history) and 03:00–04:00 CT (v1 bar updates). **Verdict: fits,
by sequencing alone — no cadence changes needed.** Confirmed at the measured bound:
v1 + the collector + the probe shared the account all day with zero 429s.

## Measured price-history depth (probed 2026-07-06, SPY)

| Frequency | ADR-0022 working figure | **Measured** | Implication |
|---|---|---|---|
| 1-min | ~7 days | **~46 days** (cap hit asking 60d and 120d) | G2 recovery window is ~6 weeks, not 7 days — ADR-0022 update candidate (favorable) |
| 5-min | ~9 months | **~207 days (~7 months)** | slightly shallower than assumed; historical 5m seeding already dropped (ADR-0022), so no plan impact |
| daily | years | **~30 years** (1996→) | confirmed |

## Verdict (pre-flight box)

Arithmetic closes at any plausible L ≥ 100/min: curated ~400–480 underlyings at 5-min
cadence inside 80% headroom, nightly train fits its window with v1 coexistence handled
by sequencing. **Closed 2026-07-06: the probe found no ceiling at 2× the documented
limit, so every row above holds with margin at the conservative planning L = 120/min.
Revisit only if production ever observes a 429 (log + alarm it when it happens).**
