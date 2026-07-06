# Schwab REST budget — Phase-0 pre-flight

> Output of the roadmap pre-flight "Schwab API budget check" (rules: ADR-0020/0024).
> Drafted 2026-07-06. **Measured limit: PENDING** — burst probe runs post-close
> 2026-07-06; every ★ cell recomputes from it.

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
| **★ measured** | **★ 4 × measured** | **final curated-universe ceiling; ADR-0019 priority/eviction sizes the actual list** |

## Nightly (sequenced, off-hours)

~10k (1-min backfill) + ~5–6k (options EOD) + ≤10k (daily job) ≈ **≤ 26k requests**.
At L=100/min run flat-out sequenced: ≈ 4.3h — fits the 20:00→03:00 CT window even
avoiding v1's 21:30 and 03:00–04:00 slots during burn-in. ★ recompute duration at the
measured limit.

## Burn-in coexistence

Shared account, shared limit. v1's RTH load is ~0 (see profile row), so v2's 80% rule
already leaves v1's interactive use inside the reserve. Nightly: schedule v2's train to
avoid 21:30 CT (v1 chain history) and 03:00–04:00 CT (v1 bar updates). **Verdict: fits,
by sequencing alone — no cadence changes needed.** ★ confirm at the measured limit.

## Measured price-history depth (probed 2026-07-06, SPY)

| Frequency | ADR-0022 working figure | **Measured** | Implication |
|---|---|---|---|
| 1-min | ~7 days | **~46 days** (cap hit asking 60d and 120d) | G2 recovery window is ~6 weeks, not 7 days — ADR-0022 update candidate (favorable) |
| 5-min | ~9 months | **~207 days (~7 months)** | slightly shallower than assumed; historical 5m seeding already dropped (ADR-0022), so no plan impact |
| daily | years | **~30 years** (1996→) | confirmed |

## Verdict (pre-flight box)

Arithmetic closes at any plausible L ≥ 100/min: curated ~400–480 underlyings at 5-min
cadence inside 80% headroom, nightly train fits its window with v1 coexistence handled
by sequencing. **Box ticks when the ★ cells hold the measured number (burst probe,
post-close 2026-07-06).**
