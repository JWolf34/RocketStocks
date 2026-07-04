# ADR-0031: Backup & disaster recovery — operator-level VM snapshots, restore-rehearsed

**Status**: Accepted
**Date locked**: 2026-07-03
**Decision-maker**: John Wolf

## Context

Every reliability mechanism in the plan — replay (ADR-0023), warm-up (ADR-0018), reconcile (ADR-0005), the burn-in gate (ADR-0028) — protects against *software* failure. None protects against the box: a dead disk, a corrupted filesystem, an accidental volume delete. The options-chain snapshots are the one asset that is permanently unreconstructable (forward-only, no backfill at any price — ADR-0020); post-cutover, losing the Postgres volume without a backup erases the product's core dataset.

Scale check: single operator, one box, no HA requirement. RTO of hours-to-a-day is acceptable (v1-style downtime). **RPO is the real dial**, because the data is forward-only: a restore loses everything since the last backup, permanently.

## Decision

**Backup is operator infrastructure, decoupled from the repo.** The mechanism is the operator's existing **daily + weekly block-level VM disk snapshots**. No in-repo backup tooling (pgBackRest, WAL archiving, dump scripts) ships in v2 — that is a **post-v2 consideration** behind the triggers below.

Why this is sound for Postgres: a block-level snapshot of a running instance is **crash-consistent** — restoring it is equivalent to a power pull, which Postgres recovers from by WAL replay at startup. (If the UNLOGGED cache is reintroduced — ADR-0003 as amended — it is truncated during that recovery *by design*; warm-up rebuilds it — ADR-0018.) The detection log and research history are logged tables and recover normally.

**Three conditions make "suffice" true** (verified as a Phase-0 pre-flight item):

1. **Crash-consistent snapshots** — block-level, point-in-time, atomic across every virtual disk backing Postgres. A file-level copy/rsync of a live data directory is *not* acceptable (torn files, unrestorable).
2. **Off-host storage** — snapshots must survive the physical machine, not just the VM.
3. **A rehearsed restore** — burn-in rehearsal #4 restores the latest snapshot to a scratch VM, starts the stack, and runs the G2 gap audit against it (gate criterion G9). An untested backup is a hypothesis.

**Accepted RPO: ≤ 24 hours.** A box-loss event forfeits up to one trading day of options snapshots — permanently. This is the same loss class and comparable rarity as a tombstoned backfill gap (ADR-0022), already an accepted, audited outcome.

**Post-restore recovery procedure** (documented with rehearsal #4): re-run the 1-min equity backfill for the lost window (the 7-day window covers a ≤24h restore comfortably — ADR-0022); the lost options snapshots are recorded as an audited hole; the bot's sweep and warm-up handle alert and detector state normally.

## Consequences

- Zero new components during the window where unattended reliability is being proven; the backup burden rides infrastructure the operator already runs and trusts.
- The repo stays portable for self-hosters — backup strategy is explicitly the operator's concern, not baked into compose.
- Cost: no point-in-time restore (an oops-class mistake noticed same-day still costs up to a day), and restore granularity is the whole VM.
- In-repo WAL archiving layers on later with zero application changes — the deferral is reversible in the cheapest way.

## Trigger conditions for revisit (→ in-repo WAL archiving, e.g. pgBackRest — post-v2)

- A restore is needed in anger and the ≤24h loss actually hurts.
- A consumer (Cowork) starts depending on snapshot *completeness* as a contract.
- The dataset outgrows the VM-snapshot restore time the operator will tolerate.

## See also

- [ADR-0003: Postgres-only + Timescale + cache](./0003-postgres-only-timescale-cache.md)
- [ADR-0018: Temporal model + warm-up](./0018-temporal-hot-cold-model-and-warmup.md) (state recovery on restart)
- [ADR-0020](./0020-three-datasets-and-backtest-constraints.md) / [ADR-0022](./0022-bar-retention-storage-compression.md) (what is and isn't reconstructable)
- [roadmap.md](../roadmap.md) — pre-flight condition check, rehearsal #4, gate G9
