# RocketStocks v2 foundation

This folder is the plan for building v2.
It is short on purpose: a charter, a one-page architecture, a roadmap and one half-page spec per slice.

## Reading order

1. [charter.md](./charter.md) - what RocketStocks is, who it is for, what ships and what does not.
2. [architecture.md](./architecture.md) - the current runtime shape, edited in place as slices land.
3. [roadmap.md](./roadmap.md) - the eight slices in order, with their done-signals.
4. [specs/](./specs/README.md) - one half-page spec per slice; start at the first unbuilt one ([01-attention-surge.md](./specs/01-attention-surge.md)).

## Where decisions live

- The [v2 foundation map](https://github.com/JWolf34/RocketStocks/issues/40) and its closed tickets hold every foundational decision and its rationale.
  The charting session record is [charting-2026-10-19.md](./charting-2026-10-19.md).
- Surviving ADRs stay in `docs/v2/adr/`.
  A new ADR is written only for a contested decision.
- The superseded plan is archived under git tag `v2-plan-archive-2026-10`.
  Archived ADR numbers resolve at that tag.

## Process rules

These come from [Build process and cutover](https://github.com/JWolf34/RocketStocks/issues/53).

- A half-page spec lands with the code, in the implementing pull request.
- A slice is done when CI is green (ruff, mandatory-DB tests, clock-discipline gate, type checker, `alembic check`, import-linter), a live run against real data is recorded, and the reliability rehearsals that apply have passed (Schwab token expiry, ingest restart, backup restore).
- Every alert type shows its measured base rate, and a directional Signal ships only after its event study passes.
- Exit is by observed events, never by elapsed time.
  There is no calendar soak.

## Background

- [research/](./research/) holds the summaries behind the vendor, evidence and Data API decisions.
- [audit/](./audit/) holds the inputs that triggered the re-plan, including the v2 code audit that the first slice reshapes against.
