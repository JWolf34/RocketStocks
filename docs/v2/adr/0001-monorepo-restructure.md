# ADR-0001: Monorepo restructure (`libs` / `services` / `tools` / `legacy`)

**Status**: Accepted (**amended** 2026-06-12)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-12
**Decision-maker**: John Wolf

> **✎ AMENDED 2026-06-12** (S001 spec review). Package naming + member layout locked: import name = **`rs_` + member dir name, no exceptions** (`rs_domain`, `rs_persistence`, `rs_observability` — never abbreviated); distributions dashed (`rs-domain`); **src-layout with per-member packaging** (`<root>/<member>/pyproject.toml` + `src/rs_<member>/` + `tests/`), so layer rules are enforced by each member's dependency closure rather than convention. Member renames vs. the draft list below: **`core` → `analysis`** (pure math — indicators, z-scores, flow metrics; "core" is a grab-bag name that also collides with v1's different meaning of it) and the source-adapters member is named **`sources`**. Repo-dev helpers live in `scripts/`, not `tools/`; top-level `tests/` is reserved for cross-service e2e/replay; `rs_common`/`rs_utils` grab-bags are banned.

## Context

v1 is a single installed package (`src/rocketstocks/`) with layered subpackages (`core`/`data`/`bot`/`api`/`backtest`/`eda`). v2 splits into multiple independently-deployable services plus shared libraries plus one-shot CLIs, all in the same git repo. We need a top-level layout that makes each unit's role obvious and keeps the v1 code addressable during the transition.

The JS/TS-monorepo convention is `packages/` (libraries) + `apps/` (deployables). The original handoff extended this to `packages/` + `services/` + `apps/` — but `services` and `apps` are near-synonyms (both "a deployable thing"), so nothing in the words conveys the intended daemon-vs-CLI split.

## Decision

Four top-level roots, each word mapping one-to-one to a category:

- **`libs/`** — shared libraries: `domain`, `persistence`, `observability`, `core` (analysis), `auto_trader`, source adapters. Imported, never run alone. *(Amended 2026-06-12: `core` → `analysis`; source adapters member named `sources`; `auto_trader` deferred with ADR-0027.)*
- **`services/`** — long-running daemons: `ingest`, `serve`, `bot`. Have `docker-compose` entries; share one `Dockerfile`.
- **`tools/`** — one-shot CLIs: `migrate`, `backfill`, `backtest`. Invoked, run, exit.
- **`legacy/`** — the old v1 `src/rocketstocks/`, moved here during the transition (move happens at Stage 2 start, not now).

The v2 rewrite lives on a long-lived **`feat/v2-rewrite`** trunk for its full duration (Phases 1–3): per-slice branches merge into the trunk, not `main`. The trunk merges to `dev` for the terminal burn-in (ADR-0028) and to `main` only at cutover — `main` keeps serving v1 untouched until then.

## Consequences

- Each directory name answers "what is this and how does it run?" without a doc lookup.
- Avoids the Python "packages of packages" redundancy (`package` already means an importable dir).
- Daemon-vs-CLI distinction is explicit, which matters: daemons get compose entries + the shared Dockerfile; CLIs don't.
- Cost: contributors from a JS-monorepo background expect `packages/`/`apps/` and must learn the local convention (documented here and in the brief).
- The long-lived branch risks drift from `main`; mitigated by merging the skeleton early and working per-component after.

## See also

- [architecture-brief.md § Persistence / Directory layout](../architecture-brief.md#directory-layout)
- [ADR-0002: Three-services topology](./0002-three-services-topology.md)
