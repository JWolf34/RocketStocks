# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Layout: single-context

This repo has **one** `CONTEXT.md` at the repo root. There is no `CONTEXT-MAP.md`.

**ADRs live at `docs/v2/adr/`** - NOT the default `docs/adr/`. This is a deliberate deviation: the surviving architectural decisions are for the v2 rewrite (the current architecture is `docs/foundation/architecture.md`; see `docs/v2/README.md`). When a skill's default instructions say "read `docs/adr/`", read `docs/v2/adr/` instead.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root - the v2 ubiquitous-language glossary (product, data plane, alerting).
- **`docs/foundation/architecture.md`** - the current v2 architecture; rationale lives in the foundation map (issue #40) tickets.
- **`docs/v2/adr/`** - read ADRs that touch the area you're about to work in. Only surviving ADRs are kept (numbers are not contiguous); archived ADRs resolve at git tag `v2-plan-archive-2026-10` via `git show v2-plan-archive-2026-10:docs/v2/adr/NNNN-*.md`.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure (this repo)

```
/
├── CONTEXT.md                         ← v2 ubiquitous language (glossary only)
└── docs/
    ├── foundation/
    │   ├── architecture.md            ← current v2 architecture (entry point)
    │   ├── research/                  ← vendor stack, signal evidence, API shape
    │   └── specs/                     ← per-slice half-page specs
    └── v2/
        ├── README.md                  ← pointer to the above
        └── adr/                       ← surviving architectural decisions (non-contiguous numbers)
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids - the glossary's `_Avoid_` lines are binding (e.g. use **alert** for the informational push post and reserve **signal** for an edge-backed claim; **report** for the pull surface, **digest** for scheduled pushes).

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0008 (detection in ingest) — but worth reopening because…_
