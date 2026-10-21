# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Layout: single-context

This repo has **one** `CONTEXT.md` at the repo root. There is no `CONTEXT-MAP.md`.

**ADRs live at `docs/adr/`**, the default location.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root - the v2 ubiquitous-language glossary (product, data plane, alerting).
- **`docs/foundation/architecture.md`** - the current v2 architecture; rationale lives in the foundation map (issue #40) tickets.
- **`docs/adr/`** - read ADRs that touch the area you're about to work in.

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
    └── adr/                           ← architectural decisions
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids - the glossary's `_Avoid_` lines are binding (e.g. use **alert** for the informational push post and reserve **signal** for an edge-backed claim; **report** for the pull surface, **digest** for scheduled pushes).

If the concept you need isn't in the glossary yet, that's a signal - either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0002 (Triggers in ingest) - but worth reopening because…_
