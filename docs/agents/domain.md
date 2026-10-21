# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Layout: single-context

This repo has **one** `CONTEXT.md` at the repo root. There is no `CONTEXT-MAP.md`.

**ADRs live at `docs/adr/`**, the default location.

## Before exploring, read these

- **`docs/vision.md`** - the only authority for what RocketStocks is, its architecture and its milestones.
- **`CONTEXT.md`** at the repo root - the glossary.
- **`docs/adr/`** - read ADRs that touch the area you're about to work in.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure (this repo)

```
/
├── CONTEXT.md                         ← glossary only
└── docs/
    ├── vision.md                      ← the authority (entry point)
    ├── operations.md                  ← deploy, sign-in, rehearsals, backup and restore
    ├── reference/                     ← measured vendor facts
    └── adr/                           ← architectural decisions
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids - the glossary's `_Avoid_` lines are binding (e.g. **Trigger**, not signal or detector; **Popularity**, not attention; **Track record**, not base rate; **Ops ping**, not alert, for a message to the operator).

If the concept you need isn't in the glossary yet, that's a signal - either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0002 (Triggers in ingest) - but worth reopening because…_
