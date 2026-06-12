# Spec template

> Copy into `specs/SNNN-short-slug.md` (next free number). Keep it to roughly half a page — if a slice genuinely needs more (e.g., `UniverseManager`), write a full design note and link it in the header; the spec then only binds interface + acceptance. **Write for a reader with zero conversation history** — the spec plus its links must be enough to implement the slice. The spec is reviewed *before* implementation; the verification output is reviewed *after*. If reality disagrees mid-build, update the spec in the same PR.

```markdown
# SNNN — <title>

**Phase**: <0–5> · **Appetite**: <days> · **Depends on**: <SNNN, …> · **ADRs / notes**: <links>

## Goal

One or two sentences, stated as an outcome ("after this slice, X exists / Y is proven"), not as activity.

## Interface

What exists when this lands and other slices may rely on: modules + key types, DB tables/revisions, endpoints, events, CLI entry points. **Names are binding; internals are not.** The only constraints on internals allowed here are ADR-mandated ones, cited (e.g., "plain ordered handler list, not a pub/sub framework — ADR-0005").

## Non-goals

What this slice deliberately does not do (usually: the neighboring slices' jobs).

## Open questions

Anything the implementer must not guess — each stated with a recommendation. Resolved at spec review and folded into the sections above. Delete the section when empty.

## Acceptance criteria

- [ ] Objective, observable statements — no "works correctly"; say what is observed. The tests the slice ships are criteria too (replay fixtures for anything detection-shaped — ADR-0023).

## Verification

Exact commands (and fixtures) a reviewer or a fresh agent session runs to confirm the acceptance criteria. Prefer one command per criterion.

## Decisions surfaced

Empty at review time; filled during implementation. Anything architectural is promoted to an ADR; the rest is recorded here with a one-line rationale.
```
