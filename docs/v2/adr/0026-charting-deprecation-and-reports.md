# ADR-0026: Charting deprecated; alert-presentation principles, buttons, and two report surfaces

**Status**: Accepted (amended 2026-07-03)
**Date locked**: 2026-06-05 · **Amended**: 2026-07-03
**Decision-maker**: John Wolf

## Context

v1 rendered chart images (mplfinance), a subsystem + dependency that created an earlier parity-gap flag. v2's Discord surface is mobile-first (ADR-0014). We must pin the alert presentation's structure and principles, its action buttons, and the report surfaces — without anchoring on v1, and without pre-committing field-level layout to a detection model that is designed later (ADR-0030's Phase-2 session).

## Decision

**Charting is deprecated in v2** — no mplfinance, no chart images. Alerts and reports are text/embed only (removes a subsystem + dependency).

**Alert-presentation principles (binding on any layout):**

- **Mobile-first** (ADR-0014) — embed width, buttons-per-row, and field counts are mobile constraints.
- **Transparency** — the embed shows the raw numbers behind the signal (ratios *and* their inputs), never a black-box score alone.
- **Information hierarchy** — a glanceable subset on the alert embed; full detail lives in the Options Report (e.g., abbreviated greeks on the alert, the full set in the report).
- **Escalation via re-posts** *(amended 2026-07-03; replaces OI-confirmation update-in-place)*: alert messages are **never edited**. The lifecycle is **threshold-escalation re-posts** — when a subject's criteria crosses a further threshold tier (the signal becomes more significantly linked to a potential price move), a **new alert row** is written and delivered on the ordinary Bus-2 rail, referencing its parent alert; lifecycle state (per-subject last tier, cooldowns) lives in Postgres and is warm-up-loaded (ADR-0018). Escalation triggers, tier definitions, and whether day-2 OI confirmation/denial participates (including downgrade posts) are Phase-2 design-session decisions. This is structural (schema + detection-lifecycle implications — the alert taxonomy carries a parent-linkage seam) and holds regardless of the detection model.

**Field-level embed layout is deliberately NOT pinned here.** The hero field, conviction framing, footer, and color semantics depend on what the detection model computes — they are bound in an **alert-presentation spec** written *after* the Phase-2 detection-model session closes, citing this ADR's principles as constraints and the detection-model ADR as its input.

**Buttons (4):** `💵 Trade` · `⭐ Watchlist` · `📄 Stock Report` · `📊 Options Report`. Trade = manual paper BUY of the **underlying** into the **clicker's** portfolio (`source=manual`, not an auto-trader — ADR-0027).

**Two report surfaces** (button + slash command + serve/Cowork exposure TBD):

- **Stock Report** — Schwab fundamentals (P/E, margins, rev growth, market cap) + price/technicals + earnings date. No chart.
- **Options Report** — top calls/puts with full greeks, IV, put/call ratio, OI, flow context (the detail tier of the information hierarchy).

Exact field sets, the "top-3" ranking basis (volume / premium / unusualness), the v2 screener definition, and serve exposure for Cowork are deferred to the Phase-3 report-design session (roadmap).

## Consequences

- One fewer subsystem/dependency; mobile-first embeds for a majority-mobile audience.
- The Phase-2 design session is unconstrained on *what* the alert says, bound only on *how* it must present (transparent, glanceable, mobile, escalation-by-re-post) — no silent divergence and no premature art direction.
- Dropping message edits simplifies the bot: no message-reference persistence for edits, no Discord edit-rate handling, no second Bus-2 work type — escalations are ordinary alert rows on the existing rail, replayable and config-stamped like any detector decision.
- Cost: users wanting a chart use their own tools — acceptable; charts weren't a differentiator.

## Change log

- 2026-06-05 — locked; originally included field-level embed layout (green calls / red puts, vol/OI hero field, premium-as-conviction line, IV+delta on the alert, detected-time footer).
- 2026-07-03 — amended: field-level layout moved out to a future **alert-presentation spec** (post-detection-model-session); this ADR now binds principles and structure only (decisions live in ADRs; layout specifics live in specs). The IV+delta-vs-full-greeks split is retained as an *instance* of the information-hierarchy principle, not a binding field list.
- 2026-07-03 — amended (alert lifecycle, final pre-implementation review): **OI-confirmation update-in-place is dropped** — replaced by **threshold-escalation re-posts** (new parent-linked alert rows on the ordinary Bus-2 rail; no message edits). Tiers, triggers, and day-2-OI participation (incl. downgrade posts) delegated to the Phase-2 detection design session. Decision change by John Wolf.

## See also

- [ADR-0014: Bot interface (amended)](./0014-bot-interface-slash-plus-components.md)
- [ADR-0025: Live data source set](./0025-live-source-set-schwab-apewisdom-nasdaq.md)
- [ADR-0027: Manual paper trading only](./0027-manual-paper-trading-only.md)
- [ADR-0030: Detection re-derived](./0030-detection-rederived-single-alert-class.md) (the Phase-2 session this defers layout to)
