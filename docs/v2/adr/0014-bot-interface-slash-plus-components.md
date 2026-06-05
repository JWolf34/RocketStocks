# ADR-0014: Bot interface — slash commands + interactive components, mobile-first

**Status**: Accepted (**amended** 2026-06-05)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05
**Decision-maker**: John Wolf

> **✎ AMENDED 2026-06-05** (ledger 'Auto-trader scope cut'; ADR-0009/0027). The **`/trade bot ...` command group is removed** — the auto-trader is cut from v2. `/trade` keeps only the manual **`buy`/`sell`** subcommands (manual paper trading, v1 parity). A post-v2 auto-trader service will reintroduce its own command surface.

## Context

v2's bot is the primary user surface, so its UX matters more than any other presentation choice. Options: slash-commands-only (simplest, but loses inline affordances), slash + interactive components (modern Discord standard), or slash + components + legacy `!` prefix (migration ramp, but doubles input surface and clashes with the clean-slate rewrite). The user also directed that **v1 designs must not anchor v2 designs** and that the surface be **mobile-friendly**.

## Decision

**Slash commands + interactive components (buttons, selects, modals). No `!` prefix.**

- Slash commands are the entry points; buttons/selects/modals provide follow-up flows (e.g. watchlist-edit modals, channel-select menus, position buttons on a manual `/portfolio` view).
- **Mobile-first layout** — embed widths, buttons-per-row, and field counts are treated as mobile constraints, not desktop-first.
- **Do not let v1 designs influence v2 designs.** Reason about the best modern Discord experience from scratch; cite v1 only to note a deliberate deviation.

Two command umbrellas, both within Discord's 3-level nesting limit (`command → group → subcommand`):
- **`/trade`** — manual paper trading: `buy`/`sell` subcommands. *(Amended 2026-06-05: the `bot` subcommand group is removed with the auto-trader — ADR-0009/0027.)*
- **`/setup`** — all admin config: `notifications`, `channels`, `roles` subcommand groups + a `schwab` (re-auth) subcommand. See ADR-0016.

## Consequences

- The affordances that make a bot better than a CLI (inline action buttons, modal forms) are available.
- A single `/setup` entry point makes admin config discoverable and keeps the global command list short.
- Mobile-first ensures the majority-mobile Discord audience isn't served desktop-shaped embeds.
- Cost: component handling (View classes, persistent `custom_id` routing across restarts) is real complexity; accepted for the UX win.
- Cost: every config area under `/setup` is locked to the `group → add/remove/show` shape by the 3-level limit (no deeper nesting).

## See also

- [architecture-brief.md § Discord surface](../architecture-brief.md#discord-surface)
- [ADR-0009: Paper trading + auto-trader](./0009-paper-trading-and-auto-trader-scope.md)
- [ADR-0016: Notifications layer](./0016-notifications-layer-db-routing.md)
- [feedback_v2_design_philosophy memory] — mobile-first, no v1 influence
