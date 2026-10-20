# ADR-0014: Bot interface — slash commands + interactive components, mobile-first

**Status**: Accepted (**amended** 2026-06-05, 2026-10-19)
**Date locked**: 2026-05-27 · **Amended**: 2026-06-05 · 2026-10-19
**Decision-maker**: John Wolf

> **Amended 2026-10-19 (foundation re-plan, [#54](https://github.com/JWolf34/RocketStocks/issues/54)):** commands are Reports only ([#47](https://github.com/JWolf34/RocketStocks/issues/47)); `/setup` is limited to channels and roles with no notification routes ([#52](https://github.com/JWolf34/RocketStocks/issues/52)); the transparency and never-edit principles from archived ADR-0026 move here.

> **✎ AMENDED 2026-06-05** (ADR-0009/0027). The **`/trade bot ...` command group is removed** — the auto-trader is cut from v2. `/trade` keeps only the manual **`buy`/`sell`** subcommands (manual paper trading, v1 parity). A post-v2 auto-trader service will reintroduce its own command surface.

## Context

v2's bot is the primary user surface, so its UX matters more than any other presentation choice. Options: slash-commands-only (simplest, but loses inline affordances), slash + interactive components (modern Discord standard), or slash + components + legacy `!` prefix (migration ramp, but doubles input surface and clashes with the clean-slate rewrite). The user also directed that **v1 designs must not anchor v2 designs** and that the surface be **mobile-friendly**.

## Decision

**Slash commands + interactive components (buttons, selects, modals). No `!` prefix.**

- Slash commands are the entry points; buttons/selects/modals provide follow-up flows (e.g. watchlist-edit modals, channel-select menus, position buttons on a manual `/portfolio` view).
- **Mobile-first layout** — embed widths, buttons-per-row, and field counts are treated as mobile constraints, not desktop-first.
- **Do not let v1 designs influence v2 designs.** Reason about the best modern Discord experience from scratch; cite v1 only to note a deliberate deviation.

Command surface *(amended 2026-10-19)*, within Discord's 3-level nesting limit (`command → group → subcommand`):
- **Reports** - the commands are Reports, read-only views of stored data (#47).
- **`/trade`** - manual paper trading: `buy`/`sell` subcommands (ADR-0027).
- **`/setup`** - admin config limited to `channels` and `roles` subcommand groups plus a `schwab` (re-auth) subcommand. There are no notification routes; ops notices go through `notify_ops()` (architecture.md).

Principles carried over from archived ADR-0026 *(amended 2026-10-19)*:
- **Transparency** - an alert shows the raw numbers behind the signal (ratios and their inputs), never a black-box score alone.
- **Never edit** - alert messages are never edited after posting; a further escalation is a new alert row and a new message referencing its parent.

## Consequences

- The affordances that make a bot better than a CLI (inline action buttons, modal forms) are available.
- A single `/setup` entry point makes admin config discoverable and keeps the global command list short.
- Mobile-first ensures the majority-mobile Discord audience isn't served desktop-shaped embeds.
- Cost: component handling (View classes, persistent `custom_id` routing across restarts) is real complexity; accepted for the UX win.
- Cost: every config area under `/setup` is locked to the `group → add/remove/show` shape by the 3-level limit (no deeper nesting).

## Change log

- 2026-10-19 - amended (foundation re-plan): command surface and principles updated; see the note above.

## See also

- [architecture.md § Notifications and observability](../../foundation/architecture.md#notifications-and-observability)
- [ADR-0027: Manual paper trading only](./0027-manual-paper-trading-only.md)
