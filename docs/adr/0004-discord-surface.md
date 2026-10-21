# Discord surface: three commands, mobile-first, Alerts are never edited

Discord is the main surface, mostly used on phones, in one private server per installation.
We decided the surface is small and fixed in shape.

- **Commands:** `/report TICKER` (the Report on demand), `/trade` (paper trading: `buy` and `sell`) and `/setup` (channels, roles, and Schwab sign-in).
  Slash commands plus interactive components (buttons, selects, modals); no `!` prefix.
  Discord allows three levels of nesting (command, group, subcommand), so each `/setup` area is a group with add, remove and show.
- **Mobile-first:** embed widths, fields per embed and buttons per row are designed for a phone screen first.
- **Alerts are never edited** after posting.
  A later Trigger on the same stock, or any other follow-up, is a reply in the Alert's thread.
- **One rendering layer:** everything shown in Discord passes through it, so a stricter display posture (for example around vendor data) is one switch rather than a hunt through call sites.
- **Ops pings** go through a single notify path to an operator webhook, not through channel routing in `/setup`.

## Consequences

- Admin configuration is discoverable under one command, and the global command list stays short.
- Component handling (views, persistent custom IDs across restarts) is real complexity, accepted for the UX.
- Because Alerts are immutable, what was posted is what was said at that time; history is never rewritten.

## Considered options

- Slash commands only: loses inline affordances such as the Schwab sign-in modal.
- Adding a `!` prefix: doubles the input surface for no benefit.
- Editing an Alert as new facts arrive: rejected, readers cannot tell what changed or when.
