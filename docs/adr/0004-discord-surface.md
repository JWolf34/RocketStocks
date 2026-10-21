# Discord surface: four commands, mobile-first, Alerts are never edited

Discord is the main surface, mostly used on phones, in one private server per installation.
We decided the surface is small and fixed in shape.

- **Commands:** `/report TICKER` (the Report on demand), `/trade` (paper trading: `buy` and `sell`), `/pings` (a member's private menu of the Alert roles to be pinged for) and `/setup` (admin only: channels, roles, and Schwab sign-in).
  Slash commands plus interactive components (buttons, selects, modals); no `!` prefix.
  Discord allows three levels of nesting (command, group, subcommand), so each `/setup` area is a group with add, remove and show.
- **Roles are opted into, not assigned:** `/setup roles` maps a Trigger to an existing server role, and `/pings` lets each member add or remove those roles for themselves.
  The bot makes the change with its Manage Roles permission, so a member needs no permission, and the bot's role must sit above every role it manages.
  An Alert mentions the role of its Trigger and no one else; a reply never pings.
- **`/setup` is hidden from members** through Discord's default member permissions (Administrator).
  `/pings` is for every member.
- **Mobile-first:** embed widths, fields per embed and buttons per row are designed for a phone screen first.
- **Alerts are never edited** after posting.
  A later Trigger on the same stock, or any other follow-up, is a reply in the Alert's thread.
- **One rendering layer:** everything shown in Discord passes through it, so a stricter display posture (for example around vendor data) is one switch rather than a hunt through call sites.
- **Ops pings** go through a single notify path to an operator webhook, not through channel routing in `/setup`.

## Consequences

- Admin configuration is discoverable under one command, and the global command list stays short.
- Component handling (views, persistent custom IDs across restarts) is real complexity, accepted for the UX.
  The `/pings` menu is ephemeral and short-lived; a member who finds it stale runs `/pings` again.
- Because Alerts are immutable, what was posted is what was said at that time; history is never rewritten.
- A role mapping can go stale in Discord (a role deleted, or moved above the bot).
  `/setup roles add` refuses a role the bot cannot manage, `/pings` names the roles it could not change, and an Alert whose role is gone posts without the mention and sends an Ops ping.

## Considered options

- Slash commands only: loses inline affordances such as the Schwab sign-in modal.
- Adding a `!` prefix: doubles the input surface for no benefit.
- Editing an Alert as new facts arrive: rejected, readers cannot tell what changed or when.
- Members assigning themselves roles through Discord's own onboarding or reaction roles: needs setup per server and gives the bot nothing to check, where `/pings` shows each member's state and names what failed.
