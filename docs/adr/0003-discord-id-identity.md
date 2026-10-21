# Users are identified by their Discord ID

State that belongs to a person (paper trades, settings, role subscriptions) needs a key.
The only surface that has users is Discord, and every interaction already carries the Discord user ID.
We decided that user-keyed tables store the Discord user ID directly (`discord_user_id BIGINT NOT NULL`), and there is no `users` table.

## Consequences

- No lookup or join on the bot's hot path, and no account model to maintain.
- A future non-Discord surface (for example a web UI) would need a retrofit: add a `users` table, populate it from the distinct Discord IDs, and swap the keys.
  That cost is bounded and is not paid in advance.

## Considered options

- A first-class `users` table: forward-compatible with a web UI that does not exist, at the price of an extra concept on every interaction.
