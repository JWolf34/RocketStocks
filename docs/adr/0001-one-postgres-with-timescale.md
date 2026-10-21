# One Postgres with Timescale, no Redis

RocketStocks runs on one home-lab VM with one operator, and its history (Popularity snapshots, options chain snapshots) cannot be fetched again.
Every extra stateful service is one more thing to deploy, back up and reason about.
We decided that Postgres with the Timescale extension is the only stateful infrastructure.

- Options chain snapshots, Popularity snapshots and daily bars are compressed hypertables.
  Hypertables and compression policies are created as raw DDL inside Alembic migrations; SQLAlchemy treats them as ordinary tables.
- Everything else (Alerts, Outcomes, Track records, Baselines, settings, roles, paper trades) is a plain table.
- Cross-service signalling uses Postgres LISTEN/NOTIFY (Alert delivery is its own decision); there is no Redis, no message broker and no cache layer.
- Backups cover one database, which keeps the "what to back up" answer short.

## Consequences

- NOTIFY has a small payload limit and no consumer groups or replay.
  That is acceptable because NOTIFY only wakes a consumer; the table is the record.
- Only history grows.
  Compression is what keeps the nightly chain sweep affordable on one disk.

## Considered options

- Postgres plus Redis for cache and pub/sub: rejected, nothing reads a cache and NOTIFY covers the one signal.
- A separate time-series database: rejected, Timescale keeps joins between bars, Popularity and Alerts in one query engine.
