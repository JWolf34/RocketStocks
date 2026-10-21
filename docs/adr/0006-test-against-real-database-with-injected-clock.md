# Tests run against a real Timescale database with an injected clock

The risk in this system sits in time-dependent logic (Baselines, rolling windows, ordering, de-duplication, Alert writes) and in Postgres-specific behaviour (hypertables, compression, LISTEN/NOTIFY).
Mocking the database under those tests proves nothing, and ambient time makes them flaky.
We decided:

- **Real database.**
  Integration tests run against a real ephemeral TimescaleDB, the same major version as production.
  Setting `RS_TEST_DB_URL` makes them mandatory: an unreachable database is a failure, not a skip.
  Without it they skip visibly, for local development.
- **Injected clock.**
  Source code never reads ambient time.
  Time comes from a small `Clock` protocol injected at the service layer, or from concrete timestamps carried on the data.
  `scripts/check_clock_discipline.sh` enforces this over every member's `src/` and fails if it finds no members to check.
- **Incidental time in tests** uses `time-machine`, not hand-mocked datetimes.
- **Fixtures** are synthetic, or gitignored and local; vendor data is never committed.
- Tests assert semantic outcomes (which Alert was written, which row was a Tombstone), not rendered bytes.

## Consequences

- Tests are slower than mocked ones and need a database container in CI.
- Every module that needs the time takes a `Clock`; the gate turns forgetting it into a failing check.

## Considered options

- Mocked database drivers: rejected, they cannot exercise Timescale or NOTIFY.
- Patching the clock in tests only: rejected, injection makes time an explicit input to the code instead of a test trick.
