# Failure semantics: fail loud, tombstone, never zero

A market-data system fails in two very different ways: a write that vanishes silently, and a fetch that quietly returns nothing and gets read as zero.
Both produce wrong output, and wrong is worse than missing.
We decided on one policy for both.

- **Writes fail loud.**
  A failed write of an Alert, a paper trade or any stored fact raises; it is never swallowed.
  Reads of refetchable data degrade by skipping the cycle and retrying, but never without a log line and a metric.
- **Tombstones record failed fetches.**
  A failed per-stock fetch stores a row with no payload and a failed status, so a later reader sees the gap instead of a hole.
- **A non-ok row is no observation, never zero.**
  Anything that reads stored rows treats failed, stale and skipped rows as "not observed", not as a zero price or a zero count.
- **A Trigger never fires on missing inputs.**
- **Missing facts are labelled.**
  Every number shown carries its As-of time; a fact that cannot be computed is shown as missing with the reason.
  An Alert whose supporting facts are missing still posts, with them labelled, and pings the operator.
- **Freshness lives on stored rows**, as a fetch time and a status (`ok` or one of a small set of non-ok reasons), and in per-source health metrics.
  In-process data carries no freshness of its own.
- **Ops pings** fire when a source starts failing and again when it clears.
  A failure is a source failing (or more than about 5% of a sweep failing), not one stock.

## Consequences

- Backtests and Track records see gaps explicitly and can exclude them.
- Every reader of stored rows must filter on status; this is the one recurring tax.

## Considered options

- Per-dataset freshness budgets and fetch-policy enums: deferred until a reader needs a staleness threshold.
- Writing zeros or last-known values on failure: rejected, it makes wrong data look like data.
