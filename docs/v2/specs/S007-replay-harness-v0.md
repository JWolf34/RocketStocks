# S007 — Replay harness v0: assert replay == live

**Phase**: 1 · **Appetite**: 2 days · **Depends on**: S003 · **ADRs / notes**: [0023](../adr/0023-test-strategy-deterministic-replay.md), [0018](../adr/0018-temporal-hot-cold-model-and-warmup.md), [0008](../adr/0008-detection-in-ingest.md), [0005](../adr/0005-event-bus-shape.md)

> Spec grilled and decisions pinned 2026-07-12, pre-implementation. S003 already shipped the record/replay seam (`FixtureRecorder`/`RecordingEventSource`/`RecordedEventSource`, the throwaway-DB replay tool, a recorded full session 2026-07-09). This slice adds the **assertion**: replay reproduces the live run's semantic alert decisions — once against the live DB (the Phase-1 exit claim), and continuously in CI (first instance of the roadmap's golden-fixture pattern).

## Goal

After this slice, replaying a recorded session fixture through the real pipeline provably reproduces the live run's semantic alert decisions: a comparator asserts it against the live database for the acceptance day, and a CI golden test keeps asserting it on every push — the refactor net Phase 2's detector work builds on (ADR-0023).

## Interface

- **Semantic alert decision** := the full `rs_domain.Alert` model — `(symbol, alert_type, triggered_at, payload)`. Semantic identity is domain-model equality: everything the detector decided, nothing the DB added (`id`, `created_at`, `posted_at` excluded by construction).
- **`rs_ingest.replay_check`** — comparator module shared by the CLI and the CI test:
  - projection: alert rows → `Alert` (the *same* code serves both baselines, so an export bug can't blind one path silently);
  - `compare(baseline, replayed)` → diff with `identical: bool`, `only_in_live`, `only_in_replay` — **multiset** semantics (full-model equality means a field mismatch lands one entry on each side; there is no third bucket).
- **`python -m rs_ingest.tools.replay`** grows an assertion mode; without the new flags, existing behavior (`--plant-raiser` boundary proof) is unchanged:
  - `--live-db <dsn>` — baseline = live alert rows, read-only, **auto-filtered to the fixture's own day/symbols derived from the fixture** (never operator-typed);
  - `--expected <file>` — baseline = a decisions JSONL previously exported by the harness itself;
  - `--export-decisions <file>` — write the replay's semantic decisions as canonical Alert JSONL (this is how golden expectations are produced);
  - **exit 0 iff the multisets are identical; exit 1 on any difference** — strict, no auto-forgiveness (a comparator that excuses differences is ADR-0023's rot hazard with extra steps);
  - result header prints fixture path, threshold, and both sides' counts; each differing Alert prints as canonical JSON under its bucket.
- **CI golden test** in `services/ingest/tests/`: replays the committed 2026-07-09 fixture (copied under `tests/fixtures/`) through the real `Pipeline` into the CI database (existing `migrated_db_url`/`session_factory` fixtures; the ci.yml TimescaleDB service already exists from S001/S002 — no S006 dependency), projects the alert rows, asserts equality with a committed expectations JSONL.
- **Known failure mode, documented not solved:** replay config ≠ live config (the volume threshold). v0 pins it operator-side — `--threshold` stays a CLI arg, printed in the result header and recorded in the acceptance note. The structural fix is Phase 2's config-version stamp on alert rows.
- **Known expected-diff cause:** a nonzero live `alerts_dropped` (Tier-2 tolerated write failure) legitimately appears as only-in-replay — the replay demonstrating the re-derivability that justified Tier-2. Drops exist only in live ERROR logs (full payload), not in any table, so reconciliation is the operator's, never the tool's.

## Non-goals

1. **No state comparison beyond alert decisions** — no bar-row diffing or window-state assertion; full "replay state == live state" is Phase-2 warm-up (ADR-0018) and gate G4. Bars keep the free `bars_persisted == fixture length` counter check.
2. **No warm-up integration** — fixture-file source only; the hypertable-tail adapter is Phase-2 work.
3. **No backtest reuse** — Phase 3, and it consumes `serve` (ADR-0011).
4. **No config stamping** — Phase 2 (see known failure mode above).
5. **No multi-symbol / multi-day fixtures** — the harness must not break on them, but v0 ships and asserts on S003's one-ticker single-session fixture.
6. **No CI live-DB path** — CI runs only the golden-file baseline; `--live-db` is a dev-box tool.
7. **No lib extraction** — the harness stays in `rs_ingest`; the shared seam (EventSource, `BarClosed`, total ordering) already lives in `rs_domain`. Extraction happens when warm-up or backtest actually consumes the harness, as that slice's work.

## Acceptance criteria

- [ ] **Live-vs-replay (the Phase-1 exit claim):** the comparator run on the 2026-07-09 fixture against the acceptance DB (5498 server) exits 0 with identical multisets (12 = 12). *(Fallback if the 7/9 artifacts are gone: record the next market day per the S003 runbook.)* The phase-exit re-run against the compose-assembled skeleton's full day is phase-gate work, not this slice.
- [ ] **Golden CI test green:** committed fixture → real `Pipeline` → CI DB → semantic Alerts == committed expectations JSONL.
- [ ] **Negative proof:** one deliberately tampered expectations line makes the comparator exit 1, classifying the alert only-in-live / only-in-replay — the comparator demonstrably *can* fail.
- [ ] Result header prints the threshold the replay ran.
- [ ] Comparator unit tests cover multiset edges: duplicate decisions, field mismatch landing one entry per side.

## Verification

```bash
# unit + CI golden (needs the spec TimescaleDB container / RS_TEST_DB_URL)
.venv/bin/pytest libs/ services/ingest -q

# live-vs-replay acceptance (dev box, post-close; threshold = the live run's)
.venv/bin/python -m rs_ingest.tools.replay --fixture data/session-fixture-2026-07-09.jsonl \
  --live-db postgresql+psycopg://postgres:<pw>@localhost:5498/rocketstocks \
  --admin-db postgresql+psycopg://postgres:<pw>@localhost:5498/postgres
# -> header shows threshold + 12/12; exit 0

# negative proof: flip one field in a copy of the expectations file, expect exit 1 + classified diff
.venv/bin/python -m rs_ingest.tools.replay --fixture <fixture> --expected /tmp/tampered.jsonl; echo $?
```

## Decisions surfaced

*(Empty at review time; grilling decisions of 2026-07-12 are folded into the sections above.)*
