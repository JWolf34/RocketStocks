"""S008 step-3: deterministic ChainScanner tests — fake clock, fake transport,
fake store; no ambient time, no network, no DB. A synthetic stale-response
sequence replays from builders."""

import asyncio
from datetime import date, datetime, timedelta, timezone

from rs_domain import Dataset, SourceStatus
from rs_ingest.chain_scanner import ChainScanner

UTC = timezone.utc
DAY = date(2026, 7, 6)
OPEN = datetime(2026, 7, 6, 13, 30, tzinfo=UTC)


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.current = start

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


class FakeCalendar:
    def __init__(self, close: datetime, session: bool = True) -> None:
        self.close = close
        self.session = session

    def is_session(self, day: date) -> bool:
        return self.session

    def session_open(self, day: date) -> datetime:
        return OPEN

    def session_close(self, day: date) -> datetime:
        return self.close


class FakeStore:
    def __init__(self, restart_state: dict | None = None) -> None:
        self.snapshots = []
        self.restart_state = restart_state or {}
        self.fail_for: set[str] = set()

    async def insert(self, snapshot) -> int:
        if snapshot.underlying in self.fail_for:
            raise RuntimeError("store down")
        self.snapshots.append(snapshot)
        return max(1, len(snapshot.rows))

    async def last_accepted_median_quote_times(self, dataset, now):
        assert dataset is Dataset.CURATED_5M
        return dict(self.restart_state)


class ScriptedFetch:
    """Per-name scripts. Items: a payload dict (returned), an Exception
    (raised), "hang" (never resolves — timeout territory), or a
    (latency_seconds, payload) pair that advances the fake clock first, to
    simulate a slow train. The last item repeats once a script runs dry."""

    def __init__(self, clock: FakeClock, scripts: dict) -> None:
        self.clock = clock
        self.scripts = {name: list(items) for name, items in scripts.items()}
        self.calls: list[tuple[str, datetime]] = []

    async def __call__(self, underlying: str) -> dict:
        self.calls.append((underlying, self.clock.now()))
        script = self.scripts[underlying]
        item = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(item, tuple):
            latency, item = item
            self.clock.advance(latency)
        if isinstance(item, Exception):
            raise item
        if item == "hang":
            await asyncio.Future()
        return item


def payload(quote_offsets_s=(1.0, 2.0, 3.0), price=100.0, base=OPEN, **top_level):
    """Minimal real-shaped chain: one expiry, one call per offset, strikes
    walking up from just under the price so every row is in-window."""
    strikes = {}
    for i, offset in enumerate(quote_offsets_s):
        strike = 95.0 + i
        strikes[str(strike)] = [
            {
                "putCall": "CALL",
                "symbol": f"XYZ   260717C{i:08d}",
                "strikePrice": strike,
                "quoteTimeInLong": int((base + timedelta(seconds=offset)).timestamp() * 1000),
                "bid": 1.0,
                "ask": 1.2,
                "totalVolume": 10,
                "openInterest": 100,
                "multiplier": 100.0,
                "expirationType": "W",
                "nonStandard": False,
            }
        ]
    return {
        "symbol": "XYZ",
        "status": "SUCCESS",
        "underlyingPrice": price,
        "callExpDateMap": {"2026-07-17:11": strikes},
        "putExpDateMap": {},
        **top_level,
    }


def make_scanner(clock, store, fetch, *, close=None, universe=("AAA",), **overrides):
    async def fake_sleep(seconds: float) -> None:
        clock.advance(seconds)

    kwargs = dict(
        universe=universe,
        fetch=fetch,
        store=store,
        clock=clock,
        calendar=FakeCalendar(close or OPEN + timedelta(minutes=10)),
        window=0.10,
        row_cap=4000,
        request_timeout=1.0,
        request_spacing=0.0,
        sleep=fake_sleep,
    )
    kwargs.update(overrides)
    return ChainScanner(**kwargs)


def by_status(store):
    return [(s.underlying, s.cycle_id, s.source_status) for s in store.snapshots]


class TestCycleModel:
    async def test_boundary_launch_lexicographic_order(self):
        # Started mid-session at 13:32 -> first boundary is 13:35; close at
        # 13:45 leaves boundaries 13:35 and 13:40. Universe given unsorted.
        clock = FakeClock(OPEN + timedelta(minutes=2))
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"BBB": [payload()], "AAA": [payload()]})
        scanner = make_scanner(
            clock, store, fetch, close=OPEN + timedelta(minutes=15), universe=("BBB", "AAA")
        )
        await scanner.run()

        t1, t2 = OPEN + timedelta(minutes=5), OPEN + timedelta(minutes=10)
        assert by_status(store) == [
            ("AAA", t1, SourceStatus.OK),
            ("BBB", t1, SourceStatus.OK),
            ("AAA", t2, SourceStatus.OK),
            ("BBB", t2, SourceStatus.OK),
        ]
        assert all(s.fetched_at >= s.cycle_id for s in store.snapshots)
        assert all(s.dataset is Dataset.CURATED_5M for s in store.snapshots)
        assert scanner.cycle_outcomes == {"completed": 2, "skipped": 0}

    async def test_premarket_start_first_cycle_at_open(self):
        clock = FakeClock(OPEN - timedelta(minutes=30))
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"AAA": [payload()]})
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=5))
        await scanner.run()
        assert by_status(store) == [("AAA", OPEN, SourceStatus.OK)]

    async def test_non_session_day_does_nothing(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        scanner = make_scanner(
            clock,
            store,
            ScriptedFetch(clock, {}),
            calendar=FakeCalendar(OPEN + timedelta(minutes=10), session=False),
        )
        await scanner.run()
        assert store.snapshots == []

    async def test_started_after_close_does_nothing(self):
        clock = FakeClock(OPEN + timedelta(hours=8))
        store = FakeStore()
        scanner = make_scanner(clock, store, ScriptedFetch(clock, {}), close=OPEN + timedelta(hours=6, minutes=30))
        await scanner.run()
        assert store.snapshots == []

    async def test_overrun_skips_boundary_with_tombstones(self):
        # The 13:30 train takes 6 minutes: boundary 13:35 passes mid-run and
        # must launch nothing — each name gets one `skipped` tombstone for it —
        # then the train resumes on time at 13:40.
        clock = FakeClock(OPEN - timedelta(minutes=1))
        store = FakeStore()
        fetch = ScriptedFetch(
            clock, {"AAA": [(360.0, payload()), payload()], "BBB": [payload()]}
        )
        scanner = make_scanner(
            clock, store, fetch, close=OPEN + timedelta(minutes=15), universe=("AAA", "BBB")
        )
        await scanner.run()

        t0, t1, t2 = OPEN, OPEN + timedelta(minutes=5), OPEN + timedelta(minutes=10)
        assert by_status(store) == [
            ("AAA", t0, SourceStatus.OK),
            ("BBB", t0, SourceStatus.OK),
            ("AAA", t1, SourceStatus.SKIPPED),
            ("BBB", t1, SourceStatus.SKIPPED),
            ("AAA", t2, SourceStatus.OK),
            ("BBB", t2, SourceStatus.OK),
        ]
        tombstones = [s for s in store.snapshots if s.source_status is SourceStatus.SKIPPED]
        assert all(s.rows == () for s in tombstones)
        assert scanner.cycle_outcomes == {"completed": 2, "skipped": 1}
        assert scanner.fetch_outcomes["skipped"] == 2

    async def test_request_spacing_paces_the_train(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"AAA": [payload()], "BBB": [payload()], "CCC": [payload()]})
        scanner = make_scanner(
            clock,
            store,
            fetch,
            close=OPEN + timedelta(minutes=5),
            universe=("AAA", "BBB", "CCC"),
            request_spacing=0.625,
        )
        await scanner.run()
        gaps = [
            (fetch.calls[i + 1][1] - fetch.calls[i][1]).total_seconds()
            for i in range(len(fetch.calls) - 1)
        ]
        assert all(gap >= 0.625 for gap in gaps)


class TestFetchFailures:
    async def test_retry_once_then_ok(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"AAA": [RuntimeError("502"), payload()]})
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=5))
        await scanner.run()
        assert by_status(store) == [("AAA", OPEN, SourceStatus.OK)]
        assert scanner.retries == 1
        assert [name for name, _ in fetch.calls] == ["AAA", "AAA"]

    async def test_second_failure_writes_failed_tombstone(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(
            clock, {"AAA": [RuntimeError("502"), RuntimeError("502")], "BBB": [payload()]}
        )
        scanner = make_scanner(
            clock, store, fetch, close=OPEN + timedelta(minutes=5), universe=("AAA", "BBB")
        )
        await scanner.run()
        assert by_status(store) == [
            ("AAA", OPEN, SourceStatus.FAILED),
            ("BBB", OPEN, SourceStatus.OK),
        ]
        (failed,) = [s for s in store.snapshots if s.source_status is SourceStatus.FAILED]
        assert failed.rows == ()
        assert scanner.retries == 1
        assert scanner.fetch_outcomes == {"ok": 1, "partial": 0, "failed": 1, "stale": 0, "skipped": 0}
        assert scanner.last_success_at is not None  # BBB succeeded

    async def test_hanging_request_bounded_by_timeout(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"AAA": ["hang"]})
        scanner = make_scanner(
            clock, store, fetch, close=OPEN + timedelta(minutes=5), request_timeout=0.05
        )
        await scanner.run()
        assert by_status(store) == [("AAA", OPEN, SourceStatus.FAILED)]
        assert scanner.retries == 1

    async def test_store_error_is_counted_and_train_continues(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        store.fail_for = {"AAA"}
        fetch = ScriptedFetch(clock, {"AAA": [payload()], "BBB": [payload()]})
        scanner = make_scanner(
            clock, store, fetch, close=OPEN + timedelta(minutes=5), universe=("AAA", "BBB")
        )
        await scanner.run()
        assert by_status(store) == [("BBB", OPEN, SourceStatus.OK)]
        assert scanner.scan_errors == 1
        assert scanner.cycle_outcomes["completed"] == 1


class TestStaleGuard:
    async def test_regression_stored_stale_then_recovery(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(
            clock,
            {
                "AAA": [
                    payload(quote_offsets_s=(8.0, 10.0, 12.0)),
                    payload(quote_offsets_s=(-850.0, -841.0, -830.0)),  # cached past
                    payload(quote_offsets_s=(608.0, 610.0, 612.0)),
                ]
            },
        )
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=15))
        await scanner.run()

        assert [s.source_status for s in store.snapshots] == [
            SourceStatus.OK,
            SourceStatus.STALE,
            SourceStatus.OK,
        ]
        stale = store.snapshots[1]
        assert len(stale.rows) == 3, "stale snapshots are stored, never dropped"
        assert scanner.rows_written == {"ok": 6, "partial": 0, "failed": 0, "stale": 3, "skipped": 0}

    async def test_equal_median_is_not_a_regression(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        same = payload(quote_offsets_s=(1.0, 2.0, 3.0))
        fetch = ScriptedFetch(clock, {"AAA": [same, same]})
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=10))
        await scanner.run()
        assert [s.source_status for s in store.snapshots] == [SourceStatus.OK, SourceStatus.OK]

    async def test_restart_state_arms_the_guard_immediately(self):
        clock = FakeClock(OPEN)
        store = FakeStore(restart_state={"AAA": OPEN + timedelta(seconds=100)})
        fetch = ScriptedFetch(clock, {"AAA": [payload(quote_offsets_s=(1.0, 2.0, 3.0))]})
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=5))
        await scanner.run()
        assert [s.source_status for s in store.snapshots] == [SourceStatus.STALE]

    async def test_stale_snapshot_does_not_move_the_baseline(self):
        # After a stale response, recovery is judged against the LAST ACCEPTED
        # median — a second response older than the accepted baseline stays
        # stale even though it is newer than the stale one before it.
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(
            clock,
            {
                "AAA": [
                    payload(quote_offsets_s=(100.0,)),
                    payload(quote_offsets_s=(-900.0,)),
                    payload(quote_offsets_s=(-500.0,)),
                ]
            },
        )
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=15))
        await scanner.run()
        assert [s.source_status for s in store.snapshots] == [
            SourceStatus.OK,
            SourceStatus.STALE,
            SourceStatus.STALE,
        ]


class TestPartial:
    async def test_truncated_chain_is_partial(self):
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"AAA": [payload(isChainTruncated=True)]})
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=5))
        await scanner.run()
        assert [s.source_status for s in store.snapshots] == [SourceStatus.PARTIAL]

    async def test_unmappable_contract_is_partial(self):
        p = payload()
        del p["callExpDateMap"]["2026-07-17:11"]["95.0"][0]["symbol"]
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"AAA": [p]})
        scanner = make_scanner(clock, store, fetch, close=OPEN + timedelta(minutes=5))
        await scanner.run()
        (snapshot,) = store.snapshots
        assert snapshot.source_status is SourceStatus.PARTIAL
        assert len(snapshot.rows) == 2


class TestStalePairReplay:
    """A synthetic stale-response sequence: the second HTTP-200 response is a
    coherent chain whose quotes are ~14 minutes older than the first; the third
    recovers. Invented numbers, not vendor data."""

    @staticmethod
    def chain(median_at: datetime) -> dict:
        # three contracts quoted around the median; row count is what matters
        return payload(quote_offsets_s=(-1.0, 0.0, 1.0), base=median_at)

    async def test_stale_guard_ok_stale_ok_sequence(self):
        clock = FakeClock(datetime(2026, 7, 6, 15, 2, 30, tzinfo=UTC))
        store = FakeStore()
        medians_in = [
            datetime(2026, 7, 6, 15, 3, 42, 250000, tzinfo=UTC),
            datetime(2026, 7, 6, 14, 49, 42, 125000, tzinfo=UTC),
            datetime(2026, 7, 6, 15, 5, 44, 500000, tzinfo=UTC),
        ]
        fetch = ScriptedFetch(clock, {"SYN": [self.chain(m) for m in medians_in]})
        # Close at 15:17:30 leaves exactly three boundaries (15:05/15:10/15:15);
        # the guard only cares about response order, not the cycle grid.
        scanner = make_scanner(
            clock,
            store,
            fetch,
            close=datetime(2026, 7, 6, 15, 17, 30, tzinfo=UTC),
            universe=("SYN",),
        )
        await scanner.run()

        assert [s.source_status for s in store.snapshots] == [
            SourceStatus.OK,
            SourceStatus.STALE,
            SourceStatus.OK,
        ]
        assert [s.median_quote_time for s in store.snapshots] == medians_in
        assert all(len(s.rows) == 3 for s in store.snapshots)


class TestObservability:
    async def test_cycle_duration_observed_and_last_success_tracked(self):
        durations = []
        clock = FakeClock(OPEN)
        store = FakeStore()
        fetch = ScriptedFetch(clock, {"AAA": [(2.0, payload())]})
        scanner = make_scanner(
            clock,
            store,
            fetch,
            close=OPEN + timedelta(minutes=10),
            on_cycle_duration=durations.append,
        )
        await scanner.run()
        assert len(durations) == 2
        assert all(d >= 2.0 for d in durations)
        assert scanner.last_success_at is not None
