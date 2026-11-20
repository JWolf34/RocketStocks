"""The evening chain pass and the morning open-interest pass against a real database, the real
exchange calendar and the real Schwab chain adapter over a fake schwab-py client: the roster, the
ladder (retry, expiry ranges, Tombstone), 400s and empty chains, resume, the deadline, the dead
token and the sweep tally."""

from datetime import UTC, date, datetime, time, timedelta

import pytest
from sqlalchemy import func, select

from rs_domain import (
    EASTERN,
    ChainPass,
    FetchStatus,
    UniverseCounts,
    UniverseListing,
    UniverseSnapshot,
)
from rs_ingest.chains import (
    ATTEMPTS,
    chain_deadline,
    expiry_ranges,
    morning_slot,
    sweep_evening_chains,
    sweep_morning_open_interest,
)
from rs_ingest.rate_limit import RateLimiter
from rs_observability import SweepTally, SweepVerdict
from rs_persistence import ChainRepository, OptionContractRow, UniverseRepository
from rs_sources import ExchangeCalendar
from rs_sources.schwab import (
    SchwabAuthError,
    SchwabOptionChains,
    SchwabResponseError,
    TokenMissingError,
)

CALENDAR = ExchangeCalendar()
# Tuesday 2026-11-17, 18:30 ET: the evening of that session has started.
SESSION = date(2026, 11, 17)
NOW = datetime(2026, 11, 17, 23, 30, tzinfo=UTC)
# 19:00 EST that evening: Schwab resets the session's volume to zero from here.
DEADLINE = datetime(2026, 11, 18, 0, 0, tzinfo=UTC)
QUOTED = int(datetime(2026, 11, 17, 21, 0, tzinfo=UTC).timestamp() * 1000)

NEAR = SESSION + timedelta(days=10)
MID = SESSION + timedelta(days=60)
FAR = SESSION + timedelta(days=300)
EXPIRIES = (NEAR, MID, FAR)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def _raw(symbol: str, expiry: date, side: str) -> dict:
    letter = side[0]
    return {
        "putCall": side,
        "symbol": f"{symbol.replace('/', ''):<6}{expiry:%y%m%d}{letter}00100000",
        "strikePrice": 100.0,
        "bid": 1.1,
        "ask": 1.2,
        "last": 1.15,
        "mark": 1.15,
        "bidSize": 10,
        "askSize": 12,
        "lastSize": 1,
        "totalVolume": 250,
        "openInterest": 4000,
        "quoteTimeInLong": QUOTED,
        "tradeTimeInLong": QUOTED - 60_000,
        "volatility": 31.5,
        "delta": 0.5,
        "gamma": 0.04,
        "theta": -0.05,
        "vega": 0.1,
        "rho": 0.01,
        "multiplier": 100.0,
        "expirationType": "R",
        "nonStandard": False,
    }


def chain_payload(symbol: str, expiries=EXPIRIES) -> dict:
    """A chain with one call and one put on each of the expiries."""

    def side_map(side: str) -> dict:
        return {f"{e.isoformat()}:{(e - SESSION).days}": {"100.0": [_raw(symbol, e, side)]} for e in expiries}

    return {
        "symbol": symbol,
        "status": "SUCCESS",
        "underlyingPrice": 99.5,
        "isChainTruncated": False,
        "callExpDateMap": side_map("CALL"),
        "putExpDateMap": side_map("PUT"),
    }


class FakeSchwab:
    """schwab-py's option chain call over a scripted market.

    Every symbol answers with one call and one put on each of NEAR, MID and FAR, bounded by the
    request's dates. `script[symbol]` is a list of answers handed out one per call before the
    default applies: a status code, an exception, or a payload.
    """

    class Options:
        class ContractType:
            ALL = "ALL"

    def __init__(self) -> None:
        self.script: dict[str, list] = {}
        self.calls: list[tuple[str, date | None, date | None]] = []
        self.empty: set[str] = set()

    async def get_option_chain(self, symbol, *, contract_type=None, from_date=None, to_date=None):
        self.calls.append((symbol, from_date, to_date))
        queued = self.script.get(symbol)
        if queued:
            answer = queued.pop(0)
            if isinstance(answer, Exception):
                raise answer
            if isinstance(answer, int):
                return FakeResponse(status_code=answer)
            if answer is not None:
                return FakeResponse(answer)
        expiries = () if symbol in self.empty else tuple(
            e for e in EXPIRIES if (from_date is None or e >= from_date) and (to_date is None or e <= to_date)
        )
        return FakeResponse(chain_payload(symbol, expiries))

    def symbols_called(self) -> list[str]:
        return [symbol for symbol, _, _ in self.calls]


class Budget:
    """A limiter in virtual time that records how long it made callers wait."""

    def __init__(self) -> None:
        self.t = 0.0
        self.waited = 0.0

    async def sleep(self, seconds: float) -> None:
        self.waited += seconds
        self.t += seconds

    def limiter(self, calls_per_minute: int = 60) -> RateLimiter:
        return RateLimiter(calls_per_minute, now=lambda: self.t, sleep=self.sleep)


def _listing(symbol: str, optionable: bool) -> UniverseListing:
    return UniverseListing(
        symbol=symbol,
        security_name=f"{symbol} Inc.",
        exchange="Q",
        etf=False,
        financial_status="N",
        cqs_symbol=None,
        nasdaq_symbol=None,
        optionable=optionable,
    )


async def _universe(session_factory, optionable: tuple[str, ...], others: tuple[str, ...] = ("PLAIN",), day=SESSION):
    listings = tuple(_listing(s, True) for s in optionable) + tuple(_listing(s, False) for s in others)
    snapshot = UniverseSnapshot(
        snapshot_date=day,
        as_of=datetime.combine(day, time(18, 1), tzinfo=EASTERN).astimezone(UTC),
        counts=UniverseCounts(nasdaq_listed=len(listings), other_listed=0, options_symbols=len(optionable), cboe_symbols=0),
        optionable_carried=False,
        listings=listings,
    )
    async with session_factory() as s:
        await UniverseRepository(s).save(snapshot)
        await s.commit()


async def _run(
    session_factory,
    schwab: FakeSchwab,
    clock: FakeClock | None = None,
    tally: SweepTally | None = None,
    budget: Budget | None = None,
    source=None,
):
    return await sweep_evening_chains(
        session_factory=session_factory,
        source=source or SchwabOptionChains(schwab),
        limiter=(budget or Budget()).limiter(),
        calendar=CALENDAR,
        clock=clock or FakeClock(NOW),
        tally=tally if tally is not None else SweepTally(),
    )


async def _record(session_factory, symbol: str, session: date = SESSION):
    async with session_factory() as s:
        return await ChainRepository(s).record(session, ChainPass.EVENING, symbol)


async def _contract_rows(session_factory, symbol: str) -> int:
    async with session_factory() as s:
        return await s.scalar(
            select(func.count()).select_from(OptionContractRow).where(OptionContractRow.underlying == symbol)
        )


async def _contracts(session_factory, symbol: str):
    async with session_factory() as s:
        return await ChainRepository(s).contracts(SESSION, symbol)


# --- the roster -------------------------------------------------------------------------------------


async def test_every_optionable_underlying_of_the_latest_universe_is_fetched_whole_and_recorded(session_factory):
    await _universe(session_factory, ("CCC", "AAA", "BRK.B"), others=("PLAIN", "ZZZ"))
    schwab = FakeSchwab()

    report = await _run(session_factory, schwab)

    # Sorted, the optionable listings only, each in one unfiltered call under Schwab's own spelling.
    assert schwab.calls == [("AAA", None, None), ("BRK/B", None, None), ("CCC", None, None)]
    assert (report.session, report.roster, report.ok, report.tombstones) == (SESSION, 3, 3, 0)
    assert (report.contracts, report.calls) == (18, 3)
    record = await _record(session_factory, "BRK.B")
    assert (record.status, record.attempts, record.contract_count, record.fetched_at) == (FetchStatus.OK, 1, 6, NOW)
    # The contracts are stored under the Universe's symbol, not the request's spelling.
    assert {c.underlying for c in await _contracts(session_factory, "BRK.B")} == {"BRK.B"}
    assert await _record(session_factory, "PLAIN") is None


async def test_only_the_latest_universe_is_the_roster(session_factory):
    await _universe(session_factory, ("OLD", "AAA"), day=date(2026, 11, 16))
    await _universe(session_factory, ("AAA", "NEW"), day=SESSION)
    schwab = FakeSchwab()

    await _run(session_factory, schwab)

    assert schwab.symbols_called() == ["AAA", "NEW"]


async def test_without_a_universe_the_pass_makes_no_call(session_factory):
    schwab = FakeSchwab()

    report = await _run(session_factory, schwab)

    assert schwab.calls == []
    assert (report.roster, report.calls) == (0, 0)


async def test_a_weekend_run_belongs_to_fridays_session(session_factory):
    friday = date(2026, 11, 13)
    await _universe(session_factory, ("AAA",), day=friday)
    saturday_noon = datetime(2026, 11, 14, 17, 0, tzinfo=UTC)

    # Friday's deadline passed at 03:00 Central Saturday: the pass only records what was missed.
    report = await _run(session_factory, FakeSchwab(), FakeClock(saturday_noon))

    assert report.session == friday
    assert (await _record(session_factory, "AAA", friday)).status is FetchStatus.FAILED


# --- the ladder -------------------------------------------------------------------------------------


async def test_a_failed_fetch_is_retried_once(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [502]

    await _run(session_factory, schwab)

    assert schwab.calls == [("AAA", None, None), ("AAA", None, None)]
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts, record.contract_count) == (FetchStatus.OK, 2, 6)


async def test_a_chain_that_fails_twice_is_fetched_in_three_expiry_ranges_the_last_open_ended(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [502, TimeoutError("read timed out")]

    await _run(session_factory, schwab)

    assert schwab.calls == [
        ("AAA", None, None),
        ("AAA", None, None),
        ("AAA", SESSION, SESSION + timedelta(days=30)),
        ("AAA", SESSION + timedelta(days=31), SESSION + timedelta(days=180)),
        ("AAA", SESSION + timedelta(days=181), None),
    ]
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts, record.contract_count) == (FetchStatus.OK, 5, 6)
    # The whole chain, assembled from its parts in expiry order.
    assert [c.expiry for c in await _contracts(session_factory, "AAA")] == [NEAR, NEAR, MID, MID, FAR, FAR]


def test_the_ladder_ranges_cover_every_expiry_from_the_session_without_a_gap_or_overlap():
    (a0, a1), (b0, b1), (c0, c1) = expiry_ranges(SESSION)

    assert (a0, c1) == (SESSION, None)
    assert b0 == a1 + timedelta(days=1)
    assert c0 == b1 + timedelta(days=1)


async def test_each_range_is_retried_once(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    # The whole chain fails twice, then the middle range fails once.
    schwab.script["AAA"] = [502, 502, None, 503]

    await _run(session_factory, schwab)

    assert len(schwab.calls) == 2 + 4  # whole x2, near, mid x2, far
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts, record.contract_count) == (FetchStatus.OK, 6, 6)


async def test_a_range_that_fails_twice_makes_the_underlying_a_tombstone_with_no_partial_chain(session_factory):
    await _universe(session_factory, ("AAA", "BBB"))
    schwab = FakeSchwab()
    # AAA: whole x2 fail, near answers, mid fails x2.
    schwab.script["AAA"] = [502, 502, None, 502, 502]

    tally = SweepTally()
    report = await _run(session_factory, schwab, tally=tally)

    assert schwab.calls[:6] == [
        ("AAA", None, None),
        ("AAA", None, None),
        ("AAA", SESSION, SESSION + timedelta(days=30)),
        ("AAA", SESSION + timedelta(days=31), SESSION + timedelta(days=180)),
        ("AAA", SESSION + timedelta(days=31), SESSION + timedelta(days=180)),
        ("BBB", None, None),
    ]  # the far range is never asked, and the next underlying goes on
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts, record.contract_count) == (FetchStatus.FAILED, 5, 0)
    assert record.status.is_tombstone
    assert await _contract_rows(session_factory, "AAA") == 0
    assert (report.failed, report.ok, tally.failed) == (1, 1, 1)


async def test_an_unexpected_error_is_retried_like_any_failed_call(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [{"unexpected": "not a chain"}, ValueError("boom")]

    await _run(session_factory, schwab)

    assert len(schwab.calls) == 2 + 3
    assert (await _record(session_factory, "AAA")).status is FetchStatus.OK


async def test_a_repeated_option_symbol_across_ranges_is_stored_once(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    # Whole chain fails twice; the near range answers with a chain that also holds MID's contracts.
    schwab.script["AAA"] = [502, 502, chain_payload("AAA", (NEAR, MID))]

    await _run(session_factory, schwab)

    record = await _record(session_factory, "AAA")
    assert (record.status, record.contract_count) == (FetchStatus.OK, 6)


@pytest.mark.parametrize("status", [400, 404])
async def test_a_symbol_schwab_refuses_is_recorded_rejected_and_never_retried(session_factory, status):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [status]

    tally = SweepTally()
    report = await _run(session_factory, schwab, tally=tally)

    assert len(schwab.calls) == 1
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts, record.contract_count) == (FetchStatus.REJECTED, 1, 0)
    assert record.status.is_tombstone
    assert (report.rejected, tally.failed, tally.attempted) == (1, 0, 1)


async def test_a_400_found_inside_the_ladder_stops_it(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [502, 502, 400]

    await _run(session_factory, schwab)

    assert len(schwab.calls) == 3
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts) == (FetchStatus.REJECTED, 3)


async def test_an_empty_chain_is_an_observation_recorded_once_and_never_retried(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.empty.add("AAA")

    tally = SweepTally()
    report = await _run(session_factory, schwab, tally=tally)

    assert len(schwab.calls) == 1
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts, record.contract_count) == (FetchStatus.EMPTY, 1, 0)
    assert not record.status.is_tombstone
    assert await _contract_rows(session_factory, "AAA") == 0
    assert (report.empty, tally.failed) == (1, 0)


async def test_ranges_that_all_answer_with_nothing_are_an_empty_chain_not_a_tombstone(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [502, 502]
    schwab.empty.add("AAA")

    await _run(session_factory, schwab)

    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts) == (FetchStatus.EMPTY, 5)


async def test_every_call_waits_on_the_shared_budget(session_factory):
    await _universe(session_factory, ("AAA", "BBB"))
    budget = Budget()
    schwab = FakeSchwab()
    schwab.script["AAA"] = [502]

    await _run(session_factory, schwab, budget=budget)

    # 3 calls at 1 per second: the first is free, the other two each wait a second.
    assert budget.waited == pytest.approx(2.0)


# --- resume -----------------------------------------------------------------------------------------


async def test_a_rerun_skips_every_underlying_already_recorded_whatever_its_status(session_factory):
    await _universe(session_factory, ("AAA", "BAD", "EMP", "DEAD"))
    schwab = FakeSchwab()
    schwab.script["BAD"] = [400]
    schwab.empty.add("EMP")
    schwab.script["DEAD"] = [502] * ATTEMPTS * 4
    await _run(session_factory, schwab)
    before = [await _record(session_factory, s) for s in ("AAA", "BAD", "EMP", "DEAD")]
    schwab.calls.clear()

    report = await _run(session_factory, schwab)

    assert schwab.calls == []
    assert (report.already_recorded, report.calls) == (4, 0)
    assert [await _record(session_factory, s) for s in ("AAA", "BAD", "EMP", "DEAD")] == before
    assert await _contract_rows(session_factory, "AAA") == 6


async def test_a_pass_a_dead_token_killed_propagates_and_resumes_without_repeating_work(session_factory):
    await _universe(session_factory, ("AAA", "BBB", "CCC", "DDD"))
    schwab = FakeSchwab()
    schwab.script["CCC"] = [SchwabAuthError("refresh token rejected")]

    tally = SweepTally()
    with pytest.raises(SchwabAuthError):
        await _run(session_factory, schwab, tally=tally)

    # Never a Tombstone: CCC has no record, AAA and BBB were committed before the token died.
    assert schwab.symbols_called() == ["AAA", "BBB", "CCC"]
    assert [(await _record(session_factory, s)) is None for s in ("AAA", "BBB", "CCC", "DDD")] == [
        False,
        False,
        True,
        True,
    ]
    assert (tally.attempted, tally.failed) == (2, 0)
    schwab.calls.clear()

    report = await _run(session_factory, schwab)

    assert schwab.symbols_called() == ["CCC", "DDD"]
    assert (report.already_recorded, report.ok) == (2, 2)


@pytest.mark.parametrize("error", [SchwabAuthError("rejected"), TokenMissingError("no token stored")])
async def test_a_dead_token_inside_the_ladder_is_not_a_tombstone(session_factory, error):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [502, 502, error]

    with pytest.raises(type(error)):
        await _run(session_factory, schwab)

    assert len(schwab.calls) == 3
    assert await _record(session_factory, "AAA") is None
    assert await _contract_rows(session_factory, "AAA") == 0


async def test_being_over_budget_stops_the_pass_without_a_tombstone_and_speaks_for_the_source(session_factory):
    await _universe(session_factory, ("AAA", "BBB", "CCC"))
    schwab = FakeSchwab()
    schwab.script["BBB"] = [429]

    tally = SweepTally()
    with pytest.raises(SchwabResponseError, match="429"):
        await _run(session_factory, schwab, tally=tally)

    assert schwab.symbols_called() == ["AAA", "BBB"]
    assert await _record(session_factory, "AAA") is not None
    assert await _record(session_factory, "BBB") is None
    assert tally.verdict is SweepVerdict.FAILING


# --- the deadline -----------------------------------------------------------------------------------


def test_the_deadline_is_7pm_eastern_on_the_sessions_own_evening():
    # Tuesday 2026-11-17: 19:00 EST is 00:00 UTC on the 18th.
    assert chain_deadline(SESSION) == DEADLINE
    # Friday's evening ends the same evening, not on the weekend.
    assert chain_deadline(date(2026, 11, 13)) == datetime(2026, 11, 14, 0, 0, tzinfo=UTC)
    # Before the clocks fall back (2026-11-01) it is 19:00 EDT, 23:00 UTC.
    assert chain_deadline(date(2026, 10, 20)) == datetime(2026, 10, 20, 23, 0, tzinfo=UTC)
    assert chain_deadline(SESSION, time(19, 30)) == datetime(2026, 11, 18, 0, 30, tzinfo=UTC)


class Ticking:
    """The chain adapter, with the clock moving on `step` after every call."""

    def __init__(self, inner: SchwabOptionChains, clock: FakeClock, step: timedelta) -> None:
        self.inner, self.clock, self.step = inner, clock, step

    async def chain(self, symbol, start=None, end=None):
        try:
            return await self.inner.chain(symbol, start, end)
        finally:
            self.clock.current += self.step


async def test_past_the_deadline_the_pass_writes_only_tombstones_and_makes_no_call(session_factory):
    await _universe(session_factory, ("AAA", "BBB", "CCC"))
    schwab = FakeSchwab()
    tally = SweepTally()

    report = await _run(session_factory, schwab, FakeClock(DEADLINE), tally)

    assert schwab.calls == []
    for symbol in ("AAA", "BBB", "CCC"):
        record = await _record(session_factory, symbol)
        assert (record.status, record.attempts, record.contract_count) == (FetchStatus.FAILED, 0, 0)
        assert record.fetched_at == DEADLINE
    assert (report.past_deadline, report.calls) == (3, 0)
    assert (tally.attempted, tally.failed) == (3, 3)


async def test_a_pass_that_reaches_the_deadline_tombstones_what_is_left(session_factory):
    await _universe(session_factory, ("AAA", "BBB", "CCC", "DDD"))
    schwab = FakeSchwab()
    # The clock is 30 minutes a call and 50 minutes from the deadline: calls 1 and 2 are made, the third is not.
    clock = FakeClock(DEADLINE - timedelta(minutes=50))
    source = Ticking(SchwabOptionChains(schwab), clock, timedelta(minutes=30))

    tally = SweepTally()
    report = await _run(session_factory, schwab, clock, tally, source=source)

    assert schwab.symbols_called() == ["AAA", "BBB"]
    assert [(await _record(session_factory, s)).status for s in ("AAA", "BBB", "CCC", "DDD")] == [
        FetchStatus.OK,
        FetchStatus.OK,
        FetchStatus.FAILED,
        FetchStatus.FAILED,
    ]
    assert [(await _record(session_factory, s)).attempts for s in ("CCC", "DDD")] == [0, 0]
    assert (report.ok, report.past_deadline) == (2, 2)
    assert tally.verdict is SweepVerdict.FAILING


async def test_a_deadline_inside_a_ladder_tombstones_that_underlying_with_the_calls_it_made(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    schwab.script["AAA"] = [502, 502]
    clock = FakeClock(DEADLINE - timedelta(minutes=25))
    source = Ticking(SchwabOptionChains(schwab), clock, timedelta(minutes=10))

    report = await _run(session_factory, schwab, clock, source=source)

    # Two whole-chain calls fail, the first range is asked, then the deadline passes.
    assert len(schwab.calls) == 3
    record = await _record(session_factory, "AAA")
    assert (record.status, record.attempts, record.contract_count) == (FetchStatus.FAILED, 3, 0)
    assert await _contract_rows(session_factory, "AAA") == 0
    assert report.past_deadline == 1


async def test_a_missed_night_is_never_caught_up(session_factory):
    await _universe(session_factory, ("AAA", "BBB", "CCC"))
    schwab = FakeSchwab()
    schwab.script["BBB"] = [SchwabAuthError("refresh token rejected")]
    with pytest.raises(SchwabAuthError):
        await _run(session_factory, schwab)
    schwab.calls.clear()

    # The token is renewed only after the deadline: what was done stays, the rest is recorded as missed.
    morning = FakeClock(DEADLINE + timedelta(hours=5))
    report = await _run(session_factory, schwab, morning)

    assert schwab.calls == []
    assert [(await _record(session_factory, s)).status for s in ("AAA", "BBB", "CCC")] == [
        FetchStatus.OK,
        FetchStatus.FAILED,
        FetchStatus.FAILED,
    ]
    assert (report.already_recorded, report.past_deadline) == (1, 2)


# --- the sweep tally --------------------------------------------------------------------------------


async def test_a_few_tombstones_among_many_underlyings_do_not_fail_the_pass(session_factory):
    symbols = tuple(f"S{n:02d}" for n in range(40))
    await _universe(session_factory, symbols)
    schwab = FakeSchwab()
    schwab.script["S01"] = [502] * 5
    schwab.script["S02"] = [400]
    schwab.empty.add("S03")
    tally = SweepTally()

    await _run(session_factory, schwab, tally=tally)

    # One Tombstone in 40 is 2.5 percent; a 400 and an empty chain are Schwab answering.
    assert (tally.attempted, tally.failed) == (40, 1)
    assert tally.verdict is SweepVerdict.HEALTHY


async def test_more_than_about_five_percent_of_underlyings_failing_fails_the_pass(session_factory):
    symbols = tuple(f"S{n:02d}" for n in range(20))
    await _universe(session_factory, symbols)
    schwab = FakeSchwab()
    for symbol in ("S01", "S05", "S09"):
        schwab.script[symbol] = [502] * 5
    tally = SweepTally()

    await _run(session_factory, schwab, tally=tally)

    assert (tally.attempted, tally.failed) == (20, 3)
    assert tally.verdict is SweepVerdict.FAILING


# --- the morning open-interest pass -----------------------------------------------------------------

# Wednesday 2026-11-18, 07:30 ET: open interest of Tuesday's session has settled; the open is 09:30 ET.
MORNING_NOW = datetime(2026, 11, 18, 12, 30, tzinfo=UTC)
OPEN = datetime(2026, 11, 18, 14, 30, tzinfo=UTC)


async def _run_morning(
    session_factory,
    schwab: FakeSchwab,
    clock: FakeClock | None = None,
    tally: SweepTally | None = None,
    source=None,
):
    return await sweep_morning_open_interest(
        session_factory=session_factory,
        source=source or SchwabOptionChains(schwab),
        limiter=Budget().limiter(),
        calendar=CALENDAR,
        clock=clock or FakeClock(MORNING_NOW),
        tally=tally if tally is not None else SweepTally(),
    )


async def _morning_record(session_factory, symbol: str, session: date = SESSION):
    async with session_factory() as s:
        return await ChainRepository(s).record(session, ChainPass.MORNING, symbol)


async def _open_interest(session_factory, symbol: str, session: date = SESSION):
    async with session_factory() as s:
        return await ChainRepository(s).open_interest(session, symbol)


def _slot(now: datetime) -> tuple[date, datetime]:
    return morning_slot(CALENDAR, now)


def test_a_morning_settles_the_session_before_it_and_is_owed_until_its_open():
    assert _slot(MORNING_NOW) == (SESSION, OPEN)
    # The slot starts at 07:00 ET exactly (12:00 UTC); a minute earlier still belongs to Tuesday's morning.
    assert _slot(datetime(2026, 11, 18, 12, 0, tzinfo=UTC)) == (SESSION, OPEN)
    assert _slot(datetime(2026, 11, 18, 11, 59, tzinfo=UTC)) == (
        date(2026, 11, 16),
        datetime(2026, 11, 17, 14, 30, tzinfo=UTC),
    )
    # Hours after the open the morning is still that morning's, until the next 07:00.
    assert _slot(datetime(2026, 11, 18, 23, 0, tzinfo=UTC)) == (SESSION, OPEN)


def test_a_monday_morning_settles_fridays_session_and_a_weekend_belongs_to_fridays_morning():
    assert _slot(datetime(2026, 11, 16, 12, 30, tzinfo=UTC)) == (
        date(2026, 11, 13),
        datetime(2026, 11, 16, 14, 30, tzinfo=UTC),
    )
    assert _slot(datetime(2026, 11, 14, 17, 0, tzinfo=UTC)) == (
        date(2026, 11, 12),
        datetime(2026, 11, 13, 14, 30, tzinfo=UTC),
    )


def test_the_morning_after_a_holiday_settles_the_session_before_it_and_the_holiday_owns_none():
    # Thanksgiving is Thursday 2026-11-26: Friday's morning settles Wednesday; Thursday belongs to Wednesday's morning.
    assert _slot(datetime(2026, 11, 27, 12, 30, tzinfo=UTC)) == (
        date(2026, 11, 25),
        datetime(2026, 11, 27, 14, 30, tzinfo=UTC),
    )
    assert _slot(datetime(2026, 11, 26, 15, 0, tzinfo=UTC)) == (
        date(2026, 11, 24),
        datetime(2026, 11, 25, 14, 30, tzinfo=UTC),
    )


def test_the_open_follows_daylight_saving():
    # 09:30 EDT is 13:30 UTC.
    assert _slot(datetime(2026, 10, 21, 12, 0, tzinfo=UTC)) == (
        date(2026, 10, 20),
        datetime(2026, 10, 21, 13, 30, tzinfo=UTC),
    )


async def test_the_morning_stores_only_open_interest_named_by_the_session_it_settled(session_factory):
    await _universe(session_factory, ("CCC", "AAA", "BRK.B"), others=("PLAIN",))
    schwab = FakeSchwab()

    report = await _run_morning(session_factory, schwab)

    assert schwab.calls == [("AAA", None, None), ("BRK/B", None, None), ("CCC", None, None)]
    assert (report.session, report.roster, report.ok, report.tombstones, report.contracts) == (SESSION, 3, 3, 0, 18)
    record = await _morning_record(session_factory, "BRK.B")
    assert (record.status, record.attempts, record.contract_count, record.fetched_at) == (
        FetchStatus.OK,
        1,
        6,
        MORNING_NOW,
    )
    rows = await _open_interest(session_factory, "BRK.B")
    assert len(rows) == 6
    assert {(row.underlying, row.open_interest) for row in rows} == {("BRK.B", 4000)}
    # No contract rows and no evening record: the morning never stores what the evening owns.
    assert await _contract_rows(session_factory, "BRK.B") == 0
    assert await _record(session_factory, "BRK.B") is None
    # Named by the settled session, never the date it was fetched.
    assert await _open_interest(session_factory, "BRK.B", date(2026, 11, 18)) == []


async def test_the_morning_and_the_evening_each_record_every_underlying_once(session_factory):
    await _universe(session_factory, ("AAA",))
    schwab = FakeSchwab()
    await _run(session_factory, schwab)

    report = await _run_morning(session_factory, schwab)

    assert report.already_recorded == 0 and report.ok == 1
    assert (await _record(session_factory, "AAA")).chain_pass is ChainPass.EVENING
    assert (await _morning_record(session_factory, "AAA")).chain_pass is ChainPass.MORNING
    assert await _contract_rows(session_factory, "AAA") == 6
    assert len(await _open_interest(session_factory, "AAA")) == 6


async def test_without_a_universe_the_morning_makes_no_call(session_factory):
    schwab = FakeSchwab()

    report = await _run_morning(session_factory, schwab)

    assert schwab.calls == []
    assert (report.roster, report.calls) == (0, 0)


async def test_the_morning_uses_the_same_ladder_and_stores_a_chain_only_whole(session_factory):
    await _universe(session_factory, ("AAA", "RETRY", "SPLIT", "DEAD"))
    schwab = FakeSchwab()
    schwab.script["RETRY"] = [502]
    schwab.script["SPLIT"] = [502, 502]
    # Whole twice, then the first range fine, then the second range twice.
    schwab.script["DEAD"] = [502, 502, None, 502, 502]

    await _run_morning(session_factory, schwab)

    assert (await _morning_record(session_factory, "RETRY")).attempts == 2
    split = await _morning_record(session_factory, "SPLIT")
    assert (split.status, split.attempts, split.contract_count) == (FetchStatus.OK, 5, 6)
    ranges = [(start, end) for symbol, start, end in schwab.calls if symbol == "SPLIT"]
    assert ranges == [(None, None), (None, None), *expiry_ranges(SESSION)]
    assert len(await _open_interest(session_factory, "SPLIT")) == 6
    dead = await _morning_record(session_factory, "DEAD")
    assert dead.status is FetchStatus.FAILED and dead.contract_count == 0
    # A range that failed twice leaves no partial open interest behind.
    assert await _open_interest(session_factory, "DEAD") == []


async def test_a_400_and_an_empty_chain_are_recorded_once_and_never_retried_in_the_morning(session_factory):
    await _universe(session_factory, ("BAD", "EMP"))
    schwab = FakeSchwab()
    schwab.script["BAD"] = [400]
    schwab.empty.add("EMP")

    await _run_morning(session_factory, schwab)

    assert schwab.symbols_called().count("BAD") == 1
    assert (await _morning_record(session_factory, "BAD")).status is FetchStatus.REJECTED
    empty = await _morning_record(session_factory, "EMP")
    assert (empty.status, empty.contract_count) == (FetchStatus.EMPTY, 0)
    assert await _open_interest(session_factory, "EMP") == []


async def test_a_morning_rerun_skips_every_underlying_it_recorded_whatever_its_status(session_factory):
    await _universe(session_factory, ("AAA", "BAD", "DEAD"))
    schwab = FakeSchwab()
    schwab.script["BAD"] = [400]
    schwab.script["DEAD"] = [502] * ATTEMPTS * 4
    await _run_morning(session_factory, schwab)
    schwab.calls.clear()

    report = await _run_morning(session_factory, schwab)

    assert schwab.calls == []
    assert (report.already_recorded, report.calls) == (3, 0)
    assert len(await _open_interest(session_factory, "AAA")) == 6


async def test_a_morning_a_dead_token_killed_resumes_without_repeating_work(session_factory):
    await _universe(session_factory, ("AAA", "BBB", "CCC"))
    schwab = FakeSchwab()
    schwab.script["BBB"] = [SchwabAuthError("refresh token rejected")]
    tally = SweepTally()

    with pytest.raises(SchwabAuthError):
        await _run_morning(session_factory, schwab, tally=tally)

    assert [(await _morning_record(session_factory, s)) is None for s in ("AAA", "BBB", "CCC")] == [False, True, True]
    assert (tally.attempted, tally.failed) == (1, 0)
    schwab.calls.clear()

    report = await _run_morning(session_factory, schwab)

    assert schwab.symbols_called() == ["BBB", "CCC"]
    assert (report.already_recorded, report.ok) == (1, 2)


async def test_being_over_budget_stops_the_morning_without_a_tombstone(session_factory):
    await _universe(session_factory, ("AAA", "BBB"))
    schwab = FakeSchwab()
    schwab.script["BBB"] = [429]
    tally = SweepTally()

    with pytest.raises(SchwabResponseError, match="429"):
        await _run_morning(session_factory, schwab, tally=tally)

    assert await _morning_record(session_factory, "AAA") is not None
    assert await _morning_record(session_factory, "BBB") is None
    assert tally.verdict is SweepVerdict.FAILING


async def test_at_the_open_the_morning_writes_only_tombstones_and_makes_no_call(session_factory):
    await _universe(session_factory, ("AAA", "BBB"))
    schwab = FakeSchwab()
    tally = SweepTally()

    report = await _run_morning(session_factory, schwab, FakeClock(OPEN), tally)

    assert schwab.calls == []
    for symbol in ("AAA", "BBB"):
        record = await _morning_record(session_factory, symbol)
        assert (record.status, record.attempts, record.contract_count) == (FetchStatus.FAILED, 0, 0)
        assert await _open_interest(session_factory, symbol) == []
    assert (report.past_deadline, report.calls) == (2, 0)
    assert tally.verdict is SweepVerdict.FAILING


async def test_a_morning_that_reaches_the_open_tombstones_what_is_left(session_factory):
    await _universe(session_factory, ("AAA", "BBB", "CCC", "DDD"))
    schwab = FakeSchwab()
    # An hour a call and 100 minutes to the open: two calls are made, the third finds the open.
    clock = FakeClock(OPEN - timedelta(minutes=100))
    source = Ticking(SchwabOptionChains(schwab), clock, timedelta(hours=1))

    report = await _run_morning(session_factory, schwab, clock, source=source)

    assert schwab.symbols_called() == ["AAA", "BBB"]
    assert [(await _morning_record(session_factory, s)).status for s in ("AAA", "BBB", "CCC", "DDD")] == [
        FetchStatus.OK,
        FetchStatus.OK,
        FetchStatus.FAILED,
        FetchStatus.FAILED,
    ]
    assert (report.ok, report.past_deadline) == (2, 2)


async def test_a_missed_morning_is_never_caught_up_and_loses_nothing_the_evening_holds(session_factory):
    await _universe(session_factory, ("AAA", "BBB"))
    schwab = FakeSchwab()
    await _run(session_factory, schwab)
    schwab.calls.clear()

    # The daemon comes back at noon: the morning is recorded as missed, and the evening's open interest stands.
    report = await _run_morning(session_factory, schwab, FakeClock(OPEN + timedelta(hours=2, minutes=30)))

    assert schwab.calls == []
    assert report.past_deadline == 2
    assert (await _morning_record(session_factory, "AAA")).status is FetchStatus.FAILED
    assert (await _record(session_factory, "AAA")).status is FetchStatus.OK
    assert await _contract_rows(session_factory, "AAA") == 6


async def test_more_than_about_five_percent_of_morning_underlyings_failing_fails_the_pass(session_factory):
    await _universe(session_factory, tuple(f"S{n:02d}" for n in range(20)))
    schwab = FakeSchwab()
    for symbol in ("S01", "S05", "S09"):
        schwab.script[symbol] = [502] * 5
    tally = SweepTally()

    await _run_morning(session_factory, schwab, tally=tally)

    assert (tally.attempted, tally.failed) == (20, 3)
    assert tally.verdict is SweepVerdict.FAILING
