"""The nightly earnings snapshot and the stored calendar the Catalyst reads, against a real
database and a scripted Nasdaq. Payloads are synthetic."""

import logging
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import select

from rs_domain import EarningsOrigin, EarningsTime, StoredEarnings
from rs_ingest.earnings_snapshot import (
    FORWARD_WEEKDAYS,
    EarningsSnapshotError,
    RetryingDays,
    StoredEarningsCalendar,
    snapshot_earnings,
)
from rs_persistence import EarningsSnapshotRepository, EarningsSnapshotRow
from rs_sources import ExchangeCalendar
from rs_sources.earnings import EarningsLookup, NextEarnings
from rs_sources.edgar import Filing8K
from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsDay, EarningsListing

CALENDAR = ExchangeCalendar()
# Tuesday 2026-10-27, 20:45 Eastern: past the snapshot slot, and already the 28th in UTC.
TONIGHT = date(2026, 10, 27)
EVENING = datetime(2026, 10, 28, 0, 45, tzinfo=UTC)
FETCH_TIME = datetime(2026, 10, 28, 0, 45, 7, tzinfo=UTC)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


def listing(ticker: str, day: date, **overrides) -> EarningsListing:
    fields = {
        "ticker": ticker,
        "report_date": day,
        "time_of_day": EarningsTime.AFTER_HOURS,
        "fiscal_quarter": "2026-09",
        "eps_forecast": 1.25,
        "estimate_count": 7,
        "eps_actual": None,
        "surprise_pct": None,
    }
    return EarningsListing(**(fields | overrides))


class FakeNasdaq:
    """A scripted calendar: `days` maps a date to its listings; `failing` days raise."""

    def __init__(self, days=None, failing=(), unparsed=None, fail_times=None):
        self.days = days or {}
        self.failing = set(failing)
        # Days that fail their first n requests, then answer.
        self.fail_times = dict(fail_times or {})
        self.unparsed = unparsed or {}
        self.requested: list[date] = []

    async def day(self, day: date) -> EarningsDay:
        self.requested.append(day)
        if day in self.failing:
            raise SourceError("GET failed", status_code=503)
        if self.fail_times.get(day, 0) > 0:
            self.fail_times[day] -= 1
            raise SourceError("GET failed", status_code=503)
        return EarningsDay(
            day=day,
            fetched_at=FETCH_TIME,
            listings=tuple(self.days.get(day, ())),
            unparsed=self.unparsed.get(day, {}),
        )


def window_days():
    """A realistic window: a listing on every weekday from 10-28 to 12-03 except Thanksgiving, and
    a past reporter that already published."""
    days = {date(2026, 10, 22): [listing("PAST", date(2026, 10, 22), eps_actual=1.3, surprise_pct=4.0)]}
    day = date(2026, 10, 28)
    while day <= date(2026, 12, 3):
        if day.weekday() < 5 and day != date(2026, 11, 26):
            days[day] = [listing(f"T{day:%m%d}", day)]
        day += timedelta(days=1)
    days[date(2026, 10, 29)].append(listing("BBB", date(2026, 10, 29), time_of_day=None))
    return days


ROWS = sum(len(listings) for listings in window_days().values())


async def take(session_factory, nasdaq, now=EVENING):
    return await snapshot_earnings(
        session_factory=session_factory, source=nasdaq, calendar=CALENDAR, clock=FakeClock(now)
    )


async def stored_rows(session_factory) -> list[EarningsSnapshotRow]:
    async with session_factory() as session:
        return list(await session.scalars(select(EarningsSnapshotRow)))


def forward_weekdays(count: int = FORWARD_WEEKDAYS) -> list[date]:
    days, day = [], TONIGHT
    while len(days) < count:
        day += timedelta(days=1)
        if day.weekday() < 5:
            days.append(day)
    return days


async def test_the_snapshot_reads_a_week_back_and_a_fixed_horizon_forward(session_factory):
    nasdaq = FakeNasdaq(window_days())

    report = await take(session_factory, nasdaq)

    weekdays_back = [date(2026, 10, d) for d in (20, 21, 22, 23, 26, 27)]
    assert nasdaq.requested == weekdays_back + forward_weekdays()
    assert len(nasdaq.requested) == len(set(nasdaq.requested))
    assert all(day.weekday() < 5 for day in nasdaq.requested)
    assert report is not None
    assert (report.rows, report.last_listed_day, report.snapshot_date) == (ROWS, date(2026, 12, 3), TONIGHT)
    assert report.days_read == len(weekdays_back) + FORWARD_WEEKDAYS


async def test_every_listing_is_stored_with_the_eastern_snapshot_date_and_its_fetch_time(session_factory):
    await take(session_factory, FakeNasdaq(window_days()))

    rows = await stored_rows(session_factory)
    assert len(rows) == ROWS and {"PAST", "BBB", "T1029", "T1127", "T1203"} <= {r.ticker for r in rows}
    assert {r.snapshot_date for r in rows} == {TONIGHT}  # the UTC date is already the 28th
    assert {r.origin for r in rows} == {"nightly"}
    assert {r.fetched_at for r in rows} == {FETCH_TIME}
    past = next(r for r in rows if r.ticker == "PAST")
    assert (past.eps_actual, past.surprise_pct, past.report_date) == (1.3, 4.0, date(2026, 10, 22))
    assert next(r for r in rows if r.ticker == "BBB").time_of_day is None


async def test_a_thin_stretch_inside_the_window_does_not_end_the_read(session_factory):
    # No listings from 12-04 to 12-31 (a quiet holiday stretch), then more in January.
    days = window_days() | {date(2027, 1, 5): [listing("LATE", date(2027, 1, 5))]}

    report = await take(session_factory, FakeNasdaq(days))

    assert report is not None and report.last_listed_day == date(2027, 1, 5)
    assert any(r.ticker == "LATE" for r in await stored_rows(session_factory))


async def test_the_last_listed_day_is_logged(session_factory, caplog):
    with caplog.at_level(logging.INFO, logger="rs_ingest.earnings_snapshot"):
        await take(session_factory, FakeNasdaq(window_days()))

    [line] = [r.getMessage() for r in caplog.records if "earnings snapshot" in r.getMessage()]
    assert "last listed day 2026-12-03" in line


async def test_unparsed_values_are_summed_over_the_window_and_logged_per_field(session_factory, caplog):
    nasdaq = FakeNasdaq(
        window_days(),
        unparsed={date(2026, 10, 22): {"eps_actual": 2}, date(2026, 10, 29): {"eps_forecast": 3, "eps_actual": 1}},
    )

    with caplog.at_level(logging.INFO, logger="rs_ingest.earnings_snapshot"):
        report = await take(session_factory, nasdaq)

    assert report is not None and dict(report.unparsed) == {"eps_actual": 3, "eps_forecast": 3}
    [line] = [r.getMessage() for r in caplog.records if "earnings snapshot" in r.getMessage()]
    assert "'eps_actual': 3" in line and "'eps_forecast': 3" in line


@pytest.mark.parametrize(
    ("label", "now"),
    [
        ("Saturday evening", datetime(2026, 11, 1, 1, 0, tzinfo=UTC)),
        ("Thanksgiving evening", datetime(2026, 11, 27, 2, 0, tzinfo=UTC)),
        ("Sunday evening", datetime(2026, 11, 2, 2, 0, tzinfo=UTC)),
        ("before the slot", datetime(2026, 10, 27, 23, 59, tzinfo=UTC)),  # 19:59 ET
        ("the morning", datetime(2026, 10, 27, 14, 0, tzinfo=UTC)),
    ],
)
async def test_nothing_is_due_off_a_trading_evening(session_factory, label, now):
    nasdaq = FakeNasdaq(window_days())

    assert await take(session_factory, nasdaq, now) is None

    assert nasdaq.requested == []
    assert await stored_rows(session_factory) == []


async def test_a_rerun_on_a_night_already_snapshotted_does_nothing(session_factory):
    await take(session_factory, FakeNasdaq(window_days()))
    nasdaq = FakeNasdaq(window_days())

    assert await take(session_factory, nasdaq, EVENING + timedelta(hours=1)) is None

    assert nasdaq.requested == []
    assert len(await stored_rows(session_factory)) == ROWS


async def test_a_backfill_run_the_same_day_does_not_count_as_the_nights_snapshot(session_factory):
    async with session_factory() as session, session.begin():
        await EarningsSnapshotRepository(session).insert_many(
            [
                StoredEarnings(
                    snapshot_date=TONIGHT,
                    origin=EarningsOrigin.BACKFILL,
                    fetched_at=FETCH_TIME,
                    **listing("OLD", date(2008, 1, 15)).model_dump(),
                )
            ]
        )

    report = await take(session_factory, FakeNasdaq(window_days()))

    assert report is not None and report.rows == ROWS
    assert len(await stored_rows(session_factory)) == ROWS + 1


async def test_a_failed_day_stores_nothing_ends_the_read_and_the_next_run_takes_the_snapshot(session_factory):
    failing = FakeNasdaq(window_days(), failing={date(2026, 10, 23)})

    with pytest.raises(EarningsSnapshotError, match="2026-10-23 failed to read after 3 good") as raised:
        await take(session_factory, failing)

    assert failing.requested[-1] == date(2026, 10, 23)  # nothing after the failed day is asked
    assert raised.value.reason == "a calendar day failed"
    assert await stored_rows(session_factory) == []

    report = await take(session_factory, FakeNasdaq(window_days()), EVENING + timedelta(minutes=30))
    assert report is not None and report.rows == ROWS


async def test_a_failed_forward_day_stores_nothing(session_factory):
    nasdaq = FakeNasdaq(window_days(), failing={date(2026, 11, 3)})

    with pytest.raises(EarningsSnapshotError):
        await take(session_factory, nasdaq)

    assert nasdaq.requested[-1] == date(2026, 11, 3)
    assert await stored_rows(session_factory) == []


async def test_a_window_with_no_listings_at_all_stores_nothing(session_factory):
    with pytest.raises(EarningsSnapshotError) as raised:
        await take(session_factory, FakeNasdaq())

    assert raised.value.reason == "no earnings listed"
    assert await stored_rows(session_factory) == []


class Sleeps:
    def __init__(self) -> None:
        self.seconds: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.seconds.append(seconds)


async def test_a_day_that_fails_once_is_asked_again_and_the_snapshot_is_whole(session_factory):
    day = date(2026, 10, 23)
    nasdaq = FakeNasdaq(window_days(), fail_times={day: 1})
    sleeps = Sleeps()

    report = await take(session_factory, RetryingDays(nasdaq, sleep=sleeps))

    assert nasdaq.requested.count(day) == 2
    assert sleeps.seconds == [30.0]
    assert report is not None and report.rows == ROWS


async def test_a_day_that_fails_three_times_stores_nothing(session_factory):
    day = date(2026, 10, 23)
    nasdaq = FakeNasdaq(window_days(), failing={day})
    sleeps = Sleeps()

    with pytest.raises(EarningsSnapshotError):
        await take(session_factory, RetryingDays(nasdaq, sleep=sleeps))

    assert nasdaq.requested.count(day) == 3
    assert sleeps.seconds == [30.0, 120.0]
    assert await stored_rows(session_factory) == []


# --- the stored calendar the Catalyst reads ---


async def store_snapshot(session_factory, snapshot_date: date, *listings: EarningsListing, origin=EarningsOrigin.NIGHTLY):
    async with session_factory() as session, session.begin():
        await EarningsSnapshotRepository(session).insert_many(
            [
                StoredEarnings(
                    snapshot_date=snapshot_date, origin=origin, fetched_at=FETCH_TIME, **item.model_dump()
                )
                for item in listings
            ]
        )


def stored_calendar(session_factory) -> StoredEarningsCalendar:
    return StoredEarningsCalendar(session_factory, CALENDAR)


async def test_the_first_listing_on_or_after_today_in_the_latest_snapshot_is_returned(session_factory):
    await store_snapshot(session_factory, date(2026, 10, 26), listing("AAA", date(2026, 11, 5)))
    await store_snapshot(
        session_factory,
        TONIGHT,
        listing("AAA", date(2026, 10, 22), eps_actual=1.0),
        listing("AAA", date(2026, 10, 29), time_of_day=EarningsTime.PRE_MARKET),
        listing("AAA", date(2027, 1, 28)),
    )

    found = await stored_calendar(session_factory).next_listed("aaa", TONIGHT)

    assert found is not None
    assert (found.report_date, found.time_of_day, found.snapshot_date) == (
        date(2026, 10, 29),
        EarningsTime.PRE_MARKET,
        TONIGHT,
    )


async def test_a_company_reporting_today_is_listed_for_today(session_factory):
    await store_snapshot(session_factory, TONIGHT, listing("AAA", TONIGHT))

    found = await stored_calendar(session_factory).next_listed("AAA", TONIGHT)

    assert found is not None and found.report_date == TONIGHT


async def test_a_ticker_absent_from_a_fresh_snapshot_is_none(session_factory):
    await store_snapshot(session_factory, TONIGHT, listing("AAA", date(2026, 10, 29)))

    assert await stored_calendar(session_factory).next_listed("ZZZ", TONIGHT) is None


async def test_a_ticker_whose_only_rows_are_past_is_none(session_factory):
    await store_snapshot(session_factory, TONIGHT, listing("AAA", date(2026, 10, 22)))

    assert await stored_calendar(session_factory).next_listed("AAA", TONIGHT) is None


async def test_no_snapshot_at_all_is_none(session_factory):
    assert await stored_calendar(session_factory).next_listed("AAA", TONIGHT) is None


async def test_backfill_rows_are_never_the_latest_snapshot(session_factory):
    await store_snapshot(session_factory, date(2026, 10, 26), listing("AAA", date(2026, 11, 5)))
    # A backfill run today stores rows dated today, some of them in the future of an old report.
    await store_snapshot(
        session_factory, TONIGHT, listing("AAA", date(2026, 10, 30)), origin=EarningsOrigin.BACKFILL
    )

    found = await stored_calendar(session_factory).next_listed("AAA", TONIGHT)

    assert found is not None and found.report_date == date(2026, 11, 5)


@pytest.mark.parametrize(
    ("snapshot", "today", "fresh"),
    [
        # Friday's snapshot over a weekend: Monday is session 1, Tuesday 2, Wednesday 3.
        (date(2026, 10, 23), date(2026, 10, 24), True),
        (date(2026, 10, 23), date(2026, 10, 26), True),
        (date(2026, 10, 23), date(2026, 10, 27), True),
        (date(2026, 10, 23), date(2026, 10, 28), False),
        # Wednesday before Thanksgiving: Friday is session 1, Monday 2, Tuesday 3.
        (date(2026, 11, 25), date(2026, 11, 26), True),
        (date(2026, 11, 25), date(2026, 11, 30), True),
        (date(2026, 11, 25), date(2026, 12, 1), False),
        # Thursday before Good Friday (closed) and the weekend: Monday 1, Tuesday 2, Wednesday 3.
        (date(2026, 4, 2), date(2026, 4, 6), True),
        (date(2026, 4, 2), date(2026, 4, 7), True),
        (date(2026, 4, 2), date(2026, 4, 8), False),
        # The same night.
        (date(2026, 10, 27), date(2026, 10, 27), True),
    ],
)
async def test_a_snapshot_is_trusted_for_two_trading_sessions(session_factory, snapshot, today, fresh):
    await store_snapshot(session_factory, snapshot, listing("AAA", date(2027, 1, 1)))

    found = await stored_calendar(session_factory).next_listed("AAA", today)

    assert (found is not None) == fresh


# --- the lookup over the stored calendar ---


class FakeFilings:
    def __init__(self, filings=()):
        self.filings = list(filings)

    async def recent_8ks(self, ticker, as_of, limit=5):
        return self.filings


def results_8k(filed: date) -> Filing8K:
    return Filing8K(
        accession_number="acc-1",
        filing_date=filed,
        accepted_at=datetime.combine(filed, datetime.min.time(), tzinfo=UTC),
        form="8-K",
        items=("2.02",),
        url="https://example.invalid/doc",
    )


def lookup(session_factory) -> EarningsLookup:
    return EarningsLookup(stored_calendar(session_factory), FakeFilings([results_8k(date(2026, 7, 30))]))


AS_OF = datetime(2026, 10, 27, 15, 0, tzinfo=UTC)


async def test_the_catalyst_reads_the_fresh_snapshot_not_the_estimate(session_factory):
    await store_snapshot(
        session_factory, date(2026, 10, 26), listing("AAA", date(2026, 11, 3), time_of_day=EarningsTime.PRE_MARKET)
    )

    result = await lookup(session_factory).next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 11, 3), time_of_day=EarningsTime.PRE_MARKET, estimated=False)


async def test_a_ticker_missing_from_a_fresh_snapshot_gets_the_edgar_estimate(session_factory):
    await store_snapshot(session_factory, date(2026, 10, 26), listing("BBB", date(2026, 11, 3)))

    result = await lookup(session_factory).next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 30), estimated=True)


async def test_a_snapshot_more_than_two_sessions_old_falls_back_to_the_estimate_and_stores_nothing(session_factory):
    await store_snapshot(session_factory, date(2026, 10, 22), listing("AAA", date(2026, 11, 3)))

    result = await lookup(session_factory).next_earnings("AAA", AS_OF)  # Tuesday: Thursday's snapshot is 3 sessions old

    assert result == NextEarnings(date=date(2026, 10, 30), estimated=True)
    assert len(await stored_rows(session_factory)) == 1


async def test_an_unreadable_database_falls_back_to_the_edgar_estimate_with_a_warning(
    session_factory, make_flaky, caplog
):
    broken = EarningsLookup(
        StoredEarningsCalendar(make_flaky(failures=5), CALENDAR), FakeFilings([results_8k(date(2026, 7, 30))])
    )

    with caplog.at_level(logging.WARNING, logger="rs_ingest.earnings_snapshot"):
        result = await broken.next_earnings("AAA", AS_OF)

    assert result == NextEarnings(date=date(2026, 10, 30), estimated=True)
    assert any("unreadable" in r.getMessage() for r in caplog.records)


async def test_with_no_snapshot_and_no_filing_the_answer_is_unknown(session_factory):
    unknown = EarningsLookup(stored_calendar(session_factory), FakeFilings())

    assert await unknown.next_earnings("AAA", AS_OF) == NextEarnings.unknown()
