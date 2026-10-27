"""The one-time earnings backfill: weekday walk, resume and labelling, against a real database."""

from datetime import UTC, date, datetime

from sqlalchemy import select

from rs_domain import EarningsOrigin, EarningsTime, StoredEarnings
from rs_ingest.earnings_snapshot import RetryingDays
from rs_ingest.tools import earnings_backfill
from rs_ingest.tools.earnings_backfill import (
    MAX_CONSECUTIVE_FAILURES,
    backfill_earnings,
)
from rs_persistence import EarningsSnapshotRepository, EarningsSnapshotRow
from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsDay, EarningsListing

# 2008-01-14 is a Monday.
MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY = (date(2008, 1, d) for d in range(14, 19))
# 23:30 Eastern on the 3rd, already the 4th in UTC.
LATE_NIGHT = datetime(2026, 11, 4, 4, 30, tzinfo=UTC)


def listing(ticker: str, day: date) -> EarningsListing:
    return EarningsListing(
        ticker=ticker,
        report_date=day,
        time_of_day=EarningsTime.PRE_MARKET,
        fiscal_quarter="2007-12",
        eps_forecast=-0.5,
        estimate_count=4,
        eps_actual=-0.75,
        surprise_pct=-50.0,
    )


class FakeNasdaq:
    def __init__(self, days=None, failing=(), fetched_at=LATE_NIGHT, fail_times=None):
        self.days = days or {}
        self.failing = set(failing)
        self.fail_times = dict(fail_times or {})
        self.fetched_at = fetched_at
        self.requested: list[date] = []

    async def day(self, day: date) -> EarningsDay:
        self.requested.append(day)
        if day in self.failing:
            raise SourceError("GET failed", status_code=503)
        if self.fail_times.get(day, 0) > 0:
            self.fail_times[day] -= 1
            raise SourceError("GET failed", status_code=503)
        return EarningsDay(
            day=day, fetched_at=self.fetched_at, listings=tuple(self.days.get(day, ())), unparsed={}
        )


async def stored_rows(session_factory) -> list[EarningsSnapshotRow]:
    async with session_factory() as session:
        return list(await session.scalars(select(EarningsSnapshotRow).order_by(EarningsSnapshotRow.report_date)))


async def run(session_factory, nasdaq, start=MONDAY, end=FRIDAY):
    return await backfill_earnings(session_factory=session_factory, source=nasdaq, start=start, end=end)


async def test_each_weekday_is_asked_once_and_weekends_are_not(session_factory):
    nasdaq = FakeNasdaq()

    report = await run(session_factory, nasdaq, MONDAY, date(2008, 1, 22))

    assert nasdaq.requested == [
        *(date(2008, 1, d) for d in range(14, 19)),
        *(date(2008, 1, d) for d in range(21, 23)),
    ]
    assert (report.days_total, report.fetched, report.skipped) == (7, 7, 0)


async def test_rows_are_labelled_backfill_with_the_fetch_time_and_the_eastern_fetch_date(session_factory):
    nasdaq = FakeNasdaq({TUESDAY: [listing("AAA", TUESDAY), listing("BBB", TUESDAY)]})

    report = await run(session_factory, nasdaq)

    rows = await stored_rows(session_factory)
    assert [(r.ticker, r.report_date) for r in rows] == [("AAA", TUESDAY), ("BBB", TUESDAY)]
    assert {r.origin for r in rows} == {"backfill"}
    assert {r.fetched_at for r in rows} == {LATE_NIGHT}
    assert {r.snapshot_date for r in rows} == {date(2026, 11, 3)}  # not the UTC date of the fetch
    assert (rows[0].eps_actual, rows[0].surprise_pct, rows[0].fiscal_quarter) == (-0.75, -50.0, "2007-12")
    assert report.rows == 2


async def test_a_rerun_skips_days_already_stored_and_asks_again_only_for_days_without_rows(session_factory):
    days = {MONDAY: [listing("A", MONDAY)], WEDNESDAY: [listing("B", WEDNESDAY)]}
    await run(session_factory, FakeNasdaq(days))

    again = FakeNasdaq(days)
    report = await run(session_factory, again)

    # Tuesday, Thursday and Friday had no rows, so a rerun cannot tell them from days never fetched.
    assert again.requested == [TUESDAY, THURSDAY, FRIDAY]
    assert (report.skipped, report.fetched, report.rows) == (2, 3, 0)
    assert len(await stored_rows(session_factory)) == 2


async def test_a_failed_day_is_left_for_the_rerun_and_the_rest_are_stored(session_factory):
    days = {d: [listing("A", d)] for d in (MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY)}
    first = await run(session_factory, FakeNasdaq(days, failing={WEDNESDAY}))

    assert first.failed == [WEDNESDAY]
    assert {r.report_date for r in await stored_rows(session_factory)} == {MONDAY, TUESDAY, THURSDAY, FRIDAY}

    rerun = FakeNasdaq(days)
    second = await run(session_factory, rerun)

    assert rerun.requested == [WEDNESDAY]
    assert second.failed == [] and second.rows == 1
    assert len(await stored_rows(session_factory)) == 5


async def test_a_transient_failure_is_retried_so_it_leaves_no_hole(session_factory):
    days = {d: [listing("A", d)] for d in (MONDAY, TUESDAY, WEDNESDAY)}
    nasdaq = FakeNasdaq(days, fail_times={TUESDAY: 2})
    sleeps = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    report = await run(session_factory, RetryingDays(nasdaq, sleep=sleep), MONDAY, WEDNESDAY)

    assert sleeps == [30.0, 120.0]
    assert report.failed == [] and report.rows == 3


async def test_a_run_stops_after_a_streak_of_failed_days(session_factory):
    start = date(2008, 1, 1)
    nasdaq = FakeNasdaq(failing={d for d in (date(2008, 1, n) for n in range(1, 29))})

    report = await run(session_factory, nasdaq, start, date(2008, 1, 31))

    assert report.stopped_early
    assert len(nasdaq.requested) == MAX_CONSECUTIVE_FAILURES
    assert len(report.failed) == MAX_CONSECUTIVE_FAILURES


async def test_a_success_resets_the_failure_streak(session_factory):
    start, end = date(2008, 1, 1), date(2008, 1, 31)
    failing = {d for d in (date(2008, 1, n) for n in range(1, 32)) if d.weekday() < 5 and d.day % 2 == 0}

    report = await run(session_factory, FakeNasdaq(failing=failing), start, end)

    assert not report.stopped_early
    assert len(report.failed) == len(failing)


async def test_a_backfill_row_does_not_displace_a_nightly_row_of_the_same_report_date(session_factory):
    async with session_factory() as session, session.begin():
        await EarningsSnapshotRepository(session).insert_many(
            [
                StoredEarnings(
                    snapshot_date=date(2026, 11, 3),
                    origin=EarningsOrigin.NIGHTLY,
                    fetched_at=LATE_NIGHT,
                    **listing("A", MONDAY).model_dump(),
                )
            ]
        )

    await run(session_factory, FakeNasdaq({MONDAY: [listing("A", MONDAY)]}))

    assert sorted(r.origin for r in await stored_rows(session_factory)) == ["backfill", "nightly"]


def test_the_tool_needs_a_database_url(monkeypatch, capsys):
    monkeypatch.delenv(earnings_backfill.DSN_VARIABLE, raising=False)
    monkeypatch.setattr("sys.argv", ["earnings_backfill"])

    assert earnings_backfill.main() == 2
    assert "RS_DB_URL is not set" in capsys.readouterr().err
