"""The daily Universe snapshot against synthetic directory files and a real database.

Time is a fake clock that the injected sleep advances, so the 15-minute retries and the 20:00
ET give-up run instantly; the trading calendar is the real XNYS one (2026-10-26 is a Monday,
2026-11-27 an early close at 13:00 ET).
"""

from datetime import UTC, date, datetime, timedelta

import pytest

from rs_domain import EASTERN, UniverseCounts, UniverseSnapshot
from rs_ingest.universe import UniverseSnapshotter
from rs_observability import HealthTracker
from rs_persistence import UniverseRepository
from rs_sources import ExchangeCalendar
from rs_sources.symbol_directory import SymbolDirectory
from rs_sources.testing import (
    NASDAQ_LISTED_HEADER,
    OTHER_LISTED_HEADER,
    FakeDirectoryHost,
    cboe_file,
    listing_file,
    nasdaq_row,
    options_file,
    other_row,
)

MONDAY = date(2026, 10, 26)
FRIDAY = date(2026, 10, 23)
CALENDAR = ExchangeCalendar()
MINUTES_15 = 15 * 60.0
# From the 18:05 start: 18:20 ... 19:50 is seven 15-minute waits, then ten minutes to 20:00.
WAITS_TO_GIVE_UP = [MINUTES_15] * 7 + [10 * 60.0]


def et(hour: int, minute: int, day: date = MONDAY) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=EASTERN).astimezone(UTC)


class Evening:
    """A fake clock, its injected sleep (which advances it) and the directory host.

    `publish` sets what the vendors serve; `on_sleep` lets a test change it as time passes.
    """

    def __init__(self, session_factory, start: datetime) -> None:
        self.moment = start
        self.sleeps: list[float] = []
        self.host = FakeDirectoryHost()
        self.health = HealthTracker("ingest")
        self.session_factory = session_factory
        self.on_sleep = lambda now: None
        self.publish(et(18, 1, start.astimezone(EASTERN).date()))

    def now(self) -> datetime:
        return self.moment

    def snapshotter(self) -> UniverseSnapshotter:
        return UniverseSnapshotter(
            self.session_factory,
            SymbolDirectory(self.host.client()),
            self,
            CALENDAR,
            self.health,
            sleep=self._sleep,
        )

    def publish(
        self,
        stamp: datetime,
        *,
        nasdaq=("AAA", "BBB"),
        other=("CCC", "DDD"),
        options=("AAA", "XOPT"),
        cboe=("CCC", "SPX"),
        options_stamp: datetime | None = None,
    ) -> None:
        self.host.bodies["nasdaqlisted.txt"] = listing_file(
            NASDAQ_LISTED_HEADER, [nasdaq_row(s) for s in nasdaq], stamp
        )
        self.host.bodies["otherlisted.txt"] = listing_file(OTHER_LISTED_HEADER, [other_row(s) for s in other], stamp)
        self.host.bodies["options.txt"] = options_file(options, options_stamp or stamp)
        self.host.bodies["cboe"] = cboe_file(list(cboe))

    async def _sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.moment += timedelta(seconds=seconds)
        self.on_sleep(self.moment)


async def stored(session_factory) -> UniverseSnapshot | None:
    async with session_factory() as s:
        return await UniverseRepository(s).latest()


def flags(snapshot: UniverseSnapshot) -> dict[str, bool]:
    return {x.symbol: x.optionable for x in snapshot.listings}


async def test_a_fresh_evening_stores_every_non_test_listing_marked_optionable_by_exact_symbol(
    session_factory, notices
):
    evening = Evening(session_factory, et(18, 5))
    evening.host.bodies["nasdaqlisted.txt"] = listing_file(
        NASDAQ_LISTED_HEADER,
        [nasdaq_row("AAA"), nasdaq_row("TESTQ", test=True), nasdaq_row("BAD", status="D"), nasdaq_row("QQQE", etf=True)],
        et(18, 1),
    )
    evening.host.bodies["otherlisted.txt"] = listing_file(
        OTHER_LISTED_HEADER,
        [
            other_row("BRK.B", cqs="BRK.B", nasdaq="BRK-B"),
            other_row("ABR$D", cqs="ABRpD", nasdaq="ABR-D"),
            other_row("ODD", exchange="?"),
            other_row("AAA", exchange="P"),  # a symbol both files list: the first file's row stays
            other_row("TESTN", test=True),
        ],
        et(18, 2),
    )
    evening.host.bodies["options.txt"] = options_file(["AAA", "ABR$D", "BRKB"], et(18, 2))
    evening.host.bodies["cboe"] = cboe_file(["BRK.B", "SPX", "QQQE"])

    use = await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None
    assert (use.session, use.snapshot_date, use.fresh) == (MONDAY, MONDAY, True)
    assert use.as_of == snapshot.as_of == et(18, 1)  # the earlier of the two listing files
    assert snapshot.counts == UniverseCounts(nasdaq_listed=3, other_listed=4, options_symbols=3, cboe_symbols=3)
    assert not snapshot.optionable_carried
    by_symbol = {x.symbol: x for x in snapshot.listings}
    assert sorted(by_symbol) == ["AAA", "ABR$D", "BAD", "BRK.B", "ODD", "QQQE"]
    # Optionable is an exact match: BRKB (options.txt strips the dot) does not mark BRK.B; Cboe does.
    assert flags(snapshot) == {"AAA": True, "ABR$D": True, "BAD": False, "BRK.B": True, "ODD": False, "QQQE": True}
    assert (by_symbol["AAA"].exchange, by_symbol["BAD"].financial_status) == ("Q", "D")
    assert by_symbol["QQQE"].etf is True
    assert by_symbol["ODD"].exchange == "?" and by_symbol["ODD"].financial_status is None
    assert (by_symbol["ABR$D"].cqs_symbol, by_symbol["ABR$D"].nasdaq_symbol) == ("ABRpD", "ABR-D")
    assert (by_symbol["BRK.B"].cqs_symbol, by_symbol["BRK.B"].nasdaq_symbol) == ("BRK.B", "BRK-B")
    assert evening.sleeps == []
    assert notices == []


async def test_files_stamped_before_the_close_are_waited_for_every_fifteen_minutes(session_factory, notices):
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(15, 59))  # the midday file: before the 16:00 close

    def refreshed_at_half_past_six(now: datetime) -> None:
        if now >= et(18, 30):
            evening.publish(et(18, 30))

    evening.on_sleep = refreshed_at_half_past_six

    use = await evening.snapshotter().take()

    assert evening.sleeps == [MINUTES_15, MINUTES_15]  # attempts at 18:05 and 18:20 found the old file
    assert (use.snapshot_date, use.fresh) == (MONDAY, True)
    assert (await stored(session_factory)) is not None
    assert notices == []


async def test_a_file_stamped_exactly_at_the_close_is_not_after_it(session_factory):
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(16, 0))

    use = await evening.snapshotter().take()

    assert not use.fresh
    assert evening.sleeps == WAITS_TO_GIVE_UP


async def test_an_early_close_counts_so_a_mid_afternoon_file_is_accepted(session_factory):
    early_close_day = date(2026, 11, 27)  # closes at 13:00 ET
    evening = Evening(session_factory, et(18, 5, early_close_day))
    evening.publish(et(14, 0, early_close_day))

    use = await evening.snapshotter().take()

    assert (use.snapshot_date, use.fresh) == (early_close_day, True)
    assert evening.sleeps == []


async def test_at_twenty_hundred_it_gives_up_uses_the_previous_snapshot_and_pings_ops(
    session_factory, seed_previous, notices
):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(12, 0))  # never refreshed

    use = await evening.snapshotter().take()

    assert evening.sleeps == WAITS_TO_GIVE_UP
    assert evening.now() == et(20, 0)
    assert (use.session, use.snapshot_date, use.as_of, use.fresh) == (MONDAY, FRIDAY, et(18, 1, FRIDAY), False)
    snapshot = await stored(session_factory)
    assert snapshot is not None and snapshot.snapshot_date == FRIDAY
    assert notices == ["Nasdaq Trader is failing (nasdaqlisted.txt not updated since the close). See the ingest logs."]


async def test_with_no_snapshot_at_all_the_night_has_nothing_to_use(session_factory, notices):
    evening = Evening(session_factory, et(18, 5))
    evening.host.statuses["nasdaqlisted.txt"] = 503

    use = await evening.snapshotter().take()

    assert (use.session, use.snapshot_date, use.as_of, use.fresh) == (MONDAY, None, None, False)
    assert len(notices) == 1


async def test_a_recovery_after_a_failed_night_pings_once(session_factory, seed_previous, notices):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(12, 0))
    await evening.snapshotter().take()
    tuesday = date(2026, 10, 27)
    evening.moment = et(18, 5, tuesday)
    evening.publish(et(18, 1, tuesday))

    use = await evening.snapshotter().take()

    assert (use.snapshot_date, use.fresh) == (tuesday, True)
    assert notices[-1] == "Nasdaq Trader recovered."
    assert len(notices) == 2


@pytest.mark.parametrize("which", ["nasdaq", "other"])
async def test_a_listing_file_far_smaller_than_the_previous_one_is_treated_as_stale(
    session_factory, seed_previous, notices, which
):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(18, 1), **{which: ("AAA",)})  # 1 entry against 2 the time before

    use = await evening.snapshotter().take()

    assert not use.fresh and use.snapshot_date == FRIDAY
    assert evening.sleeps == WAITS_TO_GIVE_UP
    assert "looks truncated" in notices[0]
    snapshot = await stored(session_factory)
    assert snapshot is not None and snapshot.snapshot_date == FRIDAY


async def test_a_truncated_listing_file_that_is_later_served_whole_is_accepted(session_factory, seed_previous, notices):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(18, 1), nasdaq=("AAA",))
    evening.on_sleep = lambda now: evening.publish(et(18, 1))

    use = await evening.snapshotter().take()

    assert (use.snapshot_date, use.fresh) == (MONDAY, True)
    assert evening.sleeps == [MINUTES_15]
    assert notices == []


@pytest.mark.parametrize(("today", "accepted"), [(90, True), (89, False)])
async def test_the_truncation_threshold_is_ninety_percent_of_the_previous_count(
    session_factory, seed_previous, today, accepted
):
    await seed_previous(nasdaq_listed=100, other_listed=1, options_symbols=1, cboe_symbols=1)
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(18, 1), nasdaq=[f"N{n:03d}" for n in range(today)], other=("CCC",))

    use = await evening.snapshotter().take()

    assert use.fresh is accepted


async def test_a_failed_options_file_keeps_the_previous_optionable_names_adds_cboes_and_pings(
    session_factory, seed_previous, notices
):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.host.statuses["options.txt"] = 500

    use = await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and use.fresh
    assert evening.sleeps == WAITS_TO_GIVE_UP  # retried until the give-up, then carried
    assert snapshot.optionable_carried
    assert snapshot.counts.options_symbols is None and snapshot.counts.cboe_symbols == 2
    # AAA stays optionable from the previous snapshot, CCC is new from Cboe; OLD left the listings.
    assert flags(snapshot) == {"AAA": True, "BBB": False, "CCC": True, "DDD": False}
    assert notices == ["Nasdaq options list is failing (options.txt could not be read). See the ingest logs."]


async def test_a_transient_options_failure_is_retried_and_the_snapshot_is_fresh_not_carried(
    session_factory, seed_previous, notices
):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.host.statuses["options.txt"] = 503
    evening.on_sleep = lambda now: evening.host.statuses.clear()

    use = await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and use.fresh and not snapshot.optionable_carried
    assert evening.sleeps == [MINUTES_15]
    assert snapshot.counts.options_symbols == 2
    assert flags(snapshot) == {"AAA": True, "BBB": False, "CCC": True, "DDD": False}  # OLD is not carried
    assert notices == []


async def test_a_cut_options_download_is_retried_until_it_is_whole(session_factory, seed_previous, notices):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    whole = evening.host.bodies["options.txt"]
    evening.host.bodies["options.txt"] = whole.rsplit("File Creation", 1)[0]  # no footer: cut short
    evening.on_sleep = lambda now: evening.host.bodies.update({"options.txt": whole})

    await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and not snapshot.optionable_carried
    assert evening.sleeps == [MINUTES_15]
    assert notices == []


async def test_a_transient_cboe_failure_is_retried_and_the_snapshot_is_not_carried(
    session_factory, seed_previous, notices
):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.host.statuses["cboe"] = 503
    evening.on_sleep = lambda now: evening.host.statuses.clear()

    await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and not snapshot.optionable_carried
    assert snapshot.counts.cboe_symbols == 2
    assert evening.sleeps == [MINUTES_15]
    assert notices == []


async def test_a_truncated_options_file_is_judged_against_the_last_good_count(session_factory, seed_previous, notices):
    await seed_previous(options_symbols=100)
    evening = Evening(session_factory, et(18, 5))  # options.txt holds 2 underlyings

    await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and snapshot.optionable_carried
    assert flags(snapshot)["AAA"] is True
    assert "options.txt looks truncated" in notices[0]


async def test_a_failed_cboe_directory_carries_the_previous_names_and_pings_under_its_own_name(
    session_factory, seed_previous, notices
):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.host.statuses["cboe"] = 403

    use = await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and use.fresh and snapshot.optionable_carried
    assert evening.sleeps == WAITS_TO_GIVE_UP  # a Cboe failure that lasts to 20:00 carries
    assert snapshot.counts.cboe_symbols is None and snapshot.counts.options_symbols == 2
    assert flags(snapshot) == {"AAA": True, "BBB": False, "CCC": False, "DDD": False}
    assert notices == ["Cboe is failing (Cboe options directory could not be read). See the ingest logs."]


async def test_with_no_previous_snapshot_a_failed_source_only_means_fewer_names(session_factory):
    evening = Evening(session_factory, et(18, 5))
    evening.host.statuses["options.txt"] = 500

    await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and flags(snapshot) == {"AAA": False, "BBB": False, "CCC": True, "DDD": False}


async def test_an_options_file_not_yet_updated_is_waited_for_like_the_listings(session_factory, seed_previous, notices):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(18, 1), options_stamp=et(15, 0))
    evening.on_sleep = lambda now: evening.publish(et(18, 1), options_stamp=et(18, 2))

    use = await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and use.fresh and not snapshot.optionable_carried
    assert evening.sleeps == [MINUTES_15]
    assert notices == []


async def test_an_options_file_never_updated_is_given_up_on_at_twenty_hundred(session_factory, seed_previous, notices):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(18, 1), options_stamp=et(15, 0))

    use = await evening.snapshotter().take()

    snapshot = await stored(session_factory)
    assert snapshot is not None and use.fresh and snapshot.optionable_carried
    assert evening.sleeps == WAITS_TO_GIVE_UP
    assert notices == ["Nasdaq options list is failing (options.txt not updated since the close). See the ingest logs."]


async def test_taking_a_snapshot_that_exists_does_nothing(session_factory):
    evening = Evening(session_factory, et(18, 5))
    first = await evening.snapshotter().take()
    requests = len(evening.host.requests)

    again = await evening.snapshotter().take()

    assert again == first and requests > 0
    assert len(evening.host.requests) == requests


async def test_a_weekend_finds_fridays_snapshot_and_makes_no_request(session_factory, seed_previous):
    await seed_previous()
    saturday = date(2026, 10, 24)
    evening = Evening(session_factory, et(18, 5, saturday))

    use = await evening.snapshotter().take()

    assert (use.session, use.snapshot_date, use.fresh) == (FRIDAY, FRIDAY, True)
    assert evening.host.requests == []


async def test_a_holiday_finds_the_last_sessions_snapshot_and_makes_no_request(session_factory, seed_previous):
    thanksgiving = date(2026, 11, 26)
    wednesday = date(2026, 11, 25)
    await seed_previous(day=wednesday)
    evening = Evening(session_factory, et(18, 5, thanksgiving))

    use = await evening.snapshotter().take()

    assert (use.session, use.snapshot_date, use.fresh) == (wednesday, wednesday, True)
    assert evening.host.requests == []


@pytest.mark.parametrize("start", [et(10, 0), et(16, 30), et(18, 4)])
async def test_before_the_evening_starts_the_night_belongs_to_the_session_before(session_factory, seed_previous, start):
    await seed_previous()
    evening = Evening(session_factory, start)  # Monday, even after the close: no hours-long wait

    use = await evening.snapshotter().take()

    assert (use.session, use.snapshot_date, use.fresh) == (FRIDAY, FRIDAY, True)
    assert evening.host.requests == []


async def test_a_run_after_the_give_up_time_makes_one_attempt_without_waiting(session_factory, seed_previous, notices):
    await seed_previous()
    evening = Evening(session_factory, et(21, 30))
    evening.publish(et(12, 0))

    stale = await evening.snapshotter().take()
    evening.publish(et(18, 1))
    late = await evening.snapshotter().take()

    assert evening.sleeps == []
    assert (stale.snapshot_date, stale.fresh) == (FRIDAY, False)
    assert (late.snapshot_date, late.fresh) == (MONDAY, True)


async def test_a_late_run_pings_for_a_real_source_failure_like_an_on_time_run(session_factory, seed_previous, notices):
    await seed_previous()
    tuesday = date(2026, 10, 27)
    evening = Evening(session_factory, et(9, 0, tuesday))  # the box was down all Monday evening
    evening.publish(et(12, 0, date(2026, 10, 23)))  # Friday's stamps: not updated since Monday's close

    stale = await evening.snapshotter().take()
    evening.publish(et(18, 1))
    evening.host.statuses["options.txt"] = 503
    evening.host.statuses["cboe"] = 503
    carried = await evening.snapshotter().take()

    assert (stale.session, stale.snapshot_date, stale.fresh) == (MONDAY, FRIDAY, False)
    assert carried.fresh
    assert notices == [
        "Nasdaq Trader is failing (nasdaqlisted.txt not updated since the close). See the ingest logs.",
        "Nasdaq Trader recovered.",
        "Nasdaq options list is failing (options.txt could not be read). See the ingest logs.",
        "Cboe is failing (Cboe options directory could not be read). See the ingest logs.",
    ]
    assert evening.sleeps == []


async def test_files_stamped_after_the_next_session_opened_are_not_that_sessions_and_ping_nothing(
    session_factory, seed_previous, notices
):
    await seed_previous()
    tuesday = date(2026, 10, 27)
    evening = Evening(session_factory, et(12, 30, tuesday))  # Monday's evening ended without a snapshot
    evening.publish(et(12, 0, tuesday))  # stamped after Tuesday's 09:30 open

    use = await evening.snapshotter().take()

    assert (use.session, use.snapshot_date, use.fresh) == (MONDAY, FRIDAY, False)
    snapshot = await stored(session_factory)
    assert snapshot is not None and snapshot.snapshot_date == FRIDAY
    assert notices == []


async def test_an_options_file_stamped_after_the_next_open_is_carried_without_a_ping(
    session_factory, seed_previous, notices
):
    await seed_previous()
    tuesday = date(2026, 10, 27)
    evening = Evening(session_factory, et(12, 30, tuesday))
    evening.publish(et(18, 1), options_stamp=et(12, 0, tuesday))

    use = await evening.snapshotter().take()

    assert (use.session, use.snapshot_date, use.fresh) == (MONDAY, MONDAY, True)
    snapshot = await stored(session_factory)
    assert snapshot is not None and snapshot.optionable_carried
    assert notices == []


async def test_a_file_stamped_over_the_weekend_still_belongs_to_fridays_session(session_factory):
    evening = Evening(session_factory, et(18, 5, FRIDAY))
    evening.publish(et(9, 0, date(2026, 10, 24)))  # Saturday morning, before Monday's open

    use = await evening.snapshotter().take()

    assert (use.snapshot_date, use.fresh) == (FRIDAY, True)


async def test_a_failing_options_list_and_failing_listing_files_ping_under_separate_subjects(
    session_factory, seed_previous, notices
):
    await seed_previous()
    evening = Evening(session_factory, et(18, 5))
    evening.publish(et(12, 0))
    await evening.snapshotter().take()  # Monday: the listing files never qualify
    tuesday = date(2026, 10, 27)
    evening.moment = et(18, 5, tuesday)
    evening.publish(et(18, 1, tuesday))
    evening.host.statuses["options.txt"] = 503

    await evening.snapshotter().take()  # Tuesday: the listings recover, options.txt fails

    assert notices == [
        "Nasdaq Trader is failing (nasdaqlisted.txt not updated since the close). See the ingest logs.",
        "Nasdaq Trader recovered.",
        "Nasdaq options list is failing (options.txt could not be read). See the ingest logs.",
    ]


async def test_a_real_shrink_is_rejected_until_the_size_guard_is_turned_off(session_factory, seed_previous, notices):
    await seed_previous(nasdaq_listed=100, other_listed=100, options_symbols=100, cboe_symbols=100)
    evening = Evening(session_factory, et(21, 30))  # after the give-up: one attempt each

    rejected = await evening.snapshotter().take()
    accepted = await evening.snapshotter().take(accept_shrink=True)

    assert (rejected.snapshot_date, rejected.fresh) == (FRIDAY, False)
    assert (accepted.snapshot_date, accepted.fresh) == (MONDAY, True)
    snapshot = await stored(session_factory)
    assert snapshot is not None and snapshot.counts.nasdaq_listed == 2
    assert not snapshot.optionable_carried  # options.txt and Cboe were small too, and accepted


async def test_turning_the_size_guard_off_still_requires_the_stamp_and_the_footer(session_factory, seed_previous):
    await seed_previous()
    evening = Evening(session_factory, et(21, 30))
    evening.publish(et(12, 0))  # stale
    stale = await evening.snapshotter().take(accept_shrink=True)
    evening.publish(et(18, 1))
    evening.host.bodies["nasdaqlisted.txt"] = evening.host.bodies["nasdaqlisted.txt"].rsplit("File Creation", 1)[0]
    cut = await evening.snapshotter().take(accept_shrink=True)

    assert not stale.fresh and not cut.fresh
