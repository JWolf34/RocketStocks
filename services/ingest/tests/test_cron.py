"""Schedule arithmetic: UTC-aligned intervals and weekly slots in a named zone."""

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from rs_ingest.cron import CronTable, Daily, Every, Job, Weekly

CHICAGO = ZoneInfo("America/Chicago")
NEW_YORK = ZoneInfo("America/New_York")
MONDAY, SUNDAY = 0, 6


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


async def noop() -> None:
    return None


class TestEvery:
    @pytest.mark.parametrize(
        ("now", "expected"),
        [
            (utc(2026, 10, 19, 12, 7), utc(2026, 10, 19, 12, 30)),
            (utc(2026, 10, 19, 12, 31, 59), utc(2026, 10, 19, 13, 0)),
            (utc(2026, 10, 19, 23, 45), utc(2026, 10, 20, 0, 0)),
        ],
    )
    def test_aligns_to_the_next_half_hour_boundary(self, now, expected):
        assert Every(30).next_after(now) == expected

    def test_a_boundary_instant_advances_to_the_following_boundary(self):
        assert Every(30).next_after(utc(2026, 10, 19, 12, 30)) == utc(2026, 10, 19, 13, 0)

    def test_aligns_on_utc_whatever_zone_the_instant_is_expressed_in(self):
        now = datetime(2026, 10, 19, 7, 7, tzinfo=CHICAGO)  # 12:07 UTC
        assert Every(30).next_after(now) == utc(2026, 10, 19, 12, 30)

    @pytest.mark.parametrize("minutes", [0, -5, 7, 1441])
    def test_rejects_intervals_that_do_not_tile_the_day(self, minutes):
        with pytest.raises(ValueError, match="divide"):
            Every(minutes)


class TestWeekly:
    def test_fires_on_the_weekday_at_local_wall_time(self):
        weekly = Weekly(MONDAY, time(9, 0), CHICAGO)
        # Mon 2026-10-19 09:00 CDT is 14:00 UTC.
        assert weekly.next_after(utc(2026, 10, 15, 0, 0)) == utc(2026, 10, 19, 14, 0)

    def test_the_slot_instant_itself_advances_a_week(self):
        weekly = Weekly(MONDAY, time(9, 0), CHICAGO)
        assert weekly.next_after(utc(2026, 10, 19, 14, 0)) == utc(2026, 10, 26, 14, 0)

    def test_keeps_the_wall_time_across_spring_forward(self):
        weekly = Weekly(MONDAY, time(9, 0), CHICAGO)
        before = weekly.next_after(utc(2026, 3, 2, 12, 0))  # 09:00 CST, 15:00 UTC
        after = weekly.next_after(before)  # DST began Sunday 2026-03-08: 09:00 CDT
        assert before == utc(2026, 3, 2, 15, 0)
        assert after == utc(2026, 3, 9, 14, 0)

    def test_keeps_the_wall_time_across_fall_back(self):
        weekly = Weekly(MONDAY, time(9, 0), CHICAGO)
        before = weekly.next_after(utc(2026, 10, 26, 12, 0))  # 09:00 CDT, 14:00 UTC
        after = weekly.next_after(before)  # DST ended Sunday 2026-11-01: 09:00 CST
        assert before == utc(2026, 10, 26, 14, 0)
        assert after == utc(2026, 11, 2, 15, 0)

    def test_a_nonexistent_spring_forward_time_fires_after_the_gap(self):
        weekly = Weekly(SUNDAY, time(2, 30), CHICAGO)
        # 02:30 does not exist on 2026-03-08; the slot lands at 03:30 CDT.
        assert weekly.next_after(utc(2026, 3, 7, 12, 0)) == utc(2026, 3, 8, 8, 30)

    def test_a_repeated_fall_back_time_fires_once_on_its_first_occurrence(self):
        weekly = Weekly(SUNDAY, time(1, 30), CHICAGO)
        first = weekly.next_after(utc(2026, 10, 31, 12, 0))
        assert first == utc(2026, 11, 1, 6, 30)  # 01:30 CDT, the first of two 01:30s
        assert weekly.next_after(first) == utc(2026, 11, 8, 7, 30)  # a week later, CST

    def test_rejects_a_zone_aware_wall_time(self):
        with pytest.raises(ValueError, match="without a tzinfo"):
            Weekly(MONDAY, time(9, 0, tzinfo=UTC), CHICAGO)

    @pytest.mark.parametrize("weekday", [-1, 7])
    def test_rejects_an_invalid_weekday(self, weekday):
        with pytest.raises(ValueError, match="weekday"):
            Weekly(weekday, time(9, 0), CHICAGO)


class TestDaily:
    def test_fires_today_when_the_wall_time_is_still_ahead(self):
        # 17:30 EDT on 2026-10-19 is 21:30 UTC.
        assert Daily(time(17, 30), NEW_YORK).next_after(utc(2026, 10, 19, 12, 0)) == utc(2026, 10, 19, 21, 30)

    def test_the_slot_instant_itself_advances_a_day(self):
        daily = Daily(time(17, 30), NEW_YORK)
        assert daily.next_after(utc(2026, 10, 19, 21, 30)) == utc(2026, 10, 20, 21, 30)

    def test_uses_the_zone_date_not_the_utc_date(self):
        # 23:00 EDT Mon is already Tue in UTC; the next slot is still Tue 17:30 EDT.
        assert Daily(time(17, 30), NEW_YORK).next_after(utc(2026, 10, 20, 3, 0)) == utc(2026, 10, 20, 21, 30)

    def test_keeps_the_wall_time_across_spring_forward(self):
        daily = Daily(time(17, 30), NEW_YORK)
        before = daily.next_after(utc(2026, 3, 7, 12, 0))  # 17:30 EST, 22:30 UTC
        after = daily.next_after(before)  # DST began Sunday 2026-03-08: 17:30 EDT
        assert before == utc(2026, 3, 7, 22, 30)
        assert after == utc(2026, 3, 8, 21, 30)

    def test_keeps_the_wall_time_across_fall_back(self):
        daily = Daily(time(17, 30), NEW_YORK)
        before = daily.next_after(utc(2026, 10, 31, 12, 0))  # 17:30 EDT, 21:30 UTC
        after = daily.next_after(before)  # DST ended Sunday 2026-11-01: 17:30 EST
        assert before == utc(2026, 10, 31, 21, 30)
        assert after == utc(2026, 11, 1, 22, 30)

    def test_a_nonexistent_spring_forward_time_fires_after_the_gap(self):
        # 02:30 does not exist on 2026-03-08; the slot lands at 03:30 EDT.
        assert Daily(time(2, 30), NEW_YORK).next_after(utc(2026, 3, 8, 0, 0)) == utc(2026, 3, 8, 7, 30)

    def test_a_repeated_fall_back_time_fires_once_on_its_first_occurrence(self):
        daily = Daily(time(1, 30), NEW_YORK)
        first = daily.next_after(utc(2026, 11, 1, 0, 0))
        assert first == utc(2026, 11, 1, 5, 30)  # 01:30 EDT, the first of two 01:30s
        assert daily.next_after(first) == utc(2026, 11, 2, 6, 30)  # next day, EST

    def test_rejects_a_zone_aware_wall_time(self):
        with pytest.raises(ValueError, match="without a tzinfo"):
            Daily(time(17, 30, tzinfo=UTC), NEW_YORK)


class TestJobTimeout:
    def test_an_interval_job_defaults_to_its_interval(self):
        assert Job("poll", Every(30), noop).time_limit == timedelta(minutes=30)

    def test_an_explicit_timeout_wins(self):
        job = Job("poll", Every(30), noop, timeout=timedelta(minutes=5))
        assert job.time_limit == timedelta(minutes=5)

    def test_a_weekly_job_must_say_how_long_it_may_run(self):
        weekly = Weekly(MONDAY, time(9, 0), CHICAGO)
        with pytest.raises(ValueError, match="explicit timeout"):
            Job("research", weekly, noop)
        assert Job("research", weekly, noop, timeout=timedelta(hours=1)).time_limit == (
            timedelta(hours=1)
        )

    def test_a_daily_job_must_say_how_long_it_may_run(self):
        daily = Daily(time(17, 30), NEW_YORK)
        with pytest.raises(ValueError, match="explicit timeout"):
            Job("bars", daily, noop)

    def test_rejects_a_non_positive_timeout(self):
        with pytest.raises(ValueError, match="positive"):
            Job("poll", Every(30), noop, timeout=timedelta(0))


class TestCronTable:
    def test_iterates_jobs_in_registration_order(self):
        cron = CronTable()
        cron.register(Job("b", Every(30), noop))
        cron.register(Job("a", Every(30), noop))
        assert [job.name for job in cron] == ["b", "a"]
        assert len(cron) == 2

    def test_rejects_a_duplicate_name(self):
        cron = CronTable()
        cron.register(Job("poll", Every(30), noop))
        with pytest.raises(ValueError, match="already registered"):
            cron.register(Job("poll", Every(60), noop))
