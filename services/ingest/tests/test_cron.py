"""Schedule arithmetic: UTC-aligned intervals and weekly slots in a named zone."""

from datetime import UTC, datetime, time
from zoneinfo import ZoneInfo

import pytest

from rs_ingest.cron import CronTable, Every, Job, Weekly

CHICAGO = ZoneInfo("America/Chicago")
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
