"""When the samples are taken: the grid, early closes, non-sessions and the grace period."""

from datetime import UTC, date, datetime

import pytest

from rs_domain import EASTERN
from rs_ingest.tools.sweep_probe.schedule import first_sample_time, sample_times
from rs_sources import ExchangeCalendar

CALENDAR = ExchangeCalendar()


class EarlyCloseCalendar:
    """The exchange calendar with an unscheduled 13:00 ET close on one session day."""

    def __init__(self, early: date) -> None:
        self._early = early

    def is_session(self, day: date) -> bool:
        return CALENDAR.is_session(day)

    def session_close(self, day: date) -> datetime:
        if day == self._early:
            return datetime(day.year, day.month, day.day, 13, 0, tzinfo=EASTERN).astimezone(UTC)
        return CALENDAR.session_close(day)


def eastern_labels(moments: tuple[datetime, ...]) -> list[str]:
    return [m.astimezone(EASTERN).strftime("%d %H:%M") for m in moments]


def test_a_normal_day_is_sampled_after_the_close_then_hourly_then_half_hourly_into_the_morning():
    day, first = first_sample_time(datetime(2026, 10, 28, 15, 0, tzinfo=UTC), CALENDAR)

    assert (day, first) == (date(2026, 10, 28), datetime(2026, 10, 28, 20, 5, tzinfo=UTC))
    assert eastern_labels(sample_times(day, first)) == [
        "28 16:05",
        "28 18:05",
        *[f"28 {h}:00" for h in (19, 20, 21, 22, 23)],
        *[f"29 0{h}:00" for h in (0, 1, 2, 3, 4)],
        *[f"29 0{h}:{m}" for h in (5, 6, 7, 8) for m in ("00", "30")],
        "29 09:00",
        "29 09:15",
    ]


def test_an_early_close_moves_the_first_sample_with_the_close():
    # Scheduled early closes all fall before a non-session, so the day is an unscheduled one.
    calendar = EarlyCloseCalendar(date(2026, 10, 28))

    day, first = first_sample_time(datetime(2026, 10, 28, 12, 0, tzinfo=UTC), calendar)

    assert day == date(2026, 10, 28)
    assert first == datetime(2026, 10, 28, 17, 5, tzinfo=UTC)
    assert eastern_labels(sample_times(day, first))[:3] == ["28 13:05", "28 18:05", "28 19:00"]


def test_a_weekend_is_skipped_to_the_next_session():
    day, first = first_sample_time(datetime(2026, 10, 31, 15, 0, tzinfo=UTC), CALENDAR)  # a Saturday

    assert day == date(2026, 11, 2)
    assert first.astimezone(EASTERN).strftime("%H:%M") == "16:05"


def test_the_default_start_skips_a_session_whose_next_morning_is_not_one():
    friday = datetime(2026, 10, 30, 15, 0, tzinfo=UTC)

    day, _ = first_sample_time(friday, CALENDAR)

    assert day == date(2026, 11, 2)


def test_a_first_sample_within_the_grace_period_is_still_taken_and_one_past_it_is_not():
    just_inside = datetime(2026, 10, 28, 20, 14, tzinfo=UTC)
    just_past = datetime(2026, 10, 28, 20, 16, tzinfo=UTC)

    assert first_sample_time(just_inside, CALENDAR)[0] == date(2026, 10, 28)
    assert first_sample_time(just_past, CALENDAR)[0] == date(2026, 10, 29)


def test_an_explicit_start_wins_over_the_close():
    start = datetime(2026, 10, 28, 17, 0, tzinfo=EASTERN)

    day, first = first_sample_time(datetime(2026, 10, 28, 12, 0, tzinfo=UTC), CALENDAR, start)

    assert (day, first) == (date(2026, 10, 28), start.astimezone(UTC))


@pytest.mark.parametrize(
    ("start", "problem", "valid"),
    [
        (datetime(2026, 10, 31, 17, 0, tzinfo=EASTERN), "2026-10-31 is not a trading session", "2026-11-02T16:05"),
        (
            datetime(2026, 10, 30, 17, 0, tzinfo=EASTERN),
            "the next morning, 2026-10-31, is not a trading session",
            "2026-11-02T16:05",
        ),
    ],
    ids=["not a session", "no session next morning"],
)
def test_a_start_that_is_not_a_session_or_has_no_session_next_morning_is_refused_naming_a_valid_one(
    start, problem, valid
):
    with pytest.raises(ValueError, match=problem) as refused:
        first_sample_time(datetime(2026, 10, 28, 12, 0, tzinfo=UTC), CALENDAR, start)

    assert f"the next valid start is {valid}" in str(refused.value)


def test_grid_points_before_a_late_start_are_dropped():
    day = date(2026, 10, 28)
    first = datetime(2026, 10, 28, 19, 30, tzinfo=EASTERN).astimezone(UTC)

    labels = eastern_labels(sample_times(day, first))

    assert labels[:3] == ["28 19:30", "28 20:00", "28 21:00"]
