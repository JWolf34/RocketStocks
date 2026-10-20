from datetime import UTC, datetime, timedelta

import pytest

from rs_backfill.zones import floor_bucket, is_local_zone, naive_to_utc, resolve_zone


def test_a_local_zone_follows_dst():
    chicago = resolve_zone("America/Chicago")
    assert naive_to_utc(datetime(2026, 1, 15, 12), chicago) == datetime(2026, 1, 15, 18, tzinfo=UTC)
    assert naive_to_utc(datetime(2026, 7, 15, 12), chicago) == datetime(2026, 7, 15, 17, tzinfo=UTC)


def test_a_fixed_offset_ignores_dst():
    assert naive_to_utc(datetime(2026, 7, 15, 12), resolve_zone("-06:00")) == datetime(
        2026, 7, 15, 18, tzinfo=UTC
    )


def test_the_spring_forward_gap_does_not_exist():
    chicago = resolve_zone("America/Chicago")
    assert naive_to_utc(datetime(2026, 3, 8, 2, 30), chicago) is None
    assert naive_to_utc(datetime(2026, 3, 8, 2, 30), resolve_zone("-06:00")) is not None


def test_the_repeated_hour_resolves_to_its_first_pass():
    chicago = resolve_zone("America/Chicago")
    assert naive_to_utc(datetime(2025, 11, 2, 1, 30), chicago) == datetime(2025, 11, 2, 6, 30, tzinfo=UTC)


def test_unknown_zones_are_refused():
    with pytest.raises(ValueError, match="unknown zone"):
        resolve_zone("Mars/Olympus")


def test_only_dst_zones_are_local():
    assert is_local_zone("America/New_York")
    assert not is_local_zone("UTC")
    assert not is_local_zone("-05:00")


def test_floor_bucket_rounds_down_to_the_half_hour():
    instant = datetime(2026, 10, 1, 12, 44, 59, tzinfo=UTC)
    assert floor_bucket(instant) == datetime(2026, 10, 1, 12, 30, tzinfo=UTC)
    assert floor_bucket(instant - timedelta(minutes=14, seconds=59)) == datetime(
        2026, 10, 1, 12, 30, tzinfo=UTC
    )
