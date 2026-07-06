from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rs_domain import Alert, Bar

UTC_NOON = datetime(2026, 7, 6, 16, 0, tzinfo=timezone.utc)


def make_bar(**overrides) -> Bar:
    fields = dict(
        symbol="SPY",
        bucket=UTC_NOON,
        open=560.10,
        high=561.25,
        low=559.90,
        close=560.75,
        volume=1_200_000,
    )
    fields.update(overrides)
    return Bar(**fields)


class TestBar:
    def test_valid_bar_builds(self):
        bar = make_bar()
        assert bar.symbol == "SPY"
        assert bar.bucket == UTC_NOON

    def test_frozen(self):
        with pytest.raises(ValidationError):
            make_bar().close = 0.0

    def test_rejects_negative_volume(self):
        with pytest.raises(ValidationError):
            make_bar(volume=-1)

    def test_rejects_naive_bucket(self):
        with pytest.raises(ValidationError, match="naive"):
            make_bar(bucket=datetime(2026, 7, 6, 16, 0))

    def test_rejects_non_utc_bucket(self):
        eastern = timezone(timedelta(hours=-4))
        with pytest.raises(ValidationError, match="non-UTC"):
            make_bar(bucket=datetime(2026, 7, 6, 12, 0, tzinfo=eastern))

    def test_accepts_zero_offset_alias(self):
        bar = make_bar(bucket=datetime(2026, 7, 6, 16, 0, tzinfo=timezone(timedelta(0))))
        assert bar.bucket.utcoffset() == timedelta(0)


class TestAlert:
    def test_valid_alert_builds(self):
        alert = Alert(symbol="SPY", alert_type="VOLUME_THRESHOLD", triggered_at=UTC_NOON)
        assert alert.payload == {}

    def test_rejects_naive_triggered_at(self):
        with pytest.raises(ValidationError, match="naive"):
            Alert(symbol="SPY", alert_type="VOLUME_THRESHOLD", triggered_at=datetime(2026, 7, 6, 16, 0))
