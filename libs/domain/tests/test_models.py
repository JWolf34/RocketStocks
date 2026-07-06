from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rs_domain import Alert

UTC_NOON = datetime(2026, 7, 6, 16, 0, tzinfo=timezone.utc)


class TestBar:
    def test_valid_bar_builds(self, make_bar):
        bar = make_bar(bucket=UTC_NOON)
        assert bar.symbol == "SPY"
        assert bar.bucket == UTC_NOON

    def test_frozen(self, make_bar):
        with pytest.raises(ValidationError):
            make_bar().close = 0.0

    def test_rejects_negative_volume(self, make_bar):
        with pytest.raises(ValidationError):
            make_bar(volume=-1)

    def test_rejects_naive_bucket(self, make_bar):
        with pytest.raises(ValidationError, match="naive"):
            make_bar(bucket=datetime(2026, 7, 6, 16, 0))

    def test_rejects_non_utc_bucket(self, make_bar):
        eastern = timezone(timedelta(hours=-4))
        with pytest.raises(ValidationError, match="non-UTC"):
            make_bar(bucket=datetime(2026, 7, 6, 12, 0, tzinfo=eastern))

    def test_zero_offset_alias_normalized_to_utc(self, make_bar):
        # A zero-offset non-UTC tzinfo passes validation but must not survive:
        # variable zones would make later wall-clock arithmetic DST-hazardous.
        bar = make_bar(bucket=datetime(2026, 7, 6, 16, 0, tzinfo=timezone(timedelta(0))))
        assert bar.bucket.tzinfo is timezone.utc
        assert bar.bucket == UTC_NOON


class TestAlert:
    def test_valid_alert_builds(self):
        alert = Alert(symbol="SPY", alert_type="VOLUME_THRESHOLD", triggered_at=UTC_NOON)
        assert alert.payload == {}

    def test_rejects_naive_triggered_at(self):
        with pytest.raises(ValidationError, match="naive"):
            Alert(symbol="SPY", alert_type="VOLUME_THRESHOLD", triggered_at=datetime(2026, 7, 6, 16, 0))
