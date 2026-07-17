from datetime import date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rs_domain import Alert, ChainSnapshot, Dataset, OptionQuote, SourceStatus, median_instant

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


def make_quote(**overrides):
    fields = {
        "option_symbol": "SPY   260717C00560000",
        "expiry": date(2026, 7, 17),
        "strike": 560.0,
        "put_call": "C",
        "quote_time": UTC_NOON,
    }
    fields.update(overrides)
    return OptionQuote(**fields)


def make_snapshot(**overrides):
    fields = {
        "underlying": "SPY",
        "dataset": Dataset.CURATED_5M,
        "cycle_id": UTC_NOON,
        "fetched_at": UTC_NOON + timedelta(seconds=12),
        "source_status": SourceStatus.OK,
        "rows": (make_quote(),),
    }
    fields.update(overrides)
    return ChainSnapshot(**fields)


class TestMedianInstant:
    def test_empty_is_none(self):
        assert median_instant([]) is None

    def test_odd_count_middle(self):
        times = [UTC_NOON + timedelta(seconds=s) for s in (5, 1, 3)]
        assert median_instant(times) == UTC_NOON + timedelta(seconds=3)

    def test_even_count_lower_middle_is_an_observed_instant(self):
        times = [UTC_NOON + timedelta(seconds=s) for s in (4, 1, 3, 2)]
        assert median_instant(times) == UTC_NOON + timedelta(seconds=2)


class TestOptionQuote:
    def test_minimal_contract_identity_builds(self):
        quote = OptionQuote(
            option_symbol="SPY   260717P00500000", expiry=date(2026, 7, 17), strike=500, put_call="P"
        )
        assert quote.bid is None and quote.volume is None

    def test_rejects_bad_put_call(self):
        with pytest.raises(ValidationError):
            make_quote(put_call="X")

    def test_rejects_negative_volume(self):
        with pytest.raises(ValidationError):
            make_quote(volume=-1)

    def test_rejects_naive_quote_time(self):
        with pytest.raises(ValidationError, match="naive"):
            make_quote(quote_time=datetime(2026, 7, 6, 16, 0))


class TestChainSnapshot:
    def test_default_rows_empty_is_tombstone_shape(self):
        tomb = make_snapshot(source_status=SourceStatus.SKIPPED, rows=())
        assert tomb.rows == ()
        assert tomb.median_quote_time is None

    def test_median_quote_time_ignores_null_quote_times(self):
        rows = (
            make_quote(quote_time=UTC_NOON + timedelta(seconds=9)),
            make_quote(quote_time=None),
            make_quote(quote_time=UTC_NOON + timedelta(seconds=1)),
        )
        assert make_snapshot(rows=rows).median_quote_time == UTC_NOON + timedelta(seconds=1)

    def test_enum_values_are_db_and_metric_vocabulary(self):
        assert [d.value for d in Dataset] == ["curated_5m", "eod_full"]
        assert [s.value for s in SourceStatus] == ["ok", "partial", "failed", "stale", "skipped"]
