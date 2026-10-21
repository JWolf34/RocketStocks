from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rs_domain import (
    Alert,
    Candidate,
    ChainSnapshot,
    Dataset,
    OptionQuote,
    PopularityOrigin,
    PopularitySnapshot,
    SourceStatus,
    Verdict,
    eastern_day,
    eastern_day_start,
)

UTC_NOON = datetime(2026, 7, 6, 16, 0, tzinfo=UTC)


class TestAlert:
    def test_valid_alert_builds(self):
        alert = Alert(symbol="SPY", trigger="VOLUME_THRESHOLD", triggered_at=UTC_NOON)
        assert alert.payload == {}

    def test_rejects_naive_triggered_at(self):
        with pytest.raises(ValidationError, match="naive"):
            Alert(symbol="SPY", trigger="VOLUME_THRESHOLD", triggered_at=datetime(2026, 7, 6, 16, 0))

    def test_rejects_non_utc_triggered_at(self):
        eastern = timezone(timedelta(hours=-4))
        with pytest.raises(ValidationError, match="non-UTC"):
            Alert(
                symbol="SPY",
                trigger="VOLUME_THRESHOLD",
                triggered_at=datetime(2026, 7, 6, 12, 0, tzinfo=eastern),
            )

    def test_zero_offset_alias_normalized_to_utc(self):
        # A zero-offset non-UTC tzinfo passes validation but must not survive:
        # variable zones would make later wall-clock arithmetic DST-hazardous.
        alert = Alert(
            symbol="SPY",
            trigger="VOLUME_THRESHOLD",
            triggered_at=datetime(2026, 7, 6, 16, 0, tzinfo=timezone(timedelta(0))),
        )
        assert alert.triggered_at.tzinfo is UTC
        assert alert.triggered_at == UTC_NOON


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

    def test_enum_values_are_db_and_metric_vocabulary(self):
        assert [d.value for d in Dataset] == ["curated_5m", "eod_full"]
        assert [s.value for s in SourceStatus] == ["ok", "partial", "failed", "stale", "skipped"]


class TestPopularitySnapshot:
    def fields(self, **overrides):
        base = dict(
            polled_at=UTC_NOON,
            ticker="ABC",
            rank=12,
            mentions=40,
            upvotes=90,
            rank_24h_ago=30,
            mentions_24h_ago=15,
            fetched_at=UTC_NOON + timedelta(seconds=12),
            origin="live",
        )
        base.update(overrides)
        return base

    def test_valid_snapshot_builds_with_origin_enum(self):
        snapshot = PopularitySnapshot(**self.fields())
        assert snapshot.origin is PopularityOrigin.LIVE

    def test_prior_day_fields_are_optional(self):
        snapshot = PopularitySnapshot(**self.fields(rank_24h_ago=None, mentions_24h_ago=None))
        assert snapshot.rank_24h_ago is None and snapshot.mentions_24h_ago is None

    def test_upvotes_may_be_absent_or_negative(self):
        assert PopularitySnapshot(**self.fields(upvotes=None)).upvotes is None
        assert PopularitySnapshot(**self.fields(upvotes=-1)).upvotes == -1

    def test_prior_rank_is_positive_or_absent(self):
        for bad in (0, -1):
            with pytest.raises(ValidationError):
                PopularitySnapshot(**self.fields(rank_24h_ago=bad))

    def test_frozen(self):
        with pytest.raises(ValidationError):
            PopularitySnapshot(**self.fields()).rank = 1

    def test_rejects_unknown_origin(self):
        with pytest.raises(ValidationError):
            PopularitySnapshot(**self.fields(origin="legacy"))

    def test_rejects_naive_polled_at(self):
        with pytest.raises(ValidationError, match="naive"):
            PopularitySnapshot(**self.fields(polled_at=datetime(2026, 7, 6, 16, 0)))

    def test_rejects_rank_zero(self):
        with pytest.raises(ValidationError):
            PopularitySnapshot(**self.fields(rank=0))


class TestCandidate:
    def test_valid_candidate_builds_without_an_alert(self):
        candidate = Candidate(
            trigger="popularity_surge",
            rule_version="2",
            symbol="GME",
            hit_at=UTC_NOON,
            inputs={"fired": ["rank_jump"]},
            verdict=Verdict.REPEAT,
            decided_at=UTC_NOON,
        )
        assert candidate.alert_id is None

    def test_verdicts_are_the_six_the_selector_can_give(self):
        assert [v.value for v in Verdict] == ["posted", "reply", "below_bar", "over_budget", "repeat", "stale"]

    def test_rejects_naive_times(self):
        with pytest.raises(ValidationError, match="naive"):
            Candidate(
                trigger="t",
                rule_version="1",
                symbol="X",
                hit_at=datetime(2026, 7, 6, 16, 0),
                inputs={},
                verdict=Verdict.POSTED,
                decided_at=UTC_NOON,
            )


class TestEasternDay:
    @pytest.mark.parametrize(
        ("moment", "day"),
        [
            (datetime(2026, 7, 7, 3, 59, tzinfo=UTC), date(2026, 7, 6)),
            (datetime(2026, 7, 7, 4, 0, tzinfo=UTC), date(2026, 7, 7)),
            (datetime(2026, 12, 8, 4, 59, tzinfo=UTC), date(2026, 12, 7)),
            (datetime(2026, 12, 8, 5, 0, tzinfo=UTC), date(2026, 12, 8)),
        ],
    )
    def test_follows_new_york_across_daylight_saving(self, moment, day):
        assert eastern_day(moment) == day

    def test_refuses_a_naive_reading(self):
        with pytest.raises(ValueError, match="naive"):
            eastern_day(datetime(2026, 7, 6, 16, 0))


class TestEasternDayStart:
    @pytest.mark.parametrize(
        ("day", "start"),
        [
            (date(2026, 7, 7), datetime(2026, 7, 7, 4, 0, tzinfo=UTC)),
            (date(2026, 12, 8), datetime(2026, 12, 8, 5, 0, tzinfo=UTC)),
        ],
    )
    def test_is_new_york_midnight_in_utc(self, day, start):
        assert eastern_day_start(day) == start
        assert eastern_day(start) == day
        assert eastern_day(start - timedelta(microseconds=1)) == day - timedelta(days=1)

    def test_a_day_is_23_hours_on_spring_forward_and_25_on_fall_back(self):
        spring, fall = date(2026, 3, 8), date(2026, 11, 1)
        assert eastern_day_start(spring + timedelta(days=1)) - eastern_day_start(spring) == timedelta(hours=23)
        assert eastern_day_start(fall + timedelta(days=1)) - eastern_day_start(fall) == timedelta(hours=25)
