import random
from datetime import UTC, datetime, timedelta

import pytest

from rs_analysis.attention_surge import (
    COOLDOWN,
    RULE_VERSION,
    SubRule,
    SurgeEvent,
    evaluate_latest,
    replay,
)
from rs_domain import PopularityOrigin, PopularitySnapshot

T0 = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)


def snap(
    *,
    ticker: str = "ABC",
    at: datetime = T0,
    rank: int = 100,
    mentions: int = 20,
    rank_24h_ago: int | None = None,
    mentions_24h_ago: int | None = 20,
) -> PopularitySnapshot:
    return PopularitySnapshot(
        polled_at=at,
        ticker=ticker,
        rank=rank,
        mentions=mentions,
        upvotes=0,
        rank_24h_ago=rank_24h_ago,
        mentions_24h_ago=mentions_24h_ago,
        fetched_at=at + timedelta(seconds=20),
        origin=PopularityOrigin.LIVE,
    )


def fired(snapshot: PopularitySnapshot) -> tuple[SubRule, ...]:
    event = evaluate_latest(snapshot, [])
    return () if event is None else event.fired


class TestMinimumMentions:
    def test_below_minimum_never_fires(self):
        assert fired(snap(rank=10, rank_24h_ago=200, mentions=14, mentions_24h_ago=1)) == ()

    def test_at_minimum_can_fire(self):
        assert fired(snap(rank=10, rank_24h_ago=200, mentions=15, mentions_24h_ago=1)) == (SubRule.RANK_JUMP,)


class TestMentionSurge:
    def test_triple_fires_at_boundary(self):
        assert fired(snap(rank=100, mentions=60, mentions_24h_ago=20)) == (SubRule.MENTION_SURGE,)

    def test_just_under_triple_does_not_fire(self):
        assert fired(snap(rank=100, mentions=59, mentions_24h_ago=20)) == ()

    def test_top_25_needs_five_times(self):
        assert fired(snap(rank=25, mentions=99, mentions_24h_ago=20)) == ()
        assert fired(snap(rank=25, mentions=100, mentions_24h_ago=20)) == (SubRule.MENTION_SURGE,)

    def test_rank_26_needs_only_three_times(self):
        assert fired(snap(rank=26, mentions=60, mentions_24h_ago=20)) == (SubRule.MENTION_SURGE,)

    def test_base_of_15_required_through_rank_500(self):
        assert fired(snap(rank=500, mentions=42, mentions_24h_ago=14)) == ()
        assert fired(snap(rank=500, mentions=45, mentions_24h_ago=15)) == (SubRule.MENTION_SURGE,)

    def test_base_of_10_allowed_beyond_rank_500(self):
        assert fired(snap(rank=501, mentions=30, mentions_24h_ago=10)) == (SubRule.MENTION_SURGE,)
        assert fired(snap(rank=501, mentions=27, mentions_24h_ago=9)) == ()

    def test_missing_or_zero_prior_mentions_cannot_surge(self):
        assert fired(snap(mentions_24h_ago=None)) == ()
        assert fired(snap(mentions_24h_ago=0)) == ()


class TestRankJump:
    def test_gain_of_50_fires_at_boundary(self):
        assert fired(snap(rank=20, rank_24h_ago=70)) == (SubRule.RANK_JUMP,)

    def test_gain_of_49_does_not_fire(self):
        assert fired(snap(rank=20, rank_24h_ago=69)) == ()

    def test_ratio_of_one_and_a_half_fires_at_boundary(self):
        assert fired(snap(rank=40, rank_24h_ago=100)) == (SubRule.RANK_JUMP,)

    def test_ratio_just_under_one_and_a_half_does_not_fire(self):
        assert fired(snap(rank=40, rank_24h_ago=99)) == ()

    def test_large_gain_from_deep_rank_is_not_enough(self):
        assert fired(snap(rank=400, rank_24h_ago=500)) == ()

    def test_losing_rank_never_fires(self):
        assert fired(snap(rank=100, rank_24h_ago=20)) == ()

    def test_missing_prior_rank_cannot_jump(self):
        assert fired(snap(rank=5, rank_24h_ago=None)) == ()


class TestEvent:
    def test_both_rules_recorded_in_stable_order(self):
        event = evaluate_latest(snap(rank=30, rank_24h_ago=200, mentions=60, mentions_24h_ago=20), [])
        assert event is not None
        assert event.fired == (SubRule.MENTION_SURGE, SubRule.RANK_JUMP)

    def test_event_carries_inputs_and_version(self):
        event = evaluate_latest(snap(rank=40, rank_24h_ago=100, mentions=60, mentions_24h_ago=20), [])
        assert event is not None
        assert (event.ticker, event.polled_at, event.rule_version) == ("ABC", T0, RULE_VERSION)
        inputs = event.inputs
        assert (inputs.rank, inputs.rank_24h_ago, inputs.rank_gain) == (40, 100, 60)
        assert (inputs.mentions, inputs.mentions_24h_ago, inputs.mention_ratio) == (60, 20, 3.0)
        assert (inputs.mention_multiplier, inputs.mention_min_prior) == (3.0, 15)

    def test_inputs_record_tier_thresholds(self):
        top = evaluate_latest(snap(rank=5, rank_24h_ago=200), [])
        tail = evaluate_latest(snap(rank=600, mentions=40, mentions_24h_ago=10), [])
        assert top is not None and tail is not None
        assert (top.inputs.mention_multiplier, top.inputs.mention_min_prior) == (5.0, 15)
        assert (tail.inputs.mention_multiplier, tail.inputs.mention_min_prior) == (3.0, 10)


class TestCooldown:
    def hot(self, **overrides) -> PopularitySnapshot:
        return snap(**{"rank": 100, "mentions": 60, "mentions_24h_ago": 20, **overrides})

    def prior(self, ticker: str = "ABC") -> list[SurgeEvent]:
        event = evaluate_latest(self.hot(ticker=ticker), [])
        assert event is not None
        return [event]

    def test_suppressed_just_inside_24h(self):
        later = self.hot(at=T0 + COOLDOWN - timedelta(minutes=30))
        assert evaluate_latest(later, self.prior()) is None

    def test_fires_again_at_24h(self):
        later = self.hot(at=T0 + COOLDOWN)
        assert evaluate_latest(later, self.prior()) is not None

    def test_other_tickers_do_not_cool_down_each_other(self):
        assert evaluate_latest(self.hot(ticker="XYZ"), self.prior("ABC")) is not None

    def test_suppressed_candidate_does_not_extend_cooldown(self):
        history = [self.hot(at=T0 + timedelta(hours=h)) for h in (0, 12, 24)]
        assert [e.polled_at for e in replay(history)] == [T0, T0 + timedelta(hours=24)]


class TestReplay:
    def test_synthetic_history_yields_expected_events(self):
        history = [
            snap(at=T0, rank=300, mentions=20, rank_24h_ago=300, mentions_24h_ago=20),
            snap(at=T0 + timedelta(minutes=30), rank=300, mentions=60, rank_24h_ago=300, mentions_24h_ago=20),
            snap(at=T0 + timedelta(hours=1), rank=250, mentions=70, rank_24h_ago=300, mentions_24h_ago=20),
            snap(at=T0 + timedelta(hours=25), rank=30, mentions=90, rank_24h_ago=250, mentions_24h_ago=70),
        ]
        assert [(e.polled_at, e.fired) for e in replay(history)] == [
            (T0 + timedelta(minutes=30), (SubRule.MENTION_SURGE,)),
            (T0 + timedelta(hours=25), (SubRule.RANK_JUMP,)),
        ]

    def test_live_snapshot_by_snapshot_equals_replay(self):
        rng = random.Random(59)
        history = [
            snap(
                ticker=ticker,
                at=T0 + timedelta(minutes=30 * step),
                rank=rng.randint(1, 700),
                rank_24h_ago=rng.choice([None, rng.randint(1, 700)]),
                mentions=rng.randint(10, 160),
                mentions_24h_ago=rng.choice([None, rng.randint(5, 40)]),
            )
            for step in range(400)
            for ticker in ("AAA", "BBB", "CCC")
        ]

        live: list[SurgeEvent] = []
        for snapshot in history:
            event = evaluate_latest(snapshot, live)
            if event is not None:
                live.append(event)

        shuffled = history[:]
        rng.shuffle(shuffled)
        assert len(live) > 10
        assert replay(shuffled) == sorted(live, key=lambda e: (e.polled_at, e.ticker))

    def test_duplicate_snapshot_is_rejected(self):
        with pytest.raises(ValueError, match="duplicate"):
            replay([snap(), snap()])
