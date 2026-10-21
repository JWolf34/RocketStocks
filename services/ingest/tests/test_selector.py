"""The selector is a pure function of a hit and its stock's day."""

from datetime import UTC, datetime

from rs_analysis.popularity_surge import SubRule, SurgeEvent, SurgeInputs
from rs_domain import Verdict
from rs_ingest.selector import DayState, select

HIT = SurgeEvent(
    ticker="ABC",
    polled_at=datetime(2026, 10, 19, 14, 0, tzinfo=UTC),
    fired=(SubRule.MENTION_SURGE,),
    inputs=SurgeInputs(
        rank=40,
        mentions=90,
        rank_24h_ago=None,
        mentions_24h_ago=20,
        rank_gain=None,
        mention_ratio=4.5,
        mention_multiplier=3.0,
        mention_min_prior=15,
    ),
)


def test_the_first_hit_of_a_stocks_day_is_posted():
    assert select(HIT, DayState(alerted=False)) is Verdict.POSTED


def test_a_later_hit_on_a_stock_already_alerted_that_day_is_a_repeat():
    assert select(HIT, DayState(alerted=True)) is Verdict.REPEAT
