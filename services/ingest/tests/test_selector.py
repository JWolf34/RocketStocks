"""The selector is a pure function of a hit, its strength and what its Eastern day holds."""

from datetime import UTC, datetime, timedelta

from rs_analysis.popularity_surge import SubRule, SurgeEvent, SurgeInputs
from rs_domain import ALERT_STALE_AFTER, Verdict
from rs_ingest.selector import (
    DAILY_BUDGET,
    MAX_REPLIES_PER_STOCK_DAY,
    REPLY_STRENGTH_MARGIN,
    DayState,
    StockDay,
    announce,
    escalation,
    select,
)

HIT_AT = datetime(2026, 10, 19, 14, 0, tzinfo=UTC)
NOW = HIT_AT + timedelta(seconds=20)
BAR = 0.9


def hit(*fired: SubRule) -> SurgeEvent:
    return SurgeEvent(
        ticker="ABC",
        polled_at=HIT_AT,
        fired=fired or (SubRule.MENTION_SURGE,),
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


def verdict(
    strength: float | None = 0.95,
    *,
    event: SurgeEvent | None = None,
    alerts: int = 0,
    stock: StockDay | None = None,
    decided_at: datetime = NOW,
) -> Verdict:
    return select(
        event or hit(), strength=strength, bar=BAR, day=DayState(alerts=alerts, stock=stock), decided_at=decided_at
    )


def alerted(strength: float | None = 0.95, *carried: str, replies: int = 0) -> StockDay:
    return StockDay(carried=frozenset(carried or {"mention_surge"}), strength=strength, replies=replies)


class TestAStockWithoutAnAlertToday:
    def test_posts_a_hit_that_clears_the_bar_with_budget_left(self):
        assert verdict(0.95) is Verdict.POSTED

    def test_a_strength_exactly_at_the_bar_clears_it(self):
        assert verdict(BAR) is Verdict.POSTED

    def test_a_hit_under_the_bar_is_below_bar(self):
        assert verdict(0.89) is Verdict.BELOW_BAR

    def test_a_hit_with_no_baseline_cannot_be_shown_unusual(self):
        assert verdict(None) is Verdict.BELOW_BAR

    def test_a_hit_that_clears_the_bar_after_the_budget_is_used_is_over_budget(self):
        assert verdict(0.99, alerts=DAILY_BUDGET) is Verdict.OVER_BUDGET
        assert verdict(0.99, alerts=DAILY_BUDGET - 1) is Verdict.POSTED

    def test_below_the_bar_is_below_bar_even_when_the_budget_is_used(self):
        assert verdict(0.5, alerts=DAILY_BUDGET) is Verdict.BELOW_BAR

    def test_a_stock_below_the_bar_or_over_budget_earlier_still_gets_its_first_alert_later(self):
        # Neither earlier verdict leaves anything on the stock's day, so a later hit
        # that clears the bar, with budget left, posts.
        assert verdict(0.5) is Verdict.BELOW_BAR
        assert verdict(0.97, alerts=DAILY_BUDGET) is Verdict.OVER_BUDGET
        assert verdict(0.97, alerts=DAILY_BUDGET - 1, stock=None) is Verdict.POSTED


class TestAStockAlreadyAlertedToday:
    def test_a_hit_that_adds_nothing_is_a_repeat(self):
        assert verdict(0.95, stock=alerted(0.95)) is Verdict.REPEAT

    def test_a_repeat_never_posts_even_when_it_would_clear_the_bar(self):
        assert verdict(0.99, alerts=0, stock=alerted(0.95)) is Verdict.REPEAT

    def test_a_sub_rule_the_alert_did_not_carry_is_a_reply(self):
        both = hit(SubRule.MENTION_SURGE, SubRule.RANK_JUMP)

        assert verdict(0.95, event=both, stock=alerted(0.95)) is Verdict.REPLY

    def test_a_sub_rule_already_carried_by_an_earlier_reply_is_not_new(self):
        both = hit(SubRule.MENTION_SURGE, SubRule.RANK_JUMP)
        carried = alerted(0.95, "mention_surge", "rank_jump", replies=1)

        assert verdict(0.95, event=both, stock=carried) is Verdict.REPEAT

    def test_a_new_sub_rule_replies_whatever_its_strength(self):
        assert verdict(0.2, event=hit(SubRule.RANK_JUMP), stock=alerted(0.95)) is Verdict.REPLY

    def test_strength_up_by_the_margin_is_a_reply(self):
        stock = alerted(0.90)

        assert verdict(0.90 + REPLY_STRENGTH_MARGIN, stock=stock) is Verdict.REPLY
        assert verdict(0.90 + REPLY_STRENGTH_MARGIN - 0.001, stock=stock) is Verdict.REPEAT

    def test_strength_up_is_measured_from_the_highest_announced_so_far(self):
        after_a_reply = announce(alerted(0.90), ["mention_surge"], 0.96, reply=True)

        assert after_a_reply.strength == 0.96
        assert verdict(0.96 + REPLY_STRENGTH_MARGIN - 0.001, stock=after_a_reply) is Verdict.REPEAT

    def test_a_stock_announced_before_strength_was_stored_escalates_only_by_sub_rule(self):
        unknown = alerted(None)

        assert verdict(1.0, stock=unknown) is Verdict.REPEAT
        assert verdict(1.0, event=hit(SubRule.RANK_JUMP), stock=unknown) is Verdict.REPLY

    def test_a_hit_with_no_baseline_can_still_reply_on_a_new_sub_rule(self):
        assert verdict(None, event=hit(SubRule.RANK_JUMP), stock=alerted(0.95)) is Verdict.REPLY

    def test_replies_stop_at_the_cap_and_later_escalations_are_repeats(self):
        escalating = hit(SubRule.RANK_JUMP)
        one_left = alerted(0.95, replies=MAX_REPLIES_PER_STOCK_DAY - 1)
        none_left = alerted(0.95, replies=MAX_REPLIES_PER_STOCK_DAY)

        assert verdict(0.95, event=escalating, stock=one_left) is Verdict.REPLY
        assert verdict(0.95, event=escalating, stock=none_left) is Verdict.REPEAT

    def test_a_reply_needs_neither_budget_nor_the_bar(self):
        escalating = hit(SubRule.RANK_JUMP)

        assert verdict(0.1, event=escalating, alerts=DAILY_BUDGET, stock=alerted(0.95)) is Verdict.REPLY


class TestStale:
    def test_a_hit_decided_more_than_an_hour_after_its_boundary_is_stale(self):
        assert verdict(0.99, decided_at=HIT_AT + ALERT_STALE_AFTER + timedelta(seconds=1)) is Verdict.STALE

    def test_a_hit_decided_within_the_hour_is_not(self):
        assert verdict(0.99, decided_at=HIT_AT + ALERT_STALE_AFTER) is Verdict.POSTED

    def test_a_stale_reply_is_not_sent(self):
        late = HIT_AT + ALERT_STALE_AFTER + timedelta(minutes=5)

        assert verdict(0.95, event=hit(SubRule.RANK_JUMP), stock=alerted(0.95), decided_at=late) is Verdict.STALE


class TestEscalation:
    def test_names_the_new_rules_and_what_the_strength_rose_from(self):
        both = hit(SubRule.MENTION_SURGE, SubRule.RANK_JUMP)

        found = escalation(both, 0.99, alerted(0.90))

        assert found is not None
        assert found.new_rules == (SubRule.RANK_JUMP,)
        assert found.previous_strength == 0.90
        assert found.by == ("sub_rule", "strength")

    def test_is_none_when_nothing_is_added(self):
        assert escalation(hit(), 0.9, alerted(0.9)) is None


class TestAnnounce:
    def test_the_alert_starts_the_stock_day(self):
        after = announce(None, ["mention_surge"], 0.93, reply=False)

        assert after == StockDay(carried=frozenset({"mention_surge"}), strength=0.93, replies=0)

    def test_a_reply_adds_its_rules_keeps_the_highest_strength_and_counts_itself(self):
        after = announce(alerted(0.95), ["rank_jump"], 0.91, reply=True)

        assert after == StockDay(carried=frozenset({"mention_surge", "rank_jump"}), strength=0.95, replies=1)

