import json
from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from rs_domain import (
    AttentionSurgePayload,
    FactUnavailable,
    FilingFact,
    NextEarningsFact,
    PriceMoveFact,
    RecentFilingsFact,
    SurgeFacts,
    VolumeFact,
)

POLLED_AT = datetime(2026, 10, 19, 14, 0, tzinfo=UTC)


def full_facts() -> SurgeFacts:
    return SurgeFacts(
        price_move=PriceMoveFact(
            last_price=12.5, previous_close=10.0, change_pct=25.0, quote_time=POLLED_AT
        ),
        volume=VolumeFact(
            volume=3_000_000, average_volume=1_000_000.0, sessions=20, ratio=3.0, intraday=True
        ),
        recent_filings=RecentFilingsFact(
            window_days=30,
            filings=(
                FilingFact(
                    form="8-K",
                    filing_date=date(2026, 10, 15),
                    items=("2.02", "9.01"),
                    url="https://www.sec.gov/Archives/edgar/data/1/2/x.htm",
                ),
            ),
        ),
        next_earnings=NextEarningsFact(date=date(2026, 11, 3), estimated=True, time_of_day="pre_market"),
    )


def unavailable_facts() -> SurgeFacts:
    reason = FactUnavailable(reason="Schwab token is dead")
    return SurgeFacts(price_move=reason, volume=reason, recent_filings=reason, next_earnings=reason)


def payload(facts: SurgeFacts) -> AttentionSurgePayload:
    return AttentionSurgePayload(
        rule_version="1",
        polled_at=POLLED_AT,
        fired=("mention_surge", "rank_jump"),
        rank=12,
        mentions=90,
        rank_24h_ago=80,
        mentions_24h_ago=20,
        rank_gain=68,
        mention_ratio=4.5,
        mention_multiplier=5.0,
        mention_min_prior=15,
        facts=facts,
    )


@pytest.mark.parametrize("facts", [full_facts(), unavailable_facts()], ids=["full", "unavailable"])
def test_payload_round_trips_through_json(facts):
    original = payload(facts)

    stored = json.loads(json.dumps(original.model_dump(mode="json")))

    assert AttentionSurgePayload.model_validate(stored) == original


def test_each_fact_keeps_its_own_variant_after_a_round_trip():
    facts = full_facts().model_copy(update={"volume": FactUnavailable(reason="only 3 of 20 daily bars")})

    restored = AttentionSurgePayload.model_validate(payload(facts).model_dump(mode="json"))

    assert isinstance(restored.facts.price_move, PriceMoveFact)
    assert restored.facts.volume == FactUnavailable(reason="only 3 of 20 daily bars")


def test_payload_records_its_schema_version():
    assert payload(full_facts()).model_dump(mode="json")["version"] == 1


def test_rejects_an_unknown_payload_version():
    stored = payload(full_facts()).model_dump(mode="json") | {"version": 2}

    with pytest.raises(ValidationError):
        AttentionSurgePayload.model_validate(stored)


def test_rejects_an_unknown_sub_rule():
    stored = payload(full_facts()).model_dump(mode="json") | {"fired": ["velocity_spike"]}

    with pytest.raises(ValidationError):
        AttentionSurgePayload.model_validate(stored)


def test_rejects_a_naive_polled_at():
    stored = payload(full_facts()).model_dump(mode="json") | {"polled_at": "2026-10-19T14:00:00"}

    with pytest.raises(ValidationError, match="naive"):
        AttentionSurgePayload.model_validate(stored)
