"""render(PendingAlert, base_rate) -> Embed: an attention-surge payload rendered
typed with its facts and base rate, inside Discord's embed limits. Anything that
cannot render typed falls back to the generic embed."""

from datetime import UTC, date, datetime

import pytest

import rs_bot.render
from rs_bot.render import render
from rs_domain import (
    Alert,
    AttentionSurgePayload,
    BaseRate,
    BaseRateInput,
    BenchmarkFact,
    FactUnavailable,
    FilingFact,
    NextEarningsFact,
    PriceMoveFact,
    RecentFilingsFact,
    SurgeFacts,
    VolumeFact,
)
from rs_persistence import PendingAlert

T0 = datetime(2026, 7, 6, 14, 30, tzinfo=UTC)
GONE = FactUnavailable(reason="quote vendor down")


def _facts(**overrides) -> SurgeFacts:
    facts = {
        "price_move": PriceMoveFact(last_price=12.5, previous_close=10.0, change_pct=25.0, quote_time=T0),
        "volume": VolumeFact(
            volume=3_000_000, average_volume=1_000_000, sessions=20, ratio=3.0, intraday=True
        ),
        "recent_filings": RecentFilingsFact(
            window_days=14,
            filings=(
                FilingFact(
                    form="8-K",
                    filing_date=date(2026, 7, 1),
                    items=("2.02", "9.01"),
                    url="https://example.test/f1",
                ),
            ),
        ),
        "next_earnings": NextEarningsFact(date=date(2026, 8, 4), estimated=True),
        "benchmark": BenchmarkFact(symbol="SPY", price=500.0, quote_time=T0),
    }
    return SurgeFacts(**{**facts, **overrides})


def _pending(facts: SurgeFacts | None = None, fired=("mention_surge",)) -> PendingAlert:
    payload = AttentionSurgePayload(
        rule_version="1",
        polled_at=T0,
        fired=fired,
        rank=12,
        mentions=150,
        rank_24h_ago=70,
        mentions_24h_ago=40,
        rank_gain=58,
        mention_ratio=3.75,
        mention_multiplier=3.0,
        mention_min_prior=15,
        facts=facts or _facts(),
    )
    return PendingAlert(
        id=42,
        alert=Alert(
            symbol="GME",
            alert_type="attention_surge",
            triggered_at=T0,
            payload=payload.model_dump(mode="json"),
            rule_version="1",
            alert_price=12.5,
            quote_time=T0,
        ),
    )


def _base_rate(input=BaseRateInput.RECONSTRUCTED, n=120) -> BaseRate:
    return BaseRate(
        alert_type="attention_surge",
        rule_version="1",
        computed_at=T0,
        input=input,
        n=n,
        n_unpriced=3,
        hit_rate_1d=0.52,
        hit_rate_5d=0.47,
        hit_rate_10d=None,
        median_excess_1d=0.004,
        median_excess_5d=-0.012,
        median_excess_10d=None,
        survivorship_caveat="Delisted names are missing.",
    )


def _fields(embed) -> dict[str, str]:
    return {f.name: f.value for f in embed.fields}


def _assert_within_discord_limits(embed) -> None:
    assert len(embed.fields) <= 25
    assert all(len(f.name) <= 256 and len(f.value) <= 1024 for f in embed.fields)
    assert len(embed) <= 6000  # discord.py's total of title, description, fields and footer


def test_full_payload_shows_rule_facts_base_rate_and_provenance():
    embed = render(_pending(), _base_rate())
    fields = _fields(embed)

    assert embed.title == "GME · attention surge"
    assert embed.timestamp == T0
    assert "150 mentions vs 40 24h ago" in fields["Why it fired"]
    assert "3.8x" in fields["Why it fired"]
    assert "$12.50" in fields["Price"] and "+25.00%" in fields["Price"]
    assert "3,000,000" in fields["Volume"] and "3.0x" in fields["Volume"]
    assert "partial day" in fields["Volume"]
    assert "[8-K 2026-07-01 items 2.02, 9.01](https://example.test/f1)" in fields["Recent 8-Ks"]
    assert fields["Next earnings"] == "2026-08-04 - estimated"
    assert fields["Benchmark"].startswith("SPY $500.00")
    for text in ("alert #42", "rule v1", "alert price $12.50", "quote 2026-07-06 14:30 UTC"):
        assert text in embed.footer.text
    _assert_within_discord_limits(embed)


def test_rank_jump_names_both_ranks():
    embed = render(_pending(fired=("rank_jump",)), _base_rate())
    assert "Rank jump: #12 from #70" in _fields(embed)["Why it fired"]


def test_every_optional_fact_unavailable_says_why():
    facts = _facts(
        price_move=GONE, volume=GONE, recent_filings=GONE, next_earnings=GONE, benchmark=GONE
    )
    fields = _fields(render(_pending(facts), _base_rate()))
    for name in ("Price", "Volume", "Recent 8-Ks", "Next earnings", "Benchmark"):
        assert fields[name] == "unavailable: quote vendor down"


def test_no_recent_filings_is_stated():
    facts = _facts(recent_filings=RecentFilingsFact(window_days=14, filings=()))
    assert _fields(render(_pending(facts)))["Recent 8-Ks"] == "none in the last 14 days"


def test_oversized_filing_list_is_cut_to_whole_lines_with_a_count():
    filings = tuple(
        FilingFact(
            form="8-K",
            filing_date=date(2026, 7, 1),
            items=("2.02",),
            url=f"https://example.test/{'x' * 80}/{i}",
        )
        for i in range(40)
    )
    facts = _facts(recent_filings=RecentFilingsFact(window_days=14, filings=filings))
    embed = render(_pending(facts), _base_rate())
    value = _fields(embed)["Recent 8-Ks"]

    *shown, tail = value.split("\n")
    assert tail == f"+{40 - len(shown)} more"
    assert 0 < len(shown) < 40
    assert all(line.endswith(")") for line in shown)  # no link cut in half
    _assert_within_discord_limits(embed)
    assert render(_pending(facts), _base_rate()).to_dict() == embed.to_dict()  # deterministic


def test_oversized_caveat_and_total_stay_inside_the_limits():
    base_rate = _base_rate().model_copy(update={"survivorship_caveat": "x" * 5000})
    filings = tuple(
        FilingFact(form="8-K", filing_date=date(2026, 7, 1), items=(), url=f"https://e.test/{'y' * 60}/{i}")
        for i in range(40)
    )
    facts = _facts(recent_filings=RecentFilingsFact(window_days=14, filings=filings))
    embed = render(_pending(facts), base_rate)
    _assert_within_discord_limits(embed)
    assert _fields(embed)["Base rate"].endswith("...")


def test_no_base_rate_says_so_explicitly():
    value = _fields(render(_pending(), None))["Base rate"]
    assert "No base rate has been measured" in value


def test_failed_base_rate_read_does_not_claim_none_measured():
    value = _fields(render(_pending(), None, base_rate_failed=True))["Base rate"]
    assert "could not be read" in value


def test_reconstructed_and_logged_rates_are_labelled():
    reconstructed = _fields(render(_pending(), _base_rate(BaseRateInput.RECONSTRUCTED)))["Base rate"]
    logged = _fields(render(_pending(), _base_rate(BaseRateInput.LOGGED, n=45)))["Base rate"]

    assert "reconstructed from history (input: reconstructed), N=120, 3 unpriced" in reconstructed
    assert "measured on past alerts (input: logged), N=45" in logged
    assert "1d: hit rate 52%, median excess vs SPY +0.40%" in logged
    assert "10d: hit rate n/a, median excess vs SPY n/a" in logged
    assert logged.endswith("Delisted names are missing.")


GENERIC_PENDING = PendingAlert(
    id=7,
    alert=Alert(
        symbol="SPY",
        alert_type="volume_threshold",
        triggered_at=T0,
        payload={"volume": 750_000, "threshold": 500_000},
    ),
)


def _assert_generic(embed, pending: PendingAlert) -> None:
    assert pending.alert.symbol in embed.title and pending.alert.alert_type in embed.title
    assert embed.timestamp == pending.alert.triggered_at
    assert str(pending.alert.payload) == embed.description
    assert f"alert #{pending.id}" in embed.footer.text
    assert not embed.fields


def test_other_alert_types_get_the_generic_embed():
    _assert_generic(render(GENERIC_PENDING), GENERIC_PENDING)


def test_unparseable_attention_surge_payload_falls_back():
    pending = PendingAlert(
        id=9,
        alert=Alert(symbol="GME", alert_type="attention_surge", triggered_at=T0, payload={"rank": 1}),
    )
    _assert_generic(render(pending), pending)


def test_render_failure_falls_back_to_generic_embed(monkeypatch):
    def poisoned(*args):
        raise RuntimeError("planted render fault")

    monkeypatch.setattr(rs_bot.render, "_price_text", poisoned)
    pending = _pending()
    _assert_generic(render(pending, _base_rate()), pending)


@pytest.mark.parametrize("size", [5000, 10])
def test_generic_embed_clips_a_huge_payload(size):
    pending = PendingAlert(
        id=1,
        alert=Alert(symbol="X", alert_type="t", triggered_at=T0, payload={"k": "v" * size}),
    )
    assert len(render(pending).description) <= 4096
