"""render(PendingAlert, track_record) -> Embed: a popularity-surge Alert rendered typed with
its talking point, reasons, price, Track record and next Catalyst, a reply in its thread as a
short follow-up, inside Discord's embed limits. Anything that cannot render typed falls back to
the generic embed. Tests assert what the embed says, not its exact bytes."""

import re
from datetime import UTC, date, datetime, timedelta

import discord
import pytest

import rs_bot.render
from rs_bot.render import marked_row_id, render, render_generic
from rs_domain import (
    Alert,
    EscalationPayload,
    FactUnavailable,
    NextEarningsFact,
    PopularitySurgePayload,
    PriceMoveFact,
    SurgeFacts,
    TrackRecord,
)
from rs_persistence import PendingAlert

T0 = datetime(2026, 7, 6, 14, 30, tzinfo=UTC)
QUOTE_AT = T0 + timedelta(minutes=2)
REPLY_AT = T0 + timedelta(minutes=90)
GONE = FactUnavailable(reason="quote vendor down")
# Discord's timestamp markup: <t:unix:style>.
STAMP = re.compile(r"<t:(\d+):[a-zA-Z]>")
CLOCK_TIME = re.compile(r"\d{1,2}:\d{2}|UTC")


def _facts(**overrides) -> SurgeFacts:
    facts = {
        "price_move": PriceMoveFact(last_price=12.5, previous_close=10.0, change_pct=25.0, quote_time=QUOTE_AT),
        "next_earnings": NextEarningsFact(date=date(2026, 8, 4), estimated=True),
    }
    return SurgeFacts(**{**facts, **overrides})


def _pending(facts: SurgeFacts | None = None, fired=("mention_surge",), **payload_overrides) -> PendingAlert:
    payload = PopularitySurgePayload(
        **{
            "rule_version": "1",
            "polled_at": T0,
            "fired": fired,
            "rank": 12,
            "mentions": 150,
            "rank_24h_ago": 70,
            "mentions_24h_ago": 40,
            "rank_gain": 58,
            "mention_ratio": 3.75,
            "mention_multiplier": 3.0,
            "mention_min_prior": 15,
            "facts": facts or _facts(),
        }
        | payload_overrides
    )
    return PendingAlert(
        id=42,
        alert=Alert(
            symbol="GME",
            trigger="popularity_surge",
            triggered_at=T0,
            payload=payload.model_dump(mode="json"),
            rule_version="1",
            alert_price=12.5,
            quote_time=QUOTE_AT,
        ),
    )


def _reply(**payload) -> PendingAlert:
    escalation = EscalationPayload(polled_at=REPLY_AT, **payload)
    return PendingAlert(
        id=43,
        alert=Alert(
            symbol="GME",
            trigger="popularity_surge",
            triggered_at=REPLY_AT,
            payload=escalation.model_dump(mode="json"),
            rule_version="1",
            reply_to=42,
        ),
    )


def _track_record(outcomes=45) -> list[TrackRecord]:
    """The three horizons of one Trigger version's Track record."""
    return [
        TrackRecord(
            trigger="popularity_surge",
            rule_version="1",
            horizon=horizon,
            computed_at=T0,
            cases=120,
            live_cases=8,
            outcomes=outcomes,
            unpriced=3,
            no_move_size=2,
            share_at_2x=0.25,
            ordinary_share=0.07,
            median_multiple=0.9,
            median_excess=-0.012,
        )
        for horizon in (1, 5, 20)
    ]


def _fields(embed) -> dict[str, str]:
    return {f.name: f.value for f in embed.fields}


def _text(embed) -> str:
    """Everything the embed shows a reader, apart from the footer."""
    parts = [embed.title or "", embed.description or "", *(f"{f.name}\n{f.value}" for f in embed.fields)]
    return "\n".join(parts)


def _assert_within_discord_limits(embed) -> None:
    assert len(embed.fields) <= 25
    assert all(len(f.name) <= 256 and len(f.value) <= 1024 for f in embed.fields)
    assert len(embed) <= 6000  # discord.py's total of title, description, fields and footer


def _unix(moment: datetime) -> int:
    return int(moment.timestamp())


# -- a full Alert ----------------------------------------------------------------------------


def test_the_talking_point_names_the_mention_multiple_and_the_rank_move():
    embed = render(_pending(fired=("mention_surge", "rank_jump")), _track_record())

    assert embed.title == "GME · Popularity surge"
    assert "3.8x" in embed.description and "40 to 150" in embed.description
    assert "#70 to #12" in embed.description
    assert "\n" not in embed.description  # one line


@pytest.mark.parametrize("fired", [("mention_surge",), ("rank_jump",)])
def test_the_talking_point_describes_only_the_sub_rule_that_fired(fired):
    description = render(_pending(fired=fired), _track_record()).description

    assert ("3.8x" in description) == (fired == ("mention_surge",))
    assert ("#70 to #12" in description) == (fired == ("rank_jump",))


def test_the_alert_never_advises():
    text = _text(render(_pending(fired=("mention_surge", "rank_jump")), _track_record())).lower()

    for advice in ("buy", "sell", "target", "should", "position"):
        assert advice not in text


def test_the_reasons_name_the_trigger_and_each_sub_rule_that_fired():
    why = _fields(render(_pending(fired=("mention_surge", "rank_jump")), _track_record()))["Why it fired"]

    assert "Popularity surge v1" in why
    assert "Mention surge: 150 mentions vs 40 a day ago" in why and "needs 3x from at least 15" in why
    assert "Rank jump: #70 to #12" in why


def test_the_price_shows_with_its_as_of_time_and_the_next_catalyst_as_a_date():
    fields = _fields(render(_pending(), _track_record()))

    assert "$12.50" in fields["Price"] and "+25.00%" in fields["Price"]
    assert f"<t:{_unix(QUOTE_AT)}:f>" in fields["Price"]
    assert fields["Next Catalyst"] == "Earnings on Aug 4, 2026, estimated"


def test_a_confirmed_earnings_date_is_not_marked_estimated_and_names_the_session():
    facts = _facts(next_earnings=NextEarningsFact(date=date(2026, 8, 4), estimated=False, time_of_day="after_hours"))

    value = _fields(render(_pending(facts), _track_record()))["Next Catalyst"]

    assert value == "Earnings on Aug 4, 2026 (after the close)"


def test_every_time_shown_is_discord_timestamp_markup_and_a_date_is_not():
    embed = render(_pending(), _track_record())
    shown = _text(embed)

    assert {int(unix) for unix in STAMP.findall(shown)} == {_unix(T0), _unix(QUOTE_AT)}
    assert not CLOCK_TIME.search(STAMP.sub("", shown))  # no clock time outside the markup
    assert not STAMP.search(_fields(embed)["Next Catalyst"])  # a date has no zone to convert
    assert embed.timestamp == T0  # Discord shows this one in the reader's zone too
    assert not CLOCK_TIME.search(embed.footer.text)


def test_volume_and_the_benchmark_are_not_shown():
    shown = _text(render(_pending(), _track_record()))

    assert "volume" not in shown.lower() and "benchmark" not in shown.lower()


def test_the_footer_opens_with_the_row_id_and_names_the_trigger_version():
    embed = render(_pending(), _track_record())

    assert embed.footer.text.startswith("alert #42")
    assert "popularity_surge v1" in embed.footer.text
    _assert_within_discord_limits(embed)


# -- the Track record ------------------------------------------------------------------------


def test_the_track_record_shows_both_measures_at_every_horizon_and_how_many_cases_are_live():
    value = _fields(render(_pending(), _track_record()))["Track record"]

    assert value.splitlines()[0].startswith("120 cases, 8 live")
    header = next(line for line in value.splitlines() if "1d" in line)
    assert all(horizon in header for horizon in ("1d", "5d", "20d"))
    assert "-1.2%" in value and "SPY" in value  # direction: the median excess return over SPY
    assert "25%" in value and "7%" in value  # move size: the share at 2x, and the ordinary share
    assert "0.9x" in value  # and the median multiple
    assert re.search(r"Outcomes\s+45\s+45\s+45", value)


def test_the_track_record_is_a_compact_block_a_phone_can_show():
    block = _fields(render(_pending(), _track_record()))["Track record"].split("```")[1]

    assert max(len(line) for line in block.splitlines()) <= 32


def test_a_horizon_below_thirty_outcomes_says_too_few_cases_and_shows_no_numbers_for_it():
    thin = _track_record()
    thin[2] = thin[2].model_copy(update={"outcomes": 29, "share_at_2x": 0.9, "median_excess": 0.5})

    value = _fields(render(_pending(), thin))["Track record"]

    assert "20d: too few cases (29 Outcomes" in value
    assert "90%" not in value and "+50" not in value
    block = value.split("```")[1]
    assert "20d" not in block and "5d" in block  # the horizons with enough cases keep their numbers


def test_a_horizon_with_few_move_sizes_shows_few_for_the_shares_but_keeps_the_excess_and_count():
    # 40 Outcomes but 15 without a move size: 25 are sized, under 30.
    thin = _track_record(outcomes=40)
    thin[1] = thin[1].model_copy(update={"no_move_size": 15})

    value = _fields(render(_pending(), thin))["Track record"]

    block = value.split("```")[1]
    assert re.search(r"2x\+\s+25%\s+few\s+25%", block)
    assert re.search(r"ordinary\s+7%\s+few\s+7%", block)
    assert re.search(r"x typical\s+0.9x\s+few\s+0.9x", block)
    assert re.search(r"vs SPY\s+-1.2%\s+-1.2%\s+-1.2%", block)  # direction rests on every Outcome
    assert re.search(r"Outcomes\s+40\s+40\s+40", block)
    assert "few: too few cases (30 needed) with a known typical move." in value
    assert max(len(line) for line in block.splitlines()) <= 32


def test_exactly_thirty_sized_outcomes_are_enough_for_the_shares():
    record = _track_record(outcomes=32)[0].model_copy(update={"no_move_size": 2})

    value = _fields(render(_pending(), [record]))["Track record"]

    assert "few" not in value and "25%" in value


def test_cases_without_a_move_size_are_counted_in_the_table():
    value = _fields(render(_pending(), _track_record()))["Track record"]

    assert re.search(r"no size\s+2\s+2\s+2", value)
    assert "no size: cases with too little price history" in value


def test_no_size_row_is_left_out_when_every_case_has_one():
    records = [r.model_copy(update={"no_move_size": 0}) for r in _track_record()]

    value = _fields(render(_pending(), records))["Track record"]

    assert "no size" not in value


def test_every_horizon_too_thin_shows_only_too_few_cases():
    value = _fields(render(_pending(), _track_record(outcomes=10)))["Track record"]

    assert value.count("too few cases") == 3
    assert "```" not in value


def test_no_track_record_says_so_and_is_not_shown_as_zero():
    value = _fields(render(_pending()))["Track record"]

    assert "Not measured yet" in value
    assert "0%" not in value and "0 cases" not in value


def test_failed_track_record_read_does_not_claim_none_measured():
    value = _fields(render(_pending(), track_record_failed=True))["Track record"]

    assert "could not be read" in value and "Not measured" not in value


def test_a_missing_measure_is_labelled_not_shown_as_zero():
    bare = [
        r.model_copy(update={"share_at_2x": None, "ordinary_share": None, "median_multiple": None, "median_excess": None})
        for r in _track_record()
    ]

    value = _fields(render(_pending(), bare))["Track record"]

    assert "n/a" in value and "not measurable" in value
    assert "0.0%" not in value and "+0.0" not in value


def test_cases_that_could_not_be_priced_are_counted():
    value = _fields(render(_pending(), _track_record()))["Track record"]

    assert re.search(r"unpriced\s+3\s+3\s+3", value)


# -- missing facts ---------------------------------------------------------------------------


def test_every_optional_fact_unavailable_says_why():
    fields = _fields(render(_pending(_facts(price_move=GONE, next_earnings=GONE)), _track_record()))

    for name in ("Price", "Next Catalyst"):
        assert fields[name] == "unavailable: quote vendor down"


def test_one_missing_fact_does_not_hide_the_others():
    fields = _fields(render(_pending(_facts(price_move=GONE)), _track_record()))

    assert "quote vendor down" in fields["Price"]
    assert "Aug 4, 2026" in fields["Next Catalyst"]
    assert "Mention surge" in fields["Why it fired"]


def test_a_payload_without_the_prior_day_still_gets_a_truthful_talking_point():
    pending = _pending(rank_24h_ago=None, mentions_24h_ago=None, rank_gain=None, mention_ratio=None)

    description = render(pending, _track_record()).description

    assert "rank #12" in description and "150 mentions" in description
    assert "3.8x" not in description


# -- older payload versions ------------------------------------------------------------------


@pytest.mark.parametrize("version", [1, 2])
def test_a_stored_older_payload_still_renders_typed_without_volume_or_benchmark(version):
    pending = _pending()
    stored_facts = pending.alert.payload["facts"] | {
        "volume": {
            "status": "ok", "volume": 3_000_000, "average_volume": 1_000_000.0,
            "sessions": 20, "ratio": 3.0, "intraday": True,
        },
        "benchmark": {"status": "ok", "symbol": "SPY", "price": 500.0, "quote_time": "2026-07-06T14:30:00Z"},
        "recent_filings": {"status": "ok", "window_days": 14, "filings": []},
    }
    old = PendingAlert(
        id=42,
        alert=pending.alert.model_copy(
            update={"payload": pending.alert.payload | {"version": version, "facts": stored_facts}}
        ),
    )

    embed = render(old, _track_record())

    assert [f.name for f in embed.fields] == ["Why it fired", "Price", "Track record", "Next Catalyst"]
    assert "$12.50" in _fields(embed)["Price"]
    assert "3,000,000" not in _text(embed) and "500.00" not in _text(embed)


def test_new_payloads_are_written_as_version_3():
    assert _pending().alert.payload["version"] == 3


# -- a reply in the Alert's thread -----------------------------------------------------------


def test_a_reply_says_what_escalated_and_when():
    embed = render(_reply(new_rules=("rank_jump",), strength_before=0.8, strength_after=0.97))

    text = _text(embed)
    assert "rank jump" in text.lower() and "did not carry" in text
    assert "97" in text and "80" in text
    assert f"<t:{_unix(REPLY_AT)}:f>" in text
    assert "GME" in embed.title
    assert not embed.fields  # a short follow-up, not a second Alert
    assert "Track record" not in text and "Price" not in text


def test_a_reply_footer_names_its_own_row_and_the_alert_it_continues():
    embed = render(_reply(new_rules=("mention_surge",)))

    assert marked_row_id(embed) == 43
    assert "reply to #42" in embed.footer.text


def test_a_strength_only_escalation_needs_no_new_sub_rule():
    embed = render(_reply(strength_after=0.97))

    assert "also fired" not in embed.description
    assert "97" in embed.description


def test_a_reply_without_detail_says_so():
    assert "details were not stored" in render(_reply()).description


def test_a_reply_that_is_not_an_escalation_payload_falls_back():
    pending = PendingAlert(
        id=44,
        alert=Alert(symbol="GME", trigger="popularity_surge", triggered_at=T0, payload={"junk": 1}, reply_to=42),
    )

    embed = render(pending)

    assert marked_row_id(embed) == 44
    assert not embed.fields


# -- limits and fallbacks --------------------------------------------------------------------


def test_fields_over_the_total_are_dropped_from_the_end_deterministically():
    embed = discord.Embed(title="T")
    fields = [(f"field {i}", "v" * 1000) for i in range(10)]

    fitted = rs_bot.render._fit(embed, fields)

    assert fitted == fields[: len(fitted)]
    assert 0 < len(fitted) < 10
    assert sum(len(n) + len(v) for n, v in fitted) + len("T") <= 6000
    assert rs_bot.render._fit(embed, fields) == fitted


GENERIC_PENDING = PendingAlert(
    id=7,
    alert=Alert(
        symbol="SPY",
        trigger="volume_threshold",
        triggered_at=T0,
        payload={"volume": 750_000, "threshold": 500_000},
    ),
)


def _assert_generic(embed, pending: PendingAlert) -> None:
    assert pending.alert.symbol in embed.title and pending.alert.trigger in embed.title
    assert embed.timestamp == pending.alert.triggered_at
    assert str(pending.alert.payload) == embed.description
    assert f"alert #{pending.id}" in embed.footer.text
    assert not embed.fields


def test_other_triggers_get_the_generic_embed():
    _assert_generic(render(GENERIC_PENDING), GENERIC_PENDING)


def test_unparseable_popularity_surge_payload_falls_back():
    pending = PendingAlert(
        id=9,
        alert=Alert(symbol="GME", trigger="popularity_surge", triggered_at=T0, payload={"rank": 1}),
    )
    _assert_generic(render(pending), pending)


def test_render_failure_falls_back_to_generic_embed(monkeypatch):
    def poisoned(*args):
        raise RuntimeError("planted render fault")

    monkeypatch.setattr(rs_bot.render, "_price_text", poisoned)
    pending = _pending()
    _assert_generic(render(pending, _track_record()), pending)


@pytest.mark.parametrize("size", [5000, 10])
def test_generic_embed_clips_a_huge_payload(size):
    pending = PendingAlert(
        id=1,
        alert=Alert(symbol="X", trigger="t", triggered_at=T0, payload={"k": "v" * size}),
    )
    assert len(render(pending).description) <= 4096


def test_every_embed_names_its_row_so_history_can_identify_it():
    typed = _pending()  # id 42
    embeds = {
        "typed": render(typed, _track_record()),
        "generic trigger": render(GENERIC_PENDING),
        "last-attempt plain": render_generic(typed),
        "unparseable": render(
            PendingAlert(
                id=9, alert=Alert(symbol="G", trigger="popularity_surge", triggered_at=T0, payload={})
            )
        ),
    }

    assert {name: marked_row_id(embed) for name, embed in embeds.items()} == {
        "typed": 42,
        "generic trigger": 7,
        "last-attempt plain": 42,
        "unparseable": 9,
    }


def test_a_marker_is_matched_as_a_whole_id_and_only_at_the_start_of_the_footer():
    def embed(footer: str | None) -> discord.Embed:
        made = discord.Embed(title="x")
        return made if footer is None else made.set_footer(text=footer)

    assert marked_row_id(embed("alert #12")) == 12
    assert marked_row_id(embed("alert #12 | rule v1 | quote 2026-07-06 14:30 UTC")) == 12
    assert marked_row_id(embed("alert #12x")) is None
    assert marked_row_id(embed("see alert #12")) is None
    assert marked_row_id(embed(None)) is None


def test_the_marker_survives_a_footer_that_has_to_be_clipped():
    pending = _pending()
    long_rule = pending.alert.model_copy(
        update={"payload": {**pending.alert.payload, "rule_version": "9" * 5000}}
    )

    embed = render(PendingAlert(id=42, alert=long_rule), _track_record())

    assert len(embed.footer.text) <= 2048
    assert marked_row_id(embed) == 42
