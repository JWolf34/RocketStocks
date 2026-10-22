"""The popularity poll against a real database with fake vendors: boundary
idempotency, a candidate for every hit with one Alert per stock per Eastern day,
the facts block's degradation, and durable writes."""

from datetime import UTC, date, datetime, timedelta
from typing import get_args, get_type_hints
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, func, insert, select

from rs_analysis.popularity_surge import RULE_VERSION, SubRule
from rs_analysis.strength import ReferenceUnavailable
from rs_domain import (
    ALERT_STALE_AFTER,
    Alert,
    Candidate,
    EscalationPayload,
    FactUnavailable,
    PopularitySurgePayload,
    PriceMoveFact,
    Verdict,
)
from rs_ingest.alert_writer import AlertWriteError, AlertWriter, RetryPolicy
from rs_ingest.popularity_poll import PopularityPoll, poll_boundary
from rs_ingest.scoring import Scored
from rs_ingest.selector import (
    DAILY_BUDGET,
    MAX_REPLIES_PER_STOCK_DAY,
    REPLY_STRENGTH_MARGIN,
)
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_persistence import (
    AlertRepository,
    AlertRow,
    CandidateRepository,
    CandidateRow,
    PopularitySnapshotRow,
)
from rs_sources import ApeWisdomError, ApeWisdomRow
from rs_sources.earnings import NextEarnings
from rs_sources.schwab import Quote, SchwabAuthError

T0 = datetime(2026, 10, 19, 14, 0, tzinfo=UTC)  # a Monday, 10:00 Eastern


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self.current = now

    def now(self) -> datetime:
        return self.current


def row(ticker: str, rank: int, mentions: int, *, mentions_24h_ago: int | None = 20, rank_24h_ago: int | None = None):
    return ApeWisdomRow(
        rank=rank,
        ticker=ticker,
        mentions=mentions,
        upvotes=mentions * 2,
        rank_24h_ago=rank_24h_ago,
        mentions_24h_ago=mentions_24h_ago,
    )


# SURGER triples its mentions at rank 40 (mention_surge); STEADY does not move.
def listing(surger_mentions: int = 90) -> list[ApeWisdomRow]:
    return [row("SURGER", 40, surger_mentions), row("STEADY", 41, 25, mentions_24h_ago=24)]


class FakeApeWisdom:
    def __init__(self, rows: list[ApeWisdomRow] | Exception) -> None:
        self.rows = rows
        self.fetches = 0

    async def fetch_all(self) -> list[ApeWisdomRow]:
        self.fetches += 1
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


class FakeQuotes:
    def __init__(self, result: Quote | Exception | None = None) -> None:
        self.result = result
        self.symbols: list[str] = []

    async def quote(self, symbol: str) -> Quote | None:
        self.symbols.append(symbol)
        if isinstance(self.result, Exception):
            raise self.result
        if self.result is None:
            return None
        return self.result.model_copy(update={"symbol": symbol})


class FakeEarnings:
    async def next_earnings(self, ticker: str, as_of: datetime) -> NextEarnings:
        return NextEarnings(date=date(2026, 11, 3))


class FakeScorer:
    """Scores every hit the same unless a ticker is given its own `Scored`; the real
    scorer has its own tests against stored history."""

    def __init__(self, default: Scored | None = None) -> None:
        self.default = default or Scored(raw_score=9.0, strength=0.99, bar=0.9)
        self.by_ticker: dict[str, Scored] = {}
        self.by_ticker_and_time: dict[tuple[str, datetime], Scored] = {}
        self.before_scoring = lambda: None  # a hook: the clock moves while the poll scores

    async def score(self, events):
        self.before_scoring()
        return [
            self.by_ticker_and_time.get((e.ticker, e.polled_at), self.by_ticker.get(e.ticker, self.default))
            for e in events
        ]


class Tripped:
    def __init__(self) -> None:
        self.reasons: list[str] = []

    async def __call__(self, reason: str) -> None:
        self.reasons.append(reason)


def good_quote(at: datetime = T0) -> Quote:
    return Quote(symbol="X", last_price=12.5, previous_close=10.0, quote_time=at, volume=3_000_000)


class CountingLimiter:
    def __init__(self) -> None:
        self.acquired = 0

    async def acquire(self) -> None:
        self.acquired += 1


class Rig:
    def __init__(self, session_factory, sleeps, rows, quotes=None, *, writer_factory=None, scorer=None) -> None:
        self.session_factory = session_factory
        self.clock = FakeClock(T0 + timedelta(seconds=20))
        self.apewisdom = FakeApeWisdom(rows)
        self.quotes = quotes or FakeQuotes(good_quote())
        self.tripped = Tripped()
        self.limiter = CountingLimiter()
        self.scorer = scorer or FakeScorer()
        self.sleeps = sleeps
        self.writer = AlertWriter(
            writer_factory or session_factory, policy=RetryPolicy(attempts=3), sleep=sleeps
        )
        self.poll = PopularityPoll(
            source=self.apewisdom,
            session_factory=session_factory,
            clock=self.clock,
            writer=self.writer,
            scorer=self.scorer,
            gatherer=lambda: SurgeFactsGatherer(
                quotes=lambda: self.quotes,
                limiter=self.limiter,
                earnings=FakeEarnings(),
                on_token_dead=self.tripped,
            ),
        )

    async def run_at(self, moment: datetime) -> None:
        self.clock.current = moment
        await self.poll.run()

    async def snapshots(self) -> int:
        async with self.session_factory() as session:
            return await session.scalar(select(func.count()).select_from(PopularitySnapshotRow))

    async def alerts(self) -> list[AlertRow]:
        async with self.session_factory() as session:
            return list(await session.scalars(select(AlertRow).order_by(AlertRow.id)))

    async def candidates(self) -> list[CandidateRow]:
        async with self.session_factory() as session:
            return list(await session.scalars(select(CandidateRow).order_by(CandidateRow.hit_at, CandidateRow.id)))

    async def verdicts(self) -> list[tuple[str, datetime, str]]:
        return [(c.symbol, c.hit_at, c.verdict) for c in await self.candidates()]


@pytest.fixture
def rig(session_factory, sleeps, notices) -> Rig:
    return Rig(session_factory, sleeps, listing())


def test_the_payload_can_name_every_sub_rule_the_rule_emits():
    named = set(get_args(get_type_hints(PopularitySurgePayload)["fired"])[0].__args__)
    assert named == {rule.value for rule in SubRule}


class TestBoundary:
    @pytest.mark.parametrize(
        ("moment", "expected"),
        [
            (datetime(2026, 10, 19, 14, 0, 0, tzinfo=UTC), datetime(2026, 10, 19, 14, 0, tzinfo=UTC)),
            (datetime(2026, 10, 19, 14, 0, 20, 5, tzinfo=UTC), datetime(2026, 10, 19, 14, 0, tzinfo=UTC)),
            (datetime(2026, 10, 19, 14, 29, 59, tzinfo=UTC), datetime(2026, 10, 19, 14, 0, tzinfo=UTC)),
            (datetime(2026, 10, 19, 14, 30, 1, tzinfo=UTC), datetime(2026, 10, 19, 14, 30, tzinfo=UTC)),
        ],
    )
    def test_floors_to_the_half_hour(self, moment, expected):
        assert poll_boundary(moment) == expected

    def test_uses_utc_whatever_the_input_zone(self):
        eastern = datetime(2026, 10, 19, 10, 45, tzinfo=ZoneInfo("America/New_York"))
        assert poll_boundary(eastern) == datetime(2026, 10, 19, 14, 30, tzinfo=UTC)


class TestStoringSnapshots:
    async def test_rows_are_stamped_with_the_boundary_and_live_origin(self, rig):
        await rig.poll.run()

        async with rig.session_factory() as session:
            stored = (await session.scalars(select(PopularitySnapshotRow))).all()
        assert {s.ticker for s in stored} == {"SURGER", "STEADY"}
        assert {s.polled_at for s in stored} == {T0}
        assert {s.fetched_at for s in stored} == {T0 + timedelta(seconds=20)}
        assert {s.origin for s in stored} == {"live"}

    async def test_new_entrants_and_negative_upvotes_are_stored(self, session_factory, sleeps, notices):
        rows = [
            ApeWisdomRow(rank=1, ticker="NEWB", mentions=30, upvotes=-1, rank_24h_ago=None, mentions_24h_ago=None),
            ApeWisdomRow(rank=2, ticker="DOWN", mentions=25, upvotes=-40, rank_24h_ago=3, mentions_24h_ago=24),
        ]
        rig = Rig(session_factory, sleeps, rows)

        await rig.poll.run()

        async with rig.session_factory() as session:
            stored = {s.ticker: s for s in (await session.scalars(select(PopularitySnapshotRow))).all()}
        assert (stored["NEWB"].upvotes, stored["NEWB"].rank_24h_ago) == (-1, None)
        assert stored["DOWN"].upvotes == -40

    async def test_a_failed_fetch_stores_nothing_and_surfaces_the_error(self, session_factory, sleeps, notices):
        rig = Rig(session_factory, sleeps, ApeWisdomError("page 2 failed after 3 attempts"))

        with pytest.raises(ApeWisdomError):
            await rig.poll.run()

        assert await rig.snapshots() == 0
        assert await rig.alerts() == []
        assert notices == []  # the daemon's failure streak is the ops channel for this

    async def test_an_empty_listing_is_a_failure_not_a_quiet_boundary(self, session_factory, sleeps, notices):
        rig = Rig(session_factory, sleeps, [])

        with pytest.raises(ApeWisdomError, match="no rows"):
            await rig.poll.run()

        assert notices == []


class TestIdempotency:
    async def test_polling_one_boundary_twice_changes_no_counts(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        snapshots, alerts = await rig.snapshots(), await rig.alerts()
        # The second fetch lands later in the same half hour and sees different numbers.
        rig.apewisdom.rows = listing(surger_mentions=200)

        await rig.run_at(T0 + timedelta(minutes=7))

        assert rig.apewisdom.fetches == 2
        assert await rig.snapshots() == snapshots == 2
        after = await rig.alerts()
        assert len(after) == len(alerts) == 1
        assert after[0].payload == alerts[0].payload  # first write wins, nothing rewritten

    async def test_the_rerun_does_not_look_up_facts_for_an_alert_it_already_has(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        lookups = len(rig.quotes.symbols)

        await rig.run_at(T0 + timedelta(minutes=7))

        assert len(rig.quotes.symbols) == lookups

    async def test_a_rerun_finishes_alerts_a_crash_left_unwritten(
        self, session_factory, sleeps, make_flaky, notices
    ):
        broken = make_flaky(failures=99)
        rig = Rig(session_factory, sleeps, listing(), writer_factory=broken)
        with pytest.raises(ExceptionGroup):
            await rig.poll.run()
        assert await rig.snapshots() == 2
        assert await rig.alerts() == []
        assert await rig.candidates() == []

        healthy = Rig(session_factory, sleeps, listing())
        await healthy.run_at(T0 + timedelta(minutes=3))

        assert await healthy.snapshots() == 2
        assert [a.symbol for a in await healthy.alerts()] == ["SURGER"]
        assert await healthy.verdicts() == [("SURGER", T0, "posted")]


T1 = T0 + timedelta(minutes=30)
T2 = T0 + timedelta(hours=1)


class TestCatchUp:
    async def _miss_the_first_boundary(self, session_factory, sleeps, make_flaky) -> Rig:
        broken = Rig(session_factory, sleeps, listing(), writer_factory=make_flaky(failures=99))
        with pytest.raises(ExceptionGroup):
            await broken.poll.run()
        assert await broken.alerts() == []
        return Rig(session_factory, sleeps, listing())

    async def test_an_alert_whose_write_failed_is_written_at_the_next_boundary_once(
        self, session_factory, sleeps, make_flaky, notices
    ):
        healthy = await self._miss_the_first_boundary(session_factory, sleeps, make_flaky)

        await healthy.run_at(T1 + timedelta(seconds=20))
        await healthy.run_at(T1 + timedelta(minutes=30, seconds=20))

        [alert] = await healthy.alerts()
        assert alert.triggered_at == T0
        assert PopularitySurgePayload.model_validate(alert.payload).polled_at == T0
        assert await healthy.verdicts() == [
            ("SURGER", T0, "posted"),
            ("SURGER", T1, "repeat"),
            ("SURGER", T1 + timedelta(minutes=30), "repeat"),
        ]

    async def test_a_ticker_caught_up_and_still_surging_alerts_once(
        self, session_factory, sleeps, make_flaky, notices
    ):
        healthy = await self._miss_the_first_boundary(session_factory, sleeps, make_flaky)

        await healthy.run_at(T1 + timedelta(seconds=20))

        assert await healthy.snapshots() == 4  # SURGER qualifies at both boundaries
        assert [a.triggered_at for a in await healthy.alerts()] == [T0]
        # The caught-up hit is the day's Alert; the current one repeats it.
        assert await healthy.verdicts() == [("SURGER", T0, "posted"), ("SURGER", T1, "repeat")]

    async def test_a_later_hit_waits_when_the_earlier_one_of_its_stock_could_not_be_stored(
        self, session_factory, sleeps, make_flaky, notices
    ):
        await self._miss_the_first_boundary(session_factory, sleeps, make_flaky)
        # Three failed opens exhaust the caught-up hit's attempts.
        stuck = Rig(session_factory, sleeps, listing(), writer_factory=make_flaky(failures=3))

        with pytest.raises(ExceptionGroup):
            await stuck.run_at(T1 + timedelta(seconds=20))

        # The repeat was decided on the premise that the first hit was stored, so it is not.
        assert await stuck.candidates() == []
        assert await stuck.alerts() == []

    async def test_a_ticker_that_only_surges_at_the_current_boundary_alerts_at_it(
        self, session_factory, sleeps, notices
    ):
        healthy = Rig(session_factory, sleeps, [row("STEADY", 41, 25, mentions_24h_ago=24)])
        await healthy.run_at(T0 + timedelta(seconds=20))
        healthy.apewisdom.rows = listing()

        await healthy.run_at(T1 + timedelta(seconds=20))

        assert [(a.symbol, a.triggered_at) for a in await healthy.alerts()] == [("SURGER", T1)]

    async def test_a_normal_run_with_nothing_missed_writes_nothing_extra(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        lookups = len(rig.quotes.symbols)
        rig.apewisdom.rows = [row("STEADY", 41, 25, mentions_24h_ago=24)]

        await rig.run_at(T1 + timedelta(seconds=20))

        assert len(await rig.alerts()) == 1
        assert len(rig.quotes.symbols) == lookups

    async def test_snapshots_older_than_one_boundary_are_never_judged(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        async with rig.session_factory() as session:
            await session.execute(delete(CandidateRow))
            await session.execute(delete(AlertRow))
            await session.commit()

        await rig.run_at(T0 + timedelta(hours=1, seconds=20))

        assert [a.triggered_at for a in await rig.alerts()] == [T0 + timedelta(hours=1)]

    async def test_imported_history_at_the_previous_boundary_is_not_caught_up(self, rig):
        async with rig.session_factory() as session:
            await session.execute(
                insert(PopularitySnapshotRow).values(
                    polled_at=T0, ticker="IMPORTED", rank=40, mentions=90, upvotes=180,
                    rank_24h_ago=None, mentions_24h_ago=20, fetched_at=T0, origin="v1_import",
                )
            )
            await session.commit()
        rig.apewisdom.rows = [row("STEADY", 41, 25, mentions_24h_ago=24)]

        await rig.run_at(T1 + timedelta(seconds=20))

        assert await rig.alerts() == []


class TestAlert:
    async def test_a_surge_writes_one_alert_with_its_decision_and_facts(self, rig):
        await rig.poll.run()

        [alert] = await rig.alerts()
        assert alert.symbol == "SURGER"
        assert alert.trigger == "popularity_surge"
        assert alert.rule_version == RULE_VERSION
        assert alert.day == date(2026, 10, 19)
        assert alert.reply_to is None
        assert alert.triggered_at == T0
        assert alert.alert_price == 12.5
        assert alert.quote_time == T0
        assert alert.posted_at is None
        payload = PopularitySurgePayload.model_validate(alert.payload)
        assert payload.fired == (SubRule.MENTION_SURGE.value,)
        assert (payload.rank, payload.mentions, payload.mentions_24h_ago) == (40, 90, 20)
        assert payload.polled_at == T0
        assert isinstance(payload.facts.price_move, PriceMoveFact)
        assert payload.facts.price_move.change_pct == pytest.approx(25.0)

    async def test_the_hit_is_stored_as_a_posted_candidate_naming_its_alert(self, rig):
        await rig.poll.run()

        [alert] = await rig.alerts()
        [candidate] = await rig.candidates()
        assert (candidate.trigger, candidate.rule_version, candidate.symbol) == (
            "popularity_surge", RULE_VERSION, "SURGER",
        )
        assert candidate.hit_at == T0
        assert candidate.day == date(2026, 10, 19)
        assert candidate.verdict == Verdict.POSTED.value
        assert candidate.decided_at == T0 + timedelta(seconds=20)
        assert candidate.alert_id == alert.id
        assert candidate.inputs == {
            "fired": ["mention_surge"], "rank": 40, "mentions": 90, "rank_24h_ago": None,
            "mentions_24h_ago": 20, "rank_gain": None, "mention_ratio": 4.5,
            "mention_multiplier": 3.0, "mention_min_prior": 15,
        }

    async def test_a_quiet_listing_writes_no_alert(self, session_factory, sleeps, notices):
        rig = Rig(session_factory, sleeps, [row("STEADY", 41, 25, mentions_24h_ago=24)])

        await rig.poll.run()

        assert await rig.alerts() == []
        assert rig.quotes.symbols == []

    async def test_the_payload_carries_the_price_and_no_volume_or_benchmark(self, rig):
        await rig.poll.run()

        [alert] = await rig.alerts()
        assert alert.payload["version"] == 3
        assert set(alert.payload["facts"]) == {"price_move", "next_earnings"}


class TestOneAlertPerStockPerDay:
    async def test_a_ticker_that_keeps_surging_alerts_once_and_repeats_are_recorded(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        await rig.run_at(T0 + timedelta(minutes=30, seconds=20))
        await rig.run_at(T0 + timedelta(hours=12, seconds=20))  # 22:00 Eastern, still that day

        assert len(await rig.alerts()) == 1
        assert await rig.verdicts() == [
            ("SURGER", T0, "posted"),
            ("SURGER", T0 + timedelta(minutes=30), "repeat"),
            ("SURGER", T0 + timedelta(hours=12), "repeat"),
        ]

    async def test_a_repeat_stores_no_alert_and_looks_up_no_facts(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        lookups = len(rig.quotes.symbols)

        await rig.run_at(T0 + timedelta(minutes=30, seconds=20))

        [_, repeat] = await rig.candidates()
        assert repeat.alert_id is None
        assert len(rig.quotes.symbols) == lookups

    async def test_the_ticker_alerts_again_on_the_next_eastern_day(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        await rig.run_at(T0 + timedelta(hours=24, seconds=20))

        alerts = await rig.alerts()
        assert [a.triggered_at for a in alerts] == [T0, T0 + timedelta(hours=24)]
        assert [a.day for a in alerts] == [date(2026, 10, 19), date(2026, 10, 20)]

    async def test_a_hit_after_midnight_eastern_is_a_new_day_within_24_hours(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))  # 10:00 Eastern
        after_midnight = T0 + timedelta(hours=14, minutes=30)  # 00:30 Eastern the next day

        await rig.run_at(after_midnight + timedelta(seconds=20))

        assert [a.triggered_at for a in await rig.alerts()] == [T0, after_midnight]

    async def test_other_tickers_are_not_held_back_by_it(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        rig.apewisdom.rows = [*listing(), row("OTHER", 60, 80)]

        await rig.run_at(T0 + timedelta(minutes=30, seconds=20))

        assert [a.symbol for a in await rig.alerts()] == ["SURGER", "OTHER"]

    async def test_an_alert_the_stock_already_has_today_from_an_earlier_rule_version_holds_the_day(self, rig):
        earlier = T0 - timedelta(hours=2)
        async with rig.session_factory() as session:
            alert_id = await AlertRepository(session).insert(
                Alert(
                    symbol="SURGER",
                    trigger="popularity_surge",
                    triggered_at=earlier,
                    payload={"version": 99},
                    rule_version="1",
                )
            )
            await CandidateRepository(session).insert(
                Candidate(
                    trigger="popularity_surge",
                    rule_version="1",
                    symbol="SURGER",
                    hit_at=earlier,
                    inputs={"fired": ["mention_surge"]},
                    verdict=Verdict.POSTED,
                    decided_at=earlier,
                    alert_id=alert_id,
                )
            )
            await session.commit()

        await rig.poll.run()

        assert len(await rig.alerts()) == 1
        assert await rig.verdicts() == [("SURGER", earlier, "posted"), ("SURGER", T0, "repeat")]

    async def test_two_hits_of_one_stock_in_one_run_post_the_earlier_and_repeat_the_later(self, rig):
        async with rig.session_factory() as session:
            await session.execute(
                insert(PopularitySnapshotRow).values(
                    polled_at=T0 - timedelta(minutes=30), ticker="SURGER", rank=40, mentions=90, upvotes=180,
                    rank_24h_ago=None, mentions_24h_ago=20, fetched_at=T0, origin="live",
                )
            )
            await session.commit()

        await rig.poll.run()

        assert await rig.verdicts() == [
            ("SURGER", T0 - timedelta(minutes=30), "posted"),
            ("SURGER", T0, "repeat"),
        ]
        [alert] = await rig.alerts()
        assert alert.triggered_at == T0 - timedelta(minutes=30)


class TestSchwabUnavailable:
    async def test_a_dead_token_writes_the_alert_without_price_facts(self, session_factory, sleeps, notices):
        quotes = FakeQuotes(SchwabAuthError("refresh token rejected"))
        rig = Rig(session_factory, sleeps, listing(), quotes)

        await rig.poll.run()

        [alert] = await rig.alerts()
        assert alert.alert_price is None
        assert alert.quote_time is None
        facts = PopularitySurgePayload.model_validate(alert.payload).facts
        assert facts.price_move == FactUnavailable(reason="Schwab token is dead")
        assert facts.next_earnings.status == "ok"
        assert len(rig.tripped.reasons) == 1

    async def test_every_quote_waits_on_the_shared_schwab_limiter(self, rig):
        await rig.poll.run()

        assert rig.quotes.symbols == ["SURGER"]
        assert rig.limiter.acquired == 1

    async def test_an_unknown_symbol_still_alerts_without_a_price(self, session_factory, sleeps, notices):
        rig = Rig(session_factory, sleeps, listing(), FakeQuotes(None))

        await rig.poll.run()

        [alert] = await rig.alerts()
        assert alert.alert_price is None


class TestDurability:
    async def test_a_database_failure_retries_then_tells_ops_and_raises(
        self, session_factory, sleeps, make_flaky, notices
    ):
        broken = make_flaky(failures=99)
        rig = Rig(session_factory, sleeps, listing(), writer_factory=broken)

        with pytest.raises(ExceptionGroup) as raised:
            await rig.poll.run()

        [failure] = raised.value.exceptions
        assert isinstance(failure, AlertWriteError)
        assert rig.sleeps.seconds == [1.0, 2.0]
        [notice] = notices
        assert "Alert NOT stored" in notice
        assert "SURGER" in notice and "popularity_surge" in notice

    async def test_one_alert_failing_does_not_lose_the_others(
        self, session_factory, sleeps, make_flaky, notices
    ):
        # Three failed opens exhaust one alert's attempts; the other alert then goes through.
        broken_for_one = make_flaky(failures=3)
        rig = Rig(
            session_factory,
            sleeps,
            [*listing(), row("OTHER", 60, 80)],
            writer_factory=broken_for_one,
        )

        with pytest.raises(ExceptionGroup) as raised:
            await rig.poll.run()

        assert len(raised.value.exceptions) == 1
        assert len(await rig.alerts()) == 1


def scored(strength: float | None, bar: float = 0.9) -> Scored:
    return Scored(raw_score=None if strength is None else strength * 10, strength=strength, bar=bar)


def both_rules(ticker: str) -> ApeWisdomRow:
    """Triples its mentions (mention_surge) and jumps 80 places (rank_jump)."""
    return row(ticker, 40, 90, mentions_24h_ago=20, rank_24h_ago=120)


class TestSelection:
    async def test_every_candidate_keeps_its_raw_score_and_strength(self, rig):
        rig.scorer.by_ticker["SURGER"] = Scored(raw_score=7.5, strength=0.97, bar=0.9)

        await rig.poll.run()

        [candidate] = await rig.candidates()
        assert (candidate.raw_score, candidate.strength) == (7.5, 0.97)

    async def test_a_hit_below_the_bar_is_recorded_without_an_alert_and_posts_when_it_later_clears_it(self, rig):
        rig.scorer.by_ticker["SURGER"] = scored(0.5)
        await rig.run_at(T0 + timedelta(seconds=20))
        assert await rig.alerts() == []

        rig.scorer.by_ticker["SURGER"] = scored(0.95)
        await rig.run_at(T1 + timedelta(seconds=20))

        assert await rig.verdicts() == [("SURGER", T0, "below_bar"), ("SURGER", T1, "posted")]
        [alert] = await rig.alerts()
        assert alert.triggered_at == T1

    async def test_a_hit_with_no_baseline_is_below_the_bar(self, rig):
        rig.scorer.by_ticker["SURGER"] = scored(None)

        await rig.poll.run()

        assert await rig.verdicts() == [("SURGER", T0, "below_bar")]
        [candidate] = await rig.candidates()
        assert (candidate.raw_score, candidate.strength) == (None, None)

    async def test_with_no_reference_every_hit_is_recorded_below_the_bar_and_the_poll_fails(
        self, session_factory, sleeps, notices
    ):
        class NoReference:
            async def score(self, events):
                raise ReferenceUnavailable("only 3 of the 30 days before 2026-07-06 were polled")

        rig = Rig(session_factory, sleeps, listing(), scorer=NoReference())

        with pytest.raises(ReferenceUnavailable, match="polled"):
            await rig.poll.run()

        assert await rig.snapshots() == 2
        assert await rig.alerts() == []
        assert await rig.verdicts() == [("SURGER", T0, "below_bar")]
        [candidate] = await rig.candidates()
        assert (candidate.raw_score, candidate.strength) == (None, None)

    async def test_the_budget_goes_to_the_strongest_of_a_boundary_not_the_first_alphabetically(
        self, session_factory, sleeps, notices
    ):
        names = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"]
        rig = Rig(session_factory, sleeps, [row(n, 40 + i, 90) for i, n in enumerate(names)])
        for strength, name in zip([0.91, 0.92, 0.93, 0.94, 0.95, 0.96], names, strict=True):
            rig.scorer.by_ticker[name] = scored(strength)

        await rig.poll.run()

        verdicts = {symbol: verdict for symbol, _, verdict in await rig.verdicts()}
        assert verdicts["AAA"] == "over_budget"
        assert list(verdicts.values()).count("posted") == DAILY_BUDGET
        assert sorted(a.symbol for a in await rig.alerts()) == names[1:]

    async def test_a_stock_over_budget_stays_so_for_the_day_and_the_budget_is_new_on_the_next_eastern_day(
        self, session_factory, sleeps, notices
    ):
        names = [f"S{i}" for i in range(DAILY_BUDGET + 1)]
        rig = Rig(session_factory, sleeps, [row(n, 40 + i, 90) for i, n in enumerate(names)])
        for i, name in enumerate(names):
            rig.scorer.by_ticker[name] = scored(0.99 - i * 0.01)  # S5 is the weakest
        tomorrow = T0 + timedelta(hours=24)
        rig.scorer.by_ticker_and_time[("S5", tomorrow)] = scored(1.0)  # the strongest of a new day
        for moment in (T0, T1, tomorrow):
            await rig.run_at(moment + timedelta(seconds=20))

        by_time = {(symbol, at): verdict for symbol, at, verdict in await rig.verdicts()}
        assert by_time[("S5", T0)] == by_time[("S5", T1)] == "over_budget"
        assert by_time[("S5", tomorrow)] == "posted"  # the day is new, so is the budget
        assert by_time[("S0", tomorrow)] == "posted"
        assert by_time[("S4", tomorrow)] == "over_budget"

    async def test_replies_never_use_the_budget(self, session_factory, sleeps, notices):
        names = [f"S{i}" for i in range(DAILY_BUDGET)]
        rig = Rig(session_factory, sleeps, [row(n, 40 + i, 90) for i, n in enumerate(names)])
        await rig.run_at(T0 + timedelta(seconds=20))
        rig.apewisdom.rows = [both_rules("S0"), *[row(n, 40 + i, 90) for i, n in enumerate(names) if n != "S0"]]

        await rig.run_at(T1 + timedelta(seconds=20))

        assert ("S0", T1, "reply") in await rig.verdicts()
        assert len([a for a in await rig.alerts() if a.reply_to is None]) == DAILY_BUDGET


class TestReplies:
    async def _alert_then(self, rig: Rig, rows: list[ApeWisdomRow], at: datetime = T1) -> None:
        await rig.run_at(T0 + timedelta(seconds=20))
        rig.apewisdom.rows = rows
        await rig.run_at(at + timedelta(seconds=20))

    async def test_a_new_sub_rule_replies_to_the_days_alert_with_what_escalated(self, rig):
        await self._alert_then(rig, [both_rules("SURGER")])

        alert, reply = await rig.alerts()
        assert reply.reply_to == alert.id
        assert (reply.symbol, reply.trigger, reply.day) == ("SURGER", "popularity_surge", alert.day)
        assert reply.triggered_at == T1
        assert reply.alert_price is None
        payload = EscalationPayload.model_validate(reply.payload)
        assert payload.new_rules == ("rank_jump",)
        assert payload.polled_at == T1
        assert (payload.strength_before, payload.strength_after) == (None, None)  # strength did not rise
        assert await rig.verdicts() == [("SURGER", T0, "posted"), ("SURGER", T1, "reply")]
        assert (await rig.candidates())[1].alert_id == reply.id

    async def test_a_reply_looks_up_no_facts(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        lookups = len(rig.quotes.symbols)
        rig.apewisdom.rows = [both_rules("SURGER")]

        await rig.run_at(T1 + timedelta(seconds=20))

        assert len(rig.quotes.symbols) == lookups

    async def test_strength_up_by_the_margin_replies_and_less_is_a_repeat(self, rig):
        rig.scorer.by_ticker_and_time[("SURGER", T0)] = scored(0.80, bar=0.7)
        rig.scorer.by_ticker_and_time[("SURGER", T1)] = scored(0.80 + REPLY_STRENGTH_MARGIN - 0.01, bar=0.7)
        rig.scorer.by_ticker_and_time[("SURGER", T2)] = scored(0.80 + REPLY_STRENGTH_MARGIN, bar=0.7)

        for at in (T0, T1, T2):
            await rig.run_at(at + timedelta(seconds=20))

        assert [v for *_, v in await rig.verdicts()] == ["posted", "repeat", "reply"]
        payload = EscalationPayload.model_validate((await rig.alerts())[1].payload)
        assert payload.new_rules == ()
        assert (payload.strength_before, payload.strength_after) == (0.80, 0.80 + REPLY_STRENGTH_MARGIN)

    async def test_a_reply_on_a_new_sub_rule_and_higher_strength_carries_both(self, rig):
        rig.scorer.by_ticker_and_time[("SURGER", T0)] = scored(0.80, bar=0.7)
        rig.scorer.by_ticker_and_time[("SURGER", T1)] = scored(0.97, bar=0.7)

        await self._alert_then(rig, [both_rules("SURGER")])

        payload = EscalationPayload.model_validate((await rig.alerts())[1].payload)
        assert payload.new_rules == ("rank_jump",)
        assert (payload.strength_before, payload.strength_after) == (0.80, 0.97)

    async def test_a_stock_gets_at_most_a_few_replies_a_day(self, rig):
        boundaries = [T0 + timedelta(minutes=30 * i) for i in range(MAX_REPLIES_PER_STOCK_DAY + 3)]
        for i, boundary in enumerate(boundaries):
            rig.scorer.by_ticker_and_time[("SURGER", boundary)] = scored(0.4 + i * (REPLY_STRENGTH_MARGIN + 0.001), bar=0.3)

        for boundary in boundaries:
            await rig.run_at(boundary + timedelta(seconds=20))

        verdicts = [v for *_, v in await rig.verdicts()]
        assert verdicts == ["posted", *["reply"] * MAX_REPLIES_PER_STOCK_DAY, *["repeat"] * 2]
        assert len(await rig.alerts()) == 1 + MAX_REPLIES_PER_STOCK_DAY

    async def test_the_next_eastern_day_starts_with_its_own_alert(self, rig):
        await self._alert_then(rig, [both_rules("SURGER")])
        rig.apewisdom.rows = [both_rules("SURGER")]

        await rig.run_at(T0 + timedelta(hours=24, seconds=20))

        alerts = await rig.alerts()
        assert [(a.reply_to is None, a.day) for a in alerts] == [
            (True, date(2026, 10, 19)),
            (False, date(2026, 10, 19)),
            (True, date(2026, 10, 20)),
        ]

    async def test_nothing_is_stored_when_the_alert_the_reply_continues_cannot_be_stored(
        self, session_factory, sleeps, make_flaky, notices
    ):
        broken = Rig(session_factory, sleeps, [both_rules("SURGER")], writer_factory=make_flaky(failures=99))
        with pytest.raises(ExceptionGroup):
            await broken.poll.run()

        assert await broken.candidates() == []
        assert await broken.alerts() == []


class TestStale:
    async def test_a_hit_decided_over_an_hour_after_its_boundary_is_recorded_stale_and_posts_nothing(self, rig):
        rig.scorer.before_scoring = lambda: setattr(rig.clock, "current", T0 + ALERT_STALE_AFTER + timedelta(minutes=1))

        await rig.run_at(T0 + timedelta(seconds=20))

        assert await rig.verdicts() == [("SURGER", T0, "stale")]
        assert await rig.alerts() == []
        assert rig.quotes.symbols == []


class TestEasternDayAcrossDaylightSaving:
    # The Eastern day starts 05:00 UTC in winter and 04:00 UTC in summer.
    @pytest.mark.parametrize(
        ("last_of_day", "first_of_next"),
        [
            (datetime(2026, 11, 2, 4, 30, tzinfo=UTC), datetime(2026, 11, 2, 5, 0, tzinfo=UTC)),  # 23:30 and 00:00 EST
            (datetime(2026, 3, 9, 3, 30, tzinfo=UTC), datetime(2026, 3, 9, 4, 0, tzinfo=UTC)),  # 23:30 and 00:00 EDT
        ],
    )
    async def test_one_alert_per_stock_per_eastern_day_and_a_new_day_at_eastern_midnight(
        self, session_factory, sleeps, notices, last_of_day, first_of_next
    ):
        rig = Rig(session_factory, sleeps, listing())
        before = last_of_day - timedelta(minutes=30)

        for moment in (before, last_of_day, first_of_next):
            await rig.run_at(moment + timedelta(seconds=20))

        assert await rig.verdicts() == [
            ("SURGER", before, "posted"),
            ("SURGER", last_of_day, "repeat"),
            ("SURGER", first_of_next, "posted"),
        ]
        days = [a.day for a in await rig.alerts()]
        assert days[1] - days[0] == timedelta(days=1)
