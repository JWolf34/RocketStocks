"""The popularity poll against a real database with fake vendors: boundary
idempotency, the cooldown read from the alert log, the facts block's
degradation, and durable alert writes."""

from datetime import UTC, date, datetime, timedelta
from typing import get_args, get_type_hints
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import func, select

from rs_analysis.attention_surge import RULE_VERSION, SubRule
from rs_domain import (
    Alert,
    AttentionSurgePayload,
    BenchmarkFact,
    FactUnavailable,
    PriceMoveFact,
    StoredDailyBar,
)
from rs_ingest.alert_writer import AlertWriteError, AlertWriter, RetryPolicy
from rs_ingest.popularity_poll import PopularityPoll, poll_boundary
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_persistence import (
    AlertRepository,
    AlertRow,
    DailyBarRepository,
    PopularitySnapshotRow,
)
from rs_sources import ApeWisdomError, ApeWisdomRow, ExchangeCalendar
from rs_sources.earnings import NextEarnings
from rs_sources.edgar import Filing8K
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


class FakeFilings:
    async def recent_8ks(self, ticker: str, as_of: datetime, limit: int | None = 5) -> list[Filing8K]:
        return []


class FakeEarnings:
    async def next_earnings(self, ticker: str, as_of: datetime) -> NextEarnings:
        return NextEarnings(date=date(2026, 11, 3))


class Tripped:
    def __init__(self) -> None:
        self.reasons: list[str] = []

    async def __call__(self, reason: str) -> None:
        self.reasons.append(reason)


def good_quote(at: datetime = T0) -> Quote:
    return Quote(symbol="X", last_price=12.5, previous_close=10.0, quote_time=at, volume=3_000_000)


@pytest.fixture(scope="module")
def calendar() -> ExchangeCalendar:
    return ExchangeCalendar()


class CountingLimiter:
    def __init__(self) -> None:
        self.acquired = 0

    async def acquire(self) -> None:
        self.acquired += 1


class Rig:
    def __init__(self, session_factory, calendar, sleeps, rows, quotes=None, *, writer_factory=None) -> None:
        self.session_factory = session_factory
        self.clock = FakeClock(T0 + timedelta(seconds=20))
        self.apewisdom = FakeApeWisdom(rows)
        self.quotes = quotes or FakeQuotes(good_quote())
        self.tripped = Tripped()
        self.limiter = CountingLimiter()
        self.sleeps = sleeps
        self.writer = AlertWriter(
            writer_factory or session_factory, policy=RetryPolicy(attempts=3), sleep=sleeps
        )
        self.poll = PopularityPoll(
            source=self.apewisdom,
            session_factory=session_factory,
            clock=self.clock,
            writer=self.writer,
            gatherer=lambda: SurgeFactsGatherer(
                quotes=lambda: self.quotes,
                limiter=self.limiter,
                filings=FakeFilings(),
                earnings=FakeEarnings(),
                session_factory=session_factory,
                calendar=calendar,
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


@pytest.fixture
def rig(session_factory, calendar, sleeps, notices) -> Rig:
    return Rig(session_factory, calendar, sleeps, listing())


def test_the_payload_can_name_every_sub_rule_the_rule_emits():
    named = set(get_args(get_type_hints(AttentionSurgePayload)["fired"])[0].__args__)
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

    async def test_a_failed_fetch_stores_nothing_and_surfaces_the_error(self, session_factory, calendar, sleeps, notices):
        rig = Rig(session_factory, calendar, sleeps, ApeWisdomError("page 2 failed after 3 attempts"))

        with pytest.raises(ApeWisdomError):
            await rig.poll.run()

        assert await rig.snapshots() == 0
        assert await rig.alerts() == []
        assert notices == []  # the daemon's failure streak is the ops channel for this

    async def test_an_empty_listing_is_a_failure_not_a_quiet_boundary(self, session_factory, calendar, sleeps, notices):
        rig = Rig(session_factory, calendar, sleeps, [])

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
        self, session_factory, calendar, sleeps, make_flaky, notices
    ):
        broken = make_flaky(failures=99)
        rig = Rig(session_factory, calendar, sleeps, listing(), writer_factory=broken)
        with pytest.raises(ExceptionGroup):
            await rig.poll.run()
        assert await rig.snapshots() == 2
        assert await rig.alerts() == []

        healthy = Rig(session_factory, calendar, sleeps, listing())
        await healthy.run_at(T0 + timedelta(minutes=3))

        assert await healthy.snapshots() == 2
        assert [a.symbol for a in await healthy.alerts()] == ["SURGER"]


class TestAlert:
    async def test_a_surge_writes_one_alert_with_its_decision_and_facts(self, rig):
        await rig.poll.run()

        [alert] = await rig.alerts()
        assert alert.symbol == "SURGER"
        assert alert.alert_type == "attention_surge"
        assert alert.rule_version == RULE_VERSION
        assert alert.dedup_key == f"attention_surge:{RULE_VERSION}:SURGER:20261019T1400Z"
        assert alert.triggered_at == T0
        assert alert.alert_price == 12.5
        assert alert.quote_time == T0
        assert alert.posted_at is None
        payload = AttentionSurgePayload.model_validate(alert.payload)
        assert payload.fired == (SubRule.MENTION_SURGE.value,)
        assert (payload.rank, payload.mentions, payload.mentions_24h_ago) == (40, 90, 20)
        assert payload.polled_at == T0
        assert isinstance(payload.facts.price_move, PriceMoveFact)
        assert payload.facts.price_move.change_pct == pytest.approx(25.0)
        assert payload.facts.benchmark == BenchmarkFact(symbol="SPY", price=12.5, quote_time=T0)

    async def test_a_quiet_listing_writes_no_alert(self, session_factory, calendar, sleeps, notices):
        rig = Rig(session_factory, calendar, sleeps, [row("STEADY", 41, 25, mentions_24h_ago=24)])

        await rig.poll.run()

        assert await rig.alerts() == []
        assert rig.quotes.symbols == []

    async def test_alert_facts_come_from_the_stored_daily_bars(self, rig, calendar):
        sessions = list(calendar.sessions_in_range(T0.date() - timedelta(days=60), T0.date() - timedelta(days=1)))[-20:]
        async with rig.session_factory() as session:
            await DailyBarRepository(session).insert_many(
                [
                    StoredDailyBar(
                        symbol="SURGER", session_date=day, open=10.0, high=11.0, low=9.0, close=10.0,
                        volume=1_000_000, fetched_at=T0, adjusted=True,
                    )
                    for day in sessions
                ]
            )
            await session.commit()

        await rig.poll.run()

        [alert] = await rig.alerts()
        payload = AttentionSurgePayload.model_validate(alert.payload)
        assert payload.facts.volume.status == "ok"
        assert payload.facts.volume.ratio == pytest.approx(3.0)

    async def test_without_daily_bars_the_volume_fact_says_why(self, rig):
        await rig.poll.run()

        [alert] = await rig.alerts()
        volume = AttentionSurgePayload.model_validate(alert.payload).facts.volume
        assert volume == FactUnavailable(reason="only 0 of 20 daily bars stored for the average")


class TestCooldown:
    async def test_a_ticker_that_keeps_surging_alerts_once_a_day(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        await rig.run_at(T0 + timedelta(minutes=30, seconds=20))
        await rig.run_at(T0 + timedelta(hours=12, seconds=20))

        assert len(await rig.alerts()) == 1

    async def test_the_ticker_alerts_again_after_the_cooldown(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        await rig.run_at(T0 + timedelta(hours=24, seconds=20))

        alerts = await rig.alerts()
        assert [a.triggered_at for a in alerts] == [T0, T0 + timedelta(hours=24)]

    async def test_other_tickers_are_not_cooled_down_by_it(self, rig):
        await rig.run_at(T0 + timedelta(seconds=20))
        rig.apewisdom.rows = [*listing(), row("OTHER", 60, 80)]

        await rig.run_at(T0 + timedelta(minutes=30, seconds=20))

        assert [a.symbol for a in await rig.alerts()] == ["SURGER", "OTHER"]

    async def test_an_unreadable_prior_payload_does_not_stall_the_poll(self, rig):
        async with rig.session_factory() as session:
            await AlertRepository(session).insert(
                Alert(
                    symbol="OLD",
                    alert_type="attention_surge",
                    triggered_at=T0 - timedelta(hours=2),
                    payload={"version": 99},
                    dedup_key="attention_surge:0:OLD:20261019T1200Z",
                )
            )
            await session.commit()

        await rig.poll.run()

        assert "SURGER" in [a.symbol for a in await rig.alerts()]


class TestSchwabUnavailable:
    async def test_a_dead_token_writes_the_alert_without_price_facts(self, session_factory, calendar, sleeps, notices):
        quotes = FakeQuotes(SchwabAuthError("refresh token rejected"))
        rig = Rig(session_factory, calendar, sleeps, listing(), quotes)

        await rig.poll.run()

        [alert] = await rig.alerts()
        assert alert.alert_price is None
        assert alert.quote_time is None
        facts = AttentionSurgePayload.model_validate(alert.payload).facts
        assert facts.price_move == FactUnavailable(reason="Schwab token is dead")
        assert facts.next_earnings.status == "ok"
        assert len(rig.tripped.reasons) == 1

    async def test_every_quote_waits_on_the_shared_schwab_limiter(self, rig):
        await rig.poll.run()

        # The ticker's quote and the SPY benchmark quote.
        assert rig.quotes.symbols == ["SURGER", "SPY"]
        assert rig.limiter.acquired == 2

    async def test_an_unknown_symbol_still_alerts_without_a_price(self, session_factory, calendar, sleeps, notices):
        rig = Rig(session_factory, calendar, sleeps, listing(), FakeQuotes(None))

        await rig.poll.run()

        [alert] = await rig.alerts()
        assert alert.alert_price is None


class TestDurability:
    async def test_a_database_failure_retries_then_tells_ops_and_raises(
        self, session_factory, calendar, sleeps, make_flaky, notices
    ):
        broken = make_flaky(failures=99)
        rig = Rig(session_factory, calendar, sleeps, listing(), writer_factory=broken)

        with pytest.raises(ExceptionGroup) as raised:
            await rig.poll.run()

        [failure] = raised.value.exceptions
        assert isinstance(failure, AlertWriteError)
        assert rig.sleeps.seconds == [1.0, 2.0]
        [notice] = notices
        assert "Alert NOT stored" in notice
        assert f"attention_surge:{RULE_VERSION}:SURGER:20261019T1400Z" in notice

    async def test_one_alert_failing_does_not_lose_the_others(
        self, session_factory, calendar, sleeps, make_flaky, notices
    ):
        # Three failed opens exhaust one alert's attempts; the other alert then goes through.
        broken_for_one = make_flaky(failures=3)
        rig = Rig(
            session_factory,
            calendar,
            sleeps,
            [*listing(), row("OTHER", 60, 80)],
            writer_factory=broken_for_one,
        )

        with pytest.raises(ExceptionGroup) as raised:
            await rig.poll.run()

        assert len(raised.value.exceptions) == 1
        assert len(await rig.alerts()) == 1
