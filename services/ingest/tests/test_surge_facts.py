"""The facts block: each fact degrades on its own and never blocks the alert."""

import asyncio
from datetime import UTC, date, datetime, timedelta

import pytest

from rs_domain import (
    BenchmarkFact,
    FactUnavailable,
    NextEarningsFact,
    PriceMoveFact,
    RecentFilingsFact,
    StoredDailyBar,
    VolumeFact,
)
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_persistence import DailyBarRepository
from rs_sources import ExchangeCalendar
from rs_sources.earnings import NextEarnings
from rs_sources.edgar import Filing8K
from rs_sources.errors import SourceError
from rs_sources.nasdaq import EarningsTime
from rs_sources.schwab import Quote, SchwabAuthError, TokenMissingError

# A Monday 15:00 UTC = 11:00 Eastern, mid-session.
NOW = datetime(2026, 10, 19, 15, 0, tzinfo=UTC)
AFTER_CLOSE = datetime(2026, 10, 19, 22, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def calendar() -> ExchangeCalendar:
    return ExchangeCalendar()


def quote(symbol: str = "ABC", *, at: datetime = NOW, volume: int = 3_000_000) -> Quote:
    return Quote(symbol=symbol, last_price=12.5, previous_close=10.0, quote_time=at, volume=volume)


def filing(filed: date, form: str = "8-K") -> Filing8K:
    return Filing8K(
        accession_number="0001-26-000001",
        filing_date=filed,
        accepted_at=datetime.combine(filed, datetime.min.time(), tzinfo=UTC),
        form=form,
        items=("2.02",),
        url=f"https://www.sec.gov/Archives/{filed.isoformat()}.htm",
    )


class FakeQuotes:
    """Answers `result` for every symbol except those in `by_symbol`."""

    def __init__(
        self,
        result: Quote | Exception | None = None,
        by_symbol: dict[str, Quote | Exception | None] | None = None,
    ) -> None:
        self.result = result
        self.by_symbol = by_symbol or {}
        self.symbols: list[str] = []

    @property
    def calls(self) -> int:
        return len(self.symbols)

    async def quote(self, symbol: str) -> Quote | None:
        self.symbols.append(symbol)
        answer = self.by_symbol.get(symbol, self.result)
        if isinstance(answer, Exception):
            raise answer
        return answer


class FakeFilings:
    def __init__(self, result: list[Filing8K] | Exception | None = None) -> None:
        self.result = [] if result is None else result

    async def recent_8ks(self, ticker: str, as_of: datetime, limit: int | None = 5) -> list[Filing8K]:
        if isinstance(self.result, Exception):
            raise self.result
        return self.result[:limit]


class FakeEarnings:
    def __init__(self, result: NextEarnings | Exception | None = None, *, hang: bool = False) -> None:
        self.result = NextEarnings.unknown() if result is None else result
        self.hang = hang

    async def next_earnings(self, ticker: str, as_of: datetime) -> NextEarnings:
        if self.hang:
            await asyncio.sleep(3600)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class NoLimit:
    async def acquire(self) -> None:
        return None


class Tripped:
    def __init__(self) -> None:
        self.reasons: list[str] = []

    async def __call__(self, reason: str) -> None:
        self.reasons.append(reason)


def gatherer(
    session_factory,
    calendar,
    *,
    quotes: FakeQuotes | None = None,
    filings: FakeFilings | None = None,
    earnings: FakeEarnings | None = None,
    tripped: Tripped | None = None,
    fact_timeout: timedelta = timedelta(seconds=5),
) -> SurgeFactsGatherer:
    quote_source = quotes or FakeQuotes(quote(), {"SPY": quote("SPY", at=NOW).model_copy(update={"last_price": 512.25})})
    return SurgeFactsGatherer(
        quotes=lambda: quote_source,
        limiter=NoLimit(),
        filings=filings or FakeFilings(),
        earnings=earnings or FakeEarnings(),
        session_factory=session_factory,
        calendar=calendar,
        on_token_dead=tripped or Tripped(),
        fact_timeout=fact_timeout,
    )


async def seed_bars(session_factory, symbol: str, sessions: list[date], volume: int = 1_000_000) -> None:
    async with session_factory() as session:
        await DailyBarRepository(session).insert_many(
            [
                StoredDailyBar(
                    symbol=symbol,
                    session_date=day,
                    open=10.0,
                    high=11.0,
                    low=9.0,
                    close=10.0,
                    volume=volume,
                    fetched_at=NOW,
                    adjusted=True,
                )
                for day in sessions
            ]
        )
        await session.commit()


def sessions_before(calendar: ExchangeCalendar, end: date, count: int) -> list[date]:
    return list(calendar.sessions_in_range(end - timedelta(days=60), end - timedelta(days=1)))[-count:]


async def test_every_fact_is_gathered_when_all_sources_answer(session_factory, calendar):
    await seed_bars(session_factory, "ABC", sessions_before(calendar, NOW.date(), 20))
    sources = gatherer(
        session_factory,
        calendar,
        filings=FakeFilings([filing(date(2026, 10, 15))]),
        earnings=FakeEarnings(
            NextEarnings(date=date(2026, 11, 3), time_of_day=EarningsTime.PRE_MARKET, estimated=False)
        ),
    )

    gathered = await sources.gather("ABC", NOW)

    facts = gathered.facts
    assert facts.price_move == PriceMoveFact(
        last_price=12.5, previous_close=10.0, change_pct=25.0, quote_time=NOW
    )
    assert facts.volume == VolumeFact(
        volume=3_000_000, average_volume=1_000_000.0, sessions=20, ratio=3.0, intraday=True
    )
    assert isinstance(facts.recent_filings, RecentFilingsFact)
    assert [f.filing_date for f in facts.recent_filings.filings] == [date(2026, 10, 15)]
    assert facts.next_earnings == NextEarningsFact(
        date=date(2026, 11, 3), estimated=False, time_of_day="pre_market"
    )
    assert facts.benchmark == BenchmarkFact(symbol="SPY", price=512.25, quote_time=NOW)
    assert (gathered.alert_price, gathered.quote_time) == (12.5, NOW)


async def test_a_quote_after_the_close_is_not_flagged_intraday(session_factory, calendar):
    await seed_bars(session_factory, "ABC", sessions_before(calendar, NOW.date(), 20))
    sources = gatherer(session_factory, calendar, quotes=FakeQuotes(quote(at=AFTER_CLOSE)))

    gathered = await sources.gather("ABC", AFTER_CLOSE)

    assert isinstance(gathered.facts.volume, VolumeFact)
    assert gathered.facts.volume.intraday is False


async def test_the_average_ignores_the_quotes_own_session(session_factory, calendar):
    prior = sessions_before(calendar, NOW.date(), 20)
    await seed_bars(session_factory, "ABC", prior)
    await seed_bars(session_factory, "ABC", [NOW.date()], volume=50_000_000)
    sources = gatherer(session_factory, calendar)

    gathered = await sources.gather("ABC", NOW)

    assert isinstance(gathered.facts.volume, VolumeFact)
    assert gathered.facts.volume.average_volume == 1_000_000.0


async def test_volume_is_omitted_with_a_reason_when_bars_are_missing(session_factory, calendar):
    sources = gatherer(session_factory, calendar)

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.volume == FactUnavailable(reason="only 0 of 20 daily bars stored for the average")
    assert isinstance(gathered.facts.price_move, PriceMoveFact)  # the quote still stands


async def test_volume_needs_the_full_twenty_sessions(session_factory, calendar):
    await seed_bars(session_factory, "ABC", sessions_before(calendar, NOW.date(), 7))

    gathered = await gatherer(session_factory, calendar).gather("ABC", NOW)

    assert gathered.facts.volume == FactUnavailable(reason="only 7 of 20 daily bars stored for the average")


async def test_stale_bars_are_not_presented_as_the_recent_average(session_factory, calendar):
    old_end = NOW.date() - timedelta(days=12)
    await seed_bars(session_factory, "ABC", sessions_before(calendar, old_end, 20))

    gathered = await gatherer(session_factory, calendar).gather("ABC", NOW)

    assert isinstance(gathered.facts.volume, FactUnavailable)
    assert gathered.facts.volume.reason.startswith("daily bars are stale (newest is ")


async def test_a_dead_token_drops_price_facts_and_trips_the_gate_once(session_factory, calendar):
    tripped = Tripped()
    sources = gatherer(
        session_factory,
        calendar,
        quotes=FakeQuotes(SchwabAuthError("refresh token rejected")),
        tripped=tripped,
    )

    first = await sources.gather("ABC", NOW)
    second = await sources.gather("XYZ", NOW)

    dead = FactUnavailable(reason="Schwab token is dead")
    for gathered in (first, second):
        assert gathered.facts.price_move == dead
        assert gathered.facts.volume == dead
        assert (gathered.alert_price, gathered.quote_time) == (None, None)
        assert isinstance(gathered.facts.recent_filings, RecentFilingsFact)
    assert len(tripped.reasons) == 1


async def test_a_dead_token_is_not_retried_for_later_alerts(session_factory, calendar):
    quotes = FakeQuotes(SchwabAuthError("refresh token rejected"))
    sources = gatherer(session_factory, calendar, quotes=quotes)

    await sources.gather("ABC", NOW)
    await sources.gather("XYZ", NOW)

    # Only the first alert's lookups reach Schwab; the second alert skips it.
    assert "XYZ" not in quotes.symbols
    assert quotes.calls <= 2


async def test_a_failing_benchmark_quote_only_loses_the_benchmark(session_factory, calendar):
    quotes = FakeQuotes(quote(), {"SPY": SourceError("quote endpoint down")})
    sources = gatherer(session_factory, calendar, quotes=quotes)

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.benchmark == FactUnavailable(reason="source failed (SourceError)")
    assert isinstance(gathered.facts.price_move, PriceMoveFact)
    assert gathered.alert_price == 12.5


async def test_an_unquoted_benchmark_is_unavailable(session_factory, calendar):
    quotes = FakeQuotes(quote(), {"SPY": None})

    gathered = await gatherer(session_factory, calendar, quotes=quotes).gather("ABC", NOW)

    assert gathered.facts.benchmark == FactUnavailable(reason="Schwab has no quote for this symbol")


async def test_a_dead_token_drops_the_benchmark_too(session_factory, calendar):
    quotes = FakeQuotes(SchwabAuthError("refresh token rejected"))

    gathered = await gatherer(session_factory, calendar, quotes=quotes).gather("ABC", NOW)

    assert gathered.facts.benchmark == FactUnavailable(reason="Schwab token is dead")


async def test_a_missing_token_drops_price_facts_without_tripping_the_gate(session_factory, calendar):
    tripped = Tripped()
    sources = gatherer(
        session_factory, calendar, quotes=FakeQuotes(TokenMissingError("no token")), tripped=tripped
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.price_move == FactUnavailable(reason="no Schwab token stored")
    assert gathered.alert_price is None
    assert tripped.reasons == []  # startup already tripped it


async def test_an_unknown_symbol_has_no_quote_facts(session_factory, calendar):
    sources = gatherer(session_factory, calendar, quotes=FakeQuotes(None))

    gathered = await sources.gather("ZZZZ", NOW)

    assert gathered.facts.price_move == FactUnavailable(reason="Schwab has no quote for this symbol")
    assert gathered.alert_price is None


async def test_a_failing_source_only_loses_its_own_fact(session_factory, calendar):
    sources = gatherer(
        session_factory,
        calendar,
        filings=FakeFilings(SourceError("GET https://data.sec.gov/ failed")),
        earnings=FakeEarnings(RuntimeError("boom")),
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.recent_filings == FactUnavailable(reason="source failed (SourceError)")
    assert gathered.facts.next_earnings == FactUnavailable(reason="source failed (RuntimeError)")
    assert isinstance(gathered.facts.price_move, PriceMoveFact)


async def test_a_slow_source_times_out_instead_of_blocking(session_factory, calendar):
    sources = gatherer(
        session_factory,
        calendar,
        earnings=FakeEarnings(hang=True),
        fact_timeout=timedelta(milliseconds=50),
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.next_earnings == FactUnavailable(reason="timed out after 0.05s")
    assert isinstance(gathered.facts.price_move, PriceMoveFact)


async def test_unknown_earnings_are_unavailable_not_guessed(session_factory, calendar):
    gathered = await gatherer(session_factory, calendar).gather("ABC", NOW)

    assert gathered.facts.next_earnings == FactUnavailable(reason="no upcoming earnings date found")


async def test_an_estimated_earnings_date_is_labelled(session_factory, calendar):
    sources = gatherer(
        session_factory,
        calendar,
        earnings=FakeEarnings(NextEarnings(date=date(2026, 11, 20), estimated=True)),
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.next_earnings == NextEarningsFact(date=date(2026, 11, 20), estimated=True)


async def test_only_recent_filings_are_kept_and_none_is_an_answer(session_factory, calendar):
    old = filing(date(2026, 8, 1))
    sources = gatherer(session_factory, calendar, filings=FakeFilings([old]))

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.recent_filings == RecentFilingsFact(window_days=30, filings=())
