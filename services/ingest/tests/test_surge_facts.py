"""The facts block: each fact degrades on its own and never blocks the alert."""

import asyncio
from datetime import UTC, date, datetime, timedelta

from rs_domain import EarningsTime, FactUnavailable, NextEarningsFact, PriceMoveFact
from rs_ingest.surge_facts import SurgeFactsGatherer
from rs_sources.earnings import NextEarnings
from rs_sources.schwab import Quote, SchwabAuthError, TokenMissingError

NOW = datetime(2026, 10, 19, 15, 0, tzinfo=UTC)


def quote(symbol: str = "ABC", *, at: datetime = NOW, volume: int = 3_000_000) -> Quote:
    return Quote(symbol=symbol, last_price=12.5, previous_close=10.0, quote_time=at, volume=volume)


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
    *,
    quotes: FakeQuotes | None = None,
    earnings: FakeEarnings | None = None,
    tripped: Tripped | None = None,
    fact_timeout: timedelta = timedelta(seconds=5),
) -> SurgeFactsGatherer:
    quote_source = quotes or FakeQuotes(quote())
    return SurgeFactsGatherer(
        quotes=lambda: quote_source,
        limiter=NoLimit(),
        earnings=earnings or FakeEarnings(),
        on_token_dead=tripped or Tripped(),
        fact_timeout=fact_timeout,
    )


async def test_the_price_and_earnings_are_gathered_when_all_sources_answer():
    sources = gatherer(
        earnings=FakeEarnings(
            NextEarnings(date=date(2026, 11, 3), time_of_day=EarningsTime.PRE_MARKET, estimated=False)
        ),
    )

    gathered = await sources.gather("ABC", NOW)

    facts = gathered.facts
    assert facts.price_move == PriceMoveFact(
        last_price=12.5, previous_close=10.0, change_pct=25.0, quote_time=NOW
    )
    assert facts.next_earnings == NextEarningsFact(
        date=date(2026, 11, 3), estimated=False, time_of_day="pre_market"
    )
    assert (gathered.alert_price, gathered.quote_time) == (12.5, NOW)


async def test_only_the_alerts_own_symbol_is_quoted():
    quotes = FakeQuotes(quote())

    await gatherer(quotes=quotes).gather("ABC", NOW)

    assert quotes.symbols == ["ABC"]  # no volume, no benchmark


async def test_a_dead_token_drops_price_facts_and_trips_the_gate_once():
    tripped = Tripped()
    sources = gatherer(
        quotes=FakeQuotes(SchwabAuthError("refresh token rejected")),
        tripped=tripped,
    )

    first = await sources.gather("ABC", NOW)
    second = await sources.gather("XYZ", NOW)

    dead = FactUnavailable(reason="Schwab token is dead")
    for gathered in (first, second):
        assert gathered.facts.price_move == dead
        assert (gathered.alert_price, gathered.quote_time) == (None, None)
    assert len(tripped.reasons) == 1


async def test_a_dead_token_is_not_retried_for_later_alerts():
    quotes = FakeQuotes(SchwabAuthError("refresh token rejected"))
    sources = gatherer(quotes=quotes)

    await sources.gather("ABC", NOW)
    await sources.gather("XYZ", NOW)

    # Only the first alert's lookups reach Schwab; the second alert skips it.
    assert "XYZ" not in quotes.symbols
    assert quotes.calls <= 2


async def test_a_missing_token_drops_price_facts_without_tripping_the_gate():
    tripped = Tripped()
    sources = gatherer(
        quotes=FakeQuotes(TokenMissingError("no token")), tripped=tripped
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.price_move == FactUnavailable(reason="no Schwab token stored")
    assert gathered.alert_price is None
    assert tripped.reasons == []  # startup already tripped it


async def test_an_unknown_symbol_has_no_quote_facts():
    sources = gatherer(quotes=FakeQuotes(None))

    gathered = await sources.gather("ZZZZ", NOW)

    assert gathered.facts.price_move == FactUnavailable(reason="Schwab has no quote for this symbol")
    assert gathered.alert_price is None


async def test_a_failing_source_only_loses_its_own_fact():
    sources = gatherer(
        earnings=FakeEarnings(RuntimeError("boom")),
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.next_earnings == FactUnavailable(reason="source failed (RuntimeError)")
    assert isinstance(gathered.facts.price_move, PriceMoveFact)


async def test_a_slow_source_times_out_instead_of_blocking():
    sources = gatherer(
        earnings=FakeEarnings(hang=True),
        fact_timeout=timedelta(milliseconds=50),
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.next_earnings == FactUnavailable(reason="timed out after 0.05s")
    assert isinstance(gathered.facts.price_move, PriceMoveFact)


async def test_unknown_earnings_are_unavailable_not_guessed():
    gathered = await gatherer().gather("ABC", NOW)

    assert gathered.facts.next_earnings == FactUnavailable(reason="no upcoming earnings date found")


async def test_an_estimated_earnings_date_is_labelled():
    sources = gatherer(
        earnings=FakeEarnings(NextEarnings(date=date(2026, 11, 20), estimated=True)),
    )

    gathered = await sources.gather("ABC", NOW)

    assert gathered.facts.next_earnings == NextEarningsFact(date=date(2026, 11, 20), estimated=True)
