"""The health-reporting wrappers: which call outcomes ping, for which source."""

from datetime import UTC, date, datetime

import httpx
import pytest

from rs_ingest.monitored import (
    MonitoredFilings,
    MonitoredPopularitySource,
    MonitoredQuotes,
    TalliedHistory,
    finish_sweep,
)
from rs_observability import (
    HealthTracker,
    QuietFailure,
    Source,
    SourceFailed,
    SweepTally,
    SweepVerdict,
)
from rs_sources import ApeWisdomError, ApeWisdomRow
from rs_sources.errors import SourceError
from rs_sources.schwab import (
    DailyHistory,
    Quote,
    SchwabAuthError,
    SchwabResponseError,
    TokenMissingError,
)

AS_OF = datetime(2026, 10, 20, 15, 0, tzinfo=UTC)
START = date(2026, 10, 20)
ROW = ApeWisdomRow(rank=1, ticker="AAA", mentions=10, upvotes=3)
QUOTE = Quote(symbol="AAA", last_price=10.0, previous_close=9.0, quote_time=AS_OF, volume=1000)
EMPTY_HISTORY = DailyHistory(bars=[], rejects=[])


class Scripted:
    """An async callable that plays a script (an exception is raised, anything else returned);
    the last entry repeats."""

    def __init__(self, *script):
        self._script = list(script)
        self.calls = 0

    async def __call__(self, *args, **kwargs):
        outcome = self._script[min(self.calls, len(self._script) - 1)]
        self.calls += 1
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class Listing:
    def __init__(self, *script):
        self.fetch_all = Scripted(*script)


class Quotes:
    def __init__(self, *script):
        self.quote = Scripted(*script)


class History:
    def __init__(self, *script):
        self.daily_history = Scripted(*script)


class Filings:
    def __init__(self, *script):
        self.recent_8ks = Scripted(*script)


def verbs(pings: list[str]) -> list[str]:
    """Each ping without its reason: 'Schwab is failing', 'Schwab recovered.'."""
    return [ping.split(" (")[0] for ping in pings]


@pytest.fixture
def health() -> HealthTracker:
    return HealthTracker("ingest")


class TestApeWisdom:
    async def test_a_failed_fetch_pings_once_and_fails_the_poll_without_a_second_ping(self, health, notices):
        source = MonitoredPopularitySource(Listing(ApeWisdomError("HTTP 503")), health)

        for _ in range(3):
            with pytest.raises(SourceFailed):
                await source.fetch_all()

        assert notices == ["ApeWisdom is failing (ApeWisdomError). See the ingest logs."]

    async def test_the_next_good_fetch_recovers_it_once(self, health, notices):
        source = MonitoredPopularitySource(Listing(httpx.ConnectError("x"), [ROW]), health)

        with pytest.raises(SourceFailed):
            await source.fetch_all()
        assert await source.fetch_all() == [ROW]
        await source.fetch_all()

        assert verbs(notices) == ["ApeWisdom is failing", "ApeWisdom recovered."]

    async def test_an_empty_listing_is_the_source_failing(self, health, notices):
        source = MonitoredPopularitySource(Listing([]), health)

        with pytest.raises(SourceFailed):
            await source.fetch_all()

        assert len(notices) == 1


class TestSchwabQuotes:
    async def test_a_connection_failure_pings_once_and_is_raised_unchanged(self, health, notices):
        quotes = MonitoredQuotes(Quotes(httpx.ConnectError("refused")), health)

        for _ in range(3):
            with pytest.raises(httpx.ConnectError):
                await quotes.quote("AAA")

        assert notices == ["Schwab is failing (ConnectError). See the ingest logs."]

    @pytest.mark.parametrize("status", [429, 500, 503])
    async def test_a_429_or_5xx_is_the_source_failing(self, health, notices, status):
        quotes = MonitoredQuotes(Quotes(SchwabResponseError("AAA: HTTP", status)), health)

        with pytest.raises(SchwabResponseError):
            await quotes.quote("AAA")

        assert len(notices) == 1

    @pytest.mark.parametrize("status", [400, 404])
    async def test_a_symbol_schwab_refuses_pings_nothing(self, health, notices, status):
        quotes = MonitoredQuotes(Quotes(SchwabResponseError("ZZZ: HTTP", status)), health)

        for _ in range(5):
            with pytest.raises(SchwabResponseError):
                await quotes.quote("ZZZ")

        assert notices == []

    async def test_no_quote_for_a_symbol_is_an_answer_not_a_failure(self, health, notices):
        quotes = MonitoredQuotes(Quotes(None), health)

        assert await quotes.quote("ZZZ") is None
        assert notices == []

    @pytest.mark.parametrize("error", [SchwabAuthError("dead"), TokenMissingError("none")])
    async def test_a_dead_token_is_the_token_gates_ping_not_a_schwab_failure(self, health, notices, error):
        quotes = MonitoredQuotes(Quotes(error), health)

        with pytest.raises(type(error)):
            await quotes.quote("AAA")

        assert notices == []

    async def test_a_good_quote_after_a_failure_recovers_schwab_once(self, health, notices):
        quotes = MonitoredQuotes(Quotes(httpx.ReadTimeout("slow"), QUOTE), health)

        with pytest.raises(httpx.ReadTimeout):
            await quotes.quote("AAA")
        assert await quotes.quote("AAA") == QUOTE
        await quotes.quote("AAA")

        assert verbs(notices) == ["Schwab is failing", "Schwab recovered."]

    async def test_a_refused_symbol_proves_schwab_answers_so_it_ends_a_failure(self, health, notices):
        outcomes = [httpx.ReadTimeout("x"), SchwabResponseError("ZZZ: HTTP", 404), httpx.ReadTimeout("x")]
        quotes = MonitoredQuotes(Quotes(*outcomes), health)

        for outcome in outcomes:
            with pytest.raises(type(outcome)):
                await quotes.quote("AAA")

        assert verbs(notices) == ["Schwab is failing", "Schwab recovered.", "Schwab is failing"]


async def run_history(*script) -> SweepTally:
    tally = SweepTally()
    history = TalliedHistory(History(*script), tally)
    for _ in script:
        try:
            await history.daily_history("AAA", START, START)
        except Exception:  # noqa: BLE001, S110 - the tally is what is under test
            pass
    return tally


class TestSchwabHistory:
    async def test_symbols_schwab_refuses_count_as_answers(self):
        tally = await run_history(
            SchwabResponseError("A: HTTP", 404), SchwabResponseError("B: HTTP", 400), EMPTY_HISTORY
        )

        assert (tally.attempted, tally.failed) == (3, 0)
        assert tally.verdict is SweepVerdict.HEALTHY

    async def test_a_server_error_or_dropped_connection_counts_as_a_failure(self):
        tally = await run_history(SchwabResponseError("A: HTTP", 503), httpx.ReadTimeout("x"))

        assert (tally.attempted, tally.failed) == (2, 2)
        assert tally.verdict is SweepVerdict.FAILING

    async def test_a_429_speaks_for_the_whole_source(self):
        tally = await run_history(SchwabResponseError("A: HTTP", 429))

        assert tally.verdict is SweepVerdict.FAILING

    async def test_a_dead_token_is_not_counted(self):
        tally = await run_history(SchwabAuthError("dead"))

        assert (tally.attempted, tally.failed) == (0, 0)


class TestEdgarFilings:
    async def test_failing_edgar_pings_edgar_once_and_recovers(self, health, notices):
        filings = MonitoredFilings(
            Filings(SourceError("GET failed", status_code=503), SourceError("GET failed", status_code=503), []),
            health,
        )

        for _ in range(2):
            with pytest.raises(SourceError):
                await filings.recent_8ks("AAA", AS_OF)
        assert await filings.recent_8ks("AAA", AS_OF) == []

        assert verbs(notices) == ["EDGAR is failing", "EDGAR recovered."]

    async def test_a_symbol_edgar_has_no_submissions_for_pings_nothing(self, health, notices):
        filings = MonitoredFilings(Filings(SourceError("GET failed", status_code=404)), health)

        for _ in range(3):
            with pytest.raises(SourceError):
                await filings.recent_8ks("ZZZ", AS_OF)

        assert notices == []


class TestFinishSweep:
    async def test_a_failing_sweep_pings_once_and_raises_a_failure_that_needs_no_second_ping(self, health, notices):
        cause = RuntimeError("x")

        with pytest.raises(SourceFailed) as raised:
            await finish_sweep(health, Source.SCHWAB, SweepTally(attempted=100, failed=10), cause)

        assert raised.value.__cause__ is cause
        assert isinstance(raised.value, QuietFailure)
        assert notices == ["Schwab is failing (10 of 100 requests failed). See the ingest logs."]

    async def test_a_sweep_with_one_stock_failing_pings_nothing_and_raises_nothing(self, health, notices):
        await finish_sweep(health, Source.SCHWAB, SweepTally(attempted=500, failed=1))
        await finish_sweep(health, Source.SCHWAB, SweepTally(attempted=3, failed=1))

        assert notices == []

    async def test_a_clean_sweep_recovers_a_failing_source(self, health, notices):
        await health.report_failed(Source.SCHWAB, "ConnectError")

        await finish_sweep(health, Source.SCHWAB, SweepTally(attempted=500, failed=0))

        assert notices[-1] == "Schwab recovered."
