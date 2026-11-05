"""DB-gated fixtures - shared via rs_persistence.testing (the 5499 pattern;
see that module for target resolution and gating semantics)."""

from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, date, datetime, time

import pytest
from sqlalchemy.exc import OperationalError

from rs_domain import EASTERN, UniverseCounts, UniverseListing, UniverseSnapshot
from rs_observability.testing import captured_ops_pings
from rs_persistence import UniverseRepository
from rs_persistence.testing import migrated_db_url, session_factory  # noqa: F401


@pytest.fixture
def notices() -> Iterator[list[str]]:
    """Ops pings the code under test sent (de-dup off so every send shows)."""
    with captured_ops_pings() as sent:
        yield sent


class Sleeps:
    """A `sleep` stand-in that records the requested delays and returns at once."""

    def __init__(self) -> None:
        self.seconds: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.seconds.append(seconds)


@pytest.fixture
def sleeps() -> Sleeps:
    return Sleeps()


class _FailingSession:
    def __init__(self, error: Exception) -> None:
        self._error = error

    async def __aenter__(self):
        raise self._error

    async def __aexit__(self, *exc_info) -> None:
        return None


class FlakyFactory:
    """A session factory whose first `failures` sessions fail to open."""

    def __init__(self, real, failures: int, error: Exception) -> None:
        self._real = real
        self.failures = failures
        self._error = error
        self.opened = 0

    def __call__(self):
        self.opened += 1
        if self.failures > 0:
            self.failures -= 1
            return _FailingSession(self._error)
        return self._real()


@pytest.fixture
def make_flaky(session_factory):  # noqa: F811 - the fixture imported above
    """Build a FlakyFactory over the test database; the default error is a
    dropped connection (transient)."""

    def build(failures: int, error: Exception | None = None) -> FlakyFactory:
        dropped = OperationalError("INSERT", {}, Exception("server closed the connection"))
        return FlakyFactory(session_factory, failures, error or dropped)

    return build


@pytest.fixture
def seed_previous(session_factory) -> Callable[..., Awaitable[None]]:  # noqa: F811 - the fixture imported above
    """Store the snapshot before: AAA and OLD optionable, source counts 2 / 2 / 2 / 2 unless overridden.

    Call it as `await seed_previous(day=..., nasdaq_listed=..., ...)`; `day` defaults to Friday 2026-10-23.
    """

    async def seed(day: date = date(2026, 10, 23), **counts: int | None) -> None:
        listings = tuple(
            UniverseListing(
                symbol=symbol,
                security_name=symbol,
                exchange="Q",
                etf=False,
                financial_status="N",
                cqs_symbol=None,
                nasdaq_symbol=None,
                optionable=symbol in {"AAA", "OLD"},
            )
            for symbol in ("AAA", "BBB", "CCC", "DDD", "OLD")
        )
        defaults = {"nasdaq_listed": 2, "other_listed": 2, "options_symbols": 2, "cboe_symbols": 2}
        async with session_factory() as s:
            await UniverseRepository(s).save(
                UniverseSnapshot(
                    snapshot_date=day,
                    as_of=datetime.combine(day, time(18, 1), tzinfo=EASTERN).astimezone(UTC),
                    counts=UniverseCounts(**(defaults | counts)),
                    optionable_carried=False,
                    listings=listings,
                )
            )
            await s.commit()

    return seed
