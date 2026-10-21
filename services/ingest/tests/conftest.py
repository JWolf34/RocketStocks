"""DB-gated fixtures — shared via rs_persistence.testing (the 5499 pattern;
see that module for target resolution and gating semantics)."""

from collections.abc import Iterator

import pytest
from sqlalchemy.exc import OperationalError

from rs_observability.testing import captured_ops_pings
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
