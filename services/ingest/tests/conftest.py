"""DB-gated fixtures — shared via rs_persistence.testing (the 5499 pattern;
see that module for target resolution and gating semantics)."""

import json

import httpx
import pytest
from sqlalchemy.exc import OperationalError

from rs_observability import OpsSettings, configure_ops
from rs_observability import ops as ops_module
from rs_persistence.testing import migrated_db_url, session_factory  # noqa: F401


@pytest.fixture
def notices(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Ops notices the code under test sent (de-dup off so every send shows)."""
    sent: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content)["content"])
        return httpx.Response(204)

    monkeypatch.setattr(ops_module, "_notifier", None)
    configure_ops(
        OpsSettings(
            _env_file=None,
            rs_ops_webhook_url="https://discord.test/api/webhooks/1/secret",
            rs_ops_dedup_window_seconds=0.0,
        ),
        transport=httpx.MockTransport(handler),
    )
    return sent


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
