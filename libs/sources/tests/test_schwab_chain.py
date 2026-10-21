"""Schwab chain fetch over a fake schwab-py client: bounded params, non-OK
raises, a dead refresh token surfaces as SchwabAuthError."""

from datetime import UTC, date, datetime, timedelta

import pytest
from authlib.integrations.base_client import OAuthError

from rs_sources.schwab import ChainFetchError, SchwabAuthError, SchwabChainFetch

NOW = datetime(2026, 7, 16, 14, 35, tzinfo=UTC)


class FixedClock:
    def now(self):
        return NOW


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {"symbol": "SPY"}

    def json(self):
        return self._payload


class FakeSchwabClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def get_option_chain(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


async def test_bounded_params_from_injected_clock():
    client = FakeSchwabClient(FakeResponse(payload={"symbol": "SPY", "status": "SUCCESS"}))
    fetch = SchwabChainFetch(client, FixedClock())
    body = await fetch("SPY")
    assert body == {"symbol": "SPY", "status": "SUCCESS"}
    ((symbol, kwargs),) = client.calls
    assert symbol == "SPY"
    # Exactly the two bounding kwargs (schwab-py 1.5.1 names) - everything
    # else stays server-default; window/cap are client-side.
    assert kwargs == {
        "from_date": date(2026, 7, 16),
        "to_date": date(2026, 7, 16) + timedelta(days=30),
    }


async def test_non_ok_status_raises_for_the_retry_path():
    fetch = SchwabChainFetch(FakeSchwabClient(FakeResponse(status_code=502)), FixedClock())
    with pytest.raises(ChainFetchError, match="SPY: HTTP 502"):
        await fetch("SPY")


async def test_dead_refresh_token_is_a_schwab_auth_error():
    fetch = SchwabChainFetch(FakeSchwabClient(OAuthError(error="invalid_grant")), FixedClock())
    with pytest.raises(SchwabAuthError):
        await fetch("SPY")
