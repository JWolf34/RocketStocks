"""The probe reads the stored token like ingest does and never writes it back (real test database)."""

import httpx
import pytest

from rs_ingest.tools.sweep_probe.tokens import ReadOnlyTokenStore
from rs_persistence import SchwabTokenStore
from rs_sources.schwab import TokenMissingError, build_schwab_client


def stored_token(expires_at: float) -> dict:
    return {
        "creation_timestamp": 1_790_000_000,
        "token": {
            "access_token": "stored-access",
            "refresh_token": "stored-refresh",
            "expires_in": 1800,
            "token_type": "Bearer",
            "scope": "api",
            "expires_at": expires_at,
        },
    }


@pytest.fixture
def store(session_factory) -> SchwabTokenStore:
    return SchwabTokenStore(session_factory)


async def test_without_a_stored_token_building_a_client_says_so(store):
    with pytest.raises(TokenMissingError):
        await build_schwab_client("key", "secret", ReadOnlyTokenStore(store))


async def test_an_expired_access_token_is_refreshed_in_memory_and_the_row_is_left_alone(store):
    expired = stored_token(1_700_000_000)  # long past for authlib, which reads the real clock
    await store.save(expired)
    client = await build_schwab_client("key", "secret", ReadOnlyTokenStore(store))
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth/token"):
            body = {"access_token": "fresh", "expires_in": 1800, "token_type": "Bearer", "scope": "api"}
            return httpx.Response(200, json=body)
        requests.append(request)
        return httpx.Response(200, json={"status": "SUCCESS"})

    client.session._transport = httpx.MockTransport(handler)  # no vendor traffic from a test

    response = await client.get_option_chain("AAPL", contract_type=client.Options.ContractType.ALL)

    assert response.status_code == 200
    assert requests[0].headers["authorization"] == "Bearer fresh"
    await store.drain()
    assert await store.load() == expired


async def test_a_refresh_write_from_schwab_py_goes_nowhere(store):
    live = stored_token(4_000_000_000)
    await store.save(live)
    client = await build_schwab_client("key", "secret", ReadOnlyTokenStore(store))

    await client.session.update_token({"access_token": "rotated", "refresh_token": "rotated", "expires_at": 1})

    await store.drain()
    assert await store.load() == live
