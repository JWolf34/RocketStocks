"""build_schwab_client wiring: token sourced from the store, write callback
bound to schedule_save, and a clear failure when no token exists."""

import pytest
from authlib.integrations.base_client.errors import OAuthError

import rs_sources.schwab.client as schwab_client
from rs_sources.schwab import TokenMissingError, build_schwab_client

TOKEN = {"creation_timestamp": 1, "token": {"access_token": "a"}}


class FakeStore:
    def __init__(self, token):
        self._token = token
        self.saved = []

    async def load(self):
        return self._token

    def schedule_save(self, token, **kwargs):
        self.saved.append(token)


async def test_missing_token_raises():
    with pytest.raises(TokenMissingError, match="no Schwab token"):
        await build_schwab_client("key", "hunter2", FakeStore(None))


async def test_client_wired_to_store(monkeypatch):
    captured = {}

    def fake_factory(api_key, app_secret, token_read_func, token_write_func, asyncio):
        captured.update(
            api_key=api_key,
            app_secret=app_secret,
            read=token_read_func,
            write=token_write_func,
            asyncio=asyncio,
        )
        return "client"

    monkeypatch.setattr(schwab_client, "client_from_access_functions", fake_factory)
    store = FakeStore(TOKEN)
    client = await build_schwab_client("key", "hunter2", store)

    assert client == "client"
    assert captured["api_key"] == "key"
    assert captured["app_secret"] == "hunter2"
    assert captured["asyncio"] is True
    assert captured["read"]() == TOKEN
    captured["write"](TOKEN, refresh_token="r")
    assert store.saved == [TOKEN]


def test_a_rejected_refresh_keeps_the_oauth_code_and_drops_the_description():
    with pytest.raises(schwab_client.SchwabAuthError) as caught:
        with schwab_client.translate_auth_errors():
            raise OAuthError(error="invalid_grant", description="token S3CR3T expired")

    assert "invalid_grant" in str(caught.value)
    assert "S3CR3T" not in str(caught.value)
