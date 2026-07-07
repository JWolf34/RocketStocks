"""build_schwab_client wiring: token sourced from the store, write callback
bound to schedule_save, and a helpful failure when no token exists."""

import pytest

import rs_ingest.schwab_client as schwab_client
from rs_ingest.schwab_client import TokenMissingError, build_schwab_client
from rs_ingest.settings import IngestSettings

TOKEN = {"creation_timestamp": 1, "token": {"access_token": "a"}}


class FakeStore:
    def __init__(self, token):
        self._token = token
        self.saved = []

    async def load(self):
        return self._token

    def schedule_save(self, token, **kwargs):
        self.saved.append(token)


@pytest.fixture
def settings(monkeypatch):
    monkeypatch.setenv("RS_DB_URL", "postgresql+psycopg://u:p@localhost:5498/rs")
    monkeypatch.setenv("SCHWAB_API_KEY", "key")
    monkeypatch.setenv("SCHWAB_API_SECRET", "hunter2")
    return IngestSettings(_env_file=None)


async def test_missing_token_raises_with_login_hint(settings):
    with pytest.raises(TokenMissingError, match="rs_ingest.tools.login"):
        await build_schwab_client(settings, FakeStore(None))


async def test_client_wired_to_store(settings, monkeypatch):
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
    client = await build_schwab_client(settings, store)

    assert client == "client"
    assert captured["api_key"] == "key"
    assert captured["app_secret"] == "hunter2"  # unwrapped from SecretStr
    assert captured["asyncio"] is True
    assert captured["read"]() == TOKEN
    captured["write"](TOKEN, refresh_token="r")
    assert store.saved == [TOKEN]
