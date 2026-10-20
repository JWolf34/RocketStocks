"""schwab-py client factory wired to a token store (S003 OQ#2).

The token is loaded once, up front, and the read func hands schwab-py that
in-memory dict; schwab-py mutates and re-writes it through the write callback
on every refresh, so the DB row tracks refreshes without re-reads. `ingest`
is the single writer (ADR-0024).

authlib stays behind this module: a dead refresh token surfaces to callers as
SchwabAuthError, so they can pause Schwab jobs without importing authlib.
"""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Protocol

from authlib.integrations.base_client import OAuthError
from schwab.auth import client_from_access_functions

logger = logging.getLogger(__name__)


class TokenMissingError(RuntimeError):
    """No token row exists yet: the one-time login has not been run."""


class SchwabAuthError(RuntimeError):
    """The refresh token is dead; only a fresh login recovers it."""


class TokenStore(Protocol):
    """The slice of rs_persistence.SchwabTokenStore the client needs."""

    async def load(self) -> dict[str, Any] | None: ...

    def schedule_save(self, token: dict[str, Any], **kwargs: Any) -> None: ...


@contextmanager
def translate_auth_errors() -> Iterator[None]:
    """Re-raise authlib's OAuthError (token refresh failed) as SchwabAuthError."""
    try:
        yield
    except OAuthError as exc:
        # The code only: the description is vendor free text that lands in logs and ops notices.
        raise SchwabAuthError(f"Schwab refresh token rejected: {exc.error}") from exc


async def build_schwab_client(
    api_key: str, api_secret: str, token_store: TokenStore, use_asyncio: bool = True
) -> Any:
    token = await token_store.load()
    if token is None:
        raise TokenMissingError("no Schwab token in the database")
    return client_from_access_functions(
        api_key,
        api_secret,
        token_read_func=lambda: token,
        token_write_func=token_store.schedule_save,
        asyncio=use_asyncio,
    )
