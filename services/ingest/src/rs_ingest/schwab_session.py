"""The daemon's ONE schwab-py client (one refresher, so one writer of the token), rebuilt
when the stored token is renewed.

schwab-py holds the token in memory, so after a re-login the old client keeps
failing until it is replaced. Jobs read `client` per run rather than caching it.
"""

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from rs_sources.schwab import TokenMissingError, TokenStore, build_schwab_client

logger = logging.getLogger(__name__)

ClientBuilder = Callable[[str, str, TokenStore], Awaitable[Any]]


class SchwabSession:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        token_store: TokenStore,
        build: ClientBuilder = build_schwab_client,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._token_store = token_store
        self._build = build
        self._client: Any | None = None

    @property
    def client(self) -> Any:
        """The current client; raises while no token has ever been stored."""
        if self._client is None:
            raise TokenMissingError("no Schwab token in the database")
        return self._client

    async def load(self) -> bool:
        """Build the client from the stored token; False when none is stored."""
        try:
            self._client = await self._build(self._api_key, self._api_secret, self._token_store)
        except TokenMissingError:
            return False
        return True

    async def reload(self) -> None:
        """Gate callback after a renewed token; a missing token is an error here."""
        if not await self.load():
            raise TokenMissingError("token row vanished while renewing")
