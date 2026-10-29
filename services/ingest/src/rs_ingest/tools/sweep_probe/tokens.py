"""The probe's view of the stored Schwab token: read, never written.

Ingest is the single writer of the token row. When the stored access token is expired at
read time, schwab-py (authlib underneath) refreshes it in memory before the first request,
using the stored refresh token, and hands the new token to the write callback; here that
callback discards it, so the row stays as ingest left it.
"""

import logging
from typing import Any

from rs_sources.schwab import TokenStore

logger = logging.getLogger(__name__)


class ReadOnlyTokenStore:
    """A `TokenStore` whose writes go nowhere."""

    def __init__(self, store: TokenStore) -> None:
        self._store = store

    async def load(self) -> dict[str, Any] | None:
        return await self._store.load()

    def schedule_save(self, token: dict[str, Any], **kwargs: Any) -> None:
        logger.debug("token refresh write discarded; ingest owns the stored token")
