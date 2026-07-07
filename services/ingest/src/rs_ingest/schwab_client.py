"""schwab-py client factory wired to the Postgres token store (S003 OQ#2).

The token is loaded once, up front, and the read func hands schwab-py that
in-memory dict; schwab-py mutates and re-writes it through the write callback
on every refresh, so the DB row tracks refreshes without re-reads. `ingest`
is the single writer (ADR-0024).
"""

import logging

from schwab.auth import client_from_access_functions

from rs_ingest.settings import IngestSettings
from rs_persistence import SchwabTokenStore

logger = logging.getLogger(__name__)

LOGIN_HINT = "no Schwab token in the database — run: python -m rs_ingest.tools.login"


class TokenMissingError(RuntimeError):
    pass


async def build_schwab_client(settings: IngestSettings, token_store: SchwabTokenStore, use_asyncio: bool = True):
    token = await token_store.load()
    if token is None:
        raise TokenMissingError(LOGIN_HINT)
    return client_from_access_functions(
        settings.schwab_api_key,
        settings.schwab_api_secret.get_secret_value(),
        token_read_func=lambda: token,
        token_write_func=token_store.schedule_save,
        asyncio=use_asyncio,
    )
