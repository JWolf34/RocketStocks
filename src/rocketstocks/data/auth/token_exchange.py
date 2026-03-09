"""OAuth code-to-token exchange for Schwab re-authentication."""
import json
import logging

import schwab

logger = logging.getLogger(__name__)


def exchange_code_for_token(
    api_key: str,
    app_secret: str,
    auth_context,
    received_url: str,
    token_path: str,
    asyncio: bool = True,
):
    """Exchange an OAuth authorization code for tokens and save them to *token_path*.

    Wraps ``schwab.auth.client_from_received_url()`` with a file-writing
    ``token_write_func``.  Returns the new Schwab client on success; raises on
    failure (e.g. invalid/expired auth code, state mismatch).
    """
    def _write_token(token_dict: dict) -> None:
        with open(token_path, "w") as f:
            json.dump(token_dict, f)
        logger.info(f"Schwab token saved to {token_path!r}")

    client = schwab.auth.client_from_received_url(
        api_key=api_key,
        app_secret=app_secret,
        auth_context=auth_context,
        received_url=received_url,
        token_write_func=_write_token,
        asyncio=asyncio,
    )
    return client
