"""OAuth code-to-token exchange for Schwab re-authentication."""
import logging

import schwab

logger = logging.getLogger(__name__)


def exchange_code_for_token(
    api_key: str,
    app_secret: str,
    auth_context,
    received_url: str,
    asyncio: bool = True,
):
    """Exchange an OAuth authorization code for tokens.

    Returns ``(new_client, token_dict)``.  The caller is responsible for
    persisting the token (e.g. via ``SchwabTokenRepository.save_token``).
    Raises on failure (e.g. invalid/expired auth code, state mismatch).
    """
    captured: dict = {}

    def _capture(token_dict: dict) -> None:
        captured.update(token_dict)

    client = schwab.auth.client_from_received_url(
        api_key=api_key,
        app_secret=app_secret,
        auth_context=auth_context,
        received_url=received_url,
        token_write_func=_capture,
        asyncio=asyncio,
    )
    return client, captured
