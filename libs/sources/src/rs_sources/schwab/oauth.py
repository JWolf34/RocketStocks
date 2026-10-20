"""The Schwab OAuth authorization-code exchange behind `/setup schwab`.

`begin()` makes the sign-in URL and an opaque context; `exchange()` turns the
callback URL John pasted back into a token. The callback URL carries a single-use
code, so nothing here logs it or puts it (or any exception text from it) in an
error message: the errors raised are fixed, user-safe strings.
"""

import asyncio
import logging
from typing import Any

import httpx
from authlib.common.errors import AuthlibBaseError
from schwab.auth import AuthContext, client_from_received_url, get_auth_context

logger = logging.getLogger(__name__)


class CallbackRejected(Exception):
    """The pasted URL or its code was not accepted; the message is safe to show."""


class ExchangeFailed(Exception):
    """Schwab could not be reached or answered badly; the message is safe to show."""


class SchwabOAuth:
    def __init__(self, api_key: str, api_secret: str, callback_url: str) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._callback_url = callback_url

    def begin(self) -> tuple[str, AuthContext]:
        context = get_auth_context(self._api_key, self._callback_url)
        return context.authorization_url, context

    async def exchange(self, context: AuthContext, received_url: str) -> dict[str, Any]:
        """The wrapped token dict schwab-py stores (creation_timestamp + token)."""
        written: list[dict[str, Any]] = []

        def exchange_sync() -> None:
            client_from_received_url(
                self._api_key,
                self._api_secret,
                context,
                received_url,
                lambda token, *args, **kwargs: written.append(token),
            )

        try:
            await asyncio.to_thread(exchange_sync)
        except (AuthlibBaseError, ValueError) as exc:
            # The type only: the text can echo the URL or the code.
            logger.warning("Schwab rejected the callback URL (%s)", type(exc).__name__)
            raise CallbackRejected(
                "Schwab did not accept that callback URL. Copy the full address from the "
                "browser right after signing in; each sign-in link works once."
            ) from None
        except httpx.HTTPError as exc:
            logger.warning("Schwab token exchange failed (%s)", type(exc).__name__)
            raise ExchangeFailed("Could not reach Schwab to finish the sign-in. Try again.") from None
        if not written:
            raise ExchangeFailed("Schwab returned no token. Try again.")
        return written[0]
