"""The check before a run starts: the stored token loads and Schwab answers one cheap call.

A probe that cannot reach Schwab would otherwise spend hours recording nothing, so it stops
within a minute and says why.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from sqlalchemy.exc import SQLAlchemyError

from rs_ingest.tools.sweep_probe.store import error_text
from rs_sources.schwab import SchwabAuthError, TokenMissingError, translate_auth_errors

PREFLIGHT_SECONDS = 45.0
PREFLIGHT_SYMBOL = "SPY"


class PreflightError(RuntimeError):
    """The run cannot start; the message says why and what to do."""


async def preflight(make_client: Callable[[], Awaitable[Any]], timeout: float = PREFLIGHT_SECONDS) -> None:
    try:
        async with asyncio.timeout(timeout):
            client = await _build(make_client)
            response = await _quote(client)
    except TimeoutError:
        raise PreflightError(f"no answer within {timeout:.0f} seconds") from None
    if response.status_code != 200:
        raise PreflightError(f"Schwab answered {response.status_code} to a quote request for {PREFLIGHT_SYMBOL}")


async def _build(make_client: Callable[[], Awaitable[Any]]) -> Any:
    try:
        return await make_client()
    except TokenMissingError:
        raise PreflightError("no Schwab token in the database: sign in first (ingest holds the only token)") from None
    except SchwabAuthError as exc:
        raise PreflightError(f"the stored Schwab token is dead ({exc}): sign in again") from None
    except (SQLAlchemyError, OSError) as exc:
        raise PreflightError(f"the stored token could not be read from the database: {error_text(exc)}") from None
    except Exception as exc:
        raise PreflightError(f"the Schwab client could not be built: {error_text(exc)}") from None


async def _quote(client: Any) -> Any:
    try:
        with translate_auth_errors():
            return await client.get_quote(PREFLIGHT_SYMBOL)
    except SchwabAuthError as exc:
        raise PreflightError(f"the stored Schwab token is dead ({exc}): sign in again") from None
    except httpx.TimeoutException:
        raise PreflightError("Schwab did not answer a quote request in time") from None
    except Exception as exc:
        raise PreflightError(f"the quote request failed: {error_text(exc)}") from None
