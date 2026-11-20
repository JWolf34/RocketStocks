"""How a Schwab call failed, sorted in one place for every caller.

- A token failure: the refresh token is dead, or no token was ever stored. Only a sign-in
  recovers it, so the caller stops and the daemon pauses the Schwab jobs until one.
- Rate limited (429): over Schwab's budget. The caller stops, and the failure speaks for the
  whole source however few calls were made.
- Per symbol (400 or 404): Schwab's answer about one symbol. Schwab is fine, the stock is not,
  and asking again gets the same answer.
- Transient: anything else (a connection error, a timeout, a 5xx, a payload that is not what it
  should be). Worth another try, and counted against Schwab's health.
"""

from enum import StrEnum

from rs_sources.errors import PER_SYMBOL_STATUSES
from rs_sources.schwab.client import SchwabAuthError, TokenMissingError
from rs_sources.schwab.market_data import SchwabResponseError

RATE_LIMITED_STATUS = 429
# The token failures, for an `except` clause that only lets them through.
TOKEN_ERRORS = (SchwabAuthError, TokenMissingError)


class SchwabFailure(StrEnum):
    TOKEN = "token"
    RATE_LIMITED = "rate limited"
    PER_SYMBOL = "per symbol"
    TRANSIENT = "transient"


def schwab_failure(exc: BaseException) -> SchwabFailure:
    """Which kind of failure `exc`, raised by a Schwab call, is."""
    if isinstance(exc, TOKEN_ERRORS):
        return SchwabFailure.TOKEN
    if isinstance(exc, SchwabResponseError):
        if exc.status_code == RATE_LIMITED_STATUS:
            return SchwabFailure.RATE_LIMITED
        if exc.status_code in PER_SYMBOL_STATUSES:
            return SchwabFailure.PER_SYMBOL
    return SchwabFailure.TRANSIENT
