"""Every Schwab call: client factory, quote and daily history.

The REST adapters keep schwab-py and authlib inside this package.
"""

from rs_sources.schwab.client import (
    SchwabAuthError,
    TokenMissingError,
    TokenStore,
    build_schwab_client,
    translate_auth_errors,
)
from rs_sources.schwab.market_data import (
    DailyBar,
    DailyHistory,
    Quote,
    RejectedBar,
    RejectReason,
    SchwabMarketData,
    SchwabResponseError,
)
from rs_sources.schwab.oauth import CallbackRejected, ExchangeFailed, SchwabOAuth

__all__ = [
    "CallbackRejected",
    "DailyBar",
    "DailyHistory",
    "ExchangeFailed",
    "Quote",
    "RejectReason",
    "RejectedBar",
    "SchwabAuthError",
    "SchwabMarketData",
    "SchwabOAuth",
    "SchwabResponseError",
    "TokenMissingError",
    "TokenStore",
    "build_schwab_client",
    "translate_auth_errors",
]
