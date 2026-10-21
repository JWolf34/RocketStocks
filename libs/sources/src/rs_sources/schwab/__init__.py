"""Every Schwab call: client factory, option chains, quote and daily history.

The REST adapters keep schwab-py and authlib inside this package.
"""

from rs_sources.schwab.chain import DTE_BOUND, ChainFetchError, SchwabChainFetch
from rs_sources.schwab.client import (
    SchwabAuthError,
    TokenMissingError,
    TokenStore,
    build_schwab_client,
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
    "DTE_BOUND",
    "CallbackRejected",
    "ChainFetchError",
    "DailyBar",
    "DailyHistory",
    "ExchangeFailed",
    "Quote",
    "RejectReason",
    "RejectedBar",
    "SchwabAuthError",
    "SchwabChainFetch",
    "SchwabMarketData",
    "SchwabOAuth",
    "SchwabResponseError",
    "TokenMissingError",
    "TokenStore",
    "build_schwab_client",
]
