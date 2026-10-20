"""Every Schwab call (D7): client factory, option chains, quote and daily history.

schwab-py and authlib are imported only inside this package.
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

__all__ = [
    "DTE_BOUND",
    "ChainFetchError",
    "DailyBar",
    "DailyHistory",
    "Quote",
    "RejectReason",
    "RejectedBar",
    "SchwabAuthError",
    "SchwabChainFetch",
    "SchwabMarketData",
    "SchwabResponseError",
    "TokenMissingError",
    "TokenStore",
    "build_schwab_client",
]
