"""Every Schwab call: client factory, quote, daily history and option chains.

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
from rs_sources.schwab.option_chain import (
    OptionChain,
    SchwabOptionChains,
    normalize_chain,
    schwab_chain_symbol,
)

__all__ = [
    "CallbackRejected",
    "DailyBar",
    "DailyHistory",
    "ExchangeFailed",
    "OptionChain",
    "Quote",
    "RejectReason",
    "RejectedBar",
    "SchwabAuthError",
    "SchwabMarketData",
    "SchwabOptionChains",
    "SchwabOAuth",
    "SchwabResponseError",
    "TokenMissingError",
    "TokenStore",
    "build_schwab_client",
    "normalize_chain",
    "schwab_chain_symbol",
    "translate_auth_errors",
]
