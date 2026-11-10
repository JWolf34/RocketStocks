"""Schwab option chains: one underlying's chain, optionally bounded by an expiry range.

The adapter fetches and normalizes; it reads no clock and keeps no state, so the ladder
(retry, split into expiry ranges, Tombstone) belongs to the caller. A vendor answer for
the symbol (400) or any other failure raises SchwabResponseError with its `status_code`; a
chain with no contracts is a result, not an error.
"""

import logging
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, ValidationError

from rs_domain import OptionContract
from rs_sources.schwab.client import translate_auth_errors
from rs_sources.schwab.market_data import SchwabResponseError, response_payload

logger = logging.getLogger(__name__)

# Schwab's "no value" for volatility and the greeks.
_NO_VALUE = -999.0
_PUT_CALL: dict[str, Literal["C", "P"]] = {"CALL": "C", "PUT": "P"}


def schwab_chain_symbol(symbol: str) -> str:
    """The spelling Schwab's chain endpoint accepts for a Universe symbol.

    Class shares are slashed (`BRK.B` is `BRK/B`); the dotted and dot-less spellings answer 400.
    """
    return symbol.strip().upper().replace(".", "/")


class OptionChain(BaseModel):
    """A normalized chain, contracts ordered by expiry, strike and side.

    `skipped` counts contracts the payload carried that could not be stored (malformed, or a
    repeat of an option symbol already seen), and `truncated` is Schwab's own flag.
    """

    model_config = ConfigDict(frozen=True)

    underlying: str
    underlying_price: float | None
    contracts: tuple[OptionContract, ...] = ()
    skipped: int = 0
    truncated: bool = False


class SchwabOptionChains:
    """Option chains over the daemon's ONE schwab-py async client."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def chain(self, symbol: str, start: date | None = None, end: date | None = None) -> OptionChain:
        """The chain of Universe symbol `symbol`, all contract types.

        `start` and `end` bound the expiries inclusively and are each optional (a range with
        no end is open-ended); with neither, the whole chain is requested.
        """
        if start is not None and end is not None and start > end:
            raise ValueError(f"start {start} is after end {end}")
        underlying = symbol.strip().upper()
        spelled = schwab_chain_symbol(underlying)
        with translate_auth_errors():
            response = await self._client.get_option_chain(
                spelled,
                contract_type=self._client.Options.ContractType.ALL,
                from_date=start,
                to_date=end,
            )
        chain = normalize_chain(underlying, response_payload(response, spelled))
        if chain.skipped:
            logger.warning("%s: skipped %d unusable contract(s) of the chain", underlying, chain.skipped)
        return chain


def normalize_chain(underlying: str, payload: Mapping[str, Any]) -> OptionChain:
    """Schwab's chain JSON as contract rows keyed under `underlying`.

    A payload without Schwab's `status` is not a chain envelope and raises; one with empty
    expiry maps is an empty chain.
    """
    if not isinstance(payload.get("status"), str):
        raise SchwabResponseError(f"{underlying}: response is not an option chain")
    price = _number(payload.get("underlyingPrice"))
    underlying_price = price if price is not None and price > 0 else None

    contracts: list[OptionContract] = []
    seen: set[str] = set()
    skipped = 0
    for side in ("callExpDateMap", "putExpDateMap"):
        expiries = payload.get(side) or {}
        if not isinstance(expiries, Mapping):
            raise SchwabResponseError(f"{underlying}: {side} is not a mapping")
        for expiry_key, strikes in expiries.items():
            for raw in _contracts_of(strikes):
                contract = _contract(underlying, expiry_key, raw, underlying_price)
                if contract is None or contract.option_symbol in seen:
                    skipped += 1
                    continue
                seen.add(contract.option_symbol)
                contracts.append(contract)
    contracts.sort(key=lambda c: (c.expiry, c.strike, c.put_call))
    return OptionChain(
        underlying=underlying,
        underlying_price=underlying_price,
        contracts=tuple(contracts),
        skipped=skipped,
        truncated=payload.get("isChainTruncated") is True,
    )


def _contracts_of(strikes: Any) -> list[Any]:
    if not isinstance(strikes, Mapping):
        return [None]
    return [raw for group in strikes.values() for raw in (group if isinstance(group, list) else [None])]


def _contract(underlying: str, expiry_key: str, raw: Any, underlying_price: float | None) -> OptionContract | None:
    if not isinstance(raw, Mapping):
        return None
    try:
        expiry = date.fromisoformat(str(expiry_key).split(":")[0])
        return OptionContract(
            underlying=underlying,
            option_symbol=raw["symbol"],
            expiry=expiry,
            strike=raw["strikePrice"],
            put_call=_PUT_CALL[raw["putCall"]],
            bid=raw["bid"],
            ask=raw["ask"],
            last=raw["last"],
            mark=raw["mark"],
            bid_size=raw["bidSize"],
            ask_size=raw["askSize"],
            last_size=raw["lastSize"],
            volume=raw["totalVolume"],
            open_interest=raw["openInterest"],
            quote_time=_epoch_time(raw.get("quoteTimeInLong")),
            trade_time=_epoch_time(raw.get("tradeTimeInLong")),
            iv=_measure(raw.get("volatility")),
            delta=_measure(raw.get("delta")),
            gamma=_measure(raw.get("gamma")),
            theta=_measure(raw.get("theta")),
            vega=_measure(raw.get("vega")),
            rho=_measure(raw.get("rho")),
            underlying_price=underlying_price,
            multiplier=_multiplier(raw.get("multiplier")),
            expiration_type=raw.get("expirationType"),
            non_standard=raw.get("nonStandard"),
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        return None


def _number(value: Any) -> float | None:
    """A finite number, or None for anything else (a string such as "NaN", a bool, a null)."""
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        return None
    return float(value)


def _measure(value: Any) -> float | None:
    number = _number(value)
    return None if number is None or number == _NO_VALUE else number


def _multiplier(value: Any) -> int | None:
    number = _number(value)
    return None if number is None or number <= 0 else int(number)


def _epoch_time(value: Any) -> datetime | None:
    """Epoch milliseconds; a non-positive time (never quoted, never traded) is absence, not 1970."""
    number = _number(value)
    if number is None or number <= 0:
        return None
    try:
        return datetime.fromtimestamp(number / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None
