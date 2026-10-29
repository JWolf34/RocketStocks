"""Reading Schwab's wire shapes: option chains (`callExpDateMap` / `putExpDateMap`) and daily candles."""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from rs_domain import EASTERN

CHAIN_MAPS = ("callExpDateMap", "putExpDateMap")
QUOTE_FIELDS = ("bid", "ask", "mark", "last", "volatility", "quoteTimeInLong")
# What the summary compares of each contract; nothing else is kept in memory.
COMPARED_FIELDS = ("totalVolume", "openInterest", *QUOTE_FIELDS)


def iter_contracts(payload: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Every contract dict of a chain, calls then puts; malformed levels are skipped."""
    for name in CHAIN_MAPS:
        expiries = payload.get(name)
        if not isinstance(expiries, dict):
            continue
        for strikes in expiries.values():
            if not isinstance(strikes, dict):
                continue
            for contracts in strikes.values():
                if not isinstance(contracts, list):
                    continue
                for contract in contracts:
                    if isinstance(contract, dict):
                        yield contract


def expiry_count(payload: dict[str, Any]) -> int:
    """Distinct expiry dates in a chain (its map keys are `YYYY-MM-DD:days`)."""
    return len(
        {
            str(key).split(":", 1)[0]
            for name in CHAIN_MAPS
            if isinstance(expiries := payload.get(name), dict)
            for key in expiries
        }
    )


def contracts_by_symbol(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The chain's contracts keyed by their OCC symbol."""
    return {c["symbol"]: c for c in iter_contracts(payload) if isinstance(c.get("symbol"), str)}


@dataclass(frozen=True)
class Snapshot:
    """One chain at one moment, whole or assembled from the responses of its expiry ranges."""

    contracts: dict[str, dict[str, Any]]  # OCC symbol -> the compared fields
    underlying: Any
    responses: int

    @classmethod
    def of(cls, *payloads: dict[str, Any]) -> "Snapshot":
        contracts: dict[str, dict[str, Any]] = {}
        for payload in payloads:
            for symbol, contract in contracts_by_symbol(payload).items():
                contracts[symbol] = {name: contract.get(name) for name in COMPARED_FIELDS}
        return cls(contracts, payloads[0].get("underlyingPrice") if payloads else None, len(payloads))


def parse_json_object(content: bytes) -> dict[str, Any] | None:
    try:
        payload = json.loads(content)
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else None


def candle_span(payload: dict[str, Any]) -> tuple[int, date | None, date | None, int]:
    """(candles, earliest ET date, latest ET date, epoch-zero candles) of a daily history.

    Epoch-zero candles are counted apart so they cannot pose as the earliest bar.
    """
    candles = payload.get("candles")
    if not isinstance(candles, list):
        return 0, None, None, 0
    stamps = []
    zeros = 0
    for candle in candles:
        millis = candle.get("datetime") if isinstance(candle, dict) else None
        if isinstance(millis, bool) or not isinstance(millis, int | float):
            continue
        if millis <= 0:
            zeros += 1
            continue
        stamps.append(millis)
    if not stamps:
        return len(candles), None, None, zeros

    def session(millis: float) -> date:
        return datetime.fromtimestamp(millis / 1000, EASTERN).date()

    return len(candles), session(min(stamps)), session(max(stamps)), zeros
