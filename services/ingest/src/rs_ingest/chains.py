"""Chain-response normalization (S008): Schwab option-chain JSON -> in-window,
capped OptionQuote rows, ready to become a ChainSnapshot's payload.

The request is bounded server-side by expiry only (fromDate/toDate, <=30 DTE);
everything client-side happens here, against the response's OWN underlyingPrice
(ADR-0032 D7): the +/-10% moneyness window, then the row cap — whole expiries
nearest-first, nearest expiry always kept. The cap is a pathology guard rail
sized to be non-binding on measured books (spec: Decisions surfaced), not a
routine selector.

Values are stored as received (point-in-time capture): `iv` is Schwab's
per-contract `volatility` in percent, untransformed. The only rewrites are
wire sentinels that mean "no value" — epoch-0 quote/trade times and -999
vol/greeks — which become NULL, the truthful capture of absence.

Pure functions: no I/O, no ambient time (ADR-0023), no logging — the scanner
owns observability and turns the returned counts into metrics/status.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import groupby
from typing import Any, Literal

from rs_domain import OptionQuote

_NO_VALUE = -999.0  # Schwab's documented "no value" sentinel for vol/greeks


@dataclass(frozen=True)
class NormalizedChain:
    """One parsed response. `rows` is deterministic (expiry, strike, side)
    order. `skipped_contracts` counts contracts the mapping rejected;
    `dropped_by_cap` counts in-window rows the cap cut — both are status/metric
    inputs for the scanner, not errors."""

    underlying_price: float | None
    rows: tuple[OptionQuote, ...]
    dropped_by_cap: int
    skipped_contracts: int
    chain_truncated: bool


def normalize_chain(payload: dict[str, Any], *, window: float, row_cap: int) -> NormalizedChain:
    underlying_price = payload.get("underlyingPrice")
    if not isinstance(underlying_price, int | float) or underlying_price <= 0:
        # Nothing to window against: capture everything the expiry-bounded
        # response contains rather than guess a price; the cap still guards.
        underlying_price = None

    rows: list[OptionQuote] = []
    skipped = 0
    for side in ("callExpDateMap", "putExpDateMap"):
        for expiry_key, strikes in (payload.get(side) or {}).items():
            try:
                expiry = date.fromisoformat(expiry_key.split(":")[0])
            except ValueError:
                skipped += sum(len(contracts) for contracts in strikes.values())
                continue
            for contracts in strikes.values():
                for contract in contracts:
                    try:
                        quote = _parse_contract(expiry, contract, underlying_price)
                    except (KeyError, TypeError, ValueError):
                        skipped += 1
                        continue
                    if underlying_price is None or (
                        abs(quote.strike - underlying_price) <= window * underlying_price
                    ):
                        rows.append(quote)

    rows.sort(key=lambda r: (r.expiry, r.strike, r.put_call))
    kept, dropped = _apply_row_cap(rows, row_cap)
    return NormalizedChain(
        underlying_price=underlying_price,
        rows=tuple(kept),
        dropped_by_cap=dropped,
        skipped_contracts=skipped,
        chain_truncated=bool(payload.get("isChainTruncated")),
    )


def _apply_row_cap(rows: list[OptionQuote], row_cap: int) -> tuple[list[OptionQuote], int]:
    """Whole expiries nearest-first while the total stays within the cap; the
    nearest expiry is kept unconditionally (even alone over the cap). Stops at
    the first expiry that does not fit — a nearest-first prefix, so what
    survives is always a contiguous front of the book."""
    if len(rows) <= row_cap:
        return rows, 0
    groups = [list(group) for _, group in groupby(rows, key=lambda r: r.expiry)]
    kept = groups[0]
    for group in groups[1:]:
        if len(kept) + len(group) > row_cap:
            break
        kept += group
    return kept, len(rows) - len(kept)


_PUT_CALL: dict[str, Literal["C", "P"]] = {"CALL": "C", "PUT": "P"}


def _parse_contract(
    expiry: date, contract: dict[str, Any], underlying_price: float | None
) -> OptionQuote:
    multiplier = contract.get("multiplier")
    return OptionQuote(
        option_symbol=contract["symbol"],
        expiry=expiry,
        strike=contract["strikePrice"],
        put_call=_PUT_CALL[contract["putCall"]],
        quote_time=_from_epoch_ms(contract.get("quoteTimeInLong")),
        bid=contract.get("bid"),
        ask=contract.get("ask"),
        last=contract.get("last"),
        mark=contract.get("mark"),
        bid_size=contract.get("bidSize"),
        ask_size=contract.get("askSize"),
        last_size=contract.get("lastSize"),
        trade_time=_from_epoch_ms(contract.get("tradeTimeInLong")),
        volume=contract.get("totalVolume"),
        open_interest=contract.get("openInterest"),
        iv=_greek(contract.get("volatility")),
        delta=_greek(contract.get("delta")),
        gamma=_greek(contract.get("gamma")),
        theta=_greek(contract.get("theta")),
        vega=_greek(contract.get("vega")),
        rho=_greek(contract.get("rho")),
        underlying_price=underlying_price,
        multiplier=int(multiplier) if multiplier is not None else None,
        expiration_type=contract.get("expirationType"),
        non_standard=contract.get("nonStandard"),
    )


def _from_epoch_ms(value: Any) -> datetime | None:
    """Epoch-ms wire timestamps; <=0 (never quoted/traded) is absence, not 1970."""
    if not isinstance(value, int | float) or value <= 0:
        return None
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _greek(value: Any) -> float | None:
    if value == _NO_VALUE:
        return None
    return value
