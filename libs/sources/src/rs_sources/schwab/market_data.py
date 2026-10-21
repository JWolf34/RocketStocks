"""Schwab market-data calls: a quote and daily price history.

Both are validated at the edge, and neither reads the clock (the caller stamps
fetched_at). Schwab floors history bounds to Eastern days and reads naive
datetimes in the process's local zone, so callers pass ET session dates and
the adapter sends tz-aware ET bounds.
"""

import logging
import math
from datetime import UTC, date, datetime, time
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, ValidationError

from rs_domain import TradingCalendar, UtcDatetime
from rs_sources.errors import SourceError
from rs_sources.schwab.client import translate_auth_errors

logger = logging.getLogger(__name__)

_EASTERN = ZoneInfo("America/New_York")


class SchwabResponseError(SourceError):
    """A non-OK or unusable Schwab response; `status_code` is set for the former."""


class Quote(BaseModel):
    """Latest quote; `previous_close` is the prior session's close."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    last_price: float = Field(gt=0, allow_inf_nan=False)
    previous_close: float = Field(gt=0, allow_inf_nan=False)
    quote_time: UtcDatetime
    volume: NonNegativeInt


class DailyBar(BaseModel):
    """One validated daily candle keyed by its Eastern session date."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    session_date: date
    open: float = Field(gt=0, allow_inf_nan=False)
    high: float = Field(gt=0, allow_inf_nan=False)
    low: float = Field(gt=0, allow_inf_nan=False)
    close: float = Field(gt=0, allow_inf_nan=False)
    volume: NonNegativeInt


class RejectReason(StrEnum):
    EPOCH_ZERO = "epoch_zero"
    NOT_A_SESSION = "not_a_session"
    OUTSIDE_WINDOW = "outside_window"
    DUPLICATE_SESSION = "duplicate_session"
    MALFORMED = "malformed"


class RejectedBar(BaseModel):
    """A candle that failed edge validation, kept so the caller can count it."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    reason: RejectReason
    timestamp_ms: int | None = None
    session_date: date | None = None


class DailyHistory(BaseModel):
    """Accepted bars in session order plus every reject.

    Schwab answers an unknown symbol and a non-session window alike with an
    empty history; callers tell "unpriceable" from the empty `bars` and
    `rejects`.
    """

    model_config = ConfigDict(frozen=True)

    bars: tuple[DailyBar, ...] = ()
    rejects: tuple[RejectedBar, ...] = ()


def _utc_from_millis(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


class SchwabMarketData:
    """Quote and daily history over the daemon's ONE schwab-py async client."""

    def __init__(self, client: Any, calendar: TradingCalendar) -> None:
        self._client = client
        self._calendar = calendar

    async def quote(self, symbol: str) -> Quote | None:
        """Latest quote, or None when Schwab does not know the symbol."""
        symbol = symbol.strip().upper()
        with translate_auth_errors():
            response = await self._client.get_quotes([symbol])
        payload = self._json(response, symbol)
        entry = payload.get(symbol)
        if not isinstance(entry, dict) or not isinstance(entry.get("quote"), dict):
            return None
        raw = entry["quote"]
        try:
            if raw["quoteTime"] <= 0:
                raise ValueError("quoteTime is not a positive epoch")
            return Quote(
                symbol=symbol,
                last_price=raw["lastPrice"],
                previous_close=raw["closePrice"],
                quote_time=_utc_from_millis(raw["quoteTime"]),
                volume=raw["totalVolume"],
            )
        except (KeyError, TypeError, ValueError, OverflowError, OSError, ValidationError) as exc:
            raise SchwabResponseError(f"{symbol}: unusable quote ({exc})") from exc

    async def daily_history(self, symbol: str, start: date, end: date) -> DailyHistory:
        """Daily OHLCV for ET session dates `start` to `end`, inclusive."""
        if start > end:
            raise ValueError(f"start {start} is after end {end}")
        symbol = symbol.strip().upper()
        # A full-day window also keeps start == end off Schwab's zero-width 400.
        window_start = datetime.combine(start, time.min, tzinfo=_EASTERN)
        window_end = datetime.combine(end, time(23, 59, 59), tzinfo=_EASTERN)
        with translate_auth_errors():
            response = await self._client.get_price_history_every_day(
                symbol,
                start_datetime=window_start,
                end_datetime=window_end,
                need_extended_hours_data=False,
            )
        payload = self._json(response, symbol)
        bars: list[DailyBar] = []
        rejects: list[RejectedBar] = []
        seen: set[date] = set()
        for candle in payload.get("candles") or []:
            outcome = self._validate_candle(symbol, candle, start, end, seen)
            if isinstance(outcome, DailyBar):
                seen.add(outcome.session_date)
                bars.append(outcome)
            else:
                rejects.append(outcome)
        if rejects:
            logger.warning(
                "%s: rejected %d daily bar(s): %s", symbol, len(rejects), sorted({r.reason for r in rejects})
            )
        bars.sort(key=lambda bar: bar.session_date)
        return DailyHistory(bars=tuple(bars), rejects=tuple(rejects))

    def _validate_candle(
        self, symbol: str, candle: Any, start: date, end: date, seen: set[date]
    ) -> DailyBar | RejectedBar:
        millis = candle.get("datetime") if isinstance(candle, dict) else None
        if isinstance(millis, bool) or not isinstance(millis, int | float) or not math.isfinite(millis):
            return RejectedBar(symbol=symbol, reason=RejectReason.MALFORMED)
        millis = int(millis)
        if millis == 0:
            return RejectedBar(symbol=symbol, reason=RejectReason.EPOCH_ZERO, timestamp_ms=0)
        try:
            session_date = _utc_from_millis(millis).astimezone(_EASTERN).date()
        except (OverflowError, OSError, ValueError):
            return RejectedBar(symbol=symbol, reason=RejectReason.MALFORMED, timestamp_ms=millis)

        def reject(reason: RejectReason) -> RejectedBar:
            return RejectedBar(symbol=symbol, reason=reason, timestamp_ms=millis, session_date=session_date)

        try:
            is_session = self._calendar.is_session(session_date)
        except ValueError:
            # Outside the calendar's supported range (e.g. 1969-12-31): not a session.
            is_session = False
        if not is_session:
            return reject(RejectReason.NOT_A_SESSION)
        if not start <= session_date <= end:
            return reject(RejectReason.OUTSIDE_WINDOW)
        if session_date in seen:
            return reject(RejectReason.DUPLICATE_SESSION)
        try:
            return DailyBar(
                symbol=symbol,
                session_date=session_date,
                open=candle["open"],
                high=candle["high"],
                low=candle["low"],
                close=candle["close"],
                volume=candle["volume"],
            )
        except (KeyError, ValidationError):
            return reject(RejectReason.MALFORMED)

    @staticmethod
    def _json(response: Any, symbol: str) -> dict[str, Any]:
        if response.status_code != 200:
            raise SchwabResponseError(f"{symbol}: HTTP {response.status_code}", response.status_code)
        try:
            payload = response.json()
        except ValueError as exc:
            raise SchwabResponseError(f"{symbol}: response is not JSON") from exc
        if not isinstance(payload, dict):
            raise SchwabResponseError(f"{symbol}: unexpected response shape")
        return payload
