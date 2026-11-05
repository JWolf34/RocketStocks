"""Virtual time, a scripted Schwab client and hand-written chain payloads for the probe's tests."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from rs_ingest.rate_limit import RateLimiter
from rs_ingest.tools.sweep_probe.runner import ProbeRunner
from rs_ingest.tools.sweep_probe.store import Plan, ProbeDir
from rs_sources.schwab import TokenMissingError

# A Wednesday, 16:05 ET (EDT) after the close.
FIRST_SAMPLE = datetime(2026, 10, 28, 20, 5, tzinfo=UTC)


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def timestamp(self) -> float:
        return self._now.timestamp()

    async def sleep(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


class Response:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


class Wire:
    """Schwab's wire shapes, written by hand."""

    @staticmethod
    def contract(
        symbol: str,
        *,
        volume: int = 0,
        oi: int = 0,
        bid: float = 1.0,
        ask: float = 1.1,
        mark: float = 1.05,
        last: float = 1.0,
        iv: float = 30.0,
        quote_time: int = 1_000,
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "totalVolume": volume,
            "openInterest": oi,
            "bid": bid,
            "ask": ask,
            "mark": mark,
            "last": last,
            "volatility": iv,
            "quoteTimeInLong": quote_time,
        }

    @staticmethod
    def chain(
        contracts: list[dict[str, Any]], *, underlying: float = 100.0, truncated: bool | None = None
    ) -> dict[str, Any]:
        calls: dict[str, dict[str, list[dict[str, Any]]]] = {"2026-11-20:23": {}}
        for index, contract in enumerate(contracts):
            calls["2026-11-20:23"][f"{100 + index}.0"] = [contract]
        payload: dict[str, Any] = {
            "status": "SUCCESS",
            "underlyingPrice": underlying,
            "callExpDateMap": calls,
            "putExpDateMap": {},
        }
        if truncated is not None:
            payload["isChainTruncated"] = truncated
        return payload

    @classmethod
    def sized_chain(cls, symbol: str, size: int, **kwargs: Any) -> dict[str, Any]:
        return cls.chain([cls.contract(f"{symbol}{n:04d}", oi=n) for n in range(size)], **kwargs)

    @staticmethod
    def ok(payload: dict[str, Any]) -> Response:
        return Response(200, json.dumps(payload).encode())

    @staticmethod
    def status(code: int, text: str = "") -> Response:
        return Response(code, text.encode())

    @staticmethod
    def candles(*stamps_ms: int) -> dict[str, Any]:
        return {
            "empty": not stamps_ms,
            "candles": [
                {"datetime": ms, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10} for ms in stamps_ms
            ],
        }


@dataclass
class Call:
    kind: str  # "chain", "history" or "quote"
    symbol: str
    start: date | None
    end: date | None
    at: datetime


class FakeSchwab:
    """The slice of schwab-py's async client the probe uses; `on_chain` and `on_history` script it."""

    class Options:
        class ContractType:
            ALL = "ALL"

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self.calls: list[Call] = []
        self.on_chain: Callable[[str, date | None, date | None], Response] = lambda symbol, start, end: Wire.ok(
            Wire.sized_chain(symbol, 25)
        )
        self.on_history: Callable[[str], Response] = lambda symbol: Wire.ok(
            Wire.candles(631_152_000_000, 1_700_000_000_000)
        )
        self.on_quote: Callable[[str], Response] = lambda symbol: Wire.ok({symbol: {}})
        # Raised by the Nth call (1-based) to stand in for the process dying.
        self.die_on_call: int | None = None

    async def get_option_chain(
        self, symbol: str, *, contract_type: str, from_date: date | None = None, to_date: date | None = None
    ) -> Response:
        assert contract_type == "ALL"
        self._record(Call("chain", symbol, from_date, to_date, self._clock.now()))
        return self.on_chain(symbol, from_date, to_date)

    async def get_quote(self, symbol: str) -> Response:
        self._record(Call("quote", symbol, None, None, self._clock.now()))
        return self.on_quote(symbol)

    async def get_price_history_every_day(
        self, symbol: str, *, start_datetime: datetime, end_datetime: datetime, need_extended_hours_data: bool
    ) -> Response:
        assert start_datetime == datetime(1990, 1, 1, tzinfo=start_datetime.tzinfo) and start_datetime.tzinfo
        assert need_extended_hours_data is False
        self._record(Call("history", symbol, None, None, self._clock.now()))
        return self.on_history(symbol)

    def _record(self, call: Call) -> None:
        self.calls.append(call)
        if self.die_on_call == len(self.calls):
            raise ProcessDied


class ProcessDied(BaseException):
    """Not an Exception, so nothing in the probe can swallow it: the process is gone."""


class Harness:
    first = FIRST_SAMPLE
    ProcessDied = ProcessDied

    def __init__(self, tmp_path: Path, start: datetime) -> None:
        self.clock = FakeClock(start)
        self.schwab = FakeSchwab(self.clock)
        self.directory = tmp_path / "probe"
        self.builds = 0
        self.client_error: Exception | None = None
        # No usable token between these instants.
        self.token_missing_between: tuple[datetime, datetime] | None = None

    @property
    def out(self) -> ProbeDir:
        return ProbeDir(self.directory)

    async def _make_client(self) -> FakeSchwab:
        if self.client_error is not None:
            raise self.client_error
        if self.token_missing_between and self.token_missing_between[0] <= self.clock.now() < self.token_missing_between[1]:
            raise TokenMissingError("no Schwab token in the database")
        self.builds += 1
        return self.schwab

    def runner(self, plan: Plan, *, calls_per_minute: int = 20) -> ProbeRunner:
        return ProbeRunner(
            plan=plan,
            out=self.out,
            make_client=self._make_client,
            limiter=RateLimiter(calls_per_minute, now=self.clock.timestamp, sleep=self.clock.sleep),
            clock=self.clock,
            sleep=self.clock.sleep,
            timer=self.clock.timestamp,
        )

    @staticmethod
    def plan(
        *,
        samples: tuple[datetime, ...] | None = None,
        named: tuple[str, ...] = (),
        spelling: tuple[str, ...] = (),
        listings: tuple[str, ...] = (),
        bars: tuple[str, ...] = (),
        fixed: tuple[str, ...] = ("AAPL",),
        small: tuple[str, ...] | None = ("SMALL",),
    ) -> Plan:
        first = FIRST_SAMPLE
        return Plan(
            seed=1,
            session_day=date(2026, 10, 28),
            samples=samples or (first, first + timedelta(hours=2), first + timedelta(hours=3)),
            named_chains=named,
            spelling_chains=spelling,
            sample_listings=listings,
            bar_symbols=bars,
            fixed_tickers=fixed,
            small_tickers=small,
        )


@pytest.fixture
def wire() -> type[Wire]:
    return Wire


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    """A clock one hour before the first sample."""
    return Harness(tmp_path, FIRST_SAMPLE - timedelta(hours=1))
