"""GET /v1/bars/{ticker} range semantics + envelope (S005 acceptance).

Expected values are independent literals (the seeded fixture bars), never
recomputed through the code under test.
"""

from datetime import datetime, timedelta, timezone

import pytest

from rs_domain import Bar
from rs_persistence import BarRepository

API_KEY = "test-key-not-secret"
HEADERS = {"X-API-Key": API_KEY}

OPEN_0930 = datetime(2026, 7, 9, 13, 30, tzinfo=timezone.utc)  # 09:30 ET open


@pytest.fixture
async def seeded(session_factory):
    async with session_factory() as s:
        repo = BarRepository(s)
        for i, volume in enumerate((100_000, 250_000, 900_000)):
            await repo.insert(
                Bar(
                    symbol="SPY",
                    bucket=OPEN_0930 + timedelta(minutes=5 * i),
                    open=620.0 + i,
                    high=621.0 + i,
                    low=619.0 + i,
                    close=620.5 + i,
                    volume=volume,
                )
            )
        await s.commit()


async def test_happy_path_envelope_oldest_first(client, seeded):
    response = await client.get(
        "/v1/bars/SPY",
        params={"start": "2026-07-09T13:30:00Z", "end": "2026-07-09T20:00:00Z"},
        headers=HEADERS,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == [
        {
            "symbol": "SPY",
            "bucket": "2026-07-09T13:30:00Z",
            "open": 620.0,
            "high": 621.0,
            "low": 619.0,
            "close": 620.5,
            "volume": 100000,
        },
        {
            "symbol": "SPY",
            "bucket": "2026-07-09T13:35:00Z",
            "open": 621.0,
            "high": 622.0,
            "low": 620.0,
            "close": 621.5,
            "volume": 250000,
        },
        {
            "symbol": "SPY",
            "bucket": "2026-07-09T13:40:00Z",
            "open": 622.0,
            "high": 623.0,
            "low": 621.0,
            "close": 622.5,
            "volume": 900000,
        },
    ]
    # as_of = newest bar's bucket CLOSE (13:40 + 5m); truncated intentionally
    # absent (reserved for the Phase-3 cap — absent means false).
    assert body["meta"] == {"as_of": "2026-07-09T13:45:00Z", "interval": "5m", "source": "stream"}


async def test_range_is_half_open(client, seeded):
    response = await client.get(
        "/v1/bars/SPY",
        params={"start": "2026-07-09T13:35:00Z", "end": "2026-07-09T13:40:00Z"},
        headers=HEADERS,
    )
    assert [bar["bucket"] for bar in response.json()["data"]] == ["2026-07-09T13:35:00Z"]


async def test_ticker_is_case_insensitive(client, seeded):
    response = await client.get(
        "/v1/bars/spy",
        params={"start": "2026-07-09T13:30:00Z", "end": "2026-07-09T20:00:00Z"},
        headers=HEADERS,
    )
    assert len(response.json()["data"]) == 3


async def test_empty_range_is_200_with_empty_data(client, seeded):
    response = await client.get(
        "/v1/bars/SPY",
        params={"start": "2026-08-01T00:00:00Z", "end": "2026-08-02T00:00:00Z"},
        headers=HEADERS,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == []
    assert body["meta"]["as_of"] is None


@pytest.mark.parametrize(
    "params",
    [
        {"end": "2026-07-09T20:00:00Z"},  # missing start
        {"start": "2026-07-09T13:30:00Z"},  # missing end
        {"start": "not-a-date", "end": "2026-07-09T20:00:00Z"},
        {"start": "2026-07-09T13:30:00", "end": "2026-07-09T20:00:00Z"},  # naive
        {"start": "2026-07-09T20:00:00Z", "end": "2026-07-09T13:30:00Z"},  # start > end
        {"start": "2026-07-09T13:30:00Z", "end": "2026-07-09T13:30:00Z"},  # start == end
    ],
)
async def test_invalid_ranges_are_422(client, seeded, params):
    response = await client.get("/v1/bars/SPY", params=params, headers=HEADERS)
    assert response.status_code == 422
