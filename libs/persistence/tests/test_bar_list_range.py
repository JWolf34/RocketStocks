"""BarRepository.list_range: the serve read seam (S005) — half-open [start, end),
oldest-first, symbol-scoped."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from rs_domain import Bar
from rs_persistence import BarRepository, build_engine, build_session_factory

T0 = datetime(2026, 7, 9, 13, 30, tzinfo=UTC)  # RTH open, 2026-07-09


def _bar(symbol: str, minutes_after_open: int) -> Bar:
    return Bar(
        symbol=symbol,
        bucket=T0 + timedelta(minutes=minutes_after_open),
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=100 + minutes_after_open,
    )


@pytest.fixture
async def session_factory(migrated_db_url):
    engine = build_engine(migrated_db_url)
    factory = build_session_factory(engine)
    async with factory() as s:
        await s.execute(text("TRUNCATE stream_bars_5m"))
        repo = BarRepository(s)
        for minutes in (0, 5, 10, 15):
            await repo.insert(_bar("SPY", minutes))
        await repo.insert(_bar("QQQ", 5))
        await s.commit()
    yield factory
    await engine.dispose()


async def test_range_is_half_open_and_oldest_first(session_factory):
    async with session_factory() as s:
        bars = await BarRepository(s).list_range(
            "SPY", start=T0 + timedelta(minutes=5), end=T0 + timedelta(minutes=15)
        )
    # [start, end): the 13:35 and 13:40 buckets; 13:45 excluded.
    assert [b.bucket for b in bars] == [
        T0 + timedelta(minutes=5),
        T0 + timedelta(minutes=10),
    ]


async def test_range_scopes_to_symbol(session_factory):
    async with session_factory() as s:
        bars = await BarRepository(s).list_range("QQQ", start=T0, end=T0 + timedelta(hours=1))
    assert [(b.symbol, b.bucket) for b in bars] == [("QQQ", T0 + timedelta(minutes=5))]


async def test_empty_range_returns_empty_list(session_factory):
    async with session_factory() as s:
        bars = await BarRepository(s).list_range(
            "SPY", start=T0 + timedelta(days=30), end=T0 + timedelta(days=31)
        )
    assert bars == []
