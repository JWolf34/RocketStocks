from datetime import UTC, datetime, timedelta

import pytest

from rs_domain import Alert
from rs_persistence import AlertRepository
from rs_query import AlertRecord, InvalidCursor, recent_alerts
from rs_query.alerts import MAX_LIMIT

T0 = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)


def _alert(symbol: str, i: int) -> Alert:
    return Alert(
        symbol=symbol,
        alert_type="attention_surge",
        triggered_at=T0 + timedelta(minutes=30 * i),
        payload={"fired": ["mention_surge"], "i": i},
        rule_version="1",
        alert_price=12.5,
        quote_time=T0 + timedelta(minutes=30 * i, seconds=3),
        dedup_key=f"attention_surge:{symbol}:{i}",
    )


async def _seed(session_factory, alerts):
    async with session_factory() as s:
        repo = AlertRepository(s)
        for alert in alerts:
            await repo.insert(alert)
        await s.commit()


async def _all(session_factory, ticker=None, limit=50):
    seen: list[AlertRecord] = []
    cursor = None
    async with session_factory() as s:
        while True:
            page = await recent_alerts(s, ticker, limit=limit, cursor=cursor)
            seen.extend(page.items)
            cursor = page.next_cursor
            if cursor is None:
                return seen


async def test_newest_first_with_log_identity(session_factory):
    alerts = [_alert("AAA", i) for i in range(3)]
    await _seed(session_factory, alerts)

    async with session_factory() as s:
        page = await recent_alerts(s)

    assert [r.alert for r in page.items] == alerts[::-1]
    assert [r.id for r in page.items] == sorted((r.id for r in page.items), reverse=True)
    assert all(r.posted_at is None for r in page.items)
    assert page.next_cursor is None


async def test_ticker_filter(session_factory):
    await _seed(session_factory, [_alert("AAA", 0), _alert("BBB", 1), _alert("AAA", 2)])

    got = await _all(session_factory, "AAA")

    assert [r.alert.dedup_key for r in got] == ["attention_surge:AAA:2", "attention_surge:AAA:0"]
    assert await _all(session_factory, "ZZZ") == []


async def test_pagination_round_trip_visits_every_row_once(session_factory):
    alerts = [_alert("AAA", i) for i in range(7)]
    await _seed(session_factory, alerts)

    got = await _all(session_factory, limit=3)

    assert [r.alert for r in got] == alerts[::-1]
    assert len({r.id for r in got}) == 7


async def test_posted_alerts_carry_their_stamp(session_factory):
    await _seed(session_factory, [_alert("AAA", 0)])
    async with session_factory() as s:
        first = (await recent_alerts(s)).items[0]
        await AlertRepository(s).mark_posted(first.id)
        await s.commit()

    async with session_factory() as s:
        (record,) = (await recent_alerts(s)).items

    assert record.posted_at is not None


async def test_limit_above_the_cap_is_capped(session_factory):
    await _seed(session_factory, [_alert("AAA", i) for i in range(MAX_LIMIT + 2)])

    async with session_factory() as s:
        page = await recent_alerts(s, limit=MAX_LIMIT * 4)

    assert len(page.items) == MAX_LIMIT
    assert page.next_cursor is not None


async def test_rejects_a_bad_limit_and_a_bad_cursor(session_factory):
    async with session_factory() as s:
        with pytest.raises(ValueError, match="limit"):
            await recent_alerts(s, limit=0)
        with pytest.raises(InvalidCursor):
            await recent_alerts(s, cursor="garbage")
