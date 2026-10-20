"""schwab_token_issued_at: the token's issue time without reading the token."""

from datetime import UTC, datetime

from sqlalchemy import text

from rs_persistence import SchwabTokenStore
from rs_query import schwab_token_issued_at


async def test_none_before_the_first_login(session_factory):
    async with session_factory() as s:
        await s.execute(text("DELETE FROM schwab_tokens"))
        await s.commit()
        assert await schwab_token_issued_at(s) is None


async def test_issue_time_is_the_creation_timestamp_not_the_last_refresh(session_factory):
    store = SchwabTokenStore(session_factory)
    issued = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
    await store.save({"creation_timestamp": int(issued.timestamp()), "token": {"a": 1}})
    await store.save({"creation_timestamp": int(issued.timestamp()), "token": {"a": 2}})  # a refresh

    async with session_factory() as s:
        assert await schwab_token_issued_at(s) == issued


async def test_none_when_the_token_has_no_creation_timestamp(session_factory):
    await SchwabTokenStore(session_factory).save({"token": {"a": 1}})
    async with session_factory() as s:
        assert await schwab_token_issued_at(s) is None
