"""TokenNoticeRepository: which expiry DMs were already sent for a token issue."""

from datetime import UTC, datetime

from rs_persistence import TokenNoticeRepository

ISSUED = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)


async def test_records_per_issue_and_threshold(session_factory):
    async with session_factory() as s:
        repo = TokenNoticeRepository(s)
        assert await repo.sent_thresholds(ISSUED) == set()
        await repo.record(ISSUED, 24)
        await repo.record(ISSUED, 24)  # repeating is a no-op
        await repo.record(ISSUED, 6)
        await s.commit()

    async with session_factory() as s:
        repo = TokenNoticeRepository(s)
        assert await repo.sent_thresholds(ISSUED) == {24, 6}
        assert await repo.sent_thresholds(datetime(2026, 10, 21, tzinfo=UTC)) == set()
