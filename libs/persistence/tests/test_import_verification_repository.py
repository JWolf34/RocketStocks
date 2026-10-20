"""ImportVerificationRepository: append-only; the latest row is the one that counts."""

from datetime import UTC, datetime, timedelta

from rs_persistence import ImportVerification, ImportVerificationRepository

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def _verification(zone: str, verified_at: datetime = T0) -> ImportVerification:
    return ImportVerification(
        verified_at=verified_at,
        zone=zone,
        overlap_rows=2000,
        matched_rows=1990,
        match_rate=0.995,
        overlap_start=T0 - timedelta(days=2),
        overlap_end=T0,
    )


async def test_latest_is_none_before_any_verification(session_factory):
    async with session_factory() as s:
        assert await ImportVerificationRepository(s).latest() is None


async def test_latest_returns_the_most_recently_recorded_row(session_factory):
    first = _verification("America/Chicago")
    second = _verification("UTC", T0 + timedelta(days=1))
    async with session_factory() as s:
        repo = ImportVerificationRepository(s)
        await repo.record(first)
        assert await repo.latest() == first
        await repo.record(second)
        await s.commit()
    async with session_factory() as s:
        assert await ImportVerificationRepository(s).latest() == second
