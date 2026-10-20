"""S008 step-4 adapter: the session-per-snapshot store (commits are real -
visible from a fresh session; DB-gated on the 5499 Timescale)."""

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import func, select

from rs_domain import ChainSnapshot, Dataset, OptionQuote, SourceStatus
from rs_ingest.chain_adapters import SessionSnapshotStore
from rs_persistence import build_engine, build_session_factory, chain_snapshot_table

NOW = datetime(2026, 7, 16, 14, 35, tzinfo=UTC)


class TestSessionSnapshotStore:
    async def test_insert_commits_and_restart_state_reads_back(self, migrated_db_url):
        engine = build_engine(migrated_db_url)
        try:
            factory = build_session_factory(engine)
            store = SessionSnapshotStore(factory)
            snapshot = ChainSnapshot(
                underlying="ADPT",
                dataset=Dataset.CURATED_5M,
                cycle_id=NOW,
                fetched_at=NOW + timedelta(seconds=5),
                source_status=SourceStatus.OK,
                rows=(
                    OptionQuote(
                        option_symbol="ADPT  260731C00100000",
                        expiry=date(2026, 7, 31),
                        strike=100.0,
                        put_call="C",
                        quote_time=NOW + timedelta(seconds=3),
                    ),
                ),
            )
            assert await store.insert(snapshot) == 1

            # Committed: a brand-new session sees the row without any shared state.
            async with factory() as session:
                count = await session.scalar(
                    select(func.count())
                    .select_from(chain_snapshot_table)
                    .where(chain_snapshot_table.c.underlying == "ADPT")
                )
            assert count == 1

            medians = await store.last_accepted_median_quote_times(
                Dataset.CURATED_5M, NOW + timedelta(minutes=10)
            )
            assert medians["ADPT"] == NOW + timedelta(seconds=3)
        finally:
            await engine.dispose()
