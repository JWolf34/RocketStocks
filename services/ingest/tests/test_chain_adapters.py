"""S008 step-4 adapters: the schwab-py fetch wrapper (bounded params, non-OK
raises) and the session-per-snapshot store (commits are real — visible from a
fresh session; DB-gated on the 5499 Timescale)."""

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from rs_domain import ChainSnapshot, Dataset, OptionQuote, SourceStatus
from rs_ingest.chain_adapters import ChainFetchError, SchwabChainFetch, SessionSnapshotStore
from rs_persistence import build_engine, build_session_factory, chain_snapshot_table

NOW = datetime(2026, 7, 16, 14, 35, tzinfo=timezone.utc)


class FixedClock:
    def now(self):
        return NOW


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {"symbol": "SPY"}

    def json(self):
        return self._payload


class FakeSchwabClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def get_option_chain(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        return self.response


class TestSchwabChainFetch:
    async def test_bounded_params_from_injected_clock(self):
        client = FakeSchwabClient(FakeResponse(payload={"symbol": "SPY", "status": "SUCCESS"}))
        fetch = SchwabChainFetch(client, FixedClock())
        body = await fetch("SPY")
        assert body == {"symbol": "SPY", "status": "SUCCESS"}
        ((symbol, kwargs),) = client.calls
        assert symbol == "SPY"
        # Exactly the two bounding kwargs (schwab-py 1.5.1 names) — everything
        # else stays server-default; window/cap are client-side (ADR-0032 D7).
        assert kwargs == {
            "from_date": date(2026, 7, 16),
            "to_date": date(2026, 7, 16) + timedelta(days=30),
        }

    async def test_non_ok_status_raises_for_the_retry_path(self):
        fetch = SchwabChainFetch(FakeSchwabClient(FakeResponse(status_code=502)), FixedClock())
        with pytest.raises(ChainFetchError, match="SPY: HTTP 502"):
            await fetch("SPY")


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
