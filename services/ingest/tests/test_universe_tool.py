"""The operator command that takes the Universe snapshot with the size guard off."""

from datetime import UTC, date, datetime

import pytest

from rs_ingest.tools import universe_snapshot
from rs_ingest.universe import UniverseUse
from rs_persistence import UniverseRepository
from rs_sources.testing import (
    NASDAQ_LISTED_HEADER,
    OTHER_LISTED_HEADER,
    FakeDirectoryHost,
    cboe_file,
    listing_file,
    nasdaq_row,
    options_file,
    other_row,
)

# Monday 2026-10-26 21:30 ET: after the evening's give-up, so the command makes one attempt.
NOW = datetime(2026, 10, 27, 1, 30, tzinfo=UTC)
STAMP = datetime(2026, 10, 26, 22, 1, tzinfo=UTC)
MONDAY = date(2026, 10, 26)
FRIDAY = date(2026, 10, 23)


class _Clock:
    def now(self) -> datetime:
        return NOW


def _small_files() -> FakeDirectoryHost:
    host = FakeDirectoryHost()
    host.bodies["nasdaqlisted.txt"] = listing_file(NASDAQ_LISTED_HEADER, [nasdaq_row("AAA")], STAMP)
    host.bodies["otherlisted.txt"] = listing_file(OTHER_LISTED_HEADER, [other_row("BBB")], STAMP)
    host.bodies["options.txt"] = options_file(["AAA"], STAMP)
    host.bodies["cboe"] = cboe_file(["BBB"])
    return host


async def test_the_command_stores_a_snapshot_that_is_far_smaller_than_the_previous_one(session_factory, seed_previous):
    await seed_previous(nasdaq_listed=100, other_listed=100, options_symbols=100, cboe_symbols=100)

    use = await universe_snapshot.take_snapshot(session_factory, _small_files().client(), _Clock())

    assert (use.snapshot_date, use.fresh) == (MONDAY, True)
    async with session_factory() as s:
        latest = await UniverseRepository(s).latest()
    assert latest is not None and {x.symbol: x.optionable for x in latest.listings} == {"AAA": True, "BBB": True}


async def test_the_command_still_refuses_a_stale_file(session_factory, seed_previous):
    await seed_previous(nasdaq_listed=100, other_listed=100, options_symbols=100, cboe_symbols=100)
    host = _small_files()
    host.bodies["nasdaqlisted.txt"] = listing_file(
        NASDAQ_LISTED_HEADER, [nasdaq_row("AAA")], datetime(2026, 10, 26, 16, 0, tzinfo=UTC)
    )

    use = await universe_snapshot.take_snapshot(session_factory, host.client(), _Clock())

    assert (use.snapshot_date, use.fresh) == (FRIDAY, False)
    assert use.night_uses == "the night uses the snapshot of 2026-10-23"


def test_the_night_without_any_snapshot_says_so_plainly():
    use = UniverseUse(MONDAY, None, None, fresh=False)

    assert use.night_uses == "no Universe snapshot exists yet"


def test_the_flag_is_required_so_the_command_is_never_run_by_accident(monkeypatch):
    monkeypatch.setattr("sys.argv", ["universe_snapshot"])

    with pytest.raises(SystemExit):
        universe_snapshot.main()
