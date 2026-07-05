"""Tests for rocketstocks.data.seed.seed_boilerplate_watchlists."""
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from rocketstocks.data.seed import _BOILERPLATE_WATCHLISTS, _SEED_KEY, seed_boilerplate_watchlists
from rocketstocks.data.watchlists import Watchlists


def _make_watchlists(sentinel_exists: bool) -> Watchlists:
    db = MagicMock()
    db.execute = AsyncMock(return_value=None)
    wl = Watchlists(db)
    wl.validate_watchlist = AsyncMock(return_value=sentinel_exists)
    wl.create_watchlist = AsyncMock(return_value=None)
    return wl


class TestSeedBoilerplateWatchlists:
    async def test_first_run_creates_all_watchlists_and_sentinel(self):
        wl = _make_watchlists(sentinel_exists=False)
        await seed_boilerplate_watchlists(wl)

        wl.validate_watchlist.assert_awaited_once_with(_SEED_KEY)

        # one call per boilerplate watchlist + one for the sentinel
        assert wl.create_watchlist.await_count == len(_BOILERPLATE_WATCHLISTS) + 1

        # each boilerplate watchlist is created with watchlist_type='named'
        for name, tickers in _BOILERPLATE_WATCHLISTS.items():
            wl.create_watchlist.assert_any_await(name, tickers, watchlist_type='named')

        # sentinel is created with watchlist_type='system'
        wl.create_watchlist.assert_any_await(_SEED_KEY, [], watchlist_type='system')

    async def test_subsequent_run_skips_seeding(self):
        wl = _make_watchlists(sentinel_exists=True)
        await seed_boilerplate_watchlists(wl)

        wl.validate_watchlist.assert_awaited_once_with(_SEED_KEY)
        wl.create_watchlist.assert_not_awaited()

    async def test_partial_state_still_calls_create_for_each_watchlist(self):
        """Sentinel absent but some watchlists exist — create_watchlist is called for
        all entries (safe because ON CONFLICT DO NOTHING makes it idempotent)."""
        wl = _make_watchlists(sentinel_exists=False)
        await seed_boilerplate_watchlists(wl)

        # All 8 boilerplate watchlists + sentinel must be attempted
        assert wl.create_watchlist.await_count == len(_BOILERPLATE_WATCHLISTS) + 1
