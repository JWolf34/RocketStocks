"""Tests for rocketstocks.bot.cogs.watchlists."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.watchlists import Watchlists


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_interaction(user_id: int = 99) -> MagicMock:
    interaction = MagicMock(name="Interaction")
    interaction.response = MagicMock()
    interaction.response.defer = AsyncMock()
    interaction.response.send_message = AsyncMock()
    interaction.followup = MagicMock()
    interaction.followup.send = AsyncMock()
    interaction.user = MagicMock()
    interaction.user.id = user_id
    interaction.user.name = "TestUser"
    return interaction


def _make_watchlists_data(validate_result: bool = True, tickers: list | None = None):
    wl = MagicMock()
    wl.validate_watchlist.return_value = validate_result
    wl.get_watchlist_tickers.return_value = tickers or []
    wl.get_watchlists.return_value = ["alpha", "beta", "personal"]
    wl.update_watchlist.return_value = None
    wl.create_watchlist.return_value = None
    wl.delete_watchlist.return_value = None
    wl.rename_watchlist.return_value = True
    return wl


def _make_stock_data(validate: bool = True, existing_tickers: list | None = None):
    sd = MagicMock(name="StockData")
    sd.watchlists = _make_watchlists_data(validate_result=validate, tickers=existing_tickers)
    sd.tickers = MagicMock()
    sd.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL", "MSFT"], []))
    return sd


def _make_cog(validate: bool = True, existing_tickers: list | None = None) -> Watchlists:
    bot = MagicMock(name="Bot")
    sd = _make_stock_data(validate=validate, existing_tickers=existing_tickers)
    cog = Watchlists(bot=bot, stock_data=sd)
    return cog


# ---------------------------------------------------------------------------
# /add-tickers
# ---------------------------------------------------------------------------

class TestAddTickers:
    @pytest.mark.asyncio
    async def test_happy_path_adds_new_tickers(self):
        cog = _make_cog(validate=True, existing_tickers=["GOOG"])
        interaction = _make_interaction()
        await cog.addtickers.callback(cog, interaction, tickers="AAPL MSFT", watchlist="alpha")

        cog.watchlists.update_watchlist.assert_called_once()
        call_kwargs = cog.watchlists.update_watchlist.call_args[1]
        assert "AAPL" in call_kwargs["tickers"]
        assert "MSFT" in call_kwargs["tickers"]
        assert "GOOG" in call_kwargs["tickers"]

    @pytest.mark.asyncio
    async def test_message_shows_only_newly_added_tickers(self):
        # AAPL already on watchlist; MSFT is new
        cog = _make_cog(validate=True, existing_tickers=["AAPL"])
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL", "MSFT"], []))
        interaction = _make_interaction()

        await cog.addtickers.callback(cog, interaction, tickers="AAPL MSFT", watchlist="alpha")

        sent = interaction.followup.send.call_args[0][0]
        # MSFT should appear in the "added" part; AAPL should appear in "Already on watchlist"
        assert "MSFT" in sent
        assert "Already on watchlist" in sent
        assert "AAPL" in sent  # mentioned as duplicate, not as added

    @pytest.mark.asyncio
    async def test_personal_watchlist_uses_user_id(self):
        cog = _make_cog(validate=False)
        interaction = _make_interaction(user_id=12345)

        await cog.addtickers.callback(cog, interaction, tickers="AAPL", watchlist="personal")

        create_call = cog.watchlists.create_watchlist.call_args[1]
        assert create_call["watchlist_id"] == "12345"

    @pytest.mark.asyncio
    async def test_invalid_tickers_mentioned_in_message(self):
        cog = _make_cog(validate=True, existing_tickers=[])
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], ["FAKEXYZ"]))
        interaction = _make_interaction()

        await cog.addtickers.callback(cog, interaction, tickers="AAPL FAKEXYZ", watchlist="alpha")

        sent = interaction.followup.send.call_args[0][0]
        assert "FAKEXYZ" in sent

    @pytest.mark.asyncio
    async def test_creates_watchlist_when_not_exists(self):
        cog = _make_cog(validate=False, existing_tickers=[])
        interaction = _make_interaction()

        await cog.addtickers.callback(cog, interaction, tickers="AAPL", watchlist="newlist")

        cog.watchlists.create_watchlist.assert_called_once()

    @pytest.mark.asyncio
    async def test_ticker_count_shown_in_message(self):
        cog = _make_cog(validate=True, existing_tickers=["GOOG"])
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        interaction = _make_interaction()

        await cog.addtickers.callback(cog, interaction, tickers="AAPL", watchlist="alpha")

        sent = interaction.followup.send.call_args[0][0]
        assert "2 tickers total" in sent


# ---------------------------------------------------------------------------
# /remove-tickers
# ---------------------------------------------------------------------------

class TestRemoveTickers:
    @pytest.mark.asyncio
    async def test_happy_path_removes_tickers(self):
        cog = _make_cog(validate=True, existing_tickers=["AAPL", "MSFT", "GOOG"])
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        interaction = _make_interaction()

        await cog.removetickers.callback(cog, interaction, tickers="AAPL", watchlist="alpha")

        call_kwargs = cog.watchlists.update_watchlist.call_args[1]
        assert "AAPL" not in call_kwargs["tickers"]
        assert "MSFT" in call_kwargs["tickers"]

    @pytest.mark.asyncio
    async def test_nonexistent_watchlist_sends_error(self):
        cog = _make_cog(validate=False)
        interaction = _make_interaction()

        await cog.removetickers.callback(cog, interaction, tickers="AAPL", watchlist="ghost")

        cog.watchlists.update_watchlist.assert_not_called()
        sent = interaction.followup.send.call_args[0][0]
        assert "does not exist" in sent

    @pytest.mark.asyncio
    async def test_ticker_count_shown_in_message(self):
        cog = _make_cog(validate=True, existing_tickers=["AAPL", "MSFT", "GOOG"])
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        interaction = _make_interaction()

        await cog.removetickers.callback(cog, interaction, tickers="AAPL", watchlist="alpha")

        sent = interaction.followup.send.call_args[0][0]
        assert "2 tickers total" in sent


# ---------------------------------------------------------------------------
# /watchlist
# ---------------------------------------------------------------------------

class TestWatchlist:
    @pytest.mark.asyncio
    async def test_shows_tickers_when_watchlist_exists(self):
        cog = _make_cog(validate=True, existing_tickers=["AAPL", "MSFT"])
        interaction = _make_interaction()

        await cog.watchlist.callback(cog, interaction, watchlist="alpha")

        interaction.followup.send.assert_called_once()
        sent = interaction.followup.send.call_args[0][0]
        assert "AAPL" in sent
        assert "MSFT" in sent

    @pytest.mark.asyncio
    async def test_defers_interaction(self):
        cog = _make_cog(validate=True, existing_tickers=["AAPL"])
        interaction = _make_interaction()

        await cog.watchlist.callback(cog, interaction, watchlist="alpha")

        interaction.response.defer.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_message_when_watchlist_not_found(self):
        cog = _make_cog(validate=False)
        interaction = _make_interaction()

        await cog.watchlist.callback(cog, interaction, watchlist="ghost")

        sent = interaction.followup.send.call_args[0][0]
        assert "does not exist" in sent

    @pytest.mark.asyncio
    async def test_empty_watchlist_message(self):
        cog = _make_cog(validate=True, existing_tickers=[])
        interaction = _make_interaction()

        await cog.watchlist.callback(cog, interaction, watchlist="alpha")

        sent = interaction.followup.send.call_args[0][0]
        assert "No tickers" in sent

    @pytest.mark.asyncio
    async def test_personal_uses_user_id(self):
        cog = _make_cog(validate=True, existing_tickers=["AAPL"])
        interaction = _make_interaction(user_id=99999)

        await cog.watchlist.callback(cog, interaction, watchlist="personal")

        validate_call = cog.watchlists.validate_watchlist.call_args[1]
        assert validate_call["watchlist_id"] == "99999"


# ---------------------------------------------------------------------------
# /set-watchlist
# ---------------------------------------------------------------------------

class TestSetWatchlist:
    @pytest.mark.asyncio
    async def test_sets_tickers(self):
        cog = _make_cog(validate=True)
        interaction = _make_interaction()

        await cog.set_watchlist.callback(cog, interaction, tickers="AAPL MSFT", watchlist="alpha")

        cog.watchlists.update_watchlist.assert_called_once()

    @pytest.mark.asyncio
    async def test_creates_watchlist_if_not_exists(self):
        cog = _make_cog(validate=False)
        interaction = _make_interaction()

        await cog.set_watchlist.callback(cog, interaction, tickers="AAPL", watchlist="newlist")

        cog.watchlists.create_watchlist.assert_called_once()
        cog.watchlists.update_watchlist.assert_called_once()

    @pytest.mark.asyncio
    async def test_ticker_count_shown(self):
        cog = _make_cog(validate=True)
        interaction = _make_interaction()

        await cog.set_watchlist.callback(cog, interaction, tickers="AAPL MSFT", watchlist="alpha")

        sent = interaction.followup.send.call_args[0][0]
        assert "tickers total" in sent


# ---------------------------------------------------------------------------
# /create-watchlist
# ---------------------------------------------------------------------------

class TestCreateWatchlist:
    @pytest.mark.asyncio
    async def test_creates_new_watchlist(self):
        cog = _make_cog(validate=False)
        interaction = _make_interaction()

        await cog.create_watchlist.callback(cog, interaction, watchlist="newlist", tickers="AAPL MSFT")

        cog.watchlists.create_watchlist.assert_called_once()
        call_kwargs = cog.watchlists.create_watchlist.call_args[1]
        assert call_kwargs["watchlist_id"] == "newlist"

    @pytest.mark.asyncio
    async def test_personal_watchlist_uses_user_id(self):
        cog = _make_cog(validate=False)
        interaction = _make_interaction(user_id=55555)

        await cog.create_watchlist.callback(cog, interaction, watchlist="personal", tickers="AAPL")

        call_kwargs = cog.watchlists.create_watchlist.call_args[1]
        assert call_kwargs["watchlist_id"] == "55555"

    @pytest.mark.asyncio
    async def test_already_exists_sends_error(self):
        cog = _make_cog(validate=True)
        interaction = _make_interaction()

        await cog.create_watchlist.callback(cog, interaction, watchlist="alpha", tickers="AAPL")

        cog.watchlists.create_watchlist.assert_not_called()
        sent = interaction.followup.send.call_args[0][0]
        assert "already exists" in sent


# ---------------------------------------------------------------------------
# /delete-watchlist
# ---------------------------------------------------------------------------

class TestDeleteWatchlist:
    @pytest.mark.asyncio
    async def test_rejects_personal_watchlist(self):
        cog = _make_cog(validate=True)
        interaction = _make_interaction()

        await cog.delete_watchlist.callback(cog, interaction, watchlist="personal")

        cog.watchlists.delete_watchlist.assert_not_called()

    @pytest.mark.asyncio
    async def test_nonexistent_watchlist_sends_error(self):
        cog = _make_cog(validate=False)
        interaction = _make_interaction()

        await cog.delete_watchlist.callback(cog, interaction, watchlist="ghost")

        cog.watchlists.delete_watchlist.assert_not_called()
        sent = interaction.followup.send.call_args[0][0]
        assert "does not exist" in sent

    @pytest.mark.asyncio
    async def test_confirmed_delete_removes_watchlist(self):
        cog = _make_cog(validate=True)
        interaction = _make_interaction()

        with patch("rocketstocks.bot.cogs.watchlists.ConfirmDeleteView") as MockView:
            view_instance = MagicMock()
            view_instance.wait = AsyncMock()
            view_instance.confirmed = True
            MockView.return_value = view_instance

            await cog.delete_watchlist.callback(cog, interaction, watchlist="alpha")

        cog.watchlists.delete_watchlist.assert_called_once_with(watchlist_id="alpha")

    @pytest.mark.asyncio
    async def test_cancelled_delete_does_not_remove_watchlist(self):
        cog = _make_cog(validate=True)
        interaction = _make_interaction()

        with patch("rocketstocks.bot.cogs.watchlists.ConfirmDeleteView") as MockView:
            view_instance = MagicMock()
            view_instance.wait = AsyncMock()
            view_instance.confirmed = False
            MockView.return_value = view_instance

            await cog.delete_watchlist.callback(cog, interaction, watchlist="alpha")

        cog.watchlists.delete_watchlist.assert_not_called()
        sent = interaction.followup.send.call_args[0][0]
        assert "cancelled" in sent.lower()


# ---------------------------------------------------------------------------
# /list-watchlists
# ---------------------------------------------------------------------------

class TestListWatchlists:
    @pytest.mark.asyncio
    async def test_lists_all_public_watchlists(self):
        cog = _make_cog(validate=True)
        cog.watchlists.get_watchlists.return_value = ["alpha", "beta"]
        cog.watchlists.get_watchlist_tickers.side_effect = lambda wl_id: (
            ["AAPL", "MSFT"] if wl_id == "alpha" else ["GOOG"]
        )
        interaction = _make_interaction()

        await cog.list_watchlists.callback(cog, interaction)

        sent = interaction.followup.send.call_args[0][0]
        assert "alpha" in sent
        assert "beta" in sent
        assert "2 tickers" in sent
        assert "1 ticker" in sent

    @pytest.mark.asyncio
    async def test_empty_watchlists_sends_message(self):
        cog = _make_cog(validate=True)
        cog.watchlists.get_watchlists.return_value = ["personal"]
        interaction = _make_interaction()

        await cog.list_watchlists.callback(cog, interaction)

        sent = interaction.followup.send.call_args[0][0]
        assert "No" in sent


# ---------------------------------------------------------------------------
# /rename-watchlist
# ---------------------------------------------------------------------------

class TestRenameWatchlist:
    @pytest.mark.asyncio
    async def test_successful_rename(self):
        cog = _make_cog(validate=True)
        cog.watchlists.rename_watchlist.return_value = True
        interaction = _make_interaction()

        await cog.rename_watchlist.callback(cog, interaction, watchlist="alpha", new_name="tech")

        cog.watchlists.rename_watchlist.assert_called_once_with(old_id="alpha", new_id="tech")
        sent = interaction.followup.send.call_args[0][0]
        assert "tech" in sent

    @pytest.mark.asyncio
    async def test_rejects_personal_watchlist(self):
        cog = _make_cog(validate=True)
        interaction = _make_interaction()

        await cog.rename_watchlist.callback(cog, interaction, watchlist="personal", new_name="mine")

        cog.watchlists.rename_watchlist.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_rename_when_source_not_found(self):
        cog = _make_cog(validate=False)  # validate=False → watchlist not found
        cog.watchlists.rename_watchlist.return_value = False
        interaction = _make_interaction()

        await cog.rename_watchlist.callback(cog, interaction, watchlist="ghost", new_name="beta")

        sent = interaction.followup.send.call_args[0][0]
        assert "does not exist" in sent

    @pytest.mark.asyncio
    async def test_failed_rename_when_target_exists(self):
        cog = _make_cog(validate=True)  # validate=True → old exists, new exists too
        cog.watchlists.rename_watchlist.return_value = False
        interaction = _make_interaction()

        await cog.rename_watchlist.callback(cog, interaction, watchlist="alpha", new_name="beta")

        sent = interaction.followup.send.call_args[0][0]
        assert "already exists" in sent
