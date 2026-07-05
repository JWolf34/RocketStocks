"""Tests for rocketstocks.bot.bot — RocketStocksBot."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_mock_bot():
    mock_bot = MagicMock()
    mock_bot.stock_data = MagicMock()
    mock_bot.stock_data.db.open = AsyncMock()
    mock_bot.stock_data.init_schwab = AsyncMock()
    mock_bot.stock_data.bot_settings.get = AsyncMock(return_value=None)
    mock_bot.notification_config = MagicMock()
    mock_bot.emitter = MagicMock()
    return mock_bot


class TestSetupHook:
    @pytest.mark.asyncio
    async def test_scheduler_started(self):
        """setup_hook must register jobs and start the AsyncIOScheduler."""
        from rocketstocks.bot.bot import RocketStocksBot

        mock_bot = _make_mock_bot()
        mock_sched = MagicMock()

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler", return_value=mock_sched),
            patch("rocketstocks.bot.bot.register_jobs") as mock_register,
            patch("rocketstocks.bot.bot.create_tables", new_callable=AsyncMock),
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        mock_register.assert_called_once_with(mock_sched, mock_bot.stock_data, mock_bot.emitter)
        mock_sched.start.assert_called_once()
        assert mock_bot.aio_sched is mock_sched

    @pytest.mark.asyncio
    async def test_setup_hook_stores_scheduler(self):
        """setup_hook must attach the scheduler as self.aio_sched."""
        from rocketstocks.bot.bot import RocketStocksBot

        mock_bot = _make_mock_bot()
        mock_sched = MagicMock()

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler", return_value=mock_sched),
            patch("rocketstocks.bot.bot.register_jobs"),
            patch("rocketstocks.bot.bot.create_tables", new_callable=AsyncMock),
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        assert mock_bot.aio_sched is mock_sched

    @pytest.mark.asyncio
    async def test_setup_hook_skips_tz_db_read_when_env_present(self, monkeypatch):
        """When TZ is in os.environ, setup_hook should NOT read tz from DB."""
        from rocketstocks.bot.bot import RocketStocksBot

        mock_bot = _make_mock_bot()
        monkeypatch.setenv("TZ", "UTC")

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler"),
            patch("rocketstocks.bot.bot.register_jobs"),
            patch("rocketstocks.bot.bot.create_tables", new_callable=AsyncMock),
            patch("rocketstocks.bot.bot.configure_tz") as mock_configure_tz,
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        mock_configure_tz.assert_not_called()

    @pytest.mark.asyncio
    async def test_setup_hook_applies_tz_from_db_when_no_env(self, monkeypatch):
        """When TZ is not in os.environ and DB has a value, configure_tz is called."""
        from rocketstocks.bot.bot import RocketStocksBot

        mock_bot = _make_mock_bot()
        mock_bot.stock_data.bot_settings.get = AsyncMock(
            side_effect=lambda key: "Europe/London" if key == "tz" else None
        )
        monkeypatch.delenv("TZ", raising=False)
        monkeypatch.delenv("NOTIFICATION_FILTER", raising=False)

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler"),
            patch("rocketstocks.bot.bot.register_jobs"),
            patch("rocketstocks.bot.bot.create_tables", new_callable=AsyncMock),
            patch("rocketstocks.bot.bot.configure_tz") as mock_configure_tz,
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        mock_configure_tz.assert_called_once_with("Europe/London")

    @pytest.mark.asyncio
    async def test_setup_hook_applies_notification_filter_from_db(self, monkeypatch):
        """When NOTIFICATION_FILTER is not in env and DB has a value, filter is updated."""
        from rocketstocks.bot.bot import RocketStocksBot
        from rocketstocks.core.notifications.config import NotificationFilter

        mock_bot = _make_mock_bot()
        mock_bot.stock_data.bot_settings.get = AsyncMock(
            side_effect=lambda key: "failures_only" if key == "notification_filter" else None
        )
        monkeypatch.delenv("TZ", raising=False)
        monkeypatch.delenv("NOTIFICATION_FILTER", raising=False)

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler"),
            patch("rocketstocks.bot.bot.register_jobs"),
            patch("rocketstocks.bot.bot.create_tables", new_callable=AsyncMock),
            patch("rocketstocks.bot.bot.configure_tz"),
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        assert mock_bot.notification_config.filter == NotificationFilter.FAILURES_ONLY

    @pytest.mark.asyncio
    async def test_setup_hook_skips_filter_db_read_when_env_present(self, monkeypatch):
        """When NOTIFICATION_FILTER is in os.environ, notification_config.filter is not updated from DB."""
        from rocketstocks.bot.bot import RocketStocksBot

        mock_bot = _make_mock_bot()
        monkeypatch.setenv("NOTIFICATION_FILTER", "off")

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler"),
            patch("rocketstocks.bot.bot.register_jobs"),
            patch("rocketstocks.bot.bot.create_tables", new_callable=AsyncMock),
            patch("rocketstocks.bot.bot.configure_tz"),
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        # filter should not have been changed by setup_hook (db path not taken)
        assert mock_bot.stock_data.bot_settings.get.await_count == 0 or \
               all(call.args[0] != "notification_filter"
                   for call in mock_bot.stock_data.bot_settings.get.await_args_list)
