"""Tests for rocketstocks.bot.bot — RocketStocksBot."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestSetupHook:
    @pytest.mark.asyncio
    async def test_scheduler_started(self):
        """setup_hook must register jobs and start the AsyncIOScheduler."""
        from rocketstocks.bot.bot import RocketStocksBot

        mock_bot = MagicMock()
        mock_bot.stock_data = MagicMock()
        mock_bot.stock_data.db.open = AsyncMock()
        mock_bot.emitter = MagicMock()

        mock_sched = MagicMock()

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler", return_value=mock_sched),
            patch("rocketstocks.bot.bot.register_jobs") as mock_register,
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        mock_register.assert_called_once_with(mock_sched, mock_bot.stock_data, mock_bot.emitter)
        mock_sched.start.assert_called_once()
        assert mock_bot.aio_sched is mock_sched

    @pytest.mark.asyncio
    async def test_setup_hook_stores_scheduler(self):
        """setup_hook must attach the scheduler as self.aio_sched."""
        from rocketstocks.bot.bot import RocketStocksBot

        mock_bot = MagicMock()
        mock_bot.stock_data = MagicMock()
        mock_bot.stock_data.db.open = AsyncMock()
        mock_bot.emitter = MagicMock()

        mock_sched = MagicMock()

        with (
            patch("rocketstocks.bot.bot.AsyncIOScheduler", return_value=mock_sched),
            patch("rocketstocks.bot.bot.register_jobs"),
        ):
            await RocketStocksBot.setup_hook(mock_bot)

        assert mock_bot.aio_sched is mock_sched
