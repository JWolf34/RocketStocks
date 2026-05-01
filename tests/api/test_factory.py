"""Tests for rocketstocks.api._factory.build_data_api."""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rocketstocks.api.client import DataAPI
from rocketstocks.data.clients.schwab import SchwabTokenError


class TestBuildDataApi:
    async def test_returns_data_api_instance(self):
        with patch("rocketstocks.api._factory.StockData") as mock_sd_cls:
            sd = MagicMock(name="StockData")
            sd.db.open = AsyncMock()
            sd.init_schwab = AsyncMock()
            mock_sd_cls.return_value = sd

            from rocketstocks.api._factory import build_data_api
            api = await build_data_api()

            assert isinstance(api, DataAPI)
            sd.db.open.assert_called_once()
            sd.init_schwab.assert_called_once()

    async def test_missing_schwab_token_does_not_crash_boot(self):
        with patch("rocketstocks.api._factory.StockData") as mock_sd_cls:
            sd = MagicMock(name="StockData")
            sd.db.open = AsyncMock()
            sd.init_schwab = AsyncMock(side_effect=SchwabTokenError("no token"))
            mock_sd_cls.return_value = sd

            from rocketstocks.api._factory import build_data_api
            api = await build_data_api()

            assert isinstance(api, DataAPI)

    async def test_db_open_failure_propagates(self):
        with patch("rocketstocks.api._factory.StockData") as mock_sd_cls:
            sd = MagicMock(name="StockData")
            sd.db.open = AsyncMock(side_effect=OSError("cannot connect"))
            mock_sd_cls.return_value = sd

            from rocketstocks.api._factory import build_data_api
            with pytest.raises(OSError, match="cannot connect"):
                await build_data_api()
