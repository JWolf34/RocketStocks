"""Tests for bot/views — URL construction in view buttons."""
import pytest


def _get_button_urls(view):
    """Extract URL strings from all Button children of a View."""
    return [item.url for item in view.children if hasattr(item, "url") and item.url]


# discord.ui.View.__init__ calls asyncio.get_running_loop(), so all view
# instantiation must happen inside an async context.

class TestStockReportButtons:
    async def test_google_url_contains_ticker(self):
        from rocketstocks.bot.views.report_views import StockReportButtons
        v = StockReportButtons("AAPL")
        urls = _get_button_urls(v)
        assert any("google.com" in u and "AAPL" in u for u in urls)

    async def test_finviz_url_contains_ticker(self):
        from rocketstocks.bot.views.report_views import StockReportButtons
        v = StockReportButtons("TSLA")
        urls = _get_button_urls(v)
        assert any("finviz.com" in u and "TSLA" in u for u in urls)

    async def test_yahoo_finance_url_contains_ticker(self):
        from rocketstocks.bot.views.report_views import StockReportButtons
        v = StockReportButtons("MSFT")
        urls = _get_button_urls(v)
        assert any("finance.yahoo.com" in u and "MSFT" in u for u in urls)

    async def test_stockinvest_url_contains_ticker(self):
        from rocketstocks.bot.views.report_views import StockReportButtons
        v = StockReportButtons("NVDA")
        urls = _get_button_urls(v)
        assert any("stockinvest.us" in u and "NVDA" in u for u in urls)


class TestGainerScreenerButtons:
    async def test_premarket_url(self):
        from rocketstocks.bot.views.report_views import GainerScreenerButtons
        v = GainerScreenerButtons("premarket")
        urls = _get_button_urls(v)
        assert any("pre-market" in u for u in urls)

    async def test_intraday_url(self):
        from rocketstocks.bot.views.report_views import GainerScreenerButtons
        v = GainerScreenerButtons("intraday")
        urls = _get_button_urls(v)
        assert any("gainers" in u and "pre-market" not in u for u in urls)

    async def test_aftermarket_url(self):
        from rocketstocks.bot.views.report_views import GainerScreenerButtons
        v = GainerScreenerButtons("aftermarket")
        urls = _get_button_urls(v)
        assert any("after-hours" in u for u in urls)

    async def test_unknown_period_fallback_url(self):
        from rocketstocks.bot.views.report_views import GainerScreenerButtons
        v = GainerScreenerButtons("unknown")
        urls = _get_button_urls(v)
        assert any("tradingview.com" in u for u in urls)


class TestVolumeScreenerButtons:
    async def test_has_unusual_volume_url(self):
        from rocketstocks.bot.views.report_views import VolumeScreenerButtons
        v = VolumeScreenerButtons()
        urls = _get_button_urls(v)
        assert any("unusual-volume" in u for u in urls)


class TestPopularityScreenerButtons:
    async def test_has_apewisdom_url(self):
        from rocketstocks.bot.views.report_views import PopularityScreenerButtons
        v = PopularityScreenerButtons()
        urls = _get_button_urls(v)
        assert any("apewisdom.io" in u for u in urls)


class TestPoliticianReportButtons:
    async def test_url_contains_politician_id(self):
        from rocketstocks.bot.views.report_views import PoliticianReportButtons
        v = PoliticianReportButtons(pid="nancy-pelosi")
        urls = _get_button_urls(v)
        assert any("nancy-pelosi" in u for u in urls)


class TestAlertButtons:
    async def test_three_urls_contain_ticker(self):
        from rocketstocks.bot.views.alert_views import AlertButtons
        v = AlertButtons("GME")
        urls = _get_button_urls(v)
        assert len(urls) == 3
        assert all("GME" in u for u in urls)


class TestWatchlistSelect:
    async def test_adds_ticker_to_selected_watchlist(self):
        from unittest.mock import AsyncMock, MagicMock
        from rocketstocks.bot.views.alert_views import WatchlistSelect

        view = WatchlistSelect("GME", ["global", "personal"])

        interaction = AsyncMock()
        interaction.user.id = 12345
        interaction.data = {"values": ["global"]}

        watchlists = MagicMock()
        watchlists.validate_watchlist.return_value = True
        watchlists.get_watchlist_tickers.return_value = ["AAPL", "TSLA"]
        interaction.client.stock_data.watchlists = watchlists

        await view._select_callback(interaction)

        watchlists.update_watchlist.assert_called_once()
        interaction.response.send_message.assert_awaited_once()
        msg = interaction.response.send_message.call_args[0][0]
        assert "GME" in msg

    async def test_already_on_watchlist_message(self):
        from unittest.mock import AsyncMock, MagicMock
        from rocketstocks.bot.views.alert_views import WatchlistSelect

        view = WatchlistSelect("AAPL", ["global"])

        interaction = AsyncMock()
        interaction.user.id = 12345
        interaction.data = {"values": ["global"]}

        watchlists = MagicMock()
        watchlists.validate_watchlist.return_value = True
        watchlists.get_watchlist_tickers.return_value = ["AAPL", "TSLA"]
        interaction.client.stock_data.watchlists = watchlists

        await view._select_callback(interaction)

        watchlists.update_watchlist.assert_not_called()
        msg = interaction.response.send_message.call_args[0][0]
        assert "already" in msg.lower()

    async def test_error_handling(self):
        from unittest.mock import AsyncMock, MagicMock
        from rocketstocks.bot.views.alert_views import WatchlistSelect

        view = WatchlistSelect("GME", ["global"])

        interaction = AsyncMock()
        interaction.user.id = 12345
        interaction.data = {"values": ["global"]}

        watchlists = MagicMock()
        watchlists.validate_watchlist.side_effect = Exception("DB error")
        interaction.client.stock_data.watchlists = watchlists

        await view._select_callback(interaction)

        interaction.response.send_message.assert_awaited_once()
        msg = interaction.response.send_message.call_args[0][0]
        assert "error" in msg.lower()


class TestAlertButtonsAddToWatchlist:
    async def test_populates_dropdown_with_watchlists(self):
        from unittest.mock import AsyncMock, MagicMock
        from rocketstocks.bot.views.alert_views import AlertButtons, WatchlistSelect

        v = AlertButtons("GME")

        interaction = AsyncMock()
        watchlists_obj = MagicMock()
        watchlists_obj.get_watchlists.return_value = ["global", "personal"]
        interaction.client.stock_data.watchlists = watchlists_obj

        await v.add_to_watchlist.callback(interaction)

        interaction.response.send_message.assert_awaited_once()
        _, kwargs = interaction.response.send_message.call_args
        assert isinstance(kwargs["view"], WatchlistSelect)
        assert kwargs["ephemeral"] is True

    async def test_error_handling(self):
        from unittest.mock import AsyncMock, MagicMock
        from rocketstocks.bot.views.alert_views import AlertButtons

        v = AlertButtons("GME")

        interaction = AsyncMock()
        watchlists_obj = MagicMock()
        watchlists_obj.get_watchlists.side_effect = Exception("DB error")
        interaction.client.stock_data.watchlists = watchlists_obj

        await v.add_to_watchlist.callback(interaction)

        interaction.response.send_message.assert_awaited_once()
        msg = interaction.response.send_message.call_args[0][0]
        assert "error" in msg.lower()


class TestAlertButtonsGenerateReport:
    async def test_successful_generation(self):
        from unittest.mock import AsyncMock, MagicMock, patch
        from rocketstocks.bot.views.alert_views import AlertButtons

        v = AlertButtons("GME")

        interaction = AsyncMock()
        interaction.client = MagicMock()
        reports_cog = AsyncMock()
        mock_report = MagicMock()
        mock_embed = MagicMock()
        mock_report.build.return_value = MagicMock()
        reports_cog.build_stock_report.return_value = mock_report
        interaction.client.get_cog.return_value = reports_cog

        with patch("rocketstocks.bot.senders.embed_utils.spec_to_embed", return_value=mock_embed):
            await v.generate_report.callback(interaction)

        interaction.response.defer.assert_awaited_once_with(ephemeral=True)
        interaction.followup.send.assert_awaited_once()
        _, kwargs = interaction.followup.send.call_args
        assert kwargs["embed"] == mock_embed

    async def test_reports_cog_unavailable(self):
        from unittest.mock import AsyncMock, MagicMock
        from rocketstocks.bot.views.alert_views import AlertButtons

        v = AlertButtons("GME")

        interaction = AsyncMock()
        interaction.client = MagicMock()
        interaction.client.get_cog.return_value = None

        await v.generate_report.callback(interaction)

        interaction.response.defer.assert_awaited_once_with(ephemeral=True)
        interaction.followup.send.assert_awaited_once()
        msg = interaction.followup.send.call_args[0][0]
        assert "not available" in msg.lower()

    async def test_error_handling(self):
        from unittest.mock import AsyncMock, MagicMock
        from rocketstocks.bot.views.alert_views import AlertButtons

        v = AlertButtons("GME")

        interaction = AsyncMock()
        interaction.client = MagicMock()
        reports_cog = AsyncMock()
        reports_cog.build_stock_report.side_effect = Exception("API error")
        interaction.client.get_cog.return_value = reports_cog

        await v.generate_report.callback(interaction)

        interaction.response.defer.assert_awaited_once_with(ephemeral=True)
        interaction.followup.send.assert_awaited_once()
        msg = interaction.followup.send.call_args[0][0]
        assert "error" in msg.lower()


class TestPopularitySurgeAlertButtons:
    async def test_has_apewisdom_url(self):
        from rocketstocks.bot.views.alert_views import PopularitySurgeAlertButtons
        v = PopularitySurgeAlertButtons("GME")
        urls = _get_button_urls(v)
        assert any("apewisdom.io" in u for u in urls)

    async def test_inherits_standard_alert_buttons(self):
        from rocketstocks.bot.views.alert_views import PopularitySurgeAlertButtons
        v = PopularitySurgeAlertButtons("GME")
        urls = _get_button_urls(v)
        assert any("finviz.com" in u and "GME" in u for u in urls)
        assert any("finance.yahoo.com" in u and "GME" in u for u in urls)
        assert any("stockinvest.us" in u and "GME" in u for u in urls)


class TestPoliticianTradeButtons:
    async def test_url_contains_politician_id(self):
        from rocketstocks.bot.views.alert_views import PoliticianTradeButtons
        v = PoliticianTradeButtons({"politician_id": "nancy-pelosi"})
        urls = _get_button_urls(v)
        assert any("nancy-pelosi" in u for u in urls)
        assert any("capitoltrades.com" in u for u in urls)
