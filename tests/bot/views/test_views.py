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
    async def test_all_four_urls_contain_ticker(self):
        from rocketstocks.bot.views.alert_views import AlertButtons
        v = AlertButtons("GME")
        urls = _get_button_urls(v)
        assert len(urls) == 4
        assert all("GME" in u for u in urls)


class TestPoliticianTradeButtons:
    async def test_url_contains_politician_id(self):
        from rocketstocks.bot.views.alert_views import PoliticianTradeButtons
        v = PoliticianTradeButtons({"politician_id": "nancy-pelosi"})
        urls = _get_button_urls(v)
        assert any("nancy-pelosi" in u for u in urls)
        assert any("capitoltrades.com" in u for u in urls)
