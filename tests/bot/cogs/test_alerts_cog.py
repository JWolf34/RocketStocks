"""Tests for rocketstocks.bot.cogs.alerts."""
import datetime
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.alerts import Alerts


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.emitter = MagicMock()
    bot.iter_channels.return_value = []
    return bot


def _make_stock_data(earnings_df: pd.DataFrame):
    sd = MagicMock(name="StockData")
    sd.earnings.get_earnings_on_date.return_value = earnings_df
    sd.alert_tickers = {}
    return sd


def _make_cog(earnings_df: pd.DataFrame):
    bot = _make_bot()
    sd = _make_stock_data(earnings_df)
    with (
        patch.object(Alerts, "post_alerts_date"),
        patch.object(Alerts, "send_popularity_movers"),
        patch.object(Alerts, "send_alerts"),
    ):
        cog = Alerts(bot=bot, stock_data=sd)
    cog.dstate = MagicMock()
    cog.mutils = MagicMock()
    return cog


class TestSendEarningsMovers:
    @pytest.mark.asyncio
    async def test_empty_dataframe_returns_early(self):
        """Should not raise KeyError when no earnings exist for today."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        # Must not raise — previously raised KeyError: 'ticker'
        await cog.send_earnings_movers(quotes={"AAPL": {"quote": {"netPercentChange": 10.0}}}, channels=[])

    @pytest.mark.asyncio
    async def test_no_matching_tickers_sends_no_alerts(self):
        """Tickers in quotes not in earnings_today produce no alerts."""
        df = pd.DataFrame({"ticker": ["MSFT"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        await cog.send_earnings_movers(quotes={"AAPL": {"quote": {"netPercentChange": 10.0}}}, channels=[channel])
        channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_matching_ticker_below_threshold_sends_no_alert(self):
        """A reporting ticker with < ±5% change should not trigger an alert."""
        df = pd.DataFrame({"ticker": ["AAPL"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        await cog.send_earnings_movers(
            quotes={"AAPL": {"quote": {"netPercentChange": 3.0}}}, channels=[channel]
        )
        channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_matching_ticker_above_threshold_sends_alert(self):
        """A reporting ticker with > ±5% change should send an alert."""
        df = pd.DataFrame({
            "ticker": ["AAPL"],
            "date": [datetime.date.today()],
            "eps_estimate": [1.5],
            "eps_actual": [1.8],
            "revenue_estimate": [None],
            "revenue_actual": [None],
        })
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()

        with (
            patch.object(cog, "build_earnings_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_build.return_value = MagicMock()
            await cog.send_earnings_movers(
                quotes={"AAPL": {"quote": {"netPercentChange": 8.5}}}, channels=[channel]
            )
            mock_build.assert_called_once()
            mock_send.assert_called_once()


class TestBuildWatchlistMover:
    @pytest.mark.asyncio
    async def test_quote_is_awaited(self):
        """build_watchlist_mover must await get_quote, not return a coroutine object."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        expected_quote = {"quote": {"netPercentChange": 5.0}}
        cog.stock_data.schwab.get_quote = AsyncMock(return_value=expected_quote)
        cog.stock_data.watchlists.get_watchlists.return_value = {}
        cog.stock_data.tickers.get_ticker_info.return_value = {}

        with (
            patch("rocketstocks.bot.cogs.alerts.WatchlistMoverData") as mock_data_cls,
            patch("rocketstocks.bot.cogs.alerts.WatchlistMoverAlert"),
        ):
            mock_data_cls.return_value = MagicMock()
            await cog.build_watchlist_mover(ticker="AAPL")

        # get_quote was awaited (AsyncMock confirms this)
        cog.stock_data.schwab.get_quote.assert_called_once_with(ticker="AAPL")
        # The quote value passed to WatchlistMoverData is the actual resolved dict, not a coroutine
        _, data_kwargs = mock_data_cls.call_args
        assert data_kwargs["quote"] == expected_quote


class TestSendWatchlistMovers:
    @pytest.mark.asyncio
    async def test_quote_passed_to_builder(self):
        """send_watchlist_movers must pass the pre-fetched quote to build_watchlist_mover."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        ticker = "AAPL"
        quote = {"quote": {"netPercentChange": 15.0}}
        cog.stock_data.watchlists.get_all_watchlist_tickers.return_value = [ticker]

        with (
            patch.object(cog, "build_watchlist_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock),
        ):
            mock_build.return_value = MagicMock()
            await cog.send_watchlist_movers(quotes={ticker: quote}, channels=[AsyncMock()])

        mock_build.assert_called_once()
        _, call_kwargs = mock_build.call_args
        assert call_kwargs.get("quote") == quote


class TestPerTickerIsolation:
    @pytest.mark.asyncio
    async def test_earnings_mover_isolation(self):
        """A single failing ticker in send_earnings_movers does not abort the rest."""
        df = pd.DataFrame({
            "ticker": ["AAPL", "MSFT"],
            "date": [datetime.date.today(), datetime.date.today()],
        })
        cog = _make_cog(earnings_df=df)

        quotes = {
            "AAPL": {"quote": {"netPercentChange": 10.0}},
            "MSFT": {"quote": {"netPercentChange": 12.0}},
        }

        built = []

        async def mock_build(ticker, **kwargs):
            if ticker == "AAPL":
                raise RuntimeError("build failed")
            built.append(ticker)
            return MagicMock()

        with (
            patch.object(cog, "build_earnings_mover", side_effect=mock_build),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog.send_earnings_movers(quotes=quotes, channels=[AsyncMock()])

        assert "MSFT" in built
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_watchlist_mover_isolation(self):
        """A single failing ticker in send_watchlist_movers does not abort the rest."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.stock_data.watchlists.get_all_watchlist_tickers.return_value = ["AAPL", "MSFT"]

        quotes = {
            "AAPL": {"quote": {"netPercentChange": 15.0}},
            "MSFT": {"quote": {"netPercentChange": 15.0}},
        }

        built = []

        async def mock_build(ticker, **kwargs):
            if ticker == "AAPL":
                raise RuntimeError("build failed")
            built.append(ticker)
            return MagicMock()

        with (
            patch.object(cog, "build_watchlist_mover", side_effect=mock_build),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog.send_watchlist_movers(quotes=quotes, channels=[AsyncMock()])

        assert "MSFT" in built
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_unusual_volume_mover_isolation(self):
        """A single failing ticker in send_unusual_volume_movers does not abort the rest."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        quotes = {
            "AAPL": {"quote": {"netPercentChange": 15.0, "totalVolume": 1_000_000}},
            "MSFT": {"quote": {"netPercentChange": 15.0, "totalVolume": 1_000_000}},
        }

        processed = []

        def fetch_daily_side_effect(ticker):
            if ticker == "AAPL":
                raise RuntimeError("price history failed")
            processed.append(ticker)
            return pd.DataFrame()  # empty → no alert, but not an error

        cog.stock_data.price_history.fetch_daily_price_history.side_effect = fetch_daily_side_effect

        await cog.send_unusual_volume_movers(quotes=quotes, channels=[AsyncMock()])

        assert "MSFT" in processed

    @pytest.mark.asyncio
    async def test_volume_spike_mover_isolation(self):
        """A single failing ticker in send_volume_spike_movers does not abort the rest."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        quotes = {
            "AAPL": {"quote": {"netPercentChange": 15.0, "totalVolume": 1_000_000}},
            "MSFT": {"quote": {"netPercentChange": 15.0, "totalVolume": 1_000_000}},
        }

        processed = []

        def fetch_5m_side_effect(ticker):
            if ticker == "AAPL":
                raise RuntimeError("5m history failed")
            processed.append(ticker)
            return pd.DataFrame()  # empty → no alert

        cog.stock_data.price_history.fetch_5m_price_history.side_effect = fetch_5m_side_effect

        await cog.send_volume_spike_movers(quotes=quotes, channels=[AsyncMock()])

        assert "MSFT" in processed

    @pytest.mark.asyncio
    async def test_popularity_mover_isolation(self):
        """A failing ticker in send_popularity_movers loop does not abort the rest."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True

        pop_df = pd.DataFrame({"ticker": ["AAPL", "MSFT"]})
        cog.stock_data.popularity.get_popular_stocks.return_value = pop_df

        processed = []

        def fetch_popularity_side_effect(ticker):
            if ticker == "AAPL":
                raise RuntimeError("popularity failed")
            processed.append(ticker)
            return pd.DataFrame()  # empty → no alert

        cog.stock_data.popularity.fetch_popularity.side_effect = fetch_popularity_side_effect

        await cog._send_popularity_movers_impl()

        assert "MSFT" in processed


class TestSendAlertsImpl:
    @pytest.mark.asyncio
    async def test_per_type_isolation(self):
        """A failing send_*_movers method does not abort the remaining methods."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "intraday"

        mock_channel = MagicMock()
        cog.bot.iter_channels.return_value = [(1, mock_channel)]
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={})

        with (
            patch.object(cog, "send_unusual_volume_movers", new_callable=AsyncMock, side_effect=RuntimeError("boom")) as mock_uvol,
            patch.object(cog, "send_volume_spike_movers", new_callable=AsyncMock) as mock_spike,
            patch.object(cog, "send_earnings_movers", new_callable=AsyncMock) as mock_earnings,
            patch.object(cog, "send_watchlist_movers", new_callable=AsyncMock) as mock_watchlist,
        ):
            await cog._send_alerts_impl()

        # Despite unusual_volume raising, the other three are still called
        mock_spike.assert_called_once()
        mock_earnings.assert_called_once()
        mock_watchlist.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_when_market_closed(self):
        """send_alerts does nothing when market is closed."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = False
        cog.mutils.get_market_period.return_value = "closed"

        with patch.object(cog, "send_unusual_volume_movers", new_callable=AsyncMock) as mock_uvol:
            await cog._send_alerts_impl()

        mock_uvol.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_eod(self):
        """send_alerts does nothing at EOD."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "EOD"

        with patch.object(cog, "send_earnings_movers", new_callable=AsyncMock) as mock_earnings:
            await cog._send_alerts_impl()

        mock_earnings.assert_not_called()


class TestRunTask:
    @pytest.mark.asyncio
    async def test_emits_success_on_completion(self):
        """_run_task emits a SUCCESS notification when the coroutine completes."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        async def noop():
            pass

        await cog._run_task("test_job", noop())

        cog.bot.emitter.emit.assert_called_once()
        event = cog.bot.emitter.emit.call_args[0][0]
        from rocketstocks.core.notifications.config import NotificationLevel
        assert event.level == NotificationLevel.SUCCESS
        assert event.job_name == "test_job"

    @pytest.mark.asyncio
    async def test_emits_failure_on_exception(self):
        """_run_task emits a FAILURE notification and does not re-raise on error."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        async def failing():
            raise ValueError("something went wrong")

        # Should not raise
        await cog._run_task("bad_job", failing())

        cog.bot.emitter.emit.assert_called_once()
        event = cog.bot.emitter.emit.call_args[0][0]
        from rocketstocks.core.notifications.config import NotificationLevel
        assert event.level == NotificationLevel.FAILURE
        assert "something went wrong" in event.message

    @pytest.mark.asyncio
    async def test_no_reraise_on_exception(self):
        """_run_task must not re-raise exceptions so discord.py reschedules normally."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        async def failing():
            raise RuntimeError("fatal")

        # Must complete without raising
        try:
            await cog._run_task("job", failing())
        except RuntimeError:
            pytest.fail("_run_task must not re-raise exceptions")


class TestPopularityMoversMarketGuard:
    @pytest.mark.asyncio
    async def test_skips_when_market_closed(self):
        """_send_popularity_movers_impl does nothing when market is closed."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = False

        await cog._send_popularity_movers_impl()

        cog.stock_data.popularity.get_popular_stocks.assert_not_called()
