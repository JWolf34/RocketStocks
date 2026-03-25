"""Tests for rocketstocks.bot.cogs.alerts."""
import datetime
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.alerts import Alerts
from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult, ConfirmationResult
from rocketstocks.core.analysis.classification import StockClass
from rocketstocks.core.analysis.volume_divergence import VolumeAccumulationResult


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.emitter = MagicMock()
    bot.iter_channels = AsyncMock(return_value=[])
    bot.stock_data.alert_roles.get_role_ids = AsyncMock(return_value=[])
    return bot


def _make_confirmation_result(should_confirm=True):
    return ConfirmationResult(
        should_confirm=should_confirm,
        pct_since_flag=3.0 if should_confirm else 0.1,
        zscore_since_flag=2.0 if should_confirm else 0.1,
        is_sustained=None,
    )


def _make_stock_data(earnings_df: pd.DataFrame):
    sd = MagicMock(name="StockData")
    sd.earnings.get_earnings_on_date = AsyncMock(return_value=earnings_df)
    sd.alert_tickers = {}
    sd.ticker_stats.get_all_classifications = AsyncMock(return_value={})
    sd.ticker_stats.get_stats = AsyncMock(return_value=None)
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
    sd.price_history.fetch_daily_price_history_batch = AsyncMock(return_value={})
    sd.surge_store.get_active_surges = AsyncMock(return_value=[])
    sd.surge_store.expire_old_surges = AsyncMock(return_value=None)
    sd.surge_store.is_already_flagged = AsyncMock(return_value=False)
    sd.surge_store.get_flagged_tickers = AsyncMock(return_value=set())
    sd.surge_store.insert_surge = AsyncMock()
    sd.surge_store.mark_confirmed = AsyncMock()
    sd.market_signal_store.get_active_signals = AsyncMock(return_value=[])
    sd.market_signal_store.expire_old_signals = AsyncMock(return_value=None)
    sd.market_signal_store.is_already_signaled = AsyncMock(return_value=False)
    sd.market_signal_store.get_latest_signal = AsyncMock(return_value=None)
    sd.market_signal_store.get_signaled_tickers_today = AsyncMock(return_value={})
    sd.market_signal_store.get_signal_history = AsyncMock(return_value=[])
    sd.market_signal_store.insert_signal = AsyncMock(return_value=None)
    sd.market_signal_store.update_alert_message_id = AsyncMock(return_value=None)
    sd.market_signal_store.mark_confirmed = AsyncMock(return_value=None)
    sd.watchlists.get_all_watchlist_tickers = AsyncMock(return_value=[])
    sd.watchlists.get_watchlists = AsyncMock(return_value=[])
    sd.watchlists.get_ticker_to_watchlist_map = AsyncMock(return_value={})
    sd.tickers.get_ticker_info = AsyncMock(return_value={})
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
    sd.earnings_results = MagicMock()
    sd.earnings_results.get_result = AsyncMock(return_value=None)
    sd.db = MagicMock()
    sd.db.execute = AsyncMock(return_value=None)
    return sd


def _make_trigger_result(should_alert=True):
    return AlertTriggerResult(
        should_alert=should_alert,
        classification=StockClass.STANDARD,
        zscore=3.0,
        percentile=98.0,
        bb_position=None,
        confluence_count=None,
        confluence_total=None,
        confluence_details=None,
        volume_zscore=2.5,
        signal_type='unusual_move',
    )


def _make_accumulation_result(is_accumulating=True):
    return VolumeAccumulationResult(
        is_accumulating=is_accumulating,
        vol_zscore=3.0 if is_accumulating else 1.0,
        price_zscore=0.3,
        rvol=4.0,
        divergence_score=2.7 if is_accumulating else 0.7,
        signal_strength='volume_only',
    )


def _make_cog(earnings_df: pd.DataFrame):
    bot = _make_bot()
    sd = _make_stock_data(earnings_df)
    with (
        patch.object(Alerts, "post_alerts_date"),
        patch.object(Alerts, "detect_popularity_surges"),
        patch.object(Alerts, "process_alerts"),
        patch("rocketstocks.bot.cogs.alerts.DiscordState"),
    ):
        cog = Alerts(bot=bot, stock_data=sd)
    cog.dstate = MagicMock()
    cog.dstate.get_alerts_by_type_today = AsyncMock(return_value=[])
    cog.mutils = MagicMock()
    return cog


# ---------------------------------------------------------------------------
# Earnings pipeline
# ---------------------------------------------------------------------------

class TestEarningsPipeline:
    @pytest.mark.asyncio
    async def test_empty_dataframe_returns_early(self):
        """Should not raise KeyError when no earnings exist for today."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        await cog._earnings_pipeline(
            quotes={"AAPL": {"quote": {"netPercentChange": 10.0, "totalVolume": 1_000_000}}},
            classifications={},
            channels=[],
            earnings_today=pd.DataFrame(),
            price_cache={},
        )

    @pytest.mark.asyncio
    async def test_no_matching_tickers_sends_no_alerts(self):
        """Tickers in quotes not in earnings_today produce no alerts."""
        df = pd.DataFrame({"ticker": ["MSFT"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        await cog._earnings_pipeline(
            quotes={"AAPL": {"quote": {"netPercentChange": 10.0, "totalVolume": 1_000_000}}},
            classifications={},
            channels=[channel],
            earnings_today=df,
            price_cache={},
        )
        channel.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_matching_ticker_evaluate_called(self):
        """evaluate_price_alert is called for matching tickers."""
        df = pd.DataFrame({"ticker": ["AAPL"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        fake_trigger = _make_trigger_result(should_alert=False)

        with patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert",
                   return_value=fake_trigger) as mock_eval:
            await cog._earnings_pipeline(
                quotes={"AAPL": {"quote": {"netPercentChange": 3.0, "totalVolume": 1_000_000}}},
                classifications={},
                channels=[channel],
                earnings_today=df,
                price_cache={},
            )
            mock_eval.assert_called_once()

    @pytest.mark.asyncio
    async def test_alert_sent_when_trigger_true(self):
        """Alert is sent when evaluate_price_alert returns should_alert=True."""
        df = pd.DataFrame({"ticker": ["AAPL"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        fake_trigger = _make_trigger_result(should_alert=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_earnings_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_build.return_value = MagicMock()
            await cog._earnings_pipeline(
                quotes={"AAPL": {"quote": {"netPercentChange": 8.5, "totalVolume": 1_000_000}}},
                classifications={},
                channels=[channel],
                earnings_today=df,
                price_cache={},
            )
            mock_build.assert_called_once()
            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_alert_when_trigger_false(self):
        """No alert sent when evaluate_price_alert returns should_alert=False."""
        df = pd.DataFrame({"ticker": ["AAPL"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        channel = AsyncMock()
        fake_trigger = _make_trigger_result(should_alert=False)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_earnings_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._earnings_pipeline(
                quotes={"AAPL": {"quote": {"netPercentChange": 8.5, "totalVolume": 1_000_000}}},
                classifications={},
                channels=[channel],
                earnings_today=df,
                price_cache={},
            )
            mock_build.assert_not_called()
            mock_send.assert_not_called()

    @pytest.mark.asyncio
    async def test_results_enrichment_passed_to_builder_when_available(self):
        """When earnings_results.get_result returns data, it is forwarded to build_earnings_mover."""
        df = pd.DataFrame({"ticker": ["AAPL"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        fake_trigger = _make_trigger_result(should_alert=True)
        cog.stock_data.earnings_results.get_result = AsyncMock(
            return_value={'eps_actual': 1.52, 'eps_estimate': 1.45, 'surprise_pct': 4.83}
        )

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_earnings_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock),
        ):
            mock_build.return_value = MagicMock()
            await cog._earnings_pipeline(
                quotes={"AAPL": {"quote": {"netPercentChange": 8.5, "totalVolume": 1_000_000}}},
                classifications={},
                channels=[AsyncMock()],
                earnings_today=df,
                price_cache={},
            )
            _, call_kwargs = mock_build.call_args
            assert call_kwargs.get('eps_actual') == pytest.approx(1.52)
            assert call_kwargs.get('eps_estimate') == pytest.approx(1.45)
            assert call_kwargs.get('surprise_pct') == pytest.approx(4.83)

    @pytest.mark.asyncio
    async def test_no_results_enrichment_when_not_available(self):
        """When get_result returns None, eps fields are not passed to build_earnings_mover."""
        df = pd.DataFrame({"ticker": ["AAPL"], "date": [datetime.date.today()]})
        cog = _make_cog(earnings_df=df)
        fake_trigger = _make_trigger_result(should_alert=True)
        cog.stock_data.earnings_results.get_result = AsyncMock(return_value=None)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_earnings_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock),
        ):
            mock_build.return_value = MagicMock()
            await cog._earnings_pipeline(
                quotes={"AAPL": {"quote": {"netPercentChange": 8.5, "totalVolume": 1_000_000}}},
                classifications={},
                channels=[AsyncMock()],
                earnings_today=df,
                price_cache={},
            )
            _, call_kwargs = mock_build.call_args
            assert 'eps_actual' not in call_kwargs
            assert 'eps_estimate' not in call_kwargs
            assert 'surprise_pct' not in call_kwargs


# ---------------------------------------------------------------------------
# Watchlist pipeline
# ---------------------------------------------------------------------------

class TestWatchlistPipeline:
    @pytest.mark.asyncio
    async def test_quote_passed_to_builder(self):
        """_watchlist_pipeline passes the pre-fetched quote to build_watchlist_mover."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "AAPL"
        quote = {"quote": {"netPercentChange": 15.0, "totalVolume": 1_000_000}}
        fake_trigger = _make_trigger_result(should_alert=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_watchlist_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock),
        ):
            mock_build.return_value = MagicMock()
            await cog._watchlist_pipeline(
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
                watchlist_tickers={ticker},
                price_cache={},
                ticker_to_watchlist={},
            )

        mock_build.assert_called_once()
        _, call_kwargs = mock_build.call_args
        assert call_kwargs.get("quote") == quote


# ---------------------------------------------------------------------------
# Volume accumulation pipeline
# ---------------------------------------------------------------------------

class TestVolumeAccumulationPipeline:
    def _make_price_df(self):
        """Return a minimal daily price history DataFrame with varying volume to avoid NaN z-scores."""
        n = 30
        close = [100.0 + i * 0.1 for i in range(n)]
        # Vary volumes so std > 0 and z-scores are finite
        volume = [900_000 + i * 10_000 for i in range(n)]
        return pd.DataFrame({
            'open': close, 'high': close, 'low': close, 'close': close,
            'volume': volume,
        })

    @pytest.mark.asyncio
    async def test_sends_alert_and_inserts_signal_when_accumulating(self):
        """Pipeline sends VolumeAccumulationAlert and inserts signal when detected."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        price_df = self._make_price_df()
        quote = {"quote": {"netPercentChange": 0.2, "totalVolume": 5_000_000},
                 "regular": {"regularMarketLastPrice": 52.0}}
        fake_result = _make_accumulation_result(is_accumulating=True)

        cog.stock_data.schwab.get_options_chain = AsyncMock(return_value=None)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_volume_accumulation", return_value=fake_result),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_send.return_value = None
            await cog._volume_accumulation_pipeline(
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
                price_cache={ticker: price_df},
            )
            mock_send.assert_called_once()

        cog.stock_data.market_signal_store.insert_signal.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_alert_when_not_accumulating(self):
        """No alert sent when volume divergence is not detected."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        price_df = self._make_price_df()
        quote = {"quote": {"netPercentChange": 0.2, "totalVolume": 100_000}}
        fake_result = _make_accumulation_result(is_accumulating=False)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_volume_accumulation", return_value=fake_result),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._volume_accumulation_pipeline(
                quotes={ticker: quote},
                classifications={},
                channels=[],
                price_cache={ticker: price_df},
            )
            mock_send.assert_not_called()

        cog.stock_data.market_signal_store.insert_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_already_signaled_ticker(self):
        """Ticker already signaled today is skipped."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        price_df = self._make_price_df()
        quote = {"quote": {"netPercentChange": 0.2, "totalVolume": 5_000_000}}
        fake_result = _make_accumulation_result(is_accumulating=True)
        # Already signaled today
        cog.stock_data.market_signal_store.get_signaled_tickers_today.return_value = {
            ticker: {'ticker': ticker, 'detected_at': datetime.datetime.utcnow()},
        }

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_volume_accumulation", return_value=fake_result),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._volume_accumulation_pipeline(
                quotes={ticker: quote},
                classifications={},
                channels=[],
                price_cache={ticker: price_df},
            )
            mock_send.assert_not_called()

        cog.stock_data.market_signal_store.insert_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_ticker_with_empty_price_history(self):
        """Tickers with no price history are skipped gracefully."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 0.2, "totalVolume": 5_000_000}}

        with patch("rocketstocks.bot.cogs.alerts.evaluate_volume_accumulation") as mock_eval:
            await cog._volume_accumulation_pipeline(
                quotes={ticker: quote},
                classifications={},
                channels=[],
                price_cache={},  # no price history
            )
            mock_eval.assert_not_called()


# ---------------------------------------------------------------------------
# Breakout pipeline
# ---------------------------------------------------------------------------

class TestBreakoutPipeline:
    @pytest.mark.asyncio
    async def test_sends_breakout_alert_when_confirmed(self):
        """_breakout_pipeline sends BreakoutAlert when price confirms."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 3.5, "totalVolume": 4_000_000},
                 "regular": {"regularMarketLastPrice": 55.0}}
        signal = {
            'ticker': ticker,
            'detected_at': datetime.datetime.utcnow() - datetime.timedelta(minutes=20),
            'price_at_flag': 50.0,
            'vol_z': 3.0,
            'composite_score': 2.8,
            'dominant_signal': 'volume_only',
            'rvol': 3.5,
            'alert_message_id': 111222333,
            'signal_data': [],
        }
        fake_confirmation = _make_confirmation_result(should_confirm=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_confirmation", return_value=fake_confirmation),
            patch.object(cog, "build_breakout", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_build.return_value = MagicMock()
            await cog._breakout_pipeline(
                active_signals=[signal],
                quotes={ticker: quote},
                channels=[AsyncMock()],
                price_cache={},
            )
            mock_build.assert_called_once()
            mock_send.assert_called_once()

        cog.stock_data.market_signal_store.mark_confirmed.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_alert_when_not_confirmed(self):
        """No alert sent when evaluate_confirmation returns False."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 0.1, "totalVolume": 500_000}}
        signal = {
            'ticker': ticker,
            'detected_at': datetime.datetime.utcnow() - datetime.timedelta(minutes=20),
            'price_at_flag': 50.0,
            'vol_z': 2.5,
            'composite_score': 2.2,
            'dominant_signal': 'volume_only',
            'rvol': 2.0,
            'alert_message_id': None,
            'signal_data': [],
        }
        fake_confirmation = _make_confirmation_result(should_confirm=False)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_confirmation", return_value=fake_confirmation),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._breakout_pipeline(
                active_signals=[signal],
                quotes={ticker: quote},
                channels=[],
                price_cache={},
            )
            mock_send.assert_not_called()

        cog.stock_data.market_signal_store.mark_confirmed.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_signal_within_10_min_delay(self):
        """Signals detected less than 10 min ago are skipped."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 3.0, "totalVolume": 4_000_000}}
        signal = {
            'ticker': ticker,
            'detected_at': datetime.datetime.utcnow() - datetime.timedelta(minutes=5),
            'price_at_flag': 50.0,
            'vol_z': 3.0,
            'composite_score': 2.8,
            'dominant_signal': 'volume_only',
            'rvol': 3.5,
            'alert_message_id': None,
            'signal_data': [],
        }

        with patch("rocketstocks.bot.cogs.alerts.evaluate_confirmation") as mock_eval:
            await cog._breakout_pipeline(
                active_signals=[signal],
                quotes={ticker: quote},
                channels=[],
                price_cache={},
            )
            mock_eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_ticker_not_in_quotes(self):
        """Signals whose ticker is not in quotes are skipped."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        signal = {
            'ticker': 'GME',
            'detected_at': datetime.datetime.utcnow() - datetime.timedelta(minutes=20),
            'price_at_flag': 50.0,
        }

        with patch("rocketstocks.bot.cogs.alerts.evaluate_confirmation") as mock_eval:
            await cog._breakout_pipeline(
                active_signals=[signal],
                quotes={},
                channels=[],
                price_cache={},
            )
            mock_eval.assert_not_called()


# ---------------------------------------------------------------------------
# Confirmation pipeline
# ---------------------------------------------------------------------------

class TestConfirmationPipeline:
    @pytest.mark.asyncio
    async def test_confirmation_fires_on_price_alert(self):
        """_confirmation_pipeline confirms a surge when evaluate_confirmation fires."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {
            "quote": {"netPercentChange": 12.0, "totalVolume": 5_000_000},
            "regular": {"regularMarketLastPrice": 27.0},
        }
        # flagged_at must be > 15 min ago to pass the delay gate
        surge = {
            "ticker": ticker,
            "flagged_at": datetime.datetime.utcnow() - datetime.timedelta(hours=1),
            "surge_types": "mention_surge,rank_jump",
            "price_at_flag": 25.0,
            "alert_message_id": 111222333,
        }
        fake_confirmation = _make_confirmation_result(should_confirm=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_confirmation", return_value=fake_confirmation),
            patch.object(cog, "build_momentum_confirmation", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_build.return_value = MagicMock()
            await cog._confirmation_pipeline(
                active_surges=[surge],
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
                price_cache={},
            )
            mock_build.assert_called_once()
            mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_confirmation_marks_surge_confirmed(self):
        """_confirmation_pipeline calls surge_store.mark_confirmed after alert."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        flagged_at = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        quote = {
            "quote": {"netPercentChange": 12.0, "totalVolume": 5_000_000},
            "regular": {"regularMarketLastPrice": 27.0},
        }
        surge = {
            "ticker": ticker,
            "flagged_at": flagged_at,
            "surge_types": "mention_surge",
            "price_at_flag": 25.0,
            "alert_message_id": None,
        }
        fake_confirmation = _make_confirmation_result(should_confirm=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_confirmation", return_value=fake_confirmation),
            patch.object(cog, "build_momentum_confirmation", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock),
        ):
            mock_build.return_value = MagicMock()
            await cog._confirmation_pipeline(
                active_surges=[surge],
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
                price_cache={},
            )

        cog.stock_data.surge_store.mark_confirmed.assert_called_once_with(ticker, flagged_at)

    @pytest.mark.asyncio
    async def test_confirmation_skips_when_no_trigger(self):
        """_confirmation_pipeline does not confirm when evaluate_confirmation returns False."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {
            "quote": {"netPercentChange": 0.5, "totalVolume": 1_000},
            "regular": {"regularMarketLastPrice": 25.0},
        }
        # flagged_at must be > 15 min ago to pass the delay gate
        surge = {
            "ticker": ticker,
            "flagged_at": datetime.datetime.utcnow() - datetime.timedelta(hours=1),
            "surge_types": "mention_surge",
            "price_at_flag": 25.0,
            "alert_message_id": None,
        }
        fake_confirmation = _make_confirmation_result(should_confirm=False)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_confirmation", return_value=fake_confirmation),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._confirmation_pipeline(
                active_surges=[surge],
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
                price_cache={},
            )
            mock_send.assert_not_called()
        cog.stock_data.surge_store.mark_confirmed.assert_not_called()

    @pytest.mark.asyncio
    async def test_confirmation_skips_when_ticker_not_in_quotes(self):
        """_confirmation_pipeline skips surges whose ticker is not in quotes."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        surge = {
            "ticker": "GME",
            "flagged_at": datetime.datetime.utcnow(),
            "surge_types": "mention_surge",
            "price_at_flag": 25.0,
            "alert_message_id": None,
        }
        with patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert") as mock_eval:
            await cog._confirmation_pipeline(
                active_surges=[surge],
                quotes={},  # GME not in quotes
                classifications={},
                channels=[],
                price_cache={},
            )
            mock_eval.assert_not_called()


# ---------------------------------------------------------------------------
# BuildWatchlistMover
# ---------------------------------------------------------------------------

class TestBuildWatchlistMover:
    @pytest.mark.asyncio
    async def test_quote_is_awaited(self):
        """build_watchlist_mover must await get_quote, not return a coroutine object."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        expected_quote = {"quote": {"netPercentChange": 5.0}}
        cog.stock_data.schwab.get_quote = AsyncMock(return_value=expected_quote)
        cog.stock_data.watchlists.get_ticker_to_watchlist_map = AsyncMock(return_value={})
        cog.stock_data.tickers.get_ticker_info.return_value = {}

        with (
            patch("rocketstocks.bot.cogs.alerts.WatchlistMoverData") as mock_data_cls,
            patch("rocketstocks.bot.cogs.alerts.WatchlistMoverAlert"),
        ):
            mock_data_cls.return_value = MagicMock()
            await cog.build_watchlist_mover(ticker="AAPL")

        cog.stock_data.schwab.get_quote.assert_called_once_with(ticker="AAPL")
        _, data_kwargs = mock_data_cls.call_args
        assert data_kwargs["quote"] == expected_quote


# ---------------------------------------------------------------------------
# Per-ticker isolation
# ---------------------------------------------------------------------------

class TestPerTickerIsolation:
    @pytest.mark.asyncio
    async def test_earnings_pipeline_isolation(self):
        """A single failing ticker in _earnings_pipeline does not abort the rest."""
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

        fake_trigger = _make_trigger_result(should_alert=True)
        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_earnings_mover", side_effect=mock_build),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._earnings_pipeline(
                quotes=quotes, classifications={}, channels=[AsyncMock()],
                earnings_today=df, price_cache={},
            )

        assert "MSFT" in built
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_watchlist_pipeline_isolation(self):
        """A single failing ticker in _watchlist_pipeline does not abort the rest."""
        cog = _make_cog(earnings_df=pd.DataFrame())
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

        fake_trigger = _make_trigger_result(should_alert=True)
        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_watchlist_mover", side_effect=mock_build),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._watchlist_pipeline(
                quotes=quotes, classifications={}, channels=[AsyncMock()],
                watchlist_tickers={"AAPL", "MSFT"},
                price_cache={}, ticker_to_watchlist={},
            )

        assert "MSFT" in built
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_volume_accumulation_pipeline_isolation(self):
        """A single failing ticker in _volume_accumulation_pipeline does not abort the rest."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        n = 30
        close = [100.0 + i * 0.1 for i in range(n)]
        volume = [900_000 + i * 10_000 for i in range(n)]
        price_df = pd.DataFrame({'open': close, 'high': close, 'low': close, 'close': close, 'volume': volume})
        quotes = {
            "AAPL": {"quote": {"netPercentChange": 0.2, "totalVolume": 5_000_000}, "regular": {"regularMarketLastPrice": 52.0}},
            "MSFT": {"quote": {"netPercentChange": 0.2, "totalVolume": 5_000_000}, "regular": {"regularMarketLastPrice": 52.0}},
        }
        fake_result = _make_accumulation_result(is_accumulating=False)

        # AAPL raises in evaluate_volume_accumulation, MSFT does not — pipeline must not abort
        with patch(
            "rocketstocks.bot.cogs.alerts.evaluate_volume_accumulation",
            side_effect=[RuntimeError("eval failed"), fake_result],
        ) as mock_eval:
            await cog._volume_accumulation_pipeline(
                quotes=quotes, classifications={}, channels=[AsyncMock()],
                price_cache={"AAPL": price_df, "MSFT": price_df},
            )

        # Both tickers were attempted despite the first one failing
        assert mock_eval.call_count == 2


# ---------------------------------------------------------------------------
# ProcessAlertsImpl
# ---------------------------------------------------------------------------

class TestProcessAlertsImpl:
    @pytest.mark.asyncio
    async def test_skips_when_market_closed(self):
        """process_alerts does nothing when market is closed."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = False
        cog.mutils.get_market_period.return_value = "closed"

        with patch.object(cog, "_volume_accumulation_pipeline", new_callable=AsyncMock) as mock_va:
            await cog._process_alerts_impl()

        mock_va.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_eod(self):
        """process_alerts does nothing at EOD."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "EOD"

        with patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock) as mock_earnings:
            await cog._process_alerts_impl()

        mock_earnings.assert_not_called()

    @pytest.mark.asyncio
    async def test_classifications_fetched_and_passed(self):
        """get_all_classifications is called and classifications passed to signal pipeline."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "intraday"
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={})
        expected_classifications = {'AAPL': 'blue_chip', 'GME': 'meme'}
        cog.stock_data.ticker_stats.get_all_classifications = AsyncMock(
            return_value=expected_classifications
        )

        received = []

        async def capture_classifications(*args, **kwargs):
            # _volume_accumulation_pipeline(quotes, classifications, channels, price_cache)
            received.append(args[1])

        with (
            patch.object(cog, "_confirmation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_volume_accumulation_pipeline", side_effect=capture_classifications),
            patch.object(cog, "_breakout_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock),
        ):
            await cog._process_alerts_impl()

        assert received[0] == expected_classifications

    @pytest.mark.asyncio
    async def test_surge_tickers_included_in_volume_accumulation_pipeline(self):
        """Surge tickers are NOT excluded from _volume_accumulation_pipeline — confluence is valuable."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "intraday"
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={"GME": {}})
        cog.stock_data.surge_store.get_active_surges.return_value = [
            {"ticker": "GME", "flagged_at": datetime.datetime.utcnow(), "surge_types": "mention_surge",
             "price_at_flag": 20.0, "alert_message_id": None}
        ]

        va_quotes = []

        async def capture_va_quotes(*args, **kwargs):
            # _volume_accumulation_pipeline(quotes, classifications, channels, price_cache)
            va_quotes.append(set(args[0].keys()))

        with (
            patch.object(cog, "_confirmation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_volume_accumulation_pipeline", side_effect=capture_va_quotes),
            patch.object(cog, "_breakout_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock),
        ):
            await cog._process_alerts_impl()

        # GME should be present in the volume accumulation pipeline (not excluded)
        assert 'GME' in va_quotes[0]

    @pytest.mark.asyncio
    async def test_all_pipelines_run_concurrently(self):
        """All five pipeline methods are invoked."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "intraday"
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={})
        cog.stock_data.ticker_stats.get_all_classifications = AsyncMock(return_value={})

        with (
            patch.object(cog, "_confirmation_pipeline", new_callable=AsyncMock) as mock_conf,
            patch.object(cog, "_volume_accumulation_pipeline", new_callable=AsyncMock) as mock_va,
            patch.object(cog, "_breakout_pipeline", new_callable=AsyncMock) as mock_breakout,
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock) as mock_watchlist,
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock) as mock_earnings,
        ):
            await cog._process_alerts_impl()

        mock_conf.assert_called_once()
        mock_va.assert_called_once()
        mock_breakout.assert_called_once()
        mock_watchlist.assert_called_once()
        mock_earnings.assert_called_once()

    @pytest.mark.asyncio
    async def test_exception_in_pipeline_logged_not_raised(self):
        """An exception in one gather sub-task is logged but does not raise."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "intraday"
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={})
        cog.stock_data.ticker_stats.get_all_classifications = AsyncMock(return_value={})

        with (
            patch.object(cog, "_confirmation_pipeline", new_callable=AsyncMock, side_effect=RuntimeError("boom")),
            patch.object(cog, "_volume_accumulation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_breakout_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock),
        ):
            # Must not raise
            await cog._process_alerts_impl()

    @pytest.mark.asyncio
    async def test_expire_surges_and_signals_called(self):
        """expire_old_surges and expire_old_signals are called at the end."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "intraday"
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={})
        cog.stock_data.ticker_stats.get_all_classifications = AsyncMock(return_value={})

        with (
            patch.object(cog, "_confirmation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_volume_accumulation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_breakout_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock),
        ):
            await cog._process_alerts_impl()

        cog.stock_data.surge_store.expire_old_surges.assert_called_once()
        cog.stock_data.market_signal_store.expire_old_signals.assert_called_once()


# ---------------------------------------------------------------------------
# RunTask
# ---------------------------------------------------------------------------

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

        await cog._run_task("bad_job", failing())

        cog.bot.emitter.emit.assert_called_once()
        event = cog.bot.emitter.emit.call_args[0][0]
        from rocketstocks.core.notifications.config import NotificationLevel
        assert event.level == NotificationLevel.FAILURE
        assert "something went wrong" in event.message

    @pytest.mark.asyncio
    async def test_no_reraise_on_exception(self):
        """_run_task must not re-raise exceptions."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        async def failing():
            raise RuntimeError("fatal")

        try:
            await cog._run_task("job", failing())
        except RuntimeError:
            pytest.fail("_run_task must not re-raise exceptions")


# ---------------------------------------------------------------------------
# DetectPopularitySurges
# ---------------------------------------------------------------------------

class TestDetectPopularitySurges:
    @pytest.mark.asyncio
    async def test_skips_when_market_closed(self):
        """_detect_popularity_surges_impl does nothing when market is closed."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = False

        await cog._detect_popularity_surges_impl()

        cog.stock_data.popularity.get_popular_stocks.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_already_flagged_tickers(self):
        """Tickers already flagged in surge_store are skipped."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])

        pop_df = pd.DataFrame({
            "ticker": ["GME"],
            "rank": [50],
            "rank_24h_ago": [200],
            "mentions": [3000],
            "mentions_24h_ago": [500],
        })
        cog.stock_data.popularity.get_popular_stocks.return_value = pop_df
        cog.stock_data.surge_store.get_flagged_tickers.return_value = {"GME"}

        with patch("rocketstocks.bot.cogs.alerts.evaluate_popularity_surge") as mock_surge:
            await cog._detect_popularity_surges_impl()
            mock_surge.assert_not_called()

    @pytest.mark.asyncio
    async def test_sends_alert_when_surge_detected(self):
        """PopularitySurgeAlert is sent when evaluate_popularity_surge returns is_surging=True."""
        from rocketstocks.core.analysis.popularity_signals import PopularitySurgeResult, SurgeType
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])

        pop_df = pd.DataFrame({
            "ticker": ["GME"],
            "rank": [50],
            "rank_24h_ago": [200],
            "mentions": [3000],
            "mentions_24h_ago": [500],
        })
        cog.stock_data.popularity.get_popular_stocks.return_value = pop_df
        cog.stock_data.surge_store.get_flagged_tickers.return_value = set()
        cog.stock_data.popularity.fetch_popularity.return_value = pd.DataFrame()
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={
            "GME": {
                "quote": {"netPercentChange": 5.0},
                "regular": {"regularMarketLastPrice": 25.0},
            }
        })
        cog.stock_data.tickers.get_ticker_info.return_value = {"name": "GameStop"}

        surge_result = PopularitySurgeResult(
            ticker="GME",
            is_surging=True,
            surge_types=[SurgeType.MENTION_SURGE],
            current_rank=50,
            rank_24h_ago=200,
            rank_change=150,
            mentions=3000,
            mentions_24h_ago=500,
            mention_ratio=6.0,
            rank_velocity=-10.0,
            rank_velocity_zscore=-2.5,
        )

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_popularity_surge", return_value=surge_result),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_send.return_value = MagicMock(id=999)
            await cog._detect_popularity_surges_impl()
            mock_send.assert_called_once()

        cog.stock_data.surge_store.insert_surge.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_alert_when_not_surging(self):
        """No alert is sent when is_surging=False."""
        from rocketstocks.core.analysis.popularity_signals import PopularitySurgeResult
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])

        pop_df = pd.DataFrame({
            "ticker": ["GME"],
            "rank": [50],
            "rank_24h_ago": [55],
            "mentions": [300],
            "mentions_24h_ago": [290],
        })
        cog.stock_data.popularity.get_popular_stocks.return_value = pop_df
        cog.stock_data.surge_store.get_flagged_tickers.return_value = set()
        cog.stock_data.popularity.fetch_popularity.return_value = pd.DataFrame()

        no_surge = PopularitySurgeResult(
            ticker="GME",
            is_surging=False,
            surge_types=[],
            current_rank=50,
            rank_24h_ago=55,
            rank_change=5,
            mentions=300,
            mentions_24h_ago=290,
            mention_ratio=1.03,
            rank_velocity=None,
            rank_velocity_zscore=None,
        )

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_popularity_surge", return_value=no_surge),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._detect_popularity_surges_impl()
            mock_send.assert_not_called()

        cog.stock_data.surge_store.insert_surge.assert_not_called()


# ---------------------------------------------------------------------------
# Before-loop timing
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Date header task
# ---------------------------------------------------------------------------

class TestPostAlertsDate:
    @pytest.mark.asyncio
    async def test_channel_send_failure_does_not_abort_remaining_channels(self):
        """One failing channel.send should not stop the header being sent to other channels."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        bad_channel = AsyncMock()
        bad_channel.id = 1
        bad_channel.send = AsyncMock(side_effect=Exception("Network error"))

        good_channel = AsyncMock()
        good_channel.id = 2
        good_channel.send = AsyncMock()

        cog.bot.iter_channels = AsyncMock(return_value=[(None, bad_channel), (None, good_channel)])
        cog.mutils.market_open_today.return_value = True

        with patch("rocketstocks.bot.cogs.alerts.format_date_mdy", return_value="Mon 03/16"):
            await cog._post_alerts_date_impl()

        bad_channel.send.assert_called_once()
        good_channel.send.assert_called_once()


class TestBeforeLoops:
    @pytest.mark.asyncio
    async def test_detect_surges_before_loop_calls_sleep(self):
        """detect_popularity_surges_before_loop sleeps until the next 30-min boundary."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        with (
            patch("rocketstocks.bot.cogs.alerts.seconds_until_minute_interval", return_value=42) as mock_sui,
            patch("rocketstocks.bot.cogs.alerts.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            await cog.detect_popularity_surges_before_loop()

        mock_sui.assert_called_once_with(30)
        mock_sleep.assert_awaited_once_with(42)
