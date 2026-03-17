"""Tests for rocketstocks.bot.cogs.alerts."""
import datetime
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

from rocketstocks.bot.cogs.alerts import Alerts
from rocketstocks.core.analysis.alert_strategy import AlertTriggerResult
from rocketstocks.core.analysis.classification import StockClass
from rocketstocks.core.analysis.composite_score import CompositeScoreResult


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.emitter = MagicMock()
    bot.iter_channels = AsyncMock(return_value=[])
    bot.stock_data.alert_roles.get_role_ids = AsyncMock(return_value=[])
    return bot


def _make_stock_data(earnings_df: pd.DataFrame):
    sd = MagicMock(name="StockData")
    sd.earnings.get_earnings_on_date = AsyncMock(return_value=earnings_df)
    sd.alert_tickers = {}
    sd.ticker_stats.get_all_classifications = AsyncMock(return_value={})
    sd.price_history.fetch_daily_price_history = AsyncMock(return_value=pd.DataFrame())
    sd.surge_store.get_active_surges = AsyncMock(return_value=[])
    sd.surge_store.expire_old_surges = AsyncMock(return_value=None)
    sd.surge_store.is_already_flagged = AsyncMock(return_value=False)
    sd.surge_store.insert_surge = AsyncMock()
    sd.market_signal_store.get_active_signals = AsyncMock(return_value=[])
    sd.market_signal_store.expire_old_signals = AsyncMock(return_value=None)
    sd.market_signal_store.is_already_signaled = AsyncMock(return_value=False)
    sd.market_signal_store.get_latest_signal = AsyncMock(return_value=None)
    sd.market_signal_store.get_signal_history = AsyncMock(return_value=[])
    sd.watchlists.get_all_watchlist_tickers = AsyncMock(return_value=[])
    sd.watchlists.get_watchlists = AsyncMock(return_value=[])
    sd.tickers.get_ticker_info = AsyncMock(return_value={})
    sd.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())
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


def _make_composite_result(should_alert=True):
    return CompositeScoreResult(
        composite_score=3.1 if should_alert else 1.0,
        should_alert=should_alert,
        volume_component=4.2,
        price_component=2.8,
        cross_signal_component=0.0,
        classification_component=0.0,
        trigger_result=_make_trigger_result(should_alert=should_alert),
        dominant_signal='volume',
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
            earnings_tickers=set(),
        )

    @pytest.mark.asyncio
    async def test_no_matching_tickers_sends_no_alerts(self):
        """Tickers in quotes not in earnings_tickers produce no alerts."""
        cog = _make_cog(earnings_df=pd.DataFrame({"ticker": ["MSFT"], "date": [datetime.date.today()]}))
        channel = AsyncMock()
        await cog._earnings_pipeline(
            quotes={"AAPL": {"quote": {"netPercentChange": 10.0, "totalVolume": 1_000_000}}},
            classifications={},
            channels=[channel],
            earnings_tickers={"MSFT"},
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
                earnings_tickers={"AAPL"},
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
                earnings_tickers={"AAPL"},
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
                earnings_tickers={"AAPL"},
            )
            mock_build.assert_not_called()
            mock_send.assert_not_called()


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
            )

        mock_build.assert_called_once()
        _, call_kwargs = mock_build.call_args
        assert call_kwargs.get("quote") == quote


# ---------------------------------------------------------------------------
# Market signal pipeline (silent recording)
# ---------------------------------------------------------------------------

class TestMarketSignalPipeline:
    @pytest.mark.asyncio
    async def test_records_signal_when_composite_triggers(self):
        """_market_signal_pipeline inserts a new signal when composite passes."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 15.0, "totalVolume": 5_000_000},
                 "regular": {"regularMarketLastPrice": 25.0}}
        fake_trigger = _make_trigger_result(should_alert=True)
        fake_composite = _make_composite_result(should_alert=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch("rocketstocks.bot.cogs.alerts.compute_composite_score", return_value=fake_composite),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._market_signal_pipeline(
                quotes={ticker: quote},
                classifications={},
                exclude=set(),
            )
            # Silent — no alert sent
            mock_send.assert_not_called()

        # Signal should be recorded
        cog.stock_data.market_signal_store.insert_signal.assert_called_once()

    @pytest.mark.asyncio
    async def test_updates_observation_when_already_signaled(self):
        """_market_signal_pipeline updates existing signal when ticker already signaled."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 15.0, "totalVolume": 5_000_000}}
        fake_trigger = _make_trigger_result(should_alert=True)
        fake_composite = _make_composite_result(should_alert=True)

        existing_signal = {
            'ticker': ticker,
            'detected_at': datetime.datetime.utcnow(),
            'composite_score': 3.0,
        }
        cog.stock_data.market_signal_store.is_already_signaled.return_value = True
        cog.stock_data.market_signal_store.get_latest_signal.return_value = existing_signal

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch("rocketstocks.bot.cogs.alerts.compute_composite_score", return_value=fake_composite),
        ):
            await cog._market_signal_pipeline(
                quotes={ticker: quote},
                classifications={},
                exclude=set(),
            )

        cog.stock_data.market_signal_store.insert_signal.assert_not_called()
        cog.stock_data.market_signal_store.update_observation.assert_called_once()

    @pytest.mark.asyncio
    async def test_excluded_tickers_skipped(self):
        """Tickers in the exclude set are not processed."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 15.0, "totalVolume": 5_000_000}}

        with patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert") as mock_eval:
            await cog._market_signal_pipeline(
                quotes={ticker: quote},
                classifications={},
                exclude={ticker},
            )
            mock_eval.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_record_when_composite_fails(self):
        """No signal recorded when composite_result.should_alert=False."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 1.0, "totalVolume": 100_000}}
        fake_trigger = _make_trigger_result(should_alert=False)
        fake_composite = _make_composite_result(should_alert=False)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch("rocketstocks.bot.cogs.alerts.compute_composite_score", return_value=fake_composite),
        ):
            await cog._market_signal_pipeline(
                quotes={ticker: quote},
                classifications={},
                exclude=set(),
            )

        cog.stock_data.market_signal_store.insert_signal.assert_not_called()


# ---------------------------------------------------------------------------
# Market confirmation pipeline
# ---------------------------------------------------------------------------

class TestMarketConfirmationPipeline:
    @pytest.mark.asyncio
    async def test_sends_mover_alert_when_confirmed(self):
        """_market_confirmation_pipeline sends Market Mover when signal confirmed."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 5.0, "totalVolume": 3_000_000},
                 "regular": {"regularMarketLastPrice": 25.0}}
        signal = {
            'ticker': ticker,
            'detected_at': datetime.datetime.utcnow() - datetime.timedelta(minutes=10),
            'pct_change': 4.0,
            'vol_z': 2.0,
            'composite_score': 3.0,
            'dominant_signal': 'volume',
            'rvol': 2.5,
        }
        fake_trigger = _make_trigger_result(should_alert=True)
        fake_composite = _make_composite_result(should_alert=True)
        cog.stock_data.market_signal_store.get_signal_history.return_value = [
            {'ts': 'a', 'pct_change': 4.0, 'vol_z': 2.0, 'price_z': 1.8, 'composite': 3.0},
            {'ts': 'b', 'pct_change': 4.5, 'vol_z': 2.2, 'price_z': 1.9, 'composite': 3.1},
        ]

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch("rocketstocks.bot.cogs.alerts.compute_composite_score", return_value=fake_composite),
            patch("rocketstocks.bot.cogs.alerts.should_confirm_signal", return_value=(True, 'sustained')),
            patch.object(cog, "build_market_mover", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_build.return_value = MagicMock()
            await cog._market_confirmation_pipeline(
                active_signals=[signal],
                quotes={ticker: quote},
                channels=[AsyncMock()],
            )
            mock_build.assert_called_once()
            mock_send.assert_called_once()

        cog.stock_data.market_signal_store.mark_confirmed.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_alert_when_not_confirmed(self):
        """No alert sent when should_confirm_signal returns False."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {"quote": {"netPercentChange": 2.0, "totalVolume": 500_000}}
        signal = {
            'ticker': ticker,
            'detected_at': datetime.datetime.utcnow(),
            'pct_change': 2.0,
            'vol_z': 1.5,
            'composite_score': 2.6,
            'dominant_signal': 'mixed',
            'rvol': None,
        }
        fake_trigger = _make_trigger_result(should_alert=False)
        cog.stock_data.market_signal_store.get_signal_history.return_value = []

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch("rocketstocks.bot.cogs.alerts.should_confirm_signal", return_value=(False, '')),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._market_confirmation_pipeline(
                active_signals=[signal],
                quotes={ticker: quote},
                channels=[AsyncMock()],
            )
            mock_send.assert_not_called()

        cog.stock_data.market_signal_store.mark_confirmed.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_ticker_not_in_quotes(self):
        """Signals whose ticker is not in quotes are skipped."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        signal = {
            'ticker': 'GME',
            'detected_at': datetime.datetime.utcnow(),
            'pct_change': 2.0,
        }

        with patch("rocketstocks.bot.cogs.alerts.should_confirm_signal") as mock_conf:
            await cog._market_confirmation_pipeline(
                active_signals=[signal],
                quotes={},
                channels=[],
            )
            mock_conf.assert_not_called()


# ---------------------------------------------------------------------------
# Confirmation pipeline
# ---------------------------------------------------------------------------

class TestConfirmationPipeline:
    @pytest.mark.asyncio
    async def test_confirmation_fires_on_price_alert(self):
        """_confirmation_pipeline confirms a surge when price alert triggers."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {
            "quote": {"netPercentChange": 12.0, "totalVolume": 5_000_000},
            "regular": {"regularMarketLastPrice": 27.0},
        }
        surge = {
            "ticker": ticker,
            "flagged_at": datetime.datetime.utcnow() - datetime.timedelta(hours=1),
            "surge_types": "mention_surge,rank_jump",
            "price_at_flag": 25.0,
            "alert_message_id": 111222333,
        }
        fake_trigger = _make_trigger_result(should_alert=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_momentum_confirmation", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            mock_build.return_value = MagicMock()
            await cog._confirmation_pipeline(
                active_surges=[surge],
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
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
        fake_trigger = _make_trigger_result(should_alert=True)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch.object(cog, "build_momentum_confirmation", new_callable=AsyncMock) as mock_build,
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock),
        ):
            mock_build.return_value = MagicMock()
            await cog._confirmation_pipeline(
                active_surges=[surge],
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
            )

        cog.stock_data.surge_store.mark_confirmed.assert_called_once_with(ticker, flagged_at)

    @pytest.mark.asyncio
    async def test_confirmation_skips_when_no_trigger(self):
        """_confirmation_pipeline does not confirm if price alert doesn't fire."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        ticker = "GME"
        quote = {
            "quote": {"netPercentChange": 0.5, "totalVolume": 1_000},
            "regular": {"regularMarketLastPrice": 25.0},
        }
        surge = {
            "ticker": ticker,
            "flagged_at": datetime.datetime.utcnow(),
            "surge_types": "mention_surge",
            "price_at_flag": 25.0,
            "alert_message_id": None,
        }
        fake_trigger = _make_trigger_result(should_alert=False)

        with (
            patch("rocketstocks.bot.cogs.alerts.evaluate_price_alert", return_value=fake_trigger),
            patch("rocketstocks.bot.cogs.alerts.send_alert", new_callable=AsyncMock) as mock_send,
        ):
            await cog._confirmation_pipeline(
                active_surges=[surge],
                quotes={ticker: quote},
                classifications={},
                channels=[AsyncMock()],
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
        cog.stock_data.watchlists.get_watchlists.return_value = {}
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
                earnings_tickers={"AAPL", "MSFT"},
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
            )

        assert "MSFT" in built
        mock_send.assert_called_once()

    @pytest.mark.asyncio
    async def test_market_signal_pipeline_isolation(self):
        """A single failing ticker in _market_signal_pipeline does not abort the rest."""
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
            return pd.DataFrame()

        cog.stock_data.price_history.fetch_daily_price_history.side_effect = fetch_daily_side_effect

        await cog._market_signal_pipeline(
            quotes=quotes, classifications={}, exclude=set()
        )

        assert "MSFT" in processed


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

        with patch.object(cog, "_market_signal_pipeline", new_callable=AsyncMock) as mock_market:
            await cog._process_alerts_impl()

        mock_market.assert_not_called()

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

        async def capture_classifications(quotes, classifications, exclude=None):
            received.append(classifications)

        with (
            patch.object(cog, "_confirmation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_market_signal_pipeline", side_effect=capture_classifications),
            patch.object(cog, "_market_confirmation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock),
        ):
            await cog._process_alerts_impl()

        assert received[0] == expected_classifications

    @pytest.mark.asyncio
    async def test_surge_tickers_excluded_from_market_signal_pipeline(self):
        """Active surge tickers are excluded from _market_signal_pipeline."""
        cog = _make_cog(earnings_df=pd.DataFrame())
        cog.mutils.market_open_today.return_value = True
        cog.mutils.get_market_period.return_value = "intraday"
        cog.bot.iter_channels = AsyncMock(return_value=[(1, MagicMock())])
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={})
        cog.stock_data.surge_store.get_active_surges.return_value = [
            {"ticker": "GME", "flagged_at": datetime.datetime.utcnow(), "surge_types": "mention_surge",
             "price_at_flag": 20.0, "alert_message_id": None}
        ]

        market_exclude = []

        async def capture_market_exclude(quotes, classifications, exclude=None):
            market_exclude.append(exclude or set())

        with (
            patch.object(cog, "_confirmation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_market_signal_pipeline", side_effect=capture_market_exclude),
            patch.object(cog, "_market_confirmation_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock),
        ):
            await cog._process_alerts_impl()

        assert 'GME' in market_exclude[0]

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
            patch.object(cog, "_market_signal_pipeline", new_callable=AsyncMock) as mock_signal,
            patch.object(cog, "_market_confirmation_pipeline", new_callable=AsyncMock) as mock_mover,
            patch.object(cog, "_watchlist_pipeline", new_callable=AsyncMock) as mock_watchlist,
            patch.object(cog, "_earnings_pipeline", new_callable=AsyncMock) as mock_earnings,
        ):
            await cog._process_alerts_impl()

        mock_conf.assert_called_once()
        mock_signal.assert_called_once()
        mock_mover.assert_called_once()
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
            patch.object(cog, "_market_signal_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_market_confirmation_pipeline", new_callable=AsyncMock),
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
            patch.object(cog, "_market_signal_pipeline", new_callable=AsyncMock),
            patch.object(cog, "_market_confirmation_pipeline", new_callable=AsyncMock),
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
        cog.stock_data.surge_store.is_already_flagged.return_value = True

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
        cog.stock_data.surge_store.is_already_flagged.return_value = False
        cog.stock_data.popularity.fetch_popularity.return_value = pd.DataFrame()
        cog.stock_data.schwab.get_quote = AsyncMock(return_value={
            "quote": {"netPercentChange": 5.0},
            "regular": {"regularMarketLastPrice": 25.0},
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
        cog.stock_data.surge_store.is_already_flagged.return_value = False
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

        with patch("rocketstocks.bot.cogs.alerts.date_utils") as mock_du:
            mock_du.format_date_mdy.return_value = "Mon 03/16"
            await cog._post_alerts_date_impl()

        bad_channel.send.assert_called_once()
        good_channel.send.assert_called_once()


class TestBeforeLoops:
    @pytest.mark.asyncio
    async def test_detect_surges_before_loop_calls_sleep(self):
        """detect_popularity_surges_before_loop sleeps until the next 30-min boundary."""
        cog = _make_cog(earnings_df=pd.DataFrame())

        with (
            patch("rocketstocks.bot.cogs.alerts.date_utils") as mock_du,
            patch("rocketstocks.bot.cogs.alerts.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            mock_du.seconds_until_minute_interval.return_value = 42
            await cog.detect_popularity_surges_before_loop()

        mock_du.seconds_until_minute_interval.assert_called_once_with(30)
        mock_sleep.assert_awaited_once_with(42)
