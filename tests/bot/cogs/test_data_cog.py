"""Tests for rocketstocks.bot.cogs.data — Data cog commands."""
import pytest
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch

import discord

from rocketstocks.bot.cogs.data import Data
from rocketstocks.data.clients.schwab import SchwabTokenError


def _make_bot():
    bot = MagicMock(name="Bot")
    bot.get_channel_for_guild = MagicMock(return_value=None)
    return bot


def _make_cog():
    bot = _make_bot()
    sd = MagicMock(name="StockData")
    sd.tickers = MagicMock()
    sd.earnings = MagicMock()
    sd.price_history = MagicMock()
    sd.popularity = MagicMock()
    sd.schwab = MagicMock()
    sd.ticker_stats = MagicMock()
    sd.yfinance = MagicMock()
    return Data(bot=bot, stock_data=sd)


def _make_interaction():
    interaction = MagicMock(spec=discord.Interaction)
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()
    interaction.user = MagicMock()
    interaction.user.name = "testuser"
    dm_message = MagicMock()
    dm_message.jump_url = "https://discord.com/channels/1/2/3"
    interaction.user.send = AsyncMock(return_value=dm_message)
    interaction.guild_id = 12345
    return interaction


# ---------------------------------------------------------------------------
# TestDataPrice (renamed from /data csv)
# ---------------------------------------------------------------------------

class TestDataPrice:
    @pytest.mark.asyncio
    async def test_valid_ticker_sends_file_to_dm_and_followup(self):
        cog = _make_cog()
        interaction = _make_interaction()

        df = MagicMock()
        df.empty = False
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=df)

        frequency = MagicMock()
        frequency.value = "daily"

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock()), \
             patch("rocketstocks.bot.cogs.data.discord.File", return_value=MagicMock()), \
             patch("rocketstocks.bot.cogs.data.PriceSnapshot") as mock_snap:
            mock_snap.return_value.build.return_value = MagicMock()
            with patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
                await cog.data_price.callback(cog, interaction, tickers="AAPL", frequency=frequency)

        interaction.user.send.assert_called_once()
        interaction.followup.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_dm_forbidden_sends_ephemeral_fallback(self):
        cog = _make_cog()
        interaction = _make_interaction()

        df = MagicMock()
        df.empty = False
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.price_history.fetch_daily_price_history = AsyncMock(return_value=df)
        interaction.user.send = AsyncMock(side_effect=discord.Forbidden(MagicMock(), "Cannot send DMs"))

        frequency = MagicMock()
        frequency.value = "daily"

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock()), \
             patch("rocketstocks.bot.cogs.data.discord.File", return_value=MagicMock()), \
             patch("rocketstocks.bot.cogs.data.PriceSnapshot") as mock_snap:
            mock_snap.return_value.build.return_value = MagicMock()
            with patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
                await cog.data_price.callback(cog, interaction, tickers="AAPL", frequency=frequency)

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert call_kwargs.get("ephemeral") is True
        assert "DM" in interaction.followup.send.call_args[0][0]

    @pytest.mark.asyncio
    async def test_invalid_ticker_skips_dm_and_sends_error_followup(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=([], ["INVALID"]))

        frequency = MagicMock()
        frequency.value = "daily"

        await cog.data_price.callback(cog, interaction, tickers="INVALID", frequency=frequency)

        interaction.user.send.assert_not_called()
        interaction.followup.send.assert_called_once()
        message = interaction.followup.send.call_args[0][0]
        assert "Could not fetch" in message
        assert "INVALID" in message


# ---------------------------------------------------------------------------
# TestDataEarnings
# ---------------------------------------------------------------------------

class TestDataEarnings:
    @pytest.mark.asyncio
    async def test_empty_eps_private_no_name_error(self):
        """Regression 1a/1e: eps.empty=True + private must not raise NameError on `file`."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.earnings.get_historical_earnings = AsyncMock(return_value=pd.DataFrame())

        visibility = MagicMock()
        visibility.value = "private"

        # Must not raise NameError
        await cog.data_earnings.callback(cog, interaction, tickers="AAPL", visibility=visibility)

        interaction.user.send.assert_called_once()
        call_kwargs = interaction.user.send.call_args.kwargs
        # files must be empty list, not [None]
        assert call_kwargs.get("files") == []

    @pytest.mark.asyncio
    async def test_public_no_channel_configured_returns_early(self):
        """Public visibility with no reports channel → early return with setup message."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.earnings.get_historical_earnings = AsyncMock(return_value=pd.DataFrame())
        cog.bot.get_channel_for_guild = MagicMock(return_value=None)

        visibility = MagicMock()
        visibility.value = "public"

        await cog.data_earnings.callback(cog, interaction, tickers="AAPL", visibility=visibility)

        call_args = interaction.followup.send.call_args[0][0]
        assert "setup" in call_args.lower()


# ---------------------------------------------------------------------------
# TestDataFundamentals
# ---------------------------------------------------------------------------

class TestDataFundamentals:
    @pytest.mark.asyncio
    async def test_each_ticker_passes_single_ticker_list(self):
        """Regression 1b: each loop iteration must call get_fundamentals([ticker]), not full list."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL", "MSFT"], []))
        cog.stock_data.schwab.get_fundamentals = AsyncMock(return_value={"data": "test"})

        with patch("rocketstocks.bot.cogs.data._write_json"), \
             patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock()), \
             patch("rocketstocks.bot.cogs.data.discord.File", return_value=MagicMock()):
            await cog.data_fundamentals.callback(cog, interaction, tickers="AAPL MSFT")

        calls = cog.stock_data.schwab.get_fundamentals.call_args_list
        assert len(calls) == 2
        assert calls[0].kwargs.get("tickers") == ["AAPL"]
        assert calls[1].kwargs.get("tickers") == ["MSFT"]

    @pytest.mark.asyncio
    async def test_empty_fundamentals_no_name_error(self):
        """Regression 1a: falsy fundamentals must not raise NameError on `file`."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_fundamentals = AsyncMock(return_value=None)

        await cog.data_fundamentals.callback(cog, interaction, tickers="AAPL")

        interaction.user.send.assert_called_once()
        # file kwarg should not be present when fundamentals is falsy
        call_kwargs = interaction.user.send.call_args.kwargs
        assert "file" not in call_kwargs

    @pytest.mark.asyncio
    async def test_schwab_token_error_returns_auth_message(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_fundamentals = AsyncMock(side_effect=SchwabTokenError("token invalid"))

        await cog.data_fundamentals.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        message = interaction.followup.send.call_args[0][0]
        assert "schwab" in message.lower() or "auth" in message.lower()


# ---------------------------------------------------------------------------
# TestDataOptions
# ---------------------------------------------------------------------------

class TestDataOptions:
    @pytest.mark.asyncio
    async def test_empty_options_no_name_error(self):
        """Regression 1a: falsy options must not raise NameError on `file`."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_options_chain = AsyncMock(return_value=None)

        await cog.data_options.callback(cog, interaction, tickers="AAPL")

        interaction.user.send.assert_called_once()
        call_kwargs = interaction.user.send.call_args.kwargs
        assert call_kwargs.get("file") is None

    @pytest.mark.asyncio
    async def test_valid_options_sends_file(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_options_chain = AsyncMock(return_value={"option": "data"})

        with patch("rocketstocks.bot.cogs.data._write_json"), \
             patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock()), \
             patch("rocketstocks.bot.cogs.data.discord.File", return_value=MagicMock()) as mock_file:
            await cog.data_options.callback(cog, interaction, tickers="AAPL")

        interaction.user.send.assert_called_once()
        call_kwargs = interaction.user.send.call_args.kwargs
        assert call_kwargs.get("file") is mock_file.return_value

    @pytest.mark.asyncio
    async def test_schwab_token_error_returns_auth_message(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_options_chain = AsyncMock(side_effect=SchwabTokenError("token invalid"))

        await cog.data_options.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        message = interaction.followup.send.call_args[0][0]
        assert "schwab" in message.lower() or "auth" in message.lower()


# ---------------------------------------------------------------------------
# TestDataPopularity
# ---------------------------------------------------------------------------

class TestDataPopularity:
    @pytest.mark.asyncio
    async def test_empty_data_no_name_error(self):
        """Regression 1a: empty popularity DataFrame must not raise NameError on `file`."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.popularity.fetch_popularity = AsyncMock(return_value=pd.DataFrame())

        await cog.data_popularity.callback(cog, interaction, tickers="AAPL")

        interaction.user.send.assert_called_once()
        call_kwargs = interaction.user.send.call_args.kwargs
        assert call_kwargs.get("file") is None

    @pytest.mark.asyncio
    async def test_valid_data_sends_file(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        df = MagicMock()
        df.empty = False
        cog.stock_data.popularity.fetch_popularity = AsyncMock(return_value=df)

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.discord.File", return_value=MagicMock()) as mock_file, \
             patch("rocketstocks.bot.cogs.data.PopularitySnapshot") as mock_snap, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_snap.return_value.build.return_value = MagicMock()
            await cog.data_popularity.callback(cog, interaction, tickers="AAPL")

        interaction.user.send.assert_called_once()
        call_kwargs = interaction.user.send.call_args.kwargs
        assert call_kwargs.get("file") is mock_file.return_value


# ---------------------------------------------------------------------------
# TestDataTickers
# ---------------------------------------------------------------------------

class TestDataTickers:
    @pytest.mark.asyncio
    async def test_defer_called_before_dm_send(self):
        """Regression 1d: defer must be called before DM send to avoid 3-second timeout."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.get_all_ticker_info = AsyncMock(return_value=MagicMock())

        call_order = []

        def track_defer(*args, **kwargs):
            call_order.append("defer")

        def track_send(*args, **kwargs):
            call_order.append("send")
            return MagicMock()

        interaction.response.defer.side_effect = track_defer
        interaction.user.send.side_effect = track_send

        with patch("rocketstocks.bot.cogs.data.discord.File", return_value=MagicMock()):
            await cog.data_tickers.callback(cog, interaction)

        assert "defer" in call_order
        assert "send" in call_order
        assert call_order.index("defer") < call_order.index("send")

    @pytest.mark.asyncio
    async def test_uses_followup_not_response_send_message(self):
        """Regression 1d: must use followup.send, not response.send_message."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.get_all_ticker_info = AsyncMock(return_value=MagicMock())

        with patch("rocketstocks.bot.cogs.data.discord.File", return_value=MagicMock()):
            await cog.data_tickers.callback(cog, interaction)

        interaction.followup.send.assert_called_once()
        interaction.response.send_message.assert_not_called()


# ---------------------------------------------------------------------------
# TestDataQuote
# ---------------------------------------------------------------------------

class TestDataQuote:
    @pytest.mark.asyncio
    async def test_valid_tickers_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_quotes = AsyncMock(return_value={
            "AAPL": {
                "quote": {
                    "bidPrice": 185.4, "askPrice": 185.5,
                    "openPrice": 183.0, "highPrice": 186.0, "lowPrice": 182.5,
                    "totalVolume": 50_000_000, "netChange": 1.5, "netPercentChange": 0.82,
                },
                "regular": {"regularMarketLastPrice": 185.45},
            }
        })

        await cog.data_quote.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert "embed" in call_kwargs
        assert call_kwargs.get("ephemeral") is True

    @pytest.mark.asyncio
    async def test_schwab_token_error_returns_auth_message(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_quotes = AsyncMock(side_effect=SchwabTokenError("token invalid"))

        await cog.data_quote.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        message = interaction.followup.send.call_args[0][0]
        assert "schwab" in message.lower() or "auth" in message.lower()


# ---------------------------------------------------------------------------
# TestDataUpcomingEarnings
# ---------------------------------------------------------------------------

class TestDataUpcomingEarnings:
    @pytest.mark.asyncio
    async def test_ticker_with_data_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.earnings.get_next_earnings_info = AsyncMock(return_value={
            "date": "2026-04-01",
            "time": "after",
            "eps_forecast": "1.50",
            "no_of_ests": 20,
            "last_year_eps": "1.29",
            "fiscal_quarter_ending": "Mar 2026",
            "last_year_rpt_dt": "2025-04-01",
            "ticker": "AAPL",
        })

        await cog.data_upcoming_earnings.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert "embed" in call_kwargs

    @pytest.mark.asyncio
    async def test_ticker_with_no_upcoming_earnings_shows_message_in_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.earnings.get_next_earnings_info = AsyncMock(return_value=None)

        await cog.data_upcoming_earnings.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert "embed" in call_kwargs
        embed = call_kwargs["embed"]
        assert any("No upcoming earnings" in str(f.value) for f in embed.fields)


# ---------------------------------------------------------------------------
# TestDataStats
# ---------------------------------------------------------------------------

class TestDataStats:
    @pytest.mark.asyncio
    async def test_ticker_with_stats_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value={
            "ticker": "AAPL",
            "market_cap": 3_000_000_000_000,
            "classification": "mega_cap",
            "volatility_20d": 0.015,
            "mean_return_20d": 0.001,
            "std_return_20d": 0.012,
            "mean_return_60d": 0.0008,
            "std_return_60d": 0.013,
            "avg_rvol_20d": 1.2,
            "std_rvol_20d": 0.3,
            "bb_upper": 192.0,
            "bb_mid": 185.0,
            "bb_lower": 178.0,
            "updated_at": "2026-03-15",
        })

        await cog.data_stats.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert "embed" in call_kwargs

    @pytest.mark.asyncio
    async def test_ticker_with_no_stats_sends_graceful_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.ticker_stats.get_stats = AsyncMock(return_value=None)

        await cog.data_stats.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert "embed" in call_kwargs
        embed = call_kwargs["embed"]
        assert any("No stats" in str(f.value) for f in embed.fields)


# ---------------------------------------------------------------------------
# TestDataMovers
# ---------------------------------------------------------------------------

class TestDataMovers:
    @pytest.mark.asyncio
    async def test_movers_returned_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.schwab.get_movers = AsyncMock(return_value={
            "screeners": [
                {"symbol": "GME", "lastPrice": 25.5, "percentChange": 15.0, "totalVolume": 10_000_000},
                {"symbol": "AMC", "lastPrice": 4.20, "percentChange": 8.5, "totalVolume": 5_000_000},
            ]
        })

        await cog.data_movers.callback(cog, interaction)

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert "embed" in call_kwargs

    @pytest.mark.asyncio
    async def test_empty_screeners_sends_embed_with_no_data_message(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.schwab.get_movers = AsyncMock(return_value={"screeners": []})

        await cog.data_movers.callback(cog, interaction)

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert "embed" in call_kwargs
        embed = call_kwargs["embed"]
        assert "No mover data" in (embed.description or "")

    @pytest.mark.asyncio
    async def test_schwab_token_error_returns_auth_message(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.schwab.get_movers = AsyncMock(side_effect=SchwabTokenError("token invalid"))

        await cog.data_movers.callback(cog, interaction)

        interaction.followup.send.assert_called_once()
        message = interaction.followup.send.call_args[0][0]
        assert "schwab" in message.lower() or "auth" in message.lower()


# ---------------------------------------------------------------------------
# Phase 3 command tests
# ---------------------------------------------------------------------------

class TestDataAnalyst:
    @pytest.mark.asyncio
    async def test_valid_ticker_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.yfinance = MagicMock()

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(return_value=None)), \
             patch("rocketstocks.bot.cogs.data.AnalystCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_analyst.callback(cog, interaction, ticker="AAPL")

        interaction.followup.send.assert_called_once()
        call_kwargs = interaction.followup.send.call_args.kwargs
        assert call_kwargs.get("embed") is mock_embed

    @pytest.mark.asyncio
    async def test_invalid_ticker_sends_error(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=([], ["BAD"]))

        await cog.data_analyst.callback(cog, interaction, ticker="BAD")

        interaction.followup.send.assert_called_once()
        msg = interaction.followup.send.call_args[0][0]
        assert "BAD" in msg


class TestDataOwnership:
    @pytest.mark.asyncio
    async def test_valid_ticker_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.yfinance = MagicMock()

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(return_value=pd.DataFrame())), \
             patch("rocketstocks.bot.cogs.data.OwnershipCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_ownership.callback(cog, interaction, ticker="AAPL")

        interaction.followup.send.assert_called_once()
        assert interaction.followup.send.call_args.kwargs.get("embed") is mock_embed

    @pytest.mark.asyncio
    async def test_invalid_ticker_sends_error(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=([], ["BAD"]))

        await cog.data_ownership.callback(cog, interaction, ticker="BAD")

        msg = interaction.followup.send.call_args[0][0]
        assert "BAD" in msg


class TestDataInsider:
    @pytest.mark.asyncio
    async def test_valid_ticker_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.yfinance = MagicMock()

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(return_value=pd.DataFrame())), \
             patch("rocketstocks.bot.cogs.data.InsiderCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_insider.callback(cog, interaction, ticker="AAPL")

        interaction.followup.send.assert_called_once()
        assert interaction.followup.send.call_args.kwargs.get("embed") is mock_embed

    @pytest.mark.asyncio
    async def test_invalid_ticker_sends_error(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=([], ["BAD"]))

        await cog.data_insider.callback(cog, interaction, ticker="BAD")

        msg = interaction.followup.send.call_args[0][0]
        assert "BAD" in msg


class TestDataShortInterest:
    @pytest.mark.asyncio
    async def test_valid_ticker_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_fundamentals = AsyncMock(return_value={
            'instruments': [{'fundamental': {'shortInterestToFloat': 2.5, 'shortInterestShares': 85_000_000}}]
        })

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.ShortInterestCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_short_interest.callback(cog, interaction, ticker="AAPL")

        interaction.followup.send.assert_called_once()
        assert interaction.followup.send.call_args.kwargs.get("embed") is mock_embed

    @pytest.mark.asyncio
    async def test_schwab_token_error_sends_message(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.schwab.get_fundamentals = AsyncMock(side_effect=SchwabTokenError("no token"))

        await cog.data_short_interest.callback(cog, interaction, ticker="AAPL")

        interaction.followup.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_ticker_sends_error(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=([], ["BAD"]))

        await cog.data_short_interest.callback(cog, interaction, ticker="BAD")

        msg = interaction.followup.send.call_args[0][0]
        assert "BAD" in msg


# ---------------------------------------------------------------------------
# Phase 4 command tests
# ---------------------------------------------------------------------------

class TestDataNews:
    @pytest.mark.asyncio
    async def test_valid_tickers_send_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))
        cog.stock_data.news = MagicMock()

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(return_value={'articles': []})), \
             patch("rocketstocks.bot.cogs.data.NewsCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_news.callback(cog, interaction, tickers="AAPL")

        interaction.followup.send.assert_called_once()
        assert interaction.followup.send.call_args.kwargs.get("embed") is mock_embed

    @pytest.mark.asyncio
    async def test_invalid_ticker_sends_error(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=([], ["BAD"]))

        await cog.data_news.callback(cog, interaction, tickers="BAD")

        msg = interaction.followup.send.call_args[0][0]
        assert "BAD" in msg


class TestDataForecast:
    @pytest.mark.asyncio
    async def test_valid_ticker_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(return_value=pd.DataFrame())), \
             patch("rocketstocks.bot.cogs.data.ForecastCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_forecast.callback(cog, interaction, ticker="AAPL")

        interaction.followup.send.assert_called_once()
        assert interaction.followup.send.call_args.kwargs.get("embed") is mock_embed

    @pytest.mark.asyncio
    async def test_nasdaq_error_sends_empty_forecast(self):
        """If NASDAQ raises, command still sends an embed with empty DataFrames."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=(["AAPL"], []))

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(side_effect=Exception("network"))), \
             patch("rocketstocks.bot.cogs.data.ForecastCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_forecast.callback(cog, interaction, ticker="AAPL")

        interaction.followup.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_invalid_ticker_sends_error(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.tickers.parse_valid_tickers = AsyncMock(return_value=([], ["BAD"]))

        await cog.data_forecast.callback(cog, interaction, ticker="BAD")

        msg = interaction.followup.send.call_args[0][0]
        assert "BAD" in msg


class TestDataScreener:
    @pytest.mark.asyncio
    async def test_intraday_sends_embed(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.trading_view = MagicMock()

        mock_embed = MagicMock()
        screener_type = MagicMock()
        screener_type.value = "intraday"
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(return_value=pd.DataFrame())), \
             patch("rocketstocks.bot.cogs.data.OnDemandScreener") as mock_screener, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_screener.return_value.build.return_value = MagicMock()
            await cog.data_screener.callback(cog, interaction, screener_type=screener_type)

        interaction.followup.send.assert_called_once()
        assert interaction.followup.send.call_args.kwargs.get("embed") is mock_embed

    @pytest.mark.asyncio
    async def test_tradingview_error_sends_empty_screener(self):
        """If TradingView raises, command still sends an embed with empty DataFrame."""
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.trading_view = MagicMock()

        screener_type = MagicMock()
        screener_type.value = "unusual-volume"
        with patch("rocketstocks.bot.cogs.data.asyncio.to_thread", new=AsyncMock(side_effect=Exception("TV down"))), \
             patch("rocketstocks.bot.cogs.data.OnDemandScreener") as mock_screener, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=MagicMock()):
            mock_screener.return_value.build.return_value = MagicMock()
            await cog.data_screener.callback(cog, interaction, screener_type=screener_type)

        interaction.followup.send.assert_called_once()


class TestDataLosers:
    @pytest.mark.asyncio
    async def test_sends_embed_with_losers_direction(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.schwab.client = MagicMock()
        cog.stock_data.schwab.get_movers = AsyncMock(return_value={'screeners': []})

        mock_embed = MagicMock()
        with patch("rocketstocks.bot.cogs.data.MoversCard") as mock_card, \
             patch("rocketstocks.bot.cogs.data.spec_to_embed", return_value=mock_embed):
            mock_card.return_value.build.return_value = MagicMock()
            await cog.data_losers.callback(cog, interaction)

        interaction.followup.send.assert_called_once()
        # Verify direction='losers' was passed to MoversCard
        call_args = mock_card.call_args
        assert call_args[0][0].direction == 'losers'

    @pytest.mark.asyncio
    async def test_no_client_sends_auth_message(self):
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.schwab.client = None

        await cog.data_losers.callback(cog, interaction)

        msg = interaction.followup.send.call_args[0][0]
        assert "auth" in msg.lower() or "schwab" in msg.lower()

    @pytest.mark.asyncio
    async def test_rate_limit_error_sends_message(self):
        from rocketstocks.data.clients.schwab import SchwabRateLimitError
        cog = _make_cog()
        interaction = _make_interaction()
        cog.stock_data.schwab.client = MagicMock()
        cog.stock_data.schwab.get_movers = AsyncMock(side_effect=SchwabRateLimitError("429"))

        await cog.data_losers.callback(cog, interaction)

        interaction.followup.send.assert_called_once()

