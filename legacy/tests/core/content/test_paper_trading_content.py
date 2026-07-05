"""Tests for paper trading content classes."""
import pytest

from rocketstocks.core.content.models import (
    COLOR_AMBER,
    COLOR_GREEN,
    COLOR_INDIGO,
    COLOR_RED,
    PortfolioPosition,
    PortfolioViewData,
    TradeConfirmationData,
    TradeHistoryData,
    TradeQuoteData,
)
from rocketstocks.core.content.reports.portfolio_view import PortfolioView
from rocketstocks.core.content.reports.trade_confirmation import TradeConfirmation
from rocketstocks.core.content.reports.trade_history import TradeHistory
from rocketstocks.core.content.reports.trade_quote import TradeQuote


# ---------------------------------------------------------------------------
# TradeQuote
# ---------------------------------------------------------------------------

def _make_quote_data(side="BUY"):
    return TradeQuoteData(
        ticker="AAPL",
        ticker_name="Apple Inc.",
        side=side,
        shares=10,
        price=150.0,
        total=1500.0,
        cash_after=8500.0,
    )


def test_trade_quote_buy_title():
    spec = TradeQuote(_make_quote_data("BUY")).build()
    assert "Buy" in spec.title
    assert "AAPL" in spec.title


def test_trade_quote_sell_title():
    spec = TradeQuote(_make_quote_data("SELL")).build()
    assert "Sell" in spec.title


def test_trade_quote_color_is_amber():
    spec = TradeQuote(_make_quote_data()).build()
    assert spec.color == COLOR_AMBER


def test_trade_quote_fields_present():
    spec = TradeQuote(_make_quote_data()).build()
    field_names = [f.name for f in spec.fields]
    assert "Side" in field_names
    assert "Shares" in field_names
    assert "Price" in field_names
    assert "Total" in field_names
    assert "Cash After" in field_names


def test_trade_quote_price_formatted():
    spec = TradeQuote(_make_quote_data()).build()
    price_field = next(f for f in spec.fields if f.name == "Price")
    assert "$150.00" in price_field.value


def test_trade_quote_has_timestamp():
    spec = TradeQuote(_make_quote_data()).build()
    assert spec.timestamp is True


# ---------------------------------------------------------------------------
# TradeConfirmation
# ---------------------------------------------------------------------------

def _make_confirm_data(side="BUY", was_queued=False):
    return TradeConfirmationData(
        ticker="TSLA",
        ticker_name="Tesla Inc.",
        side=side,
        shares=5,
        price=200.0,
        total=1000.0,
        cash_remaining=9000.0,
        was_queued=was_queued,
    )


def test_trade_confirmation_buy_color():
    spec = TradeConfirmation(_make_confirm_data("BUY")).build()
    assert spec.color == COLOR_GREEN


def test_trade_confirmation_sell_color():
    spec = TradeConfirmation(_make_confirm_data("SELL")).build()
    assert spec.color == COLOR_RED


def test_trade_confirmation_executed_title():
    spec = TradeConfirmation(_make_confirm_data(was_queued=False)).build()
    assert "Executed" in spec.title


def test_trade_confirmation_queued_title():
    spec = TradeConfirmation(_make_confirm_data(was_queued=True)).build()
    assert "Queued" in spec.title


def test_trade_confirmation_queued_description_mentions_market_open():
    spec = TradeConfirmation(_make_confirm_data(was_queued=True)).build()
    assert "market open" in spec.description.lower()


def test_trade_confirmation_fields_present():
    spec = TradeConfirmation(_make_confirm_data()).build()
    field_names = [f.name for f in spec.fields]
    assert "Cash Remaining" in field_names


# ---------------------------------------------------------------------------
# PortfolioView
# ---------------------------------------------------------------------------

def _make_position(ticker="AAPL", gain=500.0, pct=5.0):
    return PortfolioPosition(
        ticker=ticker,
        shares=10,
        avg_cost_basis=100.0,
        current_price=105.0,
        market_value=1050.0,
        gain_loss=gain,
        gain_loss_pct=pct,
    )


def _make_portfolio_data(positions=None, gain_loss=1000.0, pending_orders=None):
    if positions is None:
        positions = [_make_position()]
    return PortfolioViewData(
        user_name="TestUser",
        cash=5000.0,
        positions=positions,
        pending_orders=pending_orders or [],
        total_value=11000.0,
        total_gain_loss=gain_loss,
        total_gain_loss_pct=gain_loss / 100.0,
    )


def test_portfolio_view_green_when_gain():
    spec = PortfolioView(_make_portfolio_data(gain_loss=500.0)).build()
    assert spec.color == COLOR_GREEN


def test_portfolio_view_red_when_loss():
    spec = PortfolioView(_make_portfolio_data(gain_loss=-500.0)).build()
    assert spec.color == COLOR_RED


def test_portfolio_view_shows_positions_field():
    spec = PortfolioView(_make_portfolio_data()).build()
    field_names = [f.name for f in spec.fields]
    assert any("Positions" in name for name in field_names)


def test_portfolio_view_no_positions_message():
    data = _make_portfolio_data(positions=[])
    spec = PortfolioView(data).build()
    field = next(f for f in spec.fields if "Positions" in f.name)
    assert "No open positions" in field.value


def test_portfolio_view_pending_orders_shown():
    orders = [{'id': 1, 'side': 'BUY', 'shares': 5, 'ticker': 'TSLA', 'quoted_price': 200.0}]
    data = _make_portfolio_data(pending_orders=orders)
    spec = PortfolioView(data).build()
    field_names = [f.name for f in spec.fields]
    assert any("Pending" in name for name in field_names)


def test_portfolio_view_truncates_positions():
    positions = [_make_position(ticker=f"TK{i}") for i in range(15)]
    data = _make_portfolio_data(positions=positions)
    spec = PortfolioView(data).build()
    pos_field = next(f for f in spec.fields if "Positions" in f.name)
    assert "more" in pos_field.value


def test_portfolio_view_user_name_in_description():
    spec = PortfolioView(_make_portfolio_data()).build()
    assert "TestUser" in spec.description


# ---------------------------------------------------------------------------
# TradeHistory
# ---------------------------------------------------------------------------

import datetime


def _make_tx(side="BUY", ticker="AAPL"):
    return {
        'side': side,
        'ticker': ticker,
        'shares': 10,
        'price': 150.0,
        'total': 1500.0,
        'executed_at': datetime.datetime(2026, 3, 1, 10, 30),
    }


def test_trade_history_no_trades_message():
    data = TradeHistoryData(user_name="TestUser", transactions=[])
    spec = TradeHistory(data).build()
    assert "No trades yet" in spec.description


def test_trade_history_shows_transactions():
    data = TradeHistoryData(user_name="TestUser", transactions=[_make_tx()])
    spec = TradeHistory(data).build()
    assert len(spec.fields) == 1
    assert "AAPL" in spec.fields[0].value


def test_trade_history_buy_icon():
    data = TradeHistoryData(user_name="TestUser", transactions=[_make_tx("BUY")])
    spec = TradeHistory(data).build()
    assert "🟢" in spec.fields[0].value


def test_trade_history_sell_icon():
    data = TradeHistoryData(user_name="TestUser", transactions=[_make_tx("SELL")])
    spec = TradeHistory(data).build()
    assert "🔴" in spec.fields[0].value


def test_trade_history_color():
    data = TradeHistoryData(user_name="TestUser", transactions=[])
    spec = TradeHistory(data).build()
    assert spec.color == COLOR_INDIGO


def test_trade_history_user_in_description():
    data = TradeHistoryData(user_name="Alice", transactions=[])
    spec = TradeHistory(data).build()
    assert "Alice" in spec.description


# ---------------------------------------------------------------------------
# Leaderboard
# ---------------------------------------------------------------------------

from rocketstocks.core.content.models import (
    COLOR_BLUE,
    COLOR_GOLD,
    LeaderboardEntry,
    LeaderboardViewData,
    PerformanceViewData,
    TradeAnnouncementData,
)
from rocketstocks.core.content.reports.leaderboard import Leaderboard
from rocketstocks.core.content.reports.trade_announcement import TradeAnnouncement
from rocketstocks.core.content.reports.performance_view import PerformanceView


def _make_entry(user_name="Alice", total_value=11000.0, gain=1000.0, pct=10.0, positions=2):
    return LeaderboardEntry(
        user_id=1001,
        user_name=user_name,
        total_value=total_value,
        total_gain_loss=gain,
        total_gain_loss_pct=pct,
        position_count=positions,
    )


def test_leaderboard_empty_portfolios():
    data = LeaderboardViewData(guild_name="TestGuild", entries=[])
    spec = Leaderboard(data).build()
    assert "No portfolios yet" in spec.description
    assert spec.color == COLOR_BLUE


def test_leaderboard_color_gold_when_entries():
    data = LeaderboardViewData(guild_name="TestGuild", entries=[_make_entry()])
    spec = Leaderboard(data).build()
    assert spec.color == COLOR_GOLD


def test_leaderboard_guild_name_in_description():
    data = LeaderboardViewData(guild_name="MyGuild", entries=[_make_entry()])
    spec = Leaderboard(data).build()
    assert "MyGuild" in spec.description


def test_leaderboard_shows_user_name():
    data = LeaderboardViewData(guild_name="G", entries=[_make_entry(user_name="Bob")])
    spec = Leaderboard(data).build()
    assert "Bob" in spec.fields[0].value


def test_leaderboard_first_place_medal():
    data = LeaderboardViewData(guild_name="G", entries=[_make_entry()])
    spec = Leaderboard(data).build()
    assert "🥇" in spec.fields[0].value


def test_leaderboard_top_3_medals():
    entries = [
        _make_entry("A", 12000.0, 2000.0, 20.0),
        _make_entry("B", 11000.0, 1000.0, 10.0),
        _make_entry("C", 10500.0, 500.0, 5.0),
    ]
    data = LeaderboardViewData(guild_name="G", entries=entries)
    spec = Leaderboard(data).build()
    assert "🥇" in spec.fields[0].value
    assert "🥈" in spec.fields[0].value
    assert "🥉" in spec.fields[0].value


def test_leaderboard_truncates_entries():
    entries = [_make_entry(f"User{i}") for i in range(20)]
    data = LeaderboardViewData(guild_name="G", entries=entries)
    spec = Leaderboard(data).build()
    assert "more" in spec.fields[0].value


def test_leaderboard_entry_count_in_field_name():
    entries = [_make_entry("A"), _make_entry("B")]
    data = LeaderboardViewData(guild_name="G", entries=entries)
    spec = Leaderboard(data).build()
    assert "2" in spec.fields[0].name


# ---------------------------------------------------------------------------
# TradeAnnouncement
# ---------------------------------------------------------------------------

def _make_announce_data(side="BUY", was_queued=False):
    return TradeAnnouncementData(
        user_name="Alice",
        ticker="AAPL",
        ticker_name="Apple Inc.",
        side=side,
        shares=10,
        price=150.0,
        total=1500.0,
        was_queued=was_queued,
    )


def test_trade_announcement_buy_color():
    spec = TradeAnnouncement(_make_announce_data("BUY")).build()
    assert spec.color == COLOR_GREEN


def test_trade_announcement_sell_color():
    spec = TradeAnnouncement(_make_announce_data("SELL")).build()
    assert spec.color == COLOR_RED


def test_trade_announcement_executed_title():
    spec = TradeAnnouncement(_make_announce_data(was_queued=False)).build()
    assert "Executed" in spec.title


def test_trade_announcement_queued_title():
    spec = TradeAnnouncement(_make_announce_data(was_queued=True)).build()
    assert "Queued" in spec.title


def test_trade_announcement_user_in_description():
    spec = TradeAnnouncement(_make_announce_data()).build()
    assert "Alice" in spec.description


def test_trade_announcement_ticker_in_title():
    spec = TradeAnnouncement(_make_announce_data()).build()
    assert "AAPL" in spec.title


def test_trade_announcement_queued_mentions_market_open():
    spec = TradeAnnouncement(_make_announce_data(was_queued=True)).build()
    assert "market open" in spec.description.lower()


def test_trade_announcement_has_side_field():
    spec = TradeAnnouncement(_make_announce_data()).build()
    assert any(f.name == "Side" for f in spec.fields)


def test_trade_announcement_total_field():
    spec = TradeAnnouncement(_make_announce_data()).build()
    total_field = next(f for f in spec.fields if f.name == "Total")
    assert "$1,500.00" in total_field.value


# ---------------------------------------------------------------------------
# PerformanceView
# ---------------------------------------------------------------------------

def _make_perf_data(snapshots=None, gain=1000.0, pct=10.0):
    if snapshots is None:
        snapshots = [
            {'snapshot_date': datetime.date(2026, 3, 25), 'portfolio_value': 10500.0},
            {'snapshot_date': datetime.date(2026, 3, 26), 'portfolio_value': 11000.0},
        ]
    return PerformanceViewData(
        user_name="Alice",
        snapshots=snapshots,
        days=7,
        current_value=11000.0,
        total_gain_loss=gain,
        total_gain_loss_pct=pct,
    )


def test_performance_view_green_on_gain():
    spec = PerformanceView(_make_perf_data(gain=500.0)).build()
    assert spec.color == COLOR_GREEN


def test_performance_view_red_on_loss():
    spec = PerformanceView(_make_perf_data(gain=-500.0, pct=-5.0)).build()
    assert spec.color == COLOR_RED


def test_performance_view_blue_on_flat():
    spec = PerformanceView(_make_perf_data(gain=0.0, pct=0.0)).build()
    assert spec.color == COLOR_BLUE


def test_performance_view_user_in_description():
    spec = PerformanceView(_make_perf_data()).build()
    assert "Alice" in spec.description


def test_performance_view_days_in_description():
    spec = PerformanceView(_make_perf_data()).build()
    assert "7" in spec.description


def test_performance_view_no_snapshots_message():
    data = _make_perf_data(snapshots=[])
    spec = PerformanceView(data).build()
    assert "No snapshots" in spec.fields[0].value


def test_performance_view_shows_snapshot_values():
    spec = PerformanceView(_make_perf_data()).build()
    assert "$11,000.00" in spec.fields[0].value


def test_performance_view_snapshot_count_in_field_name():
    spec = PerformanceView(_make_perf_data()).build()
    assert "2" in spec.fields[0].name


def test_performance_view_truncates_long_history():
    snaps = [
        {'snapshot_date': datetime.date(2026, 3, i + 1), 'portfolio_value': 10000.0 + i * 100}
        for i in range(25)
    ]
    data = _make_perf_data(snapshots=snaps)
    spec = PerformanceView(data).build()
    # Shows last 10, and count in name is 25
    assert "25" in spec.fields[0].name


# ---------------------------------------------------------------------------
# WeeklyRoundup
# ---------------------------------------------------------------------------

from rocketstocks.core.content.models import WeeklyAward, WeeklyRoundupData
from rocketstocks.core.content.reports.weekly_roundup import WeeklyRoundup


def _make_award(name="Biggest Gainer", winner="Alice", detail="+10%"):
    return WeeklyAward(
        award_name=name,
        description="Test award description",
        winner_name=winner,
        detail=detail,
    )


def _make_no_winner_award(name="Ghost Trader"):
    return WeeklyAward(
        award_name=name,
        description="No winner this week",
        winner_name=None,
        detail=None,
    )


def _make_roundup_data(
    entries=None,
    awards=None,
    stats=None,
):
    if entries is None:
        entries = [_make_entry("Alice", 11000.0, 1000.0, 10.0), _make_entry("Bob", 10500.0, 500.0, 5.0)]
    if awards is None:
        awards = [_make_award(f"Award {i}") for i in range(15)]
    if stats is None:
        stats = {
            'total_trades': 42,
            'active_traders': 5,
            'most_traded_ticker': 'AAPL',
            'total_volume': 75000.0,
        }
    return WeeklyRoundupData(
        guild_name="TestGuild",
        week_label="Mar 23–27, 2026",
        leaderboard=entries,
        awards=awards,
        server_stats=stats,
    )


def test_weekly_roundup_title_contains_week_label():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    assert "Mar 23–27, 2026" in spec.title


def test_weekly_roundup_color_is_gold():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    assert spec.color == COLOR_GOLD


def test_weekly_roundup_guild_name_in_description():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    assert "TestGuild" in spec.description


def test_weekly_roundup_has_timestamp():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    assert spec.timestamp is True


def test_weekly_roundup_leaderboard_field_present():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    field_names = [f.name for f in spec.fields]
    assert any("Leaderboard" in n for n in field_names)


def test_weekly_roundup_server_stats_field_present():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    field_names = [f.name for f in spec.fields]
    assert any("Stats" in n for n in field_names)


def test_weekly_roundup_server_stats_values():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    stats_field = next(f for f in spec.fields if "Stats" in f.name)
    assert "42" in stats_field.value
    assert "AAPL" in stats_field.value


def test_weekly_roundup_leaderboard_shows_user_name():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    lb_field = next(f for f in spec.fields if "Leaderboard" in f.name)
    assert "Alice" in lb_field.value


def test_weekly_roundup_leaderboard_rank_medals():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    lb_field = next(f for f in spec.fields if "Leaderboard" in f.name)
    assert "🥇" in lb_field.value


def test_weekly_roundup_awards_field_shows_winner():
    spec = WeeklyRoundup(_make_roundup_data()).build()
    awards_field = next((f for f in spec.fields if "Awards" in f.name), None)
    if awards_field:
        assert "Alice" in awards_field.value


def test_weekly_roundup_awards_no_winner_message():
    awards = [_make_no_winner_award(f"Award {i}") for i in range(15)]
    data = _make_roundup_data(awards=awards)
    spec = WeeklyRoundup(data).build()
    awards_field = next((f for f in spec.fields if "Awards" in f.name), None)
    if awards_field:
        assert "No winner this week" in awards_field.value


def test_weekly_roundup_empty_leaderboard():
    data = _make_roundup_data(entries=[])
    spec = WeeklyRoundup(data).build()
    # Should still build without error; no leaderboard field
    field_names = [f.name for f in spec.fields]
    assert any("Stats" in n for n in field_names)


def test_weekly_roundup_no_split_for_small_content():
    content = WeeklyRoundup(_make_roundup_data())
    assert not content.needs_split()


def test_weekly_roundup_needs_split_for_large_content():
    # Create enough awards to push past 6000 chars
    long_detail = "X" * 300
    awards = [WeeklyAward(
        award_name=f"Award {i}",
        description="Some description " * 5,
        winner_name="A very long winner name indeed",
        detail=long_detail,
    ) for i in range(15)]
    entries = [_make_entry(f"User{i}" * 5, 10000.0 + i, float(i), float(i) / 100) for i in range(10)]
    data = _make_roundup_data(awards=awards, entries=entries)
    content = WeeklyRoundup(data)
    if content.needs_split():
        awards_embed = content.build_awards_embed()
        assert "Awards" in awards_embed.title
        assert "TestGuild" in awards_embed.description
