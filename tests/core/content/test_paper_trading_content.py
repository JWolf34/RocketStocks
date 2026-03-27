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
