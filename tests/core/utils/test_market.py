"""Tests for rocketstocks.core.utils.market."""
import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import pandas as pd
import pytest


def _make_market_utils(schedule=None):
    """Build a MarketUtils instance with a mocked calendar."""
    with patch("rocketstocks.core.utils.market.mcal") as mock_mcal:
        mock_cal = MagicMock()
        mock_mcal.get_calendar.return_value = mock_cal
        mock_cal.schedule.return_value = schedule if schedule is not None else pd.DataFrame()
        from rocketstocks.core.utils.market import MarketUtils
        mu = MarketUtils()
        mu._calendar = mock_cal
        if schedule is not None:
            mu._schedule = schedule
    return mu, mock_cal


def _make_schedule(pre, market_open, market_close, post):
    """Build a one-row schedule DataFrame."""
    return pd.DataFrame({
        "pre": [pre],
        "market_open": [market_open],
        "market_close": [market_close],
        "post": [post],
    })


class TestMarketOpenToday:
    def test_returns_true_when_valid_day(self):
        mu, mock_cal = _make_market_utils()
        today = datetime.datetime.now(datetime.UTC).date()
        mock_dates = MagicMock()
        mock_dates.date = [today]
        mock_cal.valid_days.return_value = mock_dates
        assert mu.market_open_today() is True

    def test_returns_false_when_not_valid_day(self):
        mu, mock_cal = _make_market_utils()
        mock_dates = MagicMock()
        mock_dates.date = []  # today not in list
        mock_cal.valid_days.return_value = mock_dates
        assert mu.market_open_today() is False


class TestInPremarket:
    def test_true_when_in_premarket_window(self):
        now = datetime.datetime.now(datetime.UTC)
        schedule = _make_schedule(
            pre=now - datetime.timedelta(hours=1),
            market_open=now + datetime.timedelta(hours=1),
            market_close=now + datetime.timedelta(hours=7),
            post=now + datetime.timedelta(hours=9),
        )
        mu, _ = _make_market_utils(schedule)
        with patch("rocketstocks.core.utils.market.datetime.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = mu.in_premarket()
        assert result is True

    def test_false_when_before_premarket(self):
        now = datetime.datetime.now(datetime.UTC)
        schedule = _make_schedule(
            pre=now + datetime.timedelta(hours=1),
            market_open=now + datetime.timedelta(hours=3),
            market_close=now + datetime.timedelta(hours=9),
            post=now + datetime.timedelta(hours=11),
        )
        mu, _ = _make_market_utils(schedule)
        with patch("rocketstocks.core.utils.market.datetime.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = mu.in_premarket()
        assert result is False

    def test_false_when_schedule_is_empty(self):
        mu, _ = _make_market_utils(pd.DataFrame())
        assert mu.in_premarket() is False


class TestInAftermarket:
    def test_true_when_in_aftermarket_window(self):
        now = datetime.datetime.now(datetime.UTC)
        schedule = _make_schedule(
            pre=now - datetime.timedelta(hours=9),
            market_open=now - datetime.timedelta(hours=7),
            market_close=now - datetime.timedelta(hours=1),
            post=now + datetime.timedelta(hours=2),
        )
        mu, _ = _make_market_utils(schedule)
        with patch("rocketstocks.core.utils.market.datetime.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = mu.in_aftermarket()
        assert result is True

    def test_false_when_after_post_close(self):
        now = datetime.datetime.now(datetime.UTC)
        schedule = _make_schedule(
            pre=now - datetime.timedelta(hours=12),
            market_open=now - datetime.timedelta(hours=10),
            market_close=now - datetime.timedelta(hours=4),
            post=now - datetime.timedelta(hours=1),
        )
        mu, _ = _make_market_utils(schedule)
        with patch("rocketstocks.core.utils.market.datetime.datetime") as mock_dt:
            mock_dt.now.return_value = now
            result = mu.in_aftermarket()
        assert result is False


class TestGetMarketPeriod:
    def _mu_with_period(self, period: str):
        """Return a MarketUtils whose period methods return one True at most."""
        mu, _ = _make_market_utils(pd.DataFrame())
        mu.in_premarket = MagicMock(return_value=(period == "premarket"))
        mu.in_intraday = MagicMock(return_value=(period == "intraday"))
        mu.in_aftermarket = MagicMock(return_value=(period == "aftermarket"))
        mu.get_market_schedule = MagicMock(return_value=pd.DataFrame())
        return mu

    def test_premarket(self):
        mu = self._mu_with_period("premarket")
        assert mu.get_market_period() == "premarket"

    def test_intraday(self):
        mu = self._mu_with_period("intraday")
        assert mu.get_market_period() == "intraday"

    def test_aftermarket(self):
        mu = self._mu_with_period("aftermarket")
        assert mu.get_market_period() == "aftermarket"

    def test_eod(self):
        mu = self._mu_with_period("EOD")
        assert mu.get_market_period() == "EOD"


class TestScheduleCaching:
    def test_schedule_fetched_on_first_call(self):
        """_refresh_schedule_if_needed fetches the schedule on first call."""
        mu, mock_cal = _make_market_utils()
        today = datetime.date.today()
        new_schedule = pd.DataFrame({"pre": [1], "market_open": [2], "market_close": [3], "post": [4]})
        mock_cal.schedule.return_value = new_schedule
        mu.get_market_schedule = MagicMock(return_value=new_schedule)

        with patch("rocketstocks.core.utils.market.datetime.datetime") as mock_dt:
            mock_dt.now.return_value.date.return_value = today
            mu._refresh_schedule_if_needed()

        mu.get_market_schedule.assert_called_once_with(today)
        assert mu._cached_date == today

    def test_schedule_not_refetched_on_same_day(self):
        """_refresh_schedule_if_needed skips the fetch when cached_date equals today."""
        mu, mock_cal = _make_market_utils()
        today = datetime.date.today()
        mu._cached_date = today
        mu.get_market_schedule = MagicMock()

        with patch("rocketstocks.core.utils.market.datetime.datetime") as mock_dt:
            mock_dt.now.return_value.date.return_value = today
            mu._refresh_schedule_if_needed()

        mu.get_market_schedule.assert_not_called()

    def test_schedule_refetched_on_new_day(self):
        """_refresh_schedule_if_needed re-fetches when the date has advanced."""
        mu, mock_cal = _make_market_utils()
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        today = datetime.date.today()
        mu._cached_date = yesterday
        new_schedule = pd.DataFrame({"pre": [1], "market_open": [2], "market_close": [3], "post": [4]})
        mu.get_market_schedule = MagicMock(return_value=new_schedule)

        with patch("rocketstocks.core.utils.market.datetime.datetime") as mock_dt:
            mock_dt.now.return_value.date.return_value = today
            mu._refresh_schedule_if_needed()

        mu.get_market_schedule.assert_called_once_with(today)
        assert mu._cached_date == today
