"""Tests for rocketstocks.core.charting.chart.Chart (construction + render split)."""
import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pandas_ta_classic as ta
import pytest


def _sample_df(n=60):
    """Build a datetime-indexed OHLCV DataFrame with ta.datetime_ordered == True."""
    dates = pd.date_range(end=datetime.date.today(), periods=n, freq="B")
    rng = np.random.default_rng(0)
    close = 100.0 + rng.standard_normal(n).cumsum()
    df = pd.DataFrame({
        "Open":   close - 0.5,
        "High":   close + 1.0,
        "Low":    close - 1.0,
        "Close":  close,
        "Volume": rng.integers(500_000, 2_000_000, n).astype(float),
    }, index=dates)
    df.index.name = "Date"
    return df


class TestChartConstruction:
    def test_construction_does_not_call_mpf_plot(self):
        """Constructing Chart must NOT call mpf.plot — that only happens in render()."""
        df = _sample_df()
        with patch("rocketstocks.core.charting.chart.mpf.plot") as mock_plot, \
             patch("rocketstocks.core.charting.chart.validate_path"):
            from rocketstocks.core.charting.chart import Chart
            chart = Chart(df=df, ticker="AAPL")
        mock_plot.assert_not_called()

    def test_construction_stores_ticker(self):
        df = _sample_df()
        with patch("rocketstocks.core.charting.chart.mpf.plot"), \
             patch("rocketstocks.core.charting.chart.validate_path"):
            from rocketstocks.core.charting.chart import Chart
            chart = Chart(df=df, ticker="TSLA")
        assert chart.ticker == "TSLA"

    def test_construction_stores_df(self):
        df = _sample_df()
        with patch("rocketstocks.core.charting.chart.mpf.plot"), \
             patch("rocketstocks.core.charting.chart.validate_path"):
            from rocketstocks.core.charting.chart import Chart
            chart = Chart(df=df, ticker="AAPL")
        assert chart.df is not None

    def test_bad_df_returns_early(self):
        """An invalid (non-datetime-ordered) df should cause early return."""
        bad_df = pd.DataFrame({"Close": [1, 2, 3]})  # no datetime index
        with patch("rocketstocks.core.charting.chart.validate_path"):
            from rocketstocks.core.charting.chart import Chart
            chart = Chart(df=bad_df, ticker="AAPL")
        # _validate_chart_kwargs won't have run, so config attr won't exist
        assert not hasattr(chart, "config")


class TestChartRender:
    def test_render_calls_mpf_plot(self):
        """render() should invoke mpf.plot exactly once."""
        df = _sample_df()
        # Need a VolXX column for volume plotting — add fake one
        df["Vol_SMA_20"] = df["Volume"].rolling(20).mean().fillna(df["Volume"])

        with patch("rocketstocks.core.charting.chart.mpf.plot") as mock_plot, \
             patch("rocketstocks.core.charting.chart.validate_path"):
            from rocketstocks.core.charting.chart import Chart
            chart = Chart(df=df, ticker="AAPL", volume=True)
            chart.render()

        mock_plot.assert_called_once()

    def test_render_returns_filepath_string(self):
        df = _sample_df()
        df["Vol_SMA_20"] = df["Volume"]

        with patch("rocketstocks.core.charting.chart.mpf.plot"), \
             patch("rocketstocks.core.charting.chart.validate_path"):
            from rocketstocks.core.charting.chart import Chart
            chart = Chart(df=df, ticker="AAPL")
            result = chart.render()

        assert result is not None
        assert "AAPL" in result
        assert result.endswith(".png")
