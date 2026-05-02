"""Tests for rocketstocks.api.exporters — Phase 4."""
import datetime
import json
from pathlib import Path

import pandas as pd
import pytest

from rocketstocks.api.exporters import (
    write_daily_csv,
    write_5m_csv,
    write_eps_csv,
    write_financials_csvs,
    write_fundamentals_json,
    write_options_json,
    write_popularity_csv,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "cowork_expected"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _daily_df(ticker="AAPL"):
    return pd.DataFrame({
        "ticker": [ticker, ticker],
        "open":   [150.0, 150.0],
        "high":   [155.0, 155.0],
        "low":    [148.0, 148.0],
        "close":  [152.0, 152.0],
        "volume": [1_000_000, 1_000_000],
        "date":   [datetime.date(2026, 1, 1), datetime.date(2026, 1, 2)],
    })


def _popularity_df():
    return pd.DataFrame({
        "datetime":         ["2026-01-01 09:30:00"],
        "rank":             [5],
        "ticker":           ["AAPL"],
        "name":             ["Apple Inc."],
        "mentions":         [100],
        "upvotes":          [50],
        "rank_24h_ago":     [8],
        "mentions_24h_ago": [80],
    })


# ---------------------------------------------------------------------------
# write_daily_csv
# ---------------------------------------------------------------------------

class TestWriteDailyCsv:
    def test_golden_file(self, tmp_path):
        path = write_daily_csv(_daily_df(), tmp_path, "AAPL")
        expected = (_FIXTURES / "AAPL_daily_data.csv").read_text()
        assert path.read_text() == expected

    def test_filename(self, tmp_path):
        path = write_daily_csv(_daily_df(), tmp_path, "MSFT")
        assert path.name == "MSFT_daily_data.csv"

    def test_column_order(self, tmp_path):
        df = _daily_df()
        df = df[["date", "close", "open", "high", "low", "volume", "ticker"]]  # scrambled
        path = write_daily_csv(df, tmp_path, "AAPL")
        header = path.read_text().splitlines()[0]
        assert header == "ticker,open,high,low,close,volume,date"

    def test_extra_columns_dropped(self, tmp_path):
        df = _daily_df()
        df["extra"] = "noise"
        path = write_daily_csv(df, tmp_path, "AAPL")
        header = path.read_text().splitlines()[0]
        assert "extra" not in header


# ---------------------------------------------------------------------------
# write_5m_csv
# ---------------------------------------------------------------------------

class TestWrite5mCsv:
    def test_filename(self, tmp_path):
        df = pd.DataFrame({
            "ticker":   ["AAPL"],
            "open":     [150.0],
            "high":     [151.0],
            "low":      [149.0],
            "close":    [150.5],
            "volume":   [100_000],
            "datetime": [datetime.datetime(2026, 1, 2, 9, 30)],
        })
        path = write_5m_csv(df, tmp_path, "AAPL")
        assert path.name == "AAPL_5m_data.csv"

    def test_column_order(self, tmp_path):
        df = pd.DataFrame({
            "ticker":   ["AAPL"],
            "open":     [150.0],
            "high":     [151.0],
            "low":      [149.0],
            "close":    [150.5],
            "volume":   [100_000],
            "datetime": [datetime.datetime(2026, 1, 2, 9, 30)],
        })
        path = write_5m_csv(df, tmp_path, "AAPL")
        header = path.read_text().splitlines()[0]
        assert header == "ticker,open,high,low,close,volume,datetime"


# ---------------------------------------------------------------------------
# write_options_json
# ---------------------------------------------------------------------------

class TestWriteOptionsJson:
    def test_filename(self, tmp_path):
        path = write_options_json({"callExpDateMap": {}}, tmp_path, "AAPL")
        assert path.name == "AAPL_options_chain.json"

    def test_roundtrip(self, tmp_path):
        data = {"callExpDateMap": {"2026-05-16:7": {}}, "putCallRatio": 0.8}
        path = write_options_json(data, tmp_path, "AAPL")
        assert json.loads(path.read_text()) == data


# ---------------------------------------------------------------------------
# write_fundamentals_json
# ---------------------------------------------------------------------------

class TestWriteFundamentalsJson:
    def test_filename(self, tmp_path):
        path = write_fundamentals_json({"instruments": []}, tmp_path, "AAPL")
        assert path.name == "AAPL_fundamentals.json"

    def test_roundtrip(self, tmp_path):
        data = {"instruments": [{"fundamental": {"symbol": "AAPL"}}]}
        path = write_fundamentals_json(data, tmp_path, "AAPL")
        assert json.loads(path.read_text()) == data


# ---------------------------------------------------------------------------
# write_financials_csvs
# ---------------------------------------------------------------------------

class TestWriteFinancialsCsvs:
    def test_writes_six_files_for_full_financials(self, tmp_path):
        df = pd.DataFrame({"Revenue": [1e9]}, index=["2026-01-01"])
        financials = {
            "income_statement":           df,
            "quarterly_income_statement": df,
            "balance_sheet":              df,
            "quarterly_balance_sheet":    df,
            "cash_flow":                  df,
            "quarterly_cash_flow":        df,
        }
        paths = write_financials_csvs(financials, tmp_path, "AAPL")
        assert len(paths) == 6

    def test_expected_filenames(self, tmp_path):
        df = pd.DataFrame({"Revenue": [1e9]})
        financials = {"income_statement": df, "balance_sheet": df, "cash_flow": df,
                      "quarterly_income_statement": df, "quarterly_balance_sheet": df,
                      "quarterly_cash_flow": df}
        paths = write_financials_csvs(financials, tmp_path, "AAPL")
        names = {p.name for p in paths}
        assert "AAPL_income_statement.csv" in names
        assert "AAPL_quarterly_balance_sheet.csv" in names
        assert "AAPL_quarterly_cash_flow.csv" in names

    def test_skips_empty_dataframes(self, tmp_path):
        financials = {
            "income_statement": pd.DataFrame(),
            "balance_sheet": pd.DataFrame({"Revenue": [1e9]}),
            "cash_flow": None,
            "quarterly_income_statement": pd.DataFrame(),
            "quarterly_balance_sheet": pd.DataFrame(),
            "quarterly_cash_flow": pd.DataFrame(),
        }
        paths = write_financials_csvs(financials, tmp_path, "AAPL")
        assert len(paths) == 1
        assert paths[0].name == "AAPL_balance_sheet.csv"


# ---------------------------------------------------------------------------
# write_eps_csv
# ---------------------------------------------------------------------------

class TestWriteEpsCsv:
    def test_filename(self, tmp_path):
        df = pd.DataFrame({"ticker": ["AAPL"], "eps": [1.5]})
        path = write_eps_csv(df, tmp_path, "AAPL")
        assert path.name == "AAPL_eps.csv"

    def test_no_index_column(self, tmp_path):
        df = pd.DataFrame({"ticker": ["AAPL"], "eps": [1.5]})
        path = write_eps_csv(df, tmp_path, "AAPL")
        header = path.read_text().splitlines()[0]
        assert header == "ticker,eps"


# ---------------------------------------------------------------------------
# write_popularity_csv
# ---------------------------------------------------------------------------

class TestWritePopularityCsv:
    def test_golden_file(self, tmp_path):
        path = write_popularity_csv(_popularity_df(), tmp_path, "AAPL")
        expected = (_FIXTURES / "AAPL_popularity.csv").read_text()
        assert path.read_text() == expected

    def test_column_order(self, tmp_path):
        path = write_popularity_csv(_popularity_df(), tmp_path, "AAPL")
        header = path.read_text().splitlines()[0]
        assert header == "datetime,rank,ticker,name,mentions,upvotes,rank_24h_ago,mentions_24h_ago"

    def test_filename(self, tmp_path):
        path = write_popularity_csv(_popularity_df(), tmp_path, "GME")
        assert path.name == "GME_popularity.csv"
