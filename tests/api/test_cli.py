"""Tests for rocketstocks.api.cli — Phase 4."""
import asyncio
import datetime
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

import pandas as pd
import pytest

from rocketstocks.api.cli import _build_parser, _async_main, _export_ticker, _ALL_INCLUDES


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_api(tmp_path):
    """Return a fully-mocked DataAPI and a mock db."""
    api = MagicMock(name="DataAPI")
    api._sd = MagicMock()
    api._sd.db.close = AsyncMock()

    sample_daily = pd.DataFrame({
        "ticker": ["AAPL"], "open": [150.0], "high": [155.0],
        "low": [148.0], "close": [152.0], "volume": [1_000_000],
        "date": [datetime.date(2026, 1, 1)],
    })
    sample_5m = pd.DataFrame({
        "ticker": ["AAPL"], "open": [150.0], "high": [151.0],
        "low": [149.0], "close": [150.5], "volume": [100_000],
        "datetime": [datetime.datetime(2026, 1, 2, 9, 30)],
    })
    sample_eps = pd.DataFrame({"ticker": ["AAPL"], "eps": [1.5]})
    sample_popularity = pd.DataFrame({
        "datetime": ["2026-01-01 09:30:00"], "rank": [5], "ticker": ["AAPL"],
        "name": ["Apple Inc."], "mentions": [100], "upvotes": [50],
        "rank_24h_ago": [8], "mentions_24h_ago": [80],
    })
    financials = {
        "income_statement":           pd.DataFrame({"Revenue": [1e9]}),
        "quarterly_income_statement": pd.DataFrame({"Revenue": [5e8]}),
        "balance_sheet":              pd.DataFrame({"Assets": [2e9]}),
        "quarterly_balance_sheet":    pd.DataFrame({"Assets": [2e9]}),
        "cash_flow":                  pd.DataFrame({"FCF": [3e8]}),
        "quarterly_cash_flow":        pd.DataFrame({"FCF": [1e8]}),
    }

    api.get_daily_history = AsyncMock(return_value=sample_daily)
    api.get_5m_history = AsyncMock(return_value=sample_5m)
    api.get_options_chain = AsyncMock(return_value={"callExpDateMap": {}})
    api.get_schwab_fundamentals = AsyncMock(return_value={"instruments": []})
    api.get_financials = MagicMock(return_value=financials)
    api.get_eps_history = AsyncMock(return_value=sample_eps)
    api.get_popularity = AsyncMock(return_value=sample_popularity)
    api.get_watchlist_tickers = AsyncMock(return_value=["AAPL", "MSFT"])
    api.get_watchlists = AsyncMock(return_value=["Tech", "Biotech"])
    api.get_quote = AsyncMock(return_value={"lastPrice": 150.0})
    api.get_quotes = AsyncMock(return_value={"AAPL": {"lastPrice": 150.0}, "MSFT": {"lastPrice": 420.0}})
    api.get_ticker_info = AsyncMock(return_value={"ticker": "AAPL", "name": "Apple Inc."})
    return api


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

class TestParser:
    def test_export_requires_to(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["export", "AAPL"])

    def test_export_parses_tickers_and_dest(self):
        parser = _build_parser()
        args = parser.parse_args(["export", "AAPL", "MSFT", "--to", "/tmp/out"])
        assert args.tickers == ["AAPL", "MSFT"]
        assert args.to == "/tmp/out"

    def test_include_default_is_none(self):
        parser = _build_parser()
        args = parser.parse_args(["export", "AAPL", "--to", "/tmp/out"])
        assert args.include is None

    def test_dry_run_flag(self):
        parser = _build_parser()
        args = parser.parse_args(["export", "AAPL", "--to", "/tmp", "--dry-run"])
        assert args.dry_run is True

    def test_force_live_flag(self):
        parser = _build_parser()
        args = parser.parse_args(["export", "AAPL", "--to", "/tmp", "--force-live"])
        assert args.force_live is True

    def test_history_days_default(self):
        parser = _build_parser()
        args = parser.parse_args(["export", "AAPL", "--to", "/tmp"])
        assert args.history_days == 365

    def test_version_subcommand(self):
        parser = _build_parser()
        args = parser.parse_args(["version"])
        assert args.command == "version"

    def test_quote_subcommand(self):
        parser = _build_parser()
        args = parser.parse_args(["quote", "AAPL", "MSFT"])
        assert args.tickers == ["AAPL", "MSFT"]

    def test_watchlists_subcommand(self):
        parser = _build_parser()
        args = parser.parse_args(["watchlists"])
        assert args.command == "watchlists"


# ---------------------------------------------------------------------------
# _export_ticker
# ---------------------------------------------------------------------------

class TestExportTicker:
    async def test_writes_all_12_files_by_default(self, tmp_path):
        api = _make_api(tmp_path)
        written = await _export_ticker(
            api, "AAPL", tmp_path, _ALL_INCLUDES,
            history_days=365, intraday_days=5,
            force_live=False, dry_run=False,
        )
        names = {p.name for p in written}
        assert "AAPL_daily_data.csv" in names
        assert "AAPL_5m_data.csv" in names
        assert "AAPL_options_chain.json" in names
        assert "AAPL_fundamentals.json" in names
        assert "AAPL_income_statement.csv" in names
        assert "AAPL_quarterly_income_statement.csv" in names
        assert "AAPL_balance_sheet.csv" in names
        assert "AAPL_quarterly_balance_sheet.csv" in names
        assert "AAPL_cash_flow.csv" in names
        assert "AAPL_quarterly_cash_flow.csv" in names
        assert "AAPL_eps.csv" in names
        assert "AAPL_popularity.csv" in names
        assert len(written) == 12

    async def test_include_filter_ohlcv_daily_only(self, tmp_path):
        api = _make_api(tmp_path)
        written = await _export_ticker(
            api, "AAPL", tmp_path, frozenset({"ohlcv-daily"}),
            365, 5, False, False,
        )
        assert len(written) == 1
        assert written[0].name == "AAPL_daily_data.csv"
        api.get_5m_history.assert_not_called()
        api.get_options_chain.assert_not_called()

    async def test_include_filter_ohlcv_daily_and_options(self, tmp_path):
        api = _make_api(tmp_path)
        written = await _export_ticker(
            api, "AAPL", tmp_path, frozenset({"ohlcv-daily", "options"}),
            365, 5, False, False,
        )
        assert len(written) == 2
        names = {p.name for p in written}
        assert "AAPL_daily_data.csv" in names
        assert "AAPL_options_chain.json" in names

    async def test_dry_run_writes_nothing(self, tmp_path):
        api = _make_api(tmp_path)
        written = await _export_ticker(
            api, "AAPL", tmp_path, _ALL_INCLUDES,
            365, 5, False, dry_run=True,
        )
        assert written == []
        assert list(tmp_path.iterdir()) == []

    async def test_force_live_propagated(self, tmp_path):
        api = _make_api(tmp_path)
        await _export_ticker(
            api, "AAPL", tmp_path, frozenset({"ohlcv-daily"}),
            365, 5, force_live=True, dry_run=False,
        )
        api.get_daily_history.assert_called_once()
        _, kwargs = api.get_daily_history.call_args
        assert kwargs.get("force_live") is True or api.get_daily_history.call_args.args[3] is True

    async def test_partial_fetch_failure_does_not_abort(self, tmp_path):
        api = _make_api(tmp_path)
        api.get_options_chain = AsyncMock(side_effect=RuntimeError("rate limit"))
        written = await _export_ticker(
            api, "AAPL", tmp_path, frozenset({"ohlcv-daily", "options"}),
            365, 5, False, False,
        )
        names = {p.name for p in written}
        assert "AAPL_daily_data.csv" in names
        assert "AAPL_options_chain.json" not in names


# ---------------------------------------------------------------------------
# Full async_main dispatch
# ---------------------------------------------------------------------------

class TestAsyncMain:
    async def test_export_calls_export_ticker(self, tmp_path):
        api = _make_api(tmp_path)
        parser = _build_parser()
        args = parser.parse_args(["export", "AAPL", "--to", str(tmp_path)])

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        api.get_daily_history.assert_called()

    async def test_export_watchlist_expansion(self, tmp_path):
        api = _make_api(tmp_path)
        api.get_watchlist_tickers = AsyncMock(return_value=["NVDA", "AMD"])
        parser = _build_parser()
        args = parser.parse_args([
            "export", "--to", str(tmp_path), "--watchlist", "Tech",
            "--include", "ohlcv-daily",
        ])
        args.tickers = []  # no positional tickers — only watchlist

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        api.get_watchlist_tickers.assert_called_once_with("Tech")
        assert api.get_daily_history.call_count == 2  # NVDA + AMD

    async def test_quote_single_emits_json_to_stdout(self, tmp_path, capsys):
        api = _make_api(tmp_path)
        parser = _build_parser()
        args = parser.parse_args(["quote", "AAPL"])

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        captured = capsys.readouterr().out
        data = json.loads(captured)
        assert "lastPrice" in data

    async def test_quote_multi_calls_get_quotes(self, tmp_path, capsys):
        api = _make_api(tmp_path)
        parser = _build_parser()
        args = parser.parse_args(["quote", "AAPL", "MSFT"])

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        api.get_quotes.assert_called_once_with(["AAPL", "MSFT"])

    async def test_info_subcommand(self, tmp_path, capsys):
        api = _make_api(tmp_path)
        parser = _build_parser()
        args = parser.parse_args(["info", "AAPL"])

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        api.get_ticker_info.assert_called_once_with("AAPL")
        captured = capsys.readouterr().out
        data = json.loads(captured)
        assert data["ticker"] == "AAPL"

    async def test_watchlists_subcommand(self, tmp_path, capsys):
        api = _make_api(tmp_path)
        parser = _build_parser()
        args = parser.parse_args(["watchlists"])

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        api.get_watchlists.assert_called_once()
        captured = capsys.readouterr().out
        assert "Tech" in captured
        assert "Biotech" in captured

    async def test_force_live_flag_propagates(self, tmp_path):
        api = _make_api(tmp_path)
        parser = _build_parser()
        args = parser.parse_args([
            "export", "AAPL", "--to", str(tmp_path),
            "--include", "ohlcv-daily", "--force-live",
        ])

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        call_kwargs = api.get_daily_history.call_args
        force_live = (
            call_kwargs.kwargs.get("force_live")
            or (len(call_kwargs.args) > 3 and call_kwargs.args[3])
        )
        assert force_live is True

    async def test_db_pool_closed_after_export(self, tmp_path):
        api = _make_api(tmp_path)
        parser = _build_parser()
        args = parser.parse_args(["export", "AAPL", "--to", str(tmp_path),
                                   "--include", "ohlcv-daily"])

        with patch("rocketstocks.api._factory.build_data_api", new=AsyncMock(return_value=api)):
            await _async_main(args)

        api._sd.db.close.assert_called_once()

    def test_version_subcommand_no_api_needed(self, capsys):
        parser = _build_parser()
        args = parser.parse_args(["version"])

        with patch("rocketstocks.api._factory.build_data_api") as mock_factory:
            asyncio.run(_async_main(args))
            mock_factory.assert_not_called()

        assert "rocketstocks" in capsys.readouterr().out
