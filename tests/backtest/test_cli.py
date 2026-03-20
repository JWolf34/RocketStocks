"""Tests for rocketstocks.backtest.cli."""
import datetime
import json
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rocketstocks.backtest.cli import (
    _handle_list,
    _handle_stats,
    _handle_compare,
    _fmt,
    build_parser,
    main,
)
from rocketstocks.backtest.registry import _REGISTRY


# ---------------------------------------------------------------------------
# build_parser
# ---------------------------------------------------------------------------

def test_parser_run_command_exists():
    parser = build_parser()
    args = parser.parse_args(['run', 'alert_signal'])
    assert args.command == 'run'
    assert args.strategy == 'alert_signal'


def test_parser_run_defaults():
    parser = build_parser()
    args = parser.parse_args(['run', 'alert_signal'])
    assert args.timeframe == 'daily'
    assert args.cash == 10_000
    assert args.commission == 0.002


def test_parser_run_filter_flags():
    parser = build_parser()
    args = parser.parse_args([
        'run', 'alert_signal',
        '--classification', 'volatile', 'meme',
        '--sector', 'Technology',
        '--min-market-cap', '1000000000',
    ])
    assert args.classification == ['volatile', 'meme']
    assert args.sector == ['Technology']
    assert args.min_market_cap == 1_000_000_000


def test_parser_stats_command():
    parser = build_parser()
    args = parser.parse_args(['stats', '42'])
    assert args.command == 'stats'
    assert args.run_id == 42


def test_parser_compare_command():
    parser = build_parser()
    args = parser.parse_args(['compare', '1', '2'])
    assert args.run_id_a == 1
    assert args.run_id_b == 2


def test_parser_optimize_command():
    parser = build_parser()
    args = parser.parse_args([
        'optimize', 'alert_signal', 'AAPL',
        '--params', '{"hold_bars": [1, 3, 5]}',
        '--maximize', 'Win Rate [%]',
    ])
    assert args.strategy == 'alert_signal'
    assert args.ticker == 'AAPL'
    assert args.maximize == 'Win Rate [%]'


def test_parser_list_command():
    parser = build_parser()
    args = parser.parse_args(['list'])
    assert args.command == 'list'


# ---------------------------------------------------------------------------
# _handle_list
# ---------------------------------------------------------------------------

def test_handle_list_prints_strategies(capsys):
    with patch('rocketstocks.backtest.cli.list_strategies', return_value=['a', 'b', 'c']):
        rc = _handle_list()
    captured = capsys.readouterr()
    assert rc == 0
    assert 'a' in captured.out
    assert 'b' in captured.out


def test_handle_list_empty(capsys):
    with patch('rocketstocks.backtest.cli.list_strategies', return_value=[]):
        rc = _handle_list()
    captured = capsys.readouterr()
    assert rc == 0
    assert 'No strategies' in captured.out


# ---------------------------------------------------------------------------
# _handle_stats
# ---------------------------------------------------------------------------

async def test_handle_stats_run_not_found(capsys):
    mock_repo = MagicMock()
    mock_repo.get_run = AsyncMock(return_value=None)
    mock_repo.get_stats_by_run = AsyncMock(return_value=[])

    class FakeArgs:
        run_id = 999
        group = None

    rc = await _handle_stats(FakeArgs(), mock_repo)
    captured = capsys.readouterr()
    assert rc == 1
    assert 'not found' in captured.out


async def test_handle_stats_prints_run_info(capsys):
    mock_repo = MagicMock()
    mock_repo.get_run = AsyncMock(return_value={
        'run_id': 1,
        'strategy_name': 'alert_signal',
        'timeframe': 'daily',
        'ticker_count': 10,
        'start_date': datetime.date(2024, 1, 1),
        'end_date': None,
        'created_at': datetime.datetime(2026, 1, 1),
        'parameters': {},
    })
    mock_repo.get_stats_by_run = AsyncMock(return_value=[])

    class FakeArgs:
        run_id = 1
        group = None

    rc = await _handle_stats(FakeArgs(), mock_repo)
    captured = capsys.readouterr()
    assert rc == 0
    assert 'alert_signal' in captured.out
    assert 'daily' in captured.out


# ---------------------------------------------------------------------------
# _handle_compare
# ---------------------------------------------------------------------------

async def test_handle_compare_run_not_found(capsys):
    mock_repo = MagicMock()
    mock_repo.get_run = AsyncMock(return_value=None)
    mock_repo.get_successful_results_by_run = AsyncMock(return_value=[])

    class FakeArgs:
        run_id_a = 1
        run_id_b = 2

    rc = await _handle_compare(FakeArgs(), mock_repo)
    captured = capsys.readouterr()
    assert rc == 1
    assert 'not found' in captured.out


# ---------------------------------------------------------------------------
# main() — list subcommand does not need DB
# ---------------------------------------------------------------------------

async def test_main_list_does_not_open_db(capsys):
    with patch('rocketstocks.backtest.cli.list_strategies', return_value=['alert_signal']):
        rc = await main(['list'])
    assert rc == 0


# ---------------------------------------------------------------------------
# _fmt helper
# ---------------------------------------------------------------------------

def test_fmt_float():
    assert _fmt(3.14159) == '3.14'


def test_fmt_none():
    assert _fmt(None) == 'N/A'


def test_fmt_nan():
    assert _fmt(float('nan')) == 'NaN'


def test_fmt_custom_decimals():
    assert _fmt(1.23456, decimals=4) == '1.2346'
