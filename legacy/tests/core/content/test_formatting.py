"""Tests for rocketstocks.core.content.formatting standalone utilities."""
import pandas as pd
import pytest

from rocketstocks.core.content.formatting import (
    build_df_table,
    build_stats_table,
    format_large_num,
)


# ---------------------------------------------------------------------------
# build_df_table
# ---------------------------------------------------------------------------

def test_build_df_table_returns_code_block():
    df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    result = build_df_table(df)
    assert result.startswith("```")
    assert result.endswith("```")


def test_build_df_table_contains_column_headers():
    df = pd.DataFrame({'Ticker': ['AAPL'], 'Price': [150.0]})
    result = build_df_table(df)
    assert 'Ticker' in result
    assert 'Price' in result


def test_build_df_table_contains_row_values():
    df = pd.DataFrame({'Ticker': ['MSFT'], 'Vol': ['5M']})
    result = build_df_table(df)
    assert 'MSFT' in result
    assert '5M' in result


def test_build_df_table_unknown_style_falls_back():
    df = pd.DataFrame({'X': [1]})
    # Should not raise; unknown style falls back to double_thin_compact
    result = build_df_table(df, style='nonexistent_style')
    assert '```' in result


def test_build_df_table_borderless_style():
    df = pd.DataFrame({'A': ['foo'], 'B': ['bar']})
    result = build_df_table(df, style='borderless')
    assert 'foo' in result


# ---------------------------------------------------------------------------
# build_stats_table
# ---------------------------------------------------------------------------

def test_build_stats_table_returns_code_block():
    result = build_stats_table(header={}, body={'Key': 'Value'}, adjust='right')
    assert result.startswith('```')
    assert result.endswith('```\n')


def test_build_stats_table_body_key_value_present():
    result = build_stats_table(header={}, body={'EPS': '3.14'}, adjust='right')
    assert 'EPS' in result
    assert '3.14' in result


def test_build_stats_table_header_separator_drawn():
    result = build_stats_table(header={'Close': 100}, body={'1D': '+2%'}, adjust='right')
    assert '━' in result


def test_build_stats_table_no_header_no_separator():
    result = build_stats_table(header={}, body={'Key': 'Val'}, adjust='right')
    assert '━' not in result


def test_build_stats_table_left_adjust():
    result = build_stats_table(header={}, body={'Ticker': 'AAPL'}, adjust='left')
    assert 'AAPL' in result


def test_build_stats_table_invalid_adjust_defaults_to_left():
    # Any value other than 'right' should use left alignment
    result = build_stats_table(header={}, body={'K': 'V'}, adjust='center')
    assert 'V' in result


def test_build_stats_table_header_with_falsy_value_omits_colon():
    result = build_stats_table(header={'Section': None}, body={}, adjust='right')
    # Falsy value → key is rendered as a plain label (no colon appended)
    assert 'Section' in result
    assert 'Section:' not in result


def test_build_stats_table_header_with_truthy_value_rendered():
    result = build_stats_table(header={'Close': '150.00'}, body={}, adjust='right')
    assert 'Close' in result
    assert '150.00' in result


# ---------------------------------------------------------------------------
# format_large_num (re-exported)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("number, expected", [
    (1_000, '1K'),
    (1_500_000, '1.5M'),
    (2_300_000_000, '2.3B'),
    (0, '0'),
    (500, '500'),
])
def test_format_large_num_standard_cases(number, expected):
    assert format_large_num(number) == expected


def test_format_large_num_none_returns_na():
    assert format_large_num(None) == 'N/A'


def test_format_large_num_string_number():
    # Numeric strings should be parseable
    result = format_large_num('2000000')
    assert result == '2M'
