"""Tests for rocketstocks.core.utils.formatting."""
import pytest
from rocketstocks.core.utils.formatting import (
    ticker_string, format_large_num,
    format_signed_pct, change_emoji, finviz_url,
    is_valid_number, get_company_name, earnings_time_label,
)


class TestTickerString:
    def test_single_ticker(self):
        result = ticker_string(["AAPL"])
        assert result == "`AAPL`"

    def test_multiple_tickers(self):
        result = ticker_string(["AAPL", "MSFT", "GOOG"])
        assert result == "`AAPL, MSFT, GOOG`"

    def test_empty_list(self):
        result = ticker_string([])
        assert result == ""

    def test_wraps_in_backticks(self):
        result = ticker_string(["X"])
        assert result.startswith("`")
        assert result.endswith("`")


class TestFormatLargeNum:
    @pytest.mark.parametrize("number, expected", [
        (0, "0"),
        (500, "500"),
        (999, "999"),
        (1_000, "1K"),
        (1_500, "1.5K"),
        (1_000_000, "1M"),
        (1_500_000, "1.5M"),
        (1_000_000_000, "1B"),
        (2_300_000_000, "2.3B"),
        (1_000_000_000_000, "1T"),
    ])
    def test_standard_cases(self, number, expected):
        assert format_large_num(number) == expected

    def test_none_returns_na(self):
        assert format_large_num(None) == "N/A"

    def test_string_number(self):
        assert format_large_num("2000000") == "2M"

    def test_negative_number(self):
        result = format_large_num(-1_000_000)
        assert "M" in result
        assert "-" in result

    def test_float_input(self):
        result = format_large_num(1_234_567.89)
        assert "M" in result


class TestFormatSignedPct:
    def test_positive_has_plus_sign(self):
        assert format_signed_pct(3.5) == "+3.50%"

    def test_negative_has_no_plus_sign(self):
        assert format_signed_pct(-2.1) == "-2.10%"

    def test_zero_has_no_plus_sign(self):
        assert format_signed_pct(0.0) == "0.00%"

    def test_custom_decimals(self):
        assert format_signed_pct(1.5, decimals=1) == "+1.5%"

    def test_large_value(self):
        result = format_signed_pct(100.0)
        assert result == "+100.00%"


class TestChangeEmoji:
    def test_positive_returns_green(self):
        assert change_emoji(1.0) == "🟢"

    def test_negative_returns_red(self):
        assert change_emoji(-1.0) == "🔻"

    def test_zero_returns_red(self):
        assert change_emoji(0.0) == "🔻"


class TestFinvizUrl:
    def test_returns_correct_url(self):
        assert finviz_url("AAPL") == "https://finviz.com/quote.ashx?t=AAPL"

    def test_lowercase_ticker(self):
        result = finviz_url("tsla")
        assert "tsla" in result


class TestIsValidNumber:
    def test_valid_int(self):
        assert is_valid_number(42) is True

    def test_valid_float(self):
        assert is_valid_number(3.14) is True

    def test_none_is_invalid(self):
        assert is_valid_number(None) is False

    def test_nan_is_invalid(self):
        import math
        assert is_valid_number(float('nan')) is False

    def test_zero_is_valid(self):
        assert is_valid_number(0) is True

    def test_negative_is_valid(self):
        assert is_valid_number(-5.5) is True


class TestGetCompanyName:
    def test_returns_name_from_dict(self):
        assert get_company_name({'name': 'Apple Inc.'}) == 'Apple Inc.'

    def test_returns_fallback_when_no_name(self):
        assert get_company_name({}, fallback='AAPL') == 'AAPL'

    def test_returns_fallback_when_none(self):
        assert get_company_name(None, fallback='AAPL') == 'AAPL'

    def test_default_fallback_is_empty_string(self):
        assert get_company_name(None) == ''


class TestEarningsTimeLabel:
    def test_pre_market(self):
        assert earnings_time_label('pre-market') == 'Pre-market'

    def test_after_hours(self):
        assert earnings_time_label('after-hours') == 'After Hours'

    def test_list_input(self):
        assert earnings_time_label(['pre-market', 'other']) == 'Pre-market'

    def test_empty_list_returns_fallback(self):
        assert earnings_time_label([]) == 'N/A'

    def test_none_returns_fallback(self):
        assert earnings_time_label(None) == 'N/A'

    def test_unknown_string_returns_fallback(self):
        assert earnings_time_label('during-market') == 'N/A'

    def test_custom_fallback(self):
        assert earnings_time_label(None, fallback='Unknown') == 'Unknown'
