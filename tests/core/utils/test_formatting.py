"""Tests for rocketstocks.core.utils.formatting."""
import pytest
from rocketstocks.core.utils.formatting import ticker_string, format_large_num


class TestTickerString:
    def test_single_ticker(self):
        result = ticker_string(["AAPL"])
        assert result == "`AAPL`"

    def test_multiple_tickers(self):
        result = ticker_string(["AAPL", "MSFT", "GOOG"])
        assert result == "`AAPL, MSFT, GOOG`"

    def test_empty_list(self):
        result = ticker_string([])
        assert result == "``"

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
