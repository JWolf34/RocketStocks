import logging

logger = logging.getLogger(__name__)


def ticker_string(tickers: list[str]) -> str:
    """Return comma-separated tickers in a code snippet. Empty list → empty string."""
    if not tickers:
        return ""
    return f"`{', '.join(tickers)}`"


def format_large_num(number: int | float | str | None) -> str:
    """Format large numbers to be human readable. i.e. 300M, 1.2B"""
    suffixes = ['', 'K', 'M', 'B', 'T']
    try:
        number = float('{:.3g}'.format(float(number)))
        magnitude = 0
        while abs(number) >= 1000:
            magnitude += 1
            number /= 1000.0
        magnitude = min(magnitude, len(suffixes) - 1)
        return '{}{}'.format('{:f}'.format(number).rstrip('0').rstrip('.'), suffixes[magnitude])
    except (TypeError, ValueError):
        return "N/A"
