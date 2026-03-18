import math
import logging

logger = logging.getLogger(__name__)

_FINVIZ_BASE = "https://finviz.com/quote.ashx?t="


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


def format_signed_pct(value: float, decimals: int = 2) -> str:
    """Format a percentage value with an explicit '+' sign for positives."""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{decimals}f}%"


def change_emoji(value: float) -> str:
    """Return 🟢 for positive values, 🔻 for non-positive."""
    return "🟢" if value > 0 else "🔻"


def finviz_url(ticker: str) -> str:
    """Return the FinViz quote URL for a ticker."""
    return f"{_FINVIZ_BASE}{ticker}"


def is_valid_number(val: object) -> bool:
    """Return True if val is not None and not NaN."""
    return val is not None and not (isinstance(val, float) and math.isnan(val))


def get_company_name(ticker_info: dict | None, fallback: str = "") -> str:
    """Extract company name from ticker_info dict, falling back to fallback."""
    return (ticker_info or {}).get('name', fallback)


def earnings_time_label(time_raw: str | None, fallback: str = "N/A") -> str:
    """Convert raw earnings time string to a human-readable label."""
    if isinstance(time_raw, list):
        time_raw = time_raw[0] if time_raw else None
    if not time_raw:
        return fallback
    s = str(time_raw).lower()
    if 'pre-market' in s or 'pre market' in s:
        return 'Pre-market'
    if 'after-hours' in s or 'after hours' in s:
        return 'After Hours'
    return fallback
