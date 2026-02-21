import logging

logger = logging.getLogger(__name__)


def ticker_string(tickers: list):
    """Return string of comma-separated tickers encased in a code snippet"""
    return f"`{', '.join(tickers)}`"


def format_large_num(number):
    """Format large numbers to be human readable. i.e. 300M, 1.2B"""
    try:
        number = float('{:.3g}'.format(float(number)))
        magnitude = 0
        while abs(number) >= 1000:
            magnitude += 1
            number /= 1000.0
        return '{}{}'.format('{:f}'.format(number).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])
    except TypeError:
        return "N/A"
