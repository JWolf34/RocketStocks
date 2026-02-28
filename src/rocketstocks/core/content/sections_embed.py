"""Compact embed description helpers for the Hybrid embed style.

These return plain strings used in EmbedSpec.description — not EmbedField objects.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def ticker_info_description(ticker_info: dict, quote: dict) -> str:
    """One-line compact ticker info for embed description header."""
    parts = []
    name = (ticker_info or {}).get('name', '')
    if name:
        parts.append(f"**{name}**")
    ticker = quote.get('symbol', '')
    if ticker:
        parts.append(f"`{ticker}`")
    sector = (ticker_info or {}).get('sector', '')
    if sector and sector != 'NaN':
        parts.append(sector)
    industry = (ticker_info or {}).get('industry', '')
    if industry and industry != 'NaN':
        parts.append(industry)
    exchange = quote.get('reference', {}).get('exchangeName', '')
    if exchange:
        parts.append(exchange)
    return ' · '.join(parts)


def todays_change_description(quote: dict) -> str:
    """One-line price and percent change for embed description header."""
    pct = quote['quote'].get('netPercentChange', 0)
    price = quote['regular']['regularMarketLastPrice']
    symbol = '🟢' if pct > 0 else '🔻'
    sign = '+' if pct > 0 else ''
    return f"{symbol} **{sign}{pct:.2f}%** — **${price:.2f}**"
