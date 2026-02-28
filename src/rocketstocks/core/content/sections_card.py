"""Card-format section builders — mobile-friendly, no multi-column ASCII tables.

Each function returns a plain string that can be embedded directly in an
EmbedSpec description. Data is presented as stacked per-record cards instead
of wide multi-column tables, which wrap unreadably on narrow mobile screens.

Used exclusively by build_embed_spec() methods — build_report() is unchanged.
"""
from __future__ import annotations

import logging

import pandas as pd

from rocketstocks.core.content.formatting import format_large_num
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


def ohlcv_card(quote: dict) -> str:
    """OHLCV as a single compact line instead of a multi-column table."""
    open_ = quote['quote']['openPrice']
    high = quote['quote']['highPrice']
    low = quote['quote']['lowPrice']
    close = quote['regular']['regularMarketLastPrice']
    vol = format_large_num(quote['quote']['totalVolume'])
    return (
        "**Today's Summary**\n"
        f"Open **${open_:.2f}** · High **${high:.2f}** · Low **${low:.2f}** · "
        f"Close **${close:.2f}** · Vol **{vol}**\n\n"
    )


def recent_earnings_card(historical_earnings: pd.DataFrame) -> str:
    """Recent earnings as stacked per-quarter cards instead of a multi-column table."""
    header = "**Recent Earnings**\n"
    if historical_earnings is None or historical_earnings.empty:
        return header + "No historical earnings found\n\n"

    lines = [header]
    for _, row in historical_earnings.tail(4).iterrows():
        date_str = date_utils.format_date_mdy(row['date'])
        eps = row['eps']
        surprise = row['surprise']
        estimate = row['epsforecast']
        quarter = row['fiscalquarterending']
        beat_miss = "✅" if surprise > 0 else "❌"
        sign = '+' if surprise > 0 else ''
        lines.append(f"{beat_miss} **{quarter}** — {date_str}")
        lines.append(f"EPS **${eps:.2f}** · Est **${estimate:.2f}** · Surprise **{sign}{surprise:.1f}%**")

    surprises = historical_earnings['surprise'].dropna().tolist()
    if surprises:
        last = surprises[-1]
        is_beat = last > 0
        streak = sum(1 for s in reversed(surprises) if (s > 0) == is_beat)
        emoji = "📈" if is_beat else "📉"
        word = "Beat" if is_beat else "Missed"
        lines.append(f"\n{emoji} {word} estimates **{streak}** straight quarter{'s' if streak != 1 else ''}")

    return '\n'.join(lines) + '\n\n'


def politician_trades_card(trades: pd.DataFrame) -> str:
    """Politician trades as stacked per-trade cards instead of a multi-column table."""
    header = "**Latest Trades**\n"
    if trades is None or trades.empty:
        return header + "No trades found\n\n"

    lines = [header]
    for _, row in trades.head(10).iterrows():
        ticker = row.get('Ticker', 'N/A')
        pub_date = row.get('Published Date', 'N/A')
        order_type = row.get('Order Type', 'N/A')
        order_size = row.get('Order Size', 'N/A')
        filed_after = row.get('Filed After', 'N/A')
        lines.append(f"**{ticker}** · {pub_date} · {order_type}")
        lines.append(f"Size {order_size} · Filed {filed_after} after trade")

    return '\n'.join(lines) + '\n\n'


def gainer_screener_cards(data: pd.DataFrame, limit: int = 15) -> str:
    """Gainer screener rows as stacked cards."""
    vol_col = next((c for c in data.columns if 'Volume' in c), None)
    lines = []
    for _, row in data[:limit].iterrows():
        ticker = row['Ticker']
        change = row['Change (%)']
        price = row.get('Price', 'N/A')
        try:
            price_str = f"${float(price):.2f}"
        except (TypeError, ValueError):
            price_str = str(price)
        line1 = f"**{ticker}** · {change} · {price_str}"
        line2_parts = []
        if vol_col:
            line2_parts.append(f"Vol {row[vol_col]}")
        if 'Market Cap' in data.columns:
            line2_parts.append(f"MCap {row['Market Cap']}")
        lines.append(line1)
        if line2_parts:
            lines.append('  '.join(line2_parts))
        lines.append("")
    return '\n'.join(lines).strip()


def volume_screener_cards(data: pd.DataFrame, limit: int = 12) -> str:
    """Volume screener rows as stacked cards."""
    lines = []
    for _, row in data[:limit].iterrows():
        ticker = row['Ticker']
        rvol = row.get('Relative Volume (10 Day)', 'N/A')
        change = row.get('Change (%)', 'N/A')
        price = row.get('Price', 'N/A')
        try:
            price_str = f"${float(price):.2f}"
        except (TypeError, ValueError):
            price_str = str(price)
        line1 = f"**{ticker}** · RVOL {rvol} · {change} · {price_str}"
        line2_parts = []
        if 'Volume' in data.columns:
            line2_parts.append(f"Vol {row['Volume']}")
        if 'Avg Volume (10 Day)' in data.columns:
            line2_parts.append(f"Avg {row['Avg Volume (10 Day)']}")
        if 'Market Cap' in data.columns:
            line2_parts.append(f"MCap {row['Market Cap']}")
        lines.append(line1)
        if line2_parts:
            lines.append('  '.join(line2_parts))
        lines.append("")
    return '\n'.join(lines).strip()


def popularity_screener_cards(data: pd.DataFrame, limit: int = 20) -> str:
    """Popularity screener rows as stacked cards."""
    lines = []
    for _, row in data[:limit].iterrows():
        rank = row.get('Rank', '?')
        ticker = row.get('Ticker', 'N/A')
        mentions = row.get('Mentions', 'N/A')
        rank_24h = row.get('Rank 24H Ago', 'N/A')
        mentions_24h = row.get('Mentions 24H Ago', 'N/A')
        lines.append(f"**#{rank} {ticker}** · {mentions} mentions")
        lines.append(f"24H ago: #{rank_24h} · {mentions_24h} mentions")
        lines.append("")
    return '\n'.join(lines).strip()


def weekly_earnings_cards(data: pd.DataFrame, watchlist_tickers: list[str]) -> str:
    """Upcoming earnings grouped by day as stacked cards."""
    import datetime as dt
    lines = []
    today = date_utils.round_down_nearest_minute(1).date()

    for i in range(7):
        day = today + dt.timedelta(days=i)
        day_rows = data[data['Date'] == day]
        if day_rows.empty:
            continue
        day_label = day.strftime("**%A %m/%d**")
        lines.append(day_label)
        for _, row in day_rows.iterrows():
            ticker = row.get('Ticker', 'N/A')
            time_raw = row.get('Time', '')
            eps = row.get('EPS Forecast', 'N/A')
            star = " ⭐" if ticker in watchlist_tickers else ""
            if 'pre' in str(time_raw).lower():
                time_label = "pre-market"
            elif 'after' in str(time_raw).lower():
                time_label = "after hours"
            else:
                time_label = str(time_raw)
            lines.append(f"**{ticker}**{star} · {time_label} · EPS Est {eps}")
        lines.append("")
    return '\n'.join(lines).strip()
