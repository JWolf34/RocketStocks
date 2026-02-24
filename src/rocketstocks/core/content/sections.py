"""Standalone section builder functions.

Each function accepts only the data it needs and returns a string fragment
ready to be assembled into a full report, screener, or alert message.
No class instantiation required.
"""
from __future__ import annotations

import datetime
import logging

import pandas as pd
import pandas_ta_classic as ta

from rocketstocks.core.content.formatting import (
    build_df_table,
    build_stats_table,
    format_large_num,
)
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------

def report_header(ticker: str) -> str:
    """Standard stock report header with ticker and today's date."""
    logger.debug("Building report header...")
    header = "# " + ticker + " Report " + date_utils.format_date_mdy(datetime.datetime.now(tz=date_utils.timezone()).date())
    return header + "\n\n"


def earnings_spotlight_header(ticker: str) -> str:
    """Header for the earnings spotlight report."""
    logger.debug("Building Earnings Spotlight Report header...")
    return f"# :bulb: Earnings Spotlight: {ticker}\n\n"


def popularity_report_header(filter_val: str) -> str:
    """Header for the popularity report."""
    logger.debug("Building Popularity Report header...")
    return f"# Most Popular Stocks ({filter_val}) {datetime.datetime.today().strftime('%m/%d/%Y')}\n\n"


def politician_report_header(politician_name: str) -> str:
    """Header for the politician report."""
    logger.debug("Building Politician Report header...")
    return f"# Politician Report: {politician_name}\n"


def news_report_header(query: str) -> str:
    """Header for the news report."""
    logger.debug("Building News Report header...")
    return f"## News articles for '{query}'\n\n"


def ticker_info_section(ticker_info: dict, quote: dict) -> str:
    """Ticker info section: name, sector, industry, asset type, exchange."""
    logger.debug("Building ticker info...")
    message = "## Ticker Info\n"

    columns = ['name', 'sector', 'industry', 'country']
    fmt_ticker_info = {}
    for key in columns:
        value = ticker_info[key]
        if value != 'NaN' and value:
            fmt_ticker_info[key.capitalize()] = value

    fmt_ticker_info['Asset'] = quote['assetSubType']
    fmt_ticker_info['Exchange'] = quote['reference']['exchangeName']

    message += build_stats_table(header={}, body=fmt_ticker_info, adjust='right')
    return message


def daily_summary_section(quote: dict) -> str:
    """Today's OHLCV summary section."""
    logger.debug("Building daily summary...")
    message = "## Today's Summary\n"
    OHLCV = {
        'Open': ["{:.2f}".format(quote['quote']['openPrice'])],
        'High': ["{:.2f}".format(quote['quote']['highPrice'])],
        'Low': ["{:.2f}".format(quote['quote']['lowPrice'])],
        'Close': ["{:.2f}".format(quote['regular']['regularMarketLastPrice'])],
        'Volume': [format_large_num(quote['quote']['totalVolume'])],
    }
    message += build_df_table(df=pd.DataFrame(OHLCV), style='borderless')
    message += '\n'
    return message


def performance_section(daily_price_history: pd.DataFrame, quote: dict) -> str:
    """Stock performance over recent intervals (1D, 5D, 1M, 3M, 6M)."""
    logger.debug("Building performance...")
    message = "## Performance\n\n"

    if not daily_price_history.empty:
        table_header = {}
        close = quote['regular']['regularMarketLastPrice']
        table_header['Close'] = close

        table_body = {}
        interval_map = {"1D": 1, "5D": 5, "1M": 30, "3M": 90, "6M": 180}

        today = datetime.datetime.now(tz=date_utils.timezone()).date()
        for label, interval in interval_map.items():
            interval_date = today - datetime.timedelta(days=interval)
            while interval_date.weekday() > 4:
                interval_date = interval_date - datetime.timedelta(days=1)

            interval_close = daily_price_history[daily_price_history['date'] == interval_date]['close']

            if not interval_close.empty:
                interval_close = interval_close.iloc[0]
                change = ((close - interval_close) / interval_close) * 100.0
            else:
                interval_close = 'N/A'
                change = None

            symbol = None
            if interval_close != 'N/A':
                symbol = "🔻" if change < 0 else "🟢"

            close_str = "{:.2f}".format(interval_close) if interval_close != 'N/A' else 'N/A'
            change_str = f"{symbol} {change:.2f}%" if (change is not None and symbol) else ''
            table_body[label] = f"{close_str:<5} {change_str}"

        message += build_stats_table(header=table_header, body=table_body, adjust='right')
    else:
        message += "No price data found for this stock\n"
    return message


def fundamentals_section(
    fundamentals: dict,
    quote: dict,
    daily_price_history: pd.DataFrame | None = None,
) -> str:
    """Stock fundamentals section: market cap, EPS, P/E, beta, dividend, shortable.

    If ``daily_price_history`` is provided, appends 52-week high/low and
    the current price's distance from the 52-week high.
    """
    logger.debug("Building ticker stats...")
    message = "## Fundamentals\n"
    table_body = {}

    if fundamentals:
        table_body['Market Cap'] = format_large_num(fundamentals['instruments'][0]['fundamental']['marketCap'])
        table_body['EPS'] = f"{'{:.2f}'.format(fundamentals['instruments'][0]['fundamental']['eps'])}"
        table_body['EPS TTM'] = f"{'{:.2f}'.format(fundamentals['instruments'][0]['fundamental']['epsTTM'])}"
        table_body['P/E Ratio'] = f"{'{:.2f}'.format(fundamentals['instruments'][0]['fundamental']['peRatio'])}"
        table_body['Beta'] = fundamentals['instruments'][0]['fundamental']['beta']
        table_body['Dividend'] = "Yes" if fundamentals['instruments'][0]['fundamental']['dividendAmount'] else "No"
        table_body['Shortable'] = "Yes" if quote['reference']['isShortable'] else "No"
        table_body['HTB'] = "Yes" if quote['reference']['isHardToBorrow'] else "No"

        if daily_price_history is not None and not daily_price_history.empty:
            close = quote['regular']['regularMarketLastPrice']
            w52_high = daily_price_history['high'].tail(252).max()
            w52_low = daily_price_history['low'].tail(252).min()
            from_high = ((close - w52_high) / w52_high) * 100.0
            table_body['52W High'] = f"${w52_high:.2f}"
            table_body['52W Low'] = f"${w52_low:.2f}"
            table_body['% From 52W High'] = f"{from_high:.2f}%"

        message += build_stats_table(header={}, body=table_body, adjust='right')
    else:
        message += "No fundamentals found"

    return message


def technical_signals_section(daily_price_history: pd.DataFrame) -> str:
    """Technical indicators section: RSI, MACD, ADX, and SMA cross."""
    logger.debug("Building technical signals...")
    message = "## Technical Signals\n"

    if daily_price_history is None or daily_price_history.empty:
        message += "No price data available for technical signals\n"
        return message

    close = daily_price_history['close']
    n = len(close)

    table_body = {}

    # RSI(14)
    if n >= 15:
        rsi_series = ta.rsi(close, length=14)
        rsi_val = rsi_series.iloc[-1] if rsi_series is not None and not rsi_series.empty else None
        if rsi_val is not None and not pd.isna(rsi_val):
            label = "Overbought" if rsi_val > 70 else "Oversold" if rsi_val < 30 else "Neutral"
            table_body['RSI (14)'] = f"{rsi_val:.1f}  — {label}"
        else:
            table_body['RSI (14)'] = "N/A"
    else:
        table_body['RSI (14)'] = "N/A"

    # MACD (12/26/9)
    if n >= 35:
        macd_df = ta.macd(close)
        if macd_df is not None and not macd_df.empty:
            macd_line = macd_df.iloc[-1, 0]   # MACD_12_26_9
            macd_hist = macd_df.iloc[-1, 1]   # MACDh_12_26_9
            if not pd.isna(macd_line) and not pd.isna(macd_hist):
                direction = "Bullish" if macd_hist > 0 else "Bearish"
                sign = "+" if macd_hist > 0 else ""
                table_body['MACD'] = f"{direction} (hist {sign}{macd_hist:.2f})"
            else:
                table_body['MACD'] = "N/A"
        else:
            table_body['MACD'] = "N/A"
    else:
        table_body['MACD'] = "N/A"

    # ADX(14)
    if n >= 28 and 'high' in daily_price_history.columns and 'low' in daily_price_history.columns:
        high = daily_price_history['high']
        low = daily_price_history['low']
        adx_df = ta.adx(close=close, high=high, low=low)
        if adx_df is not None and not adx_df.empty:
            adx_val = adx_df.iloc[-1, 0]   # ADX_14
            dip = adx_df.iloc[-1, 1]        # DMP_14
            din = adx_df.iloc[-1, 2]        # DMN_14
            if not pd.isna(adx_val):
                trend_label = "Trending" if adx_val > 25 else "Ranging"
                direction_arrow = "↑" if dip > din else "↓"
                table_body['Trend (ADX)'] = f"{adx_val:.1f}  — {trend_label} {direction_arrow}"
            else:
                table_body['Trend (ADX)'] = "N/A"
        else:
            table_body['Trend (ADX)'] = "N/A"
    else:
        table_body['Trend (ADX)'] = "N/A"

    # SMA 50/200 cross
    if n >= 200:
        sma50 = ta.sma(close, 50)
        sma200 = ta.sma(close, 200)
        s50 = sma50.iloc[-1] if sma50 is not None and not sma50.empty else None
        s200 = sma200.iloc[-1] if sma200 is not None and not sma200.empty else None
        if s50 is not None and s200 is not None and not pd.isna(s50) and not pd.isna(s200):
            if s50 > s200:
                table_body['50/200 MA'] = "50 > 200  — Golden Cross 🟢"
            else:
                table_body['50/200 MA'] = "50 < 200  — Death Cross 🔻"
        else:
            table_body['50/200 MA'] = "N/A"
    else:
        table_body['50/200 MA'] = "N/A (< 200 candles)"

    message += build_stats_table(header={}, body=table_body, adjust='right')
    return message


def popularity_section(popularity: pd.DataFrame) -> str:
    """Stock popularity ranking section over select intervals."""
    logger.debug("Building popularity...")
    message = "## Popularity\n"

    if not popularity.empty:
        table_header = {}
        now = date_utils.round_down_nearest_minute(30)
        popularity_today = popularity[(popularity['datetime'] == now)]
        current_rank = popularity_today['rank'].iloc[0] if not popularity_today.empty else 'N/A'
        table_header['Current'] = current_rank

        table_body = {}
        interval_map = {"High 1D": 1, "High 7D": 7, "High 1M": 30, "High 3M": 90, "High 6M": 180}

        for label, interval in interval_map.items():
            interval_date = now - datetime.timedelta(days=interval)
            interval_popularity = popularity[popularity['datetime'].between(interval_date, now)]
            if not interval_popularity.empty:
                max_rank = interval_popularity['rank'].min()
            else:
                max_rank = 'N/A'

            symbol = None
            if max_rank != 'N/A' and current_rank != 'N/A':
                if max_rank < current_rank:
                    symbol = "🔻"
                elif max_rank > current_rank:
                    symbol = "🟢"
                else:
                    symbol = '━'

            table_body[label] = f"{max_rank:<3} {f'{symbol} {max_rank - current_rank} spots' if symbol and current_rank != 'N/A' else 'No change'}"

        message += build_stats_table(header=table_header, body=table_body, adjust='right')
    else:
        message += "No popularity data found for this stock\n"
    return message


def recent_earnings_section(historical_earnings: pd.DataFrame) -> str:
    """Overview of the 4 most recent earnings reports with beat/miss streak."""
    logger.debug("Building recent earnings...")
    message = "## Recent Earnings Overview\n"

    if not historical_earnings.empty:
        column_map = {
            'date': 'Date Reported',
            'eps': 'EPS',
            'surprise': 'Surprise',
            'epsforecast': 'Estimate',
            'fiscalquarterending': 'Quarter',
        }
        recent_earnings = historical_earnings.tail(4)
        recent_earnings = recent_earnings.filter(list(column_map.keys()))
        recent_earnings = recent_earnings.rename(columns=column_map)
        recent_earnings['Date Reported'] = recent_earnings['Date Reported'].apply(
            lambda x: date_utils.format_date_mdy(x)
        )
        recent_earnings['Surprise'] = recent_earnings['Surprise'].apply(lambda x: f"{x}%")
        message += build_df_table(df=recent_earnings, style='borderless')

        # Beat/miss streak — walk backwards from the most recent report
        surprises = historical_earnings['surprise'].dropna().tolist()
        if surprises:
            last = surprises[-1]
            is_beat = last > 0
            streak = 0
            for s in reversed(surprises):
                if (s > 0) == is_beat:
                    streak += 1
                else:
                    break
            if is_beat:
                message += f"\n📈 Beat estimates **{streak}** straight quarter{'s' if streak != 1 else ''}\n"
            else:
                message += f"\n📉 Missed estimates **{streak}** straight quarter{'s' if streak != 1 else ''}\n"
    else:
        message += "No historical earnings found for this ticker"
    return message + "\n"


def sec_filings_section(recent_sec_filings: pd.DataFrame) -> str:
    """5 most recently released SEC filings."""
    logger.debug("Building latest SEC filings...")
    message = "## Recent SEC Filings\n\n"

    if not recent_sec_filings.empty:
        for filing in recent_sec_filings.head(5).to_dict(orient='records'):
            message += f"[Form {filing['form']} - {filing['filingDate']}]({filing['link']})\n"
    else:
        message += "This stock has no recent SEC filings\n"

    return message


def earnings_date_section(ticker: str, next_earnings_info: dict) -> str:
    """One-liner stating when the ticker reports earnings."""
    logger.debug("Building earnings date...")
    message = ''
    if next_earnings_info:
        message = f"{ticker} reports earnings on "
        message += f"{date_utils.format_date_mdy(next_earnings_info['date'])}, "
        earnings_time = next_earnings_info['time']
        if "pre-market" in earnings_time:
            message += "before market open"
        elif "after-hours" in earnings_time:
            message += "after market close"
        else:
            message += "time not specified"
        message += "\n"
    return message


def upcoming_earnings_summary_section(next_earnings_info: dict) -> str:
    """Detailed summary of the next earnings report."""
    logger.debug("Building upcoming earnings summary...")
    message = "## Next Earnings Summary\n"
    if next_earnings_info:
        fmt_earnings_info = {}
        fmt_earnings_info['Date'] = next_earnings_info['date']
        fmt_earnings_info['Time'] = (
            "Premarket" if "pre-market" in next_earnings_info['time']
            else "After hours" if "after-hours" in next_earnings_info['time']
            else "Not supplied"
        )
        fmt_earnings_info['Quarter'] = next_earnings_info['fiscal_quarter_ending']
        fmt_earnings_info['EPS Forecast'] = next_earnings_info['eps_forecast'] if len(next_earnings_info['eps_forecast']) > 0 else "N/A"
        fmt_earnings_info['Estimates'] = next_earnings_info['no_of_ests']
        fmt_earnings_info['Prev Rpt Date'] = next_earnings_info['last_year_rpt_dt']
        fmt_earnings_info['Prev Year EPS'] = next_earnings_info['last_year_eps']
        message += build_stats_table(header={}, body=fmt_earnings_info, adjust='right')
    else:
        message += "Stock has no upcoming earnings reports\n"
    return message


def politician_info_section(politician: dict, politician_facts: dict) -> str:
    """Politician party, state, and facts."""
    column_map = {'Party': 'party', 'State': 'state'}
    fmt_politician_info = {key: politician[val] for key, val in column_map.items()}
    politician_facts_combined = fmt_politician_info | politician_facts

    message = "## About\n"
    message += build_stats_table(header={}, body=politician_facts_combined, adjust='right')
    return message


def politician_trades_section(trades: pd.DataFrame) -> str:
    """Table of the politician's 10 most recent trades."""
    message = "## Latest Trades\n"
    message += build_df_table(df=trades.head(10))
    return message


def news_section(news: dict) -> str:
    """Up to 10 recent news articles as hyperlinks."""
    logger.debug("Building news...")
    report = ''
    for article in news['articles'][:10]:
        article_date = date_utils.format_date_from_iso(date=article['publishedAt']).strftime("%m/%d/%y %H:%M:%S EST")
        article_line = f"[{article['title']} - {article['source']['name']} ({article_date})](<{article['url']}>)\n"
        if len(report + article_line) <= 1900:
            report += article_line
        else:
            break
    return report


# ---------------------------------------------------------------------------
# Alert sections
# ---------------------------------------------------------------------------

def alert_header(label: str) -> str:
    """Generic alert header with rotating light emoji."""
    logger.debug("Building alert header...")
    return f"## :rotating_light: {label}\n\n\n"


def todays_change(
    ticker: str,
    pct_change: float,
    price: float | None = None,
    company_name: str | None = None,
) -> str:
    """One-liner showing ticker's percent change today.

    When ``price`` and ``company_name`` are supplied, the format is:
    **CompanyName** · `TICKER` is 🟢 **+4.23%** — **$187.50**
    """
    logger.debug("Building today's change...")
    symbol = "🟢" if pct_change > 0 else "🔻"
    sign = "+" if pct_change > 0 else ""
    pct_str = f"{symbol} **{sign}{pct_change:.2f}%**"
    if company_name and price is not None:
        return f"**{company_name}** · `{ticker}` is {pct_str} — **${price:.2f}**"
    return f"`{ticker}` is {pct_str}"


def volume_stats_section(
    quote: dict,
    daily_price_history: pd.DataFrame | None = None,
    rvol: float | None = None,
    rvol_at_time: float | None = None,
    avg_vol_at_time: float | None = None,
    time: str | None = None,
) -> str:
    """Volume statistics section used by volume alerts."""
    logger.debug("Building volume stats...")
    message = '## Volume Stats\n'
    volume_stats = {}

    if quote:
        volume_stats['Volume Today'] = format_large_num(quote['quote']['totalVolume'])

    if rvol is not None:
        volume_stats['Relative Volume (10 Day)'] = f"{rvol:.2f}x"

    if rvol_at_time is not None and avg_vol_at_time is not None:
        volume_stats[f'Relative Volume at Time ({time})'] = f"{rvol_at_time:.2f}x"
        volume_stats[f'Current Volume at Time ({time})'] = format_large_num(rvol_at_time * avg_vol_at_time)
        volume_stats[f'Average Volume at Time ({time})'] = format_large_num(avg_vol_at_time)

    if daily_price_history is not None and not daily_price_history.empty:
        volume_stats['Average Volume (10 Day)'] = format_large_num(daily_price_history['volume'].tail(10).mean())
        volume_stats['Average Volume (30 Day)'] = format_large_num(daily_price_history['volume'].tail(30).mean())
        volume_stats['Average Volume (90 Day)'] = format_large_num(daily_price_history['volume'].tail(90).mean())

    if volume_stats:
        message += build_stats_table(header={}, body=volume_stats, adjust='right')
    else:
        message += "No volume stats available"

    return message


def todays_sec_filings_section(recent_sec_filings: pd.DataFrame) -> str:
    """SEC filings released today."""
    logger.debug("Building today's SEC filings...")
    message = "## Today's SEC Filings\n\n"
    today_string = datetime.datetime.today().strftime("%Y-%m-%d")
    todays_filings = recent_sec_filings[recent_sec_filings['filingDate'] == today_string]
    for _, filing in todays_filings.iterrows():
        message += f"[Form {filing['form']} - {filing['filingDate']}]({filing['link']})\n"
    return message


def popularity_stats_section(popularity: pd.DataFrame) -> str:
    """Popularity ranking overview for alert messages."""
    logger.debug("Building popularity stats...")
    message = "## Popularity\n"

    if not popularity.empty:
        table_header = {}
        now = date_utils.round_down_nearest_minute(30)
        popularity_today = popularity[(popularity['datetime'] == now)]
        current_rank = popularity_today['rank'].iloc[0] if not popularity_today.empty else 'N/A'
        table_header['Current'] = current_rank

        table_body = {}
        interval_map = {
            "High Today": 0,
            "High 1D Ago": 1,
            "High 2D Ago": 2,
            "High 3D Ago": 3,
            "High 4D Ago": 4,
            "High 5D Ago": 5,
        }

        for label, interval in interval_map.items():
            interval_date = now.date() - datetime.timedelta(days=interval)
            interval_popularity = popularity[popularity['datetime'].dt.date == interval_date]
            if not interval_popularity.empty:
                max_rank = interval_popularity['rank'].min()
            else:
                max_rank = 'N/A'

            symbol = None
            if max_rank != 'N/A' and current_rank != 'N/A':
                if max_rank < current_rank:
                    symbol = "🔻"
                elif max_rank > current_rank:
                    symbol = "🟢"
                else:
                    symbol = '━'

            table_body[label] = f"{max_rank:<3} {f'{symbol} {max_rank - current_rank} spots' if symbol and current_rank != 'N/A' else 'No change'}"

        message += build_stats_table(header=table_header, body=table_body, adjust='right')
    else:
        message += "No popularity data found for this stock\n"

    return message
