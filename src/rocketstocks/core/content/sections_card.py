"""Card-format section builders — mobile-friendly, no multi-column ASCII tables.

Each function returns a plain string that can be embedded directly in an
EmbedSpec description. Data is presented as stacked per-record cards instead
of wide multi-column tables, which wrap unreadably on narrow mobile screens.

Used by all content class build() methods to format embed descriptions.
"""
from __future__ import annotations

import datetime
import logging

import pandas as pd
import pandas_ta_classic as ta

from rocketstocks.core.content.formatting import format_large_num
from rocketstocks.core.utils.dates import format_date_mdy, format_date_from_iso, timezone, round_down_nearest_minute

logger = logging.getLogger(__name__)


def ohlcv_card(quote: dict, daily_price_history: pd.DataFrame = None) -> str:
    """OHLCV — OHLC grouped on one line, Vol on a second line.

    Falls back to the most recent row of *daily_price_history* when the
    quote's open/high/low are zero (e.g. premarket before the session opens).
    """
    open_ = quote['quote']['openPrice']
    high = quote['quote']['highPrice']
    low = quote['quote']['lowPrice']
    close = quote['regular']['regularMarketLastPrice']
    vol = format_large_num(quote['quote']['totalVolume'])
    heading = "Today's Summary"

    if not open_ and daily_price_history is not None and not daily_price_history.empty:
        row = daily_price_history.iloc[-1]
        open_ = row['open']
        high = row['high']
        low = row['low']
        close = row['close']
        vol = format_large_num(row['volume'])
        heading = "Previous Session"

    return (
        f"__**{heading}**__\n"
        f"Open **${open_:.2f}** · High **${high:.2f}** · Low **${low:.2f}** · Close **${close:.2f}**\n"
        f"Vol **{vol}**\n\n"
    )


def recent_earnings_card(historical_earnings: pd.DataFrame, *, show_header: bool = True) -> str:
    """Recent earnings as stacked per-quarter cards instead of a multi-column table."""
    header = "__**Recent Earnings**__"
    if historical_earnings is None or historical_earnings.empty:
        return (header + "\n" if show_header else "") + "No historical earnings found\n\n"

    lines = [header] if show_header else []
    for _, row in historical_earnings.tail(4).iloc[::-1].iterrows():
        date_str = format_date_mdy(row['date'])
        eps = row['eps']
        surprise = row['surprise']
        estimate = row['epsforecast']
        quarter = row['fiscalquarterending']

        if pd.isna(surprise):
            beat_miss, surprise_str = "❓", "N/A"
        else:
            beat_miss = "✅" if surprise > 0 else "❌"
            sign = '+' if surprise > 0 else ''
            surprise_str = f"{sign}{surprise:.1f}%"

        eps_str = f"${eps:.2f}" if not pd.isna(eps) else "N/A"
        est_str = f"${estimate:.2f}" if not pd.isna(estimate) else "N/A"

        lines.append(f"{beat_miss} **{quarter}** — {date_str}")
        lines.append(f"EPS **{eps_str}** · Est **{est_str}** · Surprise **{surprise_str}**")

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
        emoji = "📈" if is_beat else "📉"
        word = "Beat" if is_beat else "Missed"
        lines.append(f"\n{emoji} {word} estimates **{streak}** straight quarter{'s' if streak != 1 else ''}")

    return '\n'.join(lines) + '\n\n'


def politician_trades_card(trades: pd.DataFrame, *, show_header: bool = True) -> str:
    """Politician trades as stacked per-trade cards instead of a multi-column table."""
    header = "__**Latest Trades**__"
    if trades is None or trades.empty:
        return (header + "\n" if show_header else "") + "No trades found\n\n"

    lines = [header] if show_header else []
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
            line2_parts.append(f"Vol **{row[vol_col]}**")
        if 'Market Cap' in data.columns:
            line2_parts.append(f"MCap **{row['Market Cap']}**")
        lines.append(line1)
        if line2_parts:
            lines.append(' · '.join(line2_parts))
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
            line2_parts.append(f"Vol **{row['Volume']}**")
        if 'Avg Volume (10 Day)' in data.columns:
            line2_parts.append(f"Avg **{row['Avg Volume (10 Day)']}**")
        if 'Market Cap' in data.columns:
            line2_parts.append(f"MCap **{row['Market Cap']}**")
        lines.append(line1)
        if line2_parts:
            lines.append(' · '.join(line2_parts))
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
        mentions_24h_val = row.get('Mentions 24H Ago')
        if mentions_24h_val is not None:
            try:
                mentions_24h = int(mentions_24h_val)
            except (TypeError, ValueError):
                mentions_24h = 'N/A'
        else:
            mentions_24h = 'N/A'
        lines.append(f"**#{rank} {ticker}** · {mentions} mentions")
        lines.append(f"24H ago: #{rank_24h} · {mentions_24h} mentions")
        lines.append("")
    return '\n'.join(lines).strip()


def performance_card(daily_price_history: pd.DataFrame, quote: dict) -> str:
    """Stock performance over recent intervals — header owns close, intervals on one dot-line."""
    if daily_price_history is None or daily_price_history.empty:
        return "__**Performance**__\nNo price data\n\n"

    close = quote['regular']['regularMarketLastPrice']
    interval_map = {"1D": 1, "5D": 5, "1M": 30, "3M": 90, "6M": 180}
    today = datetime.datetime.now(tz=timezone()).date()

    interval_parts = []
    for label, interval in interval_map.items():
        interval_date = today - datetime.timedelta(days=interval)
        while interval_date.weekday() > 4:
            interval_date -= datetime.timedelta(days=1)

        # First try exact match; if not found, look for most recent row before interval_date
        row = daily_price_history[daily_price_history['date'] == interval_date]['close']
        if row.empty:
            # Find the most recent row with date <= interval_date
            earlier = daily_price_history[daily_price_history['date'] <= interval_date]
            if not earlier.empty:
                row = earlier.sort_values('date', ascending=False).iloc[0:1]['close']
        
        if not row.empty:
            prev_close = row.iloc[0]
            change = ((close - prev_close) / prev_close) * 100.0
            symbol = "🔻" if change < 0 else "🟢"
            sign = "+" if change >= 0 else ""
            interval_parts.append(f"{label} {symbol} **{sign}{change:.2f}%**")
        else:
            interval_parts.append(f"{label} N/A")

    return (
        f"__**Performance**__ · Close **${close:.2f}**\n"
        + " · ".join(interval_parts[:3]) + "\n"
        + " · ".join(interval_parts[3:]) + "\n\n"
    )


def fundamentals_card(
    fundamentals: dict,
    quote: dict,
    daily_price_history: pd.DataFrame | None = None,
) -> str:
    """Stock fundamentals — metrics on one dot-line, optional 52W on a second dot-line."""
    if not fundamentals:
        return "__**Fundamentals**__\nNo fundamentals found\n\n"

    fund = fundamentals['instruments'][0]['fundamental']
    mcap = format_large_num(fund['marketCap'])
    eps_val = fund['eps']
    pe_val = fund['peRatio']
    eps = f"{eps_val:.2f}" if eps_val is not None else "N/A"
    pe = f"{pe_val:.2f}" if pe_val is not None else "N/A"
    beta = fund['beta']
    div = "Yes" if fund['dividendAmount'] else "No"
    short = "Yes" if quote['reference']['isShortable'] else "No"
    htb = "Yes" if quote['reference']['isHardToBorrow'] else "No"

    result = (
        "__**Fundamentals**__\n"
        f"MCap **{mcap}** · EPS **{eps}** · P/E **{pe}** · Beta **{beta}**\n"
        f"Div **{div}** · Short **{short}** · HTB **{htb}**\n"
    )

    if daily_price_history is not None and not daily_price_history.empty:
        close = quote['regular']['regularMarketLastPrice']
        w52_high = daily_price_history['high'].tail(252).max()
        w52_low = daily_price_history['low'].tail(252).min()
        from_high = ((close - w52_high) / w52_high) * 100.0
        sign = "+" if from_high >= 0 else ""
        result += f"52W High **${w52_high:.2f}** · 52W Low **${w52_low:.2f}** · **{sign}{from_high:.1f}%** from high\n"

    return result + "\n"


def technical_signals_card(daily_price_history: pd.DataFrame) -> str:
    """Key technical indicators — RSI+MACD on line 1, ADX+MA on line 2."""
    if daily_price_history is None or daily_price_history.empty:
        return "__**Technical Signals**__\nNo price data available\n\n"

    close = daily_price_history['close']
    n = len(close)
    parts = []

    # RSI(14)
    if n >= 15:
        rsi_s = ta.rsi(close, length=14)
        rsi_val = rsi_s.iloc[-1] if rsi_s is not None and not rsi_s.empty else None
        if rsi_val is not None and not pd.isna(rsi_val):
            label = "Overbought" if rsi_val > 70 else "Oversold" if rsi_val < 30 else "Neutral"
            parts.append(f"RSI **{rsi_val:.1f}** ({label})")
        else:
            parts.append("RSI **N/A**")
    else:
        parts.append("RSI **N/A**")

    # MACD (12/26/9)
    if n >= 35:
        macd_df = ta.macd(close)
        if macd_df is not None and not macd_df.empty:
            macd_hist = macd_df.iloc[-1, 1]
            if not pd.isna(macd_hist):
                direction = "Bullish" if macd_hist > 0 else "Bearish"
                sign = "+" if macd_hist > 0 else ""
                parts.append(f"MACD **{direction}** (hist {sign}{macd_hist:.2f})")
            else:
                parts.append("MACD **N/A**")
        else:
            parts.append("MACD **N/A**")
    else:
        parts.append("MACD **N/A**")

    # ADX(14)
    if n >= 28 and 'high' in daily_price_history.columns and 'low' in daily_price_history.columns:
        adx_df = ta.adx(close=close, high=daily_price_history['high'], low=daily_price_history['low'])
        if adx_df is not None and not adx_df.empty:
            adx_val = adx_df.iloc[-1, 0]
            dip = adx_df.iloc[-1, 1]
            din = adx_df.iloc[-1, 2]
            if not pd.isna(adx_val):
                trend = "Trending" if adx_val > 25 else "Ranging"
                arrow = "↑" if dip > din else "↓"
                parts.append(f"ADX **{adx_val:.1f}** ({trend} {arrow})")
            else:
                parts.append("ADX **N/A**")
        else:
            parts.append("ADX **N/A**")
    else:
        parts.append("ADX **N/A**")

    # SMA 50/200 cross
    if n >= 200:
        sma50 = ta.sma(close, 50)
        sma200 = ta.sma(close, 200)
        s50 = sma50.iloc[-1] if sma50 is not None and not sma50.empty else None
        s200 = sma200.iloc[-1] if sma200 is not None and not sma200.empty else None
        if s50 is not None and s200 is not None and not pd.isna(s50) and not pd.isna(s200):
            if s50 > s200:
                parts.append("50/200 MA **Golden Cross** 🟢")
            else:
                parts.append("50/200 MA **Death Cross** 🔻")
        else:
            parts.append("50/200 MA **N/A**")
    else:
        parts.append("50/200 MA **N/A** (< 200 candles)")

    # Group: RSI + MACD (momentum), ADX + 50/200 MA (trend)
    line1 = " · ".join(parts[0:2])
    line2 = " · ".join(parts[2:4])
    return "__**Technical Signals**__\n" + line1 + "\n" + line2 + "\n\n"


def popularity_card(popularity: pd.DataFrame) -> str:
    """Stock popularity — current rank, intraday point-in-time deltas, multi-day best-rank deltas, 24H mentions.

    Line 1: current rank + mentions
    Line 2: 2H / 4H / 8H point-in-time rank comparisons (±35 min tolerance)
    Line 3: 1D / 3D / 7D daily-best rank comparisons
    Line 4: 24H mention change (skipped when data is unavailable or zero)
    """
    if popularity is None or popularity.empty:
        return "__**Popularity**__\nNo popularity data\n\n"

    # Most recent row — fetch_popularity returns data ordered DESC by datetime
    latest = popularity.iloc[0]
    current_rank = latest.get('rank')
    if current_rank is None or pd.isna(current_rank):
        return "__**Popularity**__\nNo popularity data\n\n"
    current_rank = int(current_rank)

    mentions = latest.get('mentions')
    mentions_24h_ago = latest.get('mentions_24h_ago')
    now = datetime.datetime.now()

    def _rank_delta(past_rank) -> str:
        """↑N = gained spots (more popular), ↓N = lost spots (less popular)."""
        if past_rank is None or pd.isna(past_rank):
            return "N/A"
        delta = int(past_rank) - current_rank
        if delta > 0:
            return f"↑{delta}"
        if delta < 0:
            return f"↓{abs(delta)}"
        return "→0"

    # Intraday point-in-time lookups — find nearest row within ±35 min
    tolerance = pd.Timedelta(minutes=35)
    intraday_parts = []
    for label, hours in [("2H", 2), ("4H", 4), ("8H", 8)]:
        target_dt = now - datetime.timedelta(hours=hours)
        diffs = (popularity['datetime'] - target_dt).abs()
        idx = diffs.idxmin()
        if diffs[idx] <= tolerance:
            past_rank = int(popularity.loc[idx, 'rank'])
            intraday_parts.append(f"{label}: **#{past_rank}** ({_rank_delta(past_rank)})")
        else:
            intraday_parts.append(f"{label}: N/A")

    # Multi-day daily-best rank (min rank number = best position that day)
    daily_parts = []
    for label, days in [("1D best", 1), ("3D best", 3), ("7D best", 7)]:
        target_date = (now - datetime.timedelta(days=days)).date()
        day_rows = popularity[popularity['datetime'].dt.date == target_date]
        if not day_rows.empty:
            best_rank = int(day_rows['rank'].min())
            daily_parts.append(f"{label}: **#{best_rank}** ({_rank_delta(best_rank)})")
        else:
            daily_parts.append(f"{label}: N/A")

    # 24H mention change — skipped on zero or missing data
    mentions_line = ""
    try:
        m = int(mentions) if not pd.isna(mentions) else None
        m24 = int(mentions_24h_ago) if not pd.isna(mentions_24h_ago) else None
        if m is not None and m24 is not None and m24 != 0:
            delta_m = m - m24
            pct_m = (delta_m / m24) * 100
            sign = "+" if delta_m >= 0 else ""
            pct_sign = "+" if pct_m >= 0 else ""
            mentions_line = f"\n24H Mentions: {sign}{delta_m} ({pct_sign}{pct_m:.0f}%)"
    except (TypeError, ValueError):
        pass

    mentions_str = f" · **{mentions}** mentions" if mentions is not None and not pd.isna(mentions) else ""

    return (
        f"__**Popularity**__ · Rank **#{current_rank}**{mentions_str}\n"
        + " · ".join(intraday_parts) + "\n"
        + " · ".join(daily_parts)
        + mentions_line + "\n\n"
    )


def upcoming_earnings_card(next_earnings_info: dict) -> str:
    """Next earnings date and estimates as a compact card."""
    header = "__**Next Earnings**__\n"
    if not next_earnings_info:
        return header + "No upcoming earnings\n\n"

    date_str = format_date_mdy(next_earnings_info['date'])
    time_raw = next_earnings_info.get('time', '')
    if 'pre-market' in str(time_raw):
        time_label = "Pre-Market"
    elif 'after-hours' in str(time_raw):
        time_label = "After Hours"
    else:
        time_label = "Time N/A"

    quarter = next_earnings_info.get('fiscal_quarter_ending', 'N/A')
    eps_forecast = next_earnings_info.get('eps_forecast', '')
    no_of_ests = next_earnings_info.get('no_of_ests', 'N/A')
    last_year_eps = next_earnings_info.get('last_year_eps', 'N/A')
    last_year_rpt_dt = next_earnings_info.get('last_year_rpt_dt', 'N/A')

    line1 = f'**{date_str}** · {time_label} · {quarter}'
    eps_str = f"**${eps_forecast}**" if eps_forecast else "N/A"
    line2 = f"EPS Est {eps_str} ({no_of_ests} ests) · Last Year: **${last_year_eps}** ({last_year_rpt_dt})"

    return header + line1 + "\n" + line2 + "\n\n"


def politician_info_card(politician: dict, politician_facts: dict) -> str:
    """Politician party, state, and facts as a compact card."""
    header = "__**About**__"
    party = politician.get('party', 'N/A')
    state = politician.get('state', 'N/A')
    line1 = f"s**{party}** · **{state}**"

    lines = [header, line1]
    if politician_facts:
        facts_parts = [f"{k}: {v}" for k, v in politician_facts.items()]
        lines.append(" · ".join(facts_parts))

    return "\n".join(lines) + "\n\n"


def sec_filings_card(recent_sec_filings: pd.DataFrame) -> str:
    """5 most recent SEC filings as hyperlinks with bold header."""
    header = "__**Recent SEC Filings**__"
    if recent_sec_filings is None or recent_sec_filings.empty:
        return header + "\nNo recent SEC filings\n\n"

    lines = [header]
    for filing in recent_sec_filings.head(5).to_dict(orient='records'):
        lines.append(f"[Form {filing['form']} - {filing['filingDate']}]({filing['link']})")

    return "\n".join(lines) + "\n\n"


def earnings_date_card(ticker: str, next_earnings_info: dict) -> str:
    """One-liner stating when the ticker reports earnings."""
    if not next_earnings_info:
        return ''
    message = f"`{ticker}` reports earnings on "
    message += f"{format_date_mdy(next_earnings_info['date'])}, "
    earnings_time = next_earnings_info['time']
    if "pre-market" in earnings_time:
        message += "before market open"
    elif "after-hours" in earnings_time:
        message += "after market close"
    else:
        message += "time not specified"
    return message + "\n\n"


def news_card(news: dict) -> str:
    """Up to 10 recent news articles as hyperlinks."""
    report = ''
    for article in news['articles'][:10]:
        article_date = format_date_from_iso(date=article['publishedAt']).strftime("%m/%d/%y %H:%M:%S EST")
        article_line = f"[{article['title']} - {article['source']['name']} ({article_date})](<{article['url']}>)\n"
        if len(report + article_line) <= 1900:
            report += article_line
        else:
            break
    return report


def ticker_info_card(ticker_info: dict, quote: dict) -> str:
    """One-line compact ticker info for embed description header."""
    parts = []
    name = (ticker_info or {}).get('name', '')
    if name and name != 'NaN':
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
    country = (ticker_info or {}).get('country', '')
    if country and country != 'NaN':
        parts.append(country)
    exchange = quote.get('reference', {}).get('exchangeName', '')
    if exchange and exchange != 'NaN':
        parts.append(exchange)
    security_type = quote.get('quote', {}).get('securityType', '')
    if security_type and security_type != 'NaN':
        parts.append(security_type)
    return ' · '.join(parts)


def todays_change_card(quote: dict) -> str:
    """One-line price and percent change for embed description header."""
    pct = quote['quote'].get('netPercentChange', 0)
    price = quote['regular']['regularMarketLastPrice']
    symbol = '🟢' if pct > 0 else '🔻'
    sign = '+' if pct > 0 else ''
    return f"{symbol} **{sign}{pct:.2f}%** — **${price:.2f}**"


_ALERT_TYPE_LABELS = {
    'EARNINGS_MOVER': '🚨 Earnings Mover',
    'WATCHLIST_MOVER': '👀 Watchlist Mover',
    'MARKET_ALERT': '📈 Market Alert',
    'POPULARITY_SURGE': '🔥 Popularity Surge',
    'MOMENTUM_CONFIRMATION': '⚡ Momentum Confirmation',
}


def recent_alerts_card(recent_alerts: list) -> str:
    """Today's alerts for the ticker with Discord jump URLs. Returns '' when empty."""
    if not recent_alerts:
        return ''

    lines = ['__**Recent Alerts**__']
    for alert in recent_alerts:
        date_obj = alert['date']
        alert_type = alert['alert_type']
        url = alert.get('url')
        label = _ALERT_TYPE_LABELS.get(alert_type, alert_type)
        try:
            date_str = date_obj.strftime('%m/%d') if hasattr(date_obj, 'strftime') else str(date_obj)
        except Exception:
            date_str = str(date_obj)
        if url:
            lines.append(f"{label} · {date_str} · [View](<{url}>)")
        else:
            lines.append(f"{label} · {date_str}")

    return '\n'.join(lines) + '\n\n'


def earnings_result_card(eps_actual: float, eps_estimate: float | None, surprise_pct: float | None) -> str:
    """Earnings result card — beat/miss headline with EPS and surprise details."""
    lines = ["__**Earnings Result**__"]

    if surprise_pct is not None:
        beat = surprise_pct >= 0
        emoji = "✅" if beat else "❌"
        word = "Beat" if beat else "Missed"
        sign = "+" if surprise_pct >= 0 else ""
        lines.append(f"{emoji} **{word}** by **{sign}{surprise_pct:.1f}%**")
    else:
        lines.append("Result available")

    detail_parts = [f"EPS **${eps_actual:.2f}**"]
    if eps_estimate is not None:
        detail_parts.append(f"Est **${eps_estimate:.2f}**")
        if surprise_pct is not None:
            surprise_abs = eps_actual - eps_estimate
            s_sign = "+" if surprise_abs >= 0 else ""
            detail_parts.append(f"Surprise **{s_sign}${surprise_abs:.2f}**")
    lines.append(" · ".join(detail_parts))

    return "\n".join(lines) + "\n\n"


def weekly_earnings_cards(data: pd.DataFrame, watchlist_tickers: list[str]) -> str:
    """Upcoming earnings grouped by day as stacked cards."""
    import datetime as dt
    lines = []
    today = round_down_nearest_minute(1).date()

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
