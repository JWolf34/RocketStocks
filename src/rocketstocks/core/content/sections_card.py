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

    # Historical insights — all-time best rank, 30D averages, days tracked, 7D trend
    historical_parts = []
    try:
        best_rank = int(popularity['rank'].min())
        best_idx = popularity['rank'].idxmin()
        best_date = popularity.loc[best_idx, 'datetime']
        best_date_str = best_date.strftime("%-m/%-d/%y") if pd.notna(best_date) else "?"
        historical_parts.append(f"All-time best: **#{best_rank}** ({best_date_str})")
    except Exception:
        pass

    try:
        cutoff_30d = now - datetime.timedelta(days=30)
        recent_30d = popularity[popularity['datetime'] >= cutoff_30d]
        if not recent_30d.empty:
            avg_rank_30d = recent_30d['rank'].mean()
            historical_parts.append(f"30D avg rank: **#{avg_rank_30d:.0f}**")
            if 'mentions' in recent_30d.columns:
                avg_mentions_30d = recent_30d['mentions'].dropna().mean()
                if pd.notna(avg_mentions_30d) and avg_mentions_30d > 0:
                    historical_parts.append(f"30D avg mentions: **{avg_mentions_30d:.0f}**")
    except Exception:
        pass

    try:
        total_days = popularity['datetime'].dt.date.nunique()
        if total_days > 0:
            historical_parts.append(f"Days tracked: **{total_days}**")
    except Exception:
        pass

    # 7D rank trend: slope of daily-best-rank (lower rank = improving popularity)
    try:
        cutoff_7d = now - datetime.timedelta(days=7)
        recent_7d = popularity[popularity['datetime'] >= cutoff_7d].copy()
        if len(recent_7d) >= 3:
            daily_best = recent_7d.groupby(recent_7d['datetime'].dt.date)['rank'].min()
            if len(daily_best) >= 3:
                x = list(range(len(daily_best)))
                y = list(daily_best.values)
                n = len(x)
                slope = (n * sum(xi * yi for xi, yi in zip(x, y)) - sum(x) * sum(y)) / (n * sum(xi ** 2 for xi in x) - sum(x) ** 2)
                if slope < -2:
                    trend = "📈 Improving"
                elif slope > 2:
                    trend = "📉 Declining"
                else:
                    trend = "→ Stable"
                historical_parts.append(f"7D trend: **{trend}**")
    except Exception:
        pass

    historical_line = ""
    if historical_parts:
        historical_line = "\n" + " · ".join(historical_parts)

    return (
        f"__**Popularity**__ · Rank **#{current_rank}**{mentions_str}\n"
        + " · ".join(intraday_parts) + "\n"
        + " · ".join(daily_parts)
        + mentions_line
        + historical_line + "\n\n"
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


def classification_card(classification: str | None, volatility_20d: float | None) -> str:
    """Stock classification and 20-day volatility one-liner."""
    if not classification:
        return ''
    label = classification.replace('_', ' ').title()
    vol_str = f" · 20D Vol **{volatility_20d:.1f}%**" if volatility_20d is not None else ''
    return f"__**Classification**__\n**{label}**{vol_str}\n\n"


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


def financial_highlights_card(financials: dict) -> str:
    """Key financial statement metrics — Revenue, Income, Margins, Cash Flow (multi-period)."""
    header = "__**Financial Highlights**__"
    if not financials:
        return header + "\nNo financial data available\n\n"

    income = financials.get('quarterly_income_statement')
    if income is None or (hasattr(income, 'empty') and income.empty):
        income = financials.get('income_statement')
    cash_flow = financials.get('quarterly_cash_flow')
    if cash_flow is None or (hasattr(cash_flow, 'empty') and cash_flow.empty):
        cash_flow = financials.get('cash_flow')
    annual_income = financials.get('income_statement')

    if income is None or income.empty:
        return header + "\nNo financial data available\n\n"

    def _period_label(col) -> str:
        """Convert column (string date or Timestamp) to 'Q3'25' style."""
        try:
            dt = pd.Timestamp(col)
            quarter = (dt.month - 1) // 3 + 1
            return f"Q{quarter}'{dt.strftime('%y')}"
        except Exception:
            return str(col)[:7]

    def _get_val(df, col, *names):
        """Get a single cell value from a named row and specific column."""
        for name in names:
            if name in df.index and col in df.columns:
                val = df.loc[name, col]
                if pd.notna(val):
                    return float(val)
        return None

    def _pct_change(current, previous) -> str | None:
        """Format QoQ/YoY % change, return None if unavailable."""
        if current is None or previous is None or previous == 0:
            return None
        pct = (current - previous) / abs(previous) * 100
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.1f}%"

    lines = [header]

    # Sort columns descending (most recent first), take up to 2
    cols = list(income.columns)
    try:
        cols_sorted = sorted(cols, key=lambda c: pd.Timestamp(c), reverse=True)
    except Exception:
        cols_sorted = cols
    col_new = cols_sorted[0] if cols_sorted else None
    col_old = cols_sorted[1] if len(cols_sorted) > 1 else None

    if col_new is None:
        return header + "\nNo financial data available\n\n"

    label_new = _period_label(col_new)
    label_old = _period_label(col_old) if col_old else None

    # Revenue
    rev_new = _get_val(income, col_new, 'Total Revenue', 'Revenue', 'TotalRevenue')
    rev_old = _get_val(income, col_old, 'Total Revenue', 'Revenue', 'TotalRevenue') if col_old else None
    ni_new = _get_val(income, col_new, 'Net Income', 'NetIncome', 'Net Income Common Stockholders')
    ni_old = _get_val(income, col_old, 'Net Income', 'NetIncome', 'Net Income Common Stockholders') if col_old else None
    gp_new = _get_val(income, col_new, 'Gross Profit', 'GrossProfit')
    op_new = _get_val(income, col_new, 'Operating Income', 'OperatingIncome', 'EBIT')

    if rev_new is not None and rev_new != 0:
        rev_qoq = _pct_change(rev_new, rev_old)
        rev_line = f"**{label_new}** Revenue **{format_large_num(rev_new)}**"
        if rev_old is not None:
            rev_line += f" · **{label_old}** **{format_large_num(rev_old)}**"
            if rev_qoq:
                rev_line += f" (QoQ **{rev_qoq}**)"
        lines.append(rev_line)

        ni_qoq = _pct_change(ni_new, ni_old)
        if ni_new is not None:
            ni_line = f"Net Income **{format_large_num(ni_new)}**"
            if ni_old is not None:
                ni_line += f" · **{format_large_num(ni_old)}**"
                if ni_qoq:
                    ni_line += f" (QoQ **{ni_qoq}**)"
            lines.append(ni_line)

        if gp_new is not None:
            gm = gp_new / rev_new * 100
            om = op_new / rev_new * 100 if op_new is not None else None
            nm = ni_new / rev_new * 100 if ni_new is not None else None
            margin_parts = [f"Gross **{gm:.1f}%**"]
            if om is not None:
                margin_parts.append(f"Op **{om:.1f}%**")
            if nm is not None:
                margin_parts.append(f"Net **{nm:.1f}%**")
            lines.append("Margins — " + " · ".join(margin_parts))
    else:
        lines.append("Revenue N/A")

    # FCF / Operating Cash Flow
    if cash_flow is not None and not cash_flow.empty:
        cf_cols = list(cash_flow.columns)
        try:
            cf_cols_sorted = sorted(cf_cols, key=lambda c: pd.Timestamp(c), reverse=True)
        except Exception:
            cf_cols_sorted = cf_cols
        cf_col = cf_cols_sorted[0] if cf_cols_sorted else None
        if cf_col:
            ocf = _get_val(cash_flow, cf_col, 'Operating Cash Flow', 'Cash Flow From Operations', 'OperatingCashFlow')
            fcf = _get_val(cash_flow, cf_col, 'Free Cash Flow', 'FreeCashFlow')
            fcf_val = fcf if fcf is not None else ocf
            if fcf_val is not None:
                label = "FCF" if fcf is not None else "Op. Cash Flow"
                lines.append(f"{label} **{format_large_num(fcf_val)}**")

    # Annual YoY if 2+ annual periods available
    if annual_income is not None and not annual_income.empty:
        annual_cols = list(annual_income.columns)
        try:
            annual_cols_sorted = sorted(annual_cols, key=lambda c: pd.Timestamp(c), reverse=True)
        except Exception:
            annual_cols_sorted = annual_cols
        if len(annual_cols_sorted) >= 2:
            a_new = annual_cols_sorted[0]
            a_old = annual_cols_sorted[1]
            rev_a_new = _get_val(annual_income, a_new, 'Total Revenue', 'Revenue', 'TotalRevenue')
            rev_a_old = _get_val(annual_income, a_old, 'Total Revenue', 'Revenue', 'TotalRevenue')
            yoy = _pct_change(rev_a_new, rev_a_old)
            if rev_a_new is not None and yoy:
                a_label = _period_label(a_new).replace("Q1'", "FY'").replace("Q2'", "FY'").replace("Q3'", "FY'").replace("Q4'", "FY'")
                try:
                    a_label = f"FY'{pd.Timestamp(a_new).strftime('%y')}"
                except Exception:
                    pass
                lines.append(f"Annual Revenue **{format_large_num(rev_a_new)}** (YoY **{yoy}**)")

    return "\n".join(lines) + "\n\n"


def fundamentals_snapshot_card(fundamentals: dict) -> str:
    """Extended fundamentals card — valuation ratios, margins, short interest."""
    header = "__**Fundamentals Snapshot**__"
    if not fundamentals or not fundamentals.get('instruments'):
        return header + "\nNo fundamentals available\n\n"

    fund = fundamentals['instruments'][0]['fundamental']
    lines = [header]

    # Valuation ratios
    pe = fund.get('peRatio')
    pb = fund.get('pbRatio')
    pcf = fund.get('pcfRatio')
    eps = fund.get('epsTTM') or fund.get('eps')
    beta = fund.get('beta')
    ratio_parts = []
    if pe is not None:
        ratio_parts.append(f"P/E **{pe:.1f}**")
    if pb is not None:
        ratio_parts.append(f"P/B **{pb:.1f}**")
    if pcf is not None:
        ratio_parts.append(f"P/CF **{pcf:.1f}**")
    if eps is not None:
        ratio_parts.append(f"EPS **${eps:.2f}**")
    if beta is not None:
        ratio_parts.append(f"Beta **{beta:.2f}**")
    if ratio_parts:
        lines.append(" · ".join(ratio_parts))

    # Dividend + Dividend Yield
    div_amt = fund.get('dividendAmount')
    div_yield = fund.get('dividendYield')
    if div_amt:
        div_str = f"Div **${div_amt:.2f}**"
        if div_yield:
            div_str += f" (**{div_yield:.2f}%** yield)"
        lines.append(div_str)

    # Margins (TTM)
    gross_m = fund.get('grossMarginTTM')
    op_m = fund.get('operatingMarginTTM')
    net_m = fund.get('netProfitMarginTTM')
    roe = fund.get('returnOnEquity')
    roa = fund.get('returnOnAssets')
    margin_parts = []
    if gross_m is not None:
        margin_parts.append(f"Gross **{gross_m:.1f}%**")
    if op_m is not None:
        margin_parts.append(f"Op **{op_m:.1f}%**")
    if net_m is not None:
        margin_parts.append(f"Net **{net_m:.1f}%**")
    if margin_parts:
        lines.append("Margins TTM — " + " · ".join(margin_parts))

    eff_parts = []
    if roe is not None:
        eff_parts.append(f"ROE **{roe:.1f}%**")
    if roa is not None:
        eff_parts.append(f"ROA **{roa:.1f}%**")
    if eff_parts:
        lines.append(" · ".join(eff_parts))

    # Short interest
    si_ratio = fund.get('shortInterestRatio')
    si_shares = fund.get('shortInterestShares')
    shares_out = fund.get('sharesOutstanding')
    si_parts = []
    if si_ratio is not None:
        si_parts.append(f"Days to Cover **{si_ratio:.1f}**")
    if si_shares is not None and shares_out is not None and shares_out > 0:
        si_pct = (si_shares / shares_out) * 100
        si_parts.append(f"Short % **{si_pct:.1f}%**")
    elif si_shares is not None:
        si_parts.append(f"Short Shares **{format_large_num(si_shares)}**")
    if si_parts:
        lines.append("Short Interest — " + " · ".join(si_parts))

    return "\n".join(lines) + "\n\n"


def options_summary_card(options_chain: dict, current_price: float | None = None) -> str:
    """Key options metrics — IV, put/call ratio, nearest expiration activity."""
    from rocketstocks.core.analysis.options import (
        compute_put_call_stats, compute_max_pain, compute_iv_skew, detect_unusual_activity,
    )

    header = "__**Options Summary**__"
    if not options_chain or options_chain.get('status') == 'FAILED':
        return header + "\nNo options data available\n\n"

    lines = [header]

    # Root-level metrics
    chain_iv = options_chain.get('volatility')
    pcr = options_chain.get('putCallRatio')
    underlying = options_chain.get('underlyingPrice') or current_price

    # Nearest expiration — find ATM IV as primary
    call_map = options_chain.get('callExpDateMap', {})
    put_map = options_chain.get('putExpDateMap', {})

    atm_strike = None
    atm_iv = None
    nearest_exp = None
    if call_map:
        nearest_exp = sorted(call_map.keys())[0]
        exp_label = nearest_exp.split(':')[0]
        strikes = call_map[nearest_exp]

        # Find ATM strike (nearest to underlying price)
        if underlying:
            try:
                strike_floats = [(abs(float(s) - underlying), s) for s in strikes]
                strike_floats.sort()
                atm_strike_str = strike_floats[0][1]
                atm_contracts = strikes[atm_strike_str]
                if atm_contracts:
                    atm_iv = atm_contracts[0].get('volatility')
                    atm_strike = float(atm_strike_str)
            except (ValueError, KeyError, IndexError):
                pass

    # IV line: ATM IV is primary; chain-level shown as secondary reference
    iv_parts = []
    if atm_iv and atm_iv > 0:
        iv_parts.append(f"IV **{atm_iv:.1f}%** (ATM)")
    elif chain_iv and chain_iv > 0:
        iv_parts.append(f"IV **{chain_iv:.1f}%**")
    if chain_iv and chain_iv > 0 and atm_iv and atm_iv > 0:
        iv_parts.append(f"Chain IV **{chain_iv:.1f}%**")
    if pcr:
        iv_parts.append(f"P/C **{pcr:.2f}**")
    if iv_parts:
        lines.append(" · ".join(iv_parts))

    # Put/call aggregate volume & OI
    try:
        pc_stats = compute_put_call_stats(options_chain)
        call_vol = pc_stats.get('call_volume', 0)
        put_vol = pc_stats.get('put_volume', 0)
        call_oi = pc_stats.get('call_oi', 0)
        put_oi = pc_stats.get('put_oi', 0)
        if call_vol or put_vol:
            lines.append(
                f"Call Vol **{call_vol:,}** · Put Vol **{put_vol:,}** · "
                f"Call OI **{call_oi:,}** · Put OI **{put_oi:,}**"
            )
    except Exception:
        pass

    if nearest_exp:
        # Nearest expiration label
        lines.append(f"Nearest Exp **{exp_label}**")

        # Most active call + put by volume
        def _top_strike(exp_map, exp_key):
            if exp_key not in exp_map:
                return None
            strikes_data = exp_map[exp_key]
            best = None
            best_vol = 0
            for strike_str, contracts in strikes_data.items():
                for c in contracts:
                    vol = c.get('totalVolume', 0) or 0
                    if vol > best_vol:
                        best_vol = vol
                        best = (float(strike_str), vol, c.get('openInterest', 0))
            return best

        top_call = _top_strike(call_map, nearest_exp)
        top_put = _top_strike(put_map, nearest_exp)
        activity_parts = []
        if top_call:
            activity_parts.append(f"Call ${top_call[0]:.0f} **{top_call[1]:,}** vol")
        if top_put:
            activity_parts.append(f"Put ${top_put[0]:.0f} **{top_put[1]:,}** vol")
        if activity_parts:
            lines.append("Most Active — " + " · ".join(activity_parts))

    # Max pain
    try:
        if underlying:
            max_pain = compute_max_pain(options_chain)
            if max_pain:
                dist = max_pain - underlying
                sign = "+" if dist >= 0 else ""
                lines.append(f"Max Pain **${max_pain:.0f}** ({sign}{dist:.1f} from price)")
    except Exception:
        pass

    # IV skew
    try:
        if underlying and underlying > 0:
            skew = compute_iv_skew(options_chain, underlying)
            if skew:
                direction = skew['direction'].replace('_', ' ').title()
                lines.append(
                    f"IV Skew **{direction}** (put {skew['otm_put_iv']:.1f}% vs call {skew['otm_call_iv']:.1f}%)"
                )
    except Exception:
        pass

    # Unusual activity
    try:
        unusual = detect_unusual_activity(options_chain)
        if unusual:
            lines.append(f"Unusual Activity **{len(unusual)}** contract(s) flagged")
    except Exception:
        pass

    return "\n".join(lines) + "\n\n"


def tickers_summary_card(tickers_df) -> str:
    """Summary statistics for the tracked ticker universe."""
    header = "__**Tracked Tickers Summary**__"
    if tickers_df is None or (hasattr(tickers_df, 'empty') and tickers_df.empty):
        return header + "\nNo ticker data available\n\n"

    lines = [header]
    total = len(tickers_df)
    lines.append(f"Total Tickers **{total:,}**")

    # Active vs delisted
    if 'delist_date' in tickers_df.columns:
        delisted = tickers_df['delist_date'].notna().sum()
        active = total - delisted
        lines.append(f"Active **{active:,}** · Delisted **{delisted:,}**")

    # Sector breakdown (top 5, exclude NaN/empty/"NaN" strings)
    if 'sector' in tickers_df.columns:
        sector_counts = (
            tickers_df['sector']
            .dropna()
            .loc[lambda s: s.str.strip().ne('').values & s.ne('NaN').values]
            .value_counts()
            .head(5)
        )
        if not sector_counts.empty:
            sector_parts = [f"{s} **{n}**" for s, n in sector_counts.items()]
            lines.append("Top Sectors — " + " · ".join(sector_parts))

    # Security type breakdown (top 4)
    if 'security_type' in tickers_df.columns:
        type_counts = (
            tickers_df['security_type']
            .dropna()
            .loc[lambda s: s.str.strip().ne('').values & s.ne('NaN').values]
            .value_counts()
            .head(4)
        )
        if not type_counts.empty:
            type_parts = [f"{t} **{n}**" for t, n in type_counts.items()]
            lines.append("Security Types — " + " · ".join(type_parts))

    # Exchange breakdown (top 4)
    if 'exchange' in tickers_df.columns:
        exch_counts = (
            tickers_df['exchange']
            .dropna()
            .loc[lambda s: s.str.strip().ne('').values & s.ne('NaN').values]
            .value_counts()
            .head(4)
        )
        if not exch_counts.empty:
            exch_parts = [f"{e} **{n}**" for e, n in exch_counts.items()]
            lines.append("Exchanges — " + " · ".join(exch_parts))

    # Country breakdown (top 3, only if more than 1 unique country)
    if 'country' in tickers_df.columns:
        country_counts = (
            tickers_df['country']
            .dropna()
            .loc[lambda s: s.str.strip().ne('').values & s.ne('NaN').values]
            .value_counts()
        )
        if len(country_counts) > 1:
            country_parts = [f"{c} **{n}**" for c, n in country_counts.head(3).items()]
            lines.append("Top Countries — " + " · ".join(country_parts))

    return "\n".join(lines) + "\n\n"


def analyst_card(price_targets: dict | None, recommendations: pd.DataFrame, upgrades_downgrades: pd.DataFrame) -> str:
    """Analyst consensus — price target range, ratings breakdown, recent actions."""
    header = "__**Analyst Consensus**__"
    lines = [header]

    # Price targets
    if price_targets:
        current = price_targets.get('current')
        low = price_targets.get('low')
        mean = price_targets.get('mean')
        high = price_targets.get('high')
        if mean is not None and current and current != 0:
            upside = ((mean - current) / current) * 100
            upside_str = f"+{upside:.1f}%" if upside >= 0 else f"{upside:.1f}%"
            lines.append(f"Target Low **${low:.2f}** · Mean **${mean:.2f}** ({upside_str}) · High **${high:.2f}**")
        elif mean is not None:
            lines.append(f"Target Low **${low:.2f}** · Mean **${mean:.2f}** · High **${high:.2f}**")
    else:
        lines.append("Price targets unavailable")

    lines.append("")

    # Recommendations summary — all 5 categories, current month (period "0m" = iloc[0])
    if recommendations is not None and not recommendations.empty:
        col_map = {c.lower(): c for c in recommendations.columns}
        row = recommendations.iloc[0]
        period_col = col_map.get('period')
        period_label = ""
        if period_col:
            period_val = str(row.get(period_col, '')).strip()
            period_label = " (Current Month)" if period_val in ('0m', '0') else f" ({period_val})"

        parts = []
        for label, key in [
            ("Strong Buy", 'strongbuy'), ("Buy", 'buy'),
            ("Hold", 'hold'), ("Sell", 'sell'), ("Strong Sell", 'strongsell'),
        ]:
            actual_col = col_map.get(key)
            if actual_col:
                try:
                    val = int(row[actual_col])
                    if val > 0:
                        parts.append(f"{label} **{val}**")
                except (TypeError, ValueError):
                    pass
        if parts:
            lines.append(f"Ratings{period_label} — " + " · ".join(parts))

        # Trend vs previous month (iloc[1]) if available
        if len(recommendations) > 1:
            prev_row = recommendations.iloc[1]
            trend_parts = []
            for label, key in [("Buy", 'strongbuy'), ("", 'buy'), ("Hold", 'hold'), ("Sell", 'strongsell'), ("", 'sell')]:
                actual_col = col_map.get(key)
                if actual_col:
                    try:
                        delta = int(row[actual_col]) - int(prev_row[actual_col])
                        if delta != 0 and label:
                            sign = "+" if delta > 0 else ""
                            trend_parts.append(f"{sign}{delta} {label}")
                    except (TypeError, ValueError):
                        pass
            if trend_parts:
                lines.append("vs Last Month — " + " · ".join(trend_parts))

    # Recent upgrades/downgrades (top 5)
    _ACTION_LABELS = {'up': 'Upgraded', 'down': 'Downgraded', 'main': 'Maintained', 'init': 'Initiated'}
    if upgrades_downgrades is not None and not upgrades_downgrades.empty:
        lines.append("")
        lines.append("**Recent Actions**")
        df = upgrades_downgrades.copy()
        try:
            df.index = pd.to_datetime(df.index).tz_localize(None)
        except Exception:
            pass
        df = df.sort_index(ascending=False).head(5)
        for idx, row in df.iterrows():
            firm = row.get('Firm', row.get('firm', 'Unknown'))
            action_raw = str(row.get('Action', row.get('action', ''))).lower()
            action = _ACTION_LABELS.get(action_raw, action_raw.title())
            to_grade = row.get('To Grade', row.get('toGrade', ''))
            date_str = str(idx)[:10]
            grade_str = f" → {to_grade}" if to_grade else ""
            lines.append(f"{date_str} · **{firm}** {action}{grade_str}")
    else:
        lines.append("No recent upgrades/downgrades")

    lines.append("*Source: Yahoo Finance*")
    lines.append("")
    return "\n".join(lines)


def ownership_card(institutional_holders: pd.DataFrame, major_holders: pd.DataFrame) -> str:
    """Institutional and insider ownership breakdown."""
    header = "__**Ownership**__"
    lines = [header]

    # Major holders pct summary
    if major_holders is not None and not major_holders.empty:
        df = major_holders.copy().reset_index(drop=True)
        # major_holders has 2 columns: value and label (order may vary)
        for _, row in df.iterrows():
            vals = list(row)
            if len(vals) >= 2:
                pct_val, label = (vals[0], vals[1]) if isinstance(vals[0], (float, int)) else (vals[1], vals[0])
                try:
                    pct = float(pct_val) * 100
                    lines.append(f"{label}: **{pct:.1f}%**")
                except (TypeError, ValueError):
                    pass

    # Top 5 institutional holders
    if institutional_holders is not None and not institutional_holders.empty:
        lines.append("**Top Institutional Holders**")
        df = institutional_holders.head(5)
        for _, row in df.iterrows():
            holder = row.get('Holder', row.get('holder', 'Unknown'))
            shares = row.get('Shares', row.get('shares'))
            pct_held = row.get('% Out', row.get('pctHeld'))
            share_str = f"{int(shares):,}" if shares is not None and not (isinstance(shares, float) and shares != shares) else 'N/A'
            pct_str = f"{float(pct_held)*100:.1f}%" if pct_held is not None else ''
            lines.append(f"**{holder}** · {share_str} shares {pct_str}")
    else:
        lines.append("No institutional holder data")

    lines.append("")
    return "\n".join(lines)


def insider_activity_card(insider_transactions: pd.DataFrame, insider_purchases: pd.DataFrame) -> str:
    """Insider transaction summary — net buys/sells and recent activity."""
    header = "__**Insider Activity**__"
    lines = [header]

    # Purchases summary
    if insider_purchases is not None and not insider_purchases.empty:
        df = insider_purchases.copy().reset_index(drop=True)
        for _, row in df.iterrows():
            insider_type = row.get('Insider Purchases Last 6m', row.get('insiderPurchases', ''))
            purchases = row.get('Purchases', row.get('purchases', ''))
            sales = row.get('Sales', row.get('sales', ''))
            net = row.get('Net Activity', row.get('netActivity', ''))
            if insider_type:
                parts = []
                if purchases != '':
                    parts.append(f"Buys **{purchases}**")
                if sales != '':
                    parts.append(f"Sells **{sales}**")
                if net != '':
                    parts.append(f"Net **{net}**")
                if parts:
                    lines.append(f"{insider_type}: " + " · ".join(parts))

    # Recent transactions (top 6)
    if insider_transactions is not None and not insider_transactions.empty:
        lines.append("**Recent Transactions**")
        df = insider_transactions.copy()
        if hasattr(df.index, 'tz_localize'):
            try:
                df.index = df.index.tz_localize(None)
            except Exception:
                pass
        df = df.sort_index(ascending=False).head(6)
        for idx, row in df.iterrows():
            name = row.get('Insider', row.get('insider', 'Unknown'))
            transaction = row.get('Transaction', row.get('transaction', ''))
            shares = row.get('Shares', row.get('shares'))
            value = row.get('Value', row.get('value'))
            date_str = str(idx)[:10]
            share_str = f"{int(shares):,}" if shares is not None and not (isinstance(shares, float) and shares != shares) else ''
            val_str = f" · ${int(value):,}" if value is not None and not (isinstance(value, float) and value != value) else ''
            lines.append(f"{date_str} · **{name}** · {transaction} {share_str}{val_str}")
    else:
        lines.append("No recent insider transactions")

    lines.append("")
    return "\n".join(lines)


def short_interest_card(
    short_interest_ratio: float | None,
    short_interest_shares: float | None,
    short_percent_of_float: float | None,
    shares_outstanding: float | None,
) -> str:
    """Short interest summary — days to cover, % of float, shares short."""
    header = "__**Short Interest**__"
    lines = [header]

    if short_interest_ratio is not None:
        lines.append(f"Days to Cover **{short_interest_ratio:.1f}**")
    if short_percent_of_float is not None:
        lines.append(f"Short % of Float **{short_percent_of_float:.2f}%**")
    if short_interest_shares is not None:
        lines.append(f"Shares Short **{format_large_num(short_interest_shares)}**")
    if shares_outstanding is not None:
        lines.append(f"Shares Outstanding **{format_large_num(shares_outstanding)}**")

    if not any([short_interest_ratio, short_interest_shares, short_percent_of_float]):
        lines.append("No short interest data available")

    lines.append("")
    return "\n".join(lines)


def earnings_forecast_card(quarterly_df: pd.DataFrame, yearly_df: pd.DataFrame) -> str:
    """NASDAQ EPS forecast — current/next quarter estimates and yearly outlook."""
    header = "__**Earnings Forecast**__"
    lines = [header]

    if quarterly_df is not None and not quarterly_df.empty:
        lines.append("**Quarterly EPS Estimates**")
        for _, row in quarterly_df.head(4).iterrows():
            period = row.get('fiscalQuarter', row.get('fiscalEnd', row.get('period', row.get('Fiscal Quarter', ''))))
            eps_est = row.get('epsForecast', row.get('consensusEPSForecast', row.get('consensusEPS', row.get('EPS Forecast', 'N/A'))))
            num_analysts = row.get('noOfEsts', row.get('noOfEstimates', row.get('numOfEst', row.get('# Analysts', ''))))
            low = row.get('lowEPS', row.get('lowEPSForecast', row.get('lowEst', row.get('Low', ''))))
            high = row.get('highEPS', row.get('highEPSForecast', row.get('highEst', row.get('High', ''))))
            up = row.get('up', '')
            down = row.get('down', '')
            range_str = f" · Range **{low}**–**{high}**" if low != '' and high != '' else ''
            est_str = f" · {num_analysts} analysts" if num_analysts != '' else ''
            rev_str = ''
            try:
                up_int = int(up) if up != '' else 0
                down_int = int(down) if down != '' else 0
                if up_int or down_int:
                    rev_str = f" · ↑{up_int} ↓{down_int} revisions"
            except (ValueError, TypeError):
                pass
            lines.append(f"**{period}**: EPS Est **{eps_est}**")
            meta = (range_str + est_str + rev_str).lstrip(' ·')
            if meta:
                lines.append(meta)
    else:
        lines.append("No quarterly forecast data available")

    if yearly_df is not None and not yearly_df.empty:
        lines.append("")
        lines.append("**Annual EPS Estimates**")
        for _, row in yearly_df.head(3).iterrows():
            period = row.get('fiscalYear', row.get('fiscalEnd', row.get('year', row.get('Fiscal Year', ''))))
            eps_est = row.get('consensusEPSForecast', row.get('epsForecast', row.get('consensusEPS', row.get('EPS Forecast', 'N/A'))))
            num_analysts = row.get('noOfEstimates', row.get('noOfEsts', row.get('numOfEst', row.get('# Analysts', ''))))
            low = row.get('lowEPSForecast', row.get('lowEPS', row.get('lowEst', row.get('Low', ''))))
            high = row.get('highEPSForecast', row.get('highEPS', row.get('highEst', row.get('High', ''))))
            range_str = f" · Range **{low}**–**{high}**" if low != '' and high != '' else ''
            est_str = f" · {num_analysts} analysts" if num_analysts != '' else ''
            lines.append(f"**{period}**: EPS Est **{eps_est}**")
            meta = (range_str + est_str).lstrip(' ·')
            if meta:
                lines.append(meta)

    lines.append("")
    return "\n".join(lines)


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


# ---------------------------------------------------------------------------
# Technical report cards — EmbedField values (no header; name= is the header)
# ---------------------------------------------------------------------------

def trend_analysis_card(daily_price_history: pd.DataFrame, current_price: float | None) -> str:
    """SMA 20/50/200, EMA 9/21, price position, and SMA 50/200 cross."""
    if daily_price_history is None or daily_price_history.empty:
        return "No price data available"

    close = daily_price_history['close']
    n = len(close)
    price = current_price or (float(close.iloc[-1]) if not close.empty else None)
    if price is None:
        return "No price data available"

    # SMA 20 / 50 / 200
    sma_parts = []
    for period in [20, 50, 200]:
        if n >= period:
            sma = ta.sma(close, period)
            val = float(sma.iloc[-1]) if sma is not None and not sma.empty else None
            if val is not None and not pd.isna(val):
                pct = ((price - val) / val) * 100.0
                sign = '+' if pct >= 0 else ''
                sma_parts.append(f"SMA{period} **${val:.2f}** ({sign}{pct:.1f}%)")
            else:
                sma_parts.append(f"SMA{period} N/A")
        else:
            sma_parts.append(f"SMA{period} N/A")

    # EMA 9 / 21
    ema_parts = []
    for period in [9, 21]:
        if n >= period:
            ema = ta.ema(close, length=period)
            val = float(ema.iloc[-1]) if ema is not None and not ema.empty else None
            if val is not None and not pd.isna(val):
                pct = ((price - val) / val) * 100.0
                sign = '+' if pct >= 0 else ''
                ema_parts.append(f"EMA{period} **${val:.2f}** ({sign}{pct:.1f}%)")
            else:
                ema_parts.append(f"EMA{period} N/A")
        else:
            ema_parts.append(f"EMA{period} N/A")

    lines = [
        ' · '.join(sma_parts),
        ' · '.join(ema_parts),
    ]

    # Golden / Death cross
    if n >= 200:
        sma50 = ta.sma(close, 50)
        sma200 = ta.sma(close, 200)
        s50 = float(sma50.iloc[-1]) if sma50 is not None and not sma50.empty else None
        s200 = float(sma200.iloc[-1]) if sma200 is not None and not sma200.empty else None
        if s50 is not None and s200 is not None and not pd.isna(s50) and not pd.isna(s200):
            cross_label = "**Golden Cross** 🟢" if s50 > s200 else "**Death Cross** 🔻"
            lines.append(f"50/200 Cross: {cross_label}")

    return '\n'.join(lines) or '\u200b'


def momentum_detail_card(daily_price_history: pd.DataFrame) -> str:
    """RSI(14), MACD histogram, and ROC(10)."""
    if daily_price_history is None or daily_price_history.empty:
        return "No price data available"

    close = daily_price_history['close']
    n = len(close)
    lines = []

    # RSI(14)
    if n >= 15:
        rsi_s = ta.rsi(close, length=14)
        rsi_val = float(rsi_s.iloc[-1]) if rsi_s is not None and not rsi_s.empty and not pd.isna(rsi_s.iloc[-1]) else None
        if rsi_val is not None:
            zone = "Overbought" if rsi_val > 70 else "Oversold" if rsi_val < 30 else "Neutral"
            lines.append(f"RSI(14): **{rsi_val:.1f}** — {zone}")
        else:
            lines.append("RSI(14): N/A")
    else:
        lines.append("RSI(14): N/A (insufficient data)")

    # MACD (12/26/9)
    if n >= 35:
        macd_df = ta.macd(close)
        if macd_df is not None and not macd_df.empty:
            macd_val = float(macd_df.iloc[-1, 0])    # MACD line
            hist_val = float(macd_df.iloc[-1, 1])    # Histogram
            sig_val = float(macd_df.iloc[-1, 2])     # Signal line
            if not pd.isna(hist_val):
                direction = "Bullish" if hist_val > 0 else "Bearish"
                sign = '+' if hist_val > 0 else ''
                cross = "Above signal" if not pd.isna(macd_val) and not pd.isna(sig_val) and macd_val > sig_val else "Below signal"
                lines.append(f"MACD: **{direction}** (hist {sign}{hist_val:.2f}) · {cross}")
            else:
                lines.append("MACD: N/A")
        else:
            lines.append("MACD: N/A")
    else:
        lines.append("MACD: N/A (insufficient data)")

    # ROC(10)
    if n >= 11:
        roc_s = ta.roc(close=close, length=10)
        roc_val = float(roc_s.iloc[-1]) if roc_s is not None and not roc_s.empty and not pd.isna(roc_s.iloc[-1]) else None
        if roc_val is not None:
            sign = '+' if roc_val > 0 else ''
            bias = "Positive momentum" if roc_val > 0 else "Negative momentum"
            lines.append(f"ROC(10): **{sign}{roc_val:.2f}%** — {bias}")
        else:
            lines.append("ROC(10): N/A")
    else:
        lines.append("ROC(10): N/A (insufficient data)")

    return '\n'.join(lines) or '\u200b'


def volatility_analysis_card(daily_price_history: pd.DataFrame, current_price: float | None) -> str:
    """NATR(14) as HV proxy, Bollinger %B, and band width."""
    if daily_price_history is None or daily_price_history.empty:
        return "No price data available"
    if 'high' not in daily_price_history.columns or 'low' not in daily_price_history.columns:
        return "No OHLC data available"

    close = daily_price_history['close']
    high = daily_price_history['high']
    low = daily_price_history['low']
    n = len(close)
    lines = []

    # NATR(14) — normalized ATR as % of price
    if n >= 15:
        natr_s = ta.natr(high=high, low=low, close=close, length=14)
        natr_val = float(natr_s.iloc[-1]) if natr_s is not None and not natr_s.empty and not pd.isna(natr_s.iloc[-1]) else None
        natr_str = f"**{natr_val:.2f}%**" if natr_val is not None else "N/A"
    else:
        natr_str = "N/A"

    # ATR(14)
    if n >= 15:
        atr_s = ta.atr(high=high, low=low, close=close, length=14)
        atr_val = float(atr_s.iloc[-1]) if atr_s is not None and not atr_s.empty and not pd.isna(atr_s.iloc[-1]) else None
        atr_str = f"**${atr_val:.2f}**" if atr_val is not None else "N/A"
    else:
        atr_str = "N/A"

    lines.append(f"ATR(14): {atr_str} · NATR(14): {natr_str}")

    # Bollinger Bands (20, 2σ)
    if n >= 20:
        bb_df = ta.bbands(close, length=20)
        if bb_df is not None and not bb_df.empty and len(bb_df.columns) >= 5:
            bbp = float(bb_df.iloc[-1, 4])   # BBP — %B
            bbb = float(bb_df.iloc[-1, 3])   # BBB — Width %
            if not pd.isna(bbp):
                if bbp > 0.8:
                    position = "Near upper band"
                elif bbp > 0.5:
                    position = "Upper half"
                elif bbp > 0.2:
                    position = "Lower half"
                else:
                    position = "Near lower band"
                bb_p_str = f"**{bbp:.2f}** ({position})"
            else:
                bb_p_str = "N/A"
            bbb_str = f"**{bbb:.1f}%**" if not pd.isna(bbb) else "N/A"
            lines.append(f"BB %B: {bb_p_str} · BB Width: {bbb_str}")
        else:
            lines.append("BB: N/A")
    else:
        lines.append("BB: N/A (insufficient data)")

    return '\n'.join(lines) or '\u200b'


def volume_analysis_card(daily_price_history: pd.DataFrame, current_volume: float | None = None) -> str:
    """RVOL(10), OBV trend, and Accumulation/Distribution trend."""
    if daily_price_history is None or daily_price_history.empty:
        return "No price data available"
    if 'volume' not in daily_price_history.columns:
        return "No volume data available"

    close = daily_price_history['close']
    volume = daily_price_history['volume']
    n = len(close)

    lines = []

    # RVOL(10) — today vs 10-day average
    if n >= 11:
        vol_series = volume.tail(11)
        today_vol = float(current_volume) if current_volume is not None else float(vol_series.iloc[-1])
        avg_vol = float(vol_series.iloc[:-1].mean())
        if avg_vol > 0:
            rvol = today_vol / avg_vol
            label = "Heavy" if rvol > 2 else "Above average" if rvol > 1.2 else "Below average" if rvol < 0.8 else "Average"
            lines.append(f"RVOL(10): **{rvol:.1f}x** — {label}")
        else:
            lines.append("RVOL(10): N/A")
    else:
        lines.append("RVOL(10): N/A (insufficient data)")

    trend_parts = []

    # OBV trend (10-day SMA direction)
    if n >= 15:
        obv_s = ta.obv(close=close, volume=volume)
        if obv_s is not None and len(obv_s) >= 10:
            obv_sma = ta.sma(obv_s, 10)
            if obv_sma is not None and len(obv_sma) >= 2:
                last = float(obv_sma.iloc[-1])
                prev = float(obv_sma.iloc[-2])
                if not pd.isna(last) and not pd.isna(prev):
                    obv_dir = "Rising 🟢" if last > prev else "Falling 🔻"
                    trend_parts.append(f"OBV: **{obv_dir}**")

    # A/D trend (10-day SMA direction) — requires open column
    if n >= 15 and 'high' in daily_price_history.columns and 'low' in daily_price_history.columns:
        open_col = daily_price_history.get('open') if hasattr(daily_price_history, 'get') else daily_price_history['open'] if 'open' in daily_price_history.columns else None
        if open_col is not None:
            ad_s = ta.ad(high=daily_price_history['high'], low=daily_price_history['low'],
                         close=close, volume=volume, open=open_col)
            if ad_s is not None and len(ad_s) >= 10:
                ad_sma = ta.sma(ad_s, 10)
                if ad_sma is not None and len(ad_sma) >= 2:
                    last = float(ad_sma.iloc[-1])
                    prev = float(ad_sma.iloc[-2])
                    if not pd.isna(last) and not pd.isna(prev):
                        ad_dir = "Rising 🟢" if last > prev else "Falling 🔻"
                        trend_parts.append(f"A/D: **{ad_dir}**")

    if trend_parts:
        lines.append(' · '.join(trend_parts))

    return '\n'.join(lines) or '\u200b'


def key_levels_card(daily_price_history: pd.DataFrame, current_price: float | None) -> str:
    """52-week high/low with % distance, and Bollinger Band levels as dynamic S/R."""
    if daily_price_history is None or daily_price_history.empty:
        return "No price data available"

    close = daily_price_history['close']
    n = len(close)
    price = current_price or (float(close.iloc[-1]) if not close.empty else None)
    if price is None:
        return "No price data available"

    lines = []

    # 52W high / low
    window = min(n, 252)
    if 'high' in daily_price_history.columns and 'low' in daily_price_history.columns:
        w52_high = float(daily_price_history['high'].tail(window).max())
        w52_low = float(daily_price_history['low'].tail(window).min())
    else:
        w52_high = float(close.tail(window).max())
        w52_low = float(close.tail(window).min())

    from_high = ((price - w52_high) / w52_high) * 100.0
    from_low = ((price - w52_low) / w52_low) * 100.0
    high_sign = '+' if from_high >= 0 else ''
    low_sign = '+' if from_low >= 0 else ''
    lines.append(
        f"52W High **${w52_high:.2f}** ({high_sign}{from_high:.1f}%) · "
        f"52W Low **${w52_low:.2f}** ({low_sign}{from_low:.1f}%)"
    )

    # Bollinger Band levels as dynamic S/R
    if n >= 20:
        bb_df = ta.bbands(close, length=20)
        if bb_df is not None and not bb_df.empty and len(bb_df.columns) >= 3:
            bbl = float(bb_df.iloc[-1, 0])   # BBL (lower)
            bbm = float(bb_df.iloc[-1, 1])   # BBM (mid)
            bbu = float(bb_df.iloc[-1, 2])   # BBU (upper)
            bbl_str = f"${bbl:.2f}" if not pd.isna(bbl) else "N/A"
            bbm_str = f"${bbm:.2f}" if not pd.isna(bbm) else "N/A"
            bbu_str = f"${bbu:.2f}" if not pd.isna(bbu) else "N/A"
            lines.append(f"BB Upper **{bbu_str}** · BB Mid **{bbm_str}** · BB Lower **{bbl_str}**")

    return '\n'.join(lines) or '\u200b'


def signal_confluence_card(daily_price_history: pd.DataFrame, current_price: float | None) -> str:
    """Count bullish vs bearish signals across all indicators; show overall bias."""
    if daily_price_history is None or daily_price_history.empty:
        return "No price data available"

    close = daily_price_history['close']
    n = len(close)
    price = current_price or (float(close.iloc[-1]) if not close.empty else None)
    if price is None:
        return "No price data available"

    bullish = []
    bearish = []

    # 1. RSI(14)
    if n >= 15:
        rsi_s = ta.rsi(close, length=14)
        if rsi_s is not None and not rsi_s.empty:
            rsi_val = float(rsi_s.iloc[-1])
            if not pd.isna(rsi_val):
                if rsi_val < 30:
                    bullish.append("RSI oversold")
                elif rsi_val > 70:
                    bearish.append("RSI overbought")

    # 2. MACD histogram
    if n >= 35:
        macd_df = ta.macd(close)
        if macd_df is not None and not macd_df.empty:
            hist = float(macd_df.iloc[-1, 1])
            if not pd.isna(hist):
                if hist > 0:
                    bullish.append("MACD")
                else:
                    bearish.append("MACD")

    # 3. ADX direction (DMP vs DMN)
    has_hl = 'high' in daily_price_history.columns and 'low' in daily_price_history.columns
    if n >= 28 and has_hl:
        adx_df = ta.adx(close=close, high=daily_price_history['high'], low=daily_price_history['low'])
        if adx_df is not None and not adx_df.empty:
            dip = float(adx_df.iloc[-1, 1])
            din = float(adx_df.iloc[-1, 2])
            if not pd.isna(dip) and not pd.isna(din):
                if dip > din:
                    bullish.append("ADX trend")
                else:
                    bearish.append("ADX trend")

    # 4. SMA 50/200 cross
    if n >= 200:
        sma50 = ta.sma(close, 50)
        sma200 = ta.sma(close, 200)
        s50 = float(sma50.iloc[-1]) if sma50 is not None and not sma50.empty else None
        s200 = float(sma200.iloc[-1]) if sma200 is not None and not sma200.empty else None
        if s50 is not None and s200 is not None and not pd.isna(s50) and not pd.isna(s200):
            if s50 > s200:
                bullish.append("Golden Cross")
            else:
                bearish.append("Death Cross")

    # 5. Price vs SMA50
    if n >= 50:
        sma50 = ta.sma(close, 50)
        s50 = float(sma50.iloc[-1]) if sma50 is not None and not sma50.empty else None
        if s50 is not None and not pd.isna(s50):
            if price > s50:
                bullish.append("Price > SMA50")
            else:
                bearish.append("Price < SMA50")

    # 6. OBV trend
    if n >= 15 and 'volume' in daily_price_history.columns:
        obv_s = ta.obv(close=close, volume=daily_price_history['volume'])
        if obv_s is not None and len(obv_s) >= 10:
            obv_sma = ta.sma(obv_s, 10)
            if obv_sma is not None and len(obv_sma) >= 2:
                last = float(obv_sma.iloc[-1])
                prev = float(obv_sma.iloc[-2])
                if not pd.isna(last) and not pd.isna(prev):
                    if last > prev:
                        bullish.append("OBV")
                    else:
                        bearish.append("OBV")

    # 7. ROC(10)
    if n >= 11:
        roc_s = ta.roc(close=close, length=10)
        if roc_s is not None and not roc_s.empty:
            roc_val = float(roc_s.iloc[-1])
            if not pd.isna(roc_val):
                if roc_val > 0:
                    bullish.append("ROC")
                else:
                    bearish.append("ROC")

    total = len(bullish) + len(bearish)
    if total == 0:
        return "Insufficient data for signal scoring"

    b_count = len(bullish)
    bear_count = len(bearish)
    margin = b_count - bear_count

    if margin >= 3:
        bias = "**Strong Bullish** 🟢"
    elif margin > 0:
        bias = "**Bullish** 🟢"
    elif margin <= -3:
        bias = "**Strong Bearish** 🔻"
    elif margin < 0:
        bias = "**Bearish** 🔻"
    else:
        bias = "**Neutral** ↔"

    lines = [
        f"📈 Bullish ({b_count}): {', '.join(bullish) if bullish else 'none'}",
        f"📉 Bearish ({bear_count}): {', '.join(bearish) if bearish else 'none'}",
        f"Bias: {bias}",
    ]
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Comparison report cards — EmbedField values (no header; name= is the header)
# ---------------------------------------------------------------------------

def comparison_price_volume_card(tickers: list, quotes: dict) -> str:
    """Per-ticker current price, 1D change %, and volume."""
    lines = []
    for ticker in tickers:
        quote = (quotes or {}).get(ticker) or {}
        try:
            price = quote['regular']['regularMarketLastPrice']
            pct = quote['quote'].get('netPercentChange', 0)
            vol = format_large_num(quote['quote']['totalVolume'])
            sign = '+' if pct >= 0 else ''
            arrow = '🟢' if pct >= 0 else '🔻'
            lines.append(f"**{ticker}**: ${price:.2f} {arrow} **{sign}{pct:.2f}%** · Vol **{vol}**")
        except (KeyError, TypeError):
            lines.append(f"**{ticker}**: N/A")
    return '\n'.join(lines) or '\u200b'


def comparison_performance_card(
    tickers: list,
    quotes: dict,
    daily_price_histories: dict,
    benchmark_ticker: str | None = None,
) -> str:
    """Per-ticker returns over 1D/5D/1M/3M.  Benchmark row is marked with (B)."""
    today = datetime.datetime.now(tz=timezone()).date()
    interval_map = {"1D": 1, "5D": 5, "1M": 30, "3M": 90}
    lines = []

    for ticker in tickers:
        quote = (quotes or {}).get(ticker) or {}
        hist = (daily_price_histories or {}).get(ticker)
        try:
            close = quote['regular']['regularMarketLastPrice']
        except (KeyError, TypeError):
            lines.append(f"**{ticker}**: N/A")
            continue

        parts = []
        for label, days in interval_map.items():
            if hist is None or hist.empty:
                parts.append(f"{label} N/A")
                continue
            interval_date = today - datetime.timedelta(days=days)
            while interval_date.weekday() > 4:
                interval_date -= datetime.timedelta(days=1)
            row = hist[hist['date'] == interval_date]['close']
            if row.empty:
                earlier = hist[hist['date'] <= interval_date]
                if not earlier.empty:
                    row = earlier.sort_values('date', ascending=False).iloc[0:1]['close']
            if not row.empty:
                prev = row.iloc[0]
                pct = ((close - prev) / prev) * 100.0
                sign = '+' if pct >= 0 else ''
                parts.append(f"{label} **{sign}{pct:.1f}%**")
            else:
                parts.append(f"{label} N/A")

        label_str = f"**{ticker}** (B)" if ticker == benchmark_ticker else f"**{ticker}**"
        lines.append(label_str + ": " + " · ".join(parts))

    return '\n'.join(lines) or '\u200b'


def comparison_valuation_card(tickers: list, fundamentals: dict) -> str:
    """Per-ticker market cap, P/E, EPS, and beta."""
    lines = []
    for ticker in tickers:
        fund_data = (fundamentals or {}).get(ticker) or {}
        instruments = fund_data.get('instruments', [])
        if not instruments:
            lines.append(f"**{ticker}**: N/A")
            continue
        fund = instruments[0].get('fundamental', {})
        mcap = format_large_num(fund.get('marketCap', 0) or 0)
        eps_val = fund.get('eps')
        pe_val = fund.get('peRatio')
        beta_val = fund.get('beta')
        eps = f"{eps_val:.2f}" if eps_val is not None else "N/A"
        pe = f"{pe_val:.2f}" if pe_val is not None else "N/A"
        beta = f"{beta_val:.2f}" if beta_val is not None else "N/A"
        lines.append(f"**{ticker}**: MCap **{mcap}** · P/E **{pe}** · EPS **{eps}** · Beta **{beta}**")
    return '\n'.join(lines) or '\u200b'


def comparison_technicals_card(tickers: list, daily_price_histories: dict) -> str:
    """Per-ticker RSI, MACD direction, and SMA 50/200 cross."""
    lines = []
    for ticker in tickers:
        hist = (daily_price_histories or {}).get(ticker)
        if hist is None or hist.empty:
            lines.append(f"**{ticker}**: N/A")
            continue

        close = hist['close']
        n = len(close)
        parts = []

        # RSI(14)
        if n >= 15:
            rsi_s = ta.rsi(close, length=14)
            rsi_val = rsi_s.iloc[-1] if rsi_s is not None and not rsi_s.empty else None
            if rsi_val is not None and not pd.isna(rsi_val):
                zone = "OB" if rsi_val > 70 else "OS" if rsi_val < 30 else "Neutral"
                parts.append(f"RSI **{rsi_val:.1f}** ({zone})")
            else:
                parts.append("RSI N/A")
        else:
            parts.append("RSI N/A")

        # MACD direction
        if n >= 35:
            macd_df = ta.macd(close)
            if macd_df is not None and not macd_df.empty:
                hist_val = macd_df.iloc[-1, 1]
                if not pd.isna(hist_val):
                    direction = "Bullish" if hist_val > 0 else "Bearish"
                    parts.append(f"MACD **{direction}**")
                else:
                    parts.append("MACD N/A")
            else:
                parts.append("MACD N/A")
        else:
            parts.append("MACD N/A")

        # SMA 50/200 cross
        if n >= 200:
            sma50 = ta.sma(close, 50)
            sma200 = ta.sma(close, 200)
            s50 = sma50.iloc[-1] if sma50 is not None and not sma50.empty else None
            s200 = sma200.iloc[-1] if sma200 is not None and not sma200.empty else None
            if s50 is not None and s200 is not None and not pd.isna(s50) and not pd.isna(s200):
                cross = "Golden 🟢" if s50 > s200 else "Death 🔻"
                parts.append(f"50/200 **{cross}**")
            else:
                parts.append("50/200 N/A")
        else:
            parts.append("50/200 N/A")

        lines.append(f"**{ticker}**: " + " · ".join(parts))

    return '\n'.join(lines) or '\u200b'


def comparison_popularity_card(tickers: list, popularities: dict) -> str:
    """Per-ticker popularity rank and mentions.  Returns '' if no ticker has data."""
    rows = {}
    for ticker in tickers:
        pop = (popularities or {}).get(ticker)
        if pop is None or pop.empty:
            continue
        latest = pop.iloc[0]
        rank = latest.get('rank')
        if rank is None or pd.isna(rank):
            continue
        mentions = latest.get('mentions')
        rows[ticker] = (int(rank), int(mentions) if mentions is not None and not pd.isna(mentions) else None)

    if not rows:
        return ''

    lines = []
    for ticker in tickers:
        if ticker in rows:
            rank, mentions = rows[ticker]
            m_str = f" · {mentions} mentions" if mentions is not None else ''
            lines.append(f"**{ticker}**: #{rank}{m_str}")
        else:
            lines.append(f"**{ticker}**: No data")

    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Options report cards — EmbedField values (no header; name= is the header)
# ---------------------------------------------------------------------------

def _options_nearest_exp(exp_map: dict) -> str | None:
    return sorted(exp_map.keys())[0] if exp_map else None


def _options_find_atm(strikes_dict: dict, underlying: float) -> tuple[str, list] | tuple[None, None]:
    """Return (strike_str, contracts) for the strike nearest to underlying."""
    if not strikes_dict or not underlying:
        return None, None
    try:
        best = min(strikes_dict.keys(), key=lambda s: abs(float(s) - underlying))
        return best, strikes_dict[best]
    except (ValueError, TypeError):
        return None, None


def _top_by_volume(strikes_dict: dict, n: int = 3) -> list[tuple[float, dict]]:
    """Return up to n (strike_float, contract_dict) pairs sorted by totalVolume desc."""
    rows = []
    for s_str, contracts in strikes_dict.items():
        for c in contracts:
            try:
                rows.append((float(s_str), c))
            except (ValueError, TypeError):
                pass
    rows.sort(key=lambda x: x[1].get('totalVolume', 0) or 0, reverse=True)
    return rows[:n]


def iv_analysis_card(
    options_chain: dict,
    daily_price_history: pd.DataFrame,
    iv_history: pd.DataFrame | None = None,
) -> str:
    """Chain IV, ATM IV, 20/60D HV, IV/HV ratio, IV Rank / IV Percentile."""
    from rocketstocks.core.analysis.options import (
        compute_historical_volatility, compute_iv_rank, compute_iv_percentile,
    )
    if not options_chain or options_chain.get('status') == 'FAILED':
        return "No options data available"

    lines = []

    chain_iv = options_chain.get('volatility')
    underlying = options_chain.get('underlyingPrice')

    # Chain IV + ATM IV
    atm_iv = None
    call_map = options_chain.get('callExpDateMap', {})
    nearest = _options_nearest_exp(call_map)
    if nearest and underlying:
        atm_str_key, atm_contracts = _options_find_atm(call_map.get(nearest, {}), underlying)
        if atm_contracts:
            raw = atm_contracts[0].get('volatility')
            atm_iv = float(raw) if raw and raw > 0 else None

    iv_parts = []
    if chain_iv and chain_iv > 0:
        iv_parts.append(f"Chain IV: **{chain_iv:.1f}%**")
    if atm_iv:
        iv_parts.append(f"ATM IV: **{atm_iv:.1f}%**")
    if iv_parts:
        lines.append(' · '.join(iv_parts))

    # HV 20D and 60D via NATR
    hv20 = compute_historical_volatility(daily_price_history, 20)
    hv60 = compute_historical_volatility(daily_price_history, 60)
    hv_parts = []
    if hv20 is not None:
        hv_parts.append(f"20D HV: **{hv20:.1f}%**")
    if hv60 is not None:
        hv_parts.append(f"60D HV: **{hv60:.1f}%**")
    if hv_parts:
        lines.append(' · '.join(hv_parts))

    # IV/HV ratio
    ref_iv = atm_iv or (float(chain_iv) if chain_iv else None)
    if ref_iv and hv20 and hv20 > 0:
        ratio = ref_iv / hv20
        direction = "MORE" if ratio > 1.1 else "LESS" if ratio < 0.9 else "SIMILAR"
        lines.append(f"IV/HV: **{ratio:.2f}x** — options pricing {direction} volatility than realized")

    # IV Rank / IV Percentile
    if ref_iv and iv_history is not None and not (hasattr(iv_history, 'empty') and iv_history.empty):
        ivr = compute_iv_rank(ref_iv, iv_history)
        ivp = compute_iv_percentile(ref_iv, iv_history)
        ivr_str = f"**{ivr:.0f}%**" if ivr is not None else "N/A"
        ivp_str = f"**{ivp:.0f}%**" if ivp is not None else "N/A"
        lines.append(f"IV Rank: {ivr_str} · IV Percentile: {ivp_str}")
    else:
        lines.append("IV Rank: *Collecting data...* · IV Percentile: *Collecting data...*")

    return '\n'.join(lines) or '\u200b'


def put_call_card(options_chain: dict) -> str:
    """P/C ratio with sentiment label, total call/put volume and OI."""
    from rocketstocks.core.analysis.options import compute_put_call_stats
    if not options_chain or options_chain.get('status') == 'FAILED':
        return "No options data available"

    lines = []

    pcr = options_chain.get('putCallRatio')
    if pcr is not None:
        if pcr < 0.7:
            sentiment = "Bullish"
        elif pcr > 1.3:
            sentiment = "Bearish"
        else:
            sentiment = "Neutral"
        lines.append(f"P/C Ratio: **{pcr:.2f}** — {sentiment}")

    stats = compute_put_call_stats(options_chain)
    cv, pv = stats['call_volume'], stats['put_volume']
    co, po = stats['call_oi'], stats['put_oi']
    if cv or pv:
        lines.append(f"Volume: Calls **{cv:,}** · Puts **{pv:,}**")
    if co or po:
        lines.append(f"OI: Calls **{format_large_num(co)}** · Puts **{format_large_num(po)}**")

    return '\n'.join(lines) or '\u200b'


def max_pain_card(options_chain: dict, current_price: float | None) -> str:
    """Max pain strike and its distance from the current price."""
    from rocketstocks.core.analysis.options import compute_max_pain
    if not options_chain or options_chain.get('status') == 'FAILED':
        return "No options data available"

    strike = compute_max_pain(options_chain)
    if strike is None:
        return "Insufficient data to compute max pain"

    price = current_price or options_chain.get('underlyingPrice')
    if price:
        pct = ((price - strike) / strike) * 100.0
        sign = '+' if pct >= 0 else ''
        pos = "above" if pct >= 0 else "below"
        dist_str = f" · Current **${price:.2f}** ({sign}{pct:.1f}% {pos})"
    else:
        dist_str = ''

    return f"Max Pain: **${strike:.2f}**{dist_str}"


def iv_skew_card(options_chain: dict, current_price: float | None) -> str:
    """OTM put IV vs OTM call IV at ~5% from ATM; skew direction."""
    from rocketstocks.core.analysis.options import compute_iv_skew
    if not options_chain or options_chain.get('status') == 'FAILED':
        return "No options data available"

    price = current_price or options_chain.get('underlyingPrice')
    if not price:
        return "No underlying price available"

    result = compute_iv_skew(options_chain, price)
    if result is None:
        return "Insufficient data to compute IV skew"

    put_s = result['otm_put_strike']
    put_iv = result['otm_put_iv']
    call_s = result['otm_call_strike']
    call_iv = result['otm_call_iv']
    skew = result['skew']
    direction = result['direction']

    sign = '+' if skew >= 0 else ''
    if direction == 'put_skew':
        label = "Put Skew — bearish hedge premium elevated"
    elif direction == 'call_skew':
        label = "Call Skew — bullish demand driving up call IV"
    else:
        label = "Neutral — put/call IV roughly balanced"

    lines = [
        f"OTM Put **${put_s:.0f}**: **{put_iv:.1f}%** IV · OTM Call **${call_s:.0f}**: **{call_iv:.1f}%** IV",
        f"Skew: **{sign}{skew:.1f}%** — {label}",
    ]
    return '\n'.join(lines)


def unusual_options_card(options_chain: dict) -> str:
    """Contracts with volume/OI ≥ 3x (potential unusual/smart-money activity)."""
    from rocketstocks.core.analysis.options import detect_unusual_activity
    if not options_chain or options_chain.get('status') == 'FAILED':
        return "No options data available"

    hits = detect_unusual_activity(options_chain, vol_oi_threshold=3.0, max_results=4)
    if not hits:
        return "No unusual activity detected (vol/OI < 3x across all strikes)"

    lines = []
    for h in hits:
        opt = h['type'].upper()
        iv_str = f" · IV **{h['iv']:.1f}%**" if h['iv'] and h['iv'] > 0 else ''
        lines.append(
            f"{opt} **${h['strike']:.0f}** ({h['expiry']}) — "
            f"Vol **{h['volume']:,}** · OI **{h['oi']:,}** · Ratio **{h['ratio']:.1f}x**{iv_str}"
        )
    return '\n'.join(lines)


def active_strikes_card(options_chain: dict, current_price: float | None) -> str:
    """Top 3 calls and top 3 puts by volume for the nearest expiration."""
    if not options_chain or options_chain.get('status') == 'FAILED':
        return "No options data available"

    call_map = options_chain.get('callExpDateMap', {})
    put_map = options_chain.get('putExpDateMap', {})
    nearest = _options_nearest_exp(call_map)
    if not nearest:
        return "No expiration data available"

    exp_label = nearest.split(':')[0]
    lines = [f"Nearest exp: **{exp_label}**"]

    for label, exp_map in [("Calls", call_map), ("Puts", put_map)]:
        top = _top_by_volume(exp_map.get(nearest, {}), n=3)
        if not top:
            continue
        lines.append(f"**{label}:**")
        for strike, c in top:
            vol = c.get('totalVolume', 0) or 0
            oi = c.get('openInterest', 0) or 0
            iv = c.get('volatility')
            itm = ''
            if current_price:
                if label == "Calls" and current_price > strike:
                    itm = ' ITM'
                elif label == "Puts" and current_price < strike:
                    itm = ' ITM'
            iv_str = f" · IV **{iv:.1f}%**" if iv and iv > 0 else ''
            lines.append(f"  ${strike:.0f}{itm}: **{vol:,}** vol · **{format_large_num(oi)}** OI{iv_str}")

    return '\n'.join(lines) or '\u200b'


def greeks_summary_card(options_chain: dict, current_price: float | None) -> str:
    """ATM call and put Greeks at the nearest expiration."""
    if not options_chain or options_chain.get('status') == 'FAILED':
        return "No options data available"

    price = current_price or options_chain.get('underlyingPrice')
    call_map = options_chain.get('callExpDateMap', {})
    put_map = options_chain.get('putExpDateMap', {})
    nearest = _options_nearest_exp(call_map)
    if not nearest or not price:
        return "Insufficient data"

    exp_label = nearest.split(':')[0]
    atm_key, call_contracts = _options_find_atm(call_map.get(nearest, {}), price)
    _, put_contracts = _options_find_atm(put_map.get(nearest, {}), price)

    if not atm_key or not call_contracts:
        return "No ATM strike data available"

    lines = [f"ATM Strike **${float(atm_key):.0f}** (exp **{exp_label}**)"]

    def _fmt_greeks(contracts: list, label: str) -> str:
        if not contracts:
            return f"{label}: N/A"
        c = contracts[0]
        d = c.get('delta')
        g = c.get('gamma')
        t = c.get('theta')
        v = c.get('vega')
        parts = []
        if d is not None:
            parts.append(f"Δ **{d:.3f}**")
        if g is not None:
            parts.append(f"Γ **{g:.4f}**")
        if t is not None:
            parts.append(f"Θ **{t:.3f}**")
        if v is not None:
            parts.append(f"V **{v:.3f}**")
        return f"{label}: " + (' · '.join(parts) if parts else "N/A")

    lines.append(_fmt_greeks(call_contracts, "Call"))
    lines.append(_fmt_greeks(put_contracts or [], "Put"))
    return '\n'.join(lines)


def relative_strength_card(
    daily_price_history: pd.DataFrame,
    benchmark_history: pd.DataFrame,
    benchmark_label: str = "SPY",
) -> str:
    """Alpha vs benchmark over 1M/3M/6M/1Y — shows ticker return, benchmark return, and difference."""
    if daily_price_history.empty or benchmark_history.empty:
        return "Insufficient price data"

    ticker_closes = daily_price_history['close'].dropna()
    bench_closes = benchmark_history['close'].dropna()

    if len(ticker_closes) < 2 or len(bench_closes) < 2:
        return "Insufficient price data"

    periods = [('1M', 21), ('3M', 63), ('6M', 126), ('1Y', 252)]
    lines = []

    for label, days in periods:
        if len(ticker_closes) <= days or len(bench_closes) <= days:
            continue
        ticker_ret = (ticker_closes.iloc[-1] / ticker_closes.iloc[-days - 1]) - 1
        bench_ret = (bench_closes.iloc[-1] / bench_closes.iloc[-days - 1]) - 1
        alpha = ticker_ret - bench_ret

        t_sign = '+' if ticker_ret >= 0 else ''
        a_sign = '+' if alpha >= 0 else ''
        direction = '▲' if alpha >= 0 else '▼'
        lines.append(
            f"**{label}** {t_sign}{ticker_ret * 100:.1f}% · {benchmark_label} {'+' if bench_ret >= 0 else ''}{bench_ret * 100:.1f}% · Alpha {direction} **{a_sign}{alpha * 100:.1f}%**"
        )

    return '\n'.join(lines) if lines else "Insufficient price history"


def float_data_card(float_data: dict | None) -> str:
    """Float size, short % of float, and short ratio (days to cover) from YFinance float data."""
    if not float_data:
        return "No short interest data available"

    float_shares = float_data.get('float_shares')
    short_pct = float_data.get('short_pct_float')
    short_ratio = float_data.get('short_ratio')

    lines = []
    if float_shares is not None:
        lines.append(f"Float **{format_large_num(float_shares)}** shares")
    if short_pct is not None:
        pct_val = short_pct * 100 if short_pct < 1 else short_pct  # handle fractional vs percent
        lines.append(f"Short % of Float **{pct_val:.1f}%**")
    if short_ratio is not None:
        lines.append(f"Short Ratio **{short_ratio:.1f}** days to cover")

    return '\n'.join(lines) if lines else "No short interest data available"
