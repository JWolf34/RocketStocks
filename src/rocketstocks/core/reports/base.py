import datetime
import logging
import pandas as pd
from table2ascii import table2ascii, PresetStyle
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.core.utils.formatting import format_large_num
from rocketstocks.core.config.paths import validate_path, datapaths

logger = logging.getLogger(__name__)


class Report(object):
    """Post information about a stock or stocks to a Discord channel.

    Content-only class: no Discord channel or send_report(). Sending is handled
    by bot/senders/report_sender.py.
    """
    def __init__(self, **kwargs):

        # Parse data from keyword args
        self.ticker_info: dict = kwargs.pop('ticker_info', None)
        self.ticker: str = self.ticker_info['ticker'] if self.ticker_info else kwargs.pop('ticker', None)
        self.quote: dict = kwargs.pop('quote', None)
        self.fundamentals: dict = kwargs.pop('fundamentals', None)
        self.daily_price_history: pd.DataFrame = kwargs.pop('daily_price_history', None)
        self.next_earnings_info: dict = kwargs.pop('next_earnings_info', None)
        self.historical_earnings: pd.DataFrame = kwargs.pop('historical_earnings', None)
        self.recent_sec_filings: dict = kwargs.pop('recent_sec_filings', None)
        self.popularity: pd.DataFrame = kwargs.pop('popularity', None)
        self.company_facts: dict = kwargs.pop('company_facts', None)
        self.politician: dict = kwargs.pop('politician', None)
        self.trades: pd.DataFrame = kwargs.pop('trades', None)
        self.politician_facts: dict = kwargs.pop('politician_facts', None)

        # ASCII table styles
        self.table_styles = {
            'ascii': PresetStyle.ascii,
            'asci_borderless': PresetStyle.ascii_borderless,
            'ascii_box': PresetStyle.ascii_box,
            'ascii_compact': PresetStyle.ascii_compact,
            'ascii_double': PresetStyle.ascii_double,
            'ascii_minimalist': PresetStyle.ascii_minimalist,
            'ascii_rounded': PresetStyle.ascii_rounded,
            'ascii_rounded_box': PresetStyle.ascii_rounded_box,
            'ascii_simple': PresetStyle.ascii_simple,
            'borderless': PresetStyle.borderless,
            'double': PresetStyle.double_box,
            'double_box': PresetStyle.double_box,
            'double_compact': PresetStyle.double_compact,
            'double_thin_box': PresetStyle.double_thin_box,
            'double_thin_compact': PresetStyle.double_thin_compact,
            'markdown': PresetStyle.markdown,
            'minimalist': PresetStyle.minimalist,
            'plain': PresetStyle.plain,
            'simple': PresetStyle.simple,
            'thick': PresetStyle.thick,
            'thick_box': PresetStyle.thick_box,
            'thick_compact': PresetStyle.thick_compact,
            'thin': PresetStyle.thin,
            'thin_box': PresetStyle.thin_box,
            'thin_compact': PresetStyle.thin_compact,
            'thin_compact_rounded': PresetStyle.thin_compact_rounded,
            'thin_double': PresetStyle.thin_double,
            'thin_double_rounded': PresetStyle.thin_double_rounded,
            'thin_rounded': PresetStyle.thin_rounded,
            'thin_thick': PresetStyle.thin_thick,
            'thin_thick_rounded': PresetStyle.thin_thick_rounded,
        }

    def write_df_to_file(self, df: pd.DataFrame, filepath: str):
        """Write input DataFrame to CSV at filepath"""
        validate_path(datapaths.attachments_path)
        df.to_csv(filepath, index=False)

    def format_large_num(self, number):
        """Format large numbers to be human readable. i.e. 300M, 1.2B"""
        return format_large_num(number)

    ############################
    # Report Builder Functions #
    ############################

    def build_df_table(self, df: pd.DataFrame, style='thick_compact'):
        """Return input dataframe in ascii table format for cleanly displaying content in Discord messages"""
        logger.debug(f"Building table of shape {df.shape} with headers {df.columns.to_list()} and of style '{style}'")
        table_style = self.table_styles.get(style, PresetStyle.double_thin_compact)
        table = table2ascii(
            header=df.columns.tolist(),
            body=df.values.tolist(),
            style=table_style
        )
        return "```\n" + table + "\n```"

    def build_stats_table(self, header: dict, body: dict, adjust: str):
        """Return a two-column ascii table for cleanly displaying content in Discord messages"""
        adjust = 'left' if adjust != 'right' else adjust
        spacing = max([len(key) for key in set().union(header, body)]) + 1

        table = ''

        for key, value in header.items():
            if value:
                table += f"{f'{key}:':>{spacing}} {value}\n" if adjust == 'right' else f"{f'{key}:':<{spacing}} {value}\n"
            else:
                table += f"{key}\n"

        table += "━" * 16 + '\n' if header else ''

        for key, value in body.items():
            table += f"{f'{key}:':>{spacing}} {value}\n" if adjust == 'right' else f"{f'{key}:':<{spacing}} {value}\n"

        return '```' + table + '```\n'

    def build_report_header(self):
        """Return message content for report header"""
        logger.debug("Building report header...")
        header = "# " + self.ticker + " Report " + date_utils.format_date_mdy(datetime.datetime.now(tz=date_utils.timezone()).date()) + "\n"
        return header + "\n"

    def build_ticker_info(self):
        """Return message content with information about the report's ticker"""
        logger.debug("Building ticker info...")
        message = "## Ticker Info\n"

        columns = ['name', 'sector', 'industry', 'country']
        fmt_ticker_info = {}
        for key in columns:
            value = self.ticker_info[key]
            if value != 'NaN' and value:
                fmt_ticker_info[key.capitalize()] = value

        fmt_ticker_info['Asset'] = self.quote['assetSubType']
        fmt_ticker_info['Exchange'] = self.quote['reference']['exchangeName']

        message += self.build_stats_table(header={}, body=fmt_ticker_info, adjust='right')
        return message

    def build_recent_SEC_filings(self):
        """Return message content containing the 5 most recently released SEC filings"""
        logger.debug("Building latest SEC filings...")
        message = "## Recent SEC Filings\n\n"

        if not self.recent_sec_filings.empty:
            for filing in self.recent_sec_filings.head(5).to_dict(orient='records'):
                message += f"[Form {filing['form']} - {filing['filingDate']}]({filing['link']})\n"
        else:
            message += "This stock has no recent SEC filings\n"

        return message

    def build_todays_sec_filings(self):
        """Return message content containing SEC filings for the stock released today"""
        logger.debug("Building today's SEC filings...")
        message = "## Today's SEC Filings\n\n"

        today_string = datetime.datetime.today().strftime("%Y-%m-%d")
        todays_filings = self.recent_sec_filings[self.recent_sec_filings['filingDate'] == today_string]
        for index, filing in todays_filings.iterrows():
            message += f"[Form {filing['form']} - {filing['filingDate']}]({filing['link']})\n"
        return message

    def build_earnings_date(self):
        """Return message content with the date and release time of the stock's next earnings report"""
        logger.debug("Building earnings date...")
        message = ''
        if self.next_earnings_info:
            message = f"{self.ticker} reports earnings on "
            message += f"{date_utils.format_date_mdy(self.next_earnings_info['date'])}, "

            earnings_time = self.next_earnings_info['time']
            if "pre-market" in earnings_time:
                message += "before market open"
            elif "after-hours" in earnings_time:
                message += "after market close"
            else:
                message += "time not specified"

            message += "\n"
        return message

    def build_upcoming_earnings_summary(self):
        """Return message content that summarizes the next earnings report for the stock"""
        logger.debug("Building upcoming earnings summary...")
        message = "## Next Earnings Summary\n"
        if self.next_earnings_info:
            fmt_earnings_info = {}
            fmt_earnings_info['Date'] = self.next_earnings_info['date']
            fmt_earnings_info['Time'] = "{}".format(
                "Premarket" if "pre-market" in self.next_earnings_info['time']
                else "After hours" if "after-hours" in self.next_earnings_info['time']
                else "Not supplied"
            )
            fmt_earnings_info['Quarter'] = self.next_earnings_info['fiscal_quarter_ending']
            fmt_earnings_info['EPS Forecast'] = self.next_earnings_info['eps_forecast'] if len(self.next_earnings_info['eps_forecast']) > 0 else "N/A"
            fmt_earnings_info['Estimates'] = self.next_earnings_info['no_of_ests']
            fmt_earnings_info['Prev Rpt Date'] = self.next_earnings_info['last_year_rpt_dt']
            fmt_earnings_info['Prev Year EPS'] = self.next_earnings_info['last_year_eps']
            message += self.build_stats_table(header={}, body=fmt_earnings_info, adjust='right')
        else:
            message += "Stock has no upcoming earnings reports\n"

        return message

    def build_recent_earnings(self):
        """Return message content that summarizes 4 most recent earnings reports for the stock"""
        logger.debug("Building recent earnings...")
        message = "## Recent Earnings Overview\n"

        if not self.historical_earnings.empty:
            column_map = {'date': 'Date Reported',
                          'eps': 'EPS',
                          'surprise': 'Surprise',
                          'epsforecast': 'Estimate',
                          'fiscalquarterending': 'Quarter'}

            recent_earnings = self.historical_earnings.tail(4)
            recent_earnings = recent_earnings.filter(list(column_map.keys()))
            recent_earnings = recent_earnings.rename(columns=column_map)
            recent_earnings['Date Reported'] = recent_earnings['Date Reported'].apply(lambda x: date_utils.format_date_mdy(x))
            recent_earnings['Surprise'] = recent_earnings['Surprise'].apply(lambda x: f"{x}%")
            message += self.build_df_table(df=recent_earnings, style='borderless')
        else:
            message += "No historical earnings found for this ticker"
        return message + "\n"

    def build_performance(self):
        """Return message content with stock performance over recent weeks and months"""
        logger.debug("Building performance...")
        message = "## Performance\n\n"

        if not self.daily_price_history.empty:
            table_header = {}
            close = self.quote['regular']['regularMarketLastPrice']
            table_header['Close'] = close

            table_body = {}
            interval_map = {"1D": 1, "5D": 5, "1M": 30, "3M": 90, "6M": 180}

            today = datetime.datetime.now(tz=date_utils.timezone()).date()
            for label, interval in interval_map.items():
                interval_date = today - datetime.timedelta(days=interval)
                while interval_date.weekday() > 4:
                    interval_date = interval_date - datetime.timedelta(days=1)

                interval_close = self.daily_price_history[self.daily_price_history['date'] == interval_date]['close']

                if not interval_close.empty:
                    interval_close = interval_close.iloc[0]
                    change = ((close - interval_close) / interval_close) * 100.0
                else:
                    interval_close = 'N/A'
                    change = None

                symbol = None
                if interval_close != "N/A":
                    if change < 0:
                        symbol = "🔻"
                    else:
                        symbol = "🟢"

                close_str = "{:.2f}".format(interval_close) if interval_close != 'N/A' else 'N/A'
                change_str = f"{symbol} {change:.2f}%" if (change is not None and symbol) else ''
                table_body[label] = f"{close_str:<5} {change_str}"
            message += self.build_stats_table(header=table_header, body=table_body, adjust='right')
        else:
            message += "No price data found for this stock\n"
        return message

    def build_daily_summary(self):
        """Return message content with OHLVC data for the stock"""
        logger.debug("Building daily summary...")
        message = "## Today's Summary\n"
        OHLCV = {
            'Open': ["{:.2f}".format(self.quote['quote']['openPrice'])],
            'High': ["{:.2f}".format(self.quote['quote']['highPrice'])],
            'Low': ["{:.2f}".format(self.quote['quote']['lowPrice'])],
            'Close': ["{:.2f}".format(self.quote['regular']['regularMarketLastPrice'])],
            'Volume': [self.format_large_num(self.quote['quote']['totalVolume'])]
        }
        message += self.build_df_table(df=pd.DataFrame(OHLCV), style='borderless')
        message += '\n'
        return message

    def build_fundamentals(self):
        """Return message content with stock fundamental data"""
        logger.debug("Building ticker stats...")
        message = "## Fundamentals\n"
        table_body = {}

        if self.fundamentals:
            table_body['Market Cap'] = self.format_large_num(self.fundamentals['instruments'][0]['fundamental']['marketCap'])
            table_body['EPS'] = f"{'{:.2f}'.format(self.fundamentals['instruments'][0]['fundamental']['eps'])}"
            table_body['EPS TTM'] = f"{'{:.2f}'.format(self.fundamentals['instruments'][0]['fundamental']['epsTTM'])}"
            table_body['P/E Ratio'] = f"{'{:.2f}'.format(self.fundamentals['instruments'][0]['fundamental']['peRatio'])}"
            table_body['Beta'] = self.fundamentals['instruments'][0]['fundamental']['beta']
            table_body['Dividend'] = "Yes" if self.fundamentals['instruments'][0]['fundamental']['dividendAmount'] else "No"
            table_body['Shortable'] = "Yes" if self.quote['reference']['isShortable'] else "No"
            table_body['HTB'] = "Yes" if self.quote['reference']['isHardToBorrow'] else "No"

            message += self.build_stats_table(header={}, body=table_body, adjust='right')
        else:
            message += "No fundamentals found"

        return message

    def build_popularity(self):
        """Return message content popularity overview of stock over select intervals"""
        logger.debug("Building popularity...")
        message = "## Popularity\n"

        if not self.popularity.empty:
            table_header = {}
            now = date_utils.round_down_nearest_minute(30)
            popularity_today = self.popularity[(self.popularity['datetime'] == now)]
            current_rank = popularity_today['rank'].iloc[0] if not popularity_today.empty else 'N/A'
            table_header['Current'] = current_rank

            table_body = {}
            interval_map = {"High 1D": 1, "High 7D": 7, "High 1M": 30, "High 3M": 90, "High 6M": 180}

            for label, interval in interval_map.items():
                interval_date = now - datetime.timedelta(days=interval)
                interval_popularity = self.popularity[self.popularity['datetime'].between(interval_date, now)]
                if not interval_popularity.empty:
                    max_rank = interval_popularity['rank'].min()
                else:
                    max_rank = 'N/A'

                symbol = None
                if max_rank != "N/A" and current_rank != 'N/A':
                    if max_rank < current_rank:
                        symbol = "🔻"
                    elif max_rank > current_rank:
                        symbol = "🟢"
                    else:
                        symbol = '━'

                table_body[label] = f"{max_rank:<3} {f'{symbol} {max_rank - current_rank} spots' if symbol and current_rank != 'N/A' else 'No change'}"

            message += self.build_stats_table(header=table_header, body=table_body, adjust='right')
        else:
            message += "No popularity data found for this stock\n"
        return message

    def build_politician_info(self):
        """Return message content with information on the report's politician"""
        column_map = {'Party': 'party', 'State': 'state'}
        fmt_politician_info = {}
        for key, value in column_map.items():
            fmt_politician_info[key] = self.politician[value]

        politician_facts = fmt_politician_info | self.politician_facts

        message = "## About\n"
        message += self.build_stats_table(header={}, body=politician_facts, adjust='right')
        return message

    def build_politician_trades(self):
        """Return message content with information on the report's politician's latest trades"""
        message = "## Latest Trades\n"
        message += self.build_df_table(df=self.trades.head(10))
        return message

    def build_report(self):
        """Return string populated with content from report functions"""
        report = ''
        report += self.build_report_header()
        return report
