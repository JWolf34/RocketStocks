import logging
from tradingview_screener import Query, Column

# Logging configuration
logger = logging.getLogger(__name__)

class TradingView():
    def __init__(self):
        pass

    @staticmethod
    def get_premarket_gainers():
        logger.debug("Fetching premarket gainers from TradingView")
        num_rows, gainers = (Query()
                    .select('name', 'close', 'volume', 'market_cap_basic', 'premarket_change', 'premarket_volume', 'exchange')
                    .order_by('premarket_change', ascending=False)
                    .where(
                            Column('exchange').isin(['NASDAQ', 'NYSE', 'AMEX']))
                    .limit(100)
                    .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'Premarket Change', "Premarket Volume"]
        gainers.columns = headers
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_premarket_gainers_by_market_cap(market_cap):
        logger.debug(f"Fetching premarket gainers from TradingView with market cap  > {market_cap}")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'premarket_change', 'premarket_volume', 'exchange')
                .order_by('premarket_change', ascending=False)
                .where(
                    Column('market_cap_basic') >= market_cap,
                    Column('exchange').isin(['NASDAQ', 'NYSE', 'AMEX']))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'Premarket Change', 'Premarket Volume']
        gainers.columns = headers
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_intraday_gainers():
        logger.debug(f"Fetching intraday gainers from TradingView")
        gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'change', 'exchange' )
                .order_by('change', ascending=False)
                .where(
                            Column('exchange').isin(['NASDAQ', 'NYSE', 'AMEX']))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', '% Change']
        gainers.columns = headers
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_intraday_gainers_by_market_cap(market_cap):
        logger.debug(f"Fetching intrday gainers with market cap > {market_cap} from TradingView")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'change', 'exchange')
                .set_markets('america')
                .order_by('change', ascending=False)
                .where(
                            Column('market_cap_basic') >= market_cap,
                            Column('exchange').isin(['NASDAQ', 'NYSE', 'AMEX']))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', '% Change']
        gainers.columns = headers
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers
                
    @staticmethod
    def get_postmarket_gainers():
        logger.debug(f"Fetching after hours gainers from TradingView")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'postmarket_change', 'postmarket_volume', 'exchange')
                .order_by('postmarket_change', ascending=False)
                .where(
                            Column('exchange').isin(['NASDAQ', 'NYSE', 'AMEX']))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'After Hours Change', 'After Hours Volume']
        gainers.columns = headers
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    @staticmethod
    def get_postmarket_gainers_by_market_cap(market_cap):
        logger.debug(f"Fetching after hours gainers with market cap > {market_cap} from TradingView")
        num_rows, gainers = (Query()
                .select('name', 'close', 'volume', 'market_cap_basic', 'postmarket_change', 'postmarket_volume', 'exchange')
                .order_by('postmarket_change', ascending=False)
                .where(
                            Column('market_cap_basic') >= market_cap,
                            Column('exchange').isin(['NASDAQ', 'NYSE', 'AMEX']))
                .limit(100)
                .get_scanner_data())
        gainers = gainers.drop(columns='exchange')
        gainers = gainers.drop(columns='ticker')
        headers = ['Ticker', 'Close', 'Volume', 'Market Cap', 'After Hours Change', 'After Hours Volume']
        gainers.columns = headers
        logger.debug(f"Returned gainers dataframe with shape {gainers.shape}")
        return gainers

    
    @staticmethod
    def get_unusual_volume_movers():
        logger.debug("Fetching stocks with ununsual volume from TradingView")
        columns = ['Ticker', 'Close', '% Change', 'Volume', 'Relative Volume', 'Average Volume (10 Day)', 'Market Cap']
        num_rows, unusual_volume = (Query()
                            .select('name','close', 'change', 'volume', 'relative_volume', 'average_volume_10d_calc','market_cap_basic')
                            .set_markets('america')
                            .where(
                                Column('volume') > 1000000
                            )
                            .limit(100)
                            .order_by('relative_volume', ascending=False)
                            .get_scanner_data())
        unusual_volume = unusual_volume.drop(columns = "ticker")
        unusual_volume.columns = columns
        logger.debug(f"Returned gainers dataframe with shape {unusual_volume.shape}")
        return unusual_volume

    @staticmethod
    def get_unusual_volume_at_time_movers():
        logger.debug("Fetching stocks with ununsual volume at time form TradingView")
        columns = ['Ticker', 'Close', '% Change', 'Volume', 'Relative Volume At Time', 'Average Volume (10 Day)', 'Market Cap']
        num_rows, unusual_volume_at_time = (Query()
                            .select('name','Price', 'Change %', 'Volume', 'relative_volume_intraday|5', 'Average Volume (10 day)','Market Capitalization')
                            .set_markets('america')
                            .where(
                                Column('Volume') > 1000000
                            )
                            .limit(100)
                            .order_by('relative_volume_intraday|5', ascending=False)
                            .get_scanner_data())
        unusual_volume_at_time = unusual_volume_at_time.drop(columns = "ticker")
        unusual_volume_at_time.columns = columns
        logger.debug(f"Returned volume moovers dataframe with shape {gainers.shape}")
        return unusual_volume_at_time
    
