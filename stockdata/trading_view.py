import logging
from tradingview_screener import Query, Column as col

# Logging configuration
logger = logging.getLogger(__name__)

class TradingView():
    def __init__(self):
        pass

    @staticmethod
    def get_premarket_gainers():
        num_rows, gainers = (Query()
                            .select(
                                'logoid',
                                'name',
                                'premarket_close',
                                'premarket_change_abs',
                                'premarket_change',
                                'premarket_volume',
                                'premarket_gap',
                                'close',
                                'change',
                                'volume',
                                'market_cap_basic',
                                'Perf.1Y.MarketCap',
                                'description',
                                'type',
                                'typespecs',
                                'update_mode',
                                'pricescale',
                                'minmov',
                                'fractional',
                                'minmove2',
                                'fundamental_currency_code',
                                'currency',
                            )
                            .where(
                                col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
                                col('is_primary') == True,
                                col('typespecs').has('common'),
                                col('typespecs').has_none_of('preferred'),
                                col('type') == 'stock',
                                col('premarket_change') > 0,
                                col('premarket_change').not_empty(),
                                col('active_symbol') == True,
                            )
                            .order_by('premarket_change', ascending=False, nulls_first=False)
                            .limit(100)
                            .set_markets('america')
                            .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr', 'structured']}})
                            .set_property('preset', 'pre-market-gainers')
                            .get_scanner_data())
        
        return gainers


    @staticmethod
    def get_intraday_gainers():
        logger.debug(f"Fetching intraday gainers from TradingView")
        num_rows, gainers = (Query()
                            .select(
                                'name',
                                'description',
                                'logoid',
                                'update_mode',
                                'type',
                                'typespecs',
                                'market_cap_basic',
                                'fundamental_currency_code',
                                'close',
                                'pricescale',
                                'minmov',
                                'fractional',
                                'minmove2',
                                'currency',
                                'change',
                                'volume',
                                'price_earnings_ttm',
                                'earnings_per_share_diluted_ttm',
                                'earnings_per_share_diluted_yoy_growth_ttm',
                                'dividends_yield_current',
                                'sector.tr',
                                'sector',
                                'market',
                                'recommendation_mark',
                                'relative_volume_10d_calc',
                            )
                            .where(
                                col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
                                col('is_primary') == True,
                                col('typespecs').has('common'),
                                col('typespecs').has_none_of('preferred'),
                                col('type') == 'stock',
                                col('close').between(2, 10000),
                                col('change') > 0,
                                col('active_symbol') == True,
                            )
                            .order_by('change', ascending=False, nulls_first=False)
                            .limit(100)
                            .set_markets('america')
                            .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr', 'structured']}})
                            .set_property('preset', 'gainers')
                            .get_scanner_data())
        return gainers
                
    @staticmethod
    def get_postmarket_gainers():
        logger.debug(f"Fetching after hours gainers from TradingView")
        num_rows, gainers = (Query()
                            .select(
                                'logoid',
                                'name',
                                'postmarket_close',
                                'postmarket_change_abs',
                                'postmarket_change',
                                'postmarket_volume',
                                'close',
                                'change',
                                'volume',
                                'market_cap_basic',
                                'Perf.1Y.MarketCap',
                                'description',
                                'type',
                                'typespecs',
                                'update_mode',
                                'pricescale',
                                'minmov',
                                'fractional',
                                'minmove2',
                                'fundamental_currency_code',
                                'currency',
                            )
                            .where(
                                col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
                                col('is_primary') == True,
                                col('typespecs').has('common'),
                                col('typespecs').has_none_of('preferred'),
                                col('type') == 'stock',
                                col('postmarket_change') > 0,
                                col('postmarket_change').not_empty(),
                                col('active_symbol') == True,
                            )
                            .order_by('postmarket_change', ascending=False, nulls_first=False)
                            .limit(100)
                            .set_markets('america')
                            .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr', 'structured']}})
                            .set_property('preset', 'after_hours_gainers')
                            .get_scanner_data())
        return gainers

    
    @staticmethod
    def get_unusual_volume_movers():
        logger.debug("Fetching stocks with ununsual volume from TradingView")
        num_rows, unusual_volume = (Query()
                                    .select(
                                        'name',
                                        'description',
                                        'logoid',
                                        'update_mode',
                                        'type',
                                        'typespecs',
                                        'relative_volume_10d_calc',
                                        'close',
                                        'pricescale',
                                        'minmov',
                                        'fractional',
                                        'minmove2',
                                        'currency',
                                        'change',
                                        'volume',
                                        'market_cap_basic',
                                        'fundamental_currency_code',
                                        'price_earnings_ttm',
                                        'earnings_per_share_diluted_ttm',
                                        'earnings_per_share_diluted_yoy_growth_ttm',
                                        'dividends_yield_current',
                                        'sector.tr',
                                        'sector',
                                        'market',
                                        'recommendation_mark',
                                    )
                                    .where(
                                        col('exchange').isin(['AMEX', 'CBOE', 'NASDAQ', 'NYSE']),
                                        col('is_primary') == True,
                                        col('typespecs').has('common'),
                                        col('typespecs').has_none_of('preferred'),
                                        col('type') == 'stock',
                                        col('active_symbol') == True,
                                    )
                                    .order_by('relative_volume_10d_calc', ascending=False, nulls_first=False)
                                    .limit(100)
                                    .set_markets('america')
                                    .set_property('symbols', {'query': {'types': ['stock', 'fund', 'dr', 'structured']}})
                                    .set_property('preset', 'unusual_volume')
                                    .get_scanner_data())
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
        logger.debug(f"Returned volume moovers dataframe with shape {unusual_volume_at_time.shape}")
        return unusual_volume_at_time
