
import logging
import requests
import pandas as pd

# Logging configuration
logger = logging.getLogger(__name__)

class ApeWisdom():
    def __init__(self):
        self.base_url = "https://apewisdom.io/api/v1.0/filter"
        self.filters_map = {
                    "all subreddits":"all",  # All subreddits combined
                    'all stock subreddits':'all-stocks',  #  Only subreddits focusing on stocks such as r/wallstreetbets or r/stocks
                    'all crypto subreddits':'all-crypto',  #  Only subreddits focusing on cryptocurrencies such as r/CryptoCurrency or r/SatoshiStreetBets
                    '4chan':'4chan', 
                    'r/Cryptocurrency':'CryptoCurrency', 
                    'r/CryptoCurrencies':'CryptoCurrencies', 
                    'r/Bitcoin':'Bitcoin', 
                    'r/SatoshiStreetBets':'SatoshiStreetBets', 
                    'r/CryptoMoonShots':'CryptoMoonShots', 
                    'r/CryptoMarkets':'CryptoMarkets', 
                    'r/stocks':'stocks', 
                    'r/wallstreetbets':'wallstreetbets', 
                    'r/options':'options', 
                    'r/WallStreetbetsELITE':'WallStreetbetsELITE', 
                    'r/Wallstreetbetsnew':'Wallstreetbetsnew', 
                    'r/SPACs':'SPACs', 
                    'r/investing':'investing', 
                    'r/Daytrading':'Daytrading', 
                    'r/Shortsqueeze':'Shortsqueeze',
                    'r/SqueezePlays':"SqueezePlays"
        }
    
    def get_filter(self, filter_name):
        return self.filters_map[filter_name]

    def get_top_stocks(self, filter_name = 'all stock subreddits', page = 1):
        logger.debug(f"Fetching top stocks from source: '{filter_name}', page {page}")
        filter = self.get_filter(filter_name=filter_name)
        if filter is not None:
            top_stocks_json = requests.get(f"{self.base_url}/{filter}/page/{page}").json()
            if top_stocks_json is not None:
                top_stocks = pd.DataFrame(top_stocks_json['results'])
                return top_stocks
        else:
            return None
