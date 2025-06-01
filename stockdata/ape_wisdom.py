
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

    def get_popular_stocks(self, filter_name = 'all stock subreddits', num_stocks=1000):
        logger.debug(f"Fetching top {num_stocks} popular stocks from source '{filter_name}'")
        filter = self.get_filter(filter_name=filter_name)
        if filter:
            top_stocks = []
            num_pages = num_stocks//100

            for page in range(1, num_pages+1):
                response = requests.get(f"{self.base_url}/{filter}/page/{page}")
                data = response.json()
                top_stocks += [result for result in data['results']]

                # Check to see if number of pages need to be reduced
                if data['pages'] < num_pages:
                    num_pages = data['pages']

            if top_stocks:
                top_stocks = pd.DataFrame(top_stocks)
                top_stocks.drop('name', axis=1)
                return top_stocks
            else:
                return None
        else:
            logger.debug(f"No popular stocks found with input filter '{filter_name}'")
            return None
