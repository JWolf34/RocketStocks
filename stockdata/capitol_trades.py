import datetime
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
from RocketStocks.utils import date_utils

# Logging configuration
logger = logging.getLogger(__name__)


class CapitolTrades():

    def __init__(self, db):
        self.db = db # Postgres

    def politician(self, name:str=None, politician_id:str=None):
        '''Return information on politication with given name and ID'''
        logger.info(f"Fetching politician with id '{politician_id}' and name '{name}'")
        if not name and not politician_id:
            logger.info("No politician found with provided criteria")
            return None
        else:
            fields = self.db.get_table_columns('ct_politicians')
            where_conditions = []
            if name:
                where_conditions.append(('name', name))
            if politician_id:
                where_conditions.append(('politician_id', politician_id))
            data = self.db.select(table='ct_politicians',
                                    fields=fields,
                                    where_conditions=where_conditions,
                                    fetchall=False)
            politician = dict(zip(fields, data))                    
            logger.debug(f"Identified politician: \n{politician}")
            return politician
    
    def all_politicians(self):
        """Return dict with information on all politicians in database"""
        logger.info("Retrieving all politicians from database")
        fields = self.db.get_table_columns('ct_politicians')
        data = self.db.select(table='ct_politicians',
                                    fields=fields,
                                    fetchall=True)
        politicians = [dict(zip(fields, data[index])) for index in range(0, len(data))]
        logger.info(f"Found data on {len(politicians)} politicians")
        return politicians
             
    def update_politicians(self):
        """Update rows in ct_politicians table with latest information"""
        logger.info("Updating politicians in the database")
        politicians = []
        page_num = 1
        while True: 
            params = {'page':page_num, 'pageSize':96}
            politicians_r = requests.get(url='https://www.capitoltrades.com/politicians', params=params)
            logger.debug(f"Requesting politicians on page {page_num}, status code is {politicians_r.status_code}")
            html = politicians_r.content
            politicians_soup = BeautifulSoup(html, 'html.parser')
            cards = politicians_soup.find_all('a', class_="index-card-link")
            if cards:
                for card in cards:
                    politician_id = card['href'].split('/')[-1]
                    name = card.find('h2').text
                    party = card.find('span', class_=lambda c: "q-field party" in c).text
                    state = card.find('span', class_=lambda c: "q-field us-state-full" in c).text
                    politician = (politician_id, name, party, state)
                    logger.debug(f"Identified politician with data {politician}")
                    politicians.append(politician)
                    
                page_num += 1
            else:
                columns = self.db.get_table_columns(table='ct_politicians')
                logger.debug("Inserting politicians into database")
                self.db.insert(table='ct_politicians',
                                  fields=columns,
                                  values=politicians)
                break
        logger.info("Updating politicians complete!")

      
    @staticmethod
    def politician_facts(pid:str):
        """Return dict of facts about politician with input pid from Capitol Trades"""

        # Get webpage content
        politician_r = requests.get(url=f'https://www.capitoltrades.com/politicians/{pid}')
        logger.debug(f"Request for politician (PID:{pid}) page returned status code {politician_r.status_code}")
        html = politician_r.content
        politician_soup = BeautifulSoup(html, 'html.parser')

        # Parse from soup
        facts = {}
        bio_rows = politician_soup.find_all("div", class_="flex w-full flex-col justify-between border-muted-foreground/10 group-[.flavour--full]:flex-row group-[.flavour--full]:border-b group-[.flavour--full]:py-1")
        for row in bio_rows:
             stats = row.find_all('span')
             facts[stats[1].text] = stats[0].text

        return facts
     

    @staticmethod
    def trades(pid:str):
        """Return all trades performed by policitia with input ID"""
        logger.debug(f"Requesting trades information for politician with id '{pid}")
        trades = []
        page_num = 1
        while True: 
            params = {'page':page_num, 'pageSize':96}
            trades_r = requests.get(url=f'https://www.capitoltrades.com/politicians/{pid}', params=params)
            logger.debug(f"Requesting trades on page {page_num}, status code is {trades_r.status_code}")
            html = trades_r.content
            trades_soup = BeautifulSoup(html, 'html.parser')
            table = trades_soup.find('tbody')
            rows = table.find_all('tr')
            if len(rows) > 1:
                for row in rows:

                    # Ticker
                    ticker = row.find('span', class_='q-field issuer-ticker').text
                    if ":" in ticker:
                        ticker = ticker.split(":")[0]

                    # Published and Filed Dates
                    # Special case for September since datetime uses "Sep" but CT uses "Sept"
                    dates = row.find_all('div', class_ = "text-size-3 font-medium")
                    years = row.find_all('div', class_ = "text-size-2 text-txt-dimmer")
                    published_date = datetime.datetime.strptime(f"{dates[0].text.replace('Sept','Sep')} {years[0].text}", "%d %b %Y").date()
                    filed_date = datetime.datetime.strptime(f"{dates[1].text.replace('Sept','Sep')} {years[1].text}", "%d %b %Y").date()

                    # Filed after
                    filed_after = f"{row.find('span', class_= lambda c: 'reporting-gap-tier' in c).text} days"

                    # Order Type and Size
                    order_type = row.find('span', class_ = lambda c: "q-field tx-type" in c).text.replace('"','').upper()
                    order_size = row.find('span', class_ = "mt-1 text-size-2 text-txt-dimmer hover:text-foreground").text

                    # Add to DF and increment page_num
                    trade = (ticker, date_utils.format_date_mdy(published_date), date_utils.format_date_mdy(filed_date), filed_after, order_type, order_size)
                    logger.debug(f"Identified trade with data {trade}")
                    trades.append(trade)
                page_num += 1
            else:
                logger.debug(f"Returning data on {len(trades)} trades")
                return pd.DataFrame(trades,columns=['Ticker', 'Published Date', 'Filed Dated', 'Filed After', 'Order Type', 'Order Size'])
   