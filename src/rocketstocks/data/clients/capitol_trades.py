import datetime
import logging

import requests
from bs4 import BeautifulSoup
import pandas as pd

from rocketstocks.core.utils.dates import format_date_mdy

logger = logging.getLogger(__name__)

_CT_POLITICIAN_COLS = ['politician_id', 'name', 'party', 'state']


class CapitolTrades:

    def __init__(self, db):
        self.db = db  # Postgres

    async def politician(self, name: str = None, politician_id: str = None) -> dict | None:
        """Return information on politician with given name and/or ID."""
        logger.info(f"Fetching politician with id '{politician_id}' and name '{name}'")
        if not name and not politician_id:
            logger.info("No politician found with provided criteria")
            return None

        query = f"SELECT {', '.join(_CT_POLITICIAN_COLS)} FROM ct_politicians WHERE TRUE"
        params = []
        if name:
            query += " AND name = %s"
            params.append(name)
        if politician_id:
            query += " AND politician_id = %s"
            params.append(politician_id)

        row = await self.db.execute(query, params, fetchone=True)
        if row is None:
            logger.warning(f"No politician found with id='{politician_id}', name='{name}'")
            return None

        politician = dict(zip(_CT_POLITICIAN_COLS, row))
        logger.debug(f"Identified politician: \n{politician}")
        return politician

    async def all_politicians(self) -> list:
        """Return list of dicts with information on all politicians in database."""
        logger.info("Retrieving all politicians from database")
        rows = await self.db.execute(
            f"SELECT {', '.join(_CT_POLITICIAN_COLS)} FROM ct_politicians"
        )
        politicians = [dict(zip(_CT_POLITICIAN_COLS, row)) for row in (rows or [])]
        logger.info(f"Found data on {len(politicians)} politicians")
        return politicians

    async def update_politicians(self):
        """Update rows in ct_politicians table with latest information."""
        logger.info("Updating politicians in the database")
        politicians = []
        page_num = 1
        while True:
            params = {'page': page_num, 'pageSize': 96}
            politicians_r = requests.get(
                url='https://www.capitoltrades.com/politicians', params=params
            )
            logger.debug(
                f"Requesting politicians on page {page_num}, "
                f"status code is {politicians_r.status_code}"
            )
            politicians_r.raise_for_status()
            html = politicians_r.content
            politicians_soup = BeautifulSoup(html, 'html.parser')
            cards = politicians_soup.find_all('a', class_="index-card-link")
            if cards:
                for card in cards:
                    politician_id = card['href'].split('/')[-1]
                    name = card.find('h2').text
                    party = card.find('span', class_=lambda c: c and "q-field party" in c).text
                    state = card.find(
                        'span', class_=lambda c: c and "q-field us-state-full" in c
                    ).text
                    politicians.append((politician_id, name, party, state))
                page_num += 1
            else:
                logger.debug("Inserting politicians into database")
                await self.db.execute_batch(
                    "INSERT INTO ct_politicians (politician_id, name, party, state) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    politicians,
                )
                break
        logger.info("Updating politicians complete!")

    @staticmethod
    def politician_facts(pid: str) -> dict:
        """Return dict of facts about politician with input pid from Capitol Trades."""
        politician_r = requests.get(url=f'https://www.capitoltrades.com/politicians/{pid}')
        logger.debug(
            f"Request for politician (PID:{pid}) page returned status code "
            f"{politician_r.status_code}"
        )
        politician_r.raise_for_status()
        html = politician_r.content
        politician_soup = BeautifulSoup(html, 'html.parser')

        facts = {}
        bio_rows = politician_soup.find_all(
            "div",
            class_=(
                "flex w-full flex-col justify-between border-muted-foreground/10 "
                "group-[.flavour--full]:flex-row group-[.flavour--full]:border-b "
                "group-[.flavour--full]:py-1"
            ),
        )
        for row in bio_rows:
            stats = row.find_all('span')
            facts[stats[1].text] = stats[0].text
        return facts

    @staticmethod
    def trades(pid: str) -> pd.DataFrame:
        """Return all trades performed by politician with input ID."""
        logger.debug(f"Requesting trades information for politician with id '{pid}'")
        trades = []
        page_num = 1
        while True:
            params = {'page': page_num, 'pageSize': 96}
            trades_r = requests.get(
                url=f'https://www.capitoltrades.com/politicians/{pid}', params=params
            )
            logger.debug(
                f"Requesting trades on page {page_num}, status code is {trades_r.status_code}"
            )
            trades_r.raise_for_status()
            html = trades_r.content
            trades_soup = BeautifulSoup(html, 'html.parser')
            table = trades_soup.find('tbody')
            if table is None:
                logger.warning(f"No trades table found on page {page_num} for politician {pid}")
                break
            rows = table.find_all('tr')
            if len(rows) > 1:
                for row in rows:
                    ticker = row.find('span', class_='q-field issuer-ticker').text
                    if ":" in ticker:
                        ticker = ticker.split(":")[0]

                    dates = row.find_all('div', class_="text-size-3 font-medium")
                    years = row.find_all('div', class_="text-size-2 text-txt-dimmer")
                    published_date = datetime.datetime.strptime(
                        f"{dates[0].text.replace('Sept', 'Sep')} {years[0].text}", "%d %b %Y"
                    ).date()
                    filed_date = datetime.datetime.strptime(
                        f"{dates[1].text.replace('Sept', 'Sep')} {years[1].text}", "%d %b %Y"
                    ).date()

                    filed_after = (
                        f"{row.find('span', class_=lambda c: c and 'reporting-gap-tier' in c).text}"
                        " days"
                    )
                    order_type = (
                        row.find('span', class_=lambda c: c and "q-field tx-type" in c)
                        .text.replace('"', '')
                        .upper()
                    )
                    order_size = row.find(
                        'span', class_="mt-1 text-size-2 text-txt-dimmer hover:text-foreground"
                    ).text

                    trade = (
                        ticker,
                        format_date_mdy(published_date),
                        format_date_mdy(filed_date),
                        filed_after,
                        order_type,
                        order_size,
                    )
                    logger.debug(f"Identified trade with data {trade}")
                    trades.append(trade)
                page_num += 1
            else:
                break

        logger.debug(f"Returning data on {len(trades)} trades")
        return pd.DataFrame(
            trades,
            columns=['Ticker', 'Published Date', 'Filed Dated', 'Filed After', 'Order Type', 'Order Size'],
        )
