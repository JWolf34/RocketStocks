import json
import logging
import os
import datetime
import pandas_market_calendars as mcal
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Logging configuration
logger = logging.getLogger(__name__)

# Load dotenv
load_dotenv()


'''General utilities'''

# Validate specified path exists and create it if needed
def validate_path(path):
    logger.info("Validating that path {} exists".format(path))
    if not (os.path.isdir(path)):
        logger.info("Path {} does not exist. Creating path...".format(path))
        os.makedirs(path) 
        return 
    else:
        logger.info("Path {} exists in the filesystem".format(path))
        return True

def bot_setup():
    from db import Postgres
    # Create database tables that do not exist
    Postgres().create_tables()

    # Ensure data paths exist
    validate_path(datapaths.attachments_path)

def get_env(var_name:str):
    variable = os.getenv(var_name)
    if variable is not None:
        return variable
    else:
        logger.error(f"Failed to fetch environment variable '{var_name}'")
        return variable


class config:
    """Utilities for editing the config file"""
    def __init__(self):
        self.path = get_env("CONFIG_PATH")

    def load_config(self):
        try:
            config = open(self.path)
            data = json.load(config)
            return data 
        except FileNotFoundError as e:
            print("File not found")

    def write_config(self, data):
        with open(self.path, 'w') as config_file:
            json.dump(data, config_file)

class discord_utils():

    """Utilities for discord functions"""    
    def __init__(self, db):
        self.db = db # Postgres

    # Guild ID
    guild_id = get_env('DISCORD_GUILD_ID')    

    # Channel IDs 
    reports_channel_id = int(get_env("REPORTS_CHANNEL_ID"))
    alerts_channel_id = int(get_env("ALERTS_CHANNEL_ID"))
    screeners_channel_id = int(get_env("SCREENERS_CHANNEL_ID"))
    charts_channel_id = int(get_env("CHARTS_CHANNEL_ID"))

    # Screener and alert message IDs #
    def get_screener_message_id(self, screener_type:str):
        where_conditions = [('type', f'{screener_type}_REPORT')]

        # During market hours
        result = self.db.select(table='reports',
                                    fields=['messageid'],
                                    where_conditions=where_conditions, 
                                    fetchall=False)
        if not result:
            return result
        else:
            return result[0]

    def update_screener_message_id(self, message_id:str, screener_type:str):
            where_conditions = [('type', f'{screener_type}_REPORT')]
                
            self.db.update(table = 'reports',
                           set_fields=[('messageid', message_id)],
                           where_conditions=where_conditions)
            
    def insert_screener_message_id(self, message_id:str, screener_type:str):
        values = [(f'{screener_type}_REPORT', message_id)]
        
        self.db.insert(table='reports',
                       fields=self.db.get_table_columns(table='reports'),
                       values=values)


    def update_volume_message_id(self, message_id):
        self.db.update(table='reports',
                       set_fields=[('messageid', message_id)],
                       where_conditions=[('type', 'UNUSUAL_VOLUME_REPORT')])

        

    def get_volume_message_id(self):
        # Query
        """SELECT messageid FROM reports
        WHERE type = 'UNUSUAL_VOLUME_REPORT';
        """
        result = self.db.select(table='reports',
                                    fields=['messageid'],  
                                    where_conditions=[('type', 'UNUSUAL_VOLUME_REPORT')],
                                    fetchall=False)
        if result is None:
            return result
        else:
            return result[0]

    def update_alert_message_data(self, date, ticker, alert_type, messageid, alert_data):
        self.db.update(table='alerts',
                            set_fields = [
                                ('messageid', messageid),
                                ('alert_data', json.dumps(alert_data))
                                ],
                            where_conditions=[
                                ('date', date),
                                ('ticker', ticker),
                                ('alert_type', alert_type)
                            ])
                
    
    def get_alert_message_id(self, date, ticker, alert_type):
        
        # Query
        """
        SELECT messageid FROM alerts
        WHERE 
        date = '{date}' AND
        ticker = '{ticker}' AND
        alert_type = '{alert_type}';
        """

        result = self.db.select(table='alerts',
                                    fields=['messageid'],
                                    where_conditions=[
                                        ('date', date),
                                        ('ticker', ticker),
                                        ('alert_type', alert_type)
                                    ], 
                                    fetchall=False)
        if result is None:
            return result
        else:
            return result[0]
    
    
    def get_alert_message_data(self, date, ticker, alert_type):
        result = self.db.select(table='alerts',
                                    fields=['alert_data'],
                                    where_conditions=[
                                        ('date', date),
                                        ('ticker', ticker),
                                        ('alert_type', alert_type)
                                    ], 
                                    fetchall=False)
        if result is None:
            return result
        else:
            return result[0]
            

    def insert_alert_message_id(self, date, ticker, alert_type, message_id, alert_data):
        table = 'alerts'
        fields = self.db.get_table_columns(table)
        values = [(date, ticker, alert_type, message_id, json.dumps(alert_data))]
        self.db.insert(table=table, fields=fields, values=values)

class market_utils():

    def __init__(self):
        self._calendar = mcal.get_calendar('NYSE')
        now = datetime.datetime.now()
        self._schedule = self.get_market_schedule(now.date())

    @property
    def calendar(self):
        return self._calendar
    
    @property
    def schedule(self):
        return self._schedule
    
    @schedule.setter
    def schedule(self, new_sched):
        self._schedule = new_sched
    
    def get_market_schedule(self, date):
        schedule = self.calendar.schedule(start_date=date, end_date=date, start='pre', end='post')
        return schedule

    def market_open_today(self):
        today = datetime.datetime.now(datetime.UTC).date()
        valid_days = self.calendar.valid_days(start_date=today, end_date=today)
        return today in valid_days.date

    def market_open_on_date(self, date):
        return date in self.calendar.valid_days(start_date=date, end_date=date).date

    def in_extended_hours(self):
        return self.get_market_period() in ['premarket', 'aftermarket']
    
    def in_market_hours(self):
        return self.get_market_period() != 'EOD'

    def in_premarket(self):
        now = datetime.datetime.now(datetime.UTC)
        #self.schedule = self.get_market_self.schedule(now)
        if self.schedule.size > 0:
            premarket_start = self.schedule['pre'].iloc[0]
            intraday_start = self.schedule['market_open'].iloc[0]
            return now > premarket_start and now < intraday_start
        else: # Market is not open
            return False

    def in_intraday(self):
        now = datetime.datetime.now(datetime.UTC)
        #self.schedule = self.get_market_self.schedule(now)
        if self.schedule.size > 0:
            intraday_start = self.schedule['market_open'].iloc[0]
            aftermarket_start = self.schedule['market_close'].iloc[0]
            return now > intraday_start and now < aftermarket_start
        else: # Market is not open
            return False
    
    def in_aftermarket(self):
        now = datetime.datetime.now(datetime.UTC)
        #self.schedule = self.get_market_self.schedule(now)
        if self.schedule.size > 0:
            aftermarket_start = self.schedule['market_close'].iloc[0]
            market_end = self.schedule['post'].iloc[0]
            return now > aftermarket_start and now < market_end
        else: # Market is not open
            return False

    def get_market_period(self):
        # Update schedule
        now = datetime.datetime.now()
        self._schedule = self.get_market_schedule(now.date())
        
        if self.in_premarket():
            return "premarket"
        elif self.in_intraday():
            return "intraday"
        if self.in_aftermarket():
            return "aftermarket"
        else:
            return "EOD"    

    
    
class date_utils:


    @staticmethod
    def format_date_ymd(date):
        if isinstance(date, str):
            date = datetime.datetime.strptime(date, "%m/%d/%Y")
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def format_date_mdy(date):
        if isinstance(date, str):
            date = datetime.datetime.strptime(date, "%Y-%m-%d")
        return date.strftime("%m/%d/%Y")

    @staticmethod
    def dt_round_down(dt:datetime.datetime):
        delta = dt.minute % 5
        return dt.replace(minute = dt.minute - delta)

    @staticmethod
    def seconds_until_5m_interval():
        now = datetime.datetime.now().astimezone()
        if now.minute % 5 == 0:
            return 0
        minutes_by_five = now.minute // 5
        # get the difference in times
        diff = (minutes_by_five + 1) * 5 - now.minute
        future = (now + datetime.timedelta(minutes=diff)).replace(second=0, microsecond=0)
        return (future-now).total_seconds()
    
    @staticmethod
    def timezone():
        tz = get_env("TZ")
        return ZoneInfo(tz if tz else "America/Chicago")



class datapaths:

    attachments_path = "discord/attachments"

class secrets:

    # Discord
    discord_token = get_env('DISCORD_TOKEN')

    # News API
    news_api_token = get_env('NEWS_API_KEY')
    
    # Schwab
    schwab_api_key = get_env('SCHWAB_API_KEY')
    schwab_api_secret = get_env('SCHWAB_API_SECRET')

    # Postgres
    db_user = get_env('POSTGRES_USER')
    db_password = get_env('POSTGRES_PASSWORD')
    db_name = get_env('POSTGRES_DB')
    db_host = get_env('POSTGRES_HOST')
    db_port = (get_env('POSTGRES_PORT'))
    
if __name__  == "__main__":
    pass

        