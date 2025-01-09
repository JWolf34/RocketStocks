import json
import logging
import os
#import discord
import datetime
import stockdata as sd
import pandas_market_calendars as mcal

# Logging configuration
logger = logging.getLogger(__name__)



CONFIG_PATH = os.getenv("CONFIG_PATH")

def get_config():
    
    try:
        config = open(CONFIG_PATH)
        data = json.load(config)
        return data 
    except FileNotFoundError as e:
        print("File not found")

def write_config(data):
    with open(CONFIG_PATH, 'w') as config_file:
        json.dump(data, config_file)

def get_env(var_name:str):
    variable = os.getenv(var_name)
    if variable is not None:
        return variable
    else:
        logger.error(f"Failed to fetch environment variable '{var_name}'\n{e}".format(e))
        return variable

def bot_setup():
    # Create database tables that do not exist
    sd.Postgres().create_tables()

    # Ensure data paths exist
    sd.validate_path(get_attachments_path())

class discord_utils:

    # Fetch Guild ID
    @property
    def discord_guild_id():
        return get_env('DISCORD_GUILD_ID')    

    # Channel IDs #

    def get_reports_channel_id():
        try:
            channel_id = os.getenv("REPORTS_CHANNEL_ID")
            return int(channel_id)
        except Exception as e:
            return ""

    def get_alerts_channel_id():
        try:
            channel_id = os.getenv("ALERTS_CHANNEL_ID")
            return int(channel_id)
        except Exception as e:
            return ""

    def get_screeners_channel_id():
        try:
            channel_id = os.getenv("SCREENERS_CHANNEL_ID")
            return int(channel_id)
        except Exception as e:
            return ""

    def get_charts_channel_id():
        try:
            channel_id = os.getenv("CHARTS_CHANNEL_ID")
            return int(channel_id)
        except Exception as e:
            return ""

    # Screener and alert message IDs #

    def update_gainer_message_id(message_id):
            market_time = utils().get_market_period()
            where_conditions = []
            if market_time == "premarket":
                where_conditions.append(('type', 'PREMARKET_GAINER_REPORT'))
            elif market_time == "intraday":
                where_conditions.append(('type', 'INTRADAY_GAINER_REPORT'))
            elif market_time == "afterhours":
                where_conditions.append(('type', 'AFTERHOURS_GAINER_REPORT'))
            else:
                return None
                
            sd.Postgres().update(table = 'reports',
                                set_fields=[('messageid', message_id)],
                                where_conditions=where_conditions)

    def update_volume_message_id(message_id):
        sd.Postgres().update(table='reports',
                            set_fields=[('messageid', message_id)],
                            where_conditions=[('type', 'UNUSUAL_COLUME_REPORT')])

    def get_gainer_message_id():
        market_time = utils().get_market_period()
        where_conditions = []
        if market_time == "premarket":
            where_conditions.append(('type', 'PREMARKET_GAINER_REPORT'))
        elif market_time == "intraday":
            where_conditions.append(('type', 'INTRADAY_GAINER_REPORT'))
        elif market_time == "afterhours":
            where_conditions.append(('type', 'AFTERHOURS_GAINER_REPORT'))  
        else:
            # Outside of market hours
            return None

        # During market hours
        result = sd.Postgres().select(table='reports',
                                    fields=['messageid'],
                                    where_conditions=where_conditions, 
                                    fetchall=False)
        if result is None:
            return result
        else:
            return result[0]

    def get_volume_message_id():
        select_script = f"""SELECT messageid FROM reports
                            WHERE type = 'UNUSUAL_VOLUME_REPORT';
                            """
        result = sd.Postgres().select(table='reports',
                                    fields=['messageid'],  
                                    where_conditions=[('type', 'UNUSUAL_VOLUME_REPORT')],
                                    fetchall=False)
        if result is None:
            return result
        else:
            return result[0]

    def update_alert_message_id(date, ticker, type, message_id):
        sd.Postgres().update(table='reports',
                            set_fields = [('messageid', message_id)],
                            where_conditions=[
                                ('date', date),
                                ('ticker', ticker),
                                ('type', type)
                            ])
    
    def get_alert_message_id(date, ticker, alert_type):
        select_script = f"""SELECT messageid FROM alerts
                            WHERE 
                            date = '{date}' AND
                            ticker = '{ticker}' AND
                            alert_type = '{alert_type}';
                            """
        result = sd.Postgres().select(table='alerts',
                                    fields=['messageid'],
                                    where_conditions=[
                                        ('date', date),
                                        ('ticker', ticker),
                                        ('alert_type', alert_type)
                                    ])
        if result is None:
            return result
        else:
            return result[0]

    def insert_alert_message_id(date, ticker, alert_type, message_id):
        table = 'alerts'
        fields = sd.Postgres().get_table_columns(table)
        values = [(date, ticker, alert_type, message_id)]
        sd.Postgres().insert(table=table, fields=fields, values=values)

class market_utils:

    def get_nyse_calendar():
        return mcal.get_calendar('NYSE')

    def market_open_today():
        today = datetime.datetime.now(datetime.UTC).date()
        nyse = utils.get_nyse_calendar()
        valid_days = nyse.valid_days(start_date=today, end_date=today)
        return today in valid_days.date

    def market_open_on_date(date):
        nyse = utils.get_nyse_calendar()
        return date in nyse.valid_days(start_date=date, end_date=date).date

    def get_market_schedule(date):
        nyse = utils.get_nyse_calendar()
        schedule = nyse.schedule(start_date=date, end_date=date, start='pre', end='post')
        return schedule

    def in_extended_hours():
        return utils.in_premarket() or utils.in_afterhours()

    def in_premarket():
        now = datetime.datetime.now(datetime.UTC)
        schedule = utils.get_market_schedule(now)
        if schedule.size > 0:
            premarket_start = schedule['pre'].iloc[0]
            intraday_start = schedule['market_open'].iloc[0]
            return now > premarket_start and now < intraday_start
        else: # Market is not open
            return False

    def in_intraday():
        now = datetime.datetime.now(datetime.UTC)
        schedule = utils.get_market_schedule(now)
        if schedule.size > 0:
            intraday_start = schedule['market_open'].iloc[0]
            afterhours_start = schedule['market_close'].iloc[0]
            return now > intraday_start and now < afterhours_start
        else: # Market is not open
            return False

    
    def in_afterhours():
        now = datetime.datetime.now(datetime.UTC)
        schedule = utils.get_market_schedule(now)
        if schedule.size > 0:
            afterhours_start = schedule['market_close'].iloc[0]
            market_end = schedule['post'].iloc[0]
            return now > afterhours_start and now < market_end
        else: # Market is not open
            return False

    def get_market_period():
        if utils.in_premarket():
            return "premarket"
        elif utils.in_intraday():
            return "intraday"
        if utils.in_afterhours():
            return "afterhours"
        else:
            return "EOD"    

    
    
class date_utils:

    def format_date_ymd(date):
        return date.strftime("%Y-%m-%d")

    def format_date_mdy(date):
        return date.strftime("%m/%d/%Y")


class datapath:
    def get_daily_data_path():
        return get_config()['data_paths']['DAILY_DATA_PATH']

    def get_minute_data_path():
        return get_config()['data_paths']['MINUTE_DATA_PATH']

    def get_intraday_data_path():
        return get_config()['data paths']['INTRADAY_DATA_PATH']

    def get_attachments_path():
        return get_config()['data_paths']['ATTACHMENTS_PATH']

    def get_utils_path():
        return get_config()['data_paths']['UTILS_PATH']

    def get_watchlists_path():
        return get_config()['data paths']['WATCHLISTS_PATH']

    def get_analysis_path():
        return get_config()['data paths']['ANALYSIS_PATH']

    def get_plots_path():
        return get_config()['data paths']['PLOTS_PATH']

    def get_financials_path():
        return get_config()['data paths']['FINANCIALS_PATH']

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
    db_password = get_env('POSTGRESS_PASSWORD')
    db_name = get_env('POSTGRES_DB')
    db_host = get_env('POSTGRES_HOST')
    
if __name__  == "__main__":
    print(secrets.discord_token)


        