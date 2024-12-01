import json
import logging
import os
import discord
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

# Reports #

def update_gainer_message_id(message_id):
        market_time = utils().get_market_period()
        if market_time == "premarket":
            update_script = f"""UPDATE reports
                                SET messageid = {message_id}
                                WHERE type = 'PREMARKET_GAINER_REPORT';
                                """
            sd.Postgres().update(update_script)
        elif market_time == "intraday":
            update_script = f"""UPDATE reports
                                SET messageid = {message_id}
                                WHERE type = 'INTRADAY_GAINER_REPORT';
                                """
            sd.Postgres().update(update_script)
        elif market_time == "afterhours":
            update_script = f"""UPDATE reports
                                SET messageid = {message_id}
                                WHERE type = 'AFTERHOURS_GAINER_REPORT';
                                """
            sd.Postgres().update(update_script)
        else:
            return None

def update_volume_message_id(message_id):
    update_script = f"""UPDATE reports
                        SET messageid = {message_id}
                        WHERE type = 'UNUSUAL_VOLUME_REPORT';
                        """
    sd.Postgres().update(update_script)

def update_alert_message_id(date, ticker, type, message_id):
    update_script = f"""UPDATE reports
                        SET messageid = {message_id}
                        WHERE 
                        date = '{date}' AND
                        ticker = '{ticker}
                        type = '{type}' 
                        ;
                        """
    sd.Postgres().update(update_script)

def get_gainer_message_id():
    market_time = utils().get_market_period()
    if market_time == "premarket":
        select_script = f"""SELECT messageid FROM reports
                            WHERE type = 'PREMARKET_GAINER_REPORT';
                            """
        result = sd.Postgres().select_one(select_script)
        if result is None:
            return result
        else:
            return result[0]
    elif market_time == "intraday":
        select_script = f"""SELECT messageid FROM reports
                            WHERE type = 'INTRADAY_GAINER_REPORT';
                            """
        result = sd.Postgres().select_one(select_script)
        if result is None:
            return result
        else:
            return result[0]
    elif market_time == "afterhours":
        select_script = f"""SELECT messageid FROM reports
                            WHERE type = 'AFTERHOURS_GAINER_REPORT';
                            """
        result = sd.Postgres().select_one(select_script)
        if result is None:
            return result
        else:
            return result[0]
    else:
        return None

def get_volume_message_id():
    select_script = f"""SELECT messageid FROM reports
                        WHERE type = 'UNUSUAL_VOLUME_REPORT';
                        """
    result = sd.Postgres().select_one(select_script)
    if result is None:
        return result
    else:
        return result[0]

def get_alert_message_id(date, ticker, alert_type):
    select_script = f"""SELECT messageid FROM alerts
                        WHERE 
                        date = '{date}' AND
                        ticker = '{ticker}' AND
                        alert_type = '{alert_type}';
                        """
    result = sd.Postgres().select_one(select_script)
    if result is None:
        return result
    else:
        return result[0]

def insert_alert_message_id(date, ticker, alert_type, message_id):
    table = 'alerts'
    fields = sd.Postgres().get_table_columns(table)
    values = [(date, ticker, alert_type, message_id)]
    sd.Postgres().insert(table=table, fields=fields, values=values)
  


# Data Path #

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

# Environment Variables #

def get_reports_channel_id():
    try:
        channel_id = os.getenv("REPORTS_CHANNEL_ID")
        logger.debug("Reports channel ID is {}".format(channel_id))
        return int(channel_id)
    except Exception as e:
        logger.exception("Failed to fetch reports channel ID\n{}".format(e))
        return ""

def get_alerts_channel_id():
    try:
        channel_id = os.getenv("ALERTS_CHANNEL_ID")
        logger.debug("Alerts channel ID is {}".format(channel_id))
        return int(channel_id)
    except Exception as e:
        logger.exception("Failed to fetch alerts channel ID\n{}".format(e))
        return ""

def get_screeners_channel_id():
    try:
        channel_id = os.getenv("SCREENERS_CHANNEL_ID")
        logger.debug("Screeners channel ID is {}".format(channel_id))
        return int(channel_id)
    except Exception as e:
        logger.exception("Failed to fetch screeners channel ID\n{}".format(e))
        return ""

def get_charts_channel_id():
    try:
        channel_id = os.getenv("CHARTS_CHANNEL_ID")
        logger.debug("Charts channel ID is {}".format(channel_id))
        return int(channel_id)
    except Exception as e:
        logger.exception("Failed to fetch charts channel ID\n{}".format(e))
        return ""

def get_discord_token():
        logger.debug("Fetching Discord bot token")
        try:
            token = os.getenv('DISCORD_TOKEN')
            logger.debug("Successfully fetched token")
            return token
        except Exception as e:
            logger.exception("Failed to fetch Discord bot token\n{}".format(e))
            return ""

def get_discord_guild_id():
        logger.debug("Fetching Discord guild ID")
        try:
            guild_id = os.getenv('DISCORD_GUILD_ID')
            logger.debug("Successfully fetched Discord guild ID")
            return int(guild_id)
        except Exception as e:
            logger.exception("Failed to fetch Discord guild ID\n{}".format(e))
            return ""

def get_news_api_token():
        logger.debug("Fetching News API token")
        try:
            token = os.getenv('NEWS_API_KEY')
            logger.debug("Successfully fetched token")
            return token
        except Exception as e:
            logger.exception("Failed to fetch News API token\n{}".format(e))
            return ""

def get_dolthub_api_token():
        logger.debug("Fetching Dolthub API token")
        try:
            token = os.getenv('DOLTHUB_API_TOKEN')
            logger.debug("Successfully fetched Dolthub API token")
            return token
        except Exception as e:
            logger.exception("Failed to fetch Dolthub API token\n{}".format(e))
            return ""

def get_schwab_api_key():
        logger.debug("Fetching Schwab API key")
        try:
            token = os.getenv('SCHWAB_API_KEY')
            logger.debug("Successfully fetched Schwab API key")
            return token
        except Exception as e:
            logger.exception("Failed to fetch Schwab API key\n{}".format(e))
            return ""

def get_schwab_api_secret():
        logger.debug("Fetching Schwab API secret")
        try:
            token = os.getenv('SCHWAB_API_SECRET')
            logger.debug("Successfully fetched Schwab API secret")
            return token
        except Exception as e:
            logger.exception("Failed to fetch Schwab API secret\n{}".format(e))
            return ""




def get_db_user():
        logger.debug("Fetching DB username")
        try:
            token = os.getenv('POSTGRES_USER')
            logger.debug("Successfully fetched DB username")
            return token
        except Exception as e:
            logger.exception("Failed to fetch  DB username\n{}".format(e))
            return ""

def get_db_password():
        logger.debug("Fetching DB password")
        try:
            token = os.getenv('POSTGRES_PASSWORD')
            logger.debug("Successfully fetched DB password")
            return token
        except Exception as e:
            logger.exception("Failed to fetch DB password\n{}".format(e))
            return ""

def get_db_name():
    logger.debug("Fetching DB name")
    try:
        token = os.getenv('POSTGRES_DB')
        logger.debug("Successfully fetched DB name")
        return token
    except Exception as e:
        logger.exception("Failed to fetch  DB name\n{}".format(e))
        return ""

def get_db_host():
    logger.debug("Fetching DB host")
    try:
        token = os.getenv('POSTGRES_HOST')
        logger.debug("Successfully fetched DB host")
        return token
    except Exception as e:
        logger.exception("Failed to fetch  DB host\n{}".format(e))
        return ""

class utils():
    def __init__(self):
        pass

    @staticmethod
    def get_nyse_calendar():
        return mcal.get_calendar('NYSE')

    @staticmethod
    def market_open_today():
        today = datetime.datetime.now(datetime.UTC).date()
        nyse = utils.get_nyse_calendar()
        valid_days = nyse.valid_days(start_date=today, end_date=today)
        return today in valid_days.date
    

    @staticmethod
    def get_market_schedule(date):
        nyse = utils.get_nyse_calendar()
        schedule = nyse.schedule(start_date=date, end_date=date, start='pre', end='post')
        return schedule

    @staticmethod
    def in_extended_hours():
        return utils.in_premarket() or utils.in_afterhours()

    @staticmethod
    def in_premarket():
        now = datetime.datetime.now(datetime.UTC)
        schedule = utils.get_market_schedule(now)
        if schedule.size > 0:
            premarket_start = schedule['pre'].iloc[0]
            intraday_start = schedule['market_open'].iloc[0]
            return now > premarket_start and now < intraday_start
        else: # Market is not open
            return False

    @staticmethod
    def in_intraday():
        now = datetime.datetime.now(datetime.UTC)
        schedule = utils.get_market_schedule(now)
        if schedule.size > 0:
            intraday_start = schedule['market_open'].iloc[0]
            afterhours_start = schedule['market_close'].iloc[0]
            return now > intraday_start and now < afterhours_start
        else: # Market is not open
            return False

    
    @staticmethod
    def in_afterhours():
        now = datetime.datetime.now(datetime.UTC)
        schedule = utils.get_market_schedule(now)
        if schedule.size > 0:
            afterhours_start = schedule['market_close'].iloc[0]
            market_end = schedule['post'].iloc[0]
            return now > afterhours_start and now < market_end
        else: # Market is not open
            return False

    @staticmethod
    def get_market_period():
        if utils.in_premarket():
            return "premarket"
        elif utils.in_intraday():
            return "intraday"
        if utils.in_afterhours():
            return "afterhours"
        else:
            return "EOD"    

    @staticmethod
    def format_date_ymd(date):
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def format_date_mdy(date):
        return date.strftime("%m/%d/%Y")


if __name__ =="__main__":
    pass

        