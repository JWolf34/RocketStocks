import json
import logging
import os
import discord
import datetime
import stockdata as sd

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
            logger.exception("Failed to fetch  DB password\n{}".format(e))
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
    def in_premarket():
        today = datetime.datetime.now()
        PREMARKET_START = today.replace(hour=7, minute=0, second=0, microsecond=0)
        INTRADAY_START = today.replace(hour=8, minute=30, second=0, microsecond=0)
        return today > PREMARKET_START and today < INTRADAY_START

    @staticmethod
    def in_intraday():
        today = datetime.datetime.now()
        INTRADAY_START = today.replace(hour=8, minute=30, second=0, microsecond=0)
        AFTERHOURS_START = today.replace(hour=15, minute=0, second=0, microsecond=0)
        return today > INTRADAY_START and today < AFTERHOURS_START
    
    @staticmethod
    def in_afterhours():
        today = datetime.datetime.now()
        AFTERHOURS_START = today.replace(hour=15, minute=0, second=0, microsecond=0)
        MARKET_END = today.replace(hour=17, minute=0, second=0, microsecond=0)
        return today > AFTERHOURS_START and today < MARKET_END

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



        