import json
import logging
import os
import discord

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

def update_gainer_message_id(market_time, message_id):
        data = get_config()
        if market_time == "premarket":
            data['reports']['gainers']['PREMARKET_MESSAGE_ID'] =  message_id
        elif market_time == "intraday":
            data['reports']['gainers']['INTRADAY_MESSAGE_ID'] =  message_id
        elif market_time == "afterhours":
            data['reports']['gainers']['AFTERHOURS_MESSAGE_ID'] =  message_id
        else:
            return
        write_config(data)

def get_gainer_message_id(market_time):
    data = get_config()
    if market_time == "premarket":
        return data['reports']['gainers']['PREMARKET_MESSAGE_ID']
    elif market_time == "intraday":
        return data['reports']['gainers']['INTRADAY_MESSAGE_ID']
    elif market_time == "afterhours":
        return data['reports']['gainers']['AFTERHOURS_MESSAGE_ID']
    else:
        return ""

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

def get_gainers_channel_id():
    try:
        channel_id = os.getenv("GAINERS_CHANNEL_ID")
        logger.debug("Gainers channel ID is {}".format(channel_id))
        return int(channel_id)
    except Exception as e:
        logger.exception("Failed to fetch gainers channel ID\n{}".format(e))
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

        