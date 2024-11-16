import json
import logging
import os

# Logging configuration
logger = logging.getLogger(__name__)

def get_config():
    try:
        config = open("./discord/config.json")
        data = json.load(config)
        return data 
    except FileNotFoundError as e:
        print("File not found")

def write_config(data):
    with open("./discord/config.json", 'w') as config_file:
        json.dump(data, config_file)

# Reports #

def update_gainer_message_id(market_time, message_id):
        data = get_config()
        if market_time == "premarket":
            data['reports']['PREMARKET_MESSAGE_ID'] =  message_id
        elif market_time == "intraday":
            data['reports']['INTRADAY_MESSAGE_ID'] =  message_id
        elif market_time == "afterhours":
            data['reports']['AFTERHOURS_MESSAGE_ID'] =  message_id
        else:
            return
        write_config(data)

def get_gainer_message_id(market_time):
    data = get_config()
    if market_time == "premarket":
        return data['reports']['PREMARKET_MESSAGE_ID']
    elif market_time == "intraday":
        return data['reports']['INTRADAY_MESSAGE_ID']
    elif market_time == "afterhours":
        return data['reports']['AFTERHOURS_MESSAGE_ID']
    else:
        return ""

# Data Path #

def get_daily_data_path():
    return get_config()['data_paths']['DAILY_DATA_PATH']

def get_minute_data_path():
    return get_config()['data_paths']['MINUTE_DATA_PATH']

def get_attachments_path():
    return get_config()['data_paths']['ATTACHMENTS_PATH']

def get_utils_path():
    return get_config()['data_paths']['UTILS_PATH']

# Environment Variables #

def get_reports_channel_id():
    try:
        channel_id = os.getenv("REPORTS_CHANNEL_ID")
        logger.debug("Reports channel ID is {}".format(channel_id))
        return channel_id
    except Exception as e:
        logger.exception("Failed to fetch reports channel ID\n{}".format(e))
        return ""

def get_alerts_channel_id():
    try:
        channel_id = os.getenv("ALERTS_CHANNEL_ID")
        logger.debug("Alerts channel ID is {}".format(channel_id))
        return channel_id
    except Exception as e:
        logger.exception("Failed to fetch alerts channel ID\n{}".format(e))
        return ""


        