import datetime
import logging
from zoneinfo import ZoneInfo
from rocketstocks.core.config.environment import get_env

logger = logging.getLogger(__name__)


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
    def format_date_from_iso(date):
        return datetime.datetime.fromisoformat(date)

    @staticmethod
    def dt_round_down(dt: datetime.datetime):
        delta = dt.minute % 5
        return dt.replace(minute=dt.minute - delta)

    @staticmethod
    def seconds_until_minute_interval(minute: int):
        now = datetime.datetime.now().astimezone()
        if now.minute % minute == 0:
            return 0
        minutes_by_increment = now.minute // minute
        diff = (minutes_by_increment + 1) * minute - now.minute
        future = (now + datetime.timedelta(minutes=diff)).replace(second=0, microsecond=0)
        return (future - now).total_seconds()

    @staticmethod
    def round_down_nearest_minute(minute: int):
        now = datetime.datetime.now()
        rounded = now - (now - datetime.datetime.min) % datetime.timedelta(minutes=minute)
        return rounded

    @staticmethod
    def timezone():
        tz = get_env("TZ")
        return ZoneInfo(tz if tz else "America/Chicago")
