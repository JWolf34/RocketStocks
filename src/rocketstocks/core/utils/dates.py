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
    def format_duration_since(dt: datetime.datetime) -> str:
        """Format the elapsed time since a datetime, timezone-aware.

        Args:
            dt: A datetime object (can be naive or timezone-aware).

        Returns:
            A human-readable string like "5 minutes ago" or "2 hours and 30 minutes ago".
            Returns empty string if dt is None.
        """
        if dt is None:
            return ""

        # Ensure dt is timezone-aware; if naive, assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)

        # Get current time in the configured timezone
        tz = date_utils.timezone()
        now = datetime.datetime.now(tz=tz)

        # Calculate elapsed time
        elapsed = now - dt
        total_seconds = int(elapsed.total_seconds())

        if total_seconds < 0:
            return "(future)"

        # Format based on magnitude
        if total_seconds < 60:
            return f"{total_seconds} seconds ago"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            if seconds == 0:
                return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
            else:
                return f"{minutes} minute{'s' if minutes != 1 else ''} and {seconds} seconds ago"
        elif total_seconds < 86400:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            if minutes == 0:
                return f"{hours} hour{'s' if hours != 1 else ''} ago"
            else:
                return f"{hours} hour{'s' if hours != 1 else ''} and {minutes} minute{'s' if minutes != 1 else ''} ago"
        else:
            days = total_seconds // 86400
            hours = (total_seconds % 86400) // 3600
            if hours == 0:
                return f"{days} day{'s' if days != 1 else ''} ago"
            else:
                return f"{days} day{'s' if days != 1 else ''} and {hours} hour{'s' if hours != 1 else ''} ago"

    @staticmethod
    def timezone():
        tz = get_env("TZ")
        return ZoneInfo(tz if tz else "America/Chicago")
