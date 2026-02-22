import datetime
import json
import logging
import discord
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.data.discord_state import DiscordState

logger = logging.getLogger(__name__)


async def send_alert(
    alert,
    channel: discord.TextChannel,
    dstate: DiscordState,
    view: discord.ui.View = None,
) -> discord.Message | None:
    """Send an alert to a Discord channel, with edit-in-place support.

    If the alert was already posted today:
      - Calls alert.override_and_edit(prev_data) to decide whether to post an update.
      - If update warranted: sends a new message linking back to the previous one.
    If not yet posted:
      - Sends a new message and records the message ID in the database.

    Returns the new discord.Message, or None if no message was sent.
    """
    message = alert.build_alert()

    today = datetime.datetime.now(tz=date_utils.timezone()).date()
    message_id = dstate.get_alert_message_id(
        date=today, ticker=alert.ticker, alert_type=alert.alert_type
    )

    if message_id:
        logger.debug(
            f"Alert {alert.alert_type} already reported for ticker '{alert.ticker}' today"
        )
        prev_alert_data = dstate.get_alert_message_data(
            date=today, ticker=alert.ticker, alert_type=alert.alert_type
        )
        # Deserialize if the DB column returns a JSON string instead of a dict
        if isinstance(prev_alert_data, str):
            prev_alert_data = json.loads(prev_alert_data)

        if alert.override_and_edit(prev_alert_data=prev_alert_data):
            logger.debug(
                f"Significant movements on ticker {alert.ticker} since alert last posted — updating..."
            )
            prev_message = await channel.fetch_message(message_id)
            prev_message_time = prev_message.created_at.astimezone(date_utils.timezone())
            message += (
                f"\n[Updated from last alert at "
                f"{prev_message_time.strftime('%I:%M %p')} {prev_message_time.tzname()}]"
                f"({prev_message.jump_url})"
            )
            sent = await channel.send(message, view=view)
            dstate.update_alert_message_data(
                date=today,
                ticker=alert.ticker,
                alert_type=alert.alert_type,
                messageid=sent.id,
                alert_data=alert.alert_data,
            )
            return sent
        else:
            logger.debug(
                f"Movements for ticker {alert.ticker} not significant enough to update alert"
            )
            return None
    else:
        sent = await channel.send(message, view=view)
        dstate.insert_alert_message_id(
            date=today,
            ticker=alert.ticker,
            alert_type=alert.alert_type,
            message_id=sent.id,
            alert_data=alert.alert_data,
        )
        return sent
