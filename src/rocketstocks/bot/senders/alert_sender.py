import datetime
import json
import logging
import discord
from rocketstocks.core.utils.dates import date_utils
from rocketstocks.data.discord_state import DiscordState
from rocketstocks.bot.senders.embed_utils import spec_to_embed

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
        Records a momentum snapshot in alert_data before persisting.
    If not yet posted:
      - Sends a new message and records the message ID in the database.

    Returns the new discord.Message, or None if no message was sent.
    """
    embed = spec_to_embed(alert.build())

    # For MomentumConfirmationAlert: link back to the original surge alert with duration
    if (hasattr(alert, 'data')
            and hasattr(alert.data, 'surge_alert_message_id')
            and alert.data.surge_alert_message_id):
        surge_link = (
            f"https://discord.com/channels/{channel.guild.id}/"
            f"{channel.id}/{alert.data.surge_alert_message_id}"
        )
        # Include duration since surge was flagged if available
        duration_text = ""
        if hasattr(alert.data, 'surge_flagged_at') and alert.data.surge_flagged_at:
            duration = date_utils.format_duration_since(alert.data.surge_flagged_at)
            if duration:
                duration_text = f" ({duration})"
        embed.description = (embed.description or "") + f"\n\n[📡 View original surge alert{duration_text}]({surge_link})"

    today = datetime.datetime.now(tz=date_utils.timezone()).date()
    message_id = await dstate.get_alert_message_id(
        date=today, ticker=alert.ticker, alert_type=alert.alert_type
    )

    if message_id:
        logger.debug(
            f"Alert {alert.alert_type} already reported for ticker '{alert.ticker}' today"
        )
        prev_alert_data = await dstate.get_alert_message_data(
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
            update_link = (
                "\n"
                f"[📡 Updated from last alert at "
                f"{prev_message_time.strftime('%I:%M %p')} {prev_message_time.tzname()}]"
                f"({prev_message.jump_url})"
            )

            embed.description = (embed.description or "") + f"\n{update_link}"
            sent = await channel.send(embed=embed, view=view)

            # Record momentum snapshot before persisting
            alert.record_momentum(prev_alert_data=prev_alert_data)

            await dstate.update_alert_message_data(
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
        sent = await channel.send(embed=embed, view=view)
        await dstate.insert_alert_message_id(
            date=today,
            ticker=alert.ticker,
            alert_type=alert.alert_type,
            message_id=sent.id,
            alert_data=alert.alert_data,
        )
        return sent
