"""Tests for rocketstocks.bot.senders.notification_sender."""
import datetime
import pytest
import discord
from rocketstocks.core.notifications.config import NotificationLevel
from rocketstocks.core.notifications.event import NotificationEvent
from rocketstocks.bot.senders.notification_sender import format_notification


def _event(level=NotificationLevel.SUCCESS, job_name="test_job", message="ok",
           source="test.module", traceback=None, elapsed_seconds=None):
    return NotificationEvent(
        level=level,
        source=source,
        job_name=job_name,
        message=message,
        traceback=traceback,
        elapsed_seconds=elapsed_seconds,
        timestamp=datetime.datetime(2024, 6, 15, 12, 0, 0),
    )


class TestFormatNotification:
    def test_success_produces_green_embed(self):
        embed = format_notification(_event(level=NotificationLevel.SUCCESS))
        assert embed.color == discord.Color.green()

    def test_failure_produces_red_embed(self):
        embed = format_notification(_event(level=NotificationLevel.FAILURE))
        assert embed.color == discord.Color.red()

    def test_warning_produces_orange_embed(self):
        embed = format_notification(_event(level=NotificationLevel.WARNING))
        assert embed.color == discord.Color.orange()

    def test_title_contains_level_and_job_name(self):
        embed = format_notification(_event(level=NotificationLevel.SUCCESS, job_name="my_job"))
        assert "SUCCESS" in embed.title
        assert "my_job" in embed.title

    def test_title_contains_failure_label(self):
        embed = format_notification(_event(level=NotificationLevel.FAILURE, job_name="fail_job"))
        assert "FAILURE" in embed.title

    def test_source_field_present(self):
        embed = format_notification(_event(source="rocketstocks.bot.cogs.reports"))
        field_names = [f.name for f in embed.fields]
        assert "Source" in field_names

    def test_source_field_value(self):
        embed = format_notification(_event(source="rocketstocks.bot.cogs.alerts"))
        source_field = next(f for f in embed.fields if f.name == "Source")
        assert source_field.value == "rocketstocks.bot.cogs.alerts"

    def test_duration_field_when_elapsed_set(self):
        embed = format_notification(_event(elapsed_seconds=2.5))
        field_names = [f.name for f in embed.fields]
        assert "Duration" in field_names

    def test_duration_field_value_format(self):
        embed = format_notification(_event(elapsed_seconds=2.5))
        duration_field = next(f for f in embed.fields if f.name == "Duration")
        assert "2.50s" in duration_field.value

    def test_no_duration_field_when_elapsed_none(self):
        embed = format_notification(_event(elapsed_seconds=None))
        field_names = [f.name for f in embed.fields]
        assert "Duration" not in field_names

    def test_message_field_present(self):
        embed = format_notification(_event(message="something happened"))
        field_names = [f.name for f in embed.fields]
        assert "Message" in field_names

    def test_message_field_value(self):
        embed = format_notification(_event(message="something happened"))
        msg_field = next(f for f in embed.fields if f.name == "Message")
        assert "something happened" in msg_field.value

    def test_no_traceback_field_when_none(self):
        embed = format_notification(_event(traceback=None))
        field_names = [f.name for f in embed.fields]
        assert "Traceback" not in field_names

    def test_traceback_field_present_when_provided(self):
        embed = format_notification(_event(traceback="Traceback (most recent call last):\n  File..."))
        field_names = [f.name for f in embed.fields]
        assert "Traceback" in field_names

    def test_traceback_truncated_to_1000_chars(self):
        long_tb = "x" * 2000
        embed = format_notification(_event(traceback=long_tb))
        tb_field = next(f for f in embed.fields if f.name == "Traceback")
        # Field value contains code block markers, actual traceback is last 1000 chars
        assert len(tb_field.value) <= 1000 + 10  # allow for code block markdown

    def test_timestamp_set_on_embed(self):
        ts = datetime.datetime(2024, 6, 15, 12, 0, 0)
        embed = format_notification(_event())
        # discord.py may attach local timezone info; compare naive parts only
        assert embed.timestamp.replace(tzinfo=None) == ts
