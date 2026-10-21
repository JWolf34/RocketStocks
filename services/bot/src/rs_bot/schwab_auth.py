"""`/setup schwab`: re-authorize Schwab from Discord without ever holding the token.

The bot only moves text through schwab_auth_request: it opens a request, shows the
sign-in URL ingest fills in, stores the callback URL John pastes into a modal, and
reports what ingest decided. Ingest does the OAuth exchange and writes the token.

Every reply is ephemeral. The pasted callback URL carries a single-use OAuth code:
it is never logged or echoed back.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

import discord
from discord import app_commands
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from rs_domain import AuthRequestState, SchwabAuthRequest
from rs_persistence import AUTH_REQUEST_WINDOW, AuthRequestRepository

logger = logging.getLogger(__name__)

DENIED = "Only the server owner or the configured admin can run this."


def is_authorized(user_id: int, guild_owner_id: int | None, admin_user_id: int | None) -> bool:
    return user_id == guild_owner_id or (admin_user_id is not None and user_id == admin_user_id)


def looks_like_callback(text: str) -> bool:
    """A cheap shape check so a typo does not burn the single-use sign-in."""
    parsed = urlparse(text.strip())
    return parsed.scheme in ("http", "https") and "code" in parse_qs(parsed.query)


@dataclass(frozen=True)
class SignIn:
    request_id: int
    auth_url: str | None  # None when ingest did not answer in time


class SchwabAuthFlow:
    """The request side of the handshake; discord-free so it is testable on its own."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        poll_seconds: float = 1.0,
        url_wait_seconds: float = 30.0,
        outcome_wait_seconds: float = 60.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._session_factory = session_factory
        self._poll = poll_seconds
        self._url_wait = url_wait_seconds
        self._outcome_wait = outcome_wait_seconds
        self._sleep = sleep

    async def start(self, user_id: int) -> SignIn:
        async with self._session_factory() as session:
            request = await AuthRequestRepository(session).create(user_id)
            await session.commit()
        current = await self._wait_while(request.id, AuthRequestState.REQUESTED, self._url_wait)
        return SignIn(request.id, None if current is None else current.auth_url)

    async def submit(self, request_id: int, user_id: int, callback_url: str) -> str:
        """Hand the pasted URL to ingest and return the message to show John."""
        if not looks_like_callback(callback_url):
            return (
                "That does not look like the callback address. After signing in, copy the full "
                "address from the browser bar (it contains `code=`) and paste it again."
            )
        async with self._session_factory() as session:
            repo = AuthRequestRepository(session)
            request = await repo.get(request_id)
            if request is None or request.requested_by != user_id:
                return "That sign-in request is not yours. Run /setup schwab to start one."
            if not await repo.set_callback_url(request_id, callback_url):
                return "That sign-in request has expired or was already used. Run /setup schwab again."
            await session.commit()
        done = await self._wait_while(
            request_id, AuthRequestState.CALLBACK_RECEIVED, self._outcome_wait
        )
        if done is None or done.state is AuthRequestState.CALLBACK_RECEIVED:
            return "Ingest has not answered yet. Check the ingest logs, then run /setup schwab again."
        if done.state is AuthRequestState.DONE:
            return "Schwab is signed in again. Schwab jobs resume shortly."
        if done.state is AuthRequestState.EXPIRED:
            return "The sign-in window closed. Run /setup schwab again."
        return f"Sign-in failed: {done.error or 'unknown error'}"

    async def _wait_while(
        self, request_id: int, state: AuthRequestState, seconds: float
    ) -> SchwabAuthRequest | None:
        """Poll until the request leaves `state`; the last reading, None if it vanished."""
        current = None
        for _ in range(max(1, int(seconds / self._poll))):
            async with self._session_factory() as session:
                current = await AuthRequestRepository(session).get(request_id)
            if current is None or current.state is not state:
                return current
            await self._sleep(self._poll)
        return current


class _CallbackModal(discord.ui.Modal, title="Schwab sign-in"):
    callback_url: discord.ui.TextInput = discord.ui.TextInput(
        label="Callback URL", style=discord.TextStyle.paragraph, max_length=2000
    )

    def __init__(self, flow: SchwabAuthFlow, request_id: int) -> None:
        super().__init__()
        self._flow = flow
        self._request_id = request_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)
        message = await self._flow.submit(
            self._request_id, interaction.user.id, self.callback_url.value
        )
        await interaction.followup.send(message, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:  # type: ignore[override]
        # The type only: the traceback text can carry the pasted URL.
        logger.error("schwab callback modal failed (%s)", type(error).__name__)
        message = "Something went wrong. Run /setup schwab again."
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)


class _SignInView(discord.ui.View):
    def __init__(self, flow: SchwabAuthFlow, request_id: int, user_id: int) -> None:
        super().__init__(timeout=AUTH_REQUEST_WINDOW.total_seconds())
        self._flow = flow
        self._request_id = request_id
        self._user_id = user_id

    @discord.ui.button(label="Paste callback URL", style=discord.ButtonStyle.primary)
    async def paste(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if interaction.user.id != self._user_id:
            await interaction.response.send_message(DENIED, ephemeral=True)
            return
        await interaction.response.send_modal(_CallbackModal(self._flow, self._request_id))


def add_schwab_command(
    setup: app_commands.Group, flow: SchwabAuthFlow, admin_user_id: int | None
) -> None:
    @setup.command(name="schwab", description="Sign in to Schwab again")
    async def schwab(interaction: discord.Interaction) -> None:
        owner_id = interaction.guild.owner_id if interaction.guild is not None else None
        if not is_authorized(interaction.user.id, owner_id, admin_user_id):
            await interaction.response.send_message(DENIED, ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        sign_in = await flow.start(interaction.user.id)
        if sign_in.auth_url is None:
            await interaction.followup.send(
                "Ingest did not answer. Check that it is running, then try again.", ephemeral=True
            )
            return
        minutes = int(AUTH_REQUEST_WINDOW.total_seconds() // 60)
        await interaction.followup.send(
            "1. Open the link below and sign in to Schwab.\n"
            "2. Your browser then lands on a page that may fail to load. That is expected.\n"
            "3. Copy the full address from the browser bar, press the button and paste it.\n"
            f"The request closes in {minutes} minutes.\n\n"
            f"{sign_in.auth_url}",
            view=_SignInView(flow, sign_in.request_id, interaction.user.id),
            ephemeral=True,
        )
