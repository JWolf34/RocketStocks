"""Per-service database roles: created and granted by migrate, after `upgrade`.

Migrate is the only superuser path. Every other service connects as its own
least-privilege role, so a compromise of the internet-facing one (bot) cannot
read the Schwab refresh token, and the read-only role (mcp) cannot write anything.

GRANTS is the single source of truth: one entry per table, naming what each
service may do. A table with no entry fails the test suite, so adding a table
forces a decision. `apply_roles` revokes everything and re-grants from GRANTS in
one transaction, so it is idempotent and also removes stale privileges.
"""

import logging
import re
from collections.abc import Mapping

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

logger = logging.getLogger(__name__)

SERVICES = ("ingest", "bot", "mcp", "research")
ROLE_PREFIX = "rs_"

# Compose interpolates these passwords into DSNs unencoded, so reject characters
# that would silently break the URL instead of encoding them.
_URL_SAFE_PASSWORD = re.compile(r"[A-Za-z0-9._~-]{16,}")

_SELECT = ("SELECT",)
_SELECT_INSERT = ("SELECT", "INSERT")

# table -> service -> privileges (GRANT syntax, so column lists are allowed).
# `{}` means no service has access (migrate only).
GRANTS: dict[str, dict[str, tuple[str, ...]]] = {
    "alerts": {
        "ingest": _SELECT_INSERT,
        "bot": (
            "SELECT",
            "UPDATE (delivery, sending_at, posted_at, message_id, thread_id, post_attempts)",
        ),
        "mcp": _SELECT,
        "research": _SELECT,
    },
    # Written once by ingest and never changed; the readers rank and report over them.
    "candidate": {
        "ingest": _SELECT_INSERT,
        "mcp": _SELECT,
        "research": _SELECT,
    },
    "daily_bar": {
        "ingest": _SELECT_INSERT,
        "bot": _SELECT,
        "mcp": _SELECT,
        "research": _SELECT,
    },
    "bar_request": {
        "ingest": ("SELECT", "INSERT", "UPDATE"),
        "mcp": _SELECT,
        "research": _SELECT,
    },
    # Written by the backfill tool on the owner DSN; no service touches it.
    "import_verification": {},
    "options_chain_snapshot": {
        "ingest": _SELECT_INSERT,
        "mcp": _SELECT,
        "research": _SELECT,
    },
    "popularity_snapshot": {
        "ingest": _SELECT_INSERT,
        "bot": _SELECT,
        "mcp": _SELECT,
        "research": _SELECT,
    },
    # callback_url carries a single-use OAuth code: the bot writes it, only
    # ingest consumes it, and no read-only role ever sees it.
    "schwab_auth_request": {
        "ingest": ("SELECT", "UPDATE"),
        "bot": ("SELECT", "INSERT", "UPDATE (state, callback_url)"),
    },
    "schwab_tokens": {
        "ingest": ("SELECT", "INSERT", "UPDATE"),
    },
    # Which expiry DMs went out for which token issue; no secret in it.
    "schwab_token_notice": {
        "bot": _SELECT_INSERT,
    },
    # Rewritten whole for a Trigger version by the nightly job (delete, insert) and read by the rest.
    "outcome": {
        "ingest": ("SELECT", "INSERT", "DELETE"),
        "mcp": _SELECT,
        "research": _SELECT,
    },
    # Upserted per horizon by the nightly job; the bot shows it beside an Alert.
    "track_record": {
        "ingest": ("SELECT", "INSERT", "UPDATE"),
        "bot": _SELECT,
        "mcp": _SELECT,
        "research": _SELECT,
    },
    # Which server role an Alert pings per Trigger; the bot's /setup roles writes it.
    "trigger_role": {
        "bot": ("SELECT", "INSERT", "UPDATE", "DELETE"),
    },
    "unpriced_symbol": {
        "ingest": ("SELECT", "INSERT", "UPDATE", "DELETE"),
        "mcp": _SELECT,
        "research": _SELECT,
    },
}

# view -> service -> privileges. schwab_token_issue exposes only when the token was
# issued, so the bot can time its expiry notices without ever reading schwab_tokens.
VIEW_GRANTS: dict[str, dict[str, tuple[str, ...]]] = {
    "schwab_token_issue": {"bot": _SELECT},
}


class RolesConfigError(Exception):
    """Role passwords are missing or unusable; the message names the variables."""


def password_var(service: str) -> str:
    return f"RS_DB_PASSWORD_{service.upper()}"


def read_passwords(environ: Mapping[str, str]) -> dict[str, str]:
    """Every service's password from the environment, or RolesConfigError."""
    missing = [password_var(s) for s in SERVICES if not environ.get(password_var(s))]
    if missing:
        raise RolesConfigError(f"missing role password variable(s): {', '.join(missing)}")
    passwords = {s: environ[password_var(s)] for s in SERVICES}
    weak = [password_var(s) for s, p in passwords.items() if not _URL_SAFE_PASSWORD.fullmatch(p)]
    if weak:
        raise RolesConfigError(
            f"{', '.join(weak)} must be at least 16 characters of A-Z a-z 0-9 . _ ~ - "
            "(compose puts them in DSNs unencoded); generate one with `openssl rand -hex 24`"
        )
    return passwords


def role_name(service: str, prefix: str = ROLE_PREFIX) -> str:
    return f"{prefix}{service}"


def apply_roles(
    admin_url: str, passwords: Mapping[str, str], *, prefix: str = ROLE_PREFIX
) -> list[str]:
    """Create or update every service role and its grants; returns the role names.

    `admin_url` is the superuser SQLAlchemy DSN (the same RS_DB_URL migrate uses).
    `prefix` exists so tests can use throwaway names: roles are cluster-global and
    concurrent GRANTs on the same role race.
    """
    conninfo = make_url(admin_url).set(drivername="postgresql").render_as_string(False)
    roles = {service: role_name(service, prefix) for service in SERVICES}
    with psycopg.connect(conninfo) as conn:  # one transaction: all or nothing
        for service, role in roles.items():
            _ensure_role(conn, role, passwords[service])
            _grant(conn, role, service)
    logger.info("database roles applied: %s", ", ".join(roles.values()))
    return list(roles.values())


def _ensure_role(conn: psycopg.Connection, role: str, password: str) -> None:
    name = sql.Identifier(role)
    exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
    verb = "ALTER" if exists else "CREATE"
    conn.execute(
        sql.SQL(
            "{verb} ROLE {name} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"
        ).format(verb=sql.SQL(verb), name=name)
    )
    # Store a SCRAM verifier, never the plaintext: DDL cannot take bind
    # parameters, and plaintext in the statement would reach server logs.
    verifier = conn.pgconn.encrypt_password(password.encode(), role.encode(), b"scram-sha-256")
    conn.execute(
        sql.SQL("ALTER ROLE {name} PASSWORD {v}").format(
            name=name, v=sql.Literal(verifier.decode())
        )
    )


def _grant(conn: psycopg.Connection, role: str, service: str) -> None:
    name = sql.Identifier(role)
    conn.execute(sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {}").format(name))
    conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(name))
    for table, by_service in (GRANTS | VIEW_GRANTS).items():
        privileges = by_service.get(service)
        if privileges:
            conn.execute(
                sql.SQL("GRANT {privileges} ON TABLE {table} TO {name}").format(
                    privileges=sql.SQL(", ").join(sql.SQL(p) for p in privileges),
                    table=sql.Identifier(table),
                    name=name,
                )
            )
