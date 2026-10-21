# ruff: noqa: F811 - fixture params named after the imported migrated_db_url
# fixture are how pytest injects it, not redefinitions (test_cli idiom).

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg import errors
from sqlalchemy import select
from sqlalchemy.engine import make_url

from rs_domain import (
    Alert,
    AuthRequestState,
    PopularityOrigin,
    PopularitySnapshot,
    TrackRecord,
)
from rs_migrate.__main__ import main
from rs_migrate.roles import (
    GRANTS,
    SERVICES,
    VIEW_GRANTS,
    RolesConfigError,
    apply_roles,
    password_var,
    read_passwords,
    role_name,
)
from rs_persistence import (
    NOTIFY_CHANNEL,
    AlertRepository,
    AuthRequestRepository,
    Base,
    PopularityRepository,
    SchwabTokenStore,
    TokenNoticeRepository,
    TrackRecordRepository,
    TrackRecordRow,
    build_engine,
    build_session_factory,
)
from rs_persistence.testing import migrated_db_url  # noqa: F401

NOW = datetime(2026, 7, 6, 14, 0, tzinfo=UTC)
TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")


def _conninfo(url: str, role: str | None = None, password: str | None = None) -> str:
    parsed = make_url(url).set(drivername="postgresql")
    if role is not None:
        parsed = parsed.set(username=role, password=password)
    return parsed.render_as_string(False)


class Roles:
    """The four roles applied to a migrated database under throwaway names."""

    def __init__(self, admin_url: str, prefix: str) -> None:
        self.admin_url = admin_url
        self.prefix = prefix
        self.passwords = {s: f"pw-{s}-{uuid.uuid4().hex}" for s in SERVICES}

    def apply(self) -> None:
        apply_roles(self.admin_url, self.passwords, prefix=self.prefix)

    def name(self, service: str) -> str:
        return role_name(service, self.prefix)

    def url(self, service: str, driver: str = "postgresql+psycopg") -> str:
        return (
            make_url(self.admin_url)
            .set(drivername=driver, username=self.name(service), password=self.passwords[service])
            .render_as_string(False)
        )

    def connect(self, service: str) -> psycopg.Connection:
        return psycopg.connect(_conninfo(self.url(service)), autocommit=True)

    def admin(self) -> psycopg.Connection:
        return psycopg.connect(_conninfo(self.admin_url), autocommit=True)


def _clear_data(r: Roles) -> None:
    with r.admin() as conn:
        conn.execute(
            "TRUNCATE alerts, candidate, outcome, track_record, popularity_snapshot, schwab_auth_request,"
            " schwab_tokens, schwab_token_notice RESTART IDENTITY"
        )


@pytest.fixture
def roles(migrated_db_url) -> Iterator[Roles]:
    # Roles are cluster-global and the test cluster is shared, so every test gets
    # its own names; the grants live in this test's throwaway database.
    r = Roles(migrated_db_url, f"rs_t{uuid.uuid4().hex[:8]}_")
    r.apply()
    _clear_data(r)
    yield r
    _clear_data(r)
    with r.admin() as conn:
        for service in SERVICES:
            conn.execute(f'DROP OWNED BY "{r.name(service)}"')
            conn.execute(f'DROP ROLE "{r.name(service)}"')


def _denied(conn: psycopg.Connection, statement: str) -> None:
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute(statement)


# --- the grants table -------------------------------------------------------


def test_every_orm_table_has_a_grant_decision():
    tables = set(Base.metadata.tables)
    undecided = sorted(tables - GRANTS.keys())
    assert not undecided, (
        f"table(s) {undecided} have no entry in rs_migrate.roles.GRANTS: decide which "
        "services may read or write each (an empty dict means migrate only)"
    )
    stale = sorted(GRANTS.keys() - tables)
    assert not stale, f"GRANTS names table(s) {stale} that are not in the ORM metadata"


def test_every_migrated_table_has_a_grant_decision(roles):
    with roles.admin() as conn:
        migrated = {
            name
            for (name,) in conn.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                " AND tablename <> 'alembic_version'"
            )
        }
    undecided = sorted(migrated - GRANTS.keys())
    assert not undecided, (
        f"migrated table(s) {undecided} have no entry in rs_migrate.roles.GRANTS"
    )


def test_every_migrated_view_has_a_grant_decision(roles):
    with roles.admin() as conn:
        views = {
            name for (name,) in conn.execute("SELECT viewname FROM pg_views WHERE schemaname = 'public'")
        }
    assert views == VIEW_GRANTS.keys()


def test_grants_only_name_known_services():
    for table, by_service in (GRANTS | VIEW_GRANTS).items():
        assert set(by_service) <= set(SERVICES), f"{table} grants an unknown service"


def test_applied_privileges_match_the_grants_table(roles):
    with roles.admin() as conn:
        for table, by_service in GRANTS.items():
            columns = [
                c
                for (c,) in conn.execute(
                    "SELECT column_name FROM information_schema.columns"
                    " WHERE table_schema = 'public' AND table_name = %s",
                    (table,),
                )
            ]
            for service in SERVICES:
                role = roles.name(service)
                declared = by_service.get(service, ())
                table_level = {p for p in declared if "(" not in p}
                column_update = {
                    c.strip()
                    for p in declared
                    if p.startswith("UPDATE (")
                    for c in p[len("UPDATE (") : -1].split(",")
                }
                for privilege in TABLE_PRIVILEGES:
                    (has,) = conn.execute(
                        "SELECT has_table_privilege(%s, %s, %s)", (role, table, privilege)
                    ).fetchone()
                    assert has == (privilege in table_level), f"{role} {privilege} on {table}"
                for column in columns:
                    (has,) = conn.execute(
                        "SELECT has_column_privilege(%s, %s, %s, 'UPDATE')",
                        (role, table, column),
                    ).fetchone()
                    expected = "UPDATE" in table_level or column in column_update
                    assert has == expected, f"{role} UPDATE on {table}.{column}"


# --- acceptance: denied where it matters ------------------------------------


@pytest.mark.parametrize("service", ["bot", "mcp", "research"])
def test_only_ingest_reads_schwab_tokens(roles, service):
    with roles.connect(service) as conn:
        _denied(conn, "SELECT * FROM schwab_tokens")
        _denied(conn, "SELECT token_data FROM schwab_tokens")
        _denied(conn, "INSERT INTO schwab_tokens (id, token_data) VALUES (1, '{}')")


def test_bot_reads_token_age_but_never_the_token(roles):
    with roles.admin() as conn:
        conn.execute(
            "INSERT INTO schwab_tokens (id, token_data) VALUES"
            " (1, '{\"creation_timestamp\": 1760443200, \"token\": {\"refresh_token\": \"secret\"}}')"
        )
    with roles.connect("bot") as conn:
        cursor = conn.execute("SELECT * FROM schwab_token_issue")
        assert [c.name for c in cursor.description] == ["issued_at"]
        assert cursor.fetchone() == (datetime(2025, 10, 14, 12, 0, tzinfo=UTC),)
        _denied(conn, "SELECT * FROM schwab_tokens")
        _denied(conn, "SELECT token_data FROM schwab_tokens")
        (can_write,) = conn.execute(
            "SELECT has_table_privilege(current_user, 'schwab_token_issue', 'INSERT, UPDATE, DELETE')"
        ).fetchone()
        assert can_write is False


@pytest.mark.parametrize("service", ["ingest", "mcp", "research"])
def test_only_the_bot_reads_token_age(roles, service):
    with roles.connect(service) as conn:
        _denied(conn, "SELECT * FROM schwab_token_issue")


def test_bot_records_notices_but_cannot_rewrite_them(roles):
    with roles.connect("bot") as conn:
        conn.execute("INSERT INTO schwab_token_notice (issued_at, threshold_hours) VALUES (now(), 24)")
        conn.execute("SELECT * FROM schwab_token_notice")
        _denied(conn, "UPDATE schwab_token_notice SET threshold_hours = 6")
        _denied(conn, "DELETE FROM schwab_token_notice")


def test_ingest_reads_and_writes_schwab_tokens(roles):
    with roles.connect("ingest") as conn:
        conn.execute("INSERT INTO schwab_tokens (id, token_data) VALUES (1, '{}')")
        assert conn.execute("SELECT token_data FROM schwab_tokens").fetchone() == ({},)


def test_ingest_rewrites_outcomes_and_track_records_and_the_analysts_only_read_them(roles):
    outcome = (
        "INSERT INTO outcome (trigger, rule_version, symbol, day, horizon, case_at, hit_at, live,"
        " entry_session, exit_session, stock_return, benchmark_return, excess_return)"
        " VALUES ('t', '1', 'A', '2026-07-06', 5, now(), now(), true, '2026-07-07', '2026-07-13', 0, 0, 0)"
    )
    record = (
        "INSERT INTO track_record (trigger, rule_version, horizon, computed_at, cases, live_cases,"
        " outcomes, unpriced, split_excluded, no_move_size) VALUES ('t', '1', 5, now(), 0, 0, 0, 0, 0, 0)"
    )
    with roles.connect("ingest") as conn:
        conn.execute(outcome)
        conn.execute(record)
        conn.execute("UPDATE track_record SET outcomes = 1")
        conn.execute("DELETE FROM outcome")
        _denied(conn, "UPDATE outcome SET live = false")
        _denied(conn, "DELETE FROM track_record")
        conn.execute(outcome)
    for service in ("mcp", "research"):
        with roles.connect(service) as conn:
            conn.execute("SELECT * FROM outcome")
            conn.execute("SELECT * FROM track_record")
            _denied(conn, outcome)
            _denied(conn, record)
            _denied(conn, "UPDATE track_record SET outcomes = 2")
    with roles.connect("bot") as conn:
        conn.execute("SELECT * FROM track_record")
        _denied(conn, "SELECT * FROM outcome")
        _denied(conn, record)


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO track_record (trigger, rule_version, horizon, computed_at, cases, live_cases,"
        " outcomes, unpriced, split_excluded, no_move_size) VALUES ('t', '1', 5, now(), 0, 0, 0, 0, 0, 0)",
        "INSERT INTO alerts (symbol, trigger, triggered_at, payload)"
        " VALUES ('A', 't', now(), '{}')",
        "DELETE FROM alerts",
        "TRUNCATE alerts",
        "SELECT * FROM schwab_auth_request",
        "CREATE TABLE intruder (id int)",
    ],
)
def test_mcp_is_read_only_and_cannot_see_secrets(roles, statement):
    with roles.connect("mcp") as conn:
        _denied(conn, statement)


def test_mcp_reads_the_market_data_it_exposes(roles):
    with roles.connect("mcp") as conn:
        for table in ("alerts", "outcome", "track_record", "daily_bar", "popularity_snapshot"):
            conn.execute(f"SELECT * FROM {table}")


def test_bot_may_only_touch_delivery_columns_of_alerts(roles):
    with roles.admin() as conn:
        conn.execute(
            "INSERT INTO alerts (symbol, trigger, triggered_at, payload)"
            " VALUES ('A', 't', now(), '{}')"
        )
    with roles.connect("bot") as conn:
        conn.execute(
            "UPDATE alerts SET delivery = 'posted', sending_at = now(), posted_at = now(),"
            " message_id = 5, thread_id = 6, post_attempts = 1"
        )
        _denied(conn, "UPDATE alerts SET payload = '{\"x\": 1}'")
        _denied(conn, "UPDATE alerts SET symbol = 'B'")
        _denied(conn, "INSERT INTO alerts (symbol, trigger, triggered_at, payload) "
                "VALUES ('A', 't', now(), '{}')")
        _denied(conn, "DELETE FROM alerts")


def test_ingest_decides_candidates_once_and_the_analysts_read_them(roles):
    insert = (
        "INSERT INTO candidate (trigger, rule_version, symbol, hit_at, inputs, verdict, decided_at)"
        " VALUES ('t', '1', 'A', now(), '{}', 'repeat', now())"
    )
    with roles.connect("ingest") as conn:
        conn.execute(insert)
        conn.execute("SELECT * FROM candidate")
        _denied(conn, "UPDATE candidate SET verdict = 'posted'")
        _denied(conn, "DELETE FROM candidate")
    for service in ("mcp", "research"):
        with roles.connect(service) as conn:
            conn.execute("SELECT * FROM candidate")
            _denied(conn, insert)


def test_bot_may_insert_auth_requests_and_set_the_callback_only(roles):
    with roles.connect("bot") as conn:
        conn.execute("INSERT INTO schwab_auth_request (requested_by) VALUES (7)")
        conn.execute("UPDATE schwab_auth_request SET state = 'callback_received', callback_url = 'u'")
        _denied(conn, "UPDATE schwab_auth_request SET auth_url = 'https://evil'")
        _denied(conn, "UPDATE schwab_auth_request SET requested_by = 8")
        _denied(conn, "DELETE FROM schwab_auth_request")


def test_no_role_can_create_objects_or_is_privileged(roles):
    with roles.admin() as admin:
        for service in SERVICES:
            row = admin.execute(
                "SELECT rolsuper, rolcreatedb, rolcreaterole, rolcanlogin FROM pg_roles"
                " WHERE rolname = %s",
                (roles.name(service),),
            ).fetchone()
            assert row == (False, False, False, True)
    for service in SERVICES:
        with roles.connect(service) as conn:
            _denied(conn, "CREATE TABLE intruder (id int)")


# --- the roles work for the real repositories -------------------------------


def _factory(roles: Roles, service: str):
    return build_session_factory(build_engine(roles.url(service)))


async def test_services_run_their_real_repositories_under_their_roles(roles):
    ingest, bot, mcp, research = (_factory(roles, s) for s in SERVICES)
    alert = Alert(symbol="AAA", trigger="surge", triggered_at=NOW)
    snapshot = PopularitySnapshot(
        polled_at=NOW,
        ticker="AAA",
        rank=3,
        mentions=120,
        upvotes=8,
        fetched_at=NOW,
        origin=PopularityOrigin.LIVE,
    )

    # ingest: poll write (creates a fresh hypertable chunk), alert, token.
    async with ingest() as s:
        assert await PopularityRepository(s).insert_many([snapshot]) == 1
        assert await AlertRepository(s).insert(alert) is not None
        await s.commit()
    await SchwabTokenStore(ingest).save({"refresh_token": "r"})
    assert await SchwabTokenStore(ingest).load() == {"refresh_token": "r"}

    # bot: token-age notice bookkeeping.
    async with bot() as s:
        await TokenNoticeRepository(s).record(NOW, 24)
        await s.commit()
    async with bot() as s:
        assert await TokenNoticeRepository(s).sent_thresholds(NOW) == {24}

    # bot: sweep, retry bookkeeping, mark posted.
    async with bot() as s:
        pending = await AlertRepository(s).list_undelivered()
        assert [p.alert.symbol for p in pending] == ["AAA"]
        assert await AlertRepository(s).increment_post_attempts(pending[0].id) == 1
        assert await AlertRepository(s).mark_posted(pending[0].id, 555) is not None
        await s.commit()

    # the re-auth handshake crosses bot and ingest.
    async with bot() as s:
        request = await AuthRequestRepository(s).create(requested_by=42)
        await s.commit()
    async with ingest() as s:
        assert await AuthRequestRepository(s).set_auth_url(request.id, "https://auth")
        await s.commit()
    async with bot() as s:
        assert await AuthRequestRepository(s).set_callback_url(request.id, "https://cb?code=x")
        await s.commit()
    async with ingest() as s:
        repo = AuthRequestRepository(s)
        assert [r.id for r in await repo.list_in_state(AuthRequestState.CALLBACK_RECEIVED)]
        assert await repo.complete(request.id)
        assert await repo.expire_older_than(NOW + timedelta(days=1)) == 0
        await s.commit()

    # ingest rewrites a Trigger version's Outcomes and Track record; the others read them.
    async with ingest() as s:
        repo = TrackRecordRepository(s)
        await repo.replace_outcomes("surge", "1", [])
        await repo.upsert(
            [TrackRecord(trigger="surge", rule_version="1", horizon=5, computed_at=NOW, cases=0, live_cases=0, outcomes=0)]
        )
        await s.commit()
    async with research() as s:
        assert (await s.scalars(select(TrackRecordRow.horizon))).all() == [5]
    async with mcp() as s:
        got = await PopularityRepository(s).list_between(NOW, NOW + timedelta(minutes=30))
        assert [g.ticker for g in got] == ["AAA"]


def test_listen_notify_reaches_the_bot_role(roles):
    with roles.connect("bot") as listener, roles.connect("ingest") as writer:
        listener.execute(f"LISTEN {NOTIFY_CHANNEL}")
        writer.execute("SELECT pg_notify(%s, '1')", (NOTIFY_CHANNEL,))
        notices = list(listener.notifies(timeout=5, stop_after=1))
    assert [n.payload for n in notices] == ["1"]


# --- idempotence -------------------------------------------------------------


def _acl_snapshot(roles: Roles) -> set[tuple[str, str, str]]:
    with roles.admin() as conn:
        table_acls = conn.execute(
            "SELECT c.relname, '', unnest(c.relacl)::text FROM pg_class c"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')"
        ).fetchall()
        column_acls = conn.execute(
            "SELECT c.relname, a.attname, unnest(a.attacl)::text FROM pg_attribute a"
            " JOIN pg_class c ON c.oid = a.attrelid"
            " JOIN pg_namespace n ON n.oid = c.relnamespace"
            " WHERE n.nspname = 'public' AND a.attacl IS NOT NULL"
        ).fetchall()
    return {(t, c, acl) for t, c, acl in table_acls + column_acls if roles.prefix in acl}


def test_applying_twice_is_a_no_op(roles):
    before = _acl_snapshot(roles)
    assert before
    roles.apply()
    assert _acl_snapshot(roles) == before
    for service in SERVICES:  # the same passwords still log in
        with roles.connect(service) as conn:
            conn.execute("SELECT 1")


def test_a_changed_password_takes_effect_and_stale_grants_are_removed(roles):
    with roles.admin() as conn:
        conn.execute(f'GRANT SELECT ON schwab_tokens TO "{roles.name("mcp")}"')
    roles.passwords["mcp"] = f"pw-mcp-{uuid.uuid4().hex}"
    roles.apply()
    with roles.connect("mcp") as conn:
        _denied(conn, "SELECT * FROM schwab_tokens")


# --- the CLI -----------------------------------------------------------------


def _all_passwords() -> dict[str, str]:
    return {password_var(s): f"pw-{s}-{'0' * 16}" for s in SERVICES}


def test_read_passwords_names_every_missing_variable():
    environ = _all_passwords()
    del environ["RS_DB_PASSWORD_BOT"]
    del environ["RS_DB_PASSWORD_MCP"]
    with pytest.raises(RolesConfigError, match="RS_DB_PASSWORD_BOT, RS_DB_PASSWORD_MCP"):
        read_passwords(environ)


@pytest.mark.parametrize("bad", ["short", "has@sign-and-is-long-enough", "a b c d e f g h i j k l"])
def test_read_passwords_rejects_what_would_break_a_dsn(bad):
    environ = _all_passwords() | {"RS_DB_PASSWORD_INGEST": bad}
    with pytest.raises(RolesConfigError, match="RS_DB_PASSWORD_INGEST"):
        read_passwords(environ)


def test_roles_command_requires_the_passwords(monkeypatch, capsys, migrated_db_url):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    for service in SERVICES:
        monkeypatch.delenv(password_var(service), raising=False)
    assert main(["roles"]) == 2
    assert "RS_DB_PASSWORD_INGEST" in capsys.readouterr().err


def test_upgrade_fails_without_passwords_unless_roles_are_skipped(
    monkeypatch, capsys, migrated_db_url
):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    for service in SERVICES:
        monkeypatch.delenv(password_var(service), raising=False)
    assert main(["upgrade"]) == 2
    assert "RS_DB_PASSWORD_INGEST" in capsys.readouterr().err
    assert main(["upgrade", "--skip-roles"]) == 0
    assert main(["upgrade", "0003"]) == 0  # not head: no roles step either
    assert main(["upgrade"]) == 2  # re-upgrade to head still demands the passwords


def test_upgrade_fails_on_a_partial_password_set(monkeypatch, capsys, migrated_db_url):
    monkeypatch.setenv("RS_DB_URL", migrated_db_url)
    for service in SERVICES:
        monkeypatch.delenv(password_var(service), raising=False)
    monkeypatch.setenv("RS_DB_PASSWORD_INGEST", "x" * 20)
    assert main(["upgrade"]) == 2
    assert "RS_DB_PASSWORD_BOT" in capsys.readouterr().err
