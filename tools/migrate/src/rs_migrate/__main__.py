"""Operational migration CLI — a thin wrapper over Alembic against
rs_persistence's in-package migrations. DSN comes from RS_DB_URL.

    python -m rs_migrate upgrade [revision=head]
    python -m rs_migrate downgrade <revision>
    python -m rs_migrate current
"""

import argparse
import os
import sys

from alembic import command

from rs_observability import configure_logging
from rs_persistence import migration_config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rs_migrate", description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)

    upgrade = sub.add_parser("upgrade", help="migrate up (default: head)")
    upgrade.add_argument("revision", nargs="?", default="head")
    downgrade = sub.add_parser("downgrade", help="migrate down to a revision")
    downgrade.add_argument("revision")
    sub.add_parser("current", help="show current revision")

    args = parser.parse_args(argv)

    # S006: alembic logs via stdlib logging — route it through the one JSON
    # stream so the compose migrate service's records carry service/timestamp.
    configure_logging("migrate")

    db_url = os.environ.get("RS_DB_URL")
    if not db_url:
        print("RS_DB_URL is not set", file=sys.stderr)
        return 2

    config = migration_config(db_url)
    if args.action == "upgrade":
        command.upgrade(config, args.revision)
    elif args.action == "downgrade":
        command.downgrade(config, args.revision)
    elif args.action == "current":
        command.current(config, verbose=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
