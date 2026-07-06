"""Operational migration CLI — a thin wrapper over Alembic against
rs_persistence's config. DSN comes from RS_DB_URL.

    python -m rs_migrate upgrade [revision=head]
    python -m rs_migrate downgrade <revision>
    python -m rs_migrate current
"""

import argparse
import os
import sys

from alembic import command
from alembic.config import Config

from rs_persistence import alembic_config_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rs_migrate", description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)

    upgrade = sub.add_parser("upgrade", help="migrate up (default: head)")
    upgrade.add_argument("revision", nargs="?", default="head")
    downgrade = sub.add_parser("downgrade", help="migrate down to a revision")
    downgrade.add_argument("revision")
    sub.add_parser("current", help="show current revision")

    args = parser.parse_args(argv)

    if not os.environ.get("RS_DB_URL"):
        print("RS_DB_URL is not set", file=sys.stderr)
        return 2

    config = Config(str(alembic_config_path()))
    if args.action == "upgrade":
        command.upgrade(config, args.revision)
    elif args.action == "downgrade":
        command.downgrade(config, args.revision)
    elif args.action == "current":
        command.current(config, verbose=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
