"""daily_bar.adjusted: the bars already stored are split-adjusted.

Schwab's daily history was measured against four known splits on 2026-10-21
(the ratio of the split-day close to the previous session's close): NVDA
10-for-1 on 2024-06-10 gave 1.0075, AVGO 10-for-1 on 2024-07-15 gave 1.0080,
CMG 50-for-1 on 2024-06-26 gave 1.0030 and WMT 3-for-1 on 2024-02-26 gave
1.0185; an unadjusted series would have shown about 0.1, 0.1, 0.02 and 0.33.
Until now the ingest job stored every bar with `adjusted` false because that
was not yet known, so every stored row is flagged true here.
Adjustment holds as of the fetch: a row stored before a later split is stale,
and the daily bars job repairs it by comparing the closes of its overlap
sessions with a fresh fetch the next time it extends that symbol.
The downgrade flags every row false again, as the earlier job stored them.

`daily_bar` compresses chunks older than 180 days, and an UPDATE decompresses the
rows it touches; Timescale aborts a transaction that decompresses more than
`timescaledb.max_tuples_decompressed_per_dml_transaction` (100,000) rows, which years
of bars for thousands of stocks exceed. So each direction lifts that cap for its own
transaction, and afterwards compresses again the chunks that were compressed before
(the UPDATE leaves them partly decompressed), so the table ends as it began.

Revision ID: 0020
Revises: 0019
"""

import sqlalchemy as sa
from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def _flag_bars(adjusted: bool) -> None:
    bind = op.get_bind()
    compressed = (
        bind.execute(
            sa.text(
                "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks"
                " WHERE hypertable_name = 'daily_bar' AND is_compressed ORDER BY range_start"
            )
        )
        .scalars()
        .all()
    )
    op.execute("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
    op.execute(f"UPDATE daily_bar SET adjusted = {'true' if adjusted else 'false'}")
    for chunk in compressed:
        bind.execute(
            sa.text("SELECT compress_chunk(CAST(:chunk AS regclass), if_not_compressed => true)"),
            {"chunk": chunk},
        )


def upgrade() -> None:
    _flag_bars(True)


def downgrade() -> None:
    _flag_bars(False)
