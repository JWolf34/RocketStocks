"""Reading v1 popularity dumps: pg_dump COPY output and CSV with a header."""

from datetime import datetime

import pytest

from rs_backfill.dump import DumpError, V1Row, read_dump

PG_DUMP = """\
--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';

--
-- Data for Name: popularity; Type: TABLE DATA; Schema: public; Owner: rs
--

COPY public.popularity (datetime, rank, ticker, name, mentions, upvotes, rank_24h_ago, mentions_24h_ago) FROM stdin;
2026-10-01 12:00:00\t1\tGME\tGameStop\\tCorp\t120\t800\t4\t40
2026-10-01 12:00:00\t2\tAMC\tAMC\t90\t\\N\t\\N\t\\N
\\.

COPY public.other (id, note) FROM stdin;
1\tnot popularity
\\.

-- PostgreSQL database dump complete
"""

CSV = """\
datetime,rank,ticker,name,mentions,upvotes,rank_24h_ago,mentions_24h_ago
2026-10-01 12:00:00,1,GME,"GameStop, Corp",120,800,4,40
2026-10-01 12:00:00.5,2,AMC,AMC,90,,,
"""

GME = V1Row(datetime(2026, 10, 1, 12), 1, "GME", 120, 800, 4, 40)
AMC_CSV = V1Row(datetime(2026, 10, 1, 12, 0, 0, 500000), 2, "AMC", 90, None, None, None)
AMC_COPY = V1Row(datetime(2026, 10, 1, 12), 2, "AMC", 90, None, None, None)


def _write(tmp_path, text, name="dump"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_reads_a_pg_dump_copy_block_and_ignores_other_tables(tmp_path):
    assert list(read_dump(_write(tmp_path, PG_DUMP))) == [GME, AMC_COPY]


def test_reads_csv_with_header_and_blank_as_null(tmp_path):
    assert list(read_dump(_write(tmp_path, CSV, "dump.csv"))) == [GME, AMC_CSV]


def test_format_can_be_forced(tmp_path):
    assert list(read_dump(_write(tmp_path, CSV), "csv")) == [GME, AMC_CSV]
    with pytest.raises(DumpError):
        list(read_dump(_write(tmp_path, CSV), "copy"))


def test_copy_columns_follow_the_dump_header_order(tmp_path):
    text = (
        "COPY public.popularity (ticker, datetime, rank, mentions, upvotes,"
        " rank_24h_ago, mentions_24h_ago) FROM stdin;\n"
        "GME\t2026-10-01 12:00:00\t1\t120\t800\t4\t40\n\\.\n"
    )
    assert list(read_dump(_write(tmp_path, text))) == [GME]


@pytest.mark.parametrize(
    "text, message",
    [
        ("-- only comments\n", "no COPY block"),
        ("", "empty"),
        ("COPY public.popularity (datetime, rank) FROM stdin;\n\\.\n", "lacks columns"),
        ("datetime,rank,ticker\n", "lacks columns"),
    ],
)
def test_unusable_dumps_fail_loudly(tmp_path, text, message):
    with pytest.raises(DumpError, match=message):
        list(read_dump(_write(tmp_path, text)))


def test_a_short_row_fails_with_its_line_number(tmp_path):
    lines = PG_DUMP.splitlines()
    lines[lines.index("\\.") - 1] = "2026-10-01 12:00:00\t2\tAMC"
    with pytest.raises(DumpError, match=r"line \d+: wrong number of fields"):
        list(read_dump(_write(tmp_path, "\n".join(lines) + "\n")))


def test_a_timestamp_with_a_zone_is_refused(tmp_path):
    text = CSV.replace("2026-10-01 12:00:00,1", "2026-10-01 12:00:00+00:00,1")
    with pytest.raises(DumpError, match="carries a zone"):
        list(read_dump(_write(tmp_path, text, "dump.csv")))
