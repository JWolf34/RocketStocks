"""Symbol directory adapter tests over synthetic files in the vendors' wire format."""

import asyncio
from datetime import UTC, datetime

import httpx
import pytest

from rs_sources.errors import SourceError
from rs_sources.symbol_directory import NASDAQ_EXCHANGE, SymbolDirectory
from rs_sources.testing import (
    CBOE_HEADER,
    NASDAQ_LISTED_HEADER,
    OTHER_LISTED_HEADER,
    FakeDirectoryHost,
    cboe_file,
    footer,
    listing_file,
    nasdaq_row,
    options_file,
    other_row,
)

# 18:01 ET in October (EDT) and in January (EST).
STAMP = datetime(2026, 10, 26, 22, 1, tzinfo=UTC)
WINTER_STAMP = datetime(2027, 1, 12, 23, 1, tzinfo=UTC)


def directory_for(host: FakeDirectoryHost) -> SymbolDirectory:
    return SymbolDirectory(host.client())


async def test_nasdaq_listed_reads_listings_and_drops_test_issues():
    host = FakeDirectoryHost()
    host.bodies["nasdaqlisted.txt"] = listing_file(
        NASDAQ_LISTED_HEADER,
        [nasdaq_row("AAA"), nasdaq_row("ZTEST", test=True), nasdaq_row("QQQX", etf=True), nasdaq_row("BAD", status="D")],
        STAMP,
    )

    result = await directory_for(host).nasdaq_listed()

    assert result.created_at == STAMP
    assert [x.symbol for x in result.listings] == ["AAA", "QQQX", "BAD"]
    aaa, qqqx, bad = result.listings
    assert (aaa.exchange, aaa.etf, aaa.financial_status) == (NASDAQ_EXCHANGE, False, "N")
    assert aaa.security_name == "AAA Inc. - Common Stock"
    assert (aaa.cqs_symbol, aaa.nasdaq_symbol) == (None, None)
    assert qqqx.etf is True
    assert bad.financial_status == "D"


async def test_other_listed_keeps_the_symbol_exchange_code_and_other_symbologies_as_given():
    host = FakeDirectoryHost()
    host.bodies["otherlisted.txt"] = listing_file(
        OTHER_LISTED_HEADER,
        [
            other_row("BRK.B", exchange="N", nasdaq="BRK-B"),
            other_row("ABR$D", exchange="N", cqs="ABRpD", nasdaq="ABR-D"),
            other_row("SPYX", exchange="P", etf=True),
            other_row("ODD", exchange="?"),  # a code the vendor never documented
            other_row("TESTY", exchange="N", test=True),
        ],
        STAMP,
    )

    result = await directory_for(host).other_listed()

    assert [x.symbol for x in result.listings] == ["BRK.B", "ABR$D", "SPYX", "ODD"]
    brk, abr, spyx, odd = result.listings
    assert (brk.exchange, brk.cqs_symbol, brk.nasdaq_symbol) == ("N", "BRK.B", "BRK-B")
    assert (abr.cqs_symbol, abr.nasdaq_symbol) == ("ABRpD", "ABR-D")
    assert (spyx.exchange, spyx.etf) == ("P", True)
    assert odd.exchange == "?"
    assert all(x.financial_status is None for x in result.listings)


@pytest.mark.parametrize("chunk_size", [1, 7, 64, 100_000])
async def test_lines_are_rebuilt_across_chunk_boundaries_including_a_split_crlf(chunk_size):
    host = FakeDirectoryHost(chunk_size=chunk_size)
    host.bodies["nasdaqlisted.txt"] = listing_file(
        NASDAQ_LISTED_HEADER, [nasdaq_row("AAA"), nasdaq_row("BBB")], STAMP
    )

    result = await directory_for(host).nasdaq_listed()

    assert [x.symbol for x in result.listings] == ["AAA", "BBB"]
    assert result.created_at == STAMP


@pytest.mark.parametrize(("stamp", "text"), [(STAMP, "1026202618:01"), (WINTER_STAMP, "0112202718:01")])
async def test_the_footer_is_eastern_time_in_summer_and_winter(stamp, text):
    assert text in footer(stamp)
    host = FakeDirectoryHost()
    host.bodies["otherlisted.txt"] = listing_file(OTHER_LISTED_HEADER, [other_row("AAA")], stamp)

    assert (await directory_for(host).other_listed()).created_at == stamp


async def test_option_underlyings_keeps_only_the_set_of_underlying_symbols():
    host = FakeDirectoryHost(chunk_size=500)
    host.bodies["options.txt"] = options_file(["AAA", "BBB", "BRKB"], STAMP, series=5)

    result = await directory_for(host).option_underlyings()

    assert result.underlyings == frozenset({"AAA", "BBB", "BRKB"})
    assert result.created_at == STAMP


async def test_option_underlyings_yields_to_the_event_loop_while_parsing():
    host = FakeDirectoryHost(chunk_size=4096)
    host.bodies["options.txt"] = options_file([f"S{n}" for n in range(3000)], STAMP, series=3)
    chunks = -(-len(host.bodies["options.txt"]) // 4096)
    ticks = 0
    running = True

    async def heartbeat() -> None:
        nonlocal ticks
        while running:
            ticks += 1
            await asyncio.sleep(0)

    beat = asyncio.ensure_future(heartbeat())
    await directory_for(host).option_underlyings()
    running = False
    await beat

    # Every chunk hands the loop back at least once, so a heartbeat is never starved.
    assert ticks >= chunks


async def test_cboe_symbols_come_from_a_quoted_csv_with_a_padded_header():
    host = FakeDirectoryHost()
    host.bodies["cboe"] = cboe_file(["AAA", "BRK.B", "SPX"])

    assert await directory_for(host).cboe_underlyings() == frozenset({"AAA", "BRK.B", "SPX"})


async def test_cboe_requests_the_pinned_download_url():
    host = FakeDirectoryHost()
    host.bodies["cboe"] = cboe_file(["AAA"])

    await directory_for(host).cboe_underlyings()

    assert host.requests == ["cboe"]


@pytest.mark.parametrize(
    ("name", "call"),
    [
        ("nasdaqlisted.txt", "nasdaq_listed"),
        ("otherlisted.txt", "other_listed"),
        ("options.txt", "option_underlyings"),
        ("cboe", "cboe_underlyings"),
    ],
)
async def test_an_http_error_is_a_source_error_with_its_status(name, call):
    host = FakeDirectoryHost()
    host.statuses[name] = 503

    with pytest.raises(SourceError) as raised:
        await getattr(directory_for(host), call)()

    assert raised.value.status_code == 503


async def test_a_dropped_connection_is_a_source_error():
    def drop(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    directory = SymbolDirectory(httpx.AsyncClient(transport=httpx.MockTransport(drop)))

    with pytest.raises(SourceError):
        await directory.nasdaq_listed()


@pytest.mark.parametrize(
    "body",
    [
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF\r\n",  # a column gone
        OTHER_LISTED_HEADER + "\r\n" + other_row("AAA") + "\r\n" + footer(STAMP) + "\r\n",  # the other file's layout
    ],
)
async def test_a_changed_header_is_a_source_error(body):
    host = FakeDirectoryHost()
    host.bodies["nasdaqlisted.txt"] = body

    with pytest.raises(SourceError, match="header"):
        await directory_for(host).nasdaq_listed()


async def test_a_file_without_its_footer_is_a_source_error_because_it_may_be_cut_short():
    host = FakeDirectoryHost()
    host.bodies["nasdaqlisted.txt"] = "\r\n".join([NASDAQ_LISTED_HEADER, nasdaq_row("AAA")]) + "\r\n"
    host.bodies["options.txt"] = options_file(["AAA"], STAMP).rsplit("File Creation", 1)[0]

    with pytest.raises(SourceError, match="footer"):
        await directory_for(host).nasdaq_listed()
    with pytest.raises(SourceError, match="footer"):
        await directory_for(host).option_underlyings()


async def test_an_unreadable_footer_is_a_source_error():
    host = FakeDirectoryHost()
    host.bodies["nasdaqlisted.txt"] = "\r\n".join(
        [NASDAQ_LISTED_HEADER, nasdaq_row("AAA"), "File Creation Time: 13322026ab:cd||||"]
    )

    with pytest.raises(SourceError, match="footer"):
        await directory_for(host).nasdaq_listed()


async def test_malformed_rows_are_skipped_and_empty_files_rejected():
    host = FakeDirectoryHost()
    host.bodies["nasdaqlisted.txt"] = listing_file(
        NASDAQ_LISTED_HEADER, [nasdaq_row("AAA"), "SHORT|only|three", "|nameless|Q|N|N|100|N|N"], STAMP
    )
    host.bodies["otherlisted.txt"] = listing_file(OTHER_LISTED_HEADER, [], STAMP)
    host.bodies["cboe"] = CBOE_HEADER + "\n"

    assert [x.symbol for x in (await directory_for(host).nasdaq_listed()).listings] == ["AAA"]
    with pytest.raises(SourceError, match="no listings"):
        await directory_for(host).other_listed()
    with pytest.raises(SourceError, match="no rows"):
        await directory_for(host).cboe_underlyings()


async def test_cboe_without_a_symbol_column_is_a_source_error():
    host = FakeDirectoryHost()
    host.bodies["cboe"] = ' Company Name, Ticker\n"A CO","AAA"\n'

    with pytest.raises(SourceError, match="Stock Symbol"):
        await directory_for(host).cboe_underlyings()
