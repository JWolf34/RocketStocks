"""The Cboe directory reader and the seeded sample."""

import pytest

from rs_ingest.tools.sweep_probe.symbols import (
    Listing,
    chain_spellings,
    draw_sample,
    is_index_product,
    parse_directory,
)

# Header cells have leading spaces and fields are quoted, as in the real file.
DIRECTORY = """Company Name, Stock Symbol, DPM Name, Post/Station, Global Trading Hours DPM
"APPLE INC COM","AAPL","Citadel Securities LLC","7/1","-"
"BERKSHIRE HATHAWAY INC DEL CL B NEW","BRK.B","Susquehanna Securities, LLC","8/1","-"
"SPX","SPX","","10/1","-"
"CBOE Volatility Index","VIX","","9/1","-"
"NASDAQ 100 Index","NDX","","1/1","-"
"Russell 2000 Index","RUT","Susquehanna Securities, LLC","7/1","-"
"INVESCO DB US DLR INDEX TR BULLISH FD","UUP","Citadel Securities LLC","7/1","-"
"ISHARES TR RUSSELL 2000 ETF","IWM","Citadel Securities LLC","1/1","-"
"APPLE INC COM","AAPL","Wolverine Trading, LLC","7/1","-"
"""


def test_the_reader_keeps_equities_and_funds_and_drops_index_products():
    listings = parse_directory(DIRECTORY)

    assert [listing.symbol for listing in listings] == ["AAPL", "BRK.B", "IWM", "UUP"]
    assert listings[1] == Listing("BRK.B", "BERKSHIRE HATHAWAY INC DEL CL B NEW")


def test_an_unexpected_header_is_loud():
    with pytest.raises(ValueError, match="unexpected Cboe directory header"):
        parse_directory('"Name","Ticker"\n"x","y"\n')


@pytest.mark.parametrize("symbol", ["SPX", "SPXW", "VIX", "XSP", "NDX", "DJX", "RUT"])
def test_known_index_roots_are_index_products(symbol):
    assert is_index_product(symbol, "whatever the name says")


def test_index_tracking_funds_are_not_index_products():
    assert not is_index_product("UUP", "INVESCO DB US DLR INDEX TR BULLISH FD")
    assert not is_index_product("SCHK", "SCHWAB STRATEGIC TR 1000 INDEX ETF")
    assert is_index_product("SIXM", "Financial Select Sector Index S&P 500")


def test_class_shares_are_asked_for_with_schwabs_slash_spelling_first_then_their_own():
    assert chain_spellings("BRK.B") == ("BRK/B", "BRK.B")
    assert chain_spellings("AAPL") == ("AAPL",)


def test_the_sample_is_a_function_of_the_seed():
    listings = [Listing(f"S{n:03d}", "x") for n in range(200)]

    first = draw_sample(listings, seed=7, size=50, exclude=frozenset())

    assert first == draw_sample(listings, seed=7, size=50, exclude=frozenset())
    assert first != draw_sample(listings, seed=8, size=50, exclude=frozenset())
    assert len(set(first)) == 50


def test_named_symbols_are_not_drawn_again_under_either_spelling():
    listings = [Listing("BRK.B", "x"), Listing("AAPL", "x"), Listing("ZZZ", "x")]

    drawn = draw_sample(listings, seed=1, size=1, exclude=frozenset({"AAPL", "BRK/B"}))

    assert drawn == ["ZZZ"]


def test_asking_for_more_than_exists_is_an_error():
    with pytest.raises(ValueError, match="eligible"):
        draw_sample([Listing("AAA", "x")], seed=1, size=2, exclude=frozenset())
