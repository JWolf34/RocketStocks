"""Schwab's spelling of a Universe symbol, the one place every Schwab request is spelled.

The Universe keeps the listing file's own symbol (`BRK.B`) and everything is stored under it;
only the request to Schwab is spelled this way.
"""


def schwab_symbol(symbol: str) -> str:
    """The spelling Schwab's chain and price history endpoints accept for a Universe symbol.

    Class shares are slashed (`BRK.B` is `BRK/B`): the chain endpoint answers the dotted and
    dot-less spellings with 400, and price history answers the dotted one with no candles.
    """
    return symbol.strip().upper().replace(".", "/")
