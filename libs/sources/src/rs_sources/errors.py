"""Failure type shared by the source adapters."""

# Statuses a source answers for one symbol it will not serve: the source is fine, the stock is not.
PER_SYMBOL_STATUSES = frozenset({400, 404})


class SourceError(RuntimeError):
    """A vendor call failed or returned a payload that does not have the expected shape.

    `status_code` is the HTTP status when the vendor answered with a non-OK one.
    """

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def is_per_symbol_answer(exc: BaseException) -> bool:
    """True when `exc` is the vendor's answer about one symbol (a 400 or 404), not a source failure.

    Anything else (a connection error, a timeout, a 5xx, a 429, a 403, a payload that is
    not what it should be) says the source itself is unwell.
    """
    return isinstance(exc, SourceError) and exc.status_code in PER_SYMBOL_STATUSES
