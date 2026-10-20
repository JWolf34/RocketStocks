"""Failure type shared by the source adapters."""


class SourceError(RuntimeError):
    """A vendor call failed or returned a payload that does not have the expected shape."""
