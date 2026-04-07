"""Shared terminal table formatting utilities for the EDA CLI."""
import math
from dataclasses import dataclass


def fmt_pct(value: float, decimals: int = 2) -> str:
    """Format a float as a percentage string with sign, or 'n/a'."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 'n/a'
    sign = '+' if value >= 0 else ''
    return f"{sign}{value:.{decimals}f}%"


def fmt_float(value: float, decimals: int = 3) -> str:
    """Format a float with fixed decimals, or 'n/a'."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 'n/a'
    return f"{value:.{decimals}f}"


def fmt_pvalue(p: float) -> str:
    """Format a p-value, or 'n/a'."""
    if p is None or (isinstance(p, float) and math.isnan(p)):
        return 'n/a'
    if p < 0.001:
        return '<0.001'
    return f"{p:.3f}"


def significant_marker(p: float | None, threshold: float = 0.05) -> str:
    """Return '*' if significant, else ''."""
    if p is None or (isinstance(p, float) and math.isnan(p)):
        return ''
    return '*' if p < threshold else ''


def print_separator(width: int = 70, char: str = '━') -> None:
    print(char * width)


def print_table(
    headers: list[str],
    rows: list[list[str]],
    title: str = '',
    col_widths: list[int] | None = None,
) -> None:
    """Print a simple fixed-width text table to stdout.

    Args:
        headers: Column header strings.
        rows: List of rows, each a list of string values (same length as headers).
        title: Optional title printed above the separator.
        col_widths: Optional fixed column widths; auto-computed if None.
    """
    if col_widths is None:
        col_widths = [
            max(len(h), max((len(str(r[i])) for r in rows), default=0))
            for i, h in enumerate(headers)
        ]

    total_width = sum(col_widths) + len(col_widths) * 2 + 1
    total_width = max(total_width, 70)

    if title:
        print(f"\n{title}")
        print('━' * total_width)

    header_line = '  '.join(h.ljust(w) for h, w in zip(headers, col_widths))
    print(header_line)
    print('─' * total_width)

    for row in rows:
        line = '  '.join(str(v).ljust(w) for v, w in zip(row, col_widths))
        print(line)


def print_kv(label: str, value: str, width: int = 28) -> None:
    """Print a key-value line."""
    print(f"  {label:<{width}}{value}")
