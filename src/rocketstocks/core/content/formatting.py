"""Standalone formatting utilities for building Discord message content.

These were previously locked behind inheritance from Report. They are now
plain functions usable by any content class without inheriting anything.
"""
import logging

import pandas as pd
from table2ascii import table2ascii, PresetStyle

from rocketstocks.core.config.paths import validate_path, datapaths
from rocketstocks.core.utils.formatting import format_large_num  # re-exported for convenience

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ASCII table style map (shared constant)
# ---------------------------------------------------------------------------

TABLE_STYLES: dict[str, PresetStyle] = {
    'ascii': PresetStyle.ascii,
    'asci_borderless': PresetStyle.ascii_borderless,
    'ascii_box': PresetStyle.ascii_box,
    'ascii_compact': PresetStyle.ascii_compact,
    'ascii_double': PresetStyle.ascii_double,
    'ascii_minimalist': PresetStyle.ascii_minimalist,
    'ascii_rounded': PresetStyle.ascii_rounded,
    'ascii_rounded_box': PresetStyle.ascii_rounded_box,
    'ascii_simple': PresetStyle.ascii_simple,
    'borderless': PresetStyle.borderless,
    'double': PresetStyle.double_box,
    'double_box': PresetStyle.double_box,
    'double_compact': PresetStyle.double_compact,
    'double_thin_box': PresetStyle.double_thin_box,
    'double_thin_compact': PresetStyle.double_thin_compact,
    'markdown': PresetStyle.markdown,
    'minimalist': PresetStyle.minimalist,
    'plain': PresetStyle.plain,
    'simple': PresetStyle.simple,
    'thick': PresetStyle.thick,
    'thick_box': PresetStyle.thick_box,
    'thick_compact': PresetStyle.thick_compact,
    'thin': PresetStyle.thin,
    'thin_box': PresetStyle.thin_box,
    'thin_compact': PresetStyle.thin_compact,
    'thin_compact_rounded': PresetStyle.thin_compact_rounded,
    'thin_double': PresetStyle.thin_double,
    'thin_double_rounded': PresetStyle.thin_double_rounded,
    'thin_rounded': PresetStyle.thin_rounded,
    'thin_thick': PresetStyle.thin_thick,
    'thin_thick_rounded': PresetStyle.thin_thick_rounded,
}


def build_df_table(df: pd.DataFrame, style: str = 'thick_compact') -> str:
    """Return DataFrame formatted as an ASCII table for Discord code blocks."""
    logger.debug(f"Building table of shape {df.shape} with headers {df.columns.to_list()} and style '{style}'")
    table_style = TABLE_STYLES.get(style, PresetStyle.double_thin_compact)
    table = table2ascii(
        header=df.columns.tolist(),
        body=df.values.tolist(),
        style=table_style,
    )
    return "```\n" + table + "\n```"


def build_stats_table(header: dict, body: dict, adjust: str) -> str:
    """Return a two-column aligned key/value table for Discord code blocks."""
    adjust = 'left' if adjust != 'right' else adjust
    spacing = max([len(key) for key in set().union(header, body)]) + 1

    table = ''

    for key, value in header.items():
        if value:
            table += f"{f'{key}:':>{spacing}} {value}\n" if adjust == 'right' else f"{f'{key}:':<{spacing}} {value}\n"
        else:
            table += f"{key}\n"

    table += "━" * 16 + '\n' if header else ''

    for key, value in body.items():
        table += f"{f'{key}:':>{spacing}} {value}\n" if adjust == 'right' else f"{f'{key}:':<{spacing}} {value}\n"

    return '```' + table + '```\n'


def write_df_to_file(df: pd.DataFrame, filepath: str) -> None:
    """Write a DataFrame to CSV at filepath, ensuring the parent directory exists."""
    validate_path(datapaths.attachments_path)
    df.to_csv(filepath, index=False)
