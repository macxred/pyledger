"""Utilities for generating and formatting tables in the Typst typesetting engine."""

from pathlib import Path
import pandas as pd
import typst
from consistent_df import enforce_schema


def validate_style_matrix(style_matrix: pd.DataFrame, data_shape: tuple) -> dict:
    """
    Validate and convert style matrix to lookup dict.

    Args:
        style_matrix: DataFrame with columns [row, col, style]
        data_shape: (n_rows, n_cols) of the data DataFrame

    Returns:
        dict: {(row, col): style_dict} for fast lookup

    Raises:
        ValueError: If schema invalid or indices out of bounds
    """
    from pyledger.constants import STYLE_MATRIX_SCHEMA

    # Enforce schema
    style_matrix = enforce_schema(style_matrix, STYLE_MATRIX_SCHEMA)

    # Validate row/col indices
    # Note: row indices include header (row 0), so max valid row is n_rows (last data row)
    n_rows, n_cols = data_shape
    invalid_rows = style_matrix[style_matrix['row'] > n_rows]
    if not invalid_rows.empty:
        raise ValueError(
            f"Invalid row indices: {invalid_rows['row'].tolist()}, "
            f"max allowed: {n_rows} (including header at row 0)"
        )

    invalid_cols = style_matrix[style_matrix['col'] >= n_cols]
    if not invalid_cols.empty:
        raise ValueError(
            f"Invalid col indices: {invalid_cols['col'].tolist()}, max allowed: {n_cols - 1}"
        )

    # Validate style structure
    for idx, style in style_matrix['style'].items():
        if not isinstance(style, dict):
            raise ValueError(f"Row {idx}: style must be dict, got {type(style).__name__}")
        if not any(k in style for k in ['text', 'cell']):
            raise ValueError(f"Row {idx}: style must have 'text' and/or 'cell' keys")

    # Convert to dict for fast lookup
    return style_matrix.set_index(['row', 'col'])['style'].to_dict()


def df_to_typst(
    df: pd.DataFrame,
    align: list[str] = None,
    columns: list[str] = None,
    na_value: str = "",
    hline: list[int] = [],
    bold: list[int] = [],
    style_matrix: pd.DataFrame = None,
    colnames: bool = True,
    repeat_colnames: bool = True,
) -> str:
    """
    Convert a pandas DataFrame to a Typst-formatted table string.

    Produces a `table(...)` block for Typst with limited styling options:
    column alignment, column width, horizontal lines, and bold rows.

    Intended as a temporary utility until `.to_typst()` becomes available in pandas.

    Args:
        df (pd.DataFrame): The DataFrame to convert.
        align (list[str], optional): Alignment specifiers for columns ("left", "center", "right").
            Defaults to Typst's automatic alignment.
        columns (list[str], optional): Column width specifiers (e.g., "auto", "1fr", "2cm").
            If None, the number of columns is inferred from the DataFrame.
        na_value (str): Replacement string for NA/NaN values. Defaults to an empty string.
        hline (list[int], optional): Row indices after which horizontal lines are inserted.
            Indices are 0-based and include the header if `colnames=True`.
            Use -1 for a line above the header.
        bold (list[int], optional): Row indices to render in bold.
            Indices are 0-based and include the header if `colnames=True`.
        style_matrix (pd.DataFrame, optional): DataFrame with columns [row, col, style]
            for per-cell styling. Each row defines styling for one cell via (row, col) position.
            The style column contains a dict with 'text' and/or 'cell' keys mapping to
            Typst's text() and table.cell() properties.
        colnames (bool): Whether to include column names as the first row. Defaults to True.
        repeat_colnames (bool): Whether the header repeats on each page if the table spans multiple
            pages. Defaults to True, matching Typst's `table.header(repeat: true)` default.

    Returns:
        str: A Typst-compatible table string.

    Raises:
        ValueError: If `align` or `columns` is provided and its length does not match
        the number of DataFrame columns, or if style_matrix validation fails.
    """
    num_columns = len(df.columns)
    if align is not None and len(align) != num_columns:
        raise ValueError(f"`align` has {len(align)} elements but expected {num_columns}.")

    if columns is not None and len(columns) != num_columns:
        raise ValueError(f"`columns` has {len(columns)} elements but expected {num_columns}.")

    # Validate and convert style_matrix to lookup dict
    style_dict = validate_style_matrix(style_matrix, df.shape) if style_matrix is not None else {}

    result = ["table(", "  stroke: none,"]

    # Add layout specifiers
    if columns is None:
        result.append(f"  columns: {len(df.columns)},")
    else:
        result.append(f"  columns: ({_typst_attribute(columns)}),")
    if align is not None:
        result.append(f"  align: ({_typst_attribute(align)}),")

    row_idx = 0
    if -1 in hline:
        result.append("  table.hline(),")
    # Header
    if colnames:
        result.extend(_typst_header_row(
            df.columns, na_value=na_value, bold=(row_idx in bold), hline=(row_idx in hline),
            repeat=repeat_colnames,
        ))
        row_idx += 1
    # Data rows
    for _, row in df.iterrows():
        result.extend(_typst_row(
            row, na_value=na_value, bold=(row_idx in bold), hline=(row_idx in hline),
            row_idx=row_idx, style_dict=style_dict
        ))
        row_idx += 1
    result.append(")")
    return "\n".join(result) + "\n"


def _typst_attribute(x: list) -> str:
    """Convert list of column attributes into Typst-compatible comma-separated format."""
    return ", ".join(map(str, x))


def _typst_row(
    row: list, na_value: str, bold: bool, hline: bool, row_idx: int = None, style_dict: dict = None
) -> list[str]:
    """
    Convert a data frame row to a Typst-formatted table row.
    Optionally adding a horizontal line at the bottom.

    Args:
        row: Row data to convert
        na_value: Replacement for NA/NaN values
        bold: Whether to render entire row in bold
        hline: Whether to add horizontal line after row
        row_idx: Row index for looking up cell styles
        style_dict: Dict mapping (row, col) to style dicts
    """
    cells = [na_value if pd.isna(cell) else cell for cell in list(row)]

    # Apply per-cell styling if style_dict provided
    if style_dict and row_idx is not None:
        cell_strs = []
        for col_idx, cell in enumerate(cells):
            style = style_dict.get((row_idx, col_idx))
            if style:
                # Build cell content
                # Start with just the cell value, we'll wrap it as needed
                cell_content = cell

                # Apply text properties if present
                if 'text' in style:
                    text_props = ", ".join(f'{k}: {v}' for k, v in style['text'].items())
                    cell_content = f"#text({text_props})[{cell_content}]"

                # Wrap with table.cell if cell properties present
                if 'cell' in style:
                    def format_prop(key, val):
                        if isinstance(val, dict):
                            nested = ", ".join(f"{k}: {v}" for k, v in val.items())
                            return f"{key}: ({nested})"
                        return f"{key}: {val}"

                    cell_props = ", ".join(format_prop(k, v) for k, v in style['cell'].items())
                    cell_content = f"table.cell({cell_props})[{cell_content}]"
                else:
                    # No cell styling, just wrap in brackets
                    cell_content = f"[{cell_content}]"

                cell_strs.append(cell_content + ",")
            elif bold:
                # No specific style, but row is bold
                cell_strs.append(f'text(weight: "bold", [{cell}]),')
            else:
                # No styling
                cell_strs.append(f"[{cell}],")
        row = ["  " + " ".join(cell_strs)]
    elif bold:
        # Entire row bold (no per-cell styling)
        row = ["  " + " ".join(f'text(weight: "bold", [{cell}]),' for cell in cells)]
    else:
        # No styling
        row = ["  " + " ".join(f"[{cell}]," for cell in cells)]

    if hline:
        row.append("  table.hline(),")
    return row


def _typst_header_row(row: list, repeat: bool, hline: bool, **kwargs) -> list[str]:
    """
    Wrap a header row inside a Typst table.header(...) block.
    Reuses _typst_row for cell rendering. Optionally adds a horizontal line after the header.
    """
    inner = "  " + _typst_row(row, hline=False, **kwargs)[0]
    row = [f"  table.header(repeat: {'true' if repeat else 'false'},", inner, "  ),"]
    if hline:
        row.append("  table.hline(),")
    return row


def escape_typst_text(series: pd.Series) -> pd.Series:
    """Escape Typst-sensitive characters in content blocks.

    Escapes special characters that have meaning in Typst markup:
    - backslash (escape character itself - must be escaped first)
    - $ (math mode delimiter)
    - # (code mode prefix)
    - * (strong emphasis)
    - @ (reference)
    - < and > (label delimiters)

    Args:
        series: A pandas Series containing text values to escape.

    Returns:
        A pandas Series with Typst special characters escaped.
    """
    return series.map(
        lambda text: (
            text.replace("\\", "\\\\")  # Must be first to avoid double-escaping
                .replace("$", "\\$")
                .replace("#", "\\#")
                .replace("*", "\\*")
                .replace("@", "\\@")
                .replace("<", "\\<")
                .replace(">", "\\>")
        ) if isinstance(text, str) else text
    )


def render_typst(content: str, output_path: str | Path, keep_temp: bool = False):
    """Generate a PDF from a Typst-formatted string, using a temporary .typ file.

    Converts a Typst-formatted string into a PDF by writing it to a temporary `.typ` file,
    running the Typst compiler, and saving the result to the specified output path.
    Optionally retains the intermediate `.typ` file for inspection or debugging.

    Args:
        content (str): The Typst markup to render.
        output_path (str | Path): The destination path for the generated PDF file.
        keep_temp (bool, optional): If True, the intermediate `.typ` file is preserved.
    """
    output_path = Path(output_path)
    typst_path = output_path.with_suffix(".typ")
    typst_path.write_text(content.strip(), encoding="utf-8")
    try:
        typst.compile(input=str(typst_path), output=str(output_path))
    finally:
        if not keep_temp:
            typst_path.unlink(missing_ok=True)
