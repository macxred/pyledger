"""Module with helper functions for reporting."""

import pandas as pd
from consistent_df import enforce_schema


def summarize_groups(
    df,
    group="group",
    description="description",
    summarize={"report_balance": "sum"},
    leading_space=0,
    staggered=False
):
    """
    Summarize a DataFrame into a multi-level reporting layout with headers,
    body rows, and totals or cumulative subtotals per group.

    In hierarchical mode (`staggered=False`), slash-separated group paths
    generate nested sections with headers (`H1`, `H2`, ...) and per-level totals
    (`S1`, `S2`, ...). Optional spacing is added before headers.

    In staggered mode (`staggered=True`), the same hierarchy is retained but
    totals are replaced with cumulative subtotals (`subtotal`) and top-level
    headers are omitted.

    Args:
        df (pd.DataFrame): Input with `group`, `description`, and summary columns.
        group (str): Column with slash-separated group paths.
        description (str): Column with row labels.
        summarize (dict): Mapping of columns to aggregation functions:
            - String (e.g., "sum"): For numeric columns (Float64 dtype), aggregates using pandas
            - Callable: For dict columns (object dtype), custom aggregation function
        leading_space (int): Number of header levels to prefix with a gap (hierarchical only).
        staggered (bool): Use cumulative totals instead of per-level totals.

    Row types marked in the `level` column:
        - 'H1', 'H2', ...: Header rows (per group level)
        - 'S1', 'S2', ...: Totals (hierarchical mode only)
        - 'body': Original data rows
        - 'gap': Blank spacer rows

    Returns:
        pd.DataFrame: Structured output with the following columns:
            - 'level' (str): One of 'H1', 'H2', ..., 'S1', 'S2', ..., 'body', or 'gap'
            - 'group' (str): Group identifier for each row
            - 'description' (str): Row label (e.g., header name, item label, or total label)
            - <summary columns>: One column per key in `summarize`.
              Float64 for numeric columns (string agg), object for dict columns (callable agg).
    """
    required_columns = [group, description] + list(summarize.keys())
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}.")

    summary_dtypes = [
        "Float64" if isinstance(agg, str) else "object" for agg in summarize.values()
    ]
    schema = pd.DataFrame({
        "column": ["level", group, description] + list(summarize.keys()),
        "dtype": ["string", "string", "string"] + summary_dtypes
    })
    if df.empty:
        return enforce_schema(None, schema)

    if staggered:
        cumulative = pd.DataFrame({
            col: 0.0 if isinstance(agg, str) else {} for col, agg in summarize.items()
        }, index=[0])
    else:
        cumulative = None
    result = _summarize_groups(
        df.copy(), group, description, summarize,
        level=1, staggered=staggered, cumulative=cumulative
    )

    # Drop leading and trailing "gap" rows
    mask = (result['level'] != 'gap').cumsum() > 0
    mask &= (result['level'] != 'gap')[::-1].cumsum()[::-1] > 0
    # Drop consecutive "gap" rows
    mask &= ~((result['level'] == 'gap') & (result['level'].shift() == 'gap'))
    result = result[mask].reset_index(drop=True)

    if leading_space > 0:
        # Add an extra blank row before each non-leading "H1", "H2", etc.
        blank_row = result[result['level'] == 'gap'].iloc[0:1]
        mask = result['level'].str.fullmatch(f"H[1-{leading_space}]")
        mask.iloc[0] = False
        chunks = [
            item for _, group in result.groupby(mask.cumsum())
            for item in [group, blank_row]
        ]
        result = pd.concat(chunks[:-1], ignore_index=True)

    return enforce_schema(result, schema, sort_columns=True)


def _summarize_groups(df, group, description, summarize, level=1, staggered=False, cumulative=None):
    """Recursively adds headers and summaries for nested group levels."""
    rows = []
    next_level = df[group].apply(lambda x: x.split('/', 1)[0] if '/' in x else x)
    for part, subdf in df.groupby(next_level, sort=False):
        subdf = subdf.copy()
        # Remove current group level and optional slash from the group column
        subdf[group] = subdf[group].str.removeprefix(part).str.removeprefix("/")
        group_summary = subdf.agg(summarize).to_dict()

        # Blank line
        rows.append({group: part, description: '', 'level': 'gap'})
        # Header line
        if not (staggered and level == 1):
            rows.append({group: part, description: part, 'level': f'H{level}'})

        # Recursively process child groups
        if any(subdf[group] != ''):
            child_rows = _summarize_groups(
                subdf[subdf[group] != ''], group=group,
                description=description, summarize=summarize,
                level=level + 1, staggered=False, cumulative=cumulative
            )
            child_rows[group] = part + "/" + child_rows[group]
            rows.extend(child_rows.to_dict('records'))

        # Body rows
        body_rows = subdf[subdf[group] == ''].assign(level='body')
        rows.extend(body_rows.assign(group=part).to_dict('records'))

        # Add total
        if staggered:
            cumulative += pd.DataFrame([group_summary])
            rows.append({
                group: part, description: f'{part}',
                **cumulative.to_dict(orient="records")[0], 'level': 'H1'
            })
        else:
            rows.append({
                group: part, description: f'Total {part}',
                **group_summary, 'level': f'S{level}'
            })

        # Closing blank line
        rows.append({group: part, description: '', 'level': 'gap'})

    return pd.DataFrame(rows)
