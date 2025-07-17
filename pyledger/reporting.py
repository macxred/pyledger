"""Module with helper functions for reporting."""

import pandas as pd
from consistent_df import enforce_schema


def summarize_groups(
    df, group="group", description="description",
    summarize={"report_balance": "sum"}, leading_space=0
):
    """
    Format a table with hierarchical groups for clear reporting.

    This method expands a DataFrame where each row belongs to a hierarchical group and adds:
    - A title row above each group
    - A summary row below each group, showing aggregated values
    - A "level" column to mark the row type:
      - "H1", "H2", etc. for group headers by depth
      - "body" for original data rows
      - "S1", "S2", etc. for group summaries
      - "gap" for blank spacer rows between groups

    Args:
        df (pd.DataFrame): Input DataFrame with at least `group`, `description`,
            and the columns listed in `summarize` argument.
        group (str): Name of the column containing hierarchical group paths (POSIX-style).
        description (str): Name of the column with line item descriptions.
        summarize (dict): Columns to summarize and how (e.g. {"amount": "sum"}),
            passed to `DataFrame.agg()`.
        leading_space (int): If greater than 0, inserts a blank row before headers up to
            the specified level ("H1", "H2", etc.), except at the top of the table.

    Returns:
        pd.DataFrame: A formatted DataFrame with headers, summaries, and gaps.

    Raises:
        ValueError: If the input DataFrame does not contain all required columns:
            [group, description, *summarize.keys()]
    """
    required_columns = [group, description] + list(summarize.keys())
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}.")

    schema = pd.DataFrame({
        "column": ["level", group, description] + list(summarize.keys()),
        "dtype": ["string", "string", "string"] + ["Float64"] * len(summarize)
    })
    if df.empty:
        return enforce_schema(None, schema)

    result = _summarize_groups(
        df.copy(), group=group, description=description, summarize=summarize
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


def _summarize_groups(df, group, description, summarize, level=1):
    """Recursively adds headers and summaries for nested group levels."""
    rows = []
    next_level = df[group].apply(lambda x: x.split('/', 1)[0] if '/' in x else x)
    for part, subdf in df.groupby(next_level, sort=False):
        subdf = subdf.copy()
        subdf[group] = subdf[group].str.replace(f'^{part}/?', '', regex=True)
        group_summary = subdf.agg(summarize).to_dict()

        # Blank line
        rows.append({group: part, description: '', 'level': 'gap'})
        # Header line
        rows.append({group: part, description: part, 'level': f'H{level}'})

        # Recursively process sub-groups
        if any(subdf[group] != ''):
            child_rows = _summarize_groups(
                subdf[subdf[group] != ''], group=group,
                description=description, summarize=summarize, level=level + 1
            )
            child_rows[group] = part + "/" + child_rows[group]
            rows.extend(child_rows.to_dict('records'))

        # Body rows
        body_rows = subdf[subdf[group] == ''].assign(level='body')
        rows.extend(body_rows.assign(group=part).to_dict('records'))
        # Summary line
        rows.append({
            group: part, description: f'Total {part}', **group_summary, 'level': f'S{level}'
        })

        # Blank line
        rows.append({group: part, description: '', 'level': 'gap'})

    return pd.DataFrame(rows)
