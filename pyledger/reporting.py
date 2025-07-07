"""Sketch for a module with helper functions for reporting"""

import pandas as pd
from consistent_df import enforce_schema

def summarize_groups(df, group="group", description="description", summarize={"report_balance": "sum"}, leading_space=0):
    """
    Add header and summary for hierarchical groups

    Expand a Data Frame with rows assigned to hierarchical groups:
    - Add a title row above each group
    - Add a summary row at the bottom of each group
    - Add a column "level" indicating whether the row is a group header ("H1", "H2", etc.),
      part of the original data frame ("body"), a summary row ("S1", "S2", etc.),
      or an blank row ("gap") added to graphically distinguish groups.

    Args:
        df (pd.DataFrame): The data frame to summarize. Must contain `group`,
                           `description` and `summarize` columns.
        group (str): name of the column containing hierarchical groups
                     as POSIX paths.
        description (str): name of the column containing each line item's description.
        summarize (dict): Dict with columns (keys) and aggregation function (values)
                     to summarize when computing group summaries.
                     Passed as argument `func` to `pd.DataFrame.agg`.
        leading_space (int): Add an additional blank row before headers up to the
                     specified hierarchy level (H1, H2, etc.). No blank
                     rows will be added at the top of the data frame.

    Returns:
        pd.DataFrame: Expanded DataFrame with headers, bodies, and summaries.
    """
    schema = pd.DataFrame({
        "column": ["level", group, description] + list(summarize.keys()),
        "dtype" : ["string", "string", "string"] + ["Float64"]*len(summarize)
    })
    if df.empty:
        return enforce_schema(None, schema)

    result = _summarize_groups(df.copy(), group=group, description=description,
                               summarize=summarize)
    # drop leading ans trailing "gap" rows
    mask = (result['level'] != 'gap').cumsum() > 0
    mask &= (result['level'] != 'gap')[::-1].cumsum()[::-1] > 0
    # drop consecutive "gap" rows
    mask &= ~((result['level'] == 'gap') & (result['level'].shift() == 'gap'))
    result = result[mask].reset_index(drop=True)

    if leading_space > 0:
        # Add an additional blank row before each non-leading 'H1', 'H2', etc. level
        blank_row = result[result['level'] == 'gap'].iloc[0:1]
        mask = result['level'].str.fullmatch(f"H[1-{leading_space}]")
        mask.iloc[0] = False
        chunks = [item for _, group in result.groupby(mask.cumsum())
                       for item in [group, blank_row]]
        result = pd.concat(chunks[:-1], ignore_index=True)

    return enforce_schema(result, schema, sort_columns=True)

def _summarize_groups(df, group, description, summarize, level=1):
    """
    Recursively add group header and summary

    Args:
        level (int): Current depth in the hierarchy (used to build H1, H2, etc.
                     and S1, S2, etc.)
    """
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
            child_rows = _summarize_groups(subdf[subdf[group] != ''], group=group,
                                           description=description, summarize=summarize,
                                           level=level + 1)
            child_rows[group] = part + "/" + child_rows[group]
            rows.extend(child_rows.to_dict('records'))

        # Body rows
        body_rows = subdf[subdf[group] == ''].assign(level='body')
        rows.extend(body_rows.assign(group=part).to_dict('records'))

        # Summary line
        rows.append({group: part, description: f'Total {part}', **group_summary, 'level': f'S{level}'})

        # Blank line
        rows.append({group: part, description: '', 'level': 'gap'})

    return pd.DataFrame(rows)

