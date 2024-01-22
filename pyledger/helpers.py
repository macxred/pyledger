import datetime, numpy as np, pandas as pd, re

def represents_integer(x) -> bool:
    """
    Check if the input is an integer number and can be cast as an integer.

    Parameters:
    x (Any): The value to be checked.

    Returns:
    bool: True if x is an integer number, False otherwise.

    Examples:
    >>> represents_integer(4)
    True
    >>> represents_integer(4.0)  # Float with an integer value
    True
    >>> represents_integer("4")
    True
    >>> represents_integer("4.5")
    False
    >>> represents_integer(None)
    False
    >>> represents_integer("abc")
    False
    >>> represents_integer([4])
    False
    """
    if isinstance(x, int):
        return True
    elif isinstance(x, float):
        return x.is_integer()
    else:
        try:
            return int(x) == float(x)
        except (ValueError, TypeError):
            return False

def write_fixed_width_csv(df, path=None, sep=', ', na_rep='', n=None, *args,
                          **kwargs):
    """
    Generate a human readable CSV

    Writes a pandas DataFrame to a CSV file, ensuring that the first n columns
    have a fixed width determined by the longest entry in each column.
    Text is right aligned and NA values are represented as specified.
    If n is None, all columns except the last will have fixed width.

    Parameters:
    df (pandas.DataFrame): DataFrame to be written to CSV.
    path (str): Name/path of the CSV file to write. If None, returns the csv
        output as string.
    sep (str): Separator for CSV file, default is ', '. In contrast to
        pd.to_csv, multi-char separators are supported.
    na_rep (str): String representation for NA/NaN data. Default is ''.
    n (int): Number of columns from start to have fixed width. If None,
             applies to all columns except the last.
    *args, **kwargs: Additional arguments for pandas to_csv method.
    """
    result = {}
    fixed_width_cols = (df.shape[1] - 1 if n is None else n)

    for i in range(len(df.columns)):
        col = df.iloc[:, i]
        col_str = pd.Series(np.where(col.isna(), na_rep, col.astype(str)))
        colname = df.columns[i]
        max_length = max(col_str.dropna().apply(len).max(), len(colname))

        # Fixed width formatting
        if i < fixed_width_cols:
            col_str = col_str.apply(lambda x: x.rjust(max_length))
            colname = colname.rjust(max_length)

        # Separator for all but the first column
        if i > 0:
            col_str = sep[1:] + col_str
            colname = sep[1:] + colname

        result[colname] = col_str

    result = pd.DataFrame(result)

    # Write to CSV
    return result.to_csv(path, sep=sep[0], index=False, na_rep=na_rep,
                         *args, **kwargs)