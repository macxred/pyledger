"""Test suite for write_fixed_width_csv() method."""

import pandas as pd
import numpy as np
import tempfile
from io import StringIO
from pathlib import Path
from pyledger.helpers import write_fixed_width_csv


def test_empty_df():
    df = pd.DataFrame()
    output = write_fixed_width_csv(df)
    assert output == '\n', "Failed on empty DataFrame"


def test_empty_df_with_column_names():
    df = pd.DataFrame({
        'Col1': [],
        'LongerCol2': [],
        'Col3': []
    })
    output = write_fixed_width_csv(df)
    assert output == 'Col1, LongerCol2, Col3\n', "Failed on empty DataFrame with column names"


def test_single_column_df():
    df = pd.DataFrame({'A': [1, 2, 3]})
    output = write_fixed_width_csv(df)
    expected_output = 'A\n1\n2\n3\n'
    assert output == expected_output, "Failed on single column DataFrame"


def test_multiple_columns_mixed_values():
    df = pd.DataFrame({
        'Col1': [1, 2, 3],
        'LongerCol2': ['a', 'bb', 'ccc'],
        'Col3': [np.nan, 5.5, 6.0]
    })
    output = write_fixed_width_csv(df)
    expected_output = ('Col1, LongerCol2, Col3\n'
                       '   1,          a,\n'
                       '   2,         bb, 5.5\n'
                       '   3,        ccc, 6.0\n')
    assert output == expected_output, "Failed on multiple columns with mixed values"


def test_special_floating_numbers():
    df = pd.DataFrame({'A': [np.inf, -np.inf, np.nan], 'B': [1.1, 2.2, 3.3]})
    output = write_fixed_width_csv(df, na_rep='NA')
    expected_output = ('   A, B\n'
                       ' inf, 1.1\n'
                       '-inf, 2.2\n'
                       '  NA, 3.3\n')
    assert output == expected_output, "Failed on special floating numbers"


def test_large_integer_numbers():
    df = pd.DataFrame({'A': [12345678901234567890, -12345678901234567890], 'B': [0, 1]})
    output = write_fixed_width_csv(df)
    expected_output = ('                    A, B\n'
                       ' 12345678901234567890, 0\n'
                       '-12345678901234567890, 1\n')
    assert output == expected_output, "Failed on large integer numbers"


def test_non_string_data_types():
    df = pd.DataFrame({'A': [1, 2], 'B': [True, False], 'C': [None, 'Text']})
    output = write_fixed_width_csv(df)
    expected_output = ('A,     B, C\n'
                       '1,  True,\n'
                       '2, False, Text\n')
    assert output == expected_output, "Failed on non-string data types"


def test_empty_strings():
    df = pd.DataFrame({'A': ['', ''], 'B': ['', '']})
    output = write_fixed_width_csv(df)
    expected_output = 'A, B\n ,\n ,\n'
    assert output == expected_output, "Failed on empty strings"


def test_date_and_float_handling():
    df = pd.DataFrame({
        'IntCol': [1, 2, 3],
        'FloatCol': [1.1, 2.22, 3.333],
        'StringCol': ['a', 'bb', 'ccc'],
        'DateCol': pd.to_datetime(['2021-01-01', '2021-02-02', '2021-03-03'])
    })
    output = write_fixed_width_csv(df)
    expected_output = ('IntCol, FloatCol, StringCol, DateCol\n'
                       '     1,      1.1,         a, 2021-01-01\n'
                       '     2,     2.22,        bb, 2021-02-02\n'
                       '     3,    3.333,       ccc, 2021-03-03\n')
    assert output == expected_output, "Failed on various data types with dates"


def test_fixed_width_columns_specified_number():
    df = pd.DataFrame({
        'ShortCol': [1, 2, 3],
        'LongerCol': ['a', 'bb', 'ccc'],
        'FinalCol': [10, 20, 30]
    })
    output = write_fixed_width_csv(df, n=1)
    expected_output = ('ShortCol, LongerCol, FinalCol\n'
                       '       1, a, 10\n'
                       '       2, bb, 20\n'
                       '       3, ccc, 30\n')
    assert output == expected_output, "Failed when n applies to only the first column"


def test_fixed_width_columns_exceeding():
    df = pd.DataFrame({
        'ShortCol': [1, 2],
        'LongerCol': ['short', 'longer']
    })
    output = write_fixed_width_csv(df, n=5)
    expected_output = ('ShortCol, LongerCol\n'
                       '       1,     short\n'
                       '       2,    longer\n')
    assert output == expected_output, "Failed when n exceeds the number of columns"


def test_custom_separator():
    df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    output = write_fixed_width_csv(df, sep=' | ')
    expected_output = 'A "| B"\n1 "| 3"\n2 "| 4"\n'
    assert output == expected_output, "Failed on custom separator"


def test_custom_na_rep():
    df = pd.DataFrame({'A': [1, np.nan, 3], 'B': ['x', 'y', None]})
    output = write_fixed_width_csv(df, na_rep='NA')
    expected_output = ('  A, B\n'
                       '1.0, x\n'
                       ' NA, y\n'
                       '3.0, NA\n')
    assert output == expected_output, "Failed on custom na_rep"


def test_write_to_file():
    df = pd.DataFrame({'A': [1, 2, 3], 'B': ['x', 'y', 'z']})
    with tempfile.NamedTemporaryFile(delete=False, mode='w+') as tmpfile:
        path = Path(tmpfile.name)

    try:
        write_fixed_width_csv(df, file=path)
        assert path.exists(), "File was not created"

        output_file = path.read_text()
        expected_file_output = 'A, B\n1, x\n2, y\n3, z\n'
        assert output_file == expected_file_output, "File content is incorrect"
    finally:
        path.unlink()


def test_machine_readability():
    df = pd.DataFrame({
        'IntCol': [1, 2, 3, 4, 5],
        'FloatCol': [1.1, 2.2, 3.3, 4.4, 5.5],
        'StrCol': ['foo', 'bar', 'baz', 'qux', 'quux'],
        'BoolCol': [True, False, True, False, True]
    })

    output = write_fixed_width_csv(df)
    df_read = pd.read_csv(StringIO(output), skipinitialspace=True)
    pd.testing.assert_frame_equal(df, df_read, check_dtype=False)


def test_machine_readability_fixed_width():
    df = pd.DataFrame({
        'ShortCol': [1, 2, 3, 4, 5],
        'LongerCol': ['a', 'bb', 'ccc', 'dddd', 'eeeee'],
        'FinalCol': [10, 20, 30, 40, 50],
        'ExtraCol': ['extra1', 'extra2', 'extra3', 'extra4', 'extra5']
    })

    output = write_fixed_width_csv(df, n=3)
    df_read = pd.read_csv(StringIO(output), skipinitialspace=True)
    pd.testing.assert_frame_equal(df, df_read, check_dtype=False)


def test_machine_readability_with_none():
    df = pd.DataFrame({
        'Col1': [1, None, 3, None, 5],
        'Col2': ['a', 'b', None, 'd', None],
        'Col3': [1.5, 2.5, None, 4.5, 5.5],
        'Col4': [None, 'text', None, 'another', None]
    })

    df = df.astype(object).where(pd.notna(df), np.nan)
    output = write_fixed_width_csv(df)
    df_read = pd.read_csv(StringIO(output), skipinitialspace=True)
    df_read = df_read.astype(object).where(pd.notna(df_read), np.nan)
    pd.testing.assert_frame_equal(df, df_read, check_dtype=False)


def test_csv_lines_have_no_trailing_whitespace():
    df = pd.DataFrame({
        'Col1': [1, None, 3, None, 5],
        'Col2': ['a ', 'b', None, 'd ', None],
        'Col3': [1.5, 2.5, None, 4.5, 5.5],
        'Col4': [None, 'text ', None, 'another ', None]
    })

    df = df.astype(object).where(pd.notna(df), np.nan)
    output = write_fixed_width_csv(df)
    for line in output.split('\n'):
        assert not line.endswith(' '), f"Line has trailing whitespace: {line}"
