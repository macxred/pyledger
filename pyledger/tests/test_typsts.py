import pandas as pd
import textwrap
import pytest
from pyledger.typst import (
    df_to_typst, escape_typst_text, render_typst
)


def test_empty_dataframe():
    df = pd.DataFrame()
    result = df_to_typst(df)
    expected = (
        "table(\n"
        "  stroke: none,\n"
        "  columns: 0,\n"
        "  \n"
        ")"
    )
    assert result.strip() == expected.strip()


def test_dataframe_with_only_headers():
    df = pd.DataFrame(columns=["A", "B", "C"])
    result = df_to_typst(df)
    expected = textwrap.dedent("""\
        table(
          stroke: none,
          columns: 3,
          [A], [B], [C],
        )""")
    assert result.strip() == expected.strip()


def test_na_value_handling():
    df = pd.DataFrame({"A": [1, None], "B": [None, 2]})
    result = df_to_typst(df, na_value="NA")
    expected = textwrap.dedent("""\
        table(
          stroke: none,
          columns: 2,
          [A], [B],
          [1.0], [NA],
          [NA], [2.0],
        )""")
    assert result.strip() == expected.strip()


def test_column_alignments_and_widths():
    df = pd.DataFrame({"A": [1], "B": [2]})
    result = df_to_typst(df, align=["left", "right"], columns=["1fr", "2fr"])
    expected = textwrap.dedent("""\
        table(
          stroke: none,
          columns: (1fr, 2fr),
          align: (left, right),
          [A], [B],
          [1], [2],
        )""")
    assert result.strip() == expected.strip()


def test_bold_rows():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    result = df_to_typst(df, bold=[0, 2])
    expected = textwrap.dedent("""\
        table(
          stroke: none,
          columns: 2,
          text(weight: "bold", [A]), text(weight: "bold", [B]),
          [1], [3],
          text(weight: "bold", [2]), text(weight: "bold", [4]),
        )""")
    assert result.strip() == expected.strip()


def test_hline_rows():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    result = df_to_typst(df, hline=[0, 1, 3])
    expected = textwrap.dedent("""\
        table(
          stroke: none,
          columns: 2,
          [A], [B],
          table.hline(),
          [1], [3],
          table.hline(),
          [2], [4],
        )""")
    assert result.strip() == expected.strip()


def test_without_colnames():
    df = pd.DataFrame({"X": [10, 20], "Y": [30, 40]})
    result = df_to_typst(df, colnames=False)
    expected = textwrap.dedent("""\
        table(
          stroke: none,
          columns: 2,
          [10], [30],
          [20], [40],
        )""")
    assert result.strip() == expected.strip()


def test_various_data_types():
    df = pd.DataFrame({
        "Int": [1],
        "Float": [2.5],
        "Bool": [True],
        "Str": ["text"],
        "Date": [pd.Timestamp("2023-01-01")]
    })
    result = df_to_typst(df)
    expected = textwrap.dedent(f"""\
        table(
          stroke: none,
          columns: 5,
          [Int], [Float], [Bool], [Str], [Date],
          [1], [2.5], [True], [text], [{df['Date'][0]}],
        )""")
    assert result.strip() == expected.strip()


def test_bold_and_hline_interaction():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    result = df_to_typst(df, bold=[0, 2], hline=[0, 2])
    expected = textwrap.dedent("""\
        table(
          stroke: none,
          columns: 2,
          text(weight: "bold", [A]), text(weight: "bold", [B]),
          table.hline(),
          [1], [3],
          text(weight: "bold", [2]), text(weight: "bold", [4]),
          table.hline(),
        )""")
    assert result.strip() == expected.strip()


def test_typst_table_with_mismatched_align_length():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    with pytest.raises(ValueError, match="`align` has 1 elements but expected 2"):
        df_to_typst(df, align=["center"], columns=["1fr", "auto"])


def test_typst_table_with_mismatched_columns_length():
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    with pytest.raises(ValueError, match="`columns` has 1 elements but expected 2"):
        df_to_typst(df, align=["left", "right"], columns=["1fr"])


def test_escape_typst_text():
    series = pd.Series(["<tag>", "email@host", "1 > 0", None, 123])
    result = escape_typst_text(series)
    expected = pd.Series(["\\<tag\\>", "email\\@host", "1 \\> 0", None, 123])
    pd.testing.assert_series_equal(result, expected)


def test_render_typst_creates_pdf_and_removes_temp(tmp_path):
    content = "= Hello World\nThis is a test."
    output_pdf = tmp_path / "test_output.pdf"
    render_typst(content, output_pdf)
    typ_file = output_pdf.with_suffix(".typ")

    assert output_pdf.exists()
    assert not typ_file.exists()


def test_render_typst_keep_temp(tmp_path):
    content = "= Hello World\nThis is a test."
    output_pdf = tmp_path / "test_output.pdf"
    render_typst(content, output_pdf, keep_temp=True)
    typ_file = output_pdf.with_suffix(".typ")

    assert output_pdf.exists()
    assert typ_file.exists()
    actual_typ_content = typ_file.read_text(encoding="utf-8").strip()

    assert actual_typ_content == content.strip()
    typ_file.unlink()
