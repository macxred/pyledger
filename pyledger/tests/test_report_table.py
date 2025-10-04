"""Test suite for testing report_table() method."""

from encodings.punycode import T
import pytest
import pandas as pd
from pyledger.memory_ledger import MemoryLedger
from .base_test import BaseTest
from io import StringIO
from consistent_df import assert_frame_equal


COLUMNS_CSV = """
label,period,profit_centers
 2024,  2024,
 2025,  2025,
"""
COLUMNS = pd.read_csv(StringIO(COLUMNS_CSV), skipinitialspace=True, dtype="string")

# flake8: noqa: E501
EXPECTED_TYPST = (
    "table(\n"
    "  stroke: none,\n"
    "  columns: (1fr, auto, auto),\n"
    "  align: (left, right, right),\n"
    "  table.header(repeat: true,\n"
    "    text(weight: \"bold\", []), text(weight: \"bold\", [2024]), text(weight: \"bold\", [2025]),\n"
    "  ),\n"
    "  text(weight: \"bold\", [Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  table.hline(),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Cash]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Bank of America], [1,076,311.79], [],\n"
    "  [Other Bank], [-123.26], [],\n"
    "  [Deutsche Bank], [11,199,940.72], [],\n"
    "  [Mitsubishi UFJ], [342,620.00], [],\n"
    "  [UBS], [100,000.00], [],\n"
    "  text(weight: \"bold\", [Total Cash]), text(weight: \"bold\", [12,718,749.25]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Current receivables], [], [],\n"
    "  text(weight: \"bold\", [Total Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Tax Recoverable]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [VAT Recoverable (Input VAT)], [360.85], [],\n"
    "  text(weight: \"bold\", [Total Tax Recoverable]), text(weight: \"bold\", [360.85]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Total Assets]), text(weight: \"bold\", [12,719,110.10]), text(weight: \"bold\", []),\n"
    ")\n"
)

EXPECTED_TYPST_STAGGERED = (
    "table(\n"
    "  stroke: none,\n"
    "  columns: (1fr, auto, auto),\n"
    "  align: (left, right, right),\n"
    "  table.header(repeat: true,\n"
    "    text(weight: \"bold\", []), text(weight: \"bold\", [2024]), text(weight: \"bold\", [2025]),\n"
    "  ),\n"
    "  text(weight: \"bold\", [Cash]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Bank of America], [1'076'311.79], [],\n"
    "  [Other Bank], [-123.26], [],\n"
    "  [Deutsche Bank], [11'199'940.72], [],\n"
    "  [Mitsubishi UFJ], [342'620.00], [],\n"
    "  [UBS], [100'000.00], [],\n"
    "  text(weight: \"bold\", [Total Cash]), text(weight: \"bold\", [12'718'749.25]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Current receivables], [], [],\n"
    "  text(weight: \"bold\", [Total Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Tax Recoverable]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [VAT Recoverable (Input VAT)], [360.85], [],\n"
    "  text(weight: \"bold\", [Total Tax Recoverable]), text(weight: \"bold\", [360.85]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Assets]), text(weight: \"bold\", [12'719'110.10]), text(weight: \"bold\", []),\n"
    "  table.hline(),\n"
    ")\n"
)
EXPECTED_TYPST_DROPPED = (
    "table(\n"
    "  stroke: none,\n"
    "  columns: (1fr, auto, auto),\n"
    "  align: (left, right, right),\n"
    "  table.header(repeat: true,\n"
    "    text(weight: \"bold\", []), text(weight: \"bold\", [2024]), text(weight: \"bold\", [2025]),\n"
    "  ),\n"
    "  text(weight: \"bold\", [Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  table.hline(),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Cash]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Bank of America], [1,076,311.79], [],\n"
    "  [Other Bank], [-123.26], [],\n"
    "  [Deutsche Bank], [11,199,940.72], [],\n"
    "  [Mitsubishi UFJ], [342,620.00], [],\n"
    "  [UBS], [100,000.00], [],\n"
    "  text(weight: \"bold\", [Total Cash]), text(weight: \"bold\", [12,718,749.25]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Tax Recoverable]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [VAT Recoverable (Input VAT)], [360.85], [],\n"
    "  text(weight: \"bold\", [Total Tax Recoverable]), text(weight: \"bold\", [360.85]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Total Assets]), text(weight: \"bold\", [12,719,110.10]), text(weight: \"bold\", []),\n"
    ")\n"
)

EXPECTED_TYPST_CURRENCIES = (
    "{\n"
    "let foreign-currency-balance(body) = text(fill: gray, size: 0.7em)[#body]\n"
    "\n"
    "table(\n"
    "  stroke: none,\n"
    "  columns: (1fr, auto, auto),\n"
    "  align: (left, right, right),\n"
    "  table.header(repeat: true,\n"
    "    text(weight: \"bold\", []), text(weight: \"bold\", [2024]), text(weight: \"bold\", [2025]),\n"
    "  ),\n"
    "  text(weight: \"bold\", [Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  table.hline(),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Cash]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Bank of America], [1,076,311.79], [],\n"
    "  [Other Bank], [-123.26], [],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR -20.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD -100.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  [Deutsche Bank], [11,199,940.72], [],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 10,026,687.10]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  [Mitsubishi UFJ], [342,620.00], [],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[JPY 54,345,678.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  [UBS], [100,000.00], [],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[CHF 14,285,714.30]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  text(weight: \"bold\", [Total Cash]), text(weight: \"bold\", [12,718,749.25]), text(weight: \"bold\", []),\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[CHF 14,285,714.30]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 10,026,667.10]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[JPY 54,345,678.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 1,076,211.79]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Current receivables], [], [],\n"
    "  text(weight: \"bold\", [Total Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Tax Recoverable]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [VAT Recoverable (Input VAT)], [360.85], [],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 133.33]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 216.93]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  text(weight: \"bold\", [Total Tax Recoverable]), text(weight: \"bold\", [360.85]), text(weight: \"bold\", []),\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 133.33]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 216.93]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Total Assets]), text(weight: \"bold\", [12,719,110.10]), text(weight: \"bold\", []),\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[CHF 14,285,714.30]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 10,026,800.43]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[JPY 54,345,678.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 1,076,428.72]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
    ")\n"
    "\n"
    "}\n"
)
# flake8: enable


EXPECTED_BALANCE_CSV = """
label,                           2024,              2025
Assets,                         ,
,                               ,
Cash,                           ,
Bank of America,                1'076'311.79,
Other Bank,                     -123.26,
Deutsche Bank,                  11'199'940.72,
Mitsubishi UFJ,                 342'620.00,
UBS,                            100'000.00,
Total Cash,                     12'718'749.25,
,                               ,
Current Assets,                 ,
Current receivables,            ,
Total Current Assets,           ,
,                               ,
Tax Recoverable,                ,
VAT Recoverable (Input VAT),    360.85,
Total Tax Recoverable,          360.85,
,                               ,
Total Assets,                   12'719'110.10,
"""
EXPECTED_DATAFRAME = pd.read_csv(StringIO(EXPECTED_BALANCE_CSV), index_col=False, skipinitialspace=True)

EXPECTED_BALANCE_DROPPED_CSV = """
label,                               2024,              2025
Assets,                         ,
,                               ,
Cash,                           ,
Bank of America,                1'076'311.79,
Other Bank,                     -123.26,
Deutsche Bank,                  11'199'940.72,
Mitsubishi UFJ,                 342'620.00,
UBS,                            100'000.00,
Total Cash,                     12'718'749.25,
,                               ,
Tax Recoverable,                ,
VAT Recoverable (Input VAT),    360.85,
Total Tax Recoverable,          360.85,
,                               ,
Total Assets,                   12'719'110.10,
"""
EXPECTED_DATAFRAME_DROPPED = pd.read_csv(
    StringIO(EXPECTED_BALANCE_DROPPED_CSV), index_col=False, skipinitialspace=True
).fillna("")
# flake8: enable


def account_multiplier(row) -> int:
    g = row["group"]
    if g.startswith("Liabilities") or g.startswith("Income"):
        return -1
    if g.startswith("Equity"):
        return -1
    return 1


@pytest.fixture()
def restored_engine():
    engine = MemoryLedger()
    engine.restore(
        accounts=BaseTest.ACCOUNTS, configuration=BaseTest.CONFIGURATION,
        journal=BaseTest.JOURNAL, assets=BaseTest.ASSETS, price_history=BaseTest.PRICES,
        revaluations=BaseTest.REVALUATIONS, profit_centers=BaseTest.PROFIT_CENTERS,
        tax_codes=BaseTest.TAX_CODES,
    )
    return engine


@pytest.fixture()
def balance_accounts(restored_engine):
    accounts = restored_engine.accounts.list()
    balance_accounts = accounts.loc[
        (accounts["account"] >= 1000) & (accounts["account"] < 2000)
    ]
    balance_accounts["account_multiplier"] = balance_accounts.apply(account_multiplier, axis=1)
    return balance_accounts


def test_account_balance_typst_format(restored_engine, balance_accounts):
    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
    )
    assert balance_table == EXPECTED_TYPST, "Typst output does not match"


def test_account_balance_typst_format_staggered(restored_engine, balance_accounts):
    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=True,
        format_number = lambda x: f"{x:,.2f}".replace(",", "'")
    )
    assert balance_table == EXPECTED_TYPST_STAGGERED, "Typst output does not match"


def test_account_balance_typst_drop_empty(restored_engine, balance_accounts):
    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        format="typst",
        drop_empty=True,
    )
    assert balance_table == EXPECTED_TYPST_DROPPED


def test_account_balance_typst_format_include_currencies(restored_engine, balance_accounts):
    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        currency_balances=True,
    )
    assert balance_table == EXPECTED_TYPST_CURRENCIES, "Typst output does not match"


def test_custom_foreign_currency_style(restored_engine, balance_accounts):
    """Test that custom foreign_currency_style parameter works."""
    custom_style = "let foreign-currency-balance(body) = text(fill: blue, size: 0.9em)[#body]"

    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        currency_balances=True,
        foreign_currency_style=custom_style,
    )

    # flake8: noqa: E501
    expected = (
        "{\n"
        "let foreign-currency-balance(body) = text(fill: blue, size: 0.9em)[#body]\n"
        "\n"
        "table(\n"
        "  stroke: none,\n"
        "  columns: (1fr, auto, auto),\n"
        "  align: (left, right, right),\n"
        "  table.header(repeat: true,\n"
        "    text(weight: \"bold\", []), text(weight: \"bold\", [2024]), text(weight: \"bold\", [2025]),\n"
        "  ),\n"
        "  text(weight: \"bold\", [Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
        "  table.hline(),\n"
        "  [], [], [],\n"
        "  text(weight: \"bold\", [Cash]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
        "  [Bank of America], [1,076,311.79], [],\n"
        "  [Other Bank], [-123.26], [],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR -20.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD -100.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  [Deutsche Bank], [11,199,940.72], [],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 10,026,687.10]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  [Mitsubishi UFJ], [342,620.00], [],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[JPY 54,345,678.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  [UBS], [100,000.00], [],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[CHF 14,285,714.30]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  text(weight: \"bold\", [Total Cash]), text(weight: \"bold\", [12,718,749.25]), text(weight: \"bold\", []),\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[CHF 14,285,714.30]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 10,026,667.10]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[JPY 54,345,678.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 1,076,211.79]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  [], [], [],\n"
        "  text(weight: \"bold\", [Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
        "  [Current receivables], [], [],\n"
        "  text(weight: \"bold\", [Total Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
        "  [], [], [],\n"
        "  text(weight: \"bold\", [Tax Recoverable]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
        "  [VAT Recoverable (Input VAT)], [360.85], [],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 133.33]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 216.93]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  text(weight: \"bold\", [Total Tax Recoverable]), text(weight: \"bold\", [360.85]), text(weight: \"bold\", []),\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 133.33]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 216.93]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  [], [], [],\n"
        "  text(weight: \"bold\", [Total Assets]), text(weight: \"bold\", [12,719,110.10]), text(weight: \"bold\", []),\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[CHF 14,285,714.30]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[EUR 10,026,800.43]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[JPY 54,345,678.00]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        "  table.cell(inset: (top: 0.2pt))[], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[USD 1,076,428.72]], table.cell(inset: (top: 0.2pt))[#foreign-currency-balance()[]],\n"
        ")\n"
        "\n"
        "}\n"
    )
    # flake8: enable

    assert balance_table == expected


def test_account_balance_dataframe_format(restored_engine, balance_accounts):
    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        format="dataframe",
        format_number = lambda x: f"{x:,.2f}".replace(",", "'")
    )
    EXPECTED_DATAFRAME.columns = balance_table.columns
    assert_frame_equal(balance_table, EXPECTED_DATAFRAME.fillna(""), check_dtype=False)


def test_account_balance_dataframe_drop_empty_exact(restored_engine, balance_accounts):
    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        format="dataframe",
        drop_empty=True,
        format_number=lambda x: f"{x:,.2f}".replace(",", "'"),
    )
    EXPECTED_DATAFRAME_DROPPED.columns = balance_table.columns
    assert_frame_equal(balance_table, EXPECTED_DATAFRAME_DROPPED, check_dtype=False)


def test_duplicate_labels_raises(restored_engine, balance_accounts):
    config_with_duplicates = pd.DataFrame({
        "label": ["2024", "2024"],  # duplicate label
        "period": ["2024", "2025"],
        "profit_centers": [None, None]
    }, dtype="string")

    with pytest.raises(ValueError, match="Duplicate column names in"):
        restored_engine.report_table(
            columns=config_with_duplicates,
            accounts=balance_accounts,
            staggered=False
        )


def test_report_table_escapes_special_characters():
    """Test that Typst special characters are escaped in account descriptions."""
    COLUMNS_CSV = """
        label,period,profit_centers
         2024,  2024,
    """
    COLUMNS = pd.read_csv(StringIO(COLUMNS_CSV), skipinitialspace=True, dtype="string")
    ACCOUNT_CSV = """
        group,              account, currency, tax_code, description
        Assets/Cash,           1000,      USD,         , Cash & $100
        Expenses/Marketing,    5000,      USD,         , Ads #social *bold*
    """
    ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)
    JOURNAL_CSV = """
        id,    date, account, contra, currency, amount, report_amount, tax_code, profit_center, description, document
         1, 2024-01,    1000,   5000,      USD, 100.00,              ,         ,              , Payment,
    """
    JOURNAL = pd.read_csv(StringIO(JOURNAL_CSV), skipinitialspace=True)

    # flake8: noqa: E501
    EXPECTED_ESCAPED = (
        "table(\n"
        "  stroke: none,\n"
        "  columns: (1fr, auto),\n"
        "  align: (left, right),\n"
        "  table.header(repeat: true,\n"
        '    text(weight: "bold", []), text(weight: "bold", [2024]),\n'
        "  ),\n"
        '  text(weight: "bold", [Assets]), text(weight: "bold", []),\n'
        "  table.hline(),\n"
        "  [], [],\n"
        '  text(weight: "bold", [Cash]), text(weight: "bold", []),\n'
        "  [Cash & \\$100], [100.00],\n"
        '  text(weight: "bold", [Total Cash]), text(weight: "bold", [100.00]),\n'
        "  [], [],\n"
        '  text(weight: "bold", [Total Assets]), text(weight: "bold", [100.00]),\n'
        "  [], [],\n"
        "  [], [],\n"
        '  text(weight: "bold", [Expenses]), text(weight: "bold", []),\n'
        "  table.hline(),\n"
        "  [], [],\n"
        '  text(weight: "bold", [Marketing]), text(weight: "bold", []),\n'
        "  [Ads \\#social \\*bold\\*], [-100.00],\n"
        '  text(weight: "bold", [Total Marketing]), text(weight: "bold", [-100.00]),\n'
        "  [], [],\n"
        '  text(weight: "bold", [Total Expenses]), text(weight: "bold", [-100.00]),\n'
        ")\n"
    )
    # flake8: enable

    engine = MemoryLedger()
    engine.restore(
        accounts=ACCOUNTS, journal=JOURNAL,
        configuration=BaseTest.CONFIGURATION, assets=BaseTest.ASSETS, price_history=BaseTest.PRICES,
    )

    accounts = engine.accounts.list()
    accounts["account_multiplier"] = 1

    result = engine.report_table(accounts=accounts, columns=COLUMNS)
    assert result == EXPECTED_ESCAPED


def test_invalid_labels_raises(restored_engine, balance_accounts):
    config_with_invalid_labels = pd.DataFrame({
        "label": ["20__24", "2024"],  # Label with invalid characters
        "period": ["2024", "2025"],
        "profit_centers": [None, None]
    }, dtype="string")

    with pytest.raises(ValueError, match="Column labels should not include '__'"):
        restored_engine.report_table(
            columns=config_with_invalid_labels,
            accounts=balance_accounts,
            staggered=False,
            currency_balances=True,
        )


def test_style_matrix_text_weight(restored_engine, balance_accounts):
    style_matrix = pd.DataFrame({
        'row': [1, 2],
        'col': [0, 1],
        'style': [
            {'text': {'weight': 'bold'}},
            {'text': {'fill': 'gray', 'size': '0.7em'}}
        ]
    })

    result = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        style_matrix=style_matrix
    )

    assert '#text(weight: \'bold\')' in result or 'text(weight: \'bold\')' in result
    assert '#text(fill: \'gray\', size: \'0.7em\')' in result


def test_style_matrix_cell_inset(restored_engine, balance_accounts):
    style_matrix = pd.DataFrame({
        'row': [2],
        'col': [1],
        'style': [
            {'cell': {'inset': {'top': '0.2pt'}, 'fill': 'yellow'}}
        ]
    })

    result = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        style_matrix=style_matrix
    )

    assert 'table.cell(inset: (top: \'0.2pt\'), fill: \'yellow\')' in result


def test_style_matrix_combined_text_and_cell(restored_engine, balance_accounts):
    style_matrix = pd.DataFrame({
        'row': [3],
        'col': [0],
        'style': [
            {'text': {'weight': 'bold', 'fill': 'red'}, 'cell': {'inset': {'left': '1em'}}}
        ]
    })

    result = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        style_matrix=style_matrix
    )

    assert 'text(weight: \'bold\', fill: \'red\')' in result
    assert 'table.cell(inset: (left: \'1em\'))' in result


# flake8: noqa: E501
EXPECTED_TYPST_WITH_STYLE_MATRIX = (
    "table(\n"
    "  stroke: none,\n"
    "  columns: (1fr, auto, auto),\n"
    "  align: (left, right, right),\n"
    "  table.header(repeat: true,\n"
    "    text(weight: \"bold\", []), text(weight: \"bold\", [2024]), text(weight: \"bold\", [2025]),\n"
    "  ),\n"
    "  text(weight: \"bold\", [Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  table.hline(),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Cash]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Bank of America], table.cell(inset: (top: '0.5em'))[#text(fill: 'blue')[1,076,311.79]], [],\n"
    "  [Other Bank], [-123.26], [],\n"
    "  [Deutsche Bank], [11,199,940.72], [],\n"
    "  [Mitsubishi UFJ], [342,620.00], [],\n"
    "  [UBS], [100,000.00], [],\n"
    "  text(weight: \"bold\", [Total Cash]), text(weight: \"bold\", [12,718,749.25]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [Current receivables], [], [],\n"
    "  text(weight: \"bold\", [Total Current Assets]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Tax Recoverable]), text(weight: \"bold\", []), text(weight: \"bold\", []),\n"
    "  [VAT Recoverable (Input VAT)], [360.85], [],\n"
    "  text(weight: \"bold\", [Total Tax Recoverable]), text(weight: \"bold\", [360.85]), text(weight: \"bold\", []),\n"
    "  [], [], [],\n"
    "  text(weight: \"bold\", [Total Assets]), text(weight: \"bold\", [12,719,110.10]), text(weight: \"bold\", []),\n"
    ")\n"
)
# flake8: enable


def test_style_matrix_exact_output(restored_engine, balance_accounts):
    style_matrix = pd.DataFrame({
        'row': [4],
        'col': [1],
        'style': [
            {'text': {'fill': 'blue'}, 'cell': {'inset': {'top': '0.5em'}}}
        ]
    })

    result = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        staggered=False,
        style_matrix=style_matrix
    )

    assert result == EXPECTED_TYPST_WITH_STYLE_MATRIX


def test_style_matrix_schema_validation_error(restored_engine, balance_accounts):
    invalid_style_matrix = pd.DataFrame({
        'row': [0],
        'style': [{'text': {'weight': 'bold'}}]
    })

    with pytest.raises(ValueError):
        restored_engine.report_table(
            columns=COLUMNS,
            accounts=balance_accounts,
            style_matrix=invalid_style_matrix
        )


def test_style_matrix_out_of_bounds_row(restored_engine, balance_accounts):
    balance_table = restored_engine.report_table(
        columns=COLUMNS,
        accounts=balance_accounts,
        format="dataframe"
    )
    max_row = len(balance_table) - 1

    style_matrix = pd.DataFrame({
        'row': [max_row + 10],
        'col': [0],
        'style': [{'text': {'weight': 'bold'}}]
    })

    with pytest.raises(ValueError, match="Invalid row indices"):
        restored_engine.report_table(
            columns=COLUMNS,
            accounts=balance_accounts,
            style_matrix=style_matrix
        )


def test_style_matrix_out_of_bounds_col(restored_engine, balance_accounts):
    style_matrix = pd.DataFrame({
        'row': [0],
        'col': [10],
        'style': [{'text': {'weight': 'bold'}}]
    })

    with pytest.raises(ValueError, match="Invalid col indices"):
        restored_engine.report_table(
            columns=COLUMNS,
            accounts=balance_accounts,
            style_matrix=style_matrix
        )


def test_style_matrix_invalid_style_structure(restored_engine, balance_accounts):
    style_matrix = pd.DataFrame({
        'row': [0],
        'col': [0],
        'style': ['not a dict']
    })

    with pytest.raises(ValueError, match="style must be dict"):
        restored_engine.report_table(
            columns=COLUMNS,
            accounts=balance_accounts,
            style_matrix=style_matrix
        )


def test_style_matrix_missing_text_cell_keys(restored_engine, balance_accounts):
    style_matrix = pd.DataFrame({
        'row': [0],
        'col': [0],
        'style': [{'invalid_key': 'value'}]
    })

    with pytest.raises(ValueError, match="must have 'text' and/or 'cell' keys"):
        restored_engine.report_table(
            columns=COLUMNS,
            accounts=balance_accounts,
            style_matrix=style_matrix
        )
