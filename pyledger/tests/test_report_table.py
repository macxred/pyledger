"""Test suite for testing report_table() method."""

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
