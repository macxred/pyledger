"""Definition of abstract base class for testing reconciliation operations."""

from io import StringIO
import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest
import datetime


class BaseTestReconciliation(BaseTest):

    @abstractmethod
    @pytest.fixture
    def engine(self):
        pass

    def test_reconciliation_accessor_mutators(self, engine, ignore_row_order=False):
        # Add reconciliation one by one and with multiple rows
        reconciliation = self.RECONCILIATION.sample(frac=1).reset_index(drop=True)
        for reconciliation_entry in reconciliation.head(-3).to_dict('records'):
            engine.reconciliation.add([reconciliation_entry])
        engine.reconciliation.add(reconciliation.tail(3))
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        reconciliation.loc[0, "tolerance"] = 0.0001
        engine.reconciliation.modify([{
            "file": reconciliation.loc[0, "file"],
            "period": reconciliation.loc[0, "period"],
            "account": reconciliation.loc[0, "account"],
            "currency": reconciliation.loc[0, "currency"],
            "profit_center": reconciliation.loc[0, "profit_center"],
            "tolerance": 0.0001
        }])
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        reconciliation.loc[3, "document"] = "Modify with all columns test"
        engine.reconciliation.modify([reconciliation.loc[3]])
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify multiple rows
        reconciliation.loc[reconciliation.index[[1, -1]], "document"] = "Modify multiple rows"
        engine.reconciliation.modify(reconciliation.loc[reconciliation.index[[1, -1]]])
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.reconciliation.delete([{
            "file": reconciliation["file"].iloc[0],
            "period": reconciliation["period"].iloc[0],
            "account": reconciliation["account"].iloc[0],
            "currency": reconciliation["currency"].iloc[0],
            "profit_center": reconciliation.loc[0, "profit_center"],
        }])
        reconciliation = reconciliation.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.reconciliation.delete(reconciliation.iloc[[1, -1]])
        reconciliation = reconciliation.drop(reconciliation.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.reconciliation.list(), reconciliation,
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_reconciliation_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_reconciliation = {
            "account": "1000:5000", "period": datetime.date(2023, 1, 1),
            "profit_center": "General",
        }
        engine.reconciliation.add([new_reconciliation])
        with pytest.raises(error_class, match=error_message):
            engine.reconciliation.add([new_reconciliation])

    def test_modify_nonexistent_reconciliation_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.reconciliation.modify([{
                "account": "1000", "period": datetime.date(2023, 1, 1),
                "profit_center": "General",
            }])

    def test_delete_reconciliation_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.reconciliation.delete([{
                "account": "1000:5000", "period": datetime.date(2023, 1, 1),
                "profit_center": "General",
            }], allow_missing=False)
        engine.reconciliation.delete([{
            "account": "1000:5000", "period": datetime.date(2023, 1, 1),
            "profit_center": "General",
        }], allow_missing=True)

    def test_mirror_reconciliations(self, engine):
        engine.restore(configuration=self.CONFIGURATION)
        target = pd.concat([self.RECONCILIATION, engine.reconciliation.list()], ignore_index=True)
        original_target = target.copy()
        engine.reconciliation.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to reconciliation.mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.RECONCILIATION.query("account not in [1000]")
        engine.reconciliation.mirror(target, delete=False)
        assert_frame_equal(
            original_target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        engine.reconciliation.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["account"] == "1000:9999", "document"] = "New Document"
        engine.reconciliation.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.reconciliation.list(), ignore_row_order=True, check_like=True
        )

    def test_mirror_empty_reconciliations(self, engine):
        engine.restore(reconciliation=self.RECONCILIATION, configuration=self.CONFIGURATION)
        assert not engine.reconciliation.list().empty
        engine.reconciliation.mirror(engine.reconciliation.standardize(None), delete=True)
        assert engine.reconciliation.list().empty

    def test_reconcile(self, engine):
        engine.restore(
            accounts=self.ACCOUNTS, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS, profit_centers=self.PROFIT_CENTERS,
            reconciliation=self.RECONCILIATION
        )

        for case in self.EXPECTED_RECONCILIATION:
            df = engine.reconcile(
                df=engine.reconciliation.list(),
                period=case["period"], file_pattern=case["file_pattern"]
            )
            expected = pd.read_csv(StringIO(case["reconciliation"]), skipinitialspace=True)
            assert_frame_equal(
                df, engine.reconciliation.standardize(expected),
                ignore_columns=["file", "tolerance"],
            )

    def test_reconcile_summary(self, engine):
        engine.restore(
            accounts=self.ACCOUNTS, configuration=self.CONFIGURATION, tax_codes=self.TAX_CODES,
            journal=self.JOURNAL, assets=self.ASSETS, price_history=self.PRICES,
            revaluations=self.REVALUATIONS, profit_centers=self.PROFIT_CENTERS,
            reconciliation=self.RECONCILIATION
        )

        # flake8: noqa: E501
        RECONCILIATION_CSV = """
            period,         account, currency,          profit_center,       balance,  report_balance, tolerance, document,                           file,                           actual_balance, actual_report_balance
            2023-12-31,   1000:2999,      CHF,                       ,          0.00,            0.00,          , 2023/reconciliation/2023-12-31.pdf, 2023/financial/all.pdf,                   0.00,                    0.00
            2024-01-23,   1000:2999,      EUR,                       ,        120.00,      1098332.82,      0.01, 2024/reconciliation/2024-01-23.pdf, 2024/start/all.pdf,                     120.02,              1098332.82
            2024-09-25,        1000,      USD,                       ,     776311.79,       776311.79,         1,                                   , 2024/financial/data.pdf,             776312.80,               776312.90
            2024-Q4,      1000:2999,      EUR,                       ,   10076638.88,     11655605.63,      0.01,    2024/reconciliation/2024-Q4.pdf, 2024/financial/data.pdf,           10076600.00,             11655600.00
            2024-08,      1000:2999,      CHF,               "Bakery",          0.00,            0.00,      0.01,                                   , 2024/financial/custom/data.pdf,           0.00,                    0.00
            2024,         1000:9999,      EUR,              "General",      27078.22,            0.00,      0.01,       2024/reconciliation/2024.pdf, 2024/financial/all.pdf,               27000.00,                    0.00
            2024,         1000:9999,      USD,              "General",    -498332.82,            0.00,      0.01,       2024/reconciliation/2024.pdf, 2024/financial/all.pdf,             -498300.00,                    0.00
        """
        # flake8: enable
        RECONCILIATION = pd.read_csv(StringIO(RECONCILIATION_CSV), skipinitialspace=True)
        messages = engine.reconciliation_summary(engine.reconciliation.standardize(RECONCILIATION))
        assert len(messages) == 7, f"Expected 7 error messages, but got {len(messages)}"

    def test_reconcile_summary_empty_df(self, engine):
        messages = engine.reconciliation_summary(engine.reconciliation.standardize(None))
        assert len(messages) == 0, (
            f"Expected 0 error messages with empty DataFrame, but got {len(messages)}"
        )
