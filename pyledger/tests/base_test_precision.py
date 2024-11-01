"""Abstract base class for testing precision()."""

import pytest
from abc import abstractmethod
from .base_test import BaseTest
import datetime


class BaseTestPrecision(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    @pytest.fixture()
    def ledger_engine(self, ledger):
        ledger.restore(settings=self.SETTINGS, assets=self.ASSETS)
        return ledger

    @pytest.mark.parametrize(
        "ticker, date, expected",
        [
            # Default increment for reporting_currency (CHF); date=None uses today
            ("reporting_currency", None, 1),
            # Exact date match for AUD on 2023-01-01
            ("AUD", datetime.date(2023, 1, 1), 0.001),
            # Default to today’s date for CHF
            ("CHF", None, 1),
            # Date before all CAD entries, uses NaT entry
            ("CAD", datetime.date(2020, 1, 1), 0.1),
            # Date after all AUD dates, uses latest increment
            ("AUD", datetime.date(2222, 1, 1), 0.01),
            # Latest date on/before 2023-12-31 for AUD
            ("AUD", datetime.date(2023, 12, 31), 0.001),
            # NaT and valid dates for EUR, date before all valid dates
            ("EUR", datetime.date(2023, 1, 1), 0.001),
        ]
    )
    def test_precision(self, ledger_engine, ticker, date, expected):
        assert ledger_engine.precision(ticker, date) == expected

    @pytest.mark.parametrize(
        "ticker, date, expected_exception, match",
        [
            ("XYZ", None, ValueError, "No data available for ticker 'XYZ'"),
            ("GBP", datetime.date(2022, 1, 1), ValueError, "No increments available for 'GBP'"),
        ]
    )
    def test_precision_exceptions(self, ledger_engine, ticker, date, expected_exception, match):
        with pytest.raises(expected_exception, match=match):
            ledger_engine.precision(ticker, date)
