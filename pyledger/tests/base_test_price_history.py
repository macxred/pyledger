"""Definition of abstract base class for testing price history operations."""

import pytest
import pandas as pd
from abc import abstractmethod
from consistent_df import assert_frame_equal
from .base_test import BaseTest
import datetime


class BaseTestPriceHistory(BaseTest):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    def test_price_accessor_mutators(self, ledger, ignore_row_order=False):
        ledger.restore(settings=self.SETTINGS)

        # Add prices
        price_history = self.PRICES.sample(frac=1).reset_index(drop=True)
        for price in price_history.to_dict('records'):
            ledger.price_history.add([price])
        assert_frame_equal(
            ledger.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify prices
        rows = [0, 3, len(price_history) - 1]
        for i in rows:
            price_history.loc[i, "price"] = 0.001
            ledger.price_history.modify([price_history.loc[i]])
            assert_frame_equal(
                ledger.price_history.list(), price_history,
                check_like=True, ignore_row_order=ignore_row_order
            )

        # Delete prices
        ledger.price_history.delete([{
            "ticker": price_history['ticker'].iloc[rows[0]],
            "date": price_history['date'].iloc[rows[0]],
            "currency": price_history['currency'].iloc[rows[0]]
        }])
        price_history = price_history.drop(rows[0]).reset_index(drop=True)
        assert_frame_equal(
            ledger.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_price_raises_error(
        self, ledger, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_price = {
            "ticker": "AUD", "date": datetime.date(2023, 1, 1),
            "currency": "USD", "price": 123
        }
        ledger.price_history.add([new_price])
        with pytest.raises(error_class, match=error_message):
            ledger.price_history.add([new_price])

    def test_modify_nonexistent_price_raises_error(
        self, ledger, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            ledger.price_history.modify([{
                "ticker": "AUD", "date": datetime.date(2023, 1, 1),
                "currency": "USD", "price": 123
            }])

    def test_delete_price_allow_missing(self, ledger):
        ledger.restore(price_history=self.PRICES, settings=self.SETTINGS)
        with pytest.raises(ValueError, match="Some ids are not present in the data."):
            ledger.price_history.delete([{
                "ticker": "FAKE", "date": datetime.date(2023, 1, 1),
                "currency": "USD", "price": 123
            }], allow_missing=False)
        ledger.price_history.delete([{
            "ticker": "FAKE", "date": datetime.date(2023, 1, 1),
            "currency": "USD", "price": 123
        }], allow_missing=True)

    def test_mirror_prices(self, ledger):
        ledger.restore(settings=self.SETTINGS)
        target = pd.concat([self.PRICES, ledger.price_history.list()], ignore_index=True)
        original_target = target.copy()
        ledger.price_history.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to price_history.mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            target, ledger.price_history.list(),
            ignore_row_order=True, check_like=True
        )

        target = self.PRICES.query("ticker not in ['JPY']")
        ledger.price_history.mirror(target, delete=True)
        assert_frame_equal(
            target, ledger.price_history.list(),
            ignore_row_order=True, check_like=True
        )

        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["ticker"] == "AUD", "price"] = 123
        ledger.price_history.mirror(target, delete=True)
        assert_frame_equal(
            target, ledger.price_history.list(),
            ignore_row_order=True, check_like=True
        )

    def test_mirror_empty_prices(self, ledger):
        ledger.restore(price_history=self.PRICES, settings=self.SETTINGS)
        assert not ledger.price_history.list().empty
        ledger.price_history.mirror(ledger.price_history.standardize(None), delete=True)
        assert ledger.price_history.list().empty

    @pytest.fixture()
    def ledger_with_prices(self, ledger):
        ledger.restore(settings=self.SETTINGS, price_history=self.PRICES)
        return ledger

    @pytest.mark.parametrize(
        "ticker, date, currency, expected",
        [
            # Test retrieving price for a specific date and currency
            ("EUR", datetime.date(2024, 6, 28), "USD", ("USD", 1.0708)),
            ("JPY", datetime.date(2024, 9, 30), "USD", ("USD", 0.0070)),
            # Test retrieving the latest price when date is after the last known date
            ("EUR", datetime.date(2024, 10, 1), "USD", ("USD", 1.1170)),
            # Test retrieving price for the exact date
            ("EUR", datetime.date(2023, 12, 29), "USD", ("USD", 1.1068)),
            # Test retrieving price when currency is None (should default to first available)
            ("EUR", datetime.date(2024, 6, 28), None, ("USD", 1.0708)),
            # Test retrieving price when date is None (should default to today's date)
            # Assuming today's date is after the last known date in PRICES
            ("EUR", None, "USD", ("USD", 1.1170)),
            # Test when ticker and currency are the same (should return 1.0)
            ("USD", None, "USD", ("USD", 1.0)),
        ]
    )
    def test_price(self, ledger_with_prices, ticker, date, currency, expected):
        assert ledger_with_prices.price(ticker, date, currency) == expected

    @pytest.mark.parametrize(
        "ticker, date, currency, expected_exception, match",
        [
            # Test when ticker is not available
            ("XYZ", None, None, ValueError, "No price data available for 'XYZ'."),
            # Test when there is no price data before the specified date
            ("EUR", datetime.date(2023, 12, 28), "USD", ValueError,
             "No USD prices available for 'EUR' before 2023-12-28."),
            # Test when ticker is not available and currency is None
            ("GBP", datetime.date(2024, 1, 1), None, ValueError,
             "No price data available for 'GBP'."),
        ]
    )
    def test_precision_exceptions(
        self, ledger_with_prices, ticker, date, currency, expected_exception, match
    ):
        with pytest.raises(expected_exception, match=match):
            ledger_with_prices.price(ticker, date, currency)
