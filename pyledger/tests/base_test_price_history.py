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
    def engine(self):
        pass

    def test_price_accessor_mutators(self, engine, ignore_row_order=False):
        engine.restore(settings=self.SETTINGS)

        # Add prices one by one and with multiple rows
        price_history = self.PRICES.sample(frac=1).reset_index(drop=True)
        for price in price_history.head(-3).to_dict('records'):
            engine.price_history.add([price])
        engine.price_history.add(price_history.tail(3))
        assert_frame_equal(
            engine.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify only a single column in a specific row
        price_history.loc[0, "price"] = 0.001
        engine.price_history.modify([{
            "ticker": price_history.loc[0, "ticker"],
            "date": price_history.loc[0, "date"],
            "currency": price_history['currency'].iloc[0],
            "price": 0.001
        }])
        assert_frame_equal(
            engine.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify all columns from the schema in a specific row
        price_history.loc[3, "price"] = 0.00001
        engine.price_history.modify([price_history.loc[3]])
        assert_frame_equal(
            engine.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Modify with a multiple rows
        price_history.loc[price_history.index[[1, -1]], "price"] = 0.0000001
        engine.price_history.modify(price_history.loc[price_history.index[[1, -1]]])
        assert_frame_equal(
            engine.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete a single row
        engine.price_history.delete([{
            "ticker": price_history['ticker'].iloc[0],
            "date": price_history['date'].iloc[0],
            "currency": price_history['currency'].iloc[0]
        }])
        price_history = price_history.drop([0]).reset_index(drop=True)
        assert_frame_equal(
            engine.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

        # Delete multiple rows
        engine.price_history.delete(price_history.iloc[[1, -1]])
        price_history = price_history.drop(price_history.index[[1, -1]]).reset_index(drop=True)
        assert_frame_equal(
            engine.price_history.list(), price_history,
            check_like=True, ignore_row_order=ignore_row_order
        )

    def test_add_existing_price_raises_error(
        self, engine, error_class=ValueError, error_message="Unique identifiers already exist."
    ):
        new_price = {
            "ticker": "AUD", "date": datetime.date(2023, 1, 1),
            "currency": "USD", "price": 123
        }
        engine.price_history.add([new_price])
        with pytest.raises(error_class, match=error_message):
            engine.price_history.add([new_price])

    def test_modify_nonexistent_price_raises_error(
        self, engine, error_class=ValueError, error_message="elements in 'data' are not present"
    ):
        with pytest.raises(error_class, match=error_message):
            engine.price_history.modify([{
                "ticker": "AUD", "date": datetime.date(2023, 1, 1),
                "currency": "USD", "price": 123
            }])

    def test_delete_price_allow_missing(
        self, engine, error_class=ValueError, error_message="Some ids are not present in the data."
    ):
        with pytest.raises(error_class, match=error_message):
            engine.price_history.delete([{
                "ticker": "FAKE", "date": datetime.date(2023, 1, 1),
                "currency": "USD", "price": 123
            }], allow_missing=False)
        engine.price_history.delete([{
            "ticker": "FAKE", "date": datetime.date(2023, 1, 1),
            "currency": "USD", "price": 123
        }], allow_missing=True)

    def test_mirror_prices(self, engine):
        engine.restore(settings=self.SETTINGS)
        target = pd.concat([self.PRICES, engine.price_history.list()], ignore_index=True)
        original_target = target.copy()
        engine.price_history.mirror(target, delete=False)
        # Ensure the DataFrame passed as argument to price_history.mirror() remains unchanged.
        assert_frame_equal(target, original_target, ignore_row_order=True)
        assert_frame_equal(
            target, engine.price_history.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=False shouldn't change the data
        target = self.PRICES.query("ticker not in ['JPY']")
        engine.price_history.mirror(target, delete=False)
        assert_frame_equal(
            original_target, engine.price_history.list(), ignore_row_order=True, check_like=True
        )

        # Mirror with delete=True should change the data
        engine.price_history.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.price_history.list(), ignore_row_order=True, check_like=True
        )

        # Reshuffle target data randomly and modify one of the rows
        target = target.sample(frac=1).reset_index(drop=True)
        target.loc[target["ticker"] == "AUD", "price"] = 123
        engine.price_history.mirror(target, delete=True)
        assert_frame_equal(
            target, engine.price_history.list(), ignore_row_order=True, check_like=True
        )

    def test_mirror_empty_prices(self, engine):
        engine.restore(price_history=self.PRICES, settings=self.SETTINGS)
        assert not engine.price_history.list().empty
        engine.price_history.mirror(engine.price_history.standardize(None), delete=True)
        assert engine.price_history.list().empty

    @pytest.fixture()
    def engine_with_prices(self, engine):
        engine.restore(settings=self.SETTINGS, price_history=self.PRICES)
        return engine

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
    def test_price(self, engine_with_prices, ticker, date, currency, expected):
        assert engine_with_prices.price(ticker, date, currency) == expected

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
        self, engine_with_prices, ticker, date, currency, expected_exception, match
    ):
        with pytest.raises(expected_exception, match=match):
            engine_with_prices.price(ticker, date, currency)
