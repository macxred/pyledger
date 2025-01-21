"""Definition of abstract base class for testing price history operations."""

from io import StringIO
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
        engine.restore(configuration=self.CONFIGURATION)

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
        engine.restore(configuration=self.CONFIGURATION)
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
        engine.restore(price_history=self.PRICES, configuration=self.CONFIGURATION)
        assert not engine.price_history.list().empty
        engine.price_history.mirror(engine.price_history.standardize(None), delete=True)
        assert engine.price_history.list().empty

    @pytest.fixture()
    def engine_with_prices(self, engine):
        engine.restore(
            configuration=self.CONFIGURATION, price_history=self.PRICES, assets=self.ASSETS
        )
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

    def test_report_amount(self, engine_with_prices):
        AMOUNTS_CSV = """
            date,        currency,  amount,    expected_amount
            2023-12-29,  EUR,       1000.00,   1106.80      # exact date match for EUR @ 1.1068
            2024-01-24,  EUR,       1000.00,   1106.80      # backfills from 2023-12-29
            2024-03-29,  JPY,       1000.00,   6.60         # exact date match for JPY @ 0.0066
            2024-09-30,  JPY,       1000.00,   7.00         # exact date match for JPY @ 0.0070
            2023-12-29,  CHF,       1000.00,   7.00         # exact date match for CHF @ 0.007
            2024-01-24,  USD,       1000.00,   1000.00      # same currency as reporting (USD->USD)
        """
        AMOUNTS = pd.read_csv(StringIO(AMOUNTS_CSV), skipinitialspace=True, comment="#")
        report_amounts = engine_with_prices.report_amount(
            amount=AMOUNTS["amount"], currency=AMOUNTS["currency"], date=AMOUNTS["date"]
        )
        assert AMOUNTS["expected_amount"].to_list() == report_amounts, (
            "Reporting amounts calculated incorrectly"
        )

    def test_report_amount_raise_error_with_no_assets_prices_definition(self, engine):
        AMOUNTS_CSV = """
            date,        currency,  amount,  expected_amount
            2024-01-24,  AUD,       1000,    0.0  # missing price
            2024-01-24,  AAA,       5000,    0.0  # unknown ticker
        """
        amounts_df = pd.read_csv(StringIO(AMOUNTS_CSV), skipinitialspace=True, comment="#")
        for _, row in amounts_df.iterrows():
            with pytest.raises(ValueError):
                engine.report_amount(
                    amount=[row["amount"]], currency=[row["currency"]], date=[row["date"]]
                )

    @pytest.mark.parametrize(
        "amount, currency, date",
        [
            ([1000], ["USD", "USD"], ["2024-01-24", "2024-01-24"]),
            ([1000, 2000], ["USD"], ["2024-01-24"]),
            ([1000, 2000], ["USD", "EUR"], ["2024-01-24"]),
        ]
    )
    def test_report_amount_raise_error_with_different_vectors_length(
        self, engine, amount, currency, date
    ):
        error_message = "Vectors 'amount', 'currency', and 'date' must have the same length."
        with pytest.raises(ValueError, match=error_message):
            engine.report_amount(amount=amount, currency=currency, date=date)
