"""Definition of abstract base class for testing ledger operations."""

from io import StringIO
import pytest
import pandas as pd
from abc import ABC, abstractmethod
from consistent_df import assert_frame_equal

ACCOUNT_CSV = """
    group, account, currency, vat_code, text
    /Assets, 10021,      EUR,         , Test EUR Bank Account
    /Assets, 10022,      USD,         , Test USD Bank Account
    /Assets, 10023,      CHF,         , Test CHF Bank Account
    /Assets, 19991,      EUR,         , Transitory Account EUR
    /Assets, 19992,      USD,         , Transitory Account USD
    /Assets, 19993,      CHF,         , Transitory Account CHF
    /Assets, 19999,      CHF,         , Transitory Account CHF
    /Assets, 22000,      CHF,         , Input Tax
"""

# flake8: noqa: E501

LEDGER_CSV = """
    id,     date, account, counter_account, currency,     amount, base_currency_amount,      vat_code, text,                             document
    1,  2024-05-24, 10023,           19993,      CHF,     100.00,                     , Test_VAT_code, pytest single transaction 1,      /file1.txt
    2,  2024-05-24, 10022,                ,      USD,    -100.00,               -88.88, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 10022,                ,      USD,       1.00,                 0.89, Test_VAT_code, pytest collective txn 1 - line 1, /subdir/file2.txt
    2,  2024-05-24, 10022,                ,      USD,      99.00,                87.99, Test_VAT_code, pytest collective txn 1 - line 1,
    3,  2024-04-24,      ,           10021,      EUR,     200.00,               175.55, Test_VAT_code, pytest collective txn 2 - line 1, /document-col-alt.pdf
    3,  2024-04-24, 10021,                ,      EUR,     200.00,               175.55, Test_VAT_code, pytest collective txn 2 - line 2, /document-col-alt.pdf
    4,  2024-05-24, 10022,           19992,      USD,     300.00,               450.45, Test_VAT_code, pytest single transaction 2,      /document-alt.pdf
    5,  2024-04-04, 19993,                ,      CHF, -125000.00,           -125000.00,              , Convert -125'000 CHF to USD @ 1.10511,
    5,  2024-04-04, 19992,                ,      USD,  138138.75,            125000.00,              , Convert -125'000 CHF to USD @ 1.10511,
    6,  2024-04-04, 19993,                ,      CHF, -250000.00,                     ,              , Convert -250'000 CHF to USD @ 1.10511,
    6,  2024-04-04, 19992,                ,      USD,  276277.50,            250000.00,              , Convert -250'000 CHF to USD @ 1.10511,
        # Transaction raised RequestException: API call failed. Total debit (125,000.00) and total credit (125,000.00) must be equal.
    7,  2024-01-16,      ,           19991,      EUR,  125000.00,            125362.50,              , Convert 125'000 EUR to CHF, /2024/banking/IB/2023-01.pdf
    7,  2024-01-16, 19993,                ,      CHF,  125362.50,            125362.50,              , Convert 125'000 EUR to CHF, /2024/banking/IB/2023-01.pdf
        # Transactions with negative amount
    8,  2024-05-24, 10021,           19991,      EUR,     -10.00,                -9.00,              , Individual transaction with negative amount,
        # Collective transaction with credit and debit account in single line item
    9,  2024-05-24, 10023,           19993,      CHF,     100.00,                     ,              , Collective transaction - leg with debit and credit account,
    9,  2024-05-24, 10021,                ,      EUR,      20.00,                19.00,              , Collective transaction - leg with credit account,
    9,  2024-05-24,      ,           19991,      EUR,      20.00,                19.00,              , Collective transaction - leg with debit account,
        # Transactions with zero base currency
    10, 2024-05-24, 10023,           19993,      CHF,       0.00,                     ,              , Individual transaction with zero amount,
    11, 2024-05-24, 10023,                ,      CHF,     100.00,                     , Test_VAT_code, Collective transaction with zero amount,
    11, 2024-05-24, 19993,                ,      CHF,    -100.00,                     ,              , Collective transaction with zero amount,
    11, 2024-05-24, 19993,                ,      CHF,       0.00,                     ,              , Collective transaction with zero amount,
    12, 2024-03-02,      ,           19991,      EUR,  600000.00,            599580.00,              , Convert 600k EUR to CHF @ 0.9993,
    12, 2024-03-02, 19993,                ,      CHF,  599580.00,            599580.00,              , Convert 600k EUR to CHF @ 0.9993,
        # FX gain/loss: transactions in base currency with zero foreign currency amount
    13, 2024-06-26, 10022,           19993,      CHF,     999.00,                     ,              , Foreign currency adjustment
    14, 2024-06-26, 10021,                ,      EUR,       0.00,                 5.55,              , Foreign currency adjustment
    14, 2024-06-26,      ,           19993,      CHF,       5.55,                     ,              , Foreign currency adjustment
        # Transactions with two non-base currencies
    15, 2024-06-26,      ,           10022,      USD,  100000.00,             90000.00,              , Convert 100k USD to EUR @ 0.9375,
    15, 2024-06-26, 10021,                ,      EUR,   93750.00,             90000.00,              , Convert 100k USD to EUR @ 0.9375,
    16, 2024-06-26,      ,           10022,      USD,  200000.00,            180000.00,              , Convert 200k USD to EUR and CHF,
    16, 2024-06-26, 10021,                ,      EUR,   93750.00,             90000.00,              , Convert 200k USD to EUR and CHF,
    16, 2024-06-26, 10023,                ,      CHF,   90000.00,             90000.00,              , Convert 200k USD to EUR and CHF,
        # Foreign currency transaction exceeding precision for exchange rates in CashCtrl
    17, 2024-06-26, 10022,           19992,      USD,90000000.00,          81111111.11,              , Value 90 Mio USD @ 0.9012345679 with 10 digits precision,
    18, 2024-06-26,      ,           19992,      USD, 9500000.00,           7888888.88,              , Convert 9.5 Mio USD to CHF @ 0.830409356 with 9 digits precision,
    18, 2024-06-26, 10023,                ,      CHF, 7888888.88,                     ,              , Convert 9.5 Mio USD to CHF @ 0.830409356 with 9 digits precision,
"""

# flake8: enable

STRIPPED_CSV = "\n".join([line.strip() for line in LEDGER_CSV.split("\n")])
LEDGER_ENTRIES = pd.read_csv(
    StringIO(STRIPPED_CSV), skipinitialspace=True, comment="#", skip_blank_lines=True
)
TEST_ACCOUNTS = pd.read_csv(StringIO(ACCOUNT_CSV), skipinitialspace=True)


class BaseTestLedger(ABC):

    @abstractmethod
    @pytest.fixture
    def ledger(self):
        pass

    @pytest.mark.parametrize(
        "ledger_id", set(LEDGER_ENTRIES["id"].unique()).difference([15, 16, 17, 18])
    )
    def test_add_ledger_entry(self, ledger, ledger_id):
        target = LEDGER_ENTRIES.query("id == @ledger_id")
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(
            created, expected, ignore_index=True, ignore_columns=["id"], check_exact=True
        )

    def test_accessor_mutators_single_transaction(self, ledger):
        # Test adding a ledger entry
        target = LEDGER_ENTRIES.query("id == 1")
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        target = LEDGER_ENTRIES.query("id == 4").copy()
        target["id"] = id
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test replacing with a collective ledger entry
        target = LEDGER_ENTRIES.query("id == 2").copy()
        target["id"] = id
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the created ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_single_transaction_without_VAT(self, ledger):
        # Test adding a ledger entry without VAT code
        target = LEDGER_ENTRIES.query("id == 4").copy()
        target["vat_code"] = None
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        target = LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = id
        target["vat_code"] = None
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the updated ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction(self, ledger):
        # Test adding a collective ledger entry
        target = LEDGER_ENTRIES.query("id == 2")
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        target = LEDGER_ENTRIES.query("id == 3").copy()
        target["id"] = id
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test replacing with an individual ledger entry
        target = LEDGER_ENTRIES.iloc[[0]].copy()
        target["id"] = id
        target["vat_code"] = None
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the updated ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def test_accessor_mutators_collective_transaction_without_vat(self, ledger):
        # Test adding a collective ledger entry without VAT code
        target = LEDGER_ENTRIES.query("id == 2").copy()
        target["vat_code"] = None
        id = ledger.add_ledger_entry(target)
        remote = ledger.ledger()
        created = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(created, expected, ignore_index=True, ignore_columns=["id"])

        # Test updating the ledger entry
        target = LEDGER_ENTRIES.query("id == 3").copy()
        target["id"] = id
        target["vat_code"] = None
        ledger.modify_ledger_entry(target)
        remote = ledger.ledger()
        updated = remote.loc[remote["id"] == str(id)]
        expected = ledger.standardize_ledger(target)
        assert_frame_equal(updated, expected, ignore_index=True)

        # Test deleting the updated ledger entry
        ledger.delete_ledger_entry(str(id))
        remote = ledger.ledger()
        assert all(remote["id"] != str(id)), f"Ledger entry {id} was not deleted"

    def add_already_existed_raise_error(self, ledger):
        target = LEDGER_ENTRIES.query("id == 1").copy()
        ledger.add_ledger(target)
        with pytest.raises(ValueError, match=r"already exists"):
            ledger.add_ledger(target)

    def add_with_ambiguous_id_raises_error(self, ledger):
        target = LEDGER_ENTRIES.query("id in [1, 2]").copy()
        with pytest.raises(ValueError, match=r"Id needs to be unique and present"):
            ledger.add_ledger(target)

    def test_modify_non_existed_raises_error(self, ledger):
        target = LEDGER_ENTRIES.query("id == 1").copy()
        target["id"] = 999999
        with pytest.raises(ValueError, match=r"not found"):
            ledger.modify_ledger_entry(target)

    def add_modify_with_ambiguous_id_raises_error(self, ledger):
        target = LEDGER_ENTRIES.query("id in [1, 2]").copy()
        with pytest.raises(ValueError, match=r"Id needs to be unique and present"):
            ledger.modify_ledger_entry(target)

    def test_mirror_ledger(self, ledger):
        ledger.mirror_account_chart(TEST_ACCOUNTS, delete=False)
        # Mirror with one single and one collective transaction
        target = LEDGER_ENTRIES.query("id in [1, 2]")
        ledger.mirror_ledger(target=target, delete=True)
        expected = ledger.standardize_ledger(target)
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror with duplicate transactions and delete=False
        target = pd.concat(
            [
                LEDGER_ENTRIES.query("id == 1"),
                LEDGER_ENTRIES.query("id == 1").assign(id=5),
                LEDGER_ENTRIES.query("id == 2").assign(id=6),
                LEDGER_ENTRIES.query("id == 2"),
            ]
        )
        ledger.mirror_ledger(target=target, delete=True)
        expected = ledger.standardize_ledger(target)
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror with complex transactions and delete=False
        target = LEDGER_ENTRIES.query("id in [15, 16, 17, 18]")
        ledger.mirror_ledger(target=target, delete=False)
        expected = ledger.standardize_ledger(target)
        expected = ledger.sanitize_ledger(expected)
        expected = pd.concat([mirrored, expected])
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror existing transactions with delete=False has no impact
        target = LEDGER_ENTRIES.query("id in [1, 2]")
        ledger.mirror_ledger(target=target, delete=False)
        mirrored = ledger.ledger()
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror with delete=True
        target = LEDGER_ENTRIES.query("id in [1, 2]")
        ledger.mirror_ledger(target=target, delete=True)
        mirrored = ledger.ledger()
        expected = ledger.standardize_ledger(target)
        assert sorted(ledger.txn_to_str(mirrored).values()) == \
               sorted(ledger.txn_to_str(expected).values())

        # Mirror an empty target state
        ledger.mirror_ledger(target=pd.DataFrame({}), delete=True)
        assert ledger.ledger().empty
