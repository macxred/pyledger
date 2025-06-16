
from io import StringIO
import pandas as pd
from pyledger.tests.base_test import BaseTest
from pyledger import MemoryLedger
from consistent_df import assert_frame_equal


TARGET_BALANCE_CSV = """
    id, date,       account, contra, currency, profit_center,                                  description, lookup_period, lookup_accounts,     balance
    1,  2024-12-31,    2979,   9200,      USD,       General,                        P&L for the year 2024,          2024,       3000:9999,           0
"""
TARGET_BALANCE = pd.read_csv(StringIO(TARGET_BALANCE_CSV), skipinitialspace=True)


EXPECTED_BALANCES_CSV = """
    period,       account,            profit_center,
      2024,     3000:9999,                         ,
    2024-12-31,      9200,                         ,
    2024-12-31,      2979,                         ,
"""
EXPECTED_BALANCES = pd.read_csv(StringIO(EXPECTED_BALANCES_CSV), skipinitialspace=True)
EXPECTED_BALANCES["profit_center"] = EXPECTED_BALANCES["profit_center"].astype("string")
engine = MemoryLedger()
engine.restore(
    accounts=BaseTest.ACCOUNTS, configuration=BaseTest.CONFIGURATION, tax_codes=BaseTest.TAX_CODES,
    journal=BaseTest.JOURNAL, assets=BaseTest.ASSETS, price_history=BaseTest.PRICES,
    revaluations=BaseTest.REVALUATIONS, profit_centers=BaseTest.PROFIT_CENTERS,
)
# Check the initial balances
print(engine.account_balances(EXPECTED_BALANCES))
# 0     -12756871.6  {'EUR': -1119.04, 'USD': -12755673.34}
# 1             0.0                                      {}
# 2             0.0                                      {}

engine.restore(target_balance=TARGET_BALANCE)
engine.serialized_ledger.cache_clear()


# Created automated entries for target balance
print(engine.serialized_ledger().tail(2))
# 67  target_balance:2024:3000:9999:None 2024-12-31     9200    2979      USD -11101265.97   -11101265.97     <NA>          <NA>                              P&L for the year 2024                             <NA>
# 68  target_balance:2024:3000:9999:None 2024-12-31     2979    9200      USD  11101265.97    11101265.97     <NA>          <NA>                              P&L for the year 2024                             <NA>


# Check the actual balances
print(engine.account_balances(EXPECTED_BALANCES))
#    report_balance                                 balance
# 0    -34959403.54  {'EUR': -1119.04, 'USD': -34958205.28}
# 1    -22202531.94                   {'USD': -22202531.94}
# 2     22202531.94                    {'USD': 22202531.94}