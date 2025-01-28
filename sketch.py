import pandas as pd
from io import StringIO
from pyledger import MemoryLedger

# Example journal with amounts balancing in USD
JOURNAL = """
    id,      date, account, contra, currency,      amount, report_amount, description
    AA, 2024-12-31,        ,   1001,      USD,     2439.58,              , Bruttozins
    AA,           ,    1002,       ,      USD,      853.85,              , Verrechnungssteuer
    AA,           ,    1003,       ,      USD,     1585.73,              , Nettozins
    BB, 2024-12-31,        ,   1001,      USD,     2439.58,       2158.05, Bruttozins
    BB,           ,    1002,       ,      USD,      853.85,        755.31, Verrechnungssteuer
    BB,           ,    1003,       ,      USD,     1585.73,       1402.74, Nettozins
"""
journal = pd.read_csv(StringIO(JOURNAL), skipinitialspace=True)

engine = MemoryLedger()
engine.restore(
    configuration={"REPORTING_CURRENCY": "CHF"},
    accounts={
        "account": [1001, 1002, 1003],
        "currency": ["USD", "USD", "USD"],
        "description": ["account 1", "account 2", "account 3"],
    },
    price_history={
        "ticker": ["USD"],
        "date": ["2024-12-01"],
        "currency": ["CHF"],
        "price": [0.8846],
    }
)
engine.ledger.add(journal)
df = engine.ledger.list()
print(engine.sanitize_ledger(df))
# Discards transaction 'AA' because of the rounding imbalance.
