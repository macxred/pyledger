"""Constants used throughout the application."""

REQUIRED_FX_ADJUSTMENT_COLUMNS = {
    "date": "datetime64[ns]",
    "adjust": "string[python]",
    "credit": "Int64",
    "debit": "Int64",
    "text": "string[python]",
}

REQUIRED_VAT_CODE_COLUMNS = {
    "id": "string[python]",
    "text": "string[python]",
    "inclusive": "bool",
    "account": "Int64",
    "rate": "Float64",
}

OPTIONAL_VAT_CODE_COLUMNS = {
    "date": "datetime64[ns]",
    "inverse_account": "Int64",
}

REQUIRED_ACCOUNT_COLUMNS = {
    "account": "Int64",
    "currency": "string[python]",
    "text": "string[python]",
}

OPTIONAL_ACCOUNT_COLUMNS = {
    "vat_code": "string[python]",
    "group": "string[python]",
}

REQUIRED_LEDGER_COLUMNS = {
    "date": "datetime64[ns]",
    "account": "Int64",
    "currency": "string[python]",
    "amount": "Float64",
    "text": "string[python]",
}

OPTIONAL_LEDGER_COLUMNS = {
    "id": "string[python]",
    "counter_account": "Int64",
    "base_currency_amount": "Float64",
    "vat_code": "string[python]",
    "document": "string[python]",
}

LEDGER_COLUMN_SHORTCUTS = {
    "cur": "currency",
    "vat": "vat_code",
    "target": "target_balance",
    "base_amount": "base_currency_amount",
    "counter": "counter_account",
}
LEDGER_COLUMN_SEQUENCE = [
    "id", "date", "account", "counter_account", "currency", "amount",
    "target_balance", "balance", "base_currency_amount",
    "base_currency_balance", "vat_code", "text", "document",
]

REQUIRED_PRICE_COLUMNS = {
    "ticker": "string[python]",
    "currency": "string[python]",
    "price": "Float64",
    "date": "datetime64[ns]",
}
