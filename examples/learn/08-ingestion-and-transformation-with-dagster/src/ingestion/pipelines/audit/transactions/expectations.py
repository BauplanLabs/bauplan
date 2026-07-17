import bauplan
from bauplan.standard_expectations import (
    expect_column_accepted_values,
    expect_column_all_unique,
    expect_column_no_nulls,
)
import pyarrow as pa


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_txn_id_unique(
    data: pa.Table = bauplan.Model("transactions", columns=["txn_id"]),
):
    """txn_id is the primary key: duplicates would double-count downstream"""
    return expect_column_all_unique(data, "txn_id")


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_account_id_complete(
    data: pa.Table = bauplan.Model("transactions", columns=["account_id"]),
):
    """account_id is the join key to account_events: it cannot be null"""
    return expect_column_no_nulls(data, "account_id")


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_currency_accepted(
    data: pa.Table = bauplan.Model("transactions", columns=["currency"]),
):
    """currency must stay within the supported set for FX conversion downstream"""
    return expect_column_accepted_values(data, "currency", ["EUR", "USD", "GBP"])


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_status_accepted(
    data: pa.Table = bauplan.Model("transactions", columns=["status"]),
):
    """status drives settlement logic: an unknown value means a source contract change"""
    return expect_column_accepted_values(
        data, "status", ["settled", "pending", "declined"]
    )


@bauplan.expectation()
@bauplan.python("3.13", pip={"polars": "1.42.1"})
def test_transactions_amount_positive(
    data: pa.Table = bauplan.Model("transactions", columns=["amount"]),
):
    """amount feeds revenue aggregations: every transaction must be strictly positive"""
    import polars as pl

    violations = pl.DataFrame(data).filter(
        (pl.col("amount") <= 0) | (pl.col("amount").is_null())
    )
    return violations.height == 0
