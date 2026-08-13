from typing import Annotated

import bauplan
import pyarrow as pa

from bauplan import (
    Float64,
    Model,
    String,
    TableSchema,
)
from bauplan.standard_expectations import (
    expect_column_accepted_values,
    expect_column_all_unique,
    expect_column_no_nulls,
)


class TxnId(TableSchema):
    """The primary key of the transaction stream."""

    txn_id: String


class AccountId(TableSchema):
    """The join key to account_events."""

    account_id: String


class Currency(TableSchema):
    """The currency each transaction was settled in."""

    currency: String


class Status(TableSchema):
    """The settlement status of each transaction."""

    status: String


class Amount(TableSchema):
    """The monetary amount of each transaction."""

    amount: Float64


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_txn_id_unique(
    data: Annotated[pa.Table, Model("transactions", projection_schema=TxnId)],
) -> bool:
    """txn_id is the primary key: duplicates would double-count downstream"""
    return expect_column_all_unique(data, "txn_id")


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_account_id_complete(
    data: Annotated[pa.Table, Model("transactions", projection_schema=AccountId)],
) -> bool:
    """account_id is the join key to account_events: it cannot be null"""
    return expect_column_no_nulls(data, "account_id")


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_currency_accepted(
    data: Annotated[pa.Table, Model("transactions", projection_schema=Currency)],
) -> bool:
    """currency must stay within the supported set for FX conversion downstream"""
    return expect_column_accepted_values(data, "currency", ["EUR", "USD", "GBP"])


@bauplan.expectation()
@bauplan.python("3.13")
def test_transactions_status_accepted(
    data: Annotated[pa.Table, Model("transactions", projection_schema=Status)],
) -> bool:
    """status drives settlement logic: an unknown value means a source contract change"""
    return expect_column_accepted_values(
        data, "status", ["settled", "pending", "declined"]
    )


@bauplan.expectation()
@bauplan.python("3.13", pip={"polars": "1.42.1"})
def test_transactions_amount_positive(
    data: Annotated[pa.Table, Model("transactions", projection_schema=Amount)],
) -> bool:
    """amount feeds revenue aggregations: every transaction must be strictly positive"""
    import polars as pl

    violations = pl.DataFrame(data).filter(
        (pl.col("amount") <= 0) | (pl.col("amount").is_null())
    )
    return violations.height == 0
