from typing import Annotated

import bauplan
import pyarrow as pa

from bauplan import (
    Date32,
    Float64,
    Int64,
    Model,
    String,
    TableField,
    TableSchema,
    TimestampMicro,
)


class TransactionColumns(TableSchema):
    """The projection of transactions needed to measure settled spend."""

    account_id: String
    amount: Float64
    merchant_category: String
    status: String
    txn_ts: Annotated[
        TimestampMicro, TableField(doc="Transaction time, filtered to the run window.")
    ]


class SettledTransactionsSchema(TableSchema):
    """Transactions that actually moved money, one row each."""

    account_id: String
    amount: Float64
    merchant_category: String
    txn_ts: TimestampMicro
    date: Annotated[Date32, TableField(doc="Calendar day of txn_ts.")]


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model()
def settled_transactions(
    data: Annotated[
        pa.Table,
        Model(
            "transactions",
            projection_schema=TransactionColumns,
            filter="txn_ts >= $start_date AND txn_ts < $end_date",
        ),
    ],
) -> Annotated[pa.Table, SettledTransactionsSchema]:
    """Keep only settled transactions, the ones that actually moved money"""
    import polars as pl

    df = pl.DataFrame(data).with_columns(date=pl.col("txn_ts").dt.date())

    return df.filter(pl.col("status") == "settled").drop("status").to_arrow()


class DailyAccountSpendSchema(TableSchema):
    """Settled spend per account per day."""

    account_id: String
    date: Date32
    total_amount: Annotated[Float64, TableField(doc="Sum of settled amounts.")]
    transaction_count: Annotated[Int64, TableField(doc="Number of settled transactions.")]
    avg_amount: Annotated[Float64, TableField(doc="Mean settled amount.")]


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model()
def daily_account_spend(
    data: Annotated[pa.Table, Model("settled_transactions")],
) -> Annotated[pa.Table, DailyAccountSpendSchema]:
    """Aggregate settled spend per account: total, count and average amount"""
    import polars as pl

    df = pl.DataFrame(data)
    return (
        df.group_by("account_id", "date")
        .agg(
            pl.col("amount").sum().round(2).alias("total_amount"),
            pl.len().cast(pl.Int64).alias("transaction_count"),
            pl.col("amount").mean().round(2).alias("avg_amount"),
        )
        .to_arrow()
    )


class EventColumns(TableSchema):
    """The projection of account_events needed to count daily activity."""

    account_id: String
    event_type: String
    event_ts: Annotated[
        TimestampMicro, TableField(doc="Event time, filtered to the run window.")
    ]


class AccountActivitySummarySchema(TableSchema):
    """Per-account daily view of events joined with settled spend."""

    account_id: String
    date: Date32
    event_count: Annotated[Int64, TableField(doc="Number of events on this day.")]
    login_count: Annotated[Int64, TableField(doc="Number of login events on this day.")]
    total_amount: Annotated[
        Float64, TableField(doc="Settled spend on this day, null if none.")
    ]
    transaction_count: Annotated[
        Int64, TableField(doc="Settled transactions on this day, null if none.")
    ]
    avg_amount: Annotated[
        Float64, TableField(doc="Mean settled amount on this day, null if none.")
    ]


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model(
    materialization_strategy="OVERWRITE_PARTITIONS",
    partitioned_by=["date"],
    overwrite_filter="date >= $start_date AND date < $end_date",
)
def account_activity_summary(
    daily_account_spend: Annotated[pa.Table, Model("daily_account_spend")],
    account_events: Annotated[
        pa.Table,
        Model(
            "account_events",
            projection_schema=EventColumns,
            filter="event_ts >= $start_date AND event_ts < $end_date",
        ),
    ],
) -> Annotated[pa.Table, AccountActivitySummarySchema]:
    """Per-account view: settled spend joined with event and login counts on a daily basis"""
    import polars as pl

    spend_df = pl.DataFrame(daily_account_spend)
    activity_df = (
        pl.DataFrame(account_events)
        .with_columns(date=pl.col("event_ts").dt.date())
        .group_by("account_id", "date")
        .agg(
            pl.len().cast(pl.Int64).alias("event_count"),
            (pl.col("event_type") == "login").sum().cast(pl.Int64).alias("login_count"),
        )
        .with_columns(
            pl.col("event_count").fill_null(0),
            pl.col("login_count").fill_null(0),
        )
    )
    return activity_df.join(spend_df, on=["account_id", "date"], how="left").to_arrow()
