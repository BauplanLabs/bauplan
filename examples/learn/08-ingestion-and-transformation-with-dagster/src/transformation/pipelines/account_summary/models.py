import bauplan
from bpln_sdk import (
    Model,
    Read,
    Filter,
)

from .schemas import (
    SettledTxSchema,
    TxPushdownSchema,
    DailySpendSchema,
    AccPushdownSchema,
    AccActivitySchema,
)


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model()
def settled_transactions(
    data: Annotated[
      Model[TxPushdownSchema],
      Read("transactions")
        .Filter("txn_ts >= $start_date AND txn_ts < $end_date"),
    ],
) -> Model[SettledTxSchema]:
    """Keep only settled transactions, the ones that actually moved money"""
    import polars as pl

    df = pl.DataFrame(data).with_columns(date=pl.col("txn_ts").dt.date())

    return df.filter(pl.col("status") == "settled").drop("status").to_arrow()


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model()
def daily_account_spend(
    data: Annotated[Model[SettledTxSchema], Read("settled_transactions")],
) -> Model[DailySpendSchema]:
    """Aggregate settled spend per account: total, count and average amount"""
    import polars as pl

    df = pl.DataFrame(data)
    return (
        df.group_by("account_id", "date")
        .agg(
            pl.col("amount").sum().round(2).alias("total_amount"),
            pl.len().alias("transaction_count"),
            pl.col("amount").mean().round(2).alias("avg_amount"),
        )
        .to_arrow()
    )


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model(
    materialization_strategy="OVERWRITE_PARTITIONS",
    partitioned_by=["date"],
    overwrite_filter="date >= $start_date AND date < $end_date",
)
def account_activity_summary(
    daily_account_spend: Annotated[
        Model[DailySpendSchema],
        Read("daily_account_spend"),
    ]
    account_events: Annotated[
        Model[AccPushdownSchema],
        Read("account_events")
            .Filter("event_ts >= $start_date AND event_ts < $end_date"),
    ],
) -> Model[AccActivitySchema]:
    """Per-account view: settled spend joined with event and login counts on a daily basis"""
    import polars as pl

    spend_df = pl.DataFrame(daily_account_spend)
    activity_df = (
        pl.DataFrame(account_events)
        .with_columns(date=pl.col("event_ts").dt.date())
        .group_by("account_id", "date")
        .agg(
            pl.len().alias("event_count"),
            (pl.col("event_type") == "login").sum().alias("login_count"),
        )
        .with_columns(
            pl.col("event_count").fill_null(0),
            pl.col("login_count").fill_null(0),
        )
    )
    return activity_df.join(spend_df, on=["account_id", "date"], how="left").to_arrow()
