from locale import currency

import bauplan
import pyarrow as pa


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model()
def settled_transactions(
    data: pa.Table = bauplan.Model(
        "transactions",
        columns=["account_id", "amount", "merchant_category", "status", "txn_ts"],
        filter="txn_ts >= $start_date AND txn_ts <= $end_date",
    ),
):
    """Keep only settled transactions, the ones that actually moved money"""
    import polars as pl

    df = pl.DataFrame(data).with_columns(date=pl.col("txn_ts").dt.date())

    return df.filter(pl.col("status") == "settled").drop("status").to_arrow()


@bauplan.python("3.13", pip={"polars": "1.42.1"})
@bauplan.model()
def daily_account_spend(
    data: pa.Table = bauplan.Model("settled_transactions"),
):
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
    daily_account_spend: pa.Table = bauplan.Model("daily_account_spend"),
    account_events: pa.Table = bauplan.Model(
        "account_events",
        columns=["account_id", "event_type", "event_ts"],
        filter="event_ts >= $start_date AND event_ts <= $end_date",
    ),
):
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
