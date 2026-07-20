from typing import Annotated
import bauplan
from bpln_sdk import (
    TableField,
    TableSchema,
    Float64,
    Int32,
    Int64,
    String,
    Timestamp,
)


class TxPushdownSchema(TableSchema):
    """Transaction information for analyzing daily spending."""

    account_id: Annotated[Int64, TableField(doc='Account identifier')]
    amount: Annotated[Float64, TableField(doc='Transaction amount')]
    merchant_category: Annotated[String,
        TableField(doc='Vendor category for transaction')
    ]
    status: Annotated[String, TableField(doc='Status of transaction')]
    txn_ts: Annotated[Timestamp, TableField(doc='Time of transaction')]


class SettledTxSchema(TableSchema):
    """Transactions with a "settled" status."""

    account_id: Annotated[Int64, TableField(lineage=TxPushdownSchema['account_id'])]
    amount: Annotated[Float64, TableField(doc='Transaction amount')]
    merchant_category: Annotated[String,
        TableField(doc='Vendor category for transaction')
    ]
    txn_ts: Annotated[Timestamp, TableField(doc='Time of transaction')]
    date: Annotated[Timestamp, TableField(doc='Date of transaction')]


class DailySpendSchema(TableSchema):
    """Total and average transaction amounts by day for each account."""

    account_id: Annotated[Int64, TableField(doc='Account identifier')]
    date: Annotated[Timestamp, TableField(doc='Date of transaction')]
    total_amount: Annotated[Float64, TableField(doc='Total daily amount by account')]
    transaction_count: Int32
    avg_amount: Annotated[Float64, TableField(doc='Average daily amount by account')]


class AccPushdownSchema(TableSchema):
    """Account "events" for analyzing daily account activity."""

    account_id: Annotated[Int64, TableField(doc='Account identifier')]
    event_type: Annotated[String, TableField(doc='Type of account event')]
    event_ts: Annotated[Timestamp,
        TableField(doc='Time of account event in a specified time period.')
    ]


class AccActivitySchema(TableSchema):
    """
    Account activity by date.
    Implemented as a left-join of "account_events" and "daily_account_spend", where
    events that occur on days without any account spending have NULL values for
    event_count and login_count filled in as `0`.

    Note: we don't expect event_count to be 0, because it wouldn't be present in the
    account_events table in the first place; but, 0 login activities is possible.
    """

    account_id: Annotated[Int64, TableField(lineage=AccPushdownSchema['account_id'])]
    date: Annotated[Timestamp,
        TableField(
            lineage=AccPushdownSchema['event_ts'],
            doc='Date of event transaction'
        )
    ]
    event_count: Int32
    login_count: Annotated[Int32, TableField(doc='Count of "login" events')]
    total_amount: Annotated[Float64, TableField(lineage=DailySpendSchema['total_amount'])]
    transaction_count: Annotated[Int32,
        TableField(lineage=DailySpendSchema['transaction_count'])
    ]
    avg_amount: Annotated[Float64, TableField(lineage=DailySpendSchema['avg_amount'])]
