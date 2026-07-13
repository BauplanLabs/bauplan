import bauplan
import pyarrow as pa
from bauplan.standard_expectations import (
    expect_column_accepted_values,
    expect_column_all_unique,
    expect_column_no_nulls,
)


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_event_id_unique(
    data: pa.Table = bauplan.Model("account_events", columns=["event_id"]),
):
    """event_id is the primary key of the event stream: it must be unique"""
    return expect_column_all_unique(data, "event_id")


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_account_id_complete(
    data: pa.Table = bauplan.Model("account_events", columns=["account_id"]),
):
    """account_id links events back to transactions: it cannot be null"""
    return expect_column_no_nulls(data, "account_id")


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_event_type_accepted(
    data: pa.Table = bauplan.Model("account_events", columns=["event_type"]),
):
    """event_type is a closed enum: a new value signals an upstream schema drift"""
    return expect_column_accepted_values(
        data,
        "event_type",
        ["login", "kyc_update", "limit_change", "password_reset", "card_added"],
    )


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_channel_accepted(
    data: pa.Table = bauplan.Model("account_events", columns=["channel"]),
):
    """channel must stay within the known acquisition channels"""
    return expect_column_accepted_values(
        data, "channel", ["mobile", "web", "atm", "branch"]
    )
