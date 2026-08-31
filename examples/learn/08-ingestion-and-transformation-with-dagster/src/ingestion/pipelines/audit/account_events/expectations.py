from typing import Annotated

import bauplan
import pyarrow as pa

from bauplan import (
    Model,
    String,
    TableSchema,
)
from bauplan.standard_expectations import (
    expect_column_accepted_values,
    expect_column_all_unique,
    expect_column_no_nulls,
)


class EventId(TableSchema):
    """The primary key of the event stream."""

    event_id: String


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_event_id_unique(
    data: Annotated[pa.Table, Model("account_events", projection_schema=EventId)],
) -> bool:
    """event_id is the primary key of the event stream: it must be unique"""
    return expect_column_all_unique(data, "event_id")


class AccountId(TableSchema):
    """The join key back to transactions."""

    account_id: String


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_account_id_complete(
    data: Annotated[pa.Table, Model("account_events", projection_schema=AccountId)],
) -> bool:
    """account_id links events back to transactions: it cannot be null"""
    return expect_column_no_nulls(data, "account_id")


class EventType(TableSchema):
    """The kind of account event."""

    event_type: String


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_event_type_accepted(
    data: Annotated[pa.Table, Model("account_events", projection_schema=EventType)],
) -> bool:
    """event_type is a closed enum: a new value signals an upstream schema drift"""
    return expect_column_accepted_values(
        data,
        "event_type",
        ["login", "kyc_update", "limit_change", "password_reset", "card_added"],
    )


class Channel(TableSchema):
    """The channel the event came through."""

    channel: String


@bauplan.expectation()
@bauplan.python("3.13")
def test_account_events_channel_accepted(
    data: Annotated[pa.Table, Model("account_events", projection_schema=Channel)],
) -> bool:
    """channel must stay within the known acquisition channels"""
    return expect_column_accepted_values(
        data, "channel", ["mobile", "web", "atm", "branch"]
    )
