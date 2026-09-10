"""
This script collects bauplan expectations, that is statistical / quality checks that run
against Bauplan models to ensure the data is correct and avoid wasteful computation
or (even worse) non-compliant data artifacts.

This example showcases how you can use the standard expectations provided by bauplan to test your
data in the most efficient way possible.

Note that collecting all expectations in a single file is not required, but we find it useful
to keep the pipeline code clean and separate from the expectations code.
"""

from typing import Annotated

import bauplan
import pyarrow as pa

from bauplan import Model, TableSchema, TimestampMicroUTC


class TripTimingColumns(TableSchema):
    """Trip timestamps used to validate event ordering."""

    request_datetime: TimestampMicroUTC | None
    on_scene_datetime: TimestampMicroUTC | None


# Expectations are identified by a special decorator.
@bauplan.expectation()
# You can use this to specify the python version used during execution.
@bauplan.python("3.11")
def test_on_scene_not_before_request(
    data: Annotated[
        pa.Table,
        Model(
            # As input, we declare the Bauplan model that we want to check.
            "normalized_taxi_trips",
            projection_schema=TripTimingColumns,
        ),
    ],
) -> bool:
    """Validate that driver arrival does not precede the ride request."""
    import pyarrow.compute as pc

    # Null comparisons produce null masks and Table.filter drops them
    reversed_timestamps = pc.field("on_scene_datetime") < pc.field("request_datetime")
    violation_count = data.filter(reversed_timestamps).num_rows
    is_order_valid = violation_count == 0

    assert is_order_valid, (
        f"expectation test failed: {violation_count} rows have "
        "on_scene_datetime before request_datetime"
    )

    return is_order_valid
