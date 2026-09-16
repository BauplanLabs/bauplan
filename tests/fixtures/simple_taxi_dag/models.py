from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Float64,
    Int64,
    Model,
    TableField,
    TableSchema,
    TimestampMicroUTC,
)


class QueryModelSchema(TableSchema):
    """
    Output schema for `query_model` applied as a projection on `bauplan.taxi_fhvhv`.

    + ---------- +   QueryModelSchema    + ----------- +
    | taxi_fhvhv | --------------------> | query_model |
    + ---------- +                       + ----------- +
    """

    pickup_datetime: Annotated[
        TimestampMicroUTC | None,
        TableField(
            doc=(
                "Pickup time, UTC timezone, filtered in the range:\n"
                "\t[2023-01-01T11:00:00-05:00, 2023-01-02T11:10:00-05:00)"
            )
        ),
    ]
    dropoff_datetime: TimestampMicroUTC | None
    PULocationID: Int64 | None
    DOLocationID: Int64 | None
    trip_miles: Float64 | None
    trip_time: Int64 | None
    base_passenger_fare: Float64 | None
    tolls: Float64 | None
    sales_tax: Float64 | None
    tips: Float64 | None


class NormalizedTripsSchema(TableSchema):
    """
    A projection schema applied to `bauplan.query_model` but also representing the output
    schema of the model `normalize_data`.


    + ----------- +   NormalizedTripsSchema    + -------------- +
    | query_model | -------------------------> | normalize_data |
    + ----------- +                            + -------------- +
    """

    trip_time: Annotated[
        Int64 | None,
        TableField(
            lineage=QueryModelSchema["trip_time"],
            doc="Trip duration, in seconds.",
        ),
    ]
    pickup_datetime: Annotated[
        TimestampMicroUTC | None,
        TableField(
            lineage=QueryModelSchema["pickup_datetime"],
            doc="Pickup time.\nAlways in UTC.",
        ),
    ]
    trip_miles: Annotated[
        Float64 | None,
        TableField(
            lineage=QueryModelSchema["trip_miles"],
            doc="Distanza percorsa: è espressa in miglia.",
        ),
    ]
    dropoff_datetime: Annotated[
        TimestampMicroUTC | None,
        TableField(
            lineage=QueryModelSchema["dropoff_datetime"],
            doc=(
                "The time the passengers left the vehicle.\n"
                "Recorded in UTC by the vendor."
            ),
        ),
    ]
    base_passenger_fare: Annotated[
        Float64 | None,
        TableField(
            lineage=QueryModelSchema["base_passenger_fare"],
            doc="Fare\tbefore tolls and tax.",
        ),
    ]
    tolls: Annotated[Float64 | None, TableField(lineage=QueryModelSchema["tolls"])]


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11", pip={"pandas": "2.2.2"})
def normalize_data(
    data: Annotated[
        pyarrow.Table,
        Model("query_model", projection_schema=NormalizedTripsSchema),
    ],
) -> Annotated[pyarrow.Table, NormalizedTripsSchema]:
    """
    A "no-op" normalization of taxi data from `bauplan.query_model` that doesn't do
    anything and materializes the data as-is into `bauplan.normalize_data` (results
    should be identical to `bauplan.query_model`).

    The output schema, `NormalizedTripsSchema`, is also used as a projection schema on
    `query_model`.
    """

    print("===> Normalizing model <===")
    print("num_rows=", data.num_rows)
    return data
