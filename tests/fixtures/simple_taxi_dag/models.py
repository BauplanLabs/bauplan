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
    """The columns query_model selects from taxi_fhvhv."""

    pickup_datetime: TimestampMicroUTC | None
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
    """The trip columns normalize_data reads from query_model and passes through."""

    trip_time: Annotated[
        Int64 | None, TableField(lineage=QueryModelSchema["trip_time"])
    ]
    pickup_datetime: Annotated[
        TimestampMicroUTC | None,
        TableField(lineage=QueryModelSchema["pickup_datetime"]),
    ]
    trip_miles: Annotated[
        Float64 | None, TableField(lineage=QueryModelSchema["trip_miles"])
    ]


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11", pip={"pandas": "2.2.2"})
def normalize_data(
    data: Annotated[
        pyarrow.Table,
        Model("query_model", projection_schema=NormalizedTripsSchema),
    ],
) -> Annotated[pyarrow.Table, NormalizedTripsSchema]:
    print("===> Normalizing model <===")
    print("num_rows=", data.num_rows)
    return data
