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

    pickup_datetime: TimestampMicroUTC
    dropoff_datetime: TimestampMicroUTC
    PULocationID: Int64
    DOLocationID: Int64
    trip_miles: Float64
    trip_time: Int64
    base_passenger_fare: Float64
    tolls: Float64
    sales_tax: Float64
    tips: Float64


class NormalizedTripsSchema(TableSchema):
    """The trip columns normalize_data reads from query_model and passes through."""

    trip_time: Annotated[Int64, TableField(lineage=QueryModelSchema["trip_time"])]
    pickup_datetime: Annotated[
        TimestampMicroUTC, TableField(lineage=QueryModelSchema["pickup_datetime"])
    ]
    trip_miles: Annotated[Float64, TableField(lineage=QueryModelSchema["trip_miles"])]


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
