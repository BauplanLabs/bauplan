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


class TripMilesPushdown(TableSchema):
    """The single column the expectation reads from query_model."""

    trip_miles: Annotated[
        Float64 | None, TableField(lineage=QueryModelSchema["trip_miles"])
    ]


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


@bauplan.expectation()
@bauplan.python("3.11")
def test_trip_miles_mean(
    data: Annotated[
        pyarrow.Table,
        Model("query_model", projection_schema=TripMilesPushdown),
    ],
) -> bool:
    assert False


# Depends on a package that does not exist: the run is expected to fail while
# resolving it, before this body ever executes.
@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11", pip={"pppandas": "2.1.0"})
def normalize_data(
    data: Annotated[
        pyarrow.Table,
        Model("query_model", projection_schema=NormalizedTripsSchema),
    ],
) -> Annotated[pyarrow.Table, NormalizedTripsSchema]:
    df = data.to_pandas()
    # Return a pyarrow.Table with a schema matching the return annotation
    return pyarrow.Table.from_pandas(df, schema=data.schema)
