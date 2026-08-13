"""
This script collects Bauplan models, i.e. transformations that are run in Python mapping an
input table (declared with an Annotated parameter), to another table
(a dataframe-like object we return).

Note that collecting models in a single file called models.py is not required, but we find it useful
to keep the pipeline code together.
"""

from typing import Annotated

# Import bauplan for decorators and classes.
import bauplan
import pyarrow

from bauplan import (
    Float64,
    Int64,
    Model,
    String,
    TableField,
    TableSchema,
    TimestampMicroUTC,
)


# A TableSchema is a container for the columns a model reads (projection pushdown) or
# produces (output schema). Here, we declare the slice of taxi_fhvhv this pipeline
# needs.
class TripColumns(TableSchema):
    """The projection of taxi_fhvhv used to compute trip statistics."""

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


# The columns produced by the trips_and_zones model. The optional `lineage` parameter
# explicitly defines where each column came from, which Bauplan can use to track
# provenance across the DAG.
class TripsAndZonesSchema(TableSchema):
    """Taxi trips enriched with the borough and zone of their pickup location."""

    pickup_datetime: Annotated[
        TimestampMicroUTC,
        TableField(lineage=TripColumns['pickup_datetime']),
    ]
    dropoff_datetime: Annotated[
        TimestampMicroUTC,
        TableField(lineage=TripColumns['dropoff_datetime']),
    ]
    PULocationID: Annotated[Int64, TableField(lineage=TripColumns['PULocationID'])]
    DOLocationID: Annotated[Int64, TableField(lineage=TripColumns['DOLocationID'])]
    trip_miles: Annotated[Float64, TableField(lineage=TripColumns['trip_miles'])]
    trip_time: Annotated[Int64, TableField(lineage=TripColumns['trip_time'])]
    base_passenger_fare: Annotated[
        Float64, TableField(lineage=TripColumns['base_passenger_fare'])
    ]
    tolls: Annotated[Float64, TableField(lineage=TripColumns['tolls'])]
    sales_tax: Annotated[Float64, TableField(lineage=TripColumns['sales_tax'])]
    tips: Annotated[Float64, TableField(lineage=TripColumns['tips'])]
    Borough: Annotated[String, TableField(lineage="taxi_zones['Borough']")]
    Zone: Annotated[String, TableField(lineage="taxi_zones['Zone']")]
    service_zone: Annotated[String, TableField(lineage="taxi_zones['service_zone']")]


class NormalizedTaxiTripsSchema(TableSchema):
    """Trips filtered to plausible distances, with a log-transformed distance column."""

    pickup_datetime: Annotated[
        TimestampMicroUTC,
        TableField(
            doc=(
                "Timestamp (microseconds; UTC) of trip start time (passenger pickup). "
                "Filtered to be in 2022 or later."
            ),
        ),
    ]
    dropoff_datetime: TimestampMicroUTC
    PULocationID: Int64
    DOLocationID: Int64
    trip_miles: Annotated[
        Float64,
        TableField(doc="Trip distance (miles) filtered to be between 0 and 200."),
    ]
    trip_time: Int64
    base_passenger_fare: Float64
    tolls: Float64
    sales_tax: Float64
    tips: Float64
    Borough: String
    Zone: String
    service_zone: String
    log_trip_miles: Annotated[
        Float64, TableField(doc="Base-10 logarithm of trip_miles.")
    ]


# The `model` decorator tells Bauplan that this function defines a model: a
# transformation with many (>= 1) input tables and an output table.
# Arrow tables are the standard structure for input tables and the output table.
@bauplan.model()

# The `python` decorator allows you to specify a Python version and any pip packages that
# should be installed when executing this function. Each function is executed in an
# independent environment and may:
# - be run by a different version of the Python interpreter (e.g. "3.12" vs "3.14").
# - use different packages (e.g. polars vs pandas).
# - use different package versions (e.g. "polars": "1.43.2" vs "polars": "1.36.0b2").
@bauplan.python("3.12")
def trips_and_zones(
    trips: Annotated[
        pyarrow.Table,
        Model(
            # Specify the model identifier with the first positional arg or `name` kwarg.
            "taxi_fhvhv",

            # Specify specific columns to read with the `projection_schema` parameter.
            projection_schema=TripColumns,

            # Specify filtering for rows to retrieve with the `filter` parameter.
            filter="pickup_datetime >= '2022-12-15T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'",
        ),
    ],
    zones: Annotated[pyarrow.Table, Model("taxi_zones")],
) -> Annotated[pyarrow.Table, TripsAndZonesSchema]:
    # Using PyArrow (https://arrow.apache.org/docs/python/index.html),
    # join 'trips' with 'zones' on 'PULocationID'.
    pickup_location_table = trips.join(
        zones, "PULocationID", "LocationID"
    ).combine_chunks()

    # The return value will be checked against the return annotation:
    # A pyarrow.Table whose schema is compatible with `TripsAndZonesSchema`
    return pickup_location_table


# Polars is recommended for working with Arrow
# tables - it reads Arrow natively with zero-copy.
@bauplan.python("3.12", pip={"polars": "1.38.1"})
@bauplan.model(materialization_strategy="REPLACE")
def normalized_taxi_trips(
    # Bauplan models are referenced by name to form a DAG.
    # The `data` input comes from the results of the 'trips_and_zones' model.
    data: Annotated[pyarrow.Table, Model("trips_and_zones")],
) -> Annotated[pyarrow.Table, NormalizedTaxiTripsSchema]:
    import polars as pl
    import math

    # Print statements appear directly in your terminal and can be used for debugging.
    size_in_gb = round(data.nbytes / math.pow(1024, 3), 3)
    print(f"\nThis table is {size_in_gb} GB and has {data.num_rows} rows\n")

    # Initialize a polars.DataFrame from a pyarrow.Table (zero-copy).
    df = pl.from_arrow(data)

    # Filter by timestamp and trip_miles, and add a log-transformed column.
    df = (
        df.filter(
            pl.col("pickup_datetime")
            >= pl.lit("2022-01-01").str.to_datetime().dt.replace_time_zone("UTC")
        )
        .filter(pl.col("trip_miles") > 0.0)
        .filter(pl.col("trip_miles") < 200.0)
        .with_columns(pl.col("trip_miles").log(base=10).alias("log_trip_miles"))
    )

    # Return the data as a pyarrow.Table
    return df.to_arrow()
