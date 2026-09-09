from typing import Annotated

import bauplan
import pyarrow as pa

from bauplan import (
    Float64,
    Int64,
    Model,
    String,
    TableField,
    TableSchema,
    TimestampMicroUTC,
)


class TripColumns(TableSchema):
    """The projection of taxi_fhvhv the notebook's join needs."""

    pickup_datetime: TimestampMicroUTC | None
    PULocationID: Int64 | None
    trip_miles: Float64 | None


class ZoneColumns(TableSchema):
    """The projection of taxi_zones the notebook's join needs."""

    LocationID: Int64 | None
    Zone: String | None


class TripsAndZonesSchema(TableSchema):
    """Taxi trips enriched with the zone of their pickup location."""

    pickup_datetime: Annotated[
        TimestampMicroUTC | None,
        TableField(lineage=TripColumns["pickup_datetime"]),
    ]
    PULocationID: Annotated[
        Int64 | None,
        TableField(
            doc=(
                "Pickup location. The join is a full outer join with coalesce, so this "
                "column carries taxi_zones' LocationID for zones with no trips."
            ),
            lineage=TripColumns["PULocationID"],
        ),
    ]
    trip_miles: Annotated[Float64 | None, TableField(lineage=TripColumns["trip_miles"])]
    Zone: Annotated[String | None, TableField(lineage=ZoneColumns["Zone"])]


@bauplan.model()
@bauplan.python("3.11", pip={"polars": "1.38.1", "marimo": "0.20.4"})
def trips_and_zones(
    trips: Annotated[
        pa.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=TripColumns,
            filter="pickup_datetime >= '2022-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'",
        ),
    ],
    zones: Annotated[pa.Table, Model("taxi_zones", projection_schema=ZoneColumns)],
) -> Annotated[pa.Table, TripsAndZonesSchema]:
    # Import the necessary libraries.
    import polars as pl

    # Make sure to import the marimo function you want to use.
    from taxi_notebook import join_taxi_tables

    # Re-use marimo function - it accepts polars DataFrames as input.
    # Note that this is zero-copy, so the conversion is free.
    return join_taxi_tables(
        pl.DataFrame(trips), pl.DataFrame(zones)
    ).to_arrow()  # We return Arrow.


class StatsByTaxiZoneSchema(TableSchema):
    """Median log-transformed trip distance per pickup zone."""

    Zone: Annotated[
        String, TableField(doc="Pickup zone the statistics are grouped by.")
    ]
    log_trip_miles: Annotated[
        Float64,
        TableField(
            doc=(
                "Median base-10 logarithm of trip_miles for the zone, over trips in "
                "2022 or later whose distance is between 0 and 200 miles."
            ),
        ),
    ]


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11", pip={"polars": "1.38.1", "marimo": "0.20.4"})
def stats_by_taxi_zones(
    data: Annotated[pa.Table, Model("trips_and_zones")],
) -> Annotated[pa.Table, StatsByTaxiZoneSchema]:
    # Import the necessary libraries.
    import polars as pl

    # Make sure to import the marimo function you want to use.
    from taxi_notebook import compute_stats_by_zone

    # Re-use marimo function - it accepts a polars DataFrame as input.
    # Note that this is zero-copy, so the conversion is free.
    # We return Arrow.
    return compute_stats_by_zone(pl.DataFrame(data)).to_arrow()
