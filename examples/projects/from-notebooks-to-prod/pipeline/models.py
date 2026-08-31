from typing import Annotated

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


class TripColumns(TableSchema):
    """The projection of taxi_fhvhv the notebook's join needs."""

    pickup_datetime: TimestampMicroUTC
    PULocationID: Int64
    trip_miles: Float64


class ZoneColumns(TableSchema):
    """The projection of taxi_zones the notebook's join needs."""

    LocationID: Int64
    Zone: String


class TripsAndZonesSchema(TableSchema):
    """Taxi trips enriched with the zone of their pickup location."""

    pickup_datetime: Annotated[
        TimestampMicroUTC,
        TableField(lineage=TripColumns['pickup_datetime']),
    ]
    PULocationID: Annotated[
        Int64,
        TableField(
            doc=(
                "Pickup location. The join is a full outer join with coalesce, so this "
                "column carries taxi_zones' LocationID for zones with no trips."
            ),
            lineage=TripColumns['PULocationID'],
        ),
    ]
    trip_miles: Annotated[Float64, TableField(lineage=TripColumns['trip_miles'])]
    Zone: Annotated[String, TableField(lineage=ZoneColumns['Zone'])]


@bauplan.model()
@bauplan.python("3.11", pip={"polars": "1.38.1", "marimo": "0.20.4"})
def trips_and_zones(
    trips: Annotated[
        pyarrow.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=TripColumns,
            filter="pickup_datetime >= '2022-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'",
        ),
    ],
    zones: Annotated[pyarrow.Table, Model("taxi_zones", projection_schema=ZoneColumns)],
) -> Annotated[pyarrow.Table, TripsAndZonesSchema]:
    # Import the necessary libraries.
    import polars as pl

    # Make sure to import the marimo function you want to use.
    from taxi_notebook import join_taxi_tables

    # Re-use marimo function - it accepts polars DataFrames as input.
    # Note that this is zero-copy, so the conversion is free.
    return join_taxi_tables(
        pl.from_arrow(trips), pl.from_arrow(zones)
    ).to_arrow()  # We return Arrow.


class StatsByTaxiZoneSchema(TableSchema):
    """Median log-transformed trip distance per pickup zone."""

    Zone: Annotated[String, TableField(doc="Pickup zone the statistics are grouped by.")]
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
    data: Annotated[pyarrow.Table, Model("trips_and_zones")],
) -> Annotated[pyarrow.Table, StatsByTaxiZoneSchema]:
    # Import the necessary libraries.
    import polars as pl

    # Make sure to import the marimo function you want to use.
    from taxi_notebook import compute_stats_by_zone

    # Re-use marimo function - it accepts a polars DataFrame as input.
    # Note that this is zero-copy, so the conversion is free.
    # We return Arrow.
    return compute_stats_by_zone(pl.from_arrow(data)).to_arrow()
