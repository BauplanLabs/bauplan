"""
This pipeline computes a table with the zones of NY ordered by how long it takes to get a taxi cab on average.
"""

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


class TripTimestamps(TableSchema):
    """The projection of taxi_fhvhv used to measure how long a cab takes to arrive."""

    PULocationID: Int64
    request_datetime: TimestampMicroUTC
    on_scene_datetime: TimestampMicroUTC
    pickup_datetime: Annotated[
        TimestampMicroUTC,
        TableField(doc="Trip start time, filtered to December 2022."),
    ]
    dropoff_datetime: TimestampMicroUTC


class NormalizedTaxiTripsSchema(TableSchema):
    """Trip timestamps enriched with the borough and zone of the pickup location."""

    PULocationID: Annotated[Int64, TableField(lineage=TripTimestamps['PULocationID'])]
    request_datetime: Annotated[
        TimestampMicroUTC, TableField(lineage=TripTimestamps['request_datetime'])
    ]
    on_scene_datetime: Annotated[
        TimestampMicroUTC, TableField(lineage=TripTimestamps['on_scene_datetime'])
    ]
    pickup_datetime: Annotated[
        TimestampMicroUTC, TableField(lineage=TripTimestamps['pickup_datetime'])
    ]
    dropoff_datetime: Annotated[
        TimestampMicroUTC, TableField(lineage=TripTimestamps['dropoff_datetime'])
    ]
    Borough: Annotated[String, TableField(lineage="taxi_zones['Borough']")]
    Zone: Annotated[String, TableField(lineage="taxi_zones['Zone']")]
    service_zone: Annotated[String, TableField(lineage="taxi_zones['service_zone']")]


class TaxiTripWaitingTimesSchema(TableSchema):
    """Normalized trips with the wait between requesting a cab and its arrival."""

    PULocationID: Int64
    request_datetime: TimestampMicroUTC
    on_scene_datetime: TimestampMicroUTC
    pickup_datetime: TimestampMicroUTC
    dropoff_datetime: TimestampMicroUTC
    Borough: String
    Zone: String
    service_zone: String
    waiting_time_minutes: Annotated[
        Int64,
        TableField(doc="Whole minutes elapsed between request_datetime and on_scene_datetime."),
    ]


class ZoneAvgWaitingTimesSchema(TableSchema):
    """One row per pickup zone, ordered by the longest average wait first."""

    Borough: String
    Zone: String
    avg_waiting_time: Annotated[
        Float64,
        TableField(doc="Mean of waiting_time_minutes across the trips in the zone."),
    ]


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.38.1"})
def normalized_taxi_trips(
    trips: Annotated[
        pyarrow.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=TripTimestamps,
            filter="pickup_datetime >= '2022-12-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'",
        ),
    ],
    zones: Annotated[pyarrow.Table, Model("taxi_zones")],
) -> Annotated[pyarrow.Table, NormalizedTaxiTripsSchema]:
    import polars as pl
    import math

    size_in_gb = round(trips.nbytes / math.pow(1024, 3), 3)
    print(f"\nTaxi trips table is {size_in_gb} GB and has {trips.num_rows} rows\n")

    # Join trips with zones on PULocationID to get
    # Zone and Borough for each pickup location.
    trips_df = pl.from_arrow(trips)
    zones_df = pl.from_arrow(zones)
    result = trips_df.join(zones_df, left_on="PULocationID", right_on="LocationID")

    return result.to_arrow()


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.38.1"})
def taxi_trip_waiting_times(
    data: Annotated[pyarrow.Table, Model("normalized_taxi_trips")],
) -> Annotated[pyarrow.Table, TaxiTripWaitingTimesSchema]:
    import polars as pl

    df = pl.from_arrow(data)

    # Waiting time = minutes between request_datetime and on_scene_datetime.
    df = df.with_columns(
        (
            (
                pl.col("on_scene_datetime") - pl.col("request_datetime")
            ).dt.total_minutes()
        ).alias("waiting_time_minutes")
    )

    return df.to_arrow()


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.12", pip={"polars": "1.38.1"})
def zone_avg_waiting_times(
    taxi_trip_waiting_times: Annotated[
        pyarrow.Table, Model("taxi_trip_waiting_times")
    ],
) -> Annotated[pyarrow.Table, ZoneAvgWaitingTimesSchema]:
    import polars as pl

    df = pl.from_arrow(taxi_trip_waiting_times)

    # Average waiting time per Borough/Zone, ordered by longest wait first.
    result = (
        df.group_by("Borough", "Zone")
        .agg(pl.col("waiting_time_minutes").mean().alias("avg_waiting_time"))
        .sort("avg_waiting_time", descending=True)
    )

    return result.to_arrow()
