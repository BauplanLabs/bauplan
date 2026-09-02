from typing import Annotated

import pyarrow
import bauplan

from bauplan import (
    TableField,
    Model,
    Bool,
    Parameter,
    Int64,
    Float64,
    String,
    Binary,
    TimestampMicroUTC,
)


class TaxiModelSchema(bauplan.TableSchema):
    """Schema demonstrating all supported field types."""

    pickup_datetime: Annotated[
        TimestampMicroUTC, TableField(doc="Pickup time for the ride")
    ]
    dropoff_datetime: Annotated[
        TimestampMicroUTC, TableField(doc="Dropoff time for the ride")
    ]
    PULocationID: Annotated[Int64, TableField(doc="Identifier for pickup location")]
    DOLocationID: Annotated[Int64, TableField(doc="Identifier for dropoff location")]
    trip_miles: Annotated[Float64, TableField(doc="Miles traveled for a trip")]
    trip_time: Annotated[Int64, TableField(doc="Trip duration (in seconds) ")]
    base_passenger_fare: Annotated[Float64, TableField(doc="Base fare for a trip")]
    tolls: Annotated[Float64, TableField(doc="Total cost from road tolls")]
    sales_tax: Annotated[Float64, TableField(doc="Salex tax applied on the fare")]
    tips: Annotated[Float64, TableField(doc="Amount tendered for driver tip")]


class TripsPushdown(bauplan.TableSchema):
    """Pushdown to get just basic trip information."""

    PULocationID: Annotated[Int64, TableField(doc="Identifier for pickup location")]
    DOLocationID: Annotated[Int64, TableField(doc="Identifier for dropoff location")]
    pickup_datetime: TimestampMicroUTC
    trip_miles: Annotated[Float64, TableField(doc="Miles traveled for a trip")]
    trip_time: Annotated[Int64, TableField(doc="Trip duration (in seconds) ")]


class LocationsPushdown(bauplan.TableSchema):
    """Pushdown to get just pickup and dropoff location identifiers."""

    PULocationID: Annotated[Int64, TableField(doc="Identifier for pickup location")]
    DOLocationID: Annotated[Int64, TableField(doc="Identifier for dropoff location")]


class FullTypesSchema(bauplan.TableSchema):
    """Schema demonstrating all supported field types."""

    pickup_datetime: TimestampMicroUTC
    pickup_location: Annotated[
        Int64, bauplan.TableField(doc="ID of a trip's pickup location")
    ]
    dropoff_location: Int64
    trip_miles: Float64
    trip_time: Int64
    is_shared_ride: Bool
    driver_name: String
    raw_data: Binary


class SecondSchema(bauplan.TableSchema):
    """A second schema for multi-model coverage."""

    trip_time: Int64


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.12")
def multi_field_model(
    golden_ratio: Annotated[float, Parameter("golden_ratio")],
    start_datetime: Annotated[str, Parameter("start_datetime")],
    trips: Annotated[
        pyarrow.Table, Model("query_model", projection_schema=TripsPushdown)
    ],
    locations: Annotated[
        pyarrow.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=LocationsPushdown,
            filter=(
                "pickup_datetime >= $start_datetime AND pickup_datetime < $end_datetime"
            ),
        ),
    ],
) -> Annotated[pyarrow.Table, FullTypesSchema]:
    print(
        "Parameters:\n"
        f"\tGolden Ratio: {golden_ratio}\n"
        f"\tStart Datetime: {start_datetime}\n"
    )

    print(f"Row count for query_model trips: {trips.num_rows}")
    print(f"Row count for query_model locations: {locations.num_rows}")
    print(f"trip data preview: {trips.slice(0, 3)}")

    return pyarrow.Table.from_arrays(
        [
            trips.slice(0, 3).column(2),
            trips.slice(0, 3).column(0),
            trips.slice(0, 3).column(1),
            trips.slice(0, 3).column(3),
            trips.slice(0, 3).column(4),
            pyarrow.array([False, False, False]),
            pyarrow.array(["driver1", "driver2", "driver3"]),
            pyarrow.array([b"bin1", b"bin2", b"bin3"]),
        ],
        schema=pyarrow.schema(
            [
                pyarrow.field("pickup_datetime", pyarrow.timestamp("us", tz="UTC")),
                pyarrow.field("pickup_location", pyarrow.int64()),
                pyarrow.field("dropoff_location", pyarrow.int64()),
                pyarrow.field("trip_miles", pyarrow.float64()),
                pyarrow.field("trip_time", pyarrow.int64()),
                pyarrow.field("is_shared_ride", pyarrow.bool_()),
                pyarrow.field("driver_name", pyarrow.string()),
                pyarrow.field("raw_data", pyarrow.binary()),
            ]
        ),
    )


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.13")
def no_params_model(
    data: Annotated[
        pyarrow.Table, Model("query_model", projection_schema=SecondSchema)
    ],
) -> Annotated[pyarrow.Table, SecondSchema]:
    return data
