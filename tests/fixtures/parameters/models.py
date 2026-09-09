from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Float64,
    Int64,
    Model,
    Parameter,
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


class TripsPushdown(TableSchema):
    """The trip columns the model reads from query_model."""

    dropoff_datetime: Annotated[
        TimestampMicroUTC | None,
        TableField(lineage=QueryModelSchema["dropoff_datetime"]),
    ]
    pickup_datetime: Annotated[
        TimestampMicroUTC | None,
        TableField(lineage=QueryModelSchema["pickup_datetime"]),
    ]
    trip_miles: Annotated[
        Float64 | None, TableField(lineage=QueryModelSchema["trip_miles"])
    ]


class ConstantSchema(TableSchema):
    """A fixed table, returned so the model has an output at all."""

    y: Annotated[Int64 | None, TableField(doc="A constant, unrelated to the input.")]


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11")
def params_are_cool_model(
    yayparams: Annotated[
        pyarrow.Table,
        Model(
            "query_model",
            projection_schema=TripsPushdown,
        ),
    ],
    golden_ratio: Annotated[float, Parameter("golden_ratio")],
    use_random_forest: Annotated[bool, Parameter("use_random_forest")],
    start_datetime: Annotated[str, Parameter("start_datetime")],
    end_datetime: Annotated[str, Parameter("end_datetime")],
) -> Annotated[pyarrow.Table, ConstantSchema]:
    print(f"golden_ratio={golden_ratio}")
    print(f"use_random_forest={use_random_forest}")
    print(f"start_datetime={start_datetime}")
    print(f"end_datetime={end_datetime}")

    print(yayparams)
    print(f"yayparams.num_rows={yayparams.num_rows}")
    print(f"yayparams.num_columns={yayparams.num_columns}")

    return pyarrow.Table.from_pydict({"y": [1, 2, 3]})
