from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Float64,
    Model,
    String,
    TableField,
    TableSchema,
    TimestampMicroUTC,
)


class LicenseFare(TableSchema):
    """The projection of taxi_fhvhv needed to average fares by license number."""

    hvfhs_license_num: String
    base_passenger_fare: Float64
    on_scene_datetime: Annotated[
        TimestampMicroUTC,
        TableField(doc="Driver arrival time, filtered to July 2023 onwards."),
    ]


class AverageFareSchema(TableSchema):
    """One row per HVFHV license number, with the mean base passenger fare."""

    hvfhs_license_num: String
    base_passenger_fare: Annotated[
        Float64, TableField(doc="Mean base fare across the rides for this license.")
    ]


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_average_fares(
    data: Annotated[
        pyarrow.Table,
        Model(
            "bauplan.taxi_fhvhv",
            projection_schema=LicenseFare,
            filter="on_scene_datetime >= '2023-07-01'",
        ),
    ],
) -> Annotated[pyarrow.Table, AverageFareSchema]:
    """Compute the mean base passenger fare for each HVFHV license number, filtered to rides from July 2023 onwards."""
    import polars as pl

    df = pl.from_arrow(data)

    return (
        df.group_by(pl.col("hvfhs_license_num"))
        .agg(pl.col("base_passenger_fare").mean())
        .to_arrow()
    )
