from typing import Annotated

import bauplan
import pyarrow as pa

from bauplan import (
    Float64,
    Int32,
    Model,
    Parameter,
    String,
    TableField,
    TableSchema,
)


class PassengerFare(TableSchema):
    """The projection of titanic needed to adjust fares for inflation."""

    Name: String | None
    Fare: Float64 | None


class FareTableSchema(TableSchema):
    """One row per passenger per year the fare was calculated for."""

    Name: String
    Fare: Annotated[
        Float64, TableField(doc="Fare adjusted for inflation up to the given year.")
    ]
    fare_calculated_in: Annotated[
        Int32, TableField(doc="Year the fare in this row was calculated for.")
    ]


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="APPEND")
def workshop_fare_table(
    passengers_fare: Annotated[
        pa.Table,
        Model("titanic", projection_schema=PassengerFare),
    ],
    year: Annotated[int, Parameter("year")],
    inflation_rate: Annotated[float, Parameter("inflation_rate")],
) -> Annotated[pa.Table, FareTableSchema]:
    """
    Append inflation-adjusted fares for the given year using the correct formula.

    The inflation multiplier is (inflation_rate + 1.0)^(year - 1912), which correctly
    compounds the rate over time and causes fares to grow year-on-year.
    """
    import polars as pl

    return (
        pl.DataFrame(passengers_fare)
        .with_columns(
            Fare=pl.col("Fare")
            * pl.lit(
                (
                    float(inflation_rate)
                    # Reinstating the 1.0 to fix the calculation
                    + 1.0
                )
            ).pow(year - 1912),
            fare_calculated_in=pl.lit(int(year)),
        )
        .sort("Name")
        .to_arrow()
    )
