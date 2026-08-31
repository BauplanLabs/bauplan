from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Float64,
    Int32,
    Model,
    String,
    TableField,
    TableSchema,
)


class PassengerFare(TableSchema):
    """The projection of titanic needed to track fares by passenger."""

    Name: String
    Fare: Float64


class FareTableSchema(TableSchema):
    """One row per passenger per year the fare was calculated for."""

    Name: String
    Fare: Float64
    fare_calculated_in: Annotated[
        Int32, TableField(doc="Year the fare in this row was calculated for.")
    ]


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_fare_table(
    workshop_passengers_fare: Annotated[
        pyarrow.Table,
        Model("titanic", projection_schema=PassengerFare),
    ],
) -> Annotated[pyarrow.Table, FareTableSchema]:
    """
    Find fare for each passenger.

    Returned table:
    | Name                        | Fare  | fare_calculated_in |
    |-----------------------------|------|-------------------|
    | Braund, Mr. Owen Harris     | 7.25 | 1912              |
    | Heikkinen, Miss. Laina      | 26.0 | 1912              |
    | Allen, Mr. William Henry    | 13.88 | 1912              |
    | McCarthy, Mr. Timothy J     | 54.0 | 1912              |

    """
    import polars as pl

    return (
        pl.from_arrow(workshop_passengers_fare)
        .select("Name", "Fare")
        .sort("Name")
        .with_columns(
            fare_calculated_in=pl.lit(
                1912
            )  # Their fare was computed in 1912, we will adjust for inflation
        )
        .to_arrow()
    )
