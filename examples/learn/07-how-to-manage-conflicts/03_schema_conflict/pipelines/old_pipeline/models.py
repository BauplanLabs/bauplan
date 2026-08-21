from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Decimal128,
    Float64,
    Int64,
    Model,
    TableField,
    TableSchema,
)

# Largest value a Decimal128[4, 2] column can hold.
MAX_FARE = 99.99


class PassengerFare(TableSchema):
    """The projection of titanic needed to average fares by class."""

    Pclass: Annotated[
        Int64, TableField(doc="Passenger class, filtered to 2nd and 3rd class.")
    ]
    Fare: Float64


class AverageFareSchema(TableSchema):
    """One row per passenger class, with the mean fare as exact money."""

    Pclass: Int64
    # A different type than the input is only allowed without `lineage`.
    Fare: Annotated[
        Decimal128[4, 2],
        TableField(
            doc=(
                "Mean fare paid by passengers in this class, as precise decimal "
                "instead of floating point value. Standard fares stay well under "
                "99.99, so they are guaranteed to fit into Decimal128[4, 2]."
            ),
        ),
    ]


class FareOnly(TableSchema):
    """The column validated by `test_fare_fits_precision`."""

    Fare: Float64


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_average_fares(
    data: Annotated[
        pyarrow.Table,
        Model(
            "bauplan.titanic",
            projection_schema=PassengerFare,
            # The analysis covers standard fares only, so first class is filtered out.
            filter="Pclass > 1",
        ),
    ],
) -> Annotated[pyarrow.Table, AverageFareSchema]:
    """Compute the mean Titanic fare for each standard passenger class."""
    import polars as pl

    df = pl.from_arrow(data)

    return (
        df.group_by(pl.col("Pclass"))
        .agg(pl.col("Fare").mean().cast(pl.Decimal(4, 2)))
        .to_arrow()
    )


# `AverageFareSchema` guarantees Fare is a Decimal128[4, 2]; what it cannot check is
# whether the fares being averaged actually fit that precision.
@bauplan.expectation()
@bauplan.python("3.12")
def test_fare_fits_precision(
    data: Annotated[
        pyarrow.Table,
        Model(
            "bauplan.titanic",
            projection_schema=FareOnly,
            filter="Pclass > 1",
        ),
    ],
) -> bool:
    """Validates that every fare fits the precision the model publishes"""

    too_wide = [
        fare
        for fare in data.column("Fare").to_pylist()
        if fare is not None and fare > MAX_FARE
    ]

    assert not too_wide, (
        f"Found Fare value {too_wide[0]} that does not fit Decimal128[4, 2], "
        f"if value is valid, widen the precision in 'AverageFareSchema' and raise "
        f"MAX_FARE to match"
    )
    return True
