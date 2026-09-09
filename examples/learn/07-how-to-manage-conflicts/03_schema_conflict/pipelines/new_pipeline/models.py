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
)

# Largest value a decimal(4, 2) column can hold
MAX_FARE = 99.99


class PassengerFare(TableSchema):
    """The projection of titanic needed to average fares by class and sex."""

    Pclass: Int64 | None
    Fare: Float64 | None
    Sex: String | None


class AverageFareSchema(TableSchema):
    """One row per passenger class and sex, with the mean fare and a headcount."""

    Pclass: Int64
    Sex: String
    # Decimal fields are unpinned in the typed SDK
    Fare: bauplan.Any
    n_passengers: Annotated[
        Int64, TableField(doc="Number of passengers in this class and sex.")
    ]


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_average_fares(
    data: Annotated[
        pa.Table,
        # Widening the analysis to every class: the filter on Pclass is gone.
        Model("bauplan.titanic", projection_schema=PassengerFare),
    ],
) -> Annotated[pa.Table, AverageFareSchema]:
    """Compute the mean Titanic fare for each passenger class and sex."""
    import polars as pl

    df = pl.DataFrame(data)

    return (
        df.group_by(pl.col("Pclass"), pl.col("Sex"))
        .agg(
            pl.col("Fare").mean().cast(pl.Decimal(4, 2)),
            pl.len().cast(pl.Int64).alias("n_passengers"),
        )
        .to_arrow()
    )


class FareOnly(TableSchema):
    """The column validated by `test_fare_fits_precision`."""

    Fare: Float64 | None


# The schema leaves the decimal type unpinned, so this expectation checks precision
@bauplan.expectation()
@bauplan.python("3.12")
def test_fare_fits_precision(
    data: Annotated[
        pa.Table,
        Model("bauplan.titanic", projection_schema=FareOnly),
    ],
) -> bool:
    """Validates that every fare fits the precision the model publishes"""

    too_wide = [
        fare
        for fare in data.column("Fare").to_pylist()
        if fare is not None and fare > MAX_FARE
    ]

    assert not too_wide, (
        f"Found Fare value {too_wide[0]} that does not fit decimal(4, 2), "
        f"if value is valid, widen the model's decimal precision and raise "
        f"MAX_FARE to match"
    )
    return True
