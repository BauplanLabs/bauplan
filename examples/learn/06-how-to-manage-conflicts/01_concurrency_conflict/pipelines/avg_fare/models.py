from typing import Annotated

import bauplan
import pyarrow as pa

from bauplan import (
    Float64,
    Int64,
    Model,
    TableField,
    TableSchema,
)


class PassengerFare(TableSchema):
    """The projection of titanic needed to average fares by class."""

    Pclass: Int64 | None
    Fare: Float64 | None


class AverageFareSchema(TableSchema):
    """One row per passenger class, with the mean fare paid."""

    Pclass: Int64
    Fare: Annotated[
        Float64, TableField(doc="Mean fare paid by passengers in this class.")
    ]


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model(materialization_strategy="REPLACE")
def workshop_average_fares(
    data: Annotated[pa.Table, Model("titanic", projection_schema=PassengerFare)],
) -> Annotated[pa.Table, AverageFareSchema]:
    """Compute the mean Titanic fare for each passenger class."""
    import polars as pl

    df = pl.DataFrame(data)

    return df.group_by(pl.col("Pclass")).agg(pl.col("Fare").mean()).to_arrow()
