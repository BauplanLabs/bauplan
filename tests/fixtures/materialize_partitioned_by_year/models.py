from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Int64,
    Model,
    TableSchema,
    TimestampMicroUTC,
)


class DropoffsSchema(TableSchema):
    """The dropoff columns the materialized table is partitioned on."""

    dropoff_datetime: TimestampMicroUTC
    PULocationID: Int64


@bauplan.model(
    partitioned_by=["YEAR(dropoff_datetime)", "PULocationID"],
    materialization_strategy="REPLACE",
)
def materialized_table_model(
    parent: Annotated[
        pyarrow.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=DropoffsSchema,
            filter="dropoff_datetime >= '2023-01-01T00:00:00-00:00' AND dropoff_datetime < '2023-01-02T00:00:00-00:00'",
        ),
    ],
) -> Annotated[pyarrow.Table, DropoffsSchema]:
    return parent
