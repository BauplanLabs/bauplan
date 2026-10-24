from typing import Annotated

import bauplan
import pyarrow
from bauplan import (
    Float64,
    Model,
    TableSchema,
)


class TestSchema(TableSchema):
    """Placeholder for a schema definition."""

    trip_miles: Float64 | None


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.13")
def typed_params_model(
    taxi_trips: Annotated[
        pyarrow.Table,
        Model(
            projection_schema=TestSchema,
            name="taxi_fhvhv",
            filter="PULocationID = 138",
        ),
    ],
) -> Annotated[pyarrow.Table, TestSchemaMisnamed]:  # ty: ignore[unresolved-reference] # noqa: F821
    return taxi_trips.slice(length=5).rename_columns(["misnamed_col"])
