import pyarrow
import bauplan

from typing import Annotated
from bauplan import (
    TableSchema,
    Model,
    Float64,
)


class TestSchema(TableSchema):
    """Placeholder for a schema definition."""

    trip_miles: Float64


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
) -> 'Annotated[pyarrow.Table, TestSchemaMisnamed]':  # ty: ignore[unresolved-reference] # noqa: F821
    return taxi_trips.slice(length=5).rename_columns(['misnamed_col'])
