from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Float64,
    Model,
    TableField,
    TableSchema,
)
from bauplan.standard_expectations import expect_column_mean_greater_than


class TripMilesPushdown(TableSchema):
    """The single column the expectation reads from query_model."""

    trip_miles: Annotated[Float64, TableField(lineage="QueryModelSchema['trip_miles']")]


@bauplan.expectation()
@bauplan.python("3.11")
def test_trip_miles_mean(
    data: Annotated[
        pyarrow.Table,
        Model("query_model", projection_schema=TripMilesPushdown),
    ],
) -> bool:
    return expect_column_mean_greater_than(data, "trip_miles", 0.0)
