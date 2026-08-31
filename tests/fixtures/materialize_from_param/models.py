from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Model,
    Parameter,
    String,
    TableField,
    TableSchema,
    TimestampMicroUTC,
)


class QuerySchema(TableSchema):
    """The single column query selects from taxi_fhvhv."""

    dropoff_datetime: TimestampMicroUTC


class MagicValSchema(TableSchema):
    """A single row holding the value the run was parameterized with."""

    magicval_field: Annotated[
        String, TableField(doc="The value of the magicval parameter.")
    ]


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.11")
def materialized_table_model(
    sad_unused_parent: Annotated[
        pyarrow.Table,
        Model("query", projection_schema=QuerySchema),
    ],
    magicval: Annotated[str, Parameter("magicval")],
) -> Annotated[pyarrow.Table, MagicValSchema]:
    return pyarrow.Table.from_pydict({"magicval_field": [magicval]})
