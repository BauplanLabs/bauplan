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
    TimestampMicroUTC,
)


class PickupPushdown(TableSchema):
    """The single column parent reads from taxi_fhvhv."""

    pickup_datetime: TimestampMicroUTC


class SyntheticSchema(TableSchema):
    """Columns built by hand, standing in for a table that carries field ids."""

    col1: Annotated[Int32, TableField(doc="Carries PARQUET:field_id 3 on the way out.")]
    col2: Annotated[
        String, TableField(doc="Carries PARQUET:field_id 6 on the way out.")
    ]
    col3: Annotated[
        Float64, TableField(doc="Carries PARQUET:field_id 20 on the way out.")
    ]


def assert_no_parquet_field_ids(table: pyarrow.Table) -> None:
    for field in table.schema:
        if field.metadata and b"PARQUET:field_id" in field.metadata:
            field_id = field.metadata[b"PARQUET:field_id"]
            raise AssertionError(
                f"Field '{field.name}' has PARQUET:field_id metadata with value: {field_id}"
            )


@bauplan.model()
@bauplan.python("3.11")
def parent(
    trips: Annotated[
        pyarrow.Table,
        Model(
            "taxi_fhvhv",
            projection_schema=PickupPushdown,
            filter="pickup_datetime >= '2022-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T01:00:00-05:00'",
        ),
    ],
) -> Annotated[pyarrow.Table, SyntheticSchema]:
    # the scan should not have field ids either
    assert_no_parquet_field_ids(trips)

    # now let's simulate as if the table somehow ended having field ids added by the user
    # (even though this will likely never happen in practice)

    field1 = pyarrow.field(
        "col1", pyarrow.int32(), metadata={b"PARQUET:field_id": b"3"}
    )
    field2 = pyarrow.field(
        "col2", pyarrow.string(), metadata={b"PARQUET:field_id": b"6"}
    )
    field3 = pyarrow.field(
        "col3", pyarrow.float64(), metadata={b"PARQUET:field_id": b"20"}
    )
    schema = pyarrow.schema([field1, field2, field3])
    data = {
        "col1": [1, 2, 3, 4, 5],
        "col2": ["a", "b", "c", "d", "e"],
        "col3": [1.1, 2.2, 3.3, 4.4, 5.5],
    }
    return pyarrow.table(data, schema=schema)


@bauplan.model()
@bauplan.python("3.11")
def child(
    data: Annotated[pyarrow.Table, Model("parent")],
) -> Annotated[pyarrow.Table, SyntheticSchema]:
    assert_no_parquet_field_ids(data)
    return data
