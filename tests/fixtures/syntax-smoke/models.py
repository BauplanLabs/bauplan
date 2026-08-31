"""
A "smoke test" for sdk stubs.

This fixture implements no logic, it just attempts to use every stub; thus, it is a
"smoke test".
"""

import pyarrow
from typing import Annotated

# decorators
from bauplan import (
    model,
    python,
)

# Field related types
from bauplan import (
    Bool,
    Int32,
    Int64,
    Float64,
    Decimal128,
    String,
    Binary,
    Date32,
    Date64,
    TimestampMicro,
    TimestampNano,
    TimestampMicroUTC,
    TimestampNanoUTC,
    TableField,
)

# Primary entities
from bauplan import (
    TableSchema,
    Model,
    ModelCacheStrategy,
    ModelMaterializationStrategy,
    Parameter,
)


# Strategy values can be passed as literals (see the decorators below) or as named
# values typed by the exported aliases
cache_off: ModelCacheStrategy = "NONE"
materialize_append: ModelMaterializationStrategy = "APPEND"
materialize_partitions: ModelMaterializationStrategy = "OVERWRITE_PARTITIONS"


# Define some schemas
class BareTypesSchema(TableSchema):
    bare_bool: Bool
    bare_int: Int32
    bare_long: Int64
    bare_float: Float64
    # A parameterized type takes its parameters as type parameters
    bare_decimal: Decimal128[38, 10]  # ty: ignore[invalid-type-form]
    bare_str: String
    bare_binary: Binary
    bare_date32: Date32
    bare_date64: Date64
    bare_ts_micro: TimestampMicro
    bare_ts_nano: TimestampNano
    bare_ts_micro_utc: TimestampMicroUTC
    bare_ts_nano_utc: TimestampNanoUTC


class AnnotatedTypesSchema(TableSchema):
    annotated_bool: Annotated[Bool, TableField(doc="bool docstring")]
    annotated_int: Annotated[Int32, TableField(title="int32 doc title")]
    annotated_long: Annotated[Int64, TableField(nullable=True)]
    annotated_float: Annotated[Float64, TableField(nullable=False)]
    annotated_decimal: Annotated[
        Decimal128[38, 10],  # ty: ignore[invalid-type-form]
        TableField(
            doc="decimal docstring",
            lineage=BareTypesSchema["bare_decimal"],
        ),
    ]
    annotated_str: Annotated[String, TableField(lineage=BareTypesSchema["bare_str"])]
    annotated_binary: Annotated[
        Binary, TableField(lineage='BareTypesSchema["bare_binary"]')
    ]
    annotated_date32: Annotated[Date32, TableField(doc="days since epoch")]
    annotated_date64: Annotated[
        Date64, TableField(doc="milliseconds since epoch", nullable=False)
    ]
    annotated_ts_micro: Annotated[
        TimestampMicro,
        TableField(
            doc="TS micro docstring",
            title="TS micro doc title",
            nullable=False,
            lineage=BareTypesSchema["bare_ts_micro"],
        ),
    ]
    annotated_ts_nano: Annotated[
        TimestampNano,
        TableField(
            doc="TS nano docstring",
            title="TS nano doc title",
            nullable=False,
            lineage="BareTypesSchema['bare_ts_nano']",
        ),
    ]
    annotated_ts_micro_utc: Annotated[
        TimestampMicroUTC,
        TableField(
            doc="TS micro UTC docstring",
            title="TS micro UTC doc title",
            nullable=True,
            lineage=BareTypesSchema["bare_ts_micro_utc"],
        ),
    ]
    annotated_ts_nano_utc: Annotated[
        TimestampNanoUTC,
        TableField(
            doc="TS nano UTC docstring",
            title="TS nano UTC doc title",
            nullable=True,
            lineage="BareTypesSchema['bare_ts_nano_utc']",
        ),
    ]


@model(
    name="full_options_model",
    materialization_strategy="REPLACE",
    cache_strategy="DEFAULT",
    partitioned_by=["year", "month"],
    internet_access=True,
    overwrite_filter="year > 2020",
)
@python("3.11", pip={"polars": "1.37"})
def model_all_options(
    trips: Annotated[
        pyarrow.Table,
        Model(
            "namespace.source",
            filter="col = 1 AND col > 0",
            projection_schema=BareTypesSchema,
        ),
    ],
    rate: Annotated[float, Parameter("rate")],
    other: Annotated[
        pyarrow.Table,
        Model(
            "bare_source",
            projection_schema=AnnotatedTypesSchema,
            filter="annotated_bool = True",
        ),
    ],
) -> Annotated[pyarrow.Table, AnnotatedTypesSchema]:
    print(rate)
    print(trips)
    return other


@python("3.13")
@model(materialization_strategy="NONE")
def model_minimal(
    data: Annotated[
        pyarrow.Table,
        Model(
            projection_schema=AnnotatedTypesSchema,
            name="full_options_model",
        ),
    ],
) -> "Annotated[pyarrow.Table, AnnotatedTypesSchema]":
    return data


# `partitioned_by` takes a single column name as a bare string
@model(
    name="append_model",
    materialization_strategy=materialize_append,
    cache_strategy=cache_off,
    partitioned_by="year",
    internet_access=False,
)
@python("3.12", pip={"pandas": "2.3", "numpy": "2.1"})
def model_append(
    data: Annotated[pyarrow.Table, Model("full_options_model")],
) -> Annotated[pyarrow.Table, AnnotatedTypesSchema]:
    return data


# `partitioned_by` takes many column names as a tuple as well as a list
@model(
    materialization_strategy=materialize_partitions,
    partitioned_by=("year", "month"),
    overwrite_filter="year > 2020",
)
@python("3.11", pip={"polars": "1.37"})
def model_overwrite_partitions(
    data: Annotated[pyarrow.Table, Model("append_model")],
) -> Annotated[pyarrow.Table, AnnotatedTypesSchema]:
    return data


# Neither decorator requires arguments
@model()
@python()
def model_no_decorator_args(
    data: Annotated[pyarrow.Table, Model("model_overwrite_partitions")],
) -> Annotated[pyarrow.Table, AnnotatedTypesSchema]:
    return data
