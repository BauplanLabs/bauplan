"""Bauplan pySDK stubs for typing table fields (columns)."""

from dataclasses import dataclass
from typing import Optional, TypeVar, TypeVarTuple, Generic


class FieldType:
    """
    Core data type for a `TableField` (table column) in Bauplan. Each supported type has
    a corresponding Arrow data type and Iceberg data type.
    """

    ...


class Bool(FieldType):
    """Boolean data type corresponding to the Arrow data type `Bool`."""

    ...


class Int32(FieldType):
    """Integer data type corresponding to the Arrow data type `Int32`."""

    ...


class Int64(FieldType):
    """Integer data type corresponding to the Arrow data type `Int64`."""

    ...


class Float64(FieldType):
    """Floating point data type corresponding to the Arrow data type `Float64`."""

    ...


# A type variable for binding decimal precision
PrecisionParam = TypeVar("PrecisionParam", bound=int)

# NOTE: `TypeVar` gets a `default` parameter in python >= 3.13; but >=3.11 has no similar mechanism
# A type variable for binding an optional decimal scale; if more than 1 value is provided, an error
# will be thrown by the planner.
ScaleParam = TypeVarTuple("ScaleParam")


class Decimal128(FieldType, Generic[PrecisionParam, *ScaleParam]):
    """
    Numerical data type corresponding to the Arrow data type `Decimal128`.

    Precision and scale are given as type parameters using brackets:
    ```
    class PriceSchema(TableSchema):
        price: Decimal128[Literal[38], Literal[10]]
    ```

    A generic type is used because a function call is not a valid type expression and is reported
    as an error by type checkers.

    Additional accepted syntax includes:
    - Providing a precision only (scale defaults to 0).
    - Providing literal values without the `Literal[...]` type class wrapper (but this syntax is
      flagged as an error by type checkers).
    """

    ...


class String(FieldType):
    """String data type corresponding to the Arrow data type `String`."""

    ...


class Date32(FieldType):
    """Date data type corresponding to the Arrow data type `Date32` (days)."""

    ...


class Date64(FieldType):
    """Date data type corresponding to the Arrow data type `Date64` (milliseconds)."""

    ...


class TimestampMicro(FieldType):
    """Time data type corresponding to the Arrow data type `Timestamp('us')`."""

    ...


class TimestampNano(FieldType):
    """Time data type corresponding to the Arrow data type `Timestamp('ns')`."""

    ...


class TimestampMicroUTC(FieldType):
    """
    Time data type corresponding to the Arrow data type `Timestamp('us', tz='UTC')`.
    Values are absolute instants; Iceberg stores these as UTC and does not preserve
    the timezone they were written from.
    """

    ...


class TimestampNanoUTC(FieldType):
    """
    Time data type corresponding to the Arrow data type `Timestamp('ns', tz='UTC')`.
    Values are absolute instants; Iceberg stores these as UTC and does not preserve
    the timezone they were written from.
    """

    ...


class Binary(FieldType):
    """Binary data type corresponding to the Arrow data type `Binary`."""

    ...


class Any(FieldType):
    """Bypasses type validation"""

    ...


@dataclass
class ColumnLineage(FieldType):
    """
    Reference to an upstream table column to inherit data and metadata from.

    The reference has two required parameters:
    1. `table_name` the qualified (or bare) name of the table containing the referenced column. If
       not qualified, the default namespace will be prepended.
    2. `col_name` expects the column name as stored in the lakehouse catalog.
    """

    table_name: str
    col_name: str


class TableField:
    """
    A schema field that contains metadata and is used to annotate a table column.

    The primary use case for a `TableField` is as annotation on an attribute of a
    `TableSchema`:

    ```python
    from typing import Annotated
    import bauplan_sdk_types

    class SampleSchema(bauplan_sdk_types.TableSchema):
        col_a: Annotated[
            bauplan_sdk_types.Any,
            bauplan_sdk_types.TableField(name='.col a.')
        ]
    ```

    In the above example, `col_a` is an attribute on the class `SampleSchema` and
    represents a table column. Using the special type, `typing.Annotated`, and this
    class, `TableField`, it is possible to annotate the represented table column with
    metadata and constraints.

    The attribute name, `col_a`, corresponds to the name of the represented table column.
    If the table column's name cannot be used as the attribute name then the `name`
    parameter can be specified, for example: `name='.col a.'`.

    Lineage references another `TableField` to "inherit" or "receive" data and metadata
    from. This is an explicit property that represents dataflow. Lineage can be specified
    in one of two ways:
    1. directly reference an attribute (the python identifier) in a `TableSchema`:
    `SampleSchema['col_a']`.
    2. define a `ColumnLineage` object using the table identifier and column name:
    `ColumnLineage('namespace.table', '.col a.')`.
    """

    # TODO: make all parameters keyword args only with `*` arg marker
    def __init__(
        self,
        name: Optional[str] = None,
        doc: Optional[str] = None,
        lineage: Optional[FieldType | str] = None,
    ):
        """
        `name`: Name of the annotated table column.
        `doc`: Documentation describing the table column.
        `lineage`: Reference to another `TableField` to "inherit" data and metadata from.
        """

        super().__init__()
