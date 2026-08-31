"""Bauplan pySDK stubs for typing table fields (columns)."""

from typing import Optional, TypeVar, Generic


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


PrecisionParam = TypeVar("PrecisionParam", bound=int)
ScaleParam = TypeVar("ScaleParam", bound=int)


class Decimal128(FieldType, Generic[PrecisionParam, ScaleParam]):
    """
    Fixed-point data type corresponding to the Arrow data type `Decimal128`.

    Precision and scale are given as type parameters ("get item" syntax), because a call
    is not a valid type expression and is reported as an error by type checkers:

    ```
    class PriceSchema(TableSchema):
        price: Decimal128[Literal[38], Literal[10]]
    ```
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


class TableField:
    """A schema field that contains metadata for a table column."""

    # TODO: make all parameters keyword args only with `*` arg marker
    def __init__(
        self,
        doc: Optional[str] = None,
        title: Optional[str] = None,
        lineage: Optional[FieldType | str] = None,
        nullable: Optional[bool] = None,
    ):
        """
        `doc`: Documentation for the TableField.
        `title`: A title for the TableField and its documentation.
        `lineage`: A reference to a `TableField` to "inherit" data and metadata from.
        `nullable`: `True` if the TableField may contain `None` values (`NULL` in SQL);
                    default value is `None`, default meaning is to accept `None` values.
        """

        super().__init__()
