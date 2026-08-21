"""Bauplan pySDK stubs for typing table fields (columns)."""

from typing import Optional, TypedDict, Unpack


class FieldType:
    """
    Core data type for a `TableField` (table column) corresponding to an Arrow data type
    that is also supported by Iceberg and, thus, Bauplan.
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
    """Float data type corresponding to the Arrow data type `Float64`."""

    ...


class Decimal128(FieldType):
    """
    Fixed-point data type corresponding to the Arrow data type `Decimal128`.

    Precision and scale are given as type parameters ("get item" syntax), because a call
    is not a valid type expression and is reported as an error by type checkers:

    ```
    class PriceSchema(TableSchema):
        price: Decimal128[38, 10]
    ```
    """

    def __class_getitem__(cls, params: tuple[int, int]) -> type["Decimal128"]:
        """
        `params`: A (precision, scale) pair. `precision` is the total number of digits
        and must be between 1 and 38 (inclusive). `scale` is the number of digits after
        the decimal point and must be between 0 and `precision`.
        """

        return cls


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


class FieldKeywordArgs(TypedDict):
    """Accepted keyword arguments for a `TableField` type."""

    ...


class TableField:
    """A schema field that contains metadata for a table column."""

    def __init__(
        self,
        doc: Optional[str] = None,
        title: Optional[str] = None,
        lineage: Optional[FieldType | str] = None,
        nullable: Optional[bool] = None,
        **kwargs: Unpack[FieldKeywordArgs],
    ):
        """
        `doc`: Documentation for the TableField.
        `title`: A title for the TableField and its documentation.
        `lineage`: A reference to a `TableField` to "inherit" data and metadata from.
        `nullable`: `True` if the TableField may contain `None` values (`NULL` in SQL);
                    default value is `None`, default meaning is to accept `None` values.
        """

        super().__init__(**kwargs)
