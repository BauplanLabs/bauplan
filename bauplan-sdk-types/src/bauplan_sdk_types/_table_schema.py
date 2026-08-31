"""Bauplan pySDK stubs for table schema type contracts."""

from typing import LiteralString

from bauplan_sdk_types._table_fields import FieldType


class TableSchema:
    """
    A table schema is a collection of table column definitions and is a required base
    class when specifying a model output schema or a projection schema. A model output
    schema is specified in the return annotation of a model. A projection schema may be
    used to project a set of columns from a model input or an expectation input.

    For example:

    ```python
    class NewTableSchema(TableSchema):
        first: Annotated[Int64, TableField(doc='first column')]
        second: Annotated[Float64, TableField(doc='second column')]
    ```

    References to schema columns can be made by using "index syntax" and passing it the
    `lineage` parameter of `TableField`, for example:

    ```python
    class DerivedSchema(TableSchema):
        third: Annotated[Int64, TableField(lineage=NewTableSchema['first'])]
    ```
    """

    def __class_getitem__(cls, item: LiteralString) -> FieldType:
        return cls.__annotations__[item]
