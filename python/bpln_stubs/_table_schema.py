"""Bauplan pySDK stubs for table schema type contracts."""

from typing import LiteralString

from bpln_stubs._table_fields import FieldType


class TableSchema:
    """
    A table schema is a collection of table column definitions and is a required base class for
    schema type contracts.

    For example:

    ```
    class NewTableSchema(TableSchema):
        first: Annotated[Int64, TableField(doc='first column')]
        second: Annotated[Float64, TableField(doc='second column')]
    ```

    References to schema columns can be made using "get item" syntax and passing it the
    `lineage` parameter of `TableField`, for example:

    ```
    class DerivedSchema(TableSchema):
        # `NewTableSchema['first']` is "get item" syntax (__class_getitem__ in this case)
        third: Annotated[Int64, TableField(lineage=NewTableSchema['first'])]
    ```

    If a column from a model in the catalog is being referenced, it must be done so as a
    "type forward" (string literal) and it must use the model name as the schema
    identifier:

    ```
    class PushdownSchema(TableSchema):
        # `titanic['PassengerId']` refers to a catalog table.
        # The identifier follows standard protocol and will be prepended with
        # the default namespace (maybe 'bauplan'), or can be provided with the namespace
        # like: ```bauplan.titanic['PassengerId']```.
        third: Annotated[Int64, TableField(lineage=titanic['PassengerId'])]
    ```
    """

    def __class_getitem__(cls, item: LiteralString) -> FieldType:
        return cls.__annotations__[item]
