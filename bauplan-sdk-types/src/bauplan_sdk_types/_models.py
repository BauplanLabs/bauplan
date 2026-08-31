from dataclasses import dataclass
from typing import LiteralString, Optional

from bauplan_sdk_types._table_schema import TableSchema


@dataclass
class Model:
    """
    A reference to another Bauplan model (a DAG node) as a data dependency.
    This identifies the model by catalog identifier, which may be qualified
    ('namespace.name') or may be bare ('name'). A bare model name will be prefixed with
    the default namespace.

    As a simple example, consider two models: (1) a leaf model, `leaf_model` that reads
    from the catalog and (2) a downstream model, `my_model`, that reads from the leaf
    model. In the code below, `leaf_model` specifies the catalog table it depends on
    using `Model('src_table')`; then, `my_model` specifies its dependency on the output
    of `leaf_model` using `Model('bauplan.leaf_table')`. If `leaf_model` wasn't defined
    to have the identifier "bauplan.leaf_table", then its default identifier (its
    function name, "leaf_model") would be used instead.

    ```python
    from typing import Annotated

    #! import pyarrow
    import bauplan

    #! class MySchema(bauplan.TableSchema):
        #! my_col: bauplan.Int64

    @bauplan.model(name='bauplan.leaf_table')
    def leaf_model(
        catalog_table: Annotated[pyarrow.Table, bauplan.Model('src_table')],
    ) -> Annotated[pyarrow.Table, MySchema]:
        return catalog_table.select(['my_col'])

    @bauplan.model()
    def my_model(
        leaf_data: Annotated[
            pyarrow.Table,
            bauplan.Model(name='bauplan.leaf_table')
        ],
    ) -> Annotated[pyarrow.Table, MySchema]:
        return leaf_data
    ```

    There are two parameters supported to provide "pushdown" support for projections and
    selections.

    The `projection_schema` parameter specifies a schema, by identifier, to
    use as a projection (select column names to read into result table). Specifying a
    projection is important, because it lets the system avoid reading data that the model
    doesn't use. For example:

    ```python type:ignore
    @bauplan.model(name='bauplan.leaf_table')
    def leaf_model(
        catalog_table: Annotated[
            pyarrow.Table,
            bauplan.Model('src_table', projection_schema=MySchema),
        ],
    ) -> Annotated[pyarrow.Table, MySchema]:
        # `projection_schema` is the equivalent of `select(['my_col'])` with validation
        return catalog_table
    ```

    The `filter` parameter specifies a string-based SQL-like predicate that can be used
    to filter table rows from the input table. The predicate must be a string literal;
    usage would look like:

    ```python type:ignore
    bauplan.Model('src_table', filter='bar > 1')
    ```

    Parameters:
        name: The identifier of the model; accepted as a positional arg or as a keyword.
        projection_schema: A schema containing the columns to read for downstream use.
        filter: A SQL-like predicate, as a string literal, to filter rows from the model.
    """

    name: str
    projection_schema: Optional[type[TableSchema]] = None
    filter: Optional[LiteralString] = None
