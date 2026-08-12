import types

from dataclasses import dataclass
from typing import (
    Callable,
    Literal,
    LiteralString,
    Optional,
    TypeVar,
    TypedDict,
    Union,
    Unpack,
)

from bpln_stubs._table_schema import TableSchema

ModelCacheStrategy = Literal["NONE", "DEFAULT"]
ModelMaterializationStrategy = Literal[
    "NONE", "REPLACE", "APPEND", "OVERWRITE_PARTITIONS"
]


class ModelKeywordArgs(TypedDict):
    """Accepted keyword arguments for the `model` decorator."""

    ...


def model(
    name: Optional[str] = None,
    partitioned_by: Optional[Union[str, list[str], tuple[str, ...]]] = None,
    materialization_strategy: Optional[ModelMaterializationStrategy] = None,
    cache_strategy: Optional[ModelCacheStrategy] = None,
    overwrite_filter: Optional[str] = None,
    internet_access: Optional[bool] = None,
    **kwargs: Unpack[ModelKeywordArgs],
) -> Callable:
    """
    Decorator that specifies a Bauplan model.

    A model is a function that defines data transformation logic that takes many Arrow
    tables as input and returns a single Arrow table as output. A model can be referenced
    by type annotations on function parameters as explicit data dependencies. See
    documentation for `bauplan.Model` for more details on referencing declared models.

    Consider the following code example that defines two models, functions decorated with
    `bauplan.model`:

    ```python
    import bauplan

    class IotSchema(bauplan.TableSchema):
        '''Schema for result of `source_scan`.'''
        ...

    @bauplan.model(materialization_strategy='NONE')
    def source_scan(
        data: Annotated[
            pyarrow.Table,
            bauplan.Model('iot_kaggle', filter="motion='false'")
        ],
    ) -> Annotated[pyarrow.Table, IotSchema]:
        # your code here; schema of output should match `IotSchema`
        return data
    ```

    Parameters:
        name: the model identifier, defaults to the function name if not provided.
        partitioned_by: model columns to use for partitioning.
        materialization_strategy: how data should be written to the catalog, see
                                  `bauplan.ModelMaterializationStrategy`.
        cache_strategy: how data should be cached, see `bauplan.ModelCacheStrategy`.
        overwrite_filter: the overwrite filter expression.
        internet_access: flag to request internet access for the transformation logic.
    """

    def decorator(f: types.FunctionType) -> Callable:
        return f

    return decorator


# ------------------------------
# Model stubs

ModelSchema = TypeVar("ModelSchema", bound=TableSchema)


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
    #! import pyarrow
    import bauplan

    #! class MySchema(bauplan.TableSchema):
        #! my_col: bauplan.Int64

    @bauplan.model(name='bauplan.leaf_table')
    def leaf_model(
        catalog_table: Annotated[pyarrow.Table, Model('src_table')],
    ) -> Annotated[pyarrow.Table, MySchema]:
        return catalog_table.select(['my_col'])

    @bauplan.model()
    def my_model(
        leaf_data: Annotated[
            pyarrow.Table,
            Model(name='bauplan.leaf_table')
        ],
    ) -> Annotated[pyarrow.Table, MySchema]:
        return leaf_data
    ```

    There are two parameters supported to provide "pushdown" support for projections and
    selections.

    The `projection_schema` parameter specifies a schema, by identifier, to
    use as a projection (select column names to read into result table). Usage would look
    like: `Model('src_table', projection_schema=MySchema)`. Specifying a projection
    schema in the annotation for the `catalog_table` parameter would simplify the body of
    `leaf_model`:

    ```python type:ignore
    @bauplan.model(name='bauplan.leaf_table')
    def leaf_model(
        catalog_table: Annotated[
            pyarrow.Table,
            Model('src_table', projection_schema=MySchema)
        ],
    ) -> Annotated[pyarrow.Table, MySchema]:
        # `projection_schema` is the equivalent of `select(['my_col'])` with validation
        return catalog_table
    ```

    The `filter` parameter specifies a string-based SQL-like predicate that can be used
    to filter table rows from the input table. The predicate must be a string literal;
    usage would look like: `Model('src_table', filter='bar > 1')`.

    Parameters:
        name: The identifier of the model; accepted as a positional arg or as a keyword.
        projection_schema: A schema containing the columns to read for downstream use.
        filter: A SQL-like predicate, as a string literal, to filter rows from the model.
    """

    name: str
    projection_schema: Optional[type[TableSchema]] = None
    filter: Optional[LiteralString] = None
