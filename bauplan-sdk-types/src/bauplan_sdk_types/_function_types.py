import types

from typing import (
    Callable,
    Literal,
    Optional,
    Union,
)


ModelCacheStrategy = Literal["NONE", "DEFAULT"]
ModelMaterializationStrategy = Literal[
    "NONE", "REPLACE", "APPEND", "OVERWRITE_PARTITIONS"
]


def model(
    name: Optional[str] = None,
    partitioned_by: Optional[Union[str, list[str], tuple[str, ...]]] = None,
    materialization_strategy: Optional[ModelMaterializationStrategy] = None,
    cache_strategy: Optional[ModelCacheStrategy] = None,
    overwrite_filter: Optional[str] = None,
    internet_access: Optional[bool] = None,
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
    from typing import Annotated
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


def expectation() -> Callable:
    """
    Decorator that defines a Bauplan expectation.

    An expectation is a function from one (or more) dataframe-like object(s) to a boolean: it
    is commonly used to perform data validation and data quality checks when running a pipeline.
    Expectations take as input the table(s) they are validating and return a boolean indicating
    whether the expectation is met or not. Additionally, assert statements in the
    function body halt the pipeline immediately on failure.

    ```python
    from typing import Annotated

    import bauplan
    import pyarrow

    from bauplan.standard_expectations import expect_column_no_nulls

    class AnomalySchema(bauplan.TableSchema):
        '''The columns of `join_dataset` this expectation reads.'''

        anomaly: Annotated[
            bauplan.Bool,
            bauplan.TableField(
                doc=(
                  "An example column of some arbitrary datatype, "
                  "but is expected to not have any null values. "
                )
            )
        ],

    @bauplan.expectation()
    @bauplan.python('3.11')
    def test_joined_dataset(
        data: Annotated[
            pyarrow.Table,
            bauplan.Model('join_dataset', projection_schema=AnomalySchema),
        ],
    ) -> bool:
        # your data validation code here
        # ...

        # use assertions to stop the pipeline in critical scenarios
        assert data.num_rows > 0

        return expect_column_no_nulls(data, 'anomaly')
    ```
    """

    def decorator(f: Callable) -> Callable:
        return f

    return decorator
