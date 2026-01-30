import bauplan
import pyarrow as pa


@bauplan.model(
    columns=['magicval_field'],
    materialization_strategy='REPLACE',
)
@bauplan.python('3.11')
def materialized_table_model(
    sad_unused_parent=bauplan.Model(  # noqa: B008, ANN001
        'query',
        columns=['dropoff_datetime'],
    ),
    magicval=bauplan.Parameter('magicval'),  # noqa: ANN001, B008
) -> pa.Table:
    return pa.Table.from_pydict({'magicval_field': [magicval]})
