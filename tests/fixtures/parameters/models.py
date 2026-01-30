import bauplan
import pyarrow as pa


@bauplan.model(
    columns=['y'],
    materialization_strategy='NONE',
)
@bauplan.python('3.11')
def params_are_cool_model(
    yayparams=bauplan.Model(  # noqa: B008, ANN001
        'query_model',
        columns=[
            'dropoff_datetime',
            'pickup_datetime',
            'trip_miles',
        ],
        filter='PULocationID = $location_id',
    ),
    golden_ratio=bauplan.Parameter('golden_ratio'),  # noqa: B008, ANN001
    use_random_forest=bauplan.Parameter('use_random_forest'),  # noqa: B008, ANN001
    start_datetime=bauplan.Parameter('start_datetime'),  # noqa: B008, ANN001
    end_datetime=bauplan.Parameter('end_datetime'),  # noqa: B008, ANN001
) -> pa.Table:
    print(f'golden_ratio={golden_ratio}')
    print(f'use_random_forest={use_random_forest}')
    print(f'start_datetime={start_datetime}')
    print(f'end_datetime={end_datetime}')

    print(yayparams)
    print(f'yayparams.num_rows={yayparams.num_rows}')
    print(f'yayparams.num_columns={yayparams.num_columns}')

    return pa.Table.from_pydict({'y': [1, 2, 3]})
