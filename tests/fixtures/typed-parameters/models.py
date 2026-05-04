# Force the type-contracts feature on regardless of the compile-time flag,
# so that this fixture can be used as an idempotent test.
# Must be set before bauplan._internal is imported.
import os

os.environ['BPLN_ENABLE_TYPE_CONTRACT'] = 'True'

import bauplan
import pyarrow as pa

from bauplan import Parameter


@bauplan.model(
    columns=['y'],
    materialization_strategy='NONE',
)
@bauplan.python('3.13')
def typed_params_model(
    yayparams=bauplan.Model(  # noqa: B008, ANN001
        'query_model',
        columns=[
            'dropoff_datetime',
            'pickup_datetime',
            'trip_miles',
        ],
        filter='PULocationID = $location_id',
    ),
    golden_ratio=Parameter.golden_ratio,
    use_random_forest=Parameter.use_random_forest,
    start_datetime=Parameter.start_datetime,
    end_datetime=Parameter.end_datetime,
) -> pa.Table:
    print(f'golden_ratio={golden_ratio}')
    print(f'use_random_forest={use_random_forest}')
    print(f'start_datetime={start_datetime}')
    print(f'end_datetime={end_datetime}')

    print(yayparams)
    print(f'yayparams.num_rows={yayparams.num_rows}')
    print(f'yayparams.num_columns={yayparams.num_columns}')

    return pa.Table.from_pydict({'y': [1, 2, 3]})
