import time

import bauplan


@bauplan.model(
    columns=[
        'trip_time',
        'pickup_datetime',
        'trip_miles',
        'log_trip_miles',
        'ds',
    ],
    materialization_strategy='REPLACE',
)
@bauplan.python('3.11', pip={'pandas': '2.2.2'})
def normalize_data(
    data=bauplan.Model(
        'query_model',
        columns=[
            'trip_time',
            'pickup_datetime',
            'trip_miles',
        ],
    ),
):
    time.sleep(30)
    return data
