import bauplan


@bauplan.expectation()
@bauplan.python('3.11')
def test_trip_miles_mean(
    data=bauplan.Model(
        'query_model',
        columns=['trip_miles'],
    ),
):
    return False


@bauplan.model(
    columns=['trip_time', 'pickup_datetime', 'trip_miles'],
    materialization_strategy='NONE',
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
    df = data.to_pandas()
    return df
