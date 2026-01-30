import bauplan

key_1 = 'test_key_1'


@bauplan.expectation()
@bauplan.python('3.11')
def test_trip_miles_mean(
    data=bauplan.Model(
        'normalize_data',
        columns=['trip_miles'],
    ),
):
    saved_obj = bauplan.store.load_obj(key_1)
    assert saved_obj['foo'] == 3
    return True


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
    obj_1 = {'foo': 3}
    bauplan.store.save_obj(key_1, obj_1)

    df = data.to_pandas()
    return df
