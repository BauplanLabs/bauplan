import bauplan


@bauplan.model(
    columns=[
        'trip_time',
        'pickup_datetime',
        'trip_miles',
        'log_trip_miles',
        'ds',
    ],
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
    print('===> Normalizing model <===')
    import sys

    import numpy as np
    import pandas as pd
    import pyarrow as pa

    print('ciao gianx')
    print('Python version')
    print(sys.version)
    df = data.to_pandas()
    print('Total rows:', len(df))
    # clean up artifacts and add an explicit date column and a log transform
    df = df[df['pickup_datetime'] >= '2023-01-01']
    df = df[df['trip_miles'] > 0.0]
    df['log_trip_miles'] = np.log10(df['trip_miles'])
    df['ds'] = pd.to_datetime(df['pickup_datetime']).dt.date

    return df


@bauplan.model(
    columns=[
        'ds',
        'yhat',
        'yhat_lower',
        'yhat_upper',
    ],
    materialization_strategy='REPLACE',
)
@bauplan.python('3.11', pip={'prophet': '1.1.4', 'numpy': '1.26.4', 'cmdstanpy': '1.2.5', 'pandas': '2'})
def predict_trips(
    data=bauplan.Model(
        'training_dataset',
        columns=['ds', 'y'],
    ),
):
    """
    We take the arg to be the parent dataframe,
    and run a time-series model on the "group by" by date.
    """
    import pandas as pd
    import pyarrow as pa
    from prophet import Prophet

    print('===> Prediction model <===')
    m = Prophet()
    df = data.to_pandas()
    print('Preview for features:\n', df.head())
    print('\nTotal number of training weeks {}\n'.format(len(df)))
    m.fit(df)
    future = m.make_future_dataframe(periods=20)
    forecast = m.predict(future)
    final_forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    print('Preview for output:\n', final_forecast.head())
    print('Total predictions: {}\n'.format(len(final_forecast)))

    return final_forecast
