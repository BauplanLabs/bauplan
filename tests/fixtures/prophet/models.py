from typing import Annotated

import bauplan
import pyarrow

from bauplan import (
    Date32,
    Float64,
    Int64,
    Model,
    TableField,
    TableSchema,
    TimestampMicroUTC,
    TimestampNano,
    TimestampNanoUTC,
)


class QueryModelSchema(TableSchema):
    """The columns query_model selects from taxi_fhvhv."""

    pickup_datetime: TimestampMicroUTC
    dropoff_datetime: TimestampMicroUTC
    PULocationID: Int64
    DOLocationID: Int64
    trip_miles: Float64
    trip_time: Int64
    base_passenger_fare: Float64
    tolls: Float64
    sales_tax: Float64
    tips: Float64


class TripsPushdown(TableSchema):
    """The trip columns normalize_data reads from query_model."""

    trip_time: Annotated[Int64, TableField(lineage=QueryModelSchema["trip_time"])]
    pickup_datetime: Annotated[
        TimestampMicroUTC, TableField(lineage=QueryModelSchema["pickup_datetime"])
    ]
    trip_miles: Annotated[Float64, TableField(lineage=QueryModelSchema["trip_miles"])]


class NormalizedTripsSchema(TableSchema):
    """Trips from 2023 on with a positive distance, plus a log transform and a date."""

    trip_time: Annotated[
        Int64,
        TableField(
            doc="Trip duration (in seconds), for trips from 2023 on.",
            lineage=TripsPushdown["trip_time"],
        ),
    ]
    pickup_datetime: Annotated[
        TimestampNanoUTC,
        TableField(
            doc="Pickup time, at nanosecond precision after the pandas round trip.",
        ),
    ]
    trip_miles: Annotated[
        Float64,
        TableField(
            doc="Miles traveled, for trips that covered more than zero of them.",
            lineage=TripsPushdown["trip_miles"],
        ),
    ]
    log_trip_miles: Annotated[
        Float64, TableField(doc="Base 10 logarithm of trip_miles.")
    ]
    ds: Annotated[Date32, TableField(doc="The pickup date, without a time of day.")]


class TrainingDatePushdown(TableSchema):
    """The date column training_dataset counts trips by."""

    ds: Annotated[Date32, TableField(lineage=NormalizedTripsSchema["ds"])]


class TrainingDatasetSchema(TableSchema):
    """Trips per day, the series the forecast is fit on."""

    ds: Annotated[Date32, TableField(lineage=TrainingDatePushdown["ds"])]
    y: Annotated[Int64, TableField(doc="Number of trips taken on ds.")]


class ForecastSchema(TableSchema):
    """Predicted daily trip counts, with the bounds of the prediction interval."""

    ds: Annotated[
        TimestampNano,
        TableField(doc="Day being predicted; extends past the training data."),
    ]
    yhat: Annotated[Float64, TableField(doc="Predicted number of trips on ds.")]
    yhat_lower: Annotated[Float64, TableField(doc="Lower bound of the prediction.")]
    yhat_upper: Annotated[Float64, TableField(doc="Upper bound of the prediction.")]


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11", pip={"pandas": "2.2.2"})
def normalize_data(
    data: Annotated[
        pyarrow.Table,
        Model("query_model", projection_schema=TripsPushdown),
    ],
) -> Annotated[pyarrow.Table, NormalizedTripsSchema]:
    print("===> Normalizing model <===")
    import sys

    import numpy as np
    import pandas as pd

    print("ciao gianx")
    print("Python version")
    print(sys.version)
    df = data.to_pandas()
    print("Total rows:", len(df))
    # clean up artifacts and add an explicit date column and a log transform
    df = df[df["pickup_datetime"] >= "2023-01-01"]
    df = df[df["trip_miles"] > 0.0]
    df["log_trip_miles"] = np.log10(df["trip_miles"])
    df["ds"] = pd.to_datetime(df["pickup_datetime"]).dt.date

    # Return a pyarrow.Table with a schema matching the return annotation
    return pyarrow.Table.from_pandas(df, preserve_index=False)


@bauplan.model(materialization_strategy="NONE")
@bauplan.python("3.11", pip={"pandas": "2.2.2"})
def training_dataset(
    data: Annotated[
        pyarrow.Table,
        Model("normalize_data", projection_schema=TrainingDatePushdown),
    ],
) -> Annotated[pyarrow.Table, TrainingDatasetSchema]:
    df = data.to_pandas()
    result = df.groupby("ds").size().reset_index(name="y")
    result = result.sort_values("ds").reset_index(drop=True)

    # Return a pyarrow.Table with a schema matching the return annotation
    return pyarrow.Table.from_pandas(result, preserve_index=False)


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python(
    "3.11",
    pip={"prophet": "1.1.4", "numpy": "1.26.4", "cmdstanpy": "1.2.5", "pandas": "2"},
)
def predict_trips(
    data: Annotated[
        pyarrow.Table,
        Model("training_dataset", projection_schema=TrainingDatasetSchema),
    ],
) -> Annotated[pyarrow.Table, ForecastSchema]:
    """
    We take the arg to be the parent dataframe,
    and run a time-series model on the "group by" by date.
    """
    import pandas as pd
    from prophet import Prophet  # ty: ignore[unresolved-import]

    print("===> Prediction model <===")
    m = Prophet()
    df = data.to_pandas()
    print("Preview for features:\n", df.head())
    print("\nTotal number of training weeks {}\n".format(len(df)))
    m.fit(df)
    future = m.make_future_dataframe(periods=20)
    forecast = m.predict(future)
    final_forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    print("Preview for output:\n", final_forecast.head())
    print("Total predictions: {}\n".format(len(final_forecast)))

    # Return a pyarrow.Table with a schema matching the return annotation
    return pyarrow.Table.from_pandas(final_forecast, preserve_index=False)
