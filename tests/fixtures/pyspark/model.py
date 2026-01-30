import bauplan


def _converter(tips_as_double):
    """
    This is a UDF for PySpark to convert the tips column to a binary flag.
    """
    if tips_as_double > 0.0:
        return 1
    return 0


@bauplan.python('3.10', pip={'pandas': '2.2.1'})
@bauplan.pyspark(version='3.5.1', conf={'spark.driver.memory': '16g', 'spark.driver.maxResultSize': '8g'})
@bauplan.model(internet_access=True)
def get_trips_spark(
    spark=None,
    data=bauplan.Model(
        'taxi_fhvhv',
        columns=['trip_miles', 'trip_time', 'tips', 'pickup_datetime'],
        filter="pickup_datetime >= '2023-01-02T00:00:00-05:00' AND pickup_datetime < '2023-02-04T00:00:00-05:00'",
    ),
):
    # import pandas (we need it to convert the data to a Spark DataFrame from arrow because
    # Spark being clowns etc.)
    import pandas as pd
    from pyspark.sql.functions import udf

    # register the UDF
    NumberUDF = udf(lambda m: _converter(m))

    df = spark.createDataFrame(data.to_pandas())
    df = df.withColumn('tip_flag', NumberUDF('tips'))
    df = df.drop('tips', 'pickup_datetime', 'trip_miles', 'trip_time')

    print("\n====> Look, I'm in the spark model now!\n")

    return df


@bauplan.model()
@bauplan.python('3.10', pip={'pandas': '2.2.1'})
def class_imbalance_spark(data=bauplan.Model('get_trips_spark')):
    """
    This just shows the class imbalance in the tip_flag column, to prove
    data FROM PySpark can be used in a Python model in Bauplan.
    """
    import pandas as pd

    df = data.to_pandas()
    df = df.groupby('tip_flag').size().reset_index(name='count_flag').sort_values(by='tip_flag')

    return df
