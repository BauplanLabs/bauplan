import bauplan
import pyarrow as pa


@bauplan.model(
    columns=['dropoff_datetime'],
    partitioned_by=['YEAR(dropoff_datetime)', 'PULocationID'],
    materialization_strategy='REPLACE',
)
def materialized_table_model(
    parent=bauplan.Model(
        'taxi_fhvhv',
        columns=['dropoff_datetime', 'PULocationID'],
        filter="dropoff_datetime >= '2023-01-01T00:00:00-00:00' AND dropoff_datetime < '2023-01-02T00:00:00-00:00'",
    ),
) -> pa.Table:
    return parent
