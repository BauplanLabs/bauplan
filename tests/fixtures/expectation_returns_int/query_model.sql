-- bauplan: materialization_strategy = NONE
SELECT
    pickup_datetime,
    dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_miles,
    trip_time,
    base_passenger_fare,
    tolls,
    sales_tax,
    tips
FROM
    taxi_fhvhv
WHERE
    pickup_datetime >= '2023-01-01T00:00:00-05:00' AND
    pickup_datetime < '2023-01-02T00:00:00-05:00'
