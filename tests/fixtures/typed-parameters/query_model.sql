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
    pickup_datetime >= $start_datetime
AND pickup_datetime < $end_datetime
