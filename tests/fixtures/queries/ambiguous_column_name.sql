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
    taxi_fhvhv AS taxi_A
    JOIN taxi_fhvhv AS taxi_B ON taxi_A.PULocationID == taxi_B.DOLocationID
