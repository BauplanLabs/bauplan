-- bauplan: materialization_strategy = NONE
SELECT
    DISTINCT PULocationID AS location_id
FROM
    taxi_fhvhv
WHERE
    pickup_datetime >= '2023-01-01T00:00:00-05:00' AND
    pickup_datetime < '2023-01-02T00:00:00-05:00'
