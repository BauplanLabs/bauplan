SELECT
    tips,
    SUM(trip_miles) AS total_trip_distance
FROM
    taxi_fhvhv
WHERE
    pickup_datetime >= '2023-01-01T00:00:00-05:00'
    AND pickup_datetime < '2023-02-01T00:00:00-05:00'
    AND DOLocationID IN (
        SELECT
            DOLocationID
        FROM
            taxi_fhvhv
        WHERE
            pickup_datetime >= '2023-01-01T00:00:00-05:00'
            AND pickup_datetime < '2023-02-01T00:00:00-05:00'
        GROUP BY
            DOLocationID
        HAVING
            SUM(trip_miles) > 40000)
GROUP BY
    tips
