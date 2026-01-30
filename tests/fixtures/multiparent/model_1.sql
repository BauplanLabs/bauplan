SELECT
    CASE
        WHEN PULocationID IS NOT NULL THEN 'val_1'
        ELSE 'val_1'
    END AS col_1
FROM taxi_fhvhv
LIMIT 1
