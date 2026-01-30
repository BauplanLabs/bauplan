-- bauplan: materialization_strategy = NONE
SELECT
    ds,
    COUNT(*) AS y
FROM
    normalize_data
GROUP BY ds
ORDER BY ds ASC
