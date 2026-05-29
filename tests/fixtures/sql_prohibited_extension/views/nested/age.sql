-- bauplan: name=age_nulls
SELECT COUNT(*) - COUNT(Age) AS n_nulls_age FROM titanic
