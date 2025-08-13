-- In the cleaning transformation below, we are removing duplicate
-- entries of subscribers keeping only the first activation date.
-- To do this we use GROUP BY and the MIN aggregate function and
-- store the resulting table as a CTE called cleansed_subs.
-- We are also removing NULL values for  the sub_id columns as data
-- is meaningless without it.

-- In addition, in order to comply with the required schema, we
-- convert the act_date column from string to DATE (still in the CTE)

-- Finally we enrich the data with the row_key column which is a
-- concatenation of sub_id and act_date formatted as yyyyMMdd
WITH cleansed_subs AS (
SELECT _c0 as sub_id, min(CAST(_c1  as DATE)) as act_dt
FROM subscribers
WHERE _c0 is not NULL
GROUP BY _c0
)
SELECT 
    CONCAT(sub_id, '_', date_format(act_dt, 'yyyyMMdd')) AS row_key,
    sub_id,
    act_dt
FROM cleansed_subs