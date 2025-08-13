-- SQL statement to filter only new subscribers
SELECT *
FROM cleansed_subscribers
WHERE 
    sub_id NOT IN  (
        SELECT DISTINCT sub_id
        FROM subscribers_from_db
    )