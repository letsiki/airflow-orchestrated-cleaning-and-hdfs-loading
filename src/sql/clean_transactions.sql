-- for the data_transactions table our cleaning choices are harder, and would normally
-- require some sort of discussion-agreement with downstream stakeholders (most likely data scientists)
-- Since I am on my own in this, I am making the following choices:
-- - At Optasia, we are concerned with analytics (OLAP), I would feel comfortable to accept some inaccuracies
-- in the data and use imputation strategies to resolve them.
-- - I would drop rows with nulls as this usually means not only that our data is missing, but that it also ended up
-- in the wrong column.
-- - drop rows with malformed amounts, as this data is critical
-- - use interpolation (mode) for both channel and timestamp
-- - For date validation, I kept it generic (format-only), will refine further after discussion with stakeholders
WITH 
-- Find most common channel
channel_stats AS (
  SELECT _c3 as channel, COUNT(*) as cnt
  FROM transactions 
  WHERE _c3 IS NOT NULL AND _c3 != ''
  GROUP BY _c3
),
channel_mode AS (
  SELECT channel as mode_channel
  FROM channel_stats
  ORDER BY cnt DESC
  LIMIT 1
),

-- Find most common date
date_stats AS (
  SELECT date_trunc('day', CAST(_c0 as TIMESTAMP)) as batch_date, COUNT(*) as cnt
  FROM transactions 
  WHERE _c0 RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'
  GROUP BY date_trunc('day', CAST(_c0 as TIMESTAMP))
),
date_mode AS (
  SELECT batch_date as mode_date
  FROM date_stats
  ORDER BY cnt DESC
  LIMIT 1
),

-- Apply cleaning
cleansed_transactions AS (
  SELECT 
    CASE 
      WHEN _c0 RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*' THEN CAST(_c0 as TIMESTAMP)
      ELSE TIMESTAMP(CONCAT(date_format(dm.mode_date, 'yyyy-MM-dd'), ' 00:00:00'))
    END as timestamp,
    _c1 as subscriber_id,
    CAST(_c2 as DECIMAL(10,4)) as amount,
    CASE 
      WHEN _c3 IS NULL OR _c3 NOT RLIKE '^[A-Za-z]+$' THEN cm.mode_channel
      ELSE _c3 
    END as channel
  FROM transactions t
  CROSS JOIN channel_mode cm
  CROSS JOIN date_mode dm
  WHERE _c2 RLIKE '^[0-9]+\.?[0-9]*$'  -- valid amounts only
)


SELECT * FROM cleansed_transactions