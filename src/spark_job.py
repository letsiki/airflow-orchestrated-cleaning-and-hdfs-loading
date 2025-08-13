"""
Module responsible for cleaning, enriching, joining and loading
our raw csv data
"""

from pyspark.sql import SparkSession

# Inititiate spark session and provide the sqlite driver jdbc connection
spark = (
    SparkSession.builder.appName("optasia-jde-assessment")
    .config("spark.jars", "jars/sqlite-jdbc-3.44.1.0.jar")
    .getOrCreate()
)

# Load both csv's to spark dataframes (no headers)
df_data_transactions = spark.read.option("header", "false").csv(
    "data/input/data_transactions.csv"
)
df_subscribers = spark.read.option("header", "false").csv("data/input/subscribers.csv")

# Check schemas of both dataframes
df_data_transactions.printSchema()
df_subscribers.printSchema()

# All fields have been imported as strings

# Create temp views of both dataframes in order to work with Spark SQL
df_data_transactions.createOrReplaceTempView("transactions")
df_subscribers.createOrReplaceTempView("subscribers")

# In the cleaning transformation below, we are removing duplicate
# entries of subscribers keeping only the first activation date.
# To do this we use GROUP BY and the MIN aggregate function and
# store the result as a CTE called cleansed_subs.
# We are also removing NULL values for  the sub_id columns as data
# is meaningless without it.
#
# In addition, in order to comply with the required schema, we
# convert the act_date column from string to DATE (still in the CTE)
#
# Finally we enrich the data with the row_key column which is a
# concatenation of sub_id and act_date formatted as yyyyMMdd
df_cleansed_subscribers = spark.sql(
    """
    with cleansed_subs as (
    SELECT _c0 as sub_id, min(CAST(_c1  as DATE)) as act_dt
    FROM subscribers
    WHERE _c0 is not NULL
    GROUP BY _c0
    )
    SELECT 
        CONCAT(sub_id, '_', date_format(act_dt, 'yyyyMMdd')) as row_key,
        sub_id,
        act_dt
    FROM cleansed_subs
    """
    # Writing to subscribers table in sqlite, also providing the sqlite driver
)

df_cleansed_subscribers.write.format("jdbc").option(
    "url", "jdbc:sqlite:db/data_warehouse.db"
).option("dbtable", "subscribers").option("driver", "org.sqlite.JDBC").mode(
    "overwrite"
).save()

df_cleansed_subscribers.createOrReplaceTempView("cleansed_subscribers")

# for the data_transactions table our cleaning choices are harder, and would normally
# require some sort of discussion-agreement with downstream stakeholders (most likely data scientists)
# Since I am on my own in this, I am making the following choices:
# - At Optasia, we are concerned with analytics (OLAP), I would feel comfortable to accept some inaccuracies
# in the data and use imputation strategies to resolve them.
# - I would drop rows with nulls as this usually means not only that our data is missing, but that it also ended up
# in the wrong column.
# - drop rows with malformed amounts, as this data is critical
# - use interpolation (mode) for both channel and timestamp
# - For date validation, I kept it generic (format-only), will refine further after discussion with stakeholders
spark.sql(
    """
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
"""
).createOrReplaceTempView("cleansed_data_transactions")


# at this point we have both cleansed tables in memory and subscribers also stored in sqlite
# we will use the ones in memory for the join (faster) and save the result to a parquet file.
spark.sql(
    """
    SELECT 
        cdt.timestamp,
        cs.row_key as sub_row_key,
        cs.sub_id,
        cdt.amount,
        cdt.channel,
        date_format(cs.act_dt, 'yyyyMMdd') as activation_date
    FROM cleansed_data_transactions cdt
    JOIN cleansed_subscribers cs on cs.sub_id = cdt.subscriber_id
    """
).write.mode("overwrite").parquet("data/output/final_transactions.parquet")
