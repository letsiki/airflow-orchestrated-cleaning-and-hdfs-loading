"""
Module responsible for cleaning, enriching, joining and loading
our raw csv data
"""

from pyspark.sql import SparkSession

# inititiate spark session and provide the sqlite driver jdbc connection
spark = (
    SparkSession.builder.appName("optasia-jde-assessment")
    .config("spark.jars", "jars/sqlite-jdbc-3.44.1.0.jar")
    .getOrCreate()
)

df_data_transactions = spark.read.option("header", "false").csv(
    "data/input/data_transactions.csv"
)
df_subscribers = spark.read.option("header", "false").csv("data/input/subscribers.csv")

# for df in df_data_transactions, df_subscribers:
#     df.printSchema()
#     df.show(5)
#     print(df.count())

# check schrmas of both dataframes
df_data_transactions.printSchema()
df_subscribers.printSchema()

# all fields appear as strings

df_data_transactions.createOrReplaceTempView("transactions")
df_subscribers.createOrReplaceTempView("subscribers")

# In the cleaning transformation below, we are removing duplicate
# entries of subscribers keeping only the first activation date.
# To do this we use GROUP BY and the MIN aggregate function and
# store the result as a CTE called cleansed_subs.
#
# In addition, in order to comply with the required schema, we
# convert the act_date column from string to DATE (still in the CTE)
#
# Finally we enrich the data with the row_key column which is a
# concatenation of sub_id and act_date formatted as yyyyMMdd
spark.sql(
    """
    with cleansed_subs as (
    SELECT _c0 as sub_id, min(CAST(_c1  as DATE)) as act_dt
    FROM subscribers
    GROUP BY _c0
    )
    SELECT 
        CONCAT(sub_id, '_', date_format(act_dt, 'yyyyMMdd')) as row_key,
        sub_id,
        act_dt
    FROM cleansed_subs
    """
    # Writing to subscribers table in sqlite, also providing the sqlite driver
).write.format("jdbc").option("url", "jdbc:sqlite:db/data_warehouse.db").option(
    "dbtable", "subscribers"
).option(
    "driver", "org.sqlite.JDBC"
).mode(
    "overwrite"
).save()
