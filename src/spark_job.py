"""
Module responsible for cleaning, enriching, joining and loading
our raw csv data
"""

import os
from pyspark.sql import SparkSession
from utilities import (
    write_df_to_db_table,
    read_table_from_db,
    read_sql_file,
    print_parquet_file,
    print_sqlite_table,
)

ds = os.environ.get("ds", "2021-01-01a_")

# Initiate spark session and provide the sqlite driver jdbc connection
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

# Clean subscribers data
df_cleansed_subscribers = spark.sql(read_sql_file("src/sql/clean_subscribers.sql"))

# Create a temp view out of cleansed subscribers in order to work with Spark SQL
df_cleansed_subscribers.createOrReplaceTempView("cleansed_subscribers")

# Read from SQLite subscribers table and create temp view
read_table_from_db(
    spark, "subscribers", "subscribers_from_db", df_cleansed_subscribers.schema
)


# Filter out existing subscribers
df_only_new_cleansed_subscribers = spark.sql(
    read_sql_file("src/sql/get_new_subscribers.sql")
)

# Appending new subscribers to the subscribers table in sqlite,
# also providing the sqlite driver. In case the table does not exist, it is created
write_df_to_db_table(df_only_new_cleansed_subscribers, "subscribers", "append")

# create a view out of the cleansed transactions
spark.sql(read_sql_file("src/sql/clean_transactions.sql")).createOrReplaceTempView(
    "cleansed_data_transactions"
)

# Read unmatched transactions from the database
read_table_from_db(
    spark,
    "unmatched_transactions",
    "unmatched_transactions",
    df_data_transactions.schema,
)

# Append old unmatched transactions to the new (cleansed) transactions
# Create a dataframe out of the transactions without a matching sub_id
spark.sql(read_sql_file("src/sql/union_unmatched_with_cleansed_transactions.sql"))

# Create a dataframe out of the transactions without a matching sub_id
df_unmatched_transactions = spark.sql(
    read_sql_file("src/sql/extract_unmatching_transactions.sql")
)

# save  unmatched transactions to db
write_df_to_db_table(df_unmatched_transactions, "unmatched_transactions", "overwrite")

# Read the subscribers table again (Now contains new subscribers)
read_table_from_db(
    spark, "subscribers", "cleansed_subscribers", df_cleansed_subscribers.schema
)

# at this point we have both cleansed tables in memory and subscribers also stored in sqlite
# we will use the ones in memory for the join (faster) and save the result to a parquet file.
spark.sql(read_sql_file("src/sql/join.sql")).write.mode("overwrite").parquet(
    f"data/output/{ds}final_transactions.parquet"
)

# Print subscribers and unmatched_transactions tables as well as transactions parquet
print_sqlite_table("db/data_warehouse.db", "subscribers")
print_sqlite_table("db/data_warehouse.db", "unmatched_transactions")
print_parquet_file(spark, "data/output/2021-01-01a_final_transactions.parquet")