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
    print_postgres_table,
)

# -------------------------------------SETUP---------------------------------- #

# Get date from  dag run (to  be used as part of the exported parquet filename)
ds = os.environ.get("ds", "2021-01-01a_")

# Initiate spark session and provide the sqlite driver jdbc connection
spark = (
    SparkSession.builder.appName("optasia-jde-assessment")
    .config("spark.jars", "jars/postgresql-42.7.1.jar")
    .getOrCreate()
)

# ----------------------------LOAD DATA & INSPECTION-------------------------- #

# Load both csv's to spark dataframes (no headers)
df_data_transactions = spark.read.option("header", "false").csv(
    "data/input/data_transactions.csv"
)
df_subscribers = spark.read.option("header", "false").csv("data/input/subscribers.csv")

# Check schemas of both dataframes
df_data_transactions.printSchema()
df_subscribers.printSchema()

# All fields have been imported as strings

# ----------------------------------CLEAN DATA-------------------------------- #

# Create temp views of both dataframes in order to work with Spark SQL
df_data_transactions.createOrReplaceTempView("transactions")
df_subscribers.createOrReplaceTempView("subscribers")

# Clean subscribers data
df_cleansed_subscribers = spark.sql(read_sql_file("src/sql/clean_subscribers.sql"))

# Create a temp view out of cleansed subscribers in order to work with Spark SQL
df_cleansed_subscribers.createOrReplaceTempView("cleansed_subscribers")

# Clean transactions and create a view out of the cleansed transactions,
#  in order to work with Spark SQL
spark.sql(read_sql_file("src/sql/clean_transactions.sql")).createOrReplaceTempView(
    "cleansed_data_transactions"
)

# -------------------------INSERT NEW SUBSCRIBERS TO DB----------------------- #

# Get existing subscribers table (lazy evaluation)
read_table_from_db(
    spark, "subscribers", "subscribers_from_db", df_cleansed_subscribers.schema
)

# Identify new subscriber entries
df_only_new_cleansed_subscribers = spark.sql(
    read_sql_file("src/sql/get_new_subscribers.sql")
)

# Overwrite old subscribers table with new one
write_df_to_db_table(df_only_new_cleansed_subscribers, "subscribers", "append")

# ----------------COMBINE NEW TRANSACTIONS WITH PENDING ONES------------------ #

# Read unmatched transactions from the database
read_table_from_db(
    spark,
    "unmatched_transactions",
    "unmatched_transactions",
    df_data_transactions.schema,
)

# Combine new transactions with unmatched ones
spark.sql(
    read_sql_file("src/sql/union_unmatched_with_cleansed_transactions.sql")
).createOrReplaceTempView("cleansed_data_transactions")

# ---------DETERMINE AND WRITE UNMATCHED TRANSACTIONS TO DATABASE------------- #

# Create a dataframe out of the transactions without a matching sub_id
df_unmatched_transactions = spark.sql(
    read_sql_file("src/sql/extract_unmatching_transactions.sql")
)

# save  unmatched transactions to db
write_df_to_db_table(df_unmatched_transactions, "unmatched_transactions", "overwrite")

# --------------------JOIN TABLES AND WRITE TO PARQUET----------------------- #

# Read the subscribers table again (Now contains new subscribers)
read_table_from_db(
    spark, "subscribers", "cleansed_subscribers", df_cleansed_subscribers.schema
)

# Join both tables and write to parquet
spark.sql(read_sql_file("src/sql/join.sql")).write.mode("overwrite").parquet(
    f"data/output/{ds}final_transactions.parquet"
)

# ----------------------PRINT TABLES TO VERIFY RESULTS------------------------- #

# Print subscribers and unmatched_transactions tables as well as transactions parquet
print_postgres_table("localhost", "subscribers")
print_postgres_table("localhost", "unmatched_transactions")
print_parquet_file(spark, "data/output/2021-01-01a_final_transactions.parquet")
