"""
Module responsible for cleaning, enriching, joining and loading
our raw csv data
"""

from pyspark.sql import SparkSession

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


def read_sql_file(file_path):
    """Helper function that converts sql files to string"""
    with open(file_path, "r", encoding='utf-8') as file:
        return file.read()


df_cleansed_subscribers = spark.sql(read_sql_file("src/sql/clean_subscribers.sql"))
# Writing to subscribers table in sqlite, also providing the sqlite driver
df_cleansed_subscribers.write.format("jdbc").option(
    "url", "jdbc:sqlite:db/data_warehouse.db"
).option("dbtable", "subscribers").option("driver", "org.sqlite.JDBC").mode(
    "overwrite"
).save()

df_cleansed_subscribers.createOrReplaceTempView("cleansed_subscribers")


spark.sql(read_sql_file("src/sql/clean_transactions.sql")).createOrReplaceTempView(
    "cleansed_data_transactions"
)


# at this point we have both cleansed tables in memory and subscribers also stored in sqlite
# we will use the ones in memory for the join (faster) and save the result to a parquet file.
spark.sql(read_sql_file("src/sql/join.sql")).write.mode("overwrite").parquet(
    "data/output/final_transactions.parquet"
)
