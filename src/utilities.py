"""
module providing helper functions for our spark job
"""

import sqlite3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from py4j.protocol import Py4JJavaError
import psycopg2


def read_sql_file(file_path: str) -> str:
    """Helper function that converts sql files to string"""
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def read_table_from_db(
    spark: SparkSession, dbtable: str, temp_table: str, fallback_schema: StructType
):
    """Helper function for reading tables from the database"""
    try:
        spark.read.format("jdbc").option(
            "url", "jdbc:postgresql://postgres:5432/optasia"
        ).option("dbtable", dbtable).option("driver", "org.postgresql.Driver").option(
            "user", "testuser"
        ).option(
            "password", "password"
        ).load().createOrReplaceTempView(
            temp_table
        )
    except Py4JJavaError as e:
        print(f"Error reading {dbtable} table: {e}")
        print("Creating Empty View")
        # Create empty temp view with same schema
        spark.createDataFrame([], fallback_schema).createOrReplaceTempView(temp_table)


def write_df_to_db_table(df: DataFrame, dbtable: str, mode: str):
    """Helper function for writing a dataframe to a db table"""
    df.write.format("jdbc").option(
        "url", "jdbc:postgresql://postgres:5432/optasia"
    ).option("dbtable", dbtable).option("driver", "org.postgresql.Driver").option(
        "user", "testuser"
    ).option(
        "password", "password"
    ).mode(
        mode
    ).save()


def print_sqlite_table(db_path: str, table_name: str):
    """Print contents of SQLite table"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print(f"Printing contents of {table_name}".center(50, "-"))
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    conn.close()
    print("\n")


def print_postgres_table(host: str, table_name: str):
    """Print contents of PostgreSQL table"""

    conn = psycopg2.connect(
        host=host,  # Use postgres inside a docker network
        port="5432",
        database="optasia",
        user="testuser",
        password="password",
    )
    cursor = conn.cursor()
    print(f"Printing contents of {table_name}".center(50, "-"))
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    conn.close()
    print("\n")


def print_parquet_file(spark: SparkSession, file_path: str, max_results: int = 200):
    """Print contents of Parquet file"""
    print("Printing Contents of Parquet File")
    df = spark.read.parquet(file_path)
    df.show(max_results)
