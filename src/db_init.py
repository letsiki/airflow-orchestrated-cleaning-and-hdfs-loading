"""
Module responsible for initializing tables in the PG database
"""

import psycopg2
import os

pghost = os.getenv("PGHOST", "postgres")  # <- outer host via published port
pgport = os.getenv("PGPORT", 5432)

# Connect to database
conn = psycopg2.connect(
    host=pghost, database="optasia", user="testuser", password="password", port=pgport
)

with open("src/sql/db_init.sql", "r", encoding="utf-8") as file:
    sql = file.read()

cur = conn.cursor()
cur.execute(sql)
conn.commit()
cur.close()
conn.close()
