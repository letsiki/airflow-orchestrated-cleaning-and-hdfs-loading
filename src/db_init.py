"""
Module responsible for initializing tables in the PG database
"""

import psycopg2

# Connect to database
conn = psycopg2.connect(
    host="postgres", database="optasia", user="testuser", password="password"
)

with open("src/sql/db_init.sql", "r", encoding="utf-8") as file:
    sql = file.read()

cur = conn.cursor()
cur.execute(sql)
conn.commit()
cur.close()
conn.close()
