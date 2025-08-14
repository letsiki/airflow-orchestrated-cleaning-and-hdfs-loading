import psycopg2

# Quick connection and execution
conn = psycopg2.connect(
    host="localhost",
    database="optasia", 
    user="testuser",
    password="password"
)

with open('src/sql/db_init.sql', 'r') as file:
    sql = file.read()

cur = conn.cursor()
cur.execute(sql)
conn.commit()
cur.close()
conn.close()