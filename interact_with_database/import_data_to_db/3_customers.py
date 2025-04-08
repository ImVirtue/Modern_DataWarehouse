import psycopg2
from psycopg2.extras import execute_batch

conn = psycopg2.connect(
    host="localhost",
    database="oltp_ecommerce_db",
    user="admin",
    password="admin",
    port="5433"
)

with conn.cursor() as cur, open('/data/customers_data.csv', 'r') as f:
    cur.copy_expert("COPY customers FROM STDIN WITH CSV HEADER", f)
    conn.commit()