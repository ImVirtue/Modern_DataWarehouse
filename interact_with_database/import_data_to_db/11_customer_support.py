import psycopg2
from psycopg2.extras import execute_batch

conn = psycopg2.connect(
    host="localhost",
    database="oltp_ecommerce_db",
    user="admin",
    password="admin",
    port="5433"
)

with conn.cursor() as cur, open('/data/customer_support_tickets.csv', 'r') as f:
    cur.copy_expert("COPY customer_support FROM STDIN WITH CSV HEADER", f)
    conn.commit()