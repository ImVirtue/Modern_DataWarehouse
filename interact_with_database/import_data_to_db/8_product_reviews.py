import psycopg2
from psycopg2.extras import execute_batch

conn = psycopg2.connect(
    host="localhost",
    database="oltp_ecommerce_db",
    user="admin",
    password="admin",
    port="5433"
)

with conn.cursor() as cur, open('/data/product_reviews.csv', 'r') as f:
    cur.copy_expert("COPY product_review FROM STDIN WITH CSV HEADER", f)
    conn.commit()