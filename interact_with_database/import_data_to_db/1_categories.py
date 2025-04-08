import psycopg2
import csv
from psycopg2.extras import execute_batch  # Tối ưu hóa bulk insert


def batch_insert_categories():
    conn = psycopg2.connect(
        host="localhost",
        database="oltp_ecommerce_db",
        user="admin",
        password="admin",
        port="5433"
    )

    with conn.cursor() as cur, open('/data/categories.csv', 'r') as f:
        reader = csv.DictReader(f)

        # Chuẩn bị data batch (mỗi batch 1000 records)
        batch = []
        batch_size = 1000

        for row in reader:
            # Xử lý giá trị NULL cho parent_category_id
            parent_id = int(row['parent_category_id']) if row['parent_category_id'].strip() else None

            batch.append((
                int(row['category_id']),
                row['category_name'],
                parent_id,
                row['created_time'],
                row['updated_time']
            ))

            if len(batch) >= batch_size:
                execute_batch(
                    cur,
                    """
                    INSERT INTO categories 
                    (category_id, category_name, parent_category_id, created_time, updated_time)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    batch
                )
                batch = []

        # Insert các record còn lại
        if batch:
            execute_batch(cur, """
                INSERT INTO categories 
                (category_id, category_name, parent_category_id, created_time, updated_time)
                VALUES (%s, %s, %s, %s, %s)
                """, batch)

        conn.commit()


batch_insert_categories()