import psycopg2
import csv
from psycopg2.extras import execute_batch


def batch_insert_loyalty_program():
    # Database connection
    conn = psycopg2.connect(
        host="localhost",
        database="oltp_ecommerce_db",
        user="admin",
        password="admin",
        port="5433"
    )

    with conn.cursor() as cur, open('/data/loyalty_program_data.csv', 'r') as f:
        reader = csv.DictReader(f)

        # Prepare data batch (1000 records per batch)
        batch = []
        batch_size = 1000

        for row in reader:
            batch.append((
                row['customer_id'],
                int(row['points']),
                row['last_updated']
            ))

            # Insert when batch reaches batch_size
            if len(batch) >= batch_size:
                execute_batch(
                    cur,
                    """
                    INSERT INTO loyalty_program 
                    (customer_id, points, last_updated)
                    VALUES (%s, %s, %s)
                    """,
                    batch
                )
                batch = []

        # Insert remaining records
        if batch:
            execute_batch(
                cur,
                """
                INSERT INTO loyalty_program 
                (customer_id, points, last_updated)
                VALUES (%s, %s, %s)
                """,
                batch
            )

        conn.commit()
        print(f"Successfully inserted {len(batch)} records into loyalty_program")


batch_insert_loyalty_program()