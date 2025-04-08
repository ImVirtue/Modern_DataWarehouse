import psycopg2
import csv
from psycopg2.extras import execute_batch

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="oltp_ecommerce_db",
    user="admin",
    password="admin",
    port="5433"
)


def insert_coupons_from_csv():
    try:
        with conn.cursor() as cur, open('/data/coupons.csv', 'r') as f:
            reader = csv.DictReader(f)

            # Prepare batch
            batch = []
            batch_size = 1000

            for row in reader:
                batch.append((
                    row['coupon_id'],
                    int(row['discount_percentage']),
                    row['type'],
                    row['expired_time'],
                    row['is_active'].lower() == 'true'  # Convert to boolean
                ))

                if len(batch) >= batch_size:
                    execute_batch(
                        cur,
                        """
                        INSERT INTO coupon 
                        (coupon_id, discount_percentage, type, expired_time, is_active)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        batch
                    )
                    batch = []
                    print(f"Inserted {reader.line_num} records...")

            # Insert remaining records
            if batch:
                execute_batch(
                    cur,
                    """
                    INSERT INTO coupon 
                    (coupon_id, discount_percentage, type, expired_time, is_active)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    batch
                )

            conn.commit()
            print("Successfully imported all coupons from CSV")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    insert_coupons_from_csv()