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


def import_customer_coupons():
    try:
        with conn.cursor() as cur, open('/data/customer_coupons.csv', 'r') as f:
            reader = csv.DictReader(f)

            # Prepare batch
            batch = []
            batch_size = 1000  # Adjust based on your needs

            for row in reader:
                batch.append((
                    row['customer_id'],
                    row['coupon_id'],
                    row['used'].lower() == 'true'  # Convert to boolean
                ))

                if len(batch) >= batch_size:
                    execute_batch(
                        cur,
                        """
                        INSERT INTO customer_coupons 
                        (customer_id, coupon_id, used)
                        VALUES (%s, %s, %s)
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
                    INSERT INTO customer_coupons 
                    (customer_id, coupon_id, used)
                    VALUES (%s, %s, %s)
                    """,
                    batch
                )

            conn.commit()
            print("Successfully imported customer coupons data")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    import_customer_coupons()