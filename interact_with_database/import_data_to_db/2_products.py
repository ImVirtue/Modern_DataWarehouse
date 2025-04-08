import psycopg2


def copy_products_from_csv():
    conn = psycopg2.connect(
        host="localhost",
        database="oltp_ecommerce_db",
        user="admin",
        password="admin",
        port="5433"
    )

    try:
        with conn.cursor() as cur, open('/products.csv', 'r') as f:
            # Bỏ qua header nếu file CSV có header
            next(f)

            cur.copy_expert(
                """
                COPY products(
                    product_id,
                    product_name, 
                    category_id,
                    description,
                    brand,
                    cost_price,
                    price,
                    stock_quantity,
                    created_time,
                    updated_time
                ) 
                FROM STDIN 
                WITH (
                    FORMAT CSV,
                    DELIMITER ',',
                    NULL '',  
                    HEADER    
                )
                """,
                f
            )
        conn.commit()
        print("Import sản phẩm thành công!")

    except Exception as e:
        conn.rollback()
        print(f"Lỗi khi import: {str(e)}")
    finally:
        conn.close()


copy_products_from_csv()