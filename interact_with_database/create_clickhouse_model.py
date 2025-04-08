import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError


def create_clickhouse_tables():
    try:
        CLICKHOUSE_HOST = "localhost"
        CLICKHOUSE_PORT = 8123
        CLICKHOUSE_USER = "admin"
        CLICKHOUSE_PASSWORD = "Matkhauchung1@"

        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )

        print("Kết nối thành công đến ClickHouse server")

        client.command("DROP DATABASE IF EXISTS DWH")
        client.command("CREATE DATABASE DWH")
        print("Đã tạo database DWH")

        ddl_statements = [
            # Dimension: Customer
            """
            CREATE TABLE DWH.dim_customer (
                customer_key String,
                customer_phone String,
                customer_email String,
                customer_name String,
                is_member UInt8,
                customer_status String,
                loyalty_points UInt64
            ) ENGINE = MergeTree()
            ORDER BY customer_key
            """,

            # Dimension: Category
            """
            CREATE TABLE DWH.dim_category (
                category_key String,
                category_name String,
                parent_category_id Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY category_key
            """,

            # Dimension: Date
            """
            CREATE TABLE DWH.dim_date (
                date_key String,
                full_date Date,
                day UInt8,
                month UInt8,
                quarter UInt8,
                year UInt16,
                day_of_week String,
                is_weekend UInt8
            ) ENGINE = MergeTree()
            ORDER BY date_key
            """,

            # Dimension: Location
            """
            CREATE TABLE DWH.dim_location (
                location_key String,
                country String,
                city String,
                district String
            ) ENGINE = MergeTree()
            ORDER BY location_key
            """,

            # Dimension: Product
            """
            CREATE TABLE DWH.dim_products (
                product_key String,
                product_name String,
                brand String,
                cost Decimal(10, 2),
                current_price Decimal(10, 2),
                stock_quantity UInt64
            ) ENGINE = MergeTree()
            ORDER BY product_key
            """,

            # Dimension: Shipments
            """
            CREATE TABLE DWH.dim_shipments (
                shipment_key String,
                carrier String,
                status String
            ) ENGINE = MergeTree()
            ORDER BY shipment_key
            """,

            # Dimension: Vendors
            """
            CREATE TABLE DWH.dim_vendors (
                vendor_key String,
                name String,
                email String,
                representative_phone String,
                type String,
                expense Decimal(15,2),
                revenue Decimal(15,2)
            ) ENGINE = MergeTree()
            ORDER BY vendor_key
            """,

            # Fact Table: Orders
            """
            CREATE TABLE DWH.fact_orders (
                order_id String,
                order_detail_id String,
                time_id String,
                customer_id String,
                location_id String,
                product_id String,
                category_id String,
                shipment_id String,
                quantity UInt64,
                price Decimal(10, 2),
                cost_price Decimal(10, 2),
                order_status String,
                payment_status String
            ) ENGINE = MergeTree()
            ORDER BY (order_id, order_detail_id)
            SETTINGS max_suspicious_broken_parts = 5000
            """
        ]

        for ddl in ddl_statements:
            try:
                client.command(ddl)
                table_name = ddl.split("DWH.")[1].split("(")[0].strip()
                print(f"✅ Đã tạo bảng: {table_name}")
            except ClickHouseError as e:
                print(f"❌ Lỗi khi tạo bảng: {e}")
                continue

        # Kiểm tra các bảng đã tạo
        tables = client.query("SHOW TABLES FROM DWH").result_rows
        for table in tables:
            print(f"- {table[0]}")


    except Exception as e:
        print(f"{e}")
    finally:
        if 'client' in locals():
            client.close()


if __name__ == "__main__":
    create_clickhouse_tables()
