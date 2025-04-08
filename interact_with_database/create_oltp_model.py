import psycopg2

# Database connection parameters
DB_NAME = "oltp_ecommerce_db"
DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5433"

def create_tables(conn):
    with conn.cursor() as cursor:
        # Create categories table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL,
            parent_category_id BIGINT,
            created_time TIMESTAMP NOT NULL,
            updated_time TIMESTAMP NOT NULL,
            FOREIGN KEY (parent_category_id) REFERENCES categories(category_id)
        );""")

        # Create vendors table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
            vendor_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL,
            representative_phone VARCHAR(20) NOT NULL,
            address TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            type VARCHAR(50) NOT NULL,
            expense DECIMAL(15, 2) DEFAULT 0,
            revenue DECIMAL(15, 2) DEFAULT 0
        );""")

        # Create products table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_name VARCHAR(255) NOT NULL,
            category_id BIGINT NOT NULL,
            description TEXT,
            brand VARCHAR(255),
            cost_price DECIMAL(10, 2),
            price DECIMAL(10, 2) NOT NULL,
            stock_quantity INT NOT NULL,
            created_time TIMESTAMP NOT NULL,
            updated_time TIMESTAMP NOT NULL,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        );""")

        # Create inventory table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            inventory_id SERIAL PRIMARY KEY,
            product_id VARCHAR(50) NOT NULL,
            quantity INT NOT NULL,
            status VARCHAR(50) NOT NULL,
            created_time TIMESTAMP NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );""")

        # Create customers table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            customer_email VARCHAR(255) NOT NULL,
            is_member BOOLEAN DEFAULT FALSE,
            customer_phone VARCHAR(50) NOT NULL,
            created_time TIMESTAMP NOT NULL,
            address TEXT NOT NULL,
            customer_status VARCHAR(50) NOT NULL,
            location Varchar(100) NOT NULL
        );""")

        # Create coupon table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS coupon (
            coupon_id VARCHAR(50) PRIMARY KEY,
            discount_percentage INT NOT NULL,
            type VARCHAR(50) NOT NULL,
            expired_time DATE NOT NULL,
            is_active BOOLEAN DEFAULT TRUE
        );""")

        # Create customer_coupons table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_coupons (
            customer_id VARCHAR(50) NOT NULL,
            coupon_id VARCHAR(50) NOT NULL,
            used BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (customer_id, coupon_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (coupon_id) REFERENCES coupon(coupon_id)
        );""")

        # Create orders table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            created_time TIMESTAMP NOT NULL,
            updated_time TIMESTAMP NOT NULL,
            total_price DECIMAL(15, 2) NOT NULL,
            quantity INT NOT NULL,
            payment_status VARCHAR(50) NOT NULL,
            order_status VARCHAR(50) NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );""")

        # Create order_details table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_details (
            order_detail_id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            quantity INT NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            created_time TIMESTAMP NOT NULL,
            updated_time TIMESTAMP NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );""")

        # Create shipments table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            carrier VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            estimated_delivery DATE,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );""")

        # Create product_review table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_review (
            review_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            rating VARCHAR(50) NOT NULL,
            review_text TEXT,
            created_time BIGINT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );""")

        # Create loyalty_program table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_program (
            customer_id VARCHAR(50) PRIMARY KEY,
            points INT NOT NULL DEFAULT 0,
            last_updated timestamp NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );""")

        # Create customer_support table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_support (
            ticket_id VARCHAR(20) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            order_id VARCHAR(50),
            issue_type VARCHAR(50) NOT NULL,
            description TEXT NOT NULL,
            created_time BIGINT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );""")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vendor_product (
            vendor_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            supply_price DECIMAL(10,2) NOT NULL,
            stock_quantity BIGINT NOT NULL,
            PRIMARY KEY (vendor_id, product_id),
            FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );""")

        conn.commit()
        print("All tables created successfully!")



def main():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        # Create tables
        create_tables(conn)

        # Generate sample data
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()