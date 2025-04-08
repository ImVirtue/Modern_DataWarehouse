import clickhouse_connect

# Thông tin kết nối ClickHouse
CLICKHOUSE_HOST = "localhost"  # Đổi thành IP hoặc domain thực tế
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "Matkhauchung1@"

try:
    # Kết nối ClickHouse
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    # Thử truy vấn
    result = client.query("SELECT version()")
    print("✅ Kết nối thành công!")
    print("🔹 ClickHouse Version:", result.result_rows[0][0])

except Exception as e:
    print("❌ Lỗi kết nối:", str(e))
